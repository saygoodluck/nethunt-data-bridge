import {createClient} from '@clickhouse/client';
import {Client} from 'ssh2';
import {createClient as createRedisClient} from 'redis';
import express from 'express';
import cron from 'node-cron';
import axios from 'axios';
import crypto from 'node:crypto';

import {createTelegramBot} from './telegram.js';

import fs from 'fs';
import net from 'net';
import os from 'node:os';
import path from 'node:path';
import {performance} from 'node:perf_hooks';

import 'dotenv/config';

const app = express();
const sshClient = new Client();
const PORT = process.env.PORT || 3000;

let clickhouseClient;
let tunnelServer = null;
// the process stays alive while these are false; neither is fatal
let clickhouseReady = false;
let connecting = false;
let netHuntReady = false;

// Local development: ClickHouse is reachable directly, no bastion in between
const skipSSHTunnel = process.env.SKIP_SSH_TUNNEL === '1';
const netHuntBaseUrl = process.env.NETHUNT_BASE_URL || 'https://nethunt.com/api/v2';

const requiredEnvVars = [
    'CLICKHOUSE_HOST', 'CLICKHOUSE_PORT', 'CLICKHOUSE_USER',
    'CLICKHOUSE_PASSWORD', 'CLICKHOUSE_DATABASE', 'NETHUNT_FOLDER_ID',
    'NETHUNT_UTILS_FOLDER_ID', 'NETHUNT_API_TOKEN', 'SYNC_API_KEY'
];

if (!skipSSHTunnel) {
    requiredEnvVars.push('SSH_HOST', 'SSH_PORT', 'SSH_USER');
}

requiredEnvVars.forEach(env => {
    if (!process.env[env]) {
        console.error(`Missing required environment variable: ${env}`);
        process.exit(1);
    }
});

const dbServer = {
    host: process.env.CLICKHOUSE_HOST,
    port: Number(process.env.CLICKHOUSE_PORT),
    user: process.env.CLICKHOUSE_USER,
    password: process.env.CLICKHOUSE_PASSWORD,
    database: process.env.CLICKHOUSE_DATABASE
};

// In a container there is no key file to point at, so allow passing it directly.
// Base64 first: env editors mangle the newlines a PEM key needs.
const readPrivateKey = () => {
    if (process.env.SSH_PRIVATE_KEY_B64) {
        // environment editors like to add surrounding quotes and trailing
        // whitespace; base64 decoding turns either into an unparsable key
        const encoded = process.env.SSH_PRIVATE_KEY_B64.trim().replace(/^["']|["']$/g, '');
        let key = Buffer.from(encoded, 'base64');

        // `base64 -w0` prints no trailing newline, so a copy-paste easily picks
        // up the shell prompt after it. Those characters are largely valid
        // base64, so they decode into bytes appended to the key rather than
        // being ignored -- cut at the end marker instead of trusting the length.
        const marker = '-----END OPENSSH PRIVATE KEY-----';
        const end = key.indexOf(marker);
        if (end !== -1) {
            key = key.subarray(0, end + marker.length + 1);
        }

        if (!key.includes('PRIVATE KEY')) {
            throw new Error(
                'SSH_PRIVATE_KEY_B64 does not decode to a private key ' +
                `(got ${key.length} bytes starting "${key.subarray(0, 24).toString().trim()}"). ` +
                'Encode the private key, not the .pub, with: base64 -w0 ~/.ssh/id_ed25519'
            );
        }
        return key;
    }
    if (process.env.SSH_PRIVATE_KEY) {
        return process.env.SSH_PRIVATE_KEY;
    }
    const keyPath = process.env.SSH_KEY_PATH || './id_ed25519';
    // ssh expands a leading ~ itself, Node does not: a path that works in a
    // shell check would fail here with ENOENT
    const resolved = keyPath.startsWith('~/')
        ? path.join(os.homedir(), keyPath.slice(2))
        : keyPath;

    try {
        return fs.readFileSync(resolved);
    } catch (err) {
        if (err.code !== 'ENOENT') throw err;
        // a bare ENOENT says nothing about the three ways to supply the key
        throw new Error(
            `No SSH key at ${resolved}. Either set SSH_PRIVATE_KEY_B64 to the ` +
            `base64 of the private key (base64 -w0 ~/.ssh/id_ed25519), which needs ` +
            `no file at all, or mount the key at that path and make sure uid 1000 ` +
            `can read it.`
        );
    }
};

const buildTunnelConfig = () => ({
    host: process.env.SSH_HOST,
    port: Number(process.env.SSH_PORT),
    username: process.env.SSH_USER,
    privateKey: readPrivateKey()
});

const forwardConfig = {
    srcHost: '127.0.0.1',
    srcPort: 3306,
    dstHost: dbServer.host,
    dstPort: dbServer.port
};

const appConfig = {
    maxConcurrentRequests: Number(process.env.MAX_CONCURRENT_REQUESTS) || 5,
    batchSize: Number(process.env.BATCH_SIZE) || 100,
    syncInterval: process.env.SYNC_INTERVAL || 1,
    cacheTtlSeconds: Number(process.env.CACHE_TTL_SECONDS) || 14 * 24 * 60 * 60,
    retry: {
        retries: Number(process.env.RETRY_RETRIES) || 3,
        factor: Number(process.env.RETRY_FACTOR) || 2,
        minTimeout: Number(process.env.RETRY_MIN_TIMEOUT) || 1000,
        maxTimeout: Number(process.env.RETRY_MAX_TIMEOUT) || 10000
    }
}

const connectRetry = {
    minTimeout: Number(process.env.CONNECT_MIN_TIMEOUT) || 2000,
    maxTimeout: Number(process.env.CONNECT_MAX_TIMEOUT) || 60000,
    factor: 2,
    // only bother the chat once it is clearly not a blip
    alertAfterAttempts: Number(process.env.CONNECT_ALERT_AFTER) || 5
};

const redisConfig = {
    url: process.env.REDIS_URL || 'redis://localhost:6379',
    database: Number(process.env.REDIS_DATABASE) || 0
}

// NetHunt statuses meaning the cached record no longer exists
const STALE_RECORD_STATUSES = [404, 410];

// Retrying anything else is pointless: a rejected payload stays rejected.
// 429 is rate limiting, 409 an optimistic-concurrency clash, 408 a timeout.
const RETRYABLE_STATUSES = [408, 409, 429];

const recordsFolder = () => process.env.NETHUNT_FOLDER_ID;
const utilsFolder = () => process.env.NETHUNT_UTILS_FOLDER_ID;

// v2 keys field values by field id, so the schema is resolved once at startup.
// Each entry is {id, valueType}: the declared type decides how a value is
// serialised, which keeps a text field that should have been numeric working.
const fieldIds = {records: new Map(), utils: new Map()};

const RECORD_FIELD_NAMES = [
    'FundistUserID', 'Login', 'FirstName', 'LastName', 'Email', 'PhoneNumber',
    'PhoneVerified', 'DateOfBirth', 'Gender', 'Language', 'Country', 'City',
    'Timezone', 'LastCreditDate', 'RegistrationDate', 'LastLoginDate', 'PEP',
    'AccountStatus', 'TotalDeposit', 'TotalWithdraw'
];
const UTILS_FIELD_NAMES = [
    'finishedAt', 'totalSynced', 'duration', 'createdRecords',
    'updatedRecords', 'errorMessage'
];

// values the sync sends as JSON numbers
const NUMERIC_FIELDS = ['FundistUserID', 'TotalDeposit', 'TotalWithdraw'];
// values the sync sends as plain strings; a real date field would demand
// epoch milliseconds instead and reject every write
const DATE_LIKE_FIELDS = [
    'DateOfBirth', 'LastCreditDate', 'RegistrationDate', 'LastLoginDate'
];

// NetHunt bills by requests per minute and answers 429 past the allowance.
// Staying under it locally is far better than being rejected: a refused request
// costs the same quota as a served one.
const rateLimit = {
    perMinute: Number(process.env.NETHUNT_RATE_LIMIT) || 90,
    // Bucket capacity, kept well below the rate on purpose. A bucket as deep as
    // the rate lets a full burst land immediately and then refill inside the
    // same minute, so a 60-second window can carry twice the allowance -- which
    // is exactly what a hard limit rejects.
    burst: Number(process.env.NETHUNT_RATE_BURST) || 5
};

let tokens = rateLimit.burst;
let lastRefill = Date.now();
let requestsMade = 0;

async function takeRequestSlot() {
    while (true) {
        const now = Date.now();
        tokens = Math.min(
            rateLimit.burst,
            tokens + (now - lastRefill) * rateLimit.perMinute / 60000
        );
        lastRefill = now;

        if (tokens >= 1) {
            tokens -= 1;
            requestsMade++;
            return;
        }
        await sleep(Math.ceil((1 - tokens) * 60000 / rateLimit.perMinute));
    }
}

async function netHunt(method, path, {body, params, timeout = 15000} = {}) {
    await takeRequestSlot();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeout);
    try {
        const response = await axios({
            method,
            url: `${netHuntBaseUrl}${path}`,
            data: body,
            params,
            headers: {Authorization: `Bearer ${process.env.NETHUNT_API_TOKEN}`},
            signal: controller.signal
        });
        return response.data;
    } finally {
        clearTimeout(timer);
    }
}

const redisClient = await createRedisClient(redisConfig)
    .on('error', (err) => console.log('Redis Client Error', err))
    .on('connect', () => console.log('Connected to Redis'))
    .connect();

const connectClickHouse = (url) => {
    clickhouseClient = createClient({
        url,
        username: dbServer.user,
        password: dbServer.password,
        database: dbServer.database
    });
    // Test connection
    return clickhouseClient.query({query: 'SELECT 1'});
};

const connectDirectly = async () => {
    const url = `http://${dbServer.host}:${dbServer.port}`;
    await connectClickHouse(url);
    console.log(`SSH tunnel skipped, ClickHouse used directly on ${url}`);
};

const formatUptime = (seconds) => {
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    return h ? `${h}h ${m}m` : `${m}m`;
};

const telegramBot = createTelegramBot({
    redisClient,
    // executeSync is hoisted; the bot only calls it once a command arrives
    runSync: () => executeSync(),
    getStatus: async () => {
        const check = async (probe) => {
            try {
                await probe();
                return 'ok';
            } catch (err) {
                return `error: ${err.message}`;
            }
        };
        return {
            uptime: formatUptime(process.uptime()),
            clickhouse: await check(() => clickhouseClient.query({query: 'SELECT 1'})),
            redis: await check(() => redisClient.ping()),
            cachedIds: await redisClient.dbSize().catch(() => 'unknown')
        };
    }
});

// A persistent listener is required: ssh2 emitting 'error' with nothing
// attached would take the process down.
sshClient.on('error', (err) => console.error('SSH client error:', err.message));

const setupSSHTunnel = () => new Promise((resolve, reject) => {
    // one-shot listeners, removed on both outcomes: `on` here would leave a
    // handler behind on every reconnect, and each one builds another tunnel
    const cleanup = () => {
        sshClient.removeListener('ready', onReady);
        sshClient.removeListener('error', onError);
    };

    const fail = (err) => {
        cleanup();
        reject(err);
    };

    function onError(err) {
        fail(err);
    }

    function onReady() {
        if (tunnelServer) {
            tunnelServer.close();
            tunnelServer = null;
        }

        tunnelServer = net.createServer((socket) => {
            sshClient.forwardOut(
                socket.remoteAddress,
                socket.remotePort,
                forwardConfig.dstHost,
                forwardConfig.dstPort,
                (err, stream) => {
                    if (err) {
                        socket.destroy();
                        return;
                    }
                    socket.pipe(stream).pipe(socket);
                }
            );
        });

        // listen() reports failures through the error event, never through its
        // callback, so EADDRINUSE used to surface as an uncaught exception
        tunnelServer.once('error', fail);

        tunnelServer.listen(forwardConfig.srcPort, forwardConfig.srcHost, () => {
            console.log(`SSH tunnel ready on ${forwardConfig.srcHost}:${forwardConfig.srcPort}`);

            connectClickHouse(`http://${forwardConfig.srcHost}:${forwardConfig.srcPort}`)
                .then(() => {
                    cleanup();
                    resolve();
                })
                .catch(fail);
        });
    }

    sshClient.once('ready', onReady);
    sshClient.once('error', onError);
    sshClient.connect(buildTunnelConfig());
});

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Keeps trying instead of exiting: under a container restart policy a hard exit
// turns an unreachable bastion into a crash loop, which reports nothing and
// silences the startup notice.
async function connectWithRetry() {
    if (connecting) return;
    connecting = true;

    let attempt = 0;
    let alerted = false;

    while (!clickhouseReady) {
        try {
            await (skipSSHTunnel ? connectDirectly() : setupSSHTunnel());
            clickhouseReady = true;
            // success was silent before: the absence of an error was the only sign
            console.log(`ClickHouse connected (${dbServer.database} on ${dbServer.host})`);

            if (alerted) {
                await telegramBot.sendAlert(
                    `\u{1F7E2} <b>ClickHouse connection restored</b>\n\nRecovered after ${attempt} failed attempts.`
                );
            }
        } catch (err) {
            attempt++;
            const delay = Math.min(
                connectRetry.minTimeout * Math.pow(connectRetry.factor, attempt - 1),
                connectRetry.maxTimeout
            );
            console.error(
                `ClickHouse connection failed (attempt ${attempt}): ${err.message}. ` +
                `Retrying in ${(delay / 1000).toFixed(0)}s`
            );

            if (attempt === connectRetry.alertAfterAttempts && !alerted) {
                alerted = true;
                await telegramBot.sendAlert(
                    `\u{1F534} <b>Cannot reach ClickHouse</b>\n\n` +
                    `<code>${err.message}</code>\n\n` +
                    `Failed ${attempt} times. Retrying quietly; you will be told when it recovers.`
                );
            }

            await sleep(delay);
        }
    }

    connecting = false;
}

const authenticate = (req, res, next) => {
    const authHeader = req.headers.authorization;

    if (!authHeader || !authHeader.startsWith('Bearer ')) {
        return res.status(401).json({error: 'Missing or invalid Authorization header'});
    }

    const apiKey = authHeader.split(' ')[1];
    if (apiKey !== process.env.SYNC_API_KEY) {
        return res.status(401).json({error: 'Invalid API key'});
    }

    next();
};

app.get('/', async (req, res) => {
    // deliberately 200 even while connecting: the container is alive, and
    // failing this check would make the orchestrator restart it pointlessly
    res.json({
        status: 'ok',
        message: 'Service is operational',
        clickhouse: clickhouseReady ? 'connected' : 'connecting',
        nethunt: netHuntReady ? 'ready' : 'loading schema'
    });
});

app.get('/sync', authenticate, async (req, res) => {
    if (syncInProgress) {
        return res.status(409).json({
            status: 'error',
            message: 'A sync is already running'
        });
    }
    if (!clickhouseReady || !netHuntReady) {
        return res.status(503).json({
            status: 'error',
            message: !clickhouseReady
                ? 'ClickHouse is not connected yet'
                : 'The NetHunt schema has not been read yet'
        });
    }

    // Optional overrides so a backfill needs no environment change and no
    // redeploy, and leaves the schedule running with its usual window.
    const options = {};

    if (req.query.since !== undefined) {
        const since = new Date(req.query.since);
        if (Number.isNaN(since.getTime())) {
            return res.status(400).json({
                status: 'error',
                message: `since is not a date: ${req.query.since}`
            });
        }
        if (since.getTime() > Date.now()) {
            return res.status(400).json({
                status: 'error',
                message: 'since is in the future, which would select nothing'
            });
        }
        options.since = since;
    }

    if (req.query.batchSize !== undefined) {
        const batchSize = Number(req.query.batchSize);
        if (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > 1000) {
            return res.status(400).json({
                status: 'error',
                message: 'batchSize must be a whole number between 1 and 1000'
            });
        }
        options.batchSize = batchSize;
    }

    try {
        const result = await executeSync(options);
        res.json({
            status: 'success',
            message: 'Sync completed',
            data: result
        });
    } catch (error) {
        console.error('Sync error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Sync failed',
            error: error.message
        });
    }
});

// The HTTP server and the bot come up first, so /status and the chat can
// explain a connection problem instead of the container dying silently.
app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
}).on('error', (err) => {
    // Without this a taken port throws a bare stack trace, and an older process
    // keeps answering on it -- so the service looks healthy while serving code
    // that is not the one just deployed.
    if (err.code === 'EADDRINUSE') {
        console.error(`Port ${PORT} is already in use; another instance is probably running`);
    } else {
        console.error('HTTP server failed:', err);
    }
    process.exit(1);
});
telegramBot.startPolling();
telegramBot.notifyStartup().catch(console.error);
connectWithRetry().catch(err => console.error('Connection loop stopped:', err));
loadSchemaWithRetry().catch(err => console.error('Schema loop stopped:', err));

sshClient.on('close', () => {
    if (skipSSHTunnel) return;
    console.log('SSH connection closed. Reconnecting...');
    clickhouseReady = false;
    connectWithRetry().catch(err => console.error('Reconnect loop stopped:', err));
});

process.on('SIGINT', async () => {
    console.log('\nGracefully shutting down...');
    try {
        telegramBot.stop();
        await clickhouseClient?.close();
        await redisClient?.quit();
        console.log('Connections closed');
        process.exit(0);
    } catch (err) {
        console.error('Shutdown error:', err);
        process.exit(1);
    }
});

// Scheduled sync
cron.schedule('0,30 * * * *', async () => {
    if (syncInProgress) {
        console.warn('Skipping scheduled sync: the previous one is still running');
        return;
    }
    if (!clickhouseReady || !netHuntReady) {
        // otherwise every run would write a failed-sync record into NetHunt
        console.warn('Skipping scheduled sync: dependencies are not ready');
        return;
    }
    console.log(`${new Date()} | Running scheduled task...`);
    await executeSync().catch(console.error);
});

if (telegramBot.enabled && telegramBot.dailyReportAt) {
    const [hour, minute] = telegramBot.dailyReportAt.split(':');
    cron.schedule(`${Number(minute)} ${Number(hour)} * * *`, () => {
        telegramBot.sendDailyReport().catch(console.error);
    });
    console.log(`Daily Telegram report scheduled at ${telegramBot.dailyReportAt}`);
}

// Same treatment as the ClickHouse connection: keep serving and keep trying,
// rather than exiting and letting the restart policy hide the reason.
async function loadSchemaWithRetry() {
    let attempt = 0;
    let alerted = false;

    while (!netHuntReady) {
        try {
            await loadNetHuntSchema();
            netHuntReady = true;
            if (alerted) {
                await telegramBot.sendAlert('\u{1F7E2} <b>NetHunt schema loaded</b>\n\nRecovered.');
            }
        } catch (err) {
            attempt++;
            const delay = Math.min(
                connectRetry.minTimeout * Math.pow(connectRetry.factor, attempt - 1),
                connectRetry.maxTimeout
            );
            console.error(
                `NetHunt schema load failed (attempt ${attempt}): ${err.message}. ` +
                `Retrying in ${(delay / 1000).toFixed(0)}s`
            );
            if (attempt === connectRetry.alertAfterAttempts && !alerted) {
                alerted = true;
                await telegramBot.sendAlert(
                    `\u{1F534} <b>Cannot read the NetHunt schema</b>\n\n` +
                    `<code>${err.message}</code>\n\n` +
                    `Syncing is paused until this resolves.`
                );
            }
            await sleep(delay);
        }
    }
}

// A wide-window backfill runs for hours while the schedule keeps firing every
// half hour. Without this each tick would start another full pass over the same
// data, splitting the request allowance between runs that duplicate each other.
let syncInProgress = false;

async function executeSync(options = {}) {
    if (syncInProgress) {
        throw new Error('A sync is already running');
    }
    syncInProgress = true;

    const start = performance.now();
    // a failed sync still reports: zeros plus the error, instead of blowing up here
    let result = {totalSynced: 0, createdRecords: 0, updatedRecords: 0, skippedRecords: 0};
    let error = null;
    let syncData;

    // finally, not a plain assignment: a flag left set by an unexpected throw
    // would stop every future run, which is worse than the overlap it prevents
    try {
        try {
            result = await syncRecords(options);
        } catch (err) {
            error = err;
            console.error('Sync error:', err);
        }

        const duration = performance.now() - start;
        console.log(
            `Duration: ${(duration / 1000).toFixed(2)} seconds` +
            `, ${result.skippedRecords} record(s) unchanged`
        );

        syncData = {
            finishedAt: new Date(),
            totalSynced: result.totalSynced,
            duration: duration,
            createdRecords: result.createdRecords,
            updatedRecords: result.updatedRecords,
            errorMessage: error ? error.message : null
        };
    } finally {
        syncInProgress = false;
    }

    await sendMetrics(syncData);
    await telegramBot.notifySyncResult(syncData);

    // metrics are written either way, but the caller still learns it failed
    if (error) throw error;
    return syncData;
}

async function syncRecords({since: explicitSince, batchSize: explicitBatchSize} = {}) {
    // Comfortably above any real UserID and still exact in JS, so the first page
    // has no upper bound in practice. It is below the true UInt64 maximum: ids
    // past 2^53 would need max(UserID) to be read instead.
    let cursor = Number.MAX_SAFE_INTEGER;
    let totalSynced = 0;
    let createdRecords = 0;
    let updatedRecords = 0;
    let skippedRecords = 0;

    const batchSize = explicitBatchSize || appConfig.batchSize;

    try {
        console.log('Starting sync...');
        const requestsAtStart = requestsMade;

        // the utils folder is empty until the first run ever writes metrics into it
        const {finishedAt} = await getLastSyncTime() ?? {};

        // Pinned once, not re-evaluated per page: with now() in the query the
        // window would slide during a long run, and a user could land in two
        // pages or in none.
        const since = explicitSince
            ? Math.floor(explicitSince.getTime() / 1000)
            : Math.floor(Date.now() / 1000) - calculateInterval(finishedAt) * 60;

        console.log(
            `Window starts ${new Date(since * 1000).toISOString()}` +
            `, page size ${batchSize}`
        );

        while (true) {
            const records = await fetchRecordsBatch(cursor, since, batchSize);
            if (records.length === 0) break;

            // one lookup for the whole page, before it is split for concurrency
            const absent = await resolveKnownIds(records);

            const chunks = chunkArray(records, appConfig.maxConcurrentRequests);

            for (const chunk of chunks) {
                const results = await processChunk(chunk, absent);
                createdRecords += results.created;
                updatedRecords += results.updated;
                skippedRecords += results.skipped;
                totalSynced += chunk.length;

                logProgress(totalSynced, createdRecords, updatedRecords, skippedRecords);
            }

            // the page is ordered descending, so its last row carries the lowest id
            const next = Number(records[records.length - 1].FundistUserID);
            if (!(next < cursor)) {
                // with OFFSET a mistake here cost extra work; with a cursor it
                // is an endless loop that would burn the whole request allowance
                throw new Error(`Pagination stalled at UserID ${cursor}`);
            }
            cursor = next;
        }

        console.log(`\nSync completed. ${requestsMade - requestsAtStart} NetHunt requests used.`);
        return {totalSynced, createdRecords, updatedRecords, skippedRecords};
    } catch (err) {
        console.error('Sync failed:', err);
        throw err;
    }
}

async function getLastSyncTime() {
    const finishedAt = fieldIds.utils.get('finishedAt');

    // Listing records returns them oldest first, so the newest metrics record
    // has to be asked for by sorting -- taking the first of a plain list would
    // read the very first sync ever and blow the window wide open.
    const data = await netHunt('post', `/folders/${utilsFolder()}/records/filter`, {
        body: {
            filter: {[finishedAt.id]: {_exists: true}},
            sort: [{created: -1}]
        },
        params: {limit: 1}
    });

    const record = (data.records || [])[0];
    return record ? {finishedAt: record.fields?.[finishedAt.id]} : null;
}

function calculateInterval(lastSyncTime) {
    const DEFAULT_INTERVAL = process.env.SYNC_INTERVAL_MINUTES || 60;
    if (!lastSyncTime) {
        return DEFAULT_INTERVAL;
    }

    const now = new Date();
    const lastSync = new Date(lastSyncTime);

    const diffMs = now.getTime() - lastSync.getTime();
    const diffMinutes = Math.ceil(diffMs / (1000 * 60));
    console.info(`Last sync time detected: ${lastSyncTime}. Interval in minutes: ${diffMinutes}`);

    return Math.max(diffMinutes, DEFAULT_INTERVAL);
}

async function sendMetrics(data) {
    try {
        return await netHunt('post', `/folders/${utilsFolder()}/records`, {
            body: {
                fields: {
                    name: `Sync ${new Date(data.finishedAt).toISOString()}`,
                    ...toFieldPayload(data, fieldIds.utils)
                }
            }
        });
    } catch (err) {
        console.error('Error writing last sync time:', err.message);
    }
}

async function fetchRecordsBatch(cursor, since, batchSize) {
    // errors propagate on purpose: an empty array here would end the loop and
    // report the sync as successful
    const result = await clickhouseClient.query({
        query: clickHouseQuery,
        format: 'JSONEachRow',
        query_params: {batchSize, cursor, since}
    });
    return await result.json();
}

async function processChunk(chunk, absent = new Set()) {
    let created = 0;
    let updated = 0;
    let skipped = 0;

    const results = await Promise.allSettled(
        chunk.map(record => withRetry(() => processRecord(record, absent), appConfig.retry))
    );

    for (const result of results) {
        if (result.status === 'fulfilled') {
            if (result.value.action === 'created') created++;
            if (result.value.action === 'updated') updated++;
            if (result.value.action === 'unchanged') skipped++;
        } else {
            console.error('Record processing error:', result.reason);
        }
    }

    return {created, updated, skipped};
}

async function processRecord(record, absent = new Set()) {
    try {
        const hash = payloadHash(recordPayload(record));
        const hashKey = `h:${record.FundistUserID}`;

        const cachedNetHuntUserId = await redisClient.get(record.FundistUserID);
        if (cachedNetHuntUserId) {
            if (await redisClient.get(hashKey) === hash) {
                return {action: 'unchanged'};
            }
            try {
                await updateNetHuntRecord(cachedNetHuntUserId, record);
                await redisClient.set(hashKey, hash, {EX: appConfig.cacheTtlSeconds});
                return {action: 'updated'};
            } catch (err) {
                if (!isRecordGone(err)) throw err;
                console.warn(`Stale cache for ${record.FundistUserID}: NetHunt record ${cachedNetHuntUserId} is gone, re-resolving`);
                await redisClient.del(record.FundistUserID);
            }
        }

        // the page-level pass already established this user has no record, so
        // asking again would double the cost of a backfill for no new answer
        const [existing] = absent.has(String(record.FundistUserID))
            ? []
            : await searchNetHuntRecord(record.FundistUserID);
        if (existing) {
            await updateNetHuntRecord(existing.id, record);
            await cacheRecordId(record.FundistUserID, existing.id);
            await redisClient.set(hashKey, hash, {EX: appConfig.cacheTtlSeconds});
            return {action: 'updated'};
        }

        const {recordId} = await createNetHuntRecord(record);
        await cacheRecordId(record.FundistUserID, recordId);
        await redisClient.set(hashKey, hash, {EX: appConfig.cacheTtlSeconds});
        return {action: 'created'};
    } catch (err) {
        console.error(`Error processing ${record.FundistUserID}:`, {
            message: err.message,
            stack: err.stack
        });
        throw err;
    }
}

function isRecordGone(err) {
    return STALE_RECORD_STATUSES.includes(err.response?.status);
}

// The ClickHouse window catches any change to LastUpdated, including ones in
// columns the sync does not carry. Comparing the payload avoids paying a write
// for those -- and makes an interrupted backfill cheap to resume, since already
// synced users cost a Redis read and nothing else.
const payloadHash = (payload) =>
    crypto.createHash('sha1').update(JSON.stringify(payload)).digest('base64');

async function cacheRecordId(fundistUserId, recordId) {
    if (!recordId) {
        console.warn(`No record id to cache for ${fundistUserId}`);
        return;
    }
    await redisClient.set(fundistUserId, recordId, {EX: appConfig.cacheTtlSeconds});
}

// NetHunt API Helpers
//
// v2 addresses field values by field id, so the schema of both folders is read
// once at startup. A folder missing a field the sync writes is fatal here
// rather than a silently dropped value on the first record.
async function loadFolderSchema(folderId, expectedNames, label) {
    const folder = await netHunt('get', `/folders/${folderId}`);
    const byName = new Map((folder.fields || []).map(f => [f.name, f]));

    const missing = expectedNames.filter(name => !byName.has(name));
    if (missing.length) {
        throw new Error(
            `NetHunt folder "${folder.name}" (${label}, ${folderId}) has no field(s): ${missing.join(', ')}`
        );
    }

    const ids = new Map(expectedNames.map(name => {
        const field = byName.get(name);
        return [name, {id: field.id, valueType: field.valueType}];
    }));

    // A date field wants epoch milliseconds; the sync sends formatted strings,
    // so such a field would reject every write. Worth stopping for.
    const wrongDates = DATE_LIKE_FIELDS
        .filter(name => byName.has(name) && ['DATE', 'TIME'].includes(byName.get(name).valueType));
    if (wrongDates.length) {
        throw new Error(
            `NetHunt fields ${wrongDates.join(', ')} are date fields, but the sync sends text. ` +
            `Recreate them as text, or convert the values to epoch milliseconds first.`
        );
    }

    // Values are coerced to the declared type on the way out, so a mismatch is
    // survivable -- but worth saying out loud, because a numeric field sorts
    // and filters in the CRM and a text one does not
    for (const name of NUMERIC_FIELDS) {
        const field = byName.get(name);
        if (field && field.valueType !== 'NUMBER') {
            console.warn(
                `NetHunt field ${name} is ${field.valueType}; values are sent as text. ` +
                `Recreate it as a number field to sort and filter by it in the CRM.`
            );
        }
    }

    console.log(`NetHunt folder "${folder.name}" (${label}): ${expectedNames.length} fields resolved`);
    return ids;
}

async function loadNetHuntSchema() {
    fieldIds.records = await loadFolderSchema(recordsFolder(), RECORD_FIELD_NAMES, 'records');
    fieldIds.utils = await loadFolderSchema(utilsFolder(), UTILS_FIELD_NAMES, 'utils');
}

// Matches the value to what the field actually declares. ClickHouse hands us
// numbers for the money columns, but those fields may well have been created as
// text; sending the wrong shape is rejected outright, so convert instead.
function coerce(value, valueType) {
    if (valueType === 'NUMBER' && typeof value !== 'number') {
        const n = Number(value);
        return Number.isFinite(n) ? n : null;
    }
    if (valueType === 'STRING' && typeof value !== 'string') {
        // String(date) yields a locale-shaped stamp; the value is read back and
        // parsed to work out the sync window, so it has to round-trip exactly
        return value instanceof Date ? value.toISOString() : String(value);
    }
    return value;
}

// Turns a name-keyed object into the id-keyed payload v2 expects. Null values
// are dropped rather than sent: there is nothing to clear on a new record.
function toFieldPayload(values, fields) {
    const payload = {};
    for (const [name, value] of Object.entries(values)) {
        const field = fields.get(name);
        if (!field || value === null || value === undefined) continue;
        const converted = coerce(value, field.valueType);
        if (converted !== null && converted !== undefined) {
            payload[field.id] = converted;
        }
    }
    return payload;
}

function recordPayload(record) {
    return {
        // `name` is not a folder field in v2, it is the record's display name
        name: displayName(record),
        ...toFieldPayload(mapRecordFields(record), fieldIds.records)
    };
}

// One filter answers for a whole page of users. A number field has no _any
// operator, but an _or of equalities does the same job, turning a hundred
// lookups into a single request -- which is what makes a backfill feasible.
async function resolveKnownIds(records) {
    // ids this pass proved are absent from the CRM, so the per-record path can
    // create them without asking again
    const absent = new Set();

    const ids = records.map(r => String(r.FundistUserID));
    if (!ids.length) return absent;

    const cached = await redisClient.mGet(ids);
    const missing = ids.filter((_, i) => !cached[i]);
    if (!missing.length) return absent;

    const key = fieldIds.records.get('FundistUserID');
    let found = 0;

    for (let i = 0; i < missing.length; i += 100) {
        const slice = missing.slice(i, i + 100);
        const data = await netHunt('post', `/folders/${recordsFolder()}/records/filter`, {
            body: {filter: {_or: slice.map(id => ({[key.id]: coerce(id, key.valueType)}))}},
            params: {limit: slice.length}
        });

        const seen = new Set();
        for (const record of data.records || []) {
            const fundistId = record.fields?.[key.id];
            if (fundistId !== undefined && record.id) {
                await cacheRecordId(String(fundistId), record.id);
                seen.add(String(fundistId));
                found++;
            }
        }
        for (const id of slice) {
            if (!seen.has(id)) absent.add(id);
        }
    }

    console.log(
        `Resolved ${found} of ${missing.length} unknown ids in ` +
        `${Math.ceil(missing.length / 100)} request(s); ${absent.size} are new`
    );
    return absent;
}

// Still needed on its own: a cached id that turns out to be gone has to be
// re-resolved for that one record, outside the page-level pass above.
async function searchNetHuntRecord(userId) {
    const key = fieldIds.records.get('FundistUserID');
    const data = await netHunt('post', `/folders/${recordsFolder()}/records/filter`, {
        body: {filter: {[key.id]: coerce(userId, key.valueType)}},
        params: {limit: 1},
        timeout: 10000
    });
    return data.records || [];
}

async function createNetHuntRecord(record) {
    const data = await netHunt('post', `/folders/${recordsFolder()}/records`, {
        body: {fields: recordPayload(record)}
    });
    return {recordId: data.id};
}

// PATCH touches only the fields it is given, so anything the team owns -- the
// Comment field above all -- is left alone by construction.
async function updateNetHuntRecord(recordId, record) {
    return netHunt('patch', `/folders/${recordsFolder()}/records/${recordId}`, {
        body: {fields: recordPayload(record)}
    });
}

// NetHunt's built-in Name field is the record title shown in lists and cards.
// Without it every record reads as blank in the UI, whatever else it holds.
function displayName(record) {
    const full = [record.FirstName, record.LastName].filter(Boolean).join(' ').trim();
    return full || record.Login || String(record.FundistUserID);
}

function mapRecordFields(record) {
    return {
        FundistUserID: record.FundistUserID,
        Login: record.Login,
        FirstName: record.FirstName,
        LastName: record.LastName,
        Email: record.Email,
        PhoneNumber: record.PhoneNumber,
        PhoneVerified: record.PhoneVerified,
        DateOfBirth: record.DateOfBirth,
        Gender: record.Gender,
        Language: record.Language,
        Country: record.Country,
        City: record.City,
        Timezone: record.Timezone,
        LastCreditDate: record.LastCreditDate,
        RegistrationDate: record.RegistrationDate,
        LastLoginDate: record.LastLoginDate,
        PEP: record.PEP,
        AccountStatus: record.AccountStatus,
        TotalDeposit: record.TotalDeposit,
        TotalWithdraw: record.TotalWithdraw
    };
}

// Utils
function chunkArray(arr, size) {
    return Array.from(
        {length: Math.ceil(arr.length / size)},
        (_, i) => arr.slice(i * size, i * size + size)
    );
}

async function withRetry(fn, config) {
    let attempts = 0;
    while (true) {
        try {
            return await fn();
        } catch (err) {
            const status = err.response?.status;
            // a rejected payload stays rejected however often it is resent
            const worthRetrying = status === undefined
                || status >= 500
                || RETRYABLE_STATUSES.includes(status);
            if (!worthRetrying || ++attempts > config.retries) throw err;

            // when rate limited the API says how long to wait; obey it
            const retryAfter = Number(err.response?.headers?.['retry-after']);
            const delay = Number.isFinite(retryAfter) && retryAfter > 0
                ? retryAfter * 1000
                : Math.min(
                    config.minTimeout * Math.pow(config.factor, attempts - 1),
                    config.maxTimeout
                );
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
}

function logProgress(total, created, updated, skipped = 0) {
    const line = `Progress: ${total} | Created: ${created} | Updated: ${updated} | Unchanged: ${skipped}`;
    if (process.stdout.isTTY) {
        process.stdout.clearLine();
        process.stdout.cursorTo(0);
        process.stdout.write(line);
    } else {
        process.stdout.write(line + '\n');
    }
}

// @formatter:off
const clickHouseQuery = `
    SELECT uh.UserID as FundistUserID,
           argMax(uh.Login, uh.RecordTime) as Login,
           argMax(uh.Name, uh.RecordTime) as FirstName,
           argMax(uh.LastName, uh.RecordTime) as LastName,
           argMax(uh.Email, uh.RecordTime) as Email,
           argMax(uh.Phone, uh.RecordTime) as PhoneNumber,
           if(argMax(uh.PhoneVerified, uh.RecordTime) = 1, 'Verified', 'Unverified') as PhoneVerified,
           -- DateOfBirthNew is the current Date32 column and uses 1900-01-01 as
           -- "unknown"; the older Nullable(String) column still holds a real date
           -- for some users, so it is the fallback. Column names stay qualified:
           -- an unqualified one would resolve to this alias and nest the argMax.
           if(argMax(uh.DateOfBirthNew, uh.RecordTime) != toDate32('1900-01-01'),
              toString(argMax(uh.DateOfBirthNew, uh.RecordTime)),
              ifNull(argMax(uh.DateOfBirth, uh.RecordTime), '')) AS DateOfBirth,
           argMax(uh.Gender, uh.RecordTime) AS Gender,
           argMax(uh.Language, uh.RecordTime) AS Language,
           argMax(c.Name, uh.RecordTime) AS Country,
           argMax(uh.City, uh.RecordTime) AS City,
           argMax(uh.Timezone, uh.RecordTime) AS Timezone,
           argMax(uh.LastCreditDate, uh.RecordTime) AS LastCreditDate,
           argMax(uh.RegistrationDate, uh.RecordTime) AS RegistrationDate,
           argMax(uh.LastLoginDate, uh.RecordTime) AS LastLoginDate,
           if(argMax(uh.PEP, uh.RecordTime) = 1, 'PEP', '') AS PEP,
           if(argMax(uh.Status, uh.RecordTime) = 1, 'Active', 'Inactive') AS AccountStatus,
           any(t.TotalDeposit) as TotalDeposit,
           any(t.TotalWithdraw) as TotalWithdraw
    FROM UserHistory uh
             JOIN CountriesNew c ON c.ID = uh.CountryID
             LEFT JOIN (SELECT UserID,
                               sum(Deposit) / 100  AS TotalDeposit,
                               sum(Withdraw) / 100 AS TotalWithdraw
                        FROM Turnovers
                        -- Restricted to the users of this page, not of the whole
                        -- window. ClickHouse does not push the outer filter into
                        -- a joined subquery, and on a wide window the window
                        -- filter stops excluding anything -- which meant reading
                        -- the entire table again for every page.
                        -- GROUP BY is required: without it LIMIT counts history
                        -- rows rather than users, and this set would drift out
                        -- of step with the outer query.
                        WHERE UserID IN (SELECT UserID
                                         FROM UserHistory
                                         WHERE LastUpdated > toDateTime({since: UInt32})
                                           AND UserID < {cursor: UInt64}
                                         GROUP BY UserID
                                         ORDER BY UserID DESC
                                         LIMIT {batchSize: UInt32})
                        GROUP BY UserID) t ON uh.UserID = t.UserID
    -- Keyset paging. OFFSET made ClickHouse aggregate every group and discard
    -- the ones before it, so page 1300 cost as much as the whole table; a UserID
    -- bound prunes on the primary key instead, making every page cost the same.
    WHERE uh.LastUpdated > toDateTime({since: UInt32})
      AND uh.UserID < {cursor: UInt64}
    GROUP BY uh.UserID
    ORDER BY uh.UserID DESC
    LIMIT {batchSize: UInt32}
`;
// @formatter:on