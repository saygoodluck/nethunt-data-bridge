import {createClient} from '@clickhouse/client';
import {Client} from 'ssh2';
import {createClient as createRedisClient} from 'redis';
import express from 'express';
import cron from 'node-cron';
import axios from 'axios';

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
// the process stays alive while this is false; it is not a fatal condition
let clickhouseReady = false;
let connecting = false;

// Local development: ClickHouse is reachable directly, no bastion in between
const skipSSHTunnel = process.env.SKIP_SSH_TUNNEL === '1';
const netHuntBaseUrl = process.env.NETHUNT_BASE_URL || 'https://nethunt.com/api/v1/zapier';

const requiredEnvVars = [
    'CLICKHOUSE_HOST', 'CLICKHOUSE_PORT', 'CLICKHOUSE_USER',
    'CLICKHOUSE_PASSWORD', 'CLICKHOUSE_DATABASE', 'NETHUNT_FOLDER_ID',
    'NETHUNT_USER', 'NETHUNT_API_KEY'
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
        return Buffer.from(process.env.SSH_PRIVATE_KEY_B64, 'base64');
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
    return fs.readFileSync(resolved);
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
    if (apiKey !== process.env.NETHUNT_API_KEY) {
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
        clickhouse: clickhouseReady ? 'connected' : 'connecting'
    });
});

app.get('/sync', authenticate, async (req, res) => {
    if (!clickhouseReady) {
        return res.status(503).json({
            status: 'error',
            message: 'ClickHouse is not connected yet'
        });
    }

    try {
        const result = await executeSync();
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
});
telegramBot.startPolling();
telegramBot.notifyStartup().catch(console.error);
connectWithRetry().catch(err => console.error('Connection loop stopped:', err));

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
    if (!clickhouseReady) {
        // otherwise every run would write a failed-sync record into NetHunt
        console.warn('Skipping scheduled sync: ClickHouse is not connected');
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

async function executeSync() {
    const start = performance.now();
    // a failed sync still reports: zeros plus the error, instead of blowing up here
    let result = {totalSynced: 0, createdRecords: 0, updatedRecords: 0};
    let error = null;
    try {
        result = await syncRecords();
    } catch (err) {
        error = err;
        console.error('Sync error:', err);
    }

    const duration = performance.now() - start;
    console.log(`Duration: ${(duration / 1000).toFixed(2)} seconds`);

    const syncData = {
        finishedAt: new Date(),
        totalSynced: result.totalSynced,
        duration: duration,
        createdRecords: result.createdRecords,
        updatedRecords: result.updatedRecords,
        errorMessage: error ? error.message : null
    }
    await sendMetrics(syncData);
    await telegramBot.notifySyncResult(syncData);

    // metrics are written either way, but the caller still learns it failed
    if (error) throw error;
    return syncData;
}

async function syncRecords() {
    let offset = 0;
    let totalSynced = 0;
    let createdRecords = 0;
    let updatedRecords = 0;

    try {
        console.log('Starting sync...');

        // the utils folder is empty until the first run ever writes metrics into it
        const {finishedAt} = await getLastSyncTime() ?? {};
        const interval = calculateInterval(finishedAt);
        while (true) {
            const records = await fetchRecordsBatch(offset, interval);
            if (records.length === 0) break;

            const chunks = chunkArray(records, appConfig.maxConcurrentRequests);

            for (const chunk of chunks) {
                const results = await processChunk(chunk);
                createdRecords += results.created;
                updatedRecords += results.updated;
                totalSynced += chunk.length;

                logProgress(totalSynced, createdRecords, updatedRecords);
            }

            offset += appConfig.batchSize;
        }

        console.log(`\nSync completed.`);
        return {totalSynced, createdRecords, updatedRecords};
    } catch (err) {
        console.error('Sync failed:', err);
        throw err;
    }
}

async function getLastSyncTime() {
    const response = await axios.get(
        `${netHuntBaseUrl}/triggers/new-record/${process.env.NETHUNT_UTILS_FOLDER_ID}`,
        {
            auth: {
                username: process.env.NETHUNT_USER,
                password: process.env.NETHUNT_API_KEY
            }
        }
    );
    return response.data[0]?.fields;
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
        const response = await axios.post(
            `${netHuntBaseUrl}/actions/create-record/${process.env.NETHUNT_UTILS_FOLDER_ID}`,
            {
                fields: data,
                timeZone: "Europe/Warsaw"
            },
            {
                auth: {
                    username: process.env.NETHUNT_USER,
                    password: process.env.NETHUNT_API_KEY
                }
            }
        );
        return response.data;
    } catch (err) {
        console.error('Error writing last sync time:', err);
    }
}

async function fetchRecordsBatch(offset, interval) {
    // errors propagate on purpose: an empty array here would end the loop and
    // report the sync as successful
    const result = await clickhouseClient.query({
        query: clickHouseQuery,
        format: 'JSONEachRow',
        query_params: {batchSize: appConfig.batchSize, offset: offset, interval: interval}
    });
    return await result.json();
}

async function processChunk(chunk) {
    let created = 0;
    let updated = 0;

    const results = await Promise.allSettled(
        chunk.map(record => withRetry(() => processRecord(record), appConfig.retry))
    );

    for (const result of results) {
        if (result.status === 'fulfilled') {
            if (result.value.action === 'created') created++;
            if (result.value.action === 'updated') updated++;
        } else {
            console.error('Record processing error:', result.reason);
        }
    }

    return {created, updated};
}

async function processRecord(record) {
    try {
        const cachedNetHuntUserId = await redisClient.get(record.FundistUserID);
        if (cachedNetHuntUserId) {
            try {
                await updateNetHuntRecord(cachedNetHuntUserId, record);
                return {action: 'updated'};
            } catch (err) {
                if (!isRecordGone(err)) throw err;
                console.warn(`Stale cache for ${record.FundistUserID}: NetHunt record ${cachedNetHuntUserId} is gone, re-resolving`);
                await redisClient.del(record.FundistUserID);
            }
        }

        const [existing] = await searchNetHuntRecord(record.FundistUserID);
        if (existing) {
            await updateNetHuntRecord(existing.id, record);
            await cacheRecordId(record.FundistUserID, existing.id);
            return {action: 'updated'};
        }

        const {recordId} = await createNetHuntRecord(record);
        await cacheRecordId(record.FundistUserID, recordId);
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

async function cacheRecordId(fundistUserId, recordId) {
    if (!recordId) {
        console.warn(`No record id to cache for ${fundistUserId}`);
        return;
    }
    await redisClient.set(fundistUserId, recordId, {EX: appConfig.cacheTtlSeconds});
}

// NetHunt API Helpers
async function searchNetHuntRecord(userId) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);

    try {
        const response = await axios.get(
            `${netHuntBaseUrl}/searches/find-record/${process.env.NETHUNT_FOLDER_ID}`,
            {
                params: {query: `FundistUserID=${userId}`},
                auth: {
                    username: process.env.NETHUNT_USER,
                    password: process.env.NETHUNT_API_KEY
                },
                signal: controller.signal
            }
        );
        return response.data;
    } finally {
        clearTimeout(timeout);
    }
}

async function createNetHuntRecord(record) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);

    try {
        const response = await axios.post(
            `${netHuntBaseUrl}/actions/create-record/${process.env.NETHUNT_FOLDER_ID}`,
            {
                fields: mapRecordFields(record),
                timeZone: "Europe/Warsaw"
            },
            {
                auth: {
                    username: process.env.NETHUNT_USER,
                    password: process.env.NETHUNT_API_KEY
                },
                signal: controller.signal
            }
        );
        return response.data;
    } finally {
        clearTimeout(timeout);
    }
}

async function updateNetHuntRecord(recordId, data) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);

    try {
        const response = await axios.post(
            `${netHuntBaseUrl}/actions/update-record/${recordId}`,
            {
                fieldActions: mapRecordFieldsForUpdate(data)
            },
            {
                auth: {
                    username: process.env.NETHUNT_USER,
                    password: process.env.NETHUNT_API_KEY
                },
                signal: controller.signal
            }
        );
        return response.data;
    } finally {
        clearTimeout(timeout);
    }
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

function mapRecordFieldsForUpdate(record) {
    const fields = mapRecordFields(record);
    return Object.fromEntries(
        Object.entries(fields).map(([key, value]) => [
            key,
            {overwrite: true, add: value}
        ])
    );
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
            if (++attempts > config.retries) throw err;
            const delay = Math.min(
                config.minTimeout * Math.pow(config.factor, attempts - 1),
                config.maxTimeout
            );
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
}

function logProgress(total, created, updated) {
    const line = `Progress: ${total} | Created: ${created} | Updated: ${updated}`;
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
             LEFT JOIN (SELECT UserID, sum(Deposit) / 100 AS TotalDeposit, sum(Withdraw) / 100 AS TotalWithdraw FROM Turnovers t GROUP BY UserID) t ON uh.UserID = t.UserID
    WHERE uh.LastUpdated > now() - INTERVAL {interval: UInt32} MINUTE
    GROUP BY uh.UserID
    ORDER BY uh.UserID DESC
    LIMIT {batchSize: UInt32} OFFSET {offset: UInt32}
`;
// @formatter:on