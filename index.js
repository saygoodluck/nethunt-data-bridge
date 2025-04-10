import {createClient} from '@clickhouse/client';
import 'dotenv/config';
import fs from 'fs';
import {Client} from 'ssh2';
import net from 'net';
import express from 'express';
import cron from 'node-cron';
import axios from 'axios';
import {performance} from 'node:perf_hooks';
import {createClient as createRedisClient} from 'redis';

const app = express();
const sshClient = new Client();
const PORT = process.env.PORT || 3000;

// Validate environment variables
const requiredEnvVars = [
    'CLICKHOUSE_HOST', 'CLICKHOUSE_PORT', 'CLICKHOUSE_USER',
    'CLICKHOUSE_PASSWORD', 'CLICKHOUSE_DATABASE', 'SSH_HOST',
    'SSH_PORT', 'SSH_USER', 'NETHUNT_FOLDER_ID', 'NETHUNT_USER',
    'NETHUNT_API_KEY'
];

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

const tunnelConfig = {
    host: process.env.SSH_HOST,
    port: Number(process.env.SSH_PORT),
    username: process.env.SSH_USER,
    privateKey: fs.readFileSync(process.env.SSH_KEY_PATH || './id_ed25519')
};

const forwardConfig = {
    srcHost: '127.0.0.1',
    srcPort: 3306,
    dstHost: dbServer.host,
    dstPort: dbServer.port
};

const MAX_CONCURRENT_REQUESTS = Number(process.env.MAX_CONCURRENT_REQUESTS) || 5;
const BATCH_SIZE = Number(process.env.BATCH_SIZE) || 100;
const SYNC_INTERVAL = Number(process.env.SYNC_INTERVAL) || 1;
const RETRY_CONFIG = {
    retries: Number(process.env.RETRY_RETRIES) || 3,
    factor: Number(process.env.RETRY_FACTOR) || 2,
    minTimeout: Number(process.env.RETRY_MIN_TIMEOUT) || 1000,
    maxTimeout: Number(process.env.RETRY_MAX_TIMEOUT) || 10000
};

const redisClient = await createRedisClient({
    url: "redis://localhost:6379",
    database: 0
})
    .on('error', (err) => console.log('Redis Client Error', err))
    .on('connect', () => console.log('Connected to Redis'))
    .connect();

let clickhouse;
let tunnelServer = null;

const setupSSHTunnel = () => new Promise((resolve, reject) => {
    sshClient.on('ready', () => {
        if (tunnelServer) {
            tunnelServer.close();
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

        tunnelServer.listen(forwardConfig.srcPort, forwardConfig.srcHost, (err) => {
            if (err) return reject(err);
            console.log(`SSH tunnel ready on ${forwardConfig.srcHost}:${forwardConfig.srcPort}`);

            clickhouse = createClient({
                url: `http://${forwardConfig.srcHost}:${forwardConfig.srcPort}`,
                username: dbServer.user,
                password: dbServer.password,
                database: dbServer.database
            });

            // Test connection
            clickhouse.query({query: 'SELECT 1'})
                .then(() => resolve())
                .catch(reject);
        });
    }).on('error', reject).connect(tunnelConfig);
});

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
    res.json({status: 'ok', message: 'Service is operational'});
});

app.get('/sync', authenticate, async (req, res) => {
    try {
        const result = await syncRecords();
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

setupSSHTunnel()
    .then(() => {
        app.listen(PORT, () => {
            console.log(`Server running on http://localhost:${PORT}`);
        });
    })
    .catch(err => {
        console.error('Initialization failed:', err);
        process.exit(1);
    });

sshClient.on('close', () => {
    console.log('SSH connection closed. Reconnecting...');
    setupSSHTunnel().catch(console.error);
});

process.on('SIGINT', async () => {
    console.log('\nGracefully shutting down...');
    try {
        await clickhouse?.close();
        sshClient.end();
        tunnelServer?.close();
        console.log('Connections closed');
        process.exit(0);
    } catch (err) {
        console.error('Shutdown error:', err);
        process.exit(1);
    }
});

// Scheduled sync
cron.schedule('0 * * * *', async () => {
    console.log(`${new Date()} | Running scheduled task...`);
    await syncRecords().catch(console.error);
});

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
    WHERE uh.LastUpdated > now() - INTERVAL {interval: UInt32} HOUR
    GROUP BY uh.UserID
    ORDER BY uh.UserID DESC
    LIMIT {batchSize: UInt32} OFFSET {offset: UInt32}
`;
// @formatter:on

async function syncRecords() {
    const start = performance.now();
    let offset = 0;
    let totalSynced = 0;
    let createdRecords = 0;
    let updatedRecords = 0;

    try {
        console.log('Starting sync...');

        while (true) {
            const records = await fetchRecordsBatch(offset);
            if (records.length === 0) break;

            const chunks = chunkArray(records, MAX_CONCURRENT_REQUESTS);

            for (const chunk of chunks) {
                const results = await processChunk(chunk);
                createdRecords += results.created;
                updatedRecords += results.updated;
                totalSynced += chunk.length;

                logProgress(totalSynced, createdRecords, updatedRecords);
            }

            offset += BATCH_SIZE;
        }

        const duration = performance.now() - start;
        console.log(`\nSync completed.`);
        console.log(`Duration: ${(duration / 1000).toFixed(2)} seconds`);
        return {totalSynced, createdRecords, updatedRecords, duration};
    } catch (err) {
        console.error('Sync failed:', err);
        throw err;
    }
}

async function fetchRecordsBatch(offset) {
    try {
        const result = await clickhouse.query({
            query: clickHouseQuery,
            format: 'JSONEachRow',
            query_params: {batchSize: BATCH_SIZE, offset: offset, interval: SYNC_INTERVAL}
        });
        return await result.json();
    } catch (err) {
        console.error('Error fetching records:', err);
        return [];
    }
}

async function processChunk(chunk) {
    let created = 0;
    let updated = 0;

    const results = await Promise.allSettled(
        chunk.map(record => withRetry(() => processRecord(record), RETRY_CONFIG))
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
            await updateNetHuntRecord(cachedNetHuntUserId, record);
            return {action: 'updated'};
        }

        const [existing] = await searchNetHuntRecord(record.FundistUserID);
        if (existing) {
            await updateNetHuntRecord(existing.id, record);
            await redisClient.set(record.FundistUserID, existing.id);
            return {action: 'updated'};
        }

        const {recordId} = await createNetHuntRecord(record);
        await redisClient.set(record.FundistUserID, recordId);
        return {action: 'created'};
    } catch (err) {
        console.error(`Error processing ${record.FundistUserID}:`, {
            message: err.message,
            stack: err.stack
        });
        throw err;
    }
}

// NetHunt API Helpers
async function searchNetHuntRecord(userId) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);

    try {
        const response = await axios.get(
            `https://nethunt.com/api/v1/zapier/searches/find-record/${process.env.NETHUNT_FOLDER_ID}`,
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
            `https://nethunt.com/api/v1/zapier/actions/create-record/${process.env.NETHUNT_FOLDER_ID}`,
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
            `https://nethunt.com/api/v1/zapier/actions/update-record/${recordId}`,
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

// Field Mapping
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

// function logProgress(total, created, updated) {
//     process.stdout.clearLine();
//     process.stdout.cursorTo(0);
//     process.stdout.write(
//         `Progress: ${total} | Created: ${created} | Updated: ${updated}`
//     );
// }

// function logProgress(total, created, updated) {
//     const line = `Progress: ${total} | Created: ${created} | Updated: ${updated}`;
//     process.stdout.write(process.stdout.isTTY ? `\r${line}` : `${line}\n`);
// }