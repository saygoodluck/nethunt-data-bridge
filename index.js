import {createClient} from '@clickhouse/client';
import 'dotenv/config';
import fs from 'fs';
import {Client} from 'ssh2';
import net from 'net';
import express from 'express';
import cron from 'node-cron';
import axios from 'axios';
import {performance} from 'node:perf_hooks';

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
const RETRY_CONFIG = {
    retries: Number(process.env.RETRY_RETRIES) || 3,
    factor: Number(process.env.RETRY_FACTOR) || 2,
    minTimeout: Number(process.env.RETRY_MIN_TIMEOUT) || 1000,
    maxTimeout: Number(process.env.RETRY_MAX_TIMEOUT) || 10000
};

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
    console.log(`${new Date()} UTC+0 | Running scheduled task...`);
    await syncRecords().catch(console.error);
});

// @formatter:off
const clickHouseQuery = `
    SELECT uh.UserID as FundistUserID,
        argMax(uh.Login, uh.LastLoginDate) as Login,
        argMax(uh.Name, uh.LastLoginDate) as FirstName,
        argMax(uh.LastName, uh.LastLoginDate) as LastName,
        argMax(uh.MiddleName, uh.LastLoginDate) as MiddleName,
        argMax(uh.Email, uh.LastLoginDate) as Email,
        argMax(uh.Phone, uh.LastLoginDate) as PhoneNumber,
        if(argMax(uh.PhoneVerified, uh.LastLoginDate) = 1, 'Verified', 'Unverified') as PhoneVerified,
        argMax(uh.AlternativePhone, uh.LastLoginDate) as AlternativePhone,
        argMax(uh.Gender, uh.LastLoginDate) AS Gender,
        argMax(uh.Language, uh.LastLoginDate) AS Language,
        argMax(c.Name, uh.LastLoginDate) AS Country,
        argMax(uh.City, uh.LastLoginDate) AS City,
        argMax(uh.Timezone, uh.LastLoginDate) AS Timezone,
        argMax(uh.Address, uh.LastLoginDate) AS Address,
        argMax(uh.PostalCode, uh.LastLoginDate) AS PostalCode,
        argMax(uh.PlaceOfBirth, uh.LastLoginDate) AS PlaceOfBirth,
        argMax(uh.CityOfRegistration, uh.LastLoginDate) AS CityOfRegistration,
        argMax(uh.AddressOfRegistration, uh.LastLoginDate) AS AddressOfRegistration,
        argMax(uh.LastCreditDate, uh.LastLoginDate) AS LastCreditDate,
        argMax(uh.RegistrationDate, uh.LastLoginDate) AS RegistrationDate,
        max(uh.LastLoginDate) AS LastLoginDate,
        if(argMax(uh.PEP, uh.LastLoginDate) = 1, 'PEP', '') AS PEP,
        if(argMax(uh.Status, uh.LastLoginDate) = 1, 'Active', 'Inactive') AS AccountStatus,
        any(p.TotalDeposit) as TotalDeposit
    FROM UserHistory uh
    JOIN CountriesNew c ON c.ID = uh.CountryID
    LEFT JOIN (SELECT IDUser, SUM(AmountNotRounded) AS TotalDeposit FROM Payments p WHERE Status = 100 GROUP BY IDUser) p ON uh.UserID = p.IDUser
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
            query_params: {batchSize: BATCH_SIZE, offset}
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
        const [existing] = await searchNetHuntRecord(record.FundistUserID);

        if (!existing) {
            await createNetHuntRecord(record);
            return {action: 'created'};
        }

        await updateNetHuntRecord(existing.id, record);
        return {action: 'updated'};
    } catch (err) {
        console.error(`Error processing record ${record.FundistUserID}:`, err.message);
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
        MiddleName: record.MiddleName,
        Email: record.Email,
        PhoneNumber: record.PhoneNumber,
        PhoneVerified: record.PhoneVerified,
        AlternativePhone: record.AlternativePhone,
        DateOfBirth: record.DateOfBirth,
        Gender: record.Gender,
        Language: record.Language,
        Country: record.Country,
        City: record.City,
        Timezone: record.Timezone,
        Address: record.Address,
        PostalCode: record.PostalCode,
        PlaceOfBirth: record.PlaceOfBirth,
        CityOfRegistration: record.CityOfRegistration,
        AddressOfRegistration: record.AddressOfRegistration,
        LastCreditDate: record.LastCreditDate,
        RegistrationDate: record.RegistrationDate,
        LastLoginDate: record.LastLoginDate,
        PEP: record.PEP,
        AccountStatus: record.AccountStatus,
        TotalDeposit: record.TotalDeposit
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
    process.stdout.clearLine();
    process.stdout.cursorTo(0);
    process.stdout.write(
        `Progress: ${total} | Created: ${created} | Updated: ${updated}`
    );
}