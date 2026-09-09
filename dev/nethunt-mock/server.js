/*
 * Stand-in for the NetHunt zapier API, so the whole sync can run with no
 * credentials and no risk of writing into a real CRM folder.
 *
 * Implements only what index.js calls:
 *   GET  /api/v1/zapier/triggers/new-record/:folderId   - last sync metrics
 *   GET  /api/v1/zapier/searches/find-record/:folderId  - lookup by FundistUserID
 *   POST /api/v1/zapier/actions/create-record/:folderId
 *   POST /api/v1/zapier/actions/update-record/:recordId - 404 once a record is gone
 *
 * Plus a small admin surface for testing, which the real API does not have:
 *   GET    /__admin/state
 *   DELETE /__admin/records/:recordId   - simulate a record deleted in the CRM
 *   POST   /__admin/records/:recordId/fields - simulate a manager editing a field
 *   POST   /__admin/reset
 */

import http from 'node:http';

const PORT = Number(process.env.PORT) || 3010;

// recordId -> {folderId, fields}
const records = new Map();
let nextId = 1;

// Folder registry, shaped like the real tenant should be. Field names here are
// the contract: index.js builds its payload from exactly these keys.
const SYNCED_FIELDS = [
    ['FundistUserID', 'NUMBER'], ['Login', 'TEXT'],
    ['FirstName', 'TEXT'], ['LastName', 'TEXT'],
    ['Email', 'EMAIL'], ['PhoneNumber', 'PHONE'],
    ['PhoneVerified', 'TEXT'], ['DateOfBirth', 'TEXT'],
    ['Gender', 'TEXT'], ['Language', 'TEXT'],
    ['Country', 'TEXT'], ['City', 'TEXT'], ['Timezone', 'TEXT'],
    ['LastCreditDate', 'TEXT'], ['RegistrationDate', 'TEXT'],
    ['LastLoginDate', 'TEXT'], ['PEP', 'TEXT'],
    ['AccountStatus', 'TEXT'],
    ['TotalDeposit', 'NUMBER'], ['TotalWithdraw', 'NUMBER']
];

const folders = {
    users: {
        name: 'Players',
        // Comment is owned by managers; the sync never sends it
        fields: [...SYNCED_FIELDS, ['Comment', 'TEXT']]
    },
    utils: {
        name: 'Sync metrics',
        fields: [
            ['finishedAt', 'TEXT'], ['totalSynced', 'NUMBER'],
            ['duration', 'NUMBER'], ['createdRecords', 'NUMBER'],
            ['updatedRecords', 'NUMBER'], ['errorMessage', 'TEXT']
        ]
    }
};

const folderList = () => Object.entries(folders).map(([id, f]) => ({id, name: f.name}));

const json = (res, status, body) => {
    const payload = JSON.stringify(body);
    res.writeHead(status, {'Content-Type': 'application/json'});
    res.end(payload);
};

const readBody = (req) => new Promise((resolve, reject) => {
    let raw = '';
    req.on('data', chunk => raw += chunk);
    req.on('end', () => {
        if (!raw) return resolve({});
        try {
            resolve(JSON.parse(raw));
        } catch (err) {
            reject(err);
        }
    });
    req.on('error', reject);
});

// "FundistUserID=123" or "Name:Doe" -> {field, value}. Both separators are
// accepted because which one the real API wants is still an open question.
const parseQuery = (query) => {
    const match = /^([A-Za-z0-9_]+)[=:]([\s\S]*)$/.exec(query || '');
    return match ? {field: match[1], value: match[2]} : null;
};

const applyFieldActions = (fields, fieldActions) => {
    for (const [key, action] of Object.entries(fieldActions || {})) {
        fields[key] = action.add;
    }
    return fields;
};

// --- Telegram Bot API stand-in ---------------------------------------------
// Lets the notification logic be exercised without a real bot token.
const sentMessages = [];
let pendingUpdates = [];
let updateId = 1;

const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const path = url.pathname;
    const method = req.method;

    try {
        // --- admin -----------------------------------------------------------
        if (path === '/__admin/state' && method === 'GET') {
            return json(res, 200, {
                count: records.size,
                records: [...records.entries()].map(([id, r]) => ({
                    id, folderId: r.folderId, FundistUserID: r.fields.FundistUserID
                }))
            });
        }

        if (path.startsWith('/__admin/records/') && method === 'DELETE') {
            const id = path.split('/').pop();
            const existed = records.delete(id);
            console.log(`[admin] delete ${id}: ${existed ? 'removed' : 'not found'}`);
            return json(res, existed ? 200 : 404, {deleted: existed});
        }

        // a manager editing a field by hand, bypassing the sync entirely
        const adminFields = /^\/__admin\/records\/([^/]+)\/fields$/.exec(path);
        if (adminFields && method === 'POST') {
            const record = records.get(adminFields[1]);
            if (!record) return json(res, 404, {error: 'Record not found'});
            const body = await readBody(req);
            Object.assign(record.fields, body.fields || {});
            console.log(`[admin] manager edited ${adminFields[1]}: ${Object.keys(body.fields || {}).join(', ')}`);
            return json(res, 200, {fields: record.fields});
        }

        if (path === '/__admin/reset' && method === 'POST') {
            records.clear();
            nextId = 1;
            console.log('[admin] state reset');
            return json(res, 200, {ok: true});
        }

        if (path === '/__admin/telegram/messages' && method === 'GET') {
            return json(res, 200, sentMessages);
        }

        // queue a command as if a user had typed it in the chat
        if (path === '/__admin/telegram/send-command' && method === 'POST') {
            const body = await readBody(req);
            pendingUpdates.push({
                update_id: updateId++,
                message: {
                    text: body.text,
                    chat: {id: Number(body.chatId ?? 12345)},
                    from: {id: Number(body.fromId ?? 999)}
                }
            });
            return json(res, 200, {queued: body.text});
        }

        // --- telegram bot api ------------------------------------------------
        const tg = /^\/bot[^/]+\/(\w+)$/.exec(path);
        if (tg && method === 'POST') {
            const body = await readBody(req);
            if (tg[1] === 'sendMessage') {
                sentMessages.push({chat_id: body.chat_id, text: body.text});
                console.log(`[telegram] ${String(body.text).split('\n')[0]}`);
                return json(res, 200, {ok: true, result: {message_id: sentMessages.length}});
            }
            if (tg[1] === 'getUpdates') {
                const result = pendingUpdates;
                pendingUpdates = [];
                // answer immediately rather than long-polling
                return json(res, 200, {ok: true, result});
            }
            return json(res, 200, {ok: true, result: {}});
        }

        // --- zapier api ------------------------------------------------------
        if (/^\/api\/v1\/zapier\/triggers\/(readable|writable)-folder$/.test(path) && method === 'GET') {
            return json(res, 200, folderList());
        }

        const folderField = /^\/api\/v1\/zapier\/triggers\/folder-field\/(.+)$/.exec(path);
        if (folderField && method === 'GET') {
            const folder = folders[folderField[1]];
            if (!folder) return json(res, 404, {error: 'Folder not found'});
            return json(res, 200, folder.fields.map(([name, type]) => ({name, type})));
        }

        const trigger = /^\/api\/v1\/zapier\/triggers\/new-record\/(.+)$/.exec(path);
        if (trigger && method === 'GET') {
            const folderId = trigger[1];
            // newest first, like the real trigger feed
            const inFolder = [...records.values()]
                .filter(r => r.folderId === folderId)
                .reverse();
            return json(res, 200, inFolder.map(r => ({fields: r.fields})));
        }

        const search = /^\/api\/v1\/zapier\/searches\/find-record\/(.+)$/.exec(path);
        if (search && method === 'GET') {
            const folderId = search[1];
            const parsed = parseQuery(url.searchParams.get('query'));
            if (!parsed) return json(res, 200, []);
            const hits = [...records.entries()]
                .filter(([, r]) => r.folderId === folderId
                    && String(r.fields[parsed.field] ?? '') === String(parsed.value))
                .map(([id, r]) => ({id, fields: r.fields}));
            return json(res, 200, hits);
        }

        const create = /^\/api\/v1\/zapier\/actions\/create-record\/(.+)$/.exec(path);
        if (create && method === 'POST') {
            const folderId = create[1];
            const body = await readBody(req);
            const recordId = `rec_${nextId++}`;
            records.set(recordId, {folderId, fields: {...body.fields}});
            return json(res, 200, {recordId});
        }

        const update = /^\/api\/v1\/zapier\/actions\/update-record\/(.+)$/.exec(path);
        if (update && method === 'POST') {
            const recordId = update[1];
            const record = records.get(recordId);
            // the case the Redis invalidation exists for
            if (!record) {
                console.log(`[mock] 404 update of missing record ${recordId}`);
                return json(res, 404, {error: 'Record not found'});
            }
            const body = await readBody(req);
            applyFieldActions(record.fields, body.fieldActions);
            return json(res, 200, {recordId});
        }

        return json(res, 404, {error: `No mock route for ${method} ${path}`});
    } catch (err) {
        console.error('[mock] error:', err);
        return json(res, 500, {error: err.message});
    }
});

server.listen(PORT, () => console.log(`NetHunt mock listening on ${PORT}`));
