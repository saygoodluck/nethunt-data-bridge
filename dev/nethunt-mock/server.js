/*
 * Stand-in for the NetHunt v2 REST API, so the whole sync can run with no
 * credentials and no risk of writing into a real CRM.
 *
 * Implements what index.js calls:
 *   GET    /api/v2/folders
 *   GET    /api/v2/folders/:folderId                       - schema with types
 *   POST   /api/v2/folders/:folderId/records               - 201 + record
 *   GET    /api/v2/folders/:folderId/records               - oldest first
 *   POST   /api/v2/folders/:folderId/records/filter        - equality, _exists, sort
 *   PATCH  /api/v2/folders/:folderId/records/:recordId     - partial, 404 when gone
 *
 * Plus a test surface the real API does not have:
 *   GET    /__admin/state
 *   GET    /__admin/record?folder=&field=&value=  - lookup with names, not ids
 *   POST   /__admin/records/:recordId/fields      - a manager editing by hand
 *   DELETE /__admin/records/:recordId             - a record deleted in the CRM
 *   POST   /__admin/reset
 * and a Telegram Bot API stub, so notifications can be exercised too.
 */

import http from 'node:http';

const PORT = Number(process.env.PORT) || 3010;

// Field types mirror the real tenant.
const TEXT = ['singleLineText', 'STRING'];
const NUM = ['number', 'NUMBER'];

const folderDefs = {
    users: {
        name: 'Players',
        fields: [
            ['Name', ...TEXT], ['FundistUserID', ...NUM], ['Login', ...TEXT],
            ['FirstName', ...TEXT], ['LastName', ...TEXT], ['Email', ...TEXT],
            ['PhoneNumber', ...TEXT], ['PhoneVerified', ...TEXT],
            ['DateOfBirth', ...TEXT], ['Gender', ...TEXT], ['Language', ...TEXT],
            ['City', ...TEXT], ['Timezone', ...TEXT], ['Country', ...TEXT],
            ['LastCreditDate', ...TEXT], ['RegistrationDate', ...TEXT],
            ['LastLoginDate', ...TEXT], ['PEP', ...TEXT], ['AccountStatus', ...TEXT],
            ['TotalDeposit', ...NUM], ['TotalWithdraw', ...NUM],
            // owned by the team; the sync must never touch it
            ['Comment', ...TEXT]
        ]
    },
    utils: {
        name: 'Log Data',
        fields: [
            ['Name', ...TEXT], ['finishedAt', ...TEXT], ['totalSynced', ...NUM],
            ['duration', ...NUM], ['createdRecords', ...NUM],
            ['updatedRecords', ...NUM], ['errorMessage', ...TEXT]
        ]
    }
};

// field ids are per folder, assigned once, stable for the process lifetime
const folders = Object.fromEntries(Object.entries(folderDefs).map(([id, def]) => [
    id,
    {
        id,
        name: def.name,
        fields: def.fields.map(([name, type, valueType], i) => ({
            id: String(i + 1), name, type, valueType, multiValue: false
        }))
    }
]));

const fieldByName = (folderId, name) =>
    folders[folderId]?.fields.find(f => f.name === name);

// recordId -> {folderId, fields (keyed by field id, plus `name`), createdAt, updatedAt}
const records = new Map();
let nextId = 1;

// counts every v2 call, so tests can show what a change costs in requests
let apiCalls = 0;

const sentMessages = [];
let pendingUpdates = [];
let updateId = 1;

const json = (res, status, body) => {
    res.writeHead(status, {'Content-Type': 'application/json'});
    res.end(JSON.stringify(body));
};

const readBody = (req) => new Promise((resolve, reject) => {
    let raw = '';
    req.on('data', c => raw += c);
    req.on('end', () => {
        if (!raw) return resolve({});
        try { resolve(JSON.parse(raw)); } catch (err) { reject(err); }
    });
    req.on('error', reject);
});

const shape = (id, r) => ({
    id,
    folderId: r.folderId,
    createdAt: r.createdAt,
    createdISO: new Date(r.createdAt).toISOString(),
    updatedAt: r.updatedAt,
    updatedISO: new Date(r.updatedAt).toISOString(),
    fields: r.fields
});

// what index.js actually sends: bare equality, {_exists: true}, and an _or of
// equalities used to look a whole page of users up in one request
const matches = (record, filter) => Object.entries(filter || {}).every(([key, cond]) => {
    if (key === '_or') return cond.some(sub => matches(record, sub));
    if (key === '_and') return cond.every(sub => matches(record, sub));

    const value = record.fields[key];
    if (cond && typeof cond === 'object') {
        if ('_exists' in cond) return value !== undefined && value !== '';
        if ('_eq' in cond) return String(value) === String(cond._eq);
        return true;
    }
    return String(value) === String(cond);
});

const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const path = url.pathname;
    const method = req.method;

    try {
        // --- admin -----------------------------------------------------------
        if (path === '/__admin/calls' && method === 'GET') {
            const value = apiCalls;
            if (url.searchParams.get('reset') === '1') apiCalls = 0;
            return json(res, 200, {apiCalls: value});
        }

        if (path === '/__admin/state' && method === 'GET') {
            return json(res, 200, {
                count: records.size,
                records: [...records.entries()].map(([id, r]) => {
                    const idField = fieldByName(r.folderId, 'FundistUserID');
                    return {id, folderId: r.folderId,
                            FundistUserID: idField ? r.fields[idField.id] : undefined};
                })
            });
        }

        // lookup by field name, so tests do not have to know field ids
        if (path === '/__admin/record' && method === 'GET') {
            const folderId = url.searchParams.get('folder');
            const field = fieldByName(folderId, url.searchParams.get('field'));
            const wanted = url.searchParams.get('value');
            if (!field) return json(res, 404, {error: 'Unknown folder or field'});

            const hit = [...records.entries()].find(([, r]) =>
                r.folderId === folderId && String(r.fields[field.id]) === String(wanted));
            if (!hit) return json(res, 404, {error: 'Not found'});

            const named = Object.fromEntries(folders[folderId].fields
                .filter(f => hit[1].fields[f.id] !== undefined)
                .map(f => [f.name, hit[1].fields[f.id]]));
            return json(res, 200, {id: hit[0], name: hit[1].fields.name, fields: named});
        }

        const adminFields = /^\/__admin\/records\/([^/]+)\/fields$/.exec(path);
        if (adminFields && method === 'POST') {
            const record = records.get(adminFields[1]);
            if (!record) return json(res, 404, {error: 'Record not found'});
            const body = await readBody(req);
            for (const [name, value] of Object.entries(body.fields || {})) {
                const field = fieldByName(record.folderId, name);
                if (field) record.fields[field.id] = value;
            }
            record.updatedAt = Date.now();
            console.log(`[admin] manager edited ${adminFields[1]}: ${Object.keys(body.fields || {}).join(', ')}`);
            return json(res, 200, shape(adminFields[1], record));
        }

        if (path.startsWith('/__admin/records/') && method === 'DELETE') {
            const id = path.split('/').pop();
            const existed = records.delete(id);
            console.log(`[admin] delete ${id}: ${existed ? 'removed' : 'not found'}`);
            return json(res, existed ? 200 : 404, {deleted: existed});
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
                return json(res, 200, {ok: true, result});
            }
            return json(res, 200, {ok: true, result: {}});
        }

        // --- nethunt v2 ------------------------------------------------------
        if (path.startsWith('/api/v2/')) {
            apiCalls++;
            if (!/^Bearer\s+\S+/.test(req.headers.authorization || '')) {
                return json(res, 401, {code: 'UNAUTHORIZED', message: 'missing bearer token'});
            }
        }

        if (path === '/api/v2/folders' && method === 'GET') {
            return json(res, 200, {folders: Object.values(folders).map(f => ({id: f.id, name: f.name}))});
        }

        const folderGet = /^\/api\/v2\/folders\/([^/]+)$/.exec(path);
        if (folderGet && method === 'GET') {
            const folder = folders[folderGet[1]];
            if (!folder) return json(res, 404, {code: 'FOLDER_NOT_FOUND'});
            return json(res, 200, folder);
        }

        const filter = /^\/api\/v2\/folders\/([^/]+)\/records\/filter$/.exec(path);
        if (filter && method === 'POST') {
            const folderId = filter[1];
            if (!folders[folderId]) return json(res, 404, {code: 'FOLDER_NOT_FOUND'});
            const body = await readBody(req);
            const limit = Number(url.searchParams.get('limit')) || 100;

            let hits = [...records.entries()]
                .filter(([, r]) => r.folderId === folderId)
                .filter(([, r]) => matches(r, body.filter));

            for (const rule of (body.sort || []).slice().reverse()) {
                const [key, dir] = Object.entries(rule)[0];
                const of = ([, r]) => key === 'created' ? r.createdAt
                    : key === 'updated' ? r.updatedAt
                    : r.fields[key];
                hits.sort((a, b) => (of(a) > of(b) ? 1 : of(a) < of(b) ? -1 : 0) * dir);
            }

            return json(res, 200, {
                total: hits.length,
                records: hits.slice(0, limit).map(([id, r]) => shape(id, r))
            });
        }

        const recordsPath = /^\/api\/v2\/folders\/([^/]+)\/records$/.exec(path);
        if (recordsPath && method === 'POST') {
            const folderId = recordsPath[1];
            if (!folders[folderId]) return json(res, 404, {code: 'FOLDER_NOT_FOUND'});
            const body = await readBody(req);

            // The real API answers 502 with a Java stack trace for an empty
            // string on a text field, on any field, so the contour has to be
            // just as strict or the defect returns unnoticed.
            const empty = Object.entries(body.fields || {}).find(([, v]) => v === '');
            if (empty) {
                return json(res, 502, {
                    error: 'UPSTREAM_ERROR',
                    message: `Wrong value in column [${empty[0]}]`
                });
            }

            const now = Date.now();
            const id = `rec_${nextId++}`;
            records.set(id, {folderId, fields: {...body.fields}, createdAt: now, updatedAt: now});
            // the real API answers 201 here, not 200
            return json(res, 201, shape(id, records.get(id)));
        }

        if (recordsPath && method === 'GET') {
            const folderId = recordsPath[1];
            const list = [...records.entries()]
                .filter(([, r]) => r.folderId === folderId)
                // oldest first, as documented
                .sort((a, b) => a[1].updatedAt - b[1].updatedAt)
                .map(([id, r]) => shape(id, r));
            return json(res, 200, {records: list});
        }

        const one = /^\/api\/v2\/folders\/([^/]+)\/records\/([^/]+)$/.exec(path);
        if (one && (method === 'PATCH' || method === 'GET')) {
            const record = records.get(one[2]);
            // the case the Redis invalidation exists for
            if (!record || record.folderId !== one[1]) {
                console.log(`[mock] RECORD_NOT_FOUND ${one[2]}`);
                return json(res, 404, {code: 'RECORD_NOT_FOUND'});
            }
            if (method === 'GET') return json(res, 200, shape(one[2], record));

            const body = await readBody(req);
            const blank = Object.entries(body.fields || {}).find(([, v]) => v === '');
            if (blank) {
                return json(res, 502, {
                    error: 'UPSTREAM_ERROR',
                    message: `Wrong value in column [${blank[0]}]`
                });
            }
            // partial by nature: fields not named here keep their value
            Object.assign(record.fields, body.fields || {});
            record.updatedAt = Date.now();
            return json(res, 200, shape(one[2], record));
        }

        return json(res, 404, {error: `No mock route for ${method} ${path}`});
    } catch (err) {
        console.error('[mock] error:', err);
        return json(res, 500, {error: err.message});
    }
});

server.listen(PORT, () => console.log(`NetHunt v2 mock listening on ${PORT}`));
