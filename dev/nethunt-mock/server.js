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
 *   POST   /__admin/reset
 */

import http from 'node:http';

const PORT = Number(process.env.PORT) || 3010;

// recordId -> {folderId, fields}
const records = new Map();
let nextId = 1;

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

// "FundistUserID=123" -> "123"
const parseQuery = (query) => {
    const match = /^FundistUserID=(.+)$/.exec(query || '');
    return match ? match[1] : null;
};

const applyFieldActions = (fields, fieldActions) => {
    for (const [key, action] of Object.entries(fieldActions || {})) {
        fields[key] = action.add;
    }
    return fields;
};

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

        if (path === '/__admin/reset' && method === 'POST') {
            records.clear();
            nextId = 1;
            console.log('[admin] state reset');
            return json(res, 200, {ok: true});
        }

        // --- zapier api ------------------------------------------------------
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
            const userId = parseQuery(url.searchParams.get('query'));
            const hits = [...records.entries()]
                .filter(([, r]) => r.folderId === folderId
                    && String(r.fields.FundistUserID) === String(userId))
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
