/*
 * Telegram notifications and control for the sync service.
 *
 * Uses the Bot API over plain HTTP through axios, which the project already
 * depends on -- a bot framework would be more code to carry than the four
 * calls actually used here.
 *
 * Everything is optional: with no TELEGRAM_BOT_TOKEN the module turns into a
 * set of no-ops and the service behaves exactly as it did before.
 */

import axios from 'axios';

// Read at call time, not at import time: ESM evaluates this module before
// index.js runs `import 'dotenv/config'`, so at import time .env is not loaded yet.
const readConfig = () => ({
    token: process.env.TELEGRAM_BOT_TOKEN,
    chatId: process.env.TELEGRAM_CHAT_ID,
    apiBase: process.env.TELEGRAM_API_BASE || 'https://api.telegram.org',
    // extra user ids allowed to run commands, beyond the configured chat
    admins: (process.env.TELEGRAM_ADMINS || '')
        .split(',')
        .map(id => id.trim())
        .filter(Boolean),
    notifyErrors: process.env.NOTIFY_ERRORS !== '0',
    notifyStartup: process.env.NOTIFY_STARTUP !== '0',
    // off by default: 48 syncs a day would train you to ignore the chat
    notifyEachSync: process.env.NOTIFY_EACH_SYNC === '1',
    dailyReportAt: process.env.DAILY_REPORT_AT || ''
});

const STATS_TTL_SECONDS = 3 * 24 * 60 * 60;

const escapeHtml = (value) => String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');

const formatDuration = (ms) => `${(ms / 1000).toFixed(1)}s`;

const statsKey = (date) => `stats:${date.toISOString().slice(0, 10)}`;

export function createTelegramBot({redisClient, runSync, getStatus}) {
    const config = readConfig();
    const telegramEnabled = Boolean(config.token && config.chatId);

    // failures are reported on transition, not on every occurrence
    let failing = false;
    let lastSync = null;
    let polling = false;
    let pollController = null;

    const api = (method, payload, options = {}) => axios.post(
        `${config.apiBase}/bot${config.token}/${method}`,
        payload,
        {timeout: options.timeout ?? 15000, signal: options.signal}
    );

    async function send(text) {
        if (!telegramEnabled) return;
        try {
            await api('sendMessage', {
                chat_id: config.chatId,
                text,
                parse_mode: 'HTML',
                disable_web_page_preview: true
            });
        } catch (err) {
            // the chat being unreachable must never take the sync down with it
            console.error('Telegram send failed:', err.message);
        }
    }

    async function recordStats(syncData) {
        if (!redisClient) return;
        const key = statsKey(new Date());
        try {
            await redisClient.hIncrBy(key, 'syncs', 1);
            await redisClient.hIncrBy(key, 'totalSynced', syncData.totalSynced || 0);
            await redisClient.hIncrBy(key, 'created', syncData.createdRecords || 0);
            await redisClient.hIncrBy(key, 'updated', syncData.updatedRecords || 0);
            await redisClient.hIncrBy(key, 'failures', syncData.errorMessage ? 1 : 0);
            await redisClient.hIncrByFloat(key, 'durationMs', syncData.duration || 0);
            await redisClient.expire(key, STATS_TTL_SECONDS);
        } catch (err) {
            console.error('Failed to record sync stats:', err.message);
        }
    }

    function describe(syncData) {
        return [
            `Synced: <b>${syncData.totalSynced}</b>`,
            `Created: <b>${syncData.createdRecords}</b>`,
            `Updated: <b>${syncData.updatedRecords}</b>`,
            `Duration: ${formatDuration(syncData.duration)}`
        ].join('\n');
    }

    async function notifySyncResult(syncData) {
        lastSync = syncData;
        await recordStats(syncData);
        if (!telegramEnabled) return;

        if (syncData.errorMessage) {
            // already known to be broken: stay quiet until it recovers
            if (failing || !config.notifyErrors) {
                failing = true;
                return;
            }
            failing = true;
            await send(
                `🔴 <b>Sync failed</b>\n\n` +
                `<code>${escapeHtml(syncData.errorMessage)}</code>\n\n` +
                `Further failures stay silent until it recovers.`
            );
            return;
        }

        if (failing) {
            failing = false;
            await send(`🟢 <b>Sync recovered</b>\n\n${describe(syncData)}`);
            return;
        }

        if (config.notifyEachSync) {
            await send(`✅ <b>Sync completed</b>\n\n${describe(syncData)}`);
        }
    }

    async function sendDailyReport() {
        if (!telegramEnabled || !redisClient) return;

        const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000);
        const key = statsKey(yesterday);
        const stats = await redisClient.hGetAll(key);

        if (!stats || !stats.syncs) {
            await send(`📊 <b>Daily report</b>\n\nNo syncs ran yesterday.`);
            return;
        }

        const syncs = Number(stats.syncs);
        const failures = Number(stats.failures || 0);
        const avgMs = Number(stats.durationMs || 0) / syncs;

        await send(
            `📊 <b>Daily report</b> — ${key.replace('stats:', '')}\n\n` +
            `Syncs: <b>${syncs}</b>` + (failures ? ` (${failures} failed)` : ' (all ok)') + `\n` +
            `Records touched: <b>${stats.totalSynced || 0}</b>\n` +
            `Created: <b>${stats.created || 0}</b>\n` +
            `Updated: <b>${stats.updated || 0}</b>\n` +
            `Average duration: ${formatDuration(avgMs)}`
        );
    }

    async function notifyStartup() {
        if (!telegramEnabled || !config.notifyStartup) return;
        await send(`🚀 <b>Service started</b>\n\nSend /help for commands.`);
    }

    // --- commands -----------------------------------------------------------

    const isAuthorised = (message) => {
        if (String(message.chat?.id) === String(config.chatId)) return true;
        return config.admins.includes(String(message.from?.id));
    };

    async function handleCommand(message) {
        const text = (message.text || '').trim();
        // "/status@my_bot" is what group chats deliver
        const command = text.split(/\s+/)[0].split('@')[0].toLowerCase();

        switch (command) {
            case '/help':
                return send(
                    `<b>Commands</b>\n\n` +
                    `/status — connection and last sync\n` +
                    `/last — details of the last sync\n` +
                    `/report — yesterday's summary\n` +
                    `/sync — run a sync now`
                );

            case '/status': {
                const status = await getStatus();
                return send(
                    `<b>Status</b>\n\n` +
                    `Uptime: ${status.uptime}\n` +
                    `ClickHouse: ${status.clickhouse}\n` +
                    `Redis: ${status.redis}\n` +
                    `Redis keys: ${status.cachedIds} (ids + daily stats)\n` +
                    `Last sync: ${lastSync
                        ? (lastSync.errorMessage ? '🔴 failed' : `🟢 ${lastSync.totalSynced} records`)
                        : 'none yet'}`
                );
            }

            case '/last':
                return send(lastSync
                    ? `<b>Last sync</b>\n\n${describe(lastSync)}` +
                      (lastSync.errorMessage
                          ? `\n\n🔴 <code>${escapeHtml(lastSync.errorMessage)}</code>`
                          : '')
                    : 'No sync has run since the service started.');

            case '/report':
                return sendDailyReport();

            case '/sync': {
                await send('⏳ Sync started…');
                try {
                    const result = await runSync();
                    return send(`✅ <b>Sync completed</b>\n\n${describe(result)}`);
                } catch (err) {
                    // notifySyncResult already reported it; keep the reply short
                    return send(`🔴 <b>Sync failed</b>\n\n<code>${escapeHtml(err.message)}</code>`);
                }
            }

            default:
                return;
        }
    }

    async function poll() {
        let offset = 0;
        while (polling) {
            try {
                pollController = new AbortController();
                const response = await api('getUpdates', {
                    offset,
                    timeout: 30,
                    allowed_updates: ['message']
                }, {timeout: 40000, signal: pollController.signal});

                for (const update of response.data.result || []) {
                    offset = update.update_id + 1;
                    const message = update.message;
                    if (!message?.text?.startsWith('/')) continue;
                    if (!isAuthorised(message)) {
                        console.warn(`Ignoring command from unauthorised chat ${message.chat?.id}`);
                        continue;
                    }
                    await handleCommand(message).catch(err =>
                        console.error('Command failed:', err.message));
                }
            } catch (err) {
                if (!polling) return;
                console.error('Telegram polling error:', err.message);
                // back off so a broken token does not spin the loop
                await new Promise(resolve => setTimeout(resolve, 5000));
            }
        }
    }

    function startPolling() {
        if (!telegramEnabled || polling) return;
        polling = true;
        poll().catch(err => console.error('Telegram polling stopped:', err.message));
        console.log('Telegram bot polling for commands');
    }

    function stop() {
        polling = false;
        pollController?.abort();
    }

    return {
        enabled: telegramEnabled,
        notifySyncResult,
        sendDailyReport,
        notifyStartup,
        startPolling,
        stop,
        dailyReportAt: config.dailyReportAt
    };
}
