# Sync Service: ClickHouse ↔ NetHunt CRM

Service for synchronizing data between ClickHouse and NetHunt CRM via SSH tunnel.

## Features

- 🔐 Secure ClickHouse connection via SSH tunnel
- 🔄 Batched data synchronization with version control
- ⚡ Parallel request processing (up to 5 concurrent)
- 🔁 Automatic error retries
- ⏰ Scheduled cron synchronization
- 🛡 Graceful shutdown implementation

## Requirements

- Node.js 18+
- npm 9+
- ClickHouse access
- NetHunt CRM API key
- SSH key for server connection

## Installation

```bash
# 1. Clone repository
git clone https://github.com/saygoodluck/nethunt-data-bridge.git
cd nethunt-data-bridge

# 2. Install dependencies
npm install

# 3. Copy example .env file
cp .env.example .env

# 4. Generate ssh key if not exists
ssh-keygen -t ed25519

```

## Usage

```bash
# Start service
npm start

# Check status
curl http://localhost:3000

# Manual sync trigger
curl http://localhost:3000/sync
```

The service will:
1. Automatically connect via SSH tunnel
2. Sync data every 60 minutes
3. Provide status at http://localhost:3000
4. Run sync manually at http://localhost:3000/sync use NetHunt API key with Bearer auth

## Local development

The whole contour runs locally, so no ClickHouse credentials, no bastion access
and no NetHunt API key are needed to work on the sync.

```bash
# 1. Start ClickHouse (seeded), Redis and a NetHunt API mock
npm run dev:up

# 2. Run the service against them (.env.dev, no secrets in it)
npm run dev

# 3. Trigger a sync
curl http://localhost:3001/sync -H "Authorization: Bearer dev-key"

# Stop and wipe
npm run dev:down
```

`SKIP_SSH_TUNNEL=1` makes the service talk to ClickHouse directly instead of
opening a bastion tunnel; `NETHUNT_BASE_URL` points the CRM calls at the mock.
Both default to production behaviour when unset.

The seed anchors `LastUpdated` to `now()`, so data seeded yesterday falls
outside the sync window today and the service correctly reports 0 records.
`npm run dev:reseed` refreshes the timestamps without recreating the contour.

### Seed data

`dev/clickhouse/init/` creates the three tables the sync query reads and fills
them with 250 users x 3 revisions. The shape is deliberate:

- older revisions carry `STALE-*` values, so a broken `argMax(..., RecordTime)`
  shows up immediately in what reaches the CRM
- 180 users fall inside the default 60-minute window, 70 outside it, which
  proves the `LastUpdated` filter is doing something
- 180 matching users against `BATCH_SIZE=100` makes pagination run twice

### NetHunt mock

`dev/nethunt-mock/` implements the four endpoints the service calls, keeps
records in memory, and adds an admin surface the real API does not have:

```bash
# what the CRM currently holds
curl http://localhost:3010/__admin/state

# delete a record behind the service's back, to exercise cache invalidation
curl -X DELETE http://localhost:3010/__admin/records/rec_1

# forget everything
curl -X POST http://localhost:3010/__admin/reset
```

Deleting a record is the interesting one: the next sync finds a stale id in
Redis, gets a 404, drops the key and re-creates the record in the same run.

## Telegram bot

Optional. With no `TELEGRAM_BOT_TOKEN` the module is inert and the service runs
exactly as it did before.

```
TELEGRAM_BOT_TOKEN=...      # from @BotFather
TELEGRAM_CHAT_ID=...        # chat that gets notifications and may send commands
TELEGRAM_ADMINS=...         # optional extra user ids, comma separated
DAILY_REPORT_AT=09:00       # empty disables the summary
```

### What it sends, and what it deliberately does not

The sync runs 48 times a day. A message per run would train you to ignore the
chat, so notifications follow **state changes** instead:

| Event | Message |
|---|---|
| First failure | one alert with the error |
| Failures 2..n | nothing |
| First success after a failure | one recovery notice |
| Successful sync | nothing (set `NOTIFY_EACH_SYNC=1` to change this) |
| `DAILY_REPORT_AT` | one summary of the previous day |

So a broken sync costs two messages in total -- one when it breaks, one when it
comes back -- however long it stays broken.

### Commands

Accepted from `TELEGRAM_CHAT_ID`, or from any user id in `TELEGRAM_ADMINS`.
Anything else is logged and ignored.

- `/status` — uptime, ClickHouse and Redis health, cached ids, last sync
- `/last` — details of the most recent sync
- `/report` — yesterday's summary on demand
- `/sync` — run a sync now

### Daily counters

Per-day totals live in Redis under `stats:YYYY-MM-DD` with a three-day TTL, so
the summary survives a restart or a redeploy. That is also why `/status` counts
those keys alongside the cached ids.

### Testing it locally

The dev mock also stands in for the Bot API, so all of this can be exercised
without a real token:

```bash
# see what the service has sent
curl http://localhost:3010/__admin/telegram/messages

# pretend a user typed a command
curl -X POST http://localhost:3010/__admin/telegram/send-command \
     -H 'Content-Type: application/json' -d '{"text":"/status"}'
```

## Deployment (Dokploy)

The stack is deployed as a Dokploy **Compose** application from
[docker-compose.yml](docker-compose.yml): the service plus its own Redis.

1. Create a Compose application in Dokploy pointing at this repository.
2. Fill in the environment (Dokploy injects it into the compose file):

   ```
   CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER,
   CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE
   SSH_HOST, SSH_PORT, SSH_USER, SSH_PRIVATE_KEY_B64
   NETHUNT_USER, NETHUNT_API_KEY,
   NETHUNT_FOLDER_ID, NETHUNT_UTILS_FOLDER_ID
   ```

   `REDIS_URL` is set in the compose file and must not be overridden.

3. Deploy. `GET /` on port 3000 is the health endpoint.

### The SSH key

A container has no key file to point `SSH_KEY_PATH` at, so the key is passed
as a value instead:

```bash
base64 -i ~/.ssh/id_ed25519 | tr -d '\n'
```

Base64 rather than the raw PEM because env editors mangle the newlines the key
needs. The service falls back to `SSH_PRIVATE_KEY` (raw) and then to
`SSH_KEY_PATH`, so bare-metal runs keep working unchanged.

Note that an env var is readable by anyone with access to the Dokploy UI or
API. If that is too broad -- or if the key already lives on the server, which
is the usual case when the server's own public key was handed to the ClickHouse
admins -- mount it instead. `docker-compose.yml` carries a commented volume for
exactly that; set `SSH_KEY_PATH` to the path inside the container and make sure
the file is readable by uid 1000.

### Startup when the bastion is unreachable

The service does not exit when it cannot connect. Under a container restart
policy exiting turns an unreachable bastion into a crash loop, which reports
nothing and never gets far enough to send the startup notice.

Instead the HTTP server and the bot start immediately and the connection is
retried in the background with exponential backoff, capped at
`CONNECT_MAX_TIMEOUT`. `GET /` stays 200 -- the container really is alive --
but reports `clickhouse: connecting`, `GET /sync` answers 503, and scheduled
syncs are skipped rather than writing a failed-sync record into NetHunt every
half hour. After `CONNECT_ALERT_AFTER` attempts the chat is told once, and told
again when the connection comes back.

### What to whitelist

ClickHouse only ever sees connections from the **bastion**, never from Dokploy
or from a developer machine -- the tunnel forwards to it from inside the
bastion's network. So the ClickHouse whitelist needs the bastion's static IP,
once. What Dokploy needs is SSH access to that bastion.

### Redis persistence

Redis runs with `--appendonly yes` on a named volume. This matters: the cache
maps FundistUserID to NetHunt record id, and losing it makes the next sync
issue one extra search request per record.

