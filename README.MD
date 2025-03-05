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