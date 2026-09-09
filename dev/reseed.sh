#!/usr/bin/env bash
#
# Re-seed the dev ClickHouse with fresh timestamps.
#
# The seed anchors LastUpdated to now(), so data written on Monday falls out of
# the sync window by Tuesday and the service correctly reports 0 records. Run
# this instead of recreating the whole contour.
#
#   npm run dev:reseed

set -euo pipefail

PROJ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTAINER=nethunt-dev-clickhouse

# Credentials come from .env.dev rather than being repeated here. They belong to
# a throwaway container holding invented data, but a password literal in a
# script reads as a leak to a scanner -- and one source of truth is better
# regardless, since the value also has to match docker-compose.dev.yml.
set -a; . "$PROJ/.env.dev"; set +a

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    echo "$CONTAINER is not running. Start it with: npm run dev:up" >&2
    exit 1
fi

ch() {
    docker exec -i "$CONTAINER" clickhouse-client \
        --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
        --database "$CLICKHOUSE_DATABASE" "$@"
}

echo "-> clearing tables"
for table in UserHistory CountriesNew Turnovers; do
    ch --query "TRUNCATE TABLE IF EXISTS $table"
done

echo "-> re-running seed"
ch --multiquery < "$PROJ/dev/clickhouse/init/02-seed.sql"

echo "-> users now inside the default 60-minute window:"
ch --query "SELECT uniq(UserID) FROM UserHistory WHERE LastUpdated > now() - INTERVAL 60 MINUTE"
