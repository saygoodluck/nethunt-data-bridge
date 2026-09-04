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

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    echo "$CONTAINER is not running. Start it with: npm run dev:up" >&2
    exit 1
fi

ch() {
    docker exec -i "$CONTAINER" clickhouse-client \
        --user bridge --password bridge --database analytics "$@"
}

echo "-> clearing tables"
for table in UserHistory CountriesNew Turnovers; do
    ch --query "TRUNCATE TABLE IF EXISTS $table"
done

echo "-> re-running seed"
ch --multiquery < "$PROJ/dev/clickhouse/init/02-seed.sql"

echo "-> users now inside the default 60-minute window:"
ch --query "SELECT uniq(UserID) FROM UserHistory WHERE LastUpdated > now() - INTERVAL 60 MINUTE"
