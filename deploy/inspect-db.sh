#!/usr/bin/env bash
#
# Answers the two open questions against the real database, using the
# credentials already in .env so nothing has to be typed by hand:
#
#   * which of the DateOfBirth columns actually carries data
#   * what the Turnovers join costs, before and after narrowing it
#
# Run on the whitelisted server, same as check-access.sh:
#
#   ./deploy/inspect-db.sh              # everything
#   ./deploy/inspect-db.sh --no-timing  # skip the heavy comparison
#
# Read-only. The timing section runs the sync's own query twice, which is the
# same load the service produces on its own; --no-timing skips it.

set -uo pipefail

PROJ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$PROJ/.env}"
TUNNEL_PORT="${TUNNEL_PORT:-18124}"
RUN_TIMING=1
[[ "${1:-}" == "--no-timing" ]] && RUN_TIMING=0

SSH_PID=""
cleanup() { [[ -n "$SSH_PID" ]] && kill "$SSH_PID" 2>/dev/null; return 0; }
trap cleanup EXIT

step() { printf '\n\033[1m== %s\033[0m\n' "$1"; }
note() { printf '   %s\n' "$1"; }

[[ -f "$ENV_FILE" ]] || { echo "No env file at $ENV_FILE" >&2; exit 1; }
set -a; . "$ENV_FILE"; set +a

SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new -p "$SSH_PORT")
if [[ -n "${SSH_KEY_PATH:-}" ]]; then
    [[ "$SSH_KEY_PATH" == "~/"* ]] && SSH_KEY_PATH="${HOME}/${SSH_KEY_PATH#\~/}"
    SSH_OPTS+=(-i "$SSH_KEY_PATH" -o IdentitiesOnly=yes)
fi

step "Tunnel"
if [[ "${SKIP_SSH_TUNNEL:-}" == "1" ]]; then
    CH="http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}"
    note "SKIP_SSH_TUNNEL=1, using $CH directly"
else
    ssh "${SSH_OPTS[@]}" -o ExitOnForwardFailure=yes -N \
        -L "${TUNNEL_PORT}:${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}" "$SSH_USER@$SSH_HOST" &
    SSH_PID=$!
    sleep 3
    kill -0 "$SSH_PID" 2>/dev/null || { echo "tunnel failed to open" >&2; exit 1; }
    CH="http://127.0.0.1:${TUNNEL_PORT}"
    note "up on 127.0.0.1:${TUNNEL_PORT}"
fi

CH_AUTH=(-H "X-ClickHouse-User: ${CLICKHOUSE_USER}" -H "X-ClickHouse-Key: ${CLICKHOUSE_PASSWORD}")
DB="${CLICKHOUSE_DATABASE}"
q() { curl -s --max-time 300 "${CH}/?database=${DB}" "${CH_AUTH[@]}" --data-binary "$1"; }

if [[ "$(q 'SELECT 1')" != "1" ]]; then
    echo "ClickHouse rejected the credentials from $ENV_FILE" >&2
    exit 1
fi
note "credentials from $ENV_FILE accepted"

# --- how the tables are laid out -------------------------------------------
step "Table layout"
q "SELECT name, engine, sorting_key, formatReadableQuantity(total_rows) AS rows
   FROM system.tables
   WHERE database = '${DB}' AND name IN ('UserHistory','CountriesNew','Turnovers')
   FORMAT Vertical"

# --- the DDL, for reviewing the query against the real schema ---------------
step "Schema"
for table in UserHistory CountriesNew Turnovers; do
    printf '\n   \033[1m%s\033[0m\n' "$table"
    q "SHOW CREATE TABLE ${table}" | sed 's/\\n/\n/g' | sed 's/^/     /'
done

step "Storage"
q "SELECT table,
          formatReadableQuantity(sum(rows))                AS rows,
          formatReadableSize(sum(data_compressed_bytes))   AS compressed,
          formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
          count()                                          AS parts
   FROM system.parts
   WHERE database = '${DB}' AND active
     AND table IN ('UserHistory','CountriesNew','Turnovers')
   GROUP BY table ORDER BY table FORMAT PrettyCompactMonoBlock"

step "Types of the columns the query reads"
q "SELECT table, name, type
   FROM system.columns
   WHERE database = '${DB}'
     AND table IN ('UserHistory','CountriesNew','Turnovers')
     AND name IN ('UserID','ID','CountryID','RecordTime','RecordDate','LastUpdated',
                  'Login','Name','LastName','Email','Phone','PhoneVerified',
                  'DateOfBirth','DateOfBirthNew','Gender','Language','City','Timezone',
                  'LastCreditDate','RegistrationDate','LastLoginDate','PEP','Status',
                  'Deposit','Withdraw')
   ORDER BY table, name FORMAT PrettyCompactMonoBlock"

# --- which birth column is the real one -------------------------------------
step "Birth-date columns"
q "SELECT name, type FROM system.columns
   WHERE database = '${DB}' AND table = 'UserHistory' AND name ILIKE '%Birth%'
   ORDER BY name FORMAT TSV" | while IFS=$'\t' read -r col type; do
    [[ -z "$col" ]] && continue
    printf '\n   \033[1m%s\033[0m (%s)\n' "$col" "$type"

    # toString keeps this working whether the column is a Date or a String
    filled=$(q "SELECT
                    count() AS total,
                    countIf(toString(${col}) NOT IN ('', '1970-01-01', '0000-00-00', '1970-01-01 00:00:00')) AS filled
                FROM UserHistory
                WHERE LastUpdated > now() - INTERVAL 7 DAY
                FORMAT TSV")
    note "last 7 days: $(awk '{print $2" of "$1" rows populated"}' <<< "$filled")"

    samples=$(q "SELECT DISTINCT toString(${col})
                 FROM UserHistory
                 WHERE LastUpdated > now() - INTERVAL 7 DAY
                   AND toString(${col}) NOT IN ('', '1970-01-01', '0000-00-00', '1970-01-01 00:00:00')
                 LIMIT 5 FORMAT TSV")
    if [[ -n "$samples" ]]; then
        note "samples: $(tr '\n' ' ' <<< "$samples")"
    else
        note "samples: none -- this column is empty and must not be used"
    fi
done

# --- what the join actually costs -------------------------------------------
if [[ $RUN_TIMING -eq 1 ]]; then
    step "Paging cost: OFFSET versus keyset"
    note "the same page read two ways; numbers come from X-ClickHouse-Summary"

    timed() {
        local label="$1" sql="$2" hdr t rows bytes summary
        hdr=$(mktemp)
        t=$(curl -s -o /dev/null -D "$hdr" -w '%{time_total}' --max-time 600 \
            "${CH}/?database=${DB}" "${CH_AUTH[@]}" --data-binary "$sql")
        summary=$(grep -i '^x-clickhouse-summary' "$hdr" | tr -d '\r' | cut -d' ' -f2-)
        rm -f "$hdr"
        rows=$(sed -n 's/.*"read_rows":"\{0,1\}\([0-9]*\).*/\1/p' <<< "$summary")
        bytes=$(sed -n 's/.*"read_bytes":"\{0,1\}\([0-9]*\).*/\1/p' <<< "$summary")
        printf '   %-32s %8ss' "$label" "$t"
        if [[ -n "$rows" ]]; then
            awk -v r="$rows" -v b="${bytes:-0}" 'BEGIN {
                printf "   rows: %'"'"'d   %.2f GB\n", r, b/1e9
            }'
        else
            echo
        fi
    }

    # a year-wide window: the shape a backfill actually runs with
    SINCE=$(( $(date +%s) - 365*24*3600 ))

    # measure at half the population, so there is a page on both sides of it
    USERS=$(q "SELECT uniq(UserID) FROM UserHistory WHERE LastUpdated > toDateTime(${SINCE})" | tr -d '\r\n')
    DEEP=$(( ${USERS:-0} / 2 ))
    [[ $DEEP -lt 1 ]] && DEEP=1
    note "users in this window: ${USERS:-?}; measuring at depth ${DEEP}"

    note "window: one year back, page size 100"
    timed "OFFSET 0" "SELECT count() FROM (SELECT uh.UserID FROM UserHistory uh
        WHERE uh.LastUpdated > toDateTime(${SINCE})
        GROUP BY uh.UserID ORDER BY uh.UserID DESC LIMIT 100 OFFSET 0)"

    timed "OFFSET ${DEEP}" "SELECT count() FROM (SELECT uh.UserID FROM UserHistory uh
        WHERE uh.LastUpdated > toDateTime(${SINCE})
        GROUP BY uh.UserID ORDER BY uh.UserID DESC LIMIT 100 OFFSET ${DEEP})"

    # the id that deep page starts at, so the keyset read covers the same rows
    CURSOR=$(q "SELECT min(UserID) FROM (SELECT uh.UserID FROM UserHistory uh
        WHERE uh.LastUpdated > toDateTime(${SINCE})
        GROUP BY uh.UserID ORDER BY uh.UserID DESC LIMIT ${DEEP})" | tr -d '\r\n')
    CURSOR=${CURSOR:-9007199254740991}

    timed "keyset at the same depth" "SELECT count() FROM (SELECT uh.UserID FROM UserHistory uh
        WHERE uh.LastUpdated > toDateTime(${SINCE}) AND uh.UserID < ${CURSOR}
        GROUP BY uh.UserID ORDER BY uh.UserID DESC LIMIT 100)"

    echo
    note "the hourly sync window, with and without the RecordDate bound:"
    HOUR=$(( $(date +%s) - 3600 ))
    timed "no RecordDate bound" "SELECT count() FROM (SELECT uh.UserID FROM UserHistory uh
        WHERE uh.LastUpdated > toDateTime(${HOUR})
        GROUP BY uh.UserID)"
    timed "bounded by RecordDate" "SELECT count() FROM (SELECT uh.UserID FROM UserHistory uh
        WHERE uh.LastUpdated > toDateTime(${HOUR})
          AND uh.RecordDate >= toDate(toDateTime(${HOUR}))
        GROUP BY uh.UserID)"

    echo
    note "the Turnovers join, bounded by the window versus by the page:"
    timed "join bounded by the window" "SELECT count() FROM (SELECT uh.UserID FROM UserHistory uh
        LEFT JOIN (SELECT UserID, sum(Deposit)/100 d FROM Turnovers
                   WHERE UserID IN (SELECT UserID FROM UserHistory
                                    WHERE LastUpdated > toDateTime(${SINCE}))
                   GROUP BY UserID) t ON uh.UserID = t.UserID
        WHERE uh.LastUpdated > toDateTime(${SINCE}) AND uh.UserID < ${CURSOR}
        GROUP BY uh.UserID ORDER BY uh.UserID DESC LIMIT 100)"

    timed "join bounded by the page" "SELECT count() FROM (SELECT uh.UserID FROM UserHistory uh
        LEFT JOIN (SELECT UserID, sum(Deposit)/100 d FROM Turnovers
                   WHERE UserID IN (SELECT UserID FROM UserHistory
                                    WHERE LastUpdated > toDateTime(${SINCE}) AND UserID < ${CURSOR}
                                    GROUP BY UserID ORDER BY UserID DESC LIMIT 100)
                   GROUP BY UserID) t ON uh.UserID = t.UserID
        WHERE uh.LastUpdated > toDateTime(${SINCE}) AND uh.UserID < ${CURSOR}
        GROUP BY uh.UserID ORDER BY uh.UserID DESC LIMIT 100)"
fi

echo
