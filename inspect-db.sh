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
    step "Cost of the Turnovers join (interval: 60 minutes)"

    timed() {
        local label="$1" sql="$2" hdr t summary
        hdr=$(mktemp)
        t=$(curl -s -o /dev/null -D "$hdr" -w '%{time_total}' --max-time 300 \
            "${CH}/?database=${DB}" "${CH_AUTH[@]}" --data-binary "$sql")
        summary=$(grep -i '^x-clickhouse-summary' "$hdr" | tr -d '\r' | cut -d' ' -f2-)
        rm -f "$hdr"
        printf '   %-10s %8ss\n' "$label" "$t"
        if [[ -n "$summary" ]]; then
            # ClickHouse reports these as quoted strings; pull them out without
            # depending on a JSON parser being present
            local rows bytes
            rows=$(sed -n 's/.*"read_rows":"\{0,1\}\([0-9]*\).*/\1/p' <<< "$summary")
            bytes=$(sed -n 's/.*"read_bytes":"\{0,1\}\([0-9]*\).*/\1/p' <<< "$summary")
            if [[ -n "$rows" ]]; then
                awk -v r="$rows" -v b="${bytes:-0}" 'BEGIN {
                    printf "        rows read: %'"'"'d   bytes read: %.2f GB\n", r, b/1e9
                }'
            else
                note "  $summary"
            fi
        fi
    }

    COMMON="FROM UserHistory uh
            JOIN CountriesNew c ON c.ID = uh.CountryID"
    TAIL="WHERE uh.LastUpdated > now() - INTERVAL 60 MINUTE
          GROUP BY uh.UserID"

    note "the current form aggregates the whole table; this may take a while"
    timed "before" "SELECT count() FROM (SELECT uh.UserID ${COMMON}
        LEFT JOIN (SELECT UserID, sum(Deposit)/100 d, sum(Withdraw)/100 w
                   FROM Turnovers GROUP BY UserID) t ON uh.UserID = t.UserID
        ${TAIL})"

    timed "after" "SELECT count() FROM (SELECT uh.UserID ${COMMON}
        LEFT JOIN (SELECT UserID, sum(Deposit)/100 d, sum(Withdraw)/100 w
                   FROM Turnovers
                   WHERE UserID IN (SELECT UserID FROM UserHistory
                                    WHERE LastUpdated > now() - INTERVAL 60 MINUTE)
                   GROUP BY UserID) t ON uh.UserID = t.UserID
        ${TAIL})"
fi

echo
