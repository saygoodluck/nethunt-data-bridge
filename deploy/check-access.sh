#!/usr/bin/env bash
#
# Verifies every layer between this machine and the data, one at a time, so a
# failure points at the layer that broke instead of at "the sync does not work".
#
# Run it ON THE SERVER whose IP is whitelisted -- from a laptop it will fail at
# the ClickHouse step no matter how correct the credentials are.
#
#   ./deploy/check-access.sh            # reads ./.env
#   ./deploy/check-access.sh /path/.env
#
# Read-only: it never writes to ClickHouse and never creates a NetHunt record.

set -uo pipefail

ENV_FILE="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.env}"
TUNNEL_PORT="${TUNNEL_PORT:-18123}"
SSH_PID=""
FAILED=0

green() { printf '\033[32m%s\033[0m\n' "$1"; }
red()   { printf '\033[31m%s\033[0m\n' "$1"; }
step()  { printf '\n\033[1m== %s\033[0m\n' "$1"; }
ok()    { green "   ok: $1"; }
bad()   { red   "   FAIL: $1"; FAILED=1; }

cleanup() {
    [[ -n "$SSH_PID" ]] && kill "$SSH_PID" 2>/dev/null
    return 0
}
trap cleanup EXIT

# --- 0. configuration -------------------------------------------------------
step "Configuration"
if [[ ! -f "$ENV_FILE" ]]; then
    red "No env file at $ENV_FILE"
    exit 1
fi
set -a; . "$ENV_FILE"; set +a
ok "loaded $ENV_FILE"

missing=()
for var in CLICKHOUSE_HOST CLICKHOUSE_PORT CLICKHOUSE_USER CLICKHOUSE_PASSWORD \
           CLICKHOUSE_DATABASE SSH_HOST SSH_PORT SSH_USER; do
    [[ -z "${!var:-}" ]] && missing+=("$var")
done
if [[ ${#missing[@]} -gt 0 ]]; then
    bad "missing: ${missing[*]}"
    exit 1
fi
ok "all required variables present"

# --- 1. can we reach the bastion at all? ------------------------------------
step "SSH to bastion ($SSH_USER@$SSH_HOST:$SSH_PORT)"
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new -p "$SSH_PORT")

if [[ -n "${SSH_KEY_PATH:-}" ]]; then
    if [[ ! -r "$SSH_KEY_PATH" ]]; then
        bad "SSH_KEY_PATH points at $SSH_KEY_PATH, which does not exist here"
        if [[ "$SSH_KEY_PATH" == /Users/* ]]; then
            red "   That is a macOS path -- this .env was copied from a laptop."
        fi
        red "   Keys available to $(whoami):"
        ls -1 "$HOME"/.ssh/id_* 2>/dev/null | grep -v '\.pub$' | sed 's/^/     /' \
            || red "     none -- generate one with: ssh-keygen -t ed25519"
        red "   Set SSH_KEY_PATH to the private key whose .pub was given to the admins."
        exit 1
    fi
    # without this ssh silently falls back to other keys and the error is ambiguous
    SSH_OPTS+=(-i "$SSH_KEY_PATH" -o IdentitiesOnly=yes)
    ok "using key $SSH_KEY_PATH ($(ssh-keygen -lf "$SSH_KEY_PATH" 2>/dev/null | awk '{print $2}'))"
else
    red "   SSH_KEY_PATH is not set; relying on whatever ssh picks by default"
fi

if ssh_out=$(ssh "${SSH_OPTS[@]}" "$SSH_USER@$SSH_HOST" 'echo reachable; hostname' 2>&1); then
    ok "authenticated as $SSH_USER ($(echo "$ssh_out" | tail -1))"
else
    bad "$(echo "$ssh_out" | tail -3)"
    if [[ "$ssh_out" == *"Permission denied"* ]]; then
        red "   The server reached the bastion, so the address and port are fine."
        red "   Either the admins have not installed this key yet, or it was"
        red "   installed for a different user than '$SSH_USER'."
        if [[ -n "${SSH_KEY_PATH:-}" && -r "${SSH_KEY_PATH}.pub" ]]; then
            red "   The public key to hand them is:"
            sed 's/^/     /' "${SSH_KEY_PATH}.pub"
        fi
    fi
    exit 1
fi

# --- 2. can the bastion see ClickHouse? -------------------------------------
step "Tunnel to ClickHouse ($CLICKHOUSE_HOST:$CLICKHOUSE_PORT via bastion)"
ssh "${SSH_OPTS[@]}" -N -L "${TUNNEL_PORT}:${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}" \
    "$SSH_USER@$SSH_HOST" &
SSH_PID=$!
sleep 3

if ! kill -0 "$SSH_PID" 2>/dev/null; then
    bad "tunnel process died immediately"
    exit 1
fi
ok "tunnel up on 127.0.0.1:$TUNNEL_PORT"

CH="http://127.0.0.1:${TUNNEL_PORT}"
CH_AUTH=(-H "X-ClickHouse-User: ${CLICKHOUSE_USER}" -H "X-ClickHouse-Key: ${CLICKHOUSE_PASSWORD}")

ping_out=$(curl -s --max-time 15 "$CH/ping" 2>&1)
if [[ "$ping_out" == *"Ok"* ]]; then
    ok "ClickHouse answers /ping"
else
    bad "no answer through the tunnel: ${ping_out:-<empty>}"
    red "   The bastion is reachable but ClickHouse is not, from its network."
    exit 1
fi

# --- 3. do the credentials work? --------------------------------------------
step "ClickHouse authentication"
q() { curl -s --max-time 30 "$CH/?database=${CLICKHOUSE_DATABASE}" "${CH_AUTH[@]}" --data-binary "$1"; }

sel=$(q "SELECT 1")
if [[ "$sel" == "1" ]]; then
    ok "credentials accepted, database '$CLICKHOUSE_DATABASE' selectable"
else
    bad "${sel:-<empty response>}"
    exit 1
fi

ver=$(q "SELECT version()")
ok "server version $ver"

# --- 4. is the schema what the sync expects? --------------------------------
step "Schema"
for table in UserHistory CountriesNew Turnovers; do
    cnt=$(q "SELECT count() FROM ${table}" 2>&1)
    if [[ "$cnt" =~ ^[0-9]+$ ]]; then
        ok "$table: $cnt rows"
    else
        bad "$table: ${cnt:-<empty>}"
    fi
done

# The sync selects these by name; a rename here is a silent empty field
step "Columns the sync reads from UserHistory"
cols=$(q "SELECT name FROM system.columns WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 'UserHistory' ORDER BY name FORMAT TSV")
for col in UserID Login Name LastName Email Phone PhoneVerified Gender Language \
           CountryID City Timezone LastCreditDate RegistrationDate LastLoginDate \
           PEP Status RecordTime LastUpdated; do
    if grep -qx "$col" <<< "$cols"; then
        ok "$col"
    else
        bad "$col is missing"
    fi
done

# DateOfBirth is mapped in index.js but absent from the query -- find its real name
step "Candidates for the unmapped DateOfBirth field"
birth=$(grep -iE 'birth|dob' <<< "$cols" || true)
if [[ -n "$birth" ]]; then
    echo "$birth" | while read -r c; do green "   found: $c"; done
    red "   index.js sends record.DateOfBirth, which the query never selects."
else
    echo "   no birth-like column in UserHistory"
fi

# --- 5. does the actual sync query run? -------------------------------------
step "The sync query itself (last 60 minutes)"
rows=$(q "
    SELECT count() FROM (
        SELECT uh.UserID
        FROM UserHistory uh
        JOIN CountriesNew c ON c.ID = uh.CountryID
        WHERE uh.LastUpdated > now() - INTERVAL 60 MINUTE
        GROUP BY uh.UserID
    )" 2>&1)
if [[ "$rows" =~ ^[0-9]+$ ]]; then
    ok "$rows users changed in the last hour"
    [[ "$rows" == "0" ]] && echo "   (zero is fine off-hours; it only means nothing changed recently)"
else
    bad "${rows:-<empty>}"
fi

# --- 6. NetHunt, read-only --------------------------------------------------
if [[ -n "${NETHUNT_API_KEY:-}" && -n "${NETHUNT_USER:-}" ]]; then
    step "NetHunt API"
    base="${NETHUNT_BASE_URL:-https://nethunt.com/api/v1/zapier}"

    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 \
        -u "${NETHUNT_USER}:${NETHUNT_API_KEY}" \
        "${base}/searches/find-record/${NETHUNT_FOLDER_ID}?query=FundistUserID=0")
    [[ "$code" == "200" ]] && ok "records folder reachable (HTTP $code)" \
                           || bad "records folder returned HTTP $code"

    if [[ -n "${NETHUNT_UTILS_FOLDER_ID:-}" ]]; then
        code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 \
            -u "${NETHUNT_USER}:${NETHUNT_API_KEY}" \
            "${base}/triggers/new-record/${NETHUNT_UTILS_FOLDER_ID}")
        [[ "$code" == "200" ]] && ok "utils folder reachable (HTTP $code)" \
                               || bad "utils folder returned HTTP $code"
    else
        red "   NETHUNT_UTILS_FOLDER_ID is not set; metrics and last-sync time will not work"
    fi
fi

# --- verdict ----------------------------------------------------------------
echo
if [[ $FAILED -eq 0 ]]; then
    green "All checks passed. The service should start cleanly here."
else
    red "Some checks failed -- see the FAIL lines above."
fi
exit $FAILED
