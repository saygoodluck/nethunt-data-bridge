#!/usr/bin/env bash
#
# Checks the NetHunt side of the integration against the v2 API: the token,
# which folders exist, and whether their fields match what the sync sends --
# names and types both, which the v1 API could not report.
#
#   ./deploy/inspect-nethunt.sh              # read-only
#   ./deploy/inspect-nethunt.sh --probe      # also writes one record, see below
#
# No tunnel and no whitelisted IP needed: unlike ClickHouse, the NetHunt API is
# reachable from anywhere, so this runs on a laptop.
#
# --probe writes a single record into the SERVICE folder (never the shared one)
# and reads it back, confirming that finishedAt survives the round trip and that
# sorting really does return the newest record -- the sync window depends on
# both, and listing records returns them oldest first.

set -uo pipefail

PROJ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$PROJ/.env}"
RUN_PROBE=0
[[ "${1:-}" == "--probe" ]] && RUN_PROBE=1
FAILED=0

green() { printf '\033[32m%s\033[0m\n' "$1"; }
red()   { printf '\033[31m%s\033[0m\n' "$1"; }
step()  { printf '\n\033[1m== %s\033[0m\n' "$1"; }
ok()    { green "   ok: $1"; }
bad()   { red   "   FAIL: $1"; FAILED=1; }
note()  { printf '   %s\n' "$1"; }

command -v python3 >/dev/null 2>&1 || { echo "python3 is required to parse the API responses" >&2; exit 1; }

step "Configuration"
[[ -f "$ENV_FILE" ]] || { red "No env file at $ENV_FILE"; exit 1; }
if grep -q $'\r' "$ENV_FILE"; then
    bad "$ENV_FILE has Windows line endings; every value ends with a stray CR"
    red "   Fix with: sed -i 's/\r$//' $ENV_FILE"
    exit 1
fi
set -a; . "$ENV_FILE"; set +a
ok "loaded $ENV_FILE"

BASE="${NETHUNT_BASE_URL:-https://nethunt.com/api/v2}"
[[ -z "${NETHUNT_API_TOKEN:-}" ]] && { bad "NETHUNT_API_TOKEN is not set"; exit 1; }
note "api: ${BASE}   token length: ${#NETHUNT_API_TOKEN}"

# body on stdout, status code on the last line
call() {
    curl -s -w '\n%{http_code}' --max-time 30 \
        -H "Authorization: Bearer ${NETHUNT_API_TOKEN}" \
        -H 'Content-Type: application/json' "$@"
}

# --- 1. token ---------------------------------------------------------------
step "Authentication"
raw=$(call "${BASE}/folders")
code=$(tail -1 <<< "$raw")
body=$(sed '$d' <<< "$raw")

if [[ "$code" != "200" ]]; then
    bad "GET /folders returned HTTP $code"
    [[ -n "$body" ]] && red "   response: $(head -c 300 <<< "$body")"
    if [[ "$code" == "401" ]]; then
        red "   v2 uses a Bearer token from Settings -> API & MCP (it looks like nh_live_...)."
        red "   The key under Apps and other integrations -> API LEGACY is for the v1 API"
        red "   and is rejected here."
    fi
    exit 1
fi
ok "token accepted"

# --- 2. folders -------------------------------------------------------------
step "Folders"
FOLDERS_JSON="$body" RECORDS_ID="${NETHUNT_FOLDER_ID:-}" UTILS_ID="${NETHUNT_UTILS_FOLDER_ID:-}" python3 <<'PY'
import json, os
try:
    data = json.loads(os.environ['FOLDERS_JSON'])
except json.JSONDecodeError:
    print('   could not parse the folder list'); raise SystemExit
folders = data.get('folders', data) if isinstance(data, dict) else data
if not folders:
    print('   none -- the tenant has no folders yet'); raise SystemExit
cfg = {os.environ.get('RECORDS_ID'): 'NETHUNT_FOLDER_ID',
       os.environ.get('UTILS_ID'): 'NETHUNT_UTILS_FOLDER_ID'}
for f in folders:
    i = str(f.get('id', '?'))
    mark = f'   <- {cfg[i]}' if i in cfg and cfg[i] else ''
    print(f'   {i}  {f.get("name", "(unnamed)")}{mark}')
PY

# --- 3. field schemas -------------------------------------------------------
# mirrors RECORD_FIELD_NAMES / UTILS_FIELD_NAMES in index.js. `Name` is absent
# on purpose: in v2 the display name is the special `name` key, not a field.
RECORD_FIELDS="FundistUserID Login FirstName LastName Email PhoneNumber \
PhoneVerified DateOfBirth Gender Language Country City Timezone \
LastCreditDate RegistrationDate LastLoginDate PEP AccountStatus \
TotalDeposit TotalWithdraw"
UTILS_FIELDS="finishedAt totalSynced duration createdRecords updatedRecords errorMessage"

check_fields() {
    local label="$1" folder_id="$2" expected="$3"

    step "Fields: $label"
    if [[ -z "$folder_id" ]]; then
        bad "folder id is not set in $ENV_FILE"
        return
    fi

    local raw code body out
    raw=$(call "${BASE}/folders/${folder_id}")
    code=$(tail -1 <<< "$raw")
    body=$(sed '$d' <<< "$raw")

    if [[ "$code" != "200" ]]; then
        bad "GET /folders/${folder_id} returned HTTP $code"
        [[ -n "$body" ]] && red "   response: $(head -c 200 <<< "$body")"
        return
    fi

    out=$(SCHEMA_JSON="$body" EXPECTED="$expected" python3 <<'PY'
import json, os, sys

# the sync sends these as JSON numbers, and as plain strings respectively
NUMERIC = {'FundistUserID', 'TotalDeposit', 'TotalWithdraw'}
DATE_LIKE = {'DateOfBirth', 'LastCreditDate', 'RegistrationDate', 'LastLoginDate'}

try:
    folder = json.loads(os.environ['SCHEMA_JSON'])
except json.JSONDecodeError:
    print('RAW'); print(os.environ['SCHEMA_JSON'][:500]); sys.exit(0)

fields = {f['name']: f for f in folder.get('fields', []) if f.get('name')}
if not fields:
    print('RAW'); print(os.environ['SCHEMA_JSON'][:500]); sys.exit(0)

expected = os.environ['EXPECTED'].split()
for name in expected:
    f = fields.get(name)
    if not f:
        print(f'MISSING\t{name}')
        continue
    vt, t = f.get('valueType', '?'), f.get('type', '?')
    if name in DATE_LIKE and vt in ('DATE', 'TIME'):
        # a date field wants epoch ms; the sync sends formatted text
        print(f'BAD\t{name} is a {t} field, but the sync sends text — every write would be rejected')
    elif name in NUMERIC and vt != 'NUMBER':
        print(f'WARN\t{name} is {t}; values are sent as text, so the CRM cannot sort or filter by it')
    else:
        print(f'OK\t{name} ({t})')

for name in sorted(set(fields) - set(expected)):
    print(f'EXTRA\t{name} ({fields[name].get("type", "?")})')
PY
    )

    if [[ "$(head -1 <<< "$out")" == "RAW" ]]; then
        bad "could not recognise the schema shape; raw response follows"
        sed '1d' <<< "$out" | sed 's/^/     /'
        return
    fi

    local problems=0
    while IFS=$'\t' read -r kind rest; do
        case "$kind" in
            OK)      ok "$rest" ;;
            MISSING) bad "$rest is missing"; problems=1 ;;
            BAD)     bad "$rest"; problems=1 ;;
            WARN)    red "   warn: $rest" ;;
            # Name is the display name; Comment and anything else belongs to the team
            EXTRA)   note "extra: $rest — not sent by the sync, safe" ;;
        esac
    done <<< "$out"
    [[ $problems -eq 0 ]] && ok "every field the sync writes is present and usable"
}

check_fields "records folder" "${NETHUNT_FOLDER_ID:-}" "$RECORD_FIELDS"
check_fields "utils folder" "${NETHUNT_UTILS_FOLDER_ID:-}" "$UTILS_FIELDS"

# --- 4. probe ---------------------------------------------------------------
if [[ $RUN_PROBE -eq 1 ]]; then
    step "Probe (writes one record into the utils folder)"
    if [[ -z "${NETHUNT_UTILS_FOLDER_ID:-}" ]]; then
        bad "NETHUNT_UTILS_FOLDER_ID is not set; nothing safe to write to"
    else
        schema=$(call "${BASE}/folders/${NETHUNT_UTILS_FOLDER_ID}" | sed '$d')
        finished_id=$(SCHEMA="$schema" python3 -c "
import json,os
f=json.loads(os.environ['SCHEMA']).get('fields',[])
print(next((x['id'] for x in f if x.get('name')=='finishedAt'), ''))")

        if [[ -z "$finished_id" ]]; then
            bad "the utils folder has no finishedAt field to probe with"
        else
            stamp=$(date -u +%Y-%m-%dT%H:%M:%S.000Z)
            created=$(call -X POST "${BASE}/folders/${NETHUNT_UTILS_FOLDER_ID}/records" \
                -d "{\"fields\":{\"name\":\"probe from inspect-nethunt.sh\",\"${finished_id}\":\"${stamp}\"}}")
            ccode=$(tail -1 <<< "$created")
            cbody=$(sed '$d' <<< "$created")

            # the API answers 201 here, not 200
            if [[ "$ccode" != "201" && "$ccode" != "200" ]]; then
                bad "creating a record returned HTTP $ccode"
                red "   response: $(head -c 300 <<< "$cbody")"
            else
                probe_id=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('id',''))" "$cbody")
                ok "record created (${probe_id})"

                back=$(call -X POST "${BASE}/folders/${NETHUNT_UTILS_FOLDER_ID}/records/filter?limit=1" \
                    -d "{\"filter\":{\"${finished_id}\":{\"_exists\":true}},\"sort\":[{\"created\":-1}]}" | sed '$d')

                STAMP="$stamp" FIELD="$finished_id" BACK="$back" python3 <<'PY'
import json, os
stamp, field = os.environ['STAMP'], os.environ['FIELD']
try:
    data = json.loads(os.environ['BACK'])
except json.JSONDecodeError:
    print('   could not parse the filter response'); raise SystemExit
records = data.get('records') or []
print(f"   metrics records in the folder: {data.get('total', len(records))}")
if not records:
    print('   the filter returned nothing — getLastSyncTime() would see no last sync')
    raise SystemExit
got = str(records[0].get('fields', {}).get(field, ''))
print(f"   newest-first sorting: {'yes' if got == stamp else 'NO — the record just written is not first'}")
print(f"   finishedAt round trip: sent {stamp!r} got {got!r}")
if got != stamp:
    print('   calculateInterval() parses this with new Date(); a changed format shifts the sync window')
PY
                note "Delete the probe record from the utils folder when you are done."
            fi
        fi
    fi
fi

echo
if [[ $FAILED -eq 0 ]]; then
    green "NetHunt side looks consistent with what the sync sends."
else
    red "Some checks failed -- see the FAIL lines above."
fi
exit $FAILED
