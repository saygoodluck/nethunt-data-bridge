#!/usr/bin/env bash
#
# Checks the NetHunt side of the integration: credentials, which folders exist,
# and whether their fields match what the sync actually sends.
#
#   ./deploy/inspect-nethunt.sh              # read-only
#   ./deploy/inspect-nethunt.sh --probe      # also writes one record, see below
#
# Unlike check-access.sh this needs no tunnel and no whitelisted IP -- the
# NetHunt API is reachable from anywhere, so it runs on a laptop just as well.
#
# --probe writes a single record into the SERVICE folder (never the shared one)
# and reads it back, which settles three things that cannot be known from the
# documentation alone: whether finishedAt survives a round trip unchanged,
# whether new-record returns the newest record first, and which query syntax
# find-record actually accepts.

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

# --- 0. configuration -------------------------------------------------------
step "Configuration"
[[ -f "$ENV_FILE" ]] || { red "No env file at $ENV_FILE"; exit 1; }
if grep -q $'\r' "$ENV_FILE"; then
    bad "$ENV_FILE has Windows line endings; every value ends with a stray CR"
    red "   Fix with: sed -i 's/\r$//' $ENV_FILE"
    exit 1
fi
set -a; . "$ENV_FILE"; set +a
ok "loaded $ENV_FILE"

BASE="${NETHUNT_BASE_URL:-https://nethunt.com/api/v1/zapier}"
for var in NETHUNT_USER NETHUNT_API_KEY; do
    [[ -z "${!var:-}" ]] && { bad "$var is not set"; exit 1; }
done
note "user: ${NETHUNT_USER}   api: ${BASE}"

# body on stdout, status code on the last line
call() {
    curl -s -w '\n%{http_code}' --max-time 30 -u "${NETHUNT_USER}:${NETHUNT_API_KEY}" "$@"
}

# --- 1. credentials ---------------------------------------------------------
step "Authentication"
raw=$(call "${BASE}/triggers/readable-folder")
code=$(tail -1 <<< "$raw")
body=$(sed '$d' <<< "$raw")

if [[ "$code" != "200" ]]; then
    bad "readable-folder returned HTTP $code"
    [[ -n "$body" ]] && red "   response: $(head -c 300 <<< "$body")"
    if [[ "$code" == "401" ]]; then
        red "   NetHunt keeps two kinds of API key and they are not interchangeable:"
        red "     Settings -> API & MCP                        -> keys for API v2"
        red "     Settings -> Apps and other integrations      -> API LEGACY"
        red "   This service calls /api/v1/zapier/..., so it needs the LEGACY key."
        red "   A v2 key fails here with exactly this message."
        red "   user=${NETHUNT_USER} key length=${#NETHUNT_API_KEY}"
        [[ "${NETHUNT_USER}" != *@* ]] && red "   NETHUNT_USER does not look like an email address."
        [[ "${NETHUNT_API_KEY}" =~ [[:space:]] ]] && red "   NETHUNT_API_KEY contains whitespace."
    fi
    exit 1
fi
ok "credentials accepted"

# --- 2. folders -------------------------------------------------------------
step "Folders"
writable=$(call "${BASE}/triggers/writable-folder" | sed '$d')

FOLDERS_JSON="$body" WRITABLE_JSON="$writable" \
RECORDS_ID="${NETHUNT_FOLDER_ID:-}" UTILS_ID="${NETHUNT_UTILS_FOLDER_ID:-}" python3 <<'PY'
import json, os

def load(name):
    try:
        return json.loads(os.environ.get(name) or '[]')
    except json.JSONDecodeError:
        return None

folders, writable = load('FOLDERS_JSON'), load('WRITABLE_JSON')
if folders is None:
    print('   could not parse the folder list; raw response above')
    raise SystemExit

def ident(f):
    # the exact key names are not documented, so accept the plausible ones
    for k in ('id', 'folderId', '_id'):
        if isinstance(f, dict) and f.get(k):
            return str(f[k])
    return '?'

def label(f):
    for k in ('name', 'title', 'folderName'):
        if isinstance(f, dict) and f.get(k):
            return str(f[k])
    return '(unnamed)'

if not folders:
    print('   none -- the tenant has no folders yet')
    raise SystemExit

wr = {ident(f) for f in (writable or [])}
cfg = {os.environ.get('RECORDS_ID'): 'NETHUNT_FOLDER_ID',
       os.environ.get('UTILS_ID'): 'NETHUNT_UTILS_FOLDER_ID'}

for f in folders:
    i = ident(f)
    marks = []
    if i in wr:
        marks.append('writable')
    if i in cfg and cfg[i]:
        marks.append(f'<- {cfg[i]}')
    print(f'   {i}  {label(f)}' + (f'   [{", ".join(marks)}]' if marks else ''))
PY

# --- 3. field schemas -------------------------------------------------------
# the contract: index.js builds its payload from exactly these names
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

    local raw code body
    raw=$(call "${BASE}/triggers/folder-field/${folder_id}")
    code=$(tail -1 <<< "$raw")
    body=$(sed '$d' <<< "$raw")

    if [[ "$code" != "200" ]]; then
        bad "folder-field returned HTTP $code for id '$folder_id'"
        [[ -n "$body" ]] && red "   response: $(head -c 200 <<< "$body")"
        return
    fi

    local out
    out=$(SCHEMA_JSON="$body" EXPECTED="$expected" python3 <<'PY'
import json, os, sys

try:
    schema = json.loads(os.environ['SCHEMA_JSON'])
except json.JSONDecodeError:
    print('RAW')
    print(os.environ['SCHEMA_JSON'][:500])
    sys.exit(0)

# field entries are objects in the documented shape, but tolerate bare strings
def name_of(f):
    if isinstance(f, str):
        return f
    for k in ('name', 'fieldName', 'title'):
        if isinstance(f, dict) and f.get(k):
            return str(f[k])
    return None

def type_of(f):
    if isinstance(f, dict):
        for k in ('type', 'fieldType', 'dataType'):
            if f.get(k):
                return str(f[k])
    return '?'

if isinstance(schema, dict):
    schema = schema.get('fields') or schema.get('folderFields') or []

actual = {name_of(f): type_of(f) for f in schema if name_of(f)}
if not actual:
    print('RAW')
    print(os.environ['SCHEMA_JSON'][:500])
    sys.exit(0)

expected = os.environ['EXPECTED'].split()
missing = [f for f in expected if f not in actual]
extra = [f for f in actual if f not in expected]

for f in expected:
    if f in actual:
        print(f'OK\t{f} ({actual[f]})')
for f in missing:
    print(f'MISSING\t{f}')
for f in sorted(extra):
    print(f'EXTRA\t{f} ({actual[f]})')
PY
    )

    if [[ "$(head -1 <<< "$out")" == "RAW" ]]; then
        bad "could not recognise the schema shape; raw response follows"
        sed '1d' <<< "$out" | sed 's/^/     /'
        return
    fi

    local missing=0
    while IFS=$'\t' read -r kind rest; do
        case "$kind" in
            OK)      ok "$rest" ;;
            MISSING) bad "$rest is missing"; missing=1 ;;
            # anything the team adds is theirs; the sync never sends it
            EXTRA)   note "extra: $rest — not sent by the sync, safe" ;;
        esac
    done <<< "$out"
    [[ $missing -eq 0 ]] && ok "all fields the sync sends are present"
}

check_fields "records folder" "${NETHUNT_FOLDER_ID:-}" "$RECORD_FIELDS"
check_fields "utils folder" "${NETHUNT_UTILS_FOLDER_ID:-}" "$UTILS_FIELDS"

# --- 4. probe ---------------------------------------------------------------
if [[ $RUN_PROBE -eq 1 ]]; then
    step "Probe (writes one record into the utils folder)"
    if [[ -z "${NETHUNT_UTILS_FOLDER_ID:-}" ]]; then
        bad "NETHUNT_UTILS_FOLDER_ID is not set; nothing safe to write to"
    else
        stamp=$(date -u +%Y-%m-%dT%H:%M:%S.000Z)
        created=$(call -X POST "${BASE}/actions/create-record/${NETHUNT_UTILS_FOLDER_ID}" \
            -H 'Content-Type: application/json' \
            -d "{\"fields\":{\"finishedAt\":\"${stamp}\",\"totalSynced\":0,\"duration\":0,\"createdRecords\":0,\"updatedRecords\":0,\"errorMessage\":\"probe from inspect-nethunt.sh\"},\"timeZone\":\"Europe/Warsaw\"}")
        ccode=$(tail -1 <<< "$created")
        cbody=$(sed '$d' <<< "$created")

        if [[ "$ccode" != "200" ]]; then
            bad "create-record returned HTTP $ccode"
            red "   response: $(head -c 300 <<< "$cbody")"
        else
            ok "record created ($(head -c 120 <<< "$cbody"))"

            # does new-record return the newest first, and did finishedAt survive?
            back=$(call "${BASE}/triggers/new-record/${NETHUNT_UTILS_FOLDER_ID}" | sed '$d')
            STAMP="$stamp" BACK_JSON="$back" python3 <<'PY'
import json, os
stamp = os.environ['STAMP']
try:
    data = json.loads(os.environ['BACK_JSON'])
except json.JSONDecodeError:
    print('   could not parse new-record response'); raise SystemExit
if not isinstance(data, list) or not data:
    print('   new-record returned nothing -- getLastSyncTime() would see no last sync')
    raise SystemExit
first = (data[0] or {}).get('fields', {})
got = str(first.get('finishedAt', ''))
print(f'   newest-first: {"yes" if got == stamp else "NO -- data[0] is not the record just written"}')
print(f'   finishedAt round trip: sent {stamp!r} got {got!r}')
if got != stamp:
    print('   getLastSyncTime() parses this with new Date(); a changed format shifts the sync window')
PY

            # The open question: index.js searches with Field=Value, but the
            # documented example uses Field:Value. Ask the API which it accepts.
            # plain variables rather than an associative array: bash 3.2 on macOS
            # has no declare -A, and this script should run from a laptop too
            count_hits() {
                local hits
                hits=$(call --get --data-urlencode "query=finishedAt${1}${stamp}" \
                       "${BASE}/searches/find-record/${NETHUNT_UTILS_FOLDER_ID}" | sed '$d')
                python3 -c "
import json,sys
try:
    d = json.loads(sys.argv[1]); print(len(d) if isinstance(d, list) else -1)
except Exception: print(-1)" "$hits"
            }

            found_eq=$(count_hits '=')
            note "query 'finishedAt=<value>' -> ${found_eq} result(s)"
            found_colon=$(count_hits ':')
            note "query 'finishedAt:<value>' -> ${found_colon} result(s)"

            if [[ "$found_eq" -gt 0 ]]; then
                ok "the '=' syntax works, which is what index.js:582 sends"
            elif [[ "$found_colon" -gt 0 ]]; then
                bad "only the ':' syntax matches; index.js:582 sends '=' and would find nothing"
                red "   Every sync would treat all users as new and create duplicates."
                red "   Fix searchNetHuntRecord() before the first production run."
            else
                bad "neither syntax returned the record just written"
                red "   Searching this folder does not work at all, so the sync cannot"
                red "   recognise existing records. Resolve before running against the shared folder."
            fi
            note "Delete the probe record from the utils folder when you are done."
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
