#!/usr/bin/env bash
# laptop-pairing (P3 D7) on real processes: a K2_ROLE=controller and two
# K2_ROLE=host daemons, all K2_HOST_AUTH=enroll, each under its own
# throwaway HOME. A laptop pairs: the owner mints a laptop token, the laptop
# shows six digits, and the owner types them on the controller.
#
#   scripts/lab/topology/laptop-pairing.sh [--no-build]
#
# Checks:
#   1. the laptop, started with a laptop token (confirm, k2.kind=laptop),
#      waits: its /cli/hosts/status shows a six-digit pairing code, it is not
#      linked, and the controller lists it pending, without the code
#   2. a rogue host with its own confirm token: the owner types a wrong code
#      → refused (sas_mismatch), the token burned; the rogue's retries are
#      refused (token_invalid) and it never links
#   3. the owner confirms the laptop with the code it shows: it links; the
#      controller sees it as kind laptop (label k2.kind=laptop, from its
#      token: the laptop set no labels); its pairing code is gone
#   4. an agent first placed on it is pinned to it (D5)
#
# Ports: PAIR_CTL_PORT (default 17492), PAIR_LT_PORT (17493), PAIR_BAD_PORT (17494).
# Evidence: lab-runs/laptop-pairing/<UTC>/.

set -uo pipefail
REPO=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$REPO" || exit 1
BUILD=1
[ "${1:-}" = --no-build ] && BUILD=0
CTL_PORT=${PAIR_CTL_PORT:-17492}
LT_PORT=${PAIR_LT_PORT:-17493}
BAD_PORT=${PAIR_BAD_PORT:-17494}
for port in "$CTL_PORT" "$LT_PORT" "$BAD_PORT"; do
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "laptop-pairing: port $port is taken; set PAIR_CTL_PORT/PAIR_LT_PORT/PAIR_BAD_PORT" >&2
        exit 2
    fi
done

export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-3}
if ! command -v cargo >/dev/null 2>&1 && [ -x "$HOME/.cache/k2-dist-rust/cargo/bin/cargo" ]; then
    export RUSTUP_HOME=$HOME/.cache/k2-dist-rust/rustup CARGO_HOME=$HOME/.cache/k2-dist-rust/cargo
    export PATH=$CARGO_HOME/bin:$PATH
fi
if [ "$BUILD" = 1 ]; then
    slot=()
    [ -x "$HOME/.cache/k2-dist-cargo-slot.sh" ] && slot=("$HOME/.cache/k2-dist-cargo-slot.sh")
    ${slot[@]+"${slot[@]}"} cargo build -p k2-daemon --bin k2-daemon || exit 1
    ${slot[@]+"${slot[@]}"} cargo build -p k2-fake-agent || exit 1
fi
target=$(cargo metadata --format-version 1 --no-deps 2>/dev/null \
    | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
RUN="$REPO/lab-runs/laptop-pairing/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN/bin"
cp "$target/debug/k2-daemon" "$target/debug/k2-fake-agent" "$RUN/bin/" || exit 1
DAEMON="$RUN/bin/k2-daemon"
FAKE="$RUN/bin/k2-fake-agent"

# A short root: ptyd's socket path must fit in sun_path (104 bytes on macOS).
ROOT=$(mktemp -d "${LAB_SHORT_TMP:-/tmp}/k2pr-XXXXXX") || exit 1
ROOT=$(cd "$ROOT" && pwd -P)
mkdir -p "$ROOT"/{ctl,lt,bad}/{home,tmp,run} "$ROOT/shim" "$ROOT/work-lt"
head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$ROOT/bootstrap-token"
"$FAKE" --install-shims "$ROOT/shim" || exit 1
CTL_PID="" LT_PID="" BAD_PID=""

stop() { # pid
    local i
    [ -n "$1" ] || return 0
    kill "$1" 2>/dev/null || true
    for i in $(seq 1 25); do kill -0 "$1" 2>/dev/null || break; sleep 0.2; done
    kill -9 "$1" 2>/dev/null || true
    wait "$1" 2>/dev/null || true
}

down() {
    stop "$BAD_PID"; stop "$LT_PID"; stop "$CTL_PID"
    # Never with an empty ROOT: "/" would match every process on the machine.
    if [ -n "${ROOT:-}" ] && [ "$ROOT" != / ]; then
        pkill -9 -f "$ROOT/" 2>/dev/null || true
        rm -rf "$ROOT"
    fi
}
trap down EXIT

FAILS=0
check() { # name ok detail
    local line="PASS $1 -- $3"
    if [ "$2" != 1 ]; then line="FAIL $1 -- $3"; FAILS=$((FAILS + 1)); fi
    echo "$line" | tee -a "$RUN/checks.txt"
}

wait_ready() { # name pid port log
    local i phase=""
    for i in $(seq 1 300); do
        if ! kill -0 "$2" 2>/dev/null; then
            echo "laptop-pairing: $1 exited during boot: $(tail -3 "$4" | tr '\n' ' ')" >&2
            return 1
        fi
        phase=$(curl -s --connect-timeout 1 "http://127.0.0.1:$3/boot-status" 2>/dev/null \
            | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("phase",""))' 2>/dev/null || true)
        [ "$phase" = ready ] && return 0
        sleep 0.2
    done
    echo "laptop-pairing: $1 never reached boot phase ready in 60 s (last: '$phase')" >&2
    return 1
}

# A host's /cli/hosts/status once `test` (Python, on `s`) holds, within 60 s;
# the last status either way, saved as $RUN/<file>.
wait_status() { # port file test
    local i ok=""
    for i in $(seq 1 300); do
        curl -s --connect-timeout 1 "http://127.0.0.1:$1/cli/hosts/status" > "$RUN/$2" 2>/dev/null || true
        ok=$(python3 -I -c "import json,sys; s=json.load(open(sys.argv[1])); print(bool($3))" "$RUN/$2" 2>/dev/null || true)
        [ "$ok" = True ] && return 0
        sleep 0.2
    done
    return 1
}

field() { # file python-expr-on-s
    python3 -I -c "import json,sys; s=json.load(open(sys.argv[1])); print($2)" "$1" 2>/dev/null
}

start_ctl() {
    env -i PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
        HOME="$ROOT/ctl/home" K2_HOME="$ROOT/ctl/home/.k2" \
        TMPDIR="$ROOT/ctl/tmp" XDG_RUNTIME_DIR="$ROOT/ctl/run" K2_RUNTIME_DIR= \
        USER="$(id -un)" LANG=en_US.UTF-8 \
        K2_ROLE=controller K2_HOSTS=1 K2_HOST_BOOTSTRAP_TOKEN_FILE="$ROOT/bootstrap-token" \
        K2_HOST_AUTH=enroll \
        K2_DAEMON_PORT="$CTL_PORT" K2_TRACE_FILE="$RUN/controller.trace.jsonl" \
        "$DAEMON" >>"$RUN/controller.log" 2>&1 &
    CTL_PID=$!
    wait_ready controller "$CTL_PID" "$CTL_PORT" "$RUN/controller.log"
}

start_host() { # name dir port token-file; prints the pid. No K2_HOST_LABELS: the kind comes from the token.
    env -i PATH="$ROOT/shim:/usr/bin:/bin:/usr/sbin:/sbin" \
        HOME="$ROOT/$2/home" K2_HOME="$ROOT/$2/home/.k2" \
        TMPDIR="$ROOT/$2/tmp" XDG_RUNTIME_DIR="$ROOT/$2/run" K2_RUNTIME_DIR= \
        USER="$(id -un)" LANG=en_US.UTF-8 \
        K2_ROLE=host K2_CONTROLLER_URL="ws://127.0.0.1:$CTL_PORT" \
        K2_HOST_AUTH=enroll K2_HOST_ENROLL_TOKEN_FILE="$4" K2_HOST_NAME="$1" \
        K2_DAEMON_PORT="$3" K2_TRACE_FILE="$RUN/$1.trace.jsonl" \
        K2_TEST_AGENT_SHIM_DIR="$ROOT/shim" K2_FAKE_AGENT_TRACE="$RUN/fake-agent.trace.jsonl" \
        K2_FAKE_AGENT_TURN_MS="${LAB_TURN_MS:-200}" K2_FAKE_AGENT_SEED="${LAB_SEED:-0}" \
        "$DAEMON" >>"$RUN/$1.log" 2>&1 &
    echo $!
}

owner_post() { # path body → response body; HTTP code in $RUN/http.code
    curl -s -o "$RUN/post.out" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        "http://127.0.0.1:$CTL_PORT$1?token=$CTL_TOKEN" -d "$2" > "$RUN/http.code"
    cat "$RUN/post.out"
}
owner_get() { curl -s "http://127.0.0.1:$CTL_PORT$1?token=$CTL_TOKEN"; }
mint() { # body → token into $ROOT/<file>
    owner_post /cli/hosts/enroll-tokens "$1" > "$RUN/minted-$2.json"
    field "$RUN/minted-$2.json" 's["token"]' > "$ROOT/$2-token"
}

echo "evidence: ${RUN#$REPO/}"
start_ctl || exit 1
CTL_TOKEN=$(cat "$ROOT/ctl/home/.k2/daemon.token")

# 1. The laptop waits, showing its code.
mint '{"ttl_secs": 600, "laptop": true}' lt
LT_PID=$(start_host laptop lt "$LT_PORT" "$ROOT/lt-token")
wait_ready laptop "$LT_PID" "$LT_PORT" "$RUN/laptop.log" || exit 1
wait_status "$LT_PORT" lt-waiting.json 's.get("pairing_code")'
CODE=$(field "$RUN/lt-waiting.json" 's["pairing_code"]')
owner_get /cli/hosts/enroll/pending > "$RUN/pending.json"
pending_ok=$(python3 -I -c '
import json,sys
p=json.load(open(sys.argv[1]))["pending"]; code=sys.argv[2].replace(" ","")
print(1 if [x["host_name"] for x in p]==["laptop"] and code not in json.dumps(p) else 0)' "$RUN/pending.json" "$CODE")
check "the laptop waits for the owner, showing a six-digit code; the controller lists it pending, without the code" \
    "$([[ $CODE =~ ^[0-9]{3}\ [0-9]{3}$ ]] && [ "$(field "$RUN/lt-waiting.json" 's.get("linked")')" = False ] && [ "$(field "$RUN/minted-lt.json" 's["confirm"]')" = True ] && [ "$pending_ok" = 1 ] && echo 1)" \
    "code $CODE; linked $(field "$RUN/lt-waiting.json" 's.get("linked")'); last_error $(field "$RUN/lt-waiting.json" 's.get("last_error")' | head -c 120); pending $(head -c 200 "$RUN/pending.json")"

# 2. A rogue host: the owner types a wrong code.
mint '{"ttl_secs": 600, "confirm": true}' bad
BAD_PID=$(start_host rogue bad "$BAD_PORT" "$ROOT/bad-token")
wait_ready rogue "$BAD_PID" "$BAD_PORT" "$RUN/rogue.log" || exit 1
wait_status "$BAD_PORT" bad-waiting.json 's.get("pairing_code")'
BAD_CODE=$(field "$RUN/bad-waiting.json" 's["pairing_code"]' | tr -d ' ')
WRONG=$(printf '%06d' $(( (10#$BAD_CODE + 1) % 1000000 )))
owner_post /cli/hosts/enroll/confirm "{\"host_name\": \"rogue\", \"code\": \"$WRONG\"}" > "$RUN/confirm-wrong.json"
wrong_http=$(cat "$RUN/http.code")
wait_status "$BAD_PORT" bad-refused.json '"token_invalid" in (s.get("last_error") or "")'
sleep 2
check "a wrong code refuses the rogue and burns its token: its retries are refused, it never links" \
    "$([ "$wrong_http" = 403 ] && grep -q sas_mismatch "$RUN/confirm-wrong.json" && [ "$(field "$RUN/bad-refused.json" 's.get("linked")')" = False ] && grep -q 'token_invalid' "$RUN/bad-refused.json" && ! grep -q '"rogue"' <(owner_get /cli/hosts/status) && echo 1)" \
    "confirm HTTP $wrong_http $(head -c 160 "$RUN/confirm-wrong.json"); rogue linked $(field "$RUN/bad-refused.json" 's.get("linked")'); last_error $(field "$RUN/bad-refused.json" 's.get("last_error")' | head -c 120)"
stop "$BAD_PID"; BAD_PID=""

# 3. The owner types the laptop's code: it links, as a laptop.
owner_post /cli/hosts/enroll/confirm "{\"host_name\": \"laptop\", \"code\": \"$CODE\"}" > "$RUN/confirm.json"
confirm_http=$(cat "$RUN/http.code")
wait_status "$LT_PORT" lt-linked.json 's.get("linked") is True'
owner_get /cli/hosts/status > "$RUN/ctl-status.json"
kind_ok=$(python3 -I -c '
import json,sys
h=[x for x in json.load(open(sys.argv[1]))["hosts"] if x["host_id"]=="laptop"]
print(1 if h and h[0]["kind"]=="laptop" and h[0]["labels"]==["k2.kind=laptop"] else 0)' "$RUN/ctl-status.json")
check "the owner confirms the laptop with the code it shows: it links, as kind laptop from its token" \
    "$([ "$confirm_http" = 200 ] && [ "$(field "$RUN/lt-linked.json" 's.get("linked")')" = True ] && [ "$(field "$RUN/lt-linked.json" 's.get("pairing_code")')" = None ] && [ "$kind_ok" = 1 ] && echo 1)" \
    "confirm HTTP $confirm_http; linked $(field "$RUN/lt-linked.json" 's.get("linked")'); hosts $(field "$RUN/ctl-status.json" 'json.dumps(s["hosts"])' | head -c 200)"

# 4. An agent first placed on it is pinned to it.
mkdir -p "$ROOT/work-lt/notes"
c=$(curl -s -o "$RUN/spawn.json" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
    "http://127.0.0.1:$CTL_PORT/cli/sessions/v2/spawn?token=$CTL_TOKEN" \
    -d "{\"agent_name\":\"notes\",\"cwd\":\"$ROOT/work-lt/notes\",\"command\":\"claude\",\"args\":[\"--session-id\",\"$(uuidgen | tr A-Z a-z)\"]}")
owner_get /cli/hosts/placements > "$RUN/placements.json"
pinned=$(python3 -I -c '
import json,sys
a=[x for x in json.load(open(sys.argv[1]))["agents"] if x["agent"]=="notes"]
print("%s %s" % (a[0].get("mode"), a[0].get("pinned_host")) if a else "none")' "$RUN/placements.json")
check "an agent first placed on the paired laptop is pinned to it (D5)" \
    "$([ "$c" = 200 ] && [ "$pinned" = "pinned laptop" ] && echo 1)" \
    "spawn HTTP $c; notes $pinned"

grep -hE '\[hosts/enroll\]' "$RUN/controller.log" "$RUN/laptop.log" "$RUN/rogue.log" > "$RUN/enroll.log" 2>/dev/null || true
rm -rf "$RUN/bin"
echo "evidence: $RUN"
if [ "$FAILS" = 0 ]; then echo "laptop-pairing: PASS"; else echo "laptop-pairing: $FAILS check(s) failed"; exit 1; fi
