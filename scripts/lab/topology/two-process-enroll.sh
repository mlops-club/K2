#!/usr/bin/env bash
# Topology "two-process-enroll" (host-enroll, PRD P2 / FR2): a
# K2_ROLE=controller k2-daemon and a K2_ROLE=host k2-daemon on this machine,
# both with K2_HOST_AUTH=enroll, each under its own throwaway HOME, on fixed
# ports. Two short-lived impostor hosts try to get in.
#
#   scripts/lab/topology/two-process-enroll.sh [--no-build]
#
# Builds k2-daemon and k2-fake-agent (CARGO_BUILD_JOBS=3, through the shared
# cargo slot when present), then proves the two-process acceptance item of
# docs/prd/progress/host-enroll.md and writes the evidence under
# lab-runs/host-enroll/<UTC timestamp>/:
#
#   1. the owner mints a one-time token; host-1 enrolls with it (its key
#      file is made 0600), links with a signed hello, and an agent spawned
#      through the controller runs on it
#   2. a host with only the stolen bootstrap token (K2_HOST_AUTH=token) is
#      refused: enrollment_required
#   3. a new key with the spent one-time token is refused: token_invalid
#   4. host-1 restarts with its token file overwritten: the key alone links it
#   5. the owner revokes host-1: its live link is cut and every retry is
#      refused (revoked_host_key)
#
# Ports: ENROLL_CTL_PORT (default 17450), ENROLL_H1_PORT (17451),
# ENROLL_BAD_PORT (17452, the impostors).

set -uo pipefail
REPO=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$REPO" || exit 1
BUILD=1
[ "${1:-}" = --no-build ] && BUILD=0
CTL_PORT=${ENROLL_CTL_PORT:-17450}
H1_PORT=${ENROLL_H1_PORT:-17451}
BAD_PORT=${ENROLL_BAD_PORT:-17452}

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
RUN="$REPO/lab-runs/host-enroll/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN/bin"
cp "$target/debug/k2-daemon" "$target/debug/k2-fake-agent" "$RUN/bin/" || exit 1
DAEMON="$RUN/bin/k2-daemon"
FAKE="$RUN/bin/k2-fake-agent"

ROOT=$(mktemp -d "${TMPDIR:-/tmp}/k2lab-enroll-XXXXXX") || exit 1
mkdir -p "$ROOT"/{ctl,h1,bad1,bad2}/{home,tmp,run} "$ROOT/shim"
head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$ROOT/bootstrap-token"
"$FAKE" --install-shims "$ROOT/shim" || exit 1
CTL_PID="" H1_PID="" BAD_PID=""

stop() { # pid
    local i
    [ -n "$1" ] || return 0
    kill "$1" 2>/dev/null || true
    for i in $(seq 1 25); do kill -0 "$1" 2>/dev/null || break; sleep 0.2; done
    kill -9 "$1" 2>/dev/null || true
    wait "$1" 2>/dev/null || true
}

down() {
    stop "$BAD_PID"; stop "$H1_PID"; stop "$CTL_PID"
    [ -n "${ROOT:-}" ] && pkill -9 -f "$ROOT/" 2>/dev/null || true
    rm -rf "$ROOT"
}
trap down EXIT

FAILS=0
check() { # name ok detail
    local line="PASS $1 $3"
    if [ "$2" != 1 ]; then line="FAIL $1 $3"; FAILS=$((FAILS + 1)); fi
    echo "$line" | tee -a "$RUN/checks.txt"
}

wait_ready() { # name pid port log
    local i phase=""
    for i in $(seq 1 300); do
        if ! kill -0 "$2" 2>/dev/null; then
            echo "two-process-enroll: $1 exited during boot: $(tail -3 "$4" | tr '\n' ' ')" >&2
            return 1
        fi
        phase=$(curl -s --connect-timeout 1 "http://127.0.0.1:$3/boot-status" 2>/dev/null \
            | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("phase",""))' 2>/dev/null || true)
        [ "$phase" = ready ] && return 0
        sleep 0.2
    done
    echo "two-process-enroll: $1 never reached boot phase ready in 60 s (last: '$phase')" >&2
    return 1
}

# A host's /cli/hosts/status once `test` (Python, on `s`) holds, within 30 s;
# the last status either way, saved as $RUN/<file>.
wait_status() { # port file test
    local i ok=""
    for i in $(seq 1 150); do
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

start_host() { # name dir port auth token-file; prints the pid
    env -i PATH="$ROOT/shim:/usr/bin:/bin:/usr/sbin:/sbin" \
        HOME="$ROOT/$2/home" K2_HOME="$ROOT/$2/home/.k2" \
        TMPDIR="$ROOT/$2/tmp" XDG_RUNTIME_DIR="$ROOT/$2/run" K2_RUNTIME_DIR= \
        USER="$(id -un)" LANG=en_US.UTF-8 \
        K2_ROLE=host K2_CONTROLLER_URL="ws://127.0.0.1:$CTL_PORT" \
        K2_HOST_AUTH="$4" K2_HOST_ENROLL_TOKEN_FILE="$5" K2_HOST_NAME="$1" \
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

echo "evidence: ${RUN#$REPO/}"
start_ctl || exit 1
CTL_TOKEN=$(cat "$ROOT/ctl/home/.k2/daemon.token")
echo "controller :$CTL_PORT pid $CTL_PID (K2_HOST_AUTH=enroll)" > "$RUN/topology.txt"

# 1. Mint, enroll, link signed, run an agent there.
owner_post /cli/hosts/enroll-tokens '{"ttl_secs": 600}' > "$RUN/minted.json"
python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["token"])' "$RUN/minted.json" > "$ROOT/h1-token" 2>/dev/null
H1_PID=$(start_host host-1 h1 "$H1_PORT" enroll "$ROOT/h1-token")
echo "host-1 :$H1_PORT pid $H1_PID (K2_HOST_AUTH=enroll)" >> "$RUN/topology.txt"
wait_ready host-1 "$H1_PID" "$H1_PORT" "$RUN/host-1.log" || exit 1
wait_status "$H1_PORT" h1-status.json 's.get("linked") is True'
linked=$(field "$RUN/h1-status.json" 's.get("linked")')
curl -s "http://127.0.0.1:$CTL_PORT/cli/hosts/keys?token=$CTL_TOKEN" > "$RUN/keys.json"
fp=$(field "$RUN/keys.json" '[k["fingerprint"] for k in s["keys"] if k["host_name"]=="host-1" and not k["revoked_ms"]][0]')
keymode=$(stat -f '%Lp' "$ROOT/h1/home/.k2/host-key.pem" 2>/dev/null || echo missing)
mkdir -p "$ROOT/h1/home/work/agent"
spawn=$(curl -s -o "$RUN/spawn.out" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
    "http://127.0.0.1:$CTL_PORT/cli/sessions/v2/spawn?token=$CTL_TOKEN" \
    -d "{\"agent_name\":\"agent-1\",\"cwd\":\"$ROOT/h1/home/work/agent\",\"command\":\"claude\",\"args\":[\"--session-id\",\"$(uuidgen | tr A-Z a-z)\"]}")
placed=$(grep -c 'placed agent agent-1 on host host-1' "$RUN/controller.log")
ok=0
if [ "$linked" = True ] && [ -n "$fp" ] && [ "$keymode" = 600 ] && [ "$spawn" = 200 ] && [ "$placed" -ge 1 ] \
    && grep -q '"ctl.host_enrolled"' "$RUN/controller.trace.jsonl" && grep -q '"ctl.host_admitted"' "$RUN/controller.trace.jsonl"; then ok=1; fi
check "enroll with a one-time token, signed hello, agent runs on the host" "$ok" \
    "linked=$linked key ${fp:0:16}… mode $keymode; spawn HTTP $spawn; placed=$placed"

# 2. The stolen bootstrap token alone.
BAD_PID=$(start_host impostor-a bad1 "$BAD_PORT" token "$ROOT/bootstrap-token")
wait_ready impostor-a "$BAD_PID" "$BAD_PORT" "$RUN/impostor-a.log" || exit 1
wait_status "$BAD_PORT" impostor-a-status.json '"enrollment_required" in (s.get("last_error") or "")'
err=$(field "$RUN/impostor-a-status.json" 's.get("last_error")')
stop "$BAD_PID"; BAD_PID=""
ok=0
case "$err" in *"refused: enrollment_required"*) ok=1 ;; esac
check "bootstrap token alone is refused" "$ok" "impostor-a: $err"

# 3. A new key with the spent one-time token.
BAD_PID=$(start_host impostor-b bad2 "$BAD_PORT" enroll "$ROOT/h1-token")
wait_ready impostor-b "$BAD_PID" "$BAD_PORT" "$RUN/impostor-b.log" || exit 1
wait_status "$BAD_PORT" impostor-b-status.json '"token_invalid" in (s.get("last_error") or "")'
err=$(field "$RUN/impostor-b-status.json" 's.get("last_error")')
stop "$BAD_PID"; BAD_PID=""
curl -s "http://127.0.0.1:$CTL_PORT/cli/hosts/status" > "$RUN/ctl-status.json"
hosts=$(field "$RUN/ctl-status.json" '",".join(sorted(h.get("host_id","") for h in s.get("hosts",[])))')
ok=0
case "$err" in *"enroll refused: token_invalid"*) [ "$hosts" = host-1 ] && ok=1 ;; esac
check "spent one-time token enrolls nothing" "$ok" "impostor-b: $err; controller hosts: $hosts"

# 4. Restart host-1 with junk in its token file: the key is the credential.
stop "$H1_PID"
echo "not-a-token" > "$ROOT/h1-token"
H1_PID=$(start_host host-1 h1 "$H1_PORT" enroll "$ROOT/h1-token")
wait_ready host-1 "$H1_PID" "$H1_PORT" "$RUN/host-1.log" || exit 1
wait_status "$H1_PORT" h1-restart-status.json 's.get("linked") is True'
linked=$(field "$RUN/h1-restart-status.json" 's.get("linked")')
check "restarted host links on its key alone" "$([ "$linked" = True ] && echo 1)" "linked=$linked"

# 5. Revoke.
owner_post /cli/hosts/keys/revoke '{"host_name":"host-1","reason":"lab revoke"}' > "$RUN/revoke.json"
disc=$(field "$RUN/revoke.json" 's.get("disconnected")')
wait_status "$H1_PORT" h1-revoked-status.json \
    's.get("linked") is False and "revoked_host_key" in (s.get("last_error") or "") and s.get("links", 0) >= 1'
sleep 2 # a retry or two after the cut
curl -s "http://127.0.0.1:$H1_PORT/cli/hosts/status" > "$RUN/h1-revoked-status.json"
linked=$(field "$RUN/h1-revoked-status.json" 's.get("linked")')
err=$(field "$RUN/h1-revoked-status.json" 's.get("last_error")')
cut=$(grep -c 'refused: revoked_host_key' "$RUN/host-1.log")
ok=0
if [ "$disc" = True ] && [ "$linked" = False ] && [ "$cut" -ge 1 ]; then
    case "$err" in *revoked_host_key*) ok=1 ;; esac
fi
check "revoked host is cut off and refused" "$ok" "disconnected=$disc linked=$linked; last_error: $err"

grep -h '"host\.enrolled"\|"ctl\.host_' "$RUN"/*.trace.jsonl > "$RUN/enroll-trace.jsonl" 2>/dev/null || true
total=$(wc -l < "$RUN/checks.txt" | tr -d ' ')
if [ "$FAILS" = 0 ]; then echo "two-process-enroll: PASS $total/$total ($RUN)"; else echo "two-process-enroll: FAIL $FAILS of $total ($RUN)"; fi
[ "$FAILS" = 0 ]
