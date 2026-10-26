#!/usr/bin/env bash
# P3 T2 phase A on three real processes: a K2_ROLE=controller (static
# provisioner), host-1 (a server) and laptop (K2_HOST_LABELS k2.kind=laptop),
# both with K2_HOST_MAX_AWAKE=2; k2-fake-agent as `claude`, throwaway HOMEs.
#
# Checks:
#   1. both hosts linked; status shows their kind and k2.max_awake
#   2. sticky wake: bob placed on host-1, sleeps (close), and wakes back on
#      host-1 although the laptop has more room (its home is on host-1)
#   3. notes, first placed on the laptop, is pinned to it (D5)
#   4. capacity: both hosts at k2.max_awake, a spawn is refused naming
#      K2_HOST_MAX_AWAKE
#   5. the laptop goes away (stopped): status lists it away; a wake of the
#      sleeping pinned notes is refused ("pinned … away") and notes is not
#      placed on host-1, which has room
#   6. the laptop comes back: notes is placed on it again
#
# Ports: PLACE_CTL_PORT (default 17450), PLACE_H1_PORT (17451),
# PLACE_LAPTOP_PORT (17452). Evidence: lab-runs/placement/<UTC>/.

set -uo pipefail
REPO=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$REPO" || exit 1
BUILD=1
[ "${1:-}" = --no-build ] && BUILD=0
CTL_PORT=${PLACE_CTL_PORT:-17450}
H1_PORT=${PLACE_H1_PORT:-17451}
LT_PORT=${PLACE_LAPTOP_PORT:-17452}

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
RUN="$REPO/lab-runs/placement/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN/bin"
cp "$target/debug/k2-daemon" "$target/debug/k2-fake-agent" "$RUN/bin/" || exit 1
DAEMON="$RUN/bin/k2-daemon"
FAKE="$RUN/bin/k2-fake-agent"

ROOT=$(mktemp -d "${TMPDIR:-/tmp}/k2lab-place-XXXXXX") || exit 1
mkdir -p "$ROOT"/{ctl,h1,lt}/{home,tmp,run} "$ROOT/shim" "$ROOT/work"
head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$ROOT/host-token"
"$FAKE" --install-shims "$ROOT/shim" || exit 1
CTL_PID="" H1_PID="" LT_PID=""

down() {
    local pid i
    for pid in "$H1_PID" "$LT_PID" "$CTL_PID"; do
        [ -n "$pid" ] || continue
        kill "$pid" 2>/dev/null || true
        for i in $(seq 1 25); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
        kill -9 "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    pkill -9 -f "$ROOT/" 2>/dev/null || true
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
            echo "three-process-placement: $1 exited during boot: $(tail -3 "$4" | tr '\n' ' ')" >&2
            return 1
        fi
        phase=$(curl -s --connect-timeout 1 "http://127.0.0.1:$3/boot-status" 2>/dev/null \
            | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("phase",""))' 2>/dev/null || true)
        [ "$phase" = ready ] && return 0
        sleep 0.2
    done
    echo "three-process-placement: $1 never reached boot phase ready in 60 s (last: '$phase')" >&2
    return 1
}

wait_linked() { # port
    local i linked=""
    for i in $(seq 1 150); do
        linked=$(curl -s --connect-timeout 1 "http://127.0.0.1:$1/cli/hosts/status" 2>/dev/null \
            | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("linked"))' 2>/dev/null || true)
        [ "$linked" = True ] && return 0
        sleep 0.2
    done
    echo "three-process-placement: host on :$1 never linked (linked=$linked)" >&2
    return 1
}

start_ctl() {
    env -i PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
        HOME="$ROOT/ctl/home" K2_HOME="$ROOT/ctl/home/.k2" \
        TMPDIR="$ROOT/ctl/tmp" XDG_RUNTIME_DIR="$ROOT/ctl/run" K2_RUNTIME_DIR= \
        USER="$(id -un)" LANG=en_US.UTF-8 \
        K2_ROLE=controller K2_HOSTS=1 K2_HOST_BOOTSTRAP_TOKEN_FILE="$ROOT/host-token" K2_PROVISIONER=static \
        K2_DAEMON_PORT="$CTL_PORT" K2_TRACE_FILE="$RUN/controller.trace.jsonl" \
        "$DAEMON" >>"$RUN/controller.log" 2>&1 &
    CTL_PID=$!
    wait_ready controller "$CTL_PID" "$CTL_PORT" "$RUN/controller.log"
}

start_host() { # name dir port labels; prints the pid
    env -i PATH="$ROOT/shim:/usr/bin:/bin:/usr/sbin:/sbin" \
        HOME="$ROOT/$2/home" K2_HOME="$ROOT/$2/home/.k2" \
        TMPDIR="$ROOT/$2/tmp" XDG_RUNTIME_DIR="$ROOT/$2/run" K2_RUNTIME_DIR= \
        USER="$(id -un)" LANG=en_US.UTF-8 \
        K2_ROLE=host K2_CONTROLLER_URL="ws://127.0.0.1:$CTL_PORT" \
        K2_HOST_ENROLL_TOKEN_FILE="$ROOT/host-token" K2_HOST_NAME="$1" \
        K2_HOST_LABELS="$4" K2_HOST_MAX_AWAKE=2 \
        K2_DAEMON_PORT="$3" K2_TRACE_FILE="$RUN/$1.trace.jsonl" \
        K2_TEST_AGENT_SHIM_DIR="$ROOT/shim" K2_FAKE_AGENT_TRACE="$RUN/fake-agent.trace.jsonl" \
        K2_FAKE_AGENT_TURN_MS="${LAB_TURN_MS:-200}" K2_FAKE_AGENT_SEED="${LAB_SEED:-0}" \
        "$DAEMON" >>"$RUN/$1.log" 2>&1 &
    echo $!
}

stop() { # pid
    kill "$1" 2>/dev/null || true
    for i in $(seq 1 25); do kill -0 "$1" 2>/dev/null || break; sleep 0.2; done
    kill -9 "$1" 2>/dev/null || true
    wait "$1" 2>/dev/null || true
}

ctl_get() { curl -s "http://127.0.0.1:$CTL_PORT$1?token=$CTL_TOKEN"; }

spawn() { # agent → HTTP code; the body in $RUN/post.out
    mkdir -p "$ROOT/work/$1"
    curl -s -o "$RUN/post.out" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        "http://127.0.0.1:$CTL_PORT/cli/sessions/v2/spawn?token=$CTL_TOKEN" \
        -d "{\"agent_name\":\"$1\",\"cwd\":\"$ROOT/work/$1\",\"command\":\"claude\",\"args\":[\"--session-id\",\"$(uuidgen | tr A-Z a-z)\"]}"
}

close() { # agent (a sleep: close the session, keep the index)
    curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        "http://127.0.0.1:$CTL_PORT/cli/sessions/v2/close?token=$CTL_TOKEN" -d "{\"agent_name\":\"$1\",\"force\":true}"
}

# The host an agent is placed on now (from /cli/sessions/v2 placements in the
# controller log's last "placed agent <a> on host <h>" line), and its policy.
placed_on() { grep -E "\[hosts\] placed agent $1 on host " "$RUN/controller.log" | tail -1 | sed -E 's/.* on host ([^ ]+) .*/\1/'; }
policy() { # agent field
    ctl_get /cli/hosts/placements | python3 -I -c '
import json,sys
a=[x for x in json.load(sys.stdin)["agents"] if x["agent"]==sys.argv[1]]
print((a[0].get(sys.argv[2]) or "") if a else "")' "$1" "$2"
}
status_field() { ctl_get /cli/hosts/status | python3 -I -c 'import json,sys; print(json.dumps(json.load(sys.stdin)[sys.argv[1]]))' "$1"; }

start_ctl || exit 1
CTL_TOKEN=$(cat "$ROOT/ctl/home/.k2/daemon.token")
H1_PID=$(start_host host-1 h1 "$H1_PORT" "pool=default")
wait_ready host-1 "$H1_PID" "$H1_PORT" "$RUN/host-1.log" && wait_linked "$H1_PORT" || exit 1
LT_PID=$(start_host laptop lt "$LT_PORT" "pool=default,k2.kind=laptop")
wait_ready laptop "$LT_PID" "$LT_PORT" "$RUN/laptop.log" && wait_linked "$LT_PORT" || exit 1

# 1. hosts, kinds, limits
ctl_get /cli/hosts/status > "$RUN/status-1.json"
ok=$(python3 -I -c '
import json,sys
s=json.load(open(sys.argv[1])); h={x["host_id"]:x for x in s["hosts"]}
print(1 if s.get("provisioner")=="static" and h["host-1"]["kind"]=="server" and h["laptop"]["kind"]=="laptop"
      and h["host-1"]["max_awake"]==2 and h["laptop"]["max_awake"]==2 and s["away"]==[] else 0)' "$RUN/status-1.json")
check "two hosts linked; kinds and k2.max_awake from their labels" "$ok" "$(cat "$RUN/status-1.json")"

# 2. sticky wake (and 3. the laptop pin)
c1=$(spawn bob); h_bob=$(placed_on bob)
c2=$(spawn notes); h_notes=$(placed_on notes)
check "notes, first placed on the laptop, is pinned to it (D5)" \
    "$([ "$c2" = 200 ] && [ "$h_notes" = laptop ] && [ "$(policy notes mode)" = pinned ] && [ "$(policy notes pinned_host)" = laptop ] && echo 1)" \
    "notes on $h_notes, mode $(policy notes mode) $(policy notes pinned_host)"
s1=$(close bob); s2=$(close notes)
c3=$(spawn carol)
c4=$(spawn bob); h_bob2=$(placed_on bob)
check "sticky wake: bob sleeps and wakes back on its last host, not the emptier one" \
    "$([ "$c1" = 200 ] && [ "$h_bob" = host-1 ] && [ "$s1" = 200 ] && [ "$c4" = 200 ] && [ "$h_bob2" = host-1 ] && [ "$(policy bob mode)" = portable ] && [ "$(policy bob last_host)" = host-1 ] && echo 1)" \
    "bob $h_bob -> asleep -> $h_bob2 (carol on $(placed_on carol); laptop emptier); policy $(policy bob mode) last $(policy bob last_host)"

# 4. capacity: host-1 has bob and carol (2/2); fill the laptop, then one more
spawn dave >/dev/null; spawn erin >/dev/null
c5=$(spawn frank); body=$(cat "$RUN/post.out")
check "every host at k2.max_awake: refused naming K2_HOST_MAX_AWAKE" \
    "$([ "$c5" = 503 ] && echo "$body" | grep -q K2_HOST_MAX_AWAKE && echo 1)" "HTTP $c5 $body"

# 5. the laptop goes away; make room on host-1 first
close carol >/dev/null
stop "$LT_PID"; LT_PID=""
for i in $(seq 1 100); do [ "$(status_field away)" = '["laptop"]' ] && break; sleep 0.1; done
away=$(status_field away)
c6=$(spawn notes); body=$(cat "$RUN/post.out")
check "laptop away: a wake of the pinned notes waits for it, never lands on host-1 (which has room)" \
    "$([ "$away" = '["laptop"]' ] && [ "$c6" = 503 ] && echo "$body" | grep -q 'pinned to host laptop' && [ "$(placed_on notes)" = laptop ] && echo 1)" \
    "away $away; HTTP $c6 $body; notes last placed on $(placed_on notes)"

# 6. the laptop comes back
LT_PID=$(start_host laptop lt "$LT_PORT" "pool=default,k2.kind=laptop")
wait_ready laptop "$LT_PID" "$LT_PORT" "$RUN/laptop.log" && wait_linked "$LT_PORT" || exit 1
for i in $(seq 1 100); do [ "$(status_field away)" = '[]' ] && break; sleep 0.1; done
c7=$(spawn notes)
check "the laptop is back: notes goes to it again" \
    "$([ "$c7" = 200 ] && [ "$(placed_on notes)" = laptop ] && [ "$(status_field away)" = '[]' ] && echo 1)" \
    "HTTP $c7; notes on $(placed_on notes); away $(status_field away)"

ctl_get /cli/hosts/placements > "$RUN/placements.json"
grep -E '\[hosts\] (placed agent|no host for|spawn .* refused|agent .*: last host)' "$RUN/controller.log" > "$RUN/controller-placement.log" || true
echo "evidence: $RUN"
if [ "$FAILS" = 0 ]; then echo "three-process-placement: PASS"; else echo "three-process-placement: $FAILS check(s) failed"; exit 1; fi
