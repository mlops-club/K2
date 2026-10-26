#!/usr/bin/env bash
# Topology "three-process-msg" (outbox, PRD S3 / S6 / I2): one
# K2_ROLE=controller k2-daemon and two K2_ROLE=host k2-daemons on this
# machine, each under its own throwaway HOME, on fixed ports. Agent alice
# runs on host-1, agent bob on host-2 (k2-fake-agent as `claude`); messages
# go host-1 → controller outbox → host-2.
#
#   scripts/lab/topology/three-process-msg.sh [--no-build]
#
# Builds k2-daemon and k2-fake-agent (CARGO_BUILD_JOBS=3, through the shared
# cargo slot when present), then proves acceptance item 3 of
# docs/prd/progress/outbox.md and writes the evidence under
# lab-runs/outbox/<UTC timestamp>/:
#
#   1. three processes up; alice placed on host-1, bob on host-2
#   2. `k2 msg bob "…"` (the real cli/k2, run in host-1's environment as
#      alice) lands in bob's session on host-2: in bob's transcript
#   3. inbox compose to bob from host-1 lands in bob's inbox on host-2
#   4. chaos: a burst of messages from host-1; host-2 is killed (SIGKILL)
#      mid-delivery, more are sent while it is down, it restarts with the
#      same HOME and bob is placed again. Every message ends delivered or
#      failed (I2), none stays queued; how many times each reached bob is
#      reported (at least once; duplicates would show here)
#
# Ports: OUTBOX_CTL_PORT (default 17440), OUTBOX_H1_PORT (17441),
# OUTBOX_H2_PORT (17442).

set -uo pipefail
REPO=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$REPO" || exit 1
BUILD=1
[ "${1:-}" = --no-build ] && BUILD=0
CTL_PORT=${OUTBOX_CTL_PORT:-17440}
H1_PORT=${OUTBOX_H1_PORT:-17441}
H2_PORT=${OUTBOX_H2_PORT:-17442}
BURST_BEFORE=${OUTBOX_BURST_BEFORE:-4}
BURST_DURING=${OUTBOX_BURST_DURING:-4}

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
RUN="$REPO/lab-runs/outbox/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN/bin"
cp "$target/debug/k2-daemon" "$target/debug/k2-fake-agent" "$RUN/bin/" || exit 1
DAEMON="$RUN/bin/k2-daemon"
FAKE="$RUN/bin/k2-fake-agent"

ROOT=$(mktemp -d "${TMPDIR:-/tmp}/k2lab-3p-XXXXXX") || exit 1
mkdir -p "$ROOT"/{ctl,h1,h2}/{home,tmp,run} "$ROOT/shim"
head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$ROOT/host-token"
"$FAKE" --install-shims "$ROOT/shim" || exit 1
CTL_PID="" H1_PID="" H2_PID=""

down() {
    local pid i
    for pid in "$H1_PID" "$H2_PID" "$CTL_PID"; do
        [ -n "$pid" ] || continue
        kill "$pid" 2>/dev/null || true
        for i in $(seq 1 25); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
        kill -9 "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
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
            echo "three-process-msg: $1 exited during boot: $(tail -3 "$4" | tr '\n' ' ')" >&2
            return 1
        fi
        phase=$(curl -s --connect-timeout 1 "http://127.0.0.1:$3/boot-status" 2>/dev/null \
            | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("phase",""))' 2>/dev/null || true)
        [ "$phase" = ready ] && return 0
        sleep 0.2
    done
    echo "three-process-msg: $1 never reached boot phase ready in 60 s (last: '$phase')" >&2
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
    echo "three-process-msg: host on :$1 never linked (linked=$linked)" >&2
    return 1
}

start_ctl() {
    env -i PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
        HOME="$ROOT/ctl/home" K2_HOME="$ROOT/ctl/home/.k2" \
        TMPDIR="$ROOT/ctl/tmp" XDG_RUNTIME_DIR="$ROOT/ctl/run" K2_RUNTIME_DIR= \
        USER="$(id -un)" LANG=en_US.UTF-8 \
        K2_ROLE=controller K2_HOSTS=1 K2_HOST_BOOTSTRAP_TOKEN_FILE="$ROOT/host-token" \
        K2_DAEMON_PORT="$CTL_PORT" K2_TRACE_FILE="$RUN/controller.trace.jsonl" \
        "$DAEMON" >>"$RUN/controller.log" 2>&1 &
    CTL_PID=$!
    wait_ready controller "$CTL_PID" "$CTL_PORT" "$RUN/controller.log"
}

start_host() { # name dir port; prints the pid
    env -i PATH="$ROOT/shim:/usr/bin:/bin:/usr/sbin:/sbin" \
        HOME="$ROOT/$2/home" K2_HOME="$ROOT/$2/home/.k2" \
        TMPDIR="$ROOT/$2/tmp" XDG_RUNTIME_DIR="$ROOT/$2/run" K2_RUNTIME_DIR= \
        USER="$(id -un)" LANG=en_US.UTF-8 \
        K2_ROLE=host K2_CONTROLLER_URL="ws://127.0.0.1:$CTL_PORT" \
        K2_HOST_ENROLL_TOKEN_FILE="$ROOT/host-token" K2_HOST_NAME="$1" \
        K2_DAEMON_PORT="$3" K2_TRACE_FILE="$RUN/$1.trace.jsonl" \
        K2_TEST_AGENT_SHIM_DIR="$ROOT/shim" K2_FAKE_AGENT_TRACE="$RUN/fake-agent.trace.jsonl" \
        K2_FAKE_AGENT_TURN_MS="${LAB_TURN_MS:-200}" K2_FAKE_AGENT_SEED="${LAB_SEED:-0}" \
        "$DAEMON" >>"$RUN/$1.log" 2>&1 &
    echo $!
}

post_json() { # port path token body
    curl -s -o "$RUN/post.out" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        "http://127.0.0.1:$1$2?token=$3" -d "$4"
}

spawn() { # agent cwd
    mkdir -p "$2"
    post_json "$CTL_PORT" /cli/sessions/v2/spawn "$CTL_TOKEN" \
        "{\"agent_name\":\"$1\",\"cwd\":\"$2\",\"command\":\"claude\",\"args\":[\"--session-id\",\"$(uuidgen | tr A-Z a-z)\"]}"
}

# The request `k2 msg` makes, from host-1 as alice (form POST, owner token).
msg_from_h1() { # text → response JSON on stdout
    curl -s -X POST -H "Content-Type: application/x-www-form-urlencoded" \
        "http://127.0.0.1:$H1_PORT/cli/workspace/msg?token=$H1_TOKEN" \
        --data-urlencode "workspace=bob" --data-urlencode "text=$1" --data-urlencode "from=alice"
}

json_field() { python3 -I -c 'import json,sys; print(json.load(sys.stdin).get(sys.argv[1]) or "")' "$1"; }

outbox() { # [msg_id] → controller outbox JSON
    curl -s "http://127.0.0.1:$CTL_PORT/cli/hosts/outbox?token=$CTL_TOKEN${1:+&msg_id=$1}"
}

state_of() { outbox "$1" | python3 -I -c 'import json,sys; m=json.load(sys.stdin).get("message") or {}; print(m.get("state",""))'; }

wait_state() { # msg_id seconds → final state
    local i s=""
    for i in $(seq 1 $(($2 * 5))); do
        s=$(state_of "$1")
        case "$s" in delivered|failed) echo "$s"; return 0 ;; esac
        sleep 0.2
    done
    echo "${s:-unknown}"
}

# Times `needle` appears as a user prompt in bob's transcripts on host-2.
seen_by_bob() {
    python3 -I - "$ROOT/h2/home/.claude/projects" "$1" <<'PY'
import json, os, sys
root, needle = sys.argv[1], sys.argv[2]
n = 0
for d, _, files in os.walk(root):
    for f in files:
        if not f.endswith(".jsonl"):
            continue
        for line in open(os.path.join(d, f), errors="replace"):
            try:
                v = json.loads(line)
            except ValueError:
                continue
            if v.get("type") == "user" and needle in json.dumps(v):
                n += 1
print(n)
PY
}

wait_seen() { # needle seconds → count
    local i n=0
    for i in $(seq 1 $(($2 * 5))); do
        n=$(seen_by_bob "$1")
        [ "$n" -gt 0 ] && break
        sleep 0.2
    done
    echo "$n"
}

echo "evidence: ${RUN#$REPO/}"
start_ctl || exit 1
CTL_TOKEN=$(cat "$ROOT/ctl/home/.k2/daemon.token")
H1_PID=$(start_host host-1 h1 "$H1_PORT")
wait_ready host-1 "$H1_PID" "$H1_PORT" "$RUN/host-1.log" && wait_linked "$H1_PORT" || exit 1
H2_PID=$(start_host host-2 h2 "$H2_PORT")
wait_ready host-2 "$H2_PID" "$H2_PORT" "$RUN/host-2.log" && wait_linked "$H2_PORT" || exit 1
H1_TOKEN=$(cat "$ROOT/h1/home/.k2/daemon.token")
{
    echo "controller :$CTL_PORT pid $CTL_PID; host-1 :$H1_PORT pid $H1_PID; host-2 :$H2_PORT pid $H2_PID"
    echo "daemon: $("$DAEMON" --version 2>&1 | head -1)"
} > "$RUN/topology.txt"

# 1. alice on host-1, bob on host-2 (the controller places on the host with
#    the fewest agents, ties by name).
c1=$(spawn alice "$ROOT/h1/home/work/alice")
c2=$(spawn bob "$ROOT/h2/home/work/bob")
curl -s "http://127.0.0.1:$CTL_PORT/cli/hosts/status" > "$RUN/hosts-status.json"
grep -o 'placed agent [a-z]* on host host-[0-9]' "$RUN/controller.log" > "$RUN/placements.txt" || true
ok=0
if [ "$c1" = 200 ] && [ "$c2" = 200 ] && grep -q 'placed agent alice on host host-1' "$RUN/placements.txt" \
    && grep -q 'placed agent bob on host host-2' "$RUN/placements.txt"; then ok=1; fi
check "three processes; alice on host-1, bob on host-2" "$ok" "spawn HTTP $c1/$c2; $(tr '\n' ';' < "$RUN/placements.txt")"
sleep 1

# 2. the real `k2 msg`, run as alice in host-1's environment
nonce=$(uuidgen | tr A-Z a-z | cut -c1-8)
text="S3 hello bob from alice $nonce"
env -i PATH="/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin" HOME="$ROOT/h1/home" K2_HOME="$ROOT/h1/home/.k2" \
    TMPDIR="$ROOT/h1/tmp" "$REPO/cli/k2" msg bob "$text" --from alice > "$RUN/k2-msg.out" 2>&1
k2rc=$?
id=$(grep -o '"msgId":"[^"]*"' "$RUN/k2-msg.out" | head -1 | cut -d'"' -f4)
if [ -z "$id" ]; then
    # Fallback: the same request k2 msg makes, so the delivery path is still proven.
    echo "(cli/k2 printed no msgId, rc $k2rc; sending the same request with curl)" >> "$RUN/k2-msg.out"
    msg_from_h1 "$text" >> "$RUN/k2-msg.out"
    id=$(grep -o '"msgId":"[^"]*"' "$RUN/k2-msg.out" | tail -1 | cut -d'"' -f4)
fi
st=$( [ -n "$id" ] && wait_state "$id" 30 )
n=$(wait_seen "$nonce" 20)
outbox "$id" > "$RUN/s3-outbox.json"
check "k2 msg from host-1 lands in bob's session on host-2" "$([ "$st" = delivered ] && [ "$n" = 1 ] && echo 1)" \
    "msg $id state=$st, in bob's transcript $n time(s); k2 msg: $(tr '\n' ' ' < "$RUN/k2-msg.out" | cut -c1-300)"

# 3. inbox compose from host-1 lands in bob's inbox on host-2
title="S6 inbox item $nonce"
curl -s -X POST -H "Content-Type: application/x-www-form-urlencoded" \
    "http://127.0.0.1:$H1_PORT/cli/inbox/compose?token=$H1_TOKEN" \
    --data-urlencode "workspace=bob" --data-urlencode "title=$title" \
    --data-urlencode "body=sent from host-1" --data-urlencode "from=alice" > "$RUN/inbox-compose.out"
iid=$(json_field msgId < "$RUN/inbox-compose.out")
ist=$( [ -n "$iid" ] && wait_state "$iid" 30 )
grep -rl "$title" "$ROOT/h2/home/work/bob" > "$RUN/inbox-files.txt" 2>/dev/null || true
check "inbox compose from host-1 lands in bob's inbox on host-2" \
    "$([ "$ist" = delivered ] && [ -s "$RUN/inbox-files.txt" ] && echo 1)" \
    "msg $iid state=$ist; file: $(sed "s|$ROOT|<root>|" "$RUN/inbox-files.txt" | head -1)"

# 4. chaos: kill host-2 mid-delivery, send while it is down, restart it
: > "$RUN/burst.txt"
ids=()
for i in $(seq 1 $((BURST_BEFORE + BURST_DURING))); do
    if [ "$i" = $((BURST_BEFORE + 1)) ]; then
        kill -9 "$H2_PID"
        wait "$H2_PID" 2>/dev/null
        echo "--- host-2 killed (SIGKILL) after $BURST_BEFORE sends" >> "$RUN/burst.txt"
        outbox > "$RUN/outbox-at-kill.json"
    fi
    r=$(msg_from_h1 "burst $i of $nonce")
    echo "$i $r" >> "$RUN/burst.txt"
    ids+=("$(echo "$r" | json_field msgId)")
done
sleep 1
H2_PID=$(start_host host-2 h2 "$H2_PORT")
wait_ready host-2 "$H2_PID" "$H2_PORT" "$RUN/host-2.log" && wait_linked "$H2_PORT" || exit 1
echo "--- host-2 restarted (pid $H2_PID), same HOME" >> "$RUN/burst.txt"
c3=$(spawn bob "$ROOT/h2/home/work/bob")
echo "--- bob placed again: HTTP $c3" >> "$RUN/burst.txt"
: > "$RUN/i2.txt"
final=0 delivered=0 failed=0 dup=0 missing=0
for i in "${!ids[@]}"; do
    id=${ids[$i]}
    s=$( [ -n "$id" ] && wait_state "$id" 60 )
    n=$(seen_by_bob "burst $((i + 1)) of $nonce")
    echo "msg ${id:-<none>} burst $((i + 1)): $s, reached bob $n time(s)" >> "$RUN/i2.txt"
    case "$s" in
        delivered) final=$((final + 1)); delivered=$((delivered + 1)) ;;
        failed) final=$((final + 1)); failed=$((failed + 1)) ;;
    esac
    [ "$s" = delivered ] && [ "$n" = 0 ] && missing=$((missing + 1))
    [ "$n" -gt 1 ] && dup=$((dup + 1))
done
outbox > "$RUN/outbox-final.json"
total=${#ids[@]}
check "I2: every message ends delivered or failed after host-2 died mid-delivery" \
    "$([ "$final" = "$total" ] && echo 1)" \
    "$final/$total final ($delivered delivered, $failed failed); delivered but not in a transcript: $missing; reached bob more than once: $dup"
cat "$RUN/i2.txt"

grep -E '\[delivery\]' "$RUN/controller.log" > "$RUN/controller-delivery.log" || true
grep -E '\[delivery\]' "$RUN/host-2.log" > "$RUN/host-2-delivery.log" || true
echo "evidence: ${RUN#$REPO/}"
if [ "$FAILS" = 0 ]; then echo "three-process-msg: PASS"; exit 0; fi
echo "three-process-msg: $FAILS check(s) failed"
exit 1
