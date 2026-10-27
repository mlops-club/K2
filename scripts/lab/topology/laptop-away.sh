#!/usr/bin/env bash
# laptop-away (P3 T3, S11) on three real processes: a K2_ROLE=controller,
# laptop (K2_ROLE=host, K2_HOST_LABELS k2.kind=laptop) and host-1 (a
# server), each its own throwaway HOME, enrolled with W5 one-time tokens,
# k2-fake-agent as `claude`. K2_HOST_AWAY_AFTER is LA_AWAY_AFTER (3 s).
#
# The laptop's lid closes: SIGSTOP its daemon. Its link stays open (the
# kernel keeps the socket) but nothing comes from it. Before T3 the
# controller kept sending into it and counted the messages as sent until
# they failed no_ack.
#
# Checks:
#   1. notes, first placed on the laptop, is pinned to it (D5)
#   2. an idle laptop that is awake stays linked: its pongs are signs of
#      life (3 × K2_HOST_AWAY_AFTER with nothing to do, never away)
#   3. SIGSTOP the laptop: K2_HOST_AWAY_AFTER after it was last heard (its
#      last pong, before the stop) the controller closes its link; status
#      shows it away (silent, since then) and notes paused, still placed on it
#   4. k2 msg notes from host-1 (the up hop), with and without --wake:
#      accepted and held {agent notes, host laptop, since, why}; nothing is
#      sent (queued, 0 attempts) and nothing is placed on host-1
#   5. SIGCONT: the laptop links again still running notes (adopted, same
#      epoch); both messages are delivered once, on their first send
#
# Ports: LA_CTL_PORT (default 17489), LA_LT_PORT (17490), LA_H1_PORT (17491).
# Evidence: lab-runs/laptop-away/<UTC>/.

set -uo pipefail
REPO=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$REPO" || exit 1
BUILD=1
[ "${1:-}" = --no-build ] && BUILD=0
CTL_PORT=${LA_CTL_PORT:-17489}
LT_PORT=${LA_LT_PORT:-17490}
H1_PORT=${LA_H1_PORT:-17491}
AWAY_AFTER=${LA_AWAY_AFTER:-3}
for port in "$CTL_PORT" "$LT_PORT" "$H1_PORT"; do
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "laptop-away: port $port is taken ($(lsof -nP -iTCP:"$port" -sTCP:LISTEN | awk 'NR==2{print $1, $2}')); set LA_CTL_PORT/LA_LT_PORT/LA_H1_PORT" >&2
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
RUN="$REPO/lab-runs/laptop-away/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN/bin"
cp "$target/debug/k2-daemon" "$target/debug/k2-fake-agent" "$RUN/bin/" || exit 1
DAEMON="$RUN/bin/k2-daemon"
FAKE="$RUN/bin/k2-fake-agent"

# A short root: ptyd's socket path must fit in sun_path (104 bytes on macOS).
ROOT=$(mktemp -d "${LAB_SHORT_TMP:-/tmp}/k2la-XXXXXX") || exit 1
ROOT=$(cd "$ROOT" && pwd -P)
mkdir -p "$ROOT"/{ctl,lt,h1}/{home,tmp,run} "$ROOT/shim" "$ROOT/work-lt" "$ROOT/work-h1"
head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$ROOT/bootstrap-token"
"$FAKE" --install-shims "$ROOT/shim" || exit 1
CTL_PID="" LT_PID="" H1_PID=""
FT="$RUN/fake-agent.trace.jsonl"

down() {
    local pid i
    # A stopped laptop can't exit on SIGTERM.
    [ -n "$LT_PID" ] && kill -CONT "$LT_PID" 2>/dev/null
    for pid in "$LT_PID" "$H1_PID" "$CTL_PID"; do
        [ -n "$pid" ] || continue
        kill "$pid" 2>/dev/null || true
        for i in $(seq 1 25); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
        kill -9 "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
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
            echo "laptop-away: $1 exited during boot: $(tail -3 "$4" | tr '\n' ' ')" >&2
            return 1
        fi
        phase=$(curl -s --connect-timeout 1 "http://127.0.0.1:$3/boot-status" 2>/dev/null \
            | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("phase",""))' 2>/dev/null || true)
        [ "$phase" = ready ] && return 0
        sleep 0.2
    done
    echo "laptop-away: $1 never reached boot phase ready in 60 s (last: '$phase')" >&2
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
    echo "laptop-away: host on :$1 never linked (linked=$linked)" >&2
    return 1
}

start_ctl() {
    env -i PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
        HOME="$ROOT/ctl/home" K2_HOME="$ROOT/ctl/home/.k2" \
        TMPDIR="$ROOT/ctl/tmp" XDG_RUNTIME_DIR="$ROOT/ctl/run" K2_RUNTIME_DIR= \
        USER="$(id -un)" LANG=en_US.UTF-8 \
        K2_ROLE=controller K2_HOSTS=1 K2_HOST_BOOTSTRAP_TOKEN_FILE="$ROOT/bootstrap-token" K2_HOST_AUTH=enroll \
        K2_HOST_AWAY_AFTER="$AWAY_AFTER" \
        K2_DAEMON_PORT="$CTL_PORT" K2_TRACE_FILE="$RUN/controller.trace.jsonl" \
        "$DAEMON" >>"$RUN/controller.log" 2>&1 &
    CTL_PID=$!
    wait_ready controller "$CTL_PID" "$CTL_PORT" "$RUN/controller.log"
}

start_host() { # name dir port labels; prints the pid
    curl -s -X POST -H 'Content-Type: application/json' "http://127.0.0.1:$CTL_PORT/cli/hosts/enroll-tokens?token=$CTL_TOKEN" \
        -d '{"ttl_secs": 600}' | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["token"])' > "$ROOT/$2-token"
    env -i PATH="$ROOT/shim:/usr/bin:/bin:/usr/sbin:/sbin" \
        HOME="$ROOT/$2/home" K2_HOME="$ROOT/$2/home/.k2" \
        TMPDIR="$ROOT/$2/tmp" XDG_RUNTIME_DIR="$ROOT/$2/run" K2_RUNTIME_DIR= \
        USER="$(id -un)" LANG=en_US.UTF-8 \
        K2_ROLE=host K2_CONTROLLER_URL="ws://127.0.0.1:$CTL_PORT" \
        K2_HOST_AUTH=enroll K2_HOST_ENROLL_TOKEN_FILE="$ROOT/$2-token" K2_HOST_NAME="$1" \
        K2_HOST_LABELS="$4" K2_WORK_ROOT="$ROOT/work-$2" \
        K2_DAEMON_PORT="$3" K2_TRACE_FILE="$RUN/$1.trace.jsonl" \
        K2_TEST_AGENT_SHIM_DIR="$ROOT/shim" K2_FAKE_AGENT_TRACE="$FT" \
        K2_FAKE_AGENT_TURN_MS="${LAB_TURN_MS:-200}" K2_FAKE_AGENT_SEED="${LAB_SEED:-0}" \
        "$DAEMON" >>"$RUN/$1.log" 2>&1 &
    echo $!
}

ctl_get() { curl -s "http://127.0.0.1:$CTL_PORT$1?token=$CTL_TOKEN"; }

post_json() { # path json → HTTP code; the body in $RUN/post.out
    curl -s -o "$RUN/post.out" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        "http://127.0.0.1:$CTL_PORT$1?token=$CTL_TOKEN" -d "$2"
}

# The request `k2 msg` makes, from host-1 as alice: the up hop.
msg_from_h1() { # text wake → response JSON on stdout (and appended to msgs.jsonl)
    local out
    out=$(curl -s -X POST -H "Content-Type: application/x-www-form-urlencoded" \
        "http://127.0.0.1:$H1_PORT/cli/workspace/msg?token=$H1_TOKEN" \
        --data-urlencode "workspace=notes" --data-urlencode "text=$1" --data-urlencode "from=alice" --data-urlencode "wake=$2")
    echo "$out" >> "$RUN/msgs.jsonl"
    echo "$out"
}

outbox_row() { ctl_get /cli/hosts/outbox | python3 -I -c '
import json,sys
m=[r for r in json.load(sys.stdin)["messages"] if r["msg_id"]==sys.argv[1]]
print(json.dumps(m[0] if m else {}))' "$1"; }
field() { python3 -I -c '
import json,sys
v=json.loads(sys.stdin.read() or "{}")
for k in sys.argv[1].split("."):
    v=(v or {}).get(k) if isinstance(v, dict) else None
print("" if v is None else (json.dumps(v) if isinstance(v,(dict,list)) else v))' "$1"; }
status() { ctl_get /cli/hosts/status; }
away_of() { status | python3 -I -c '
import json,sys
a=[h for h in json.load(sys.stdin).get("away_hosts",[]) if h["host_id"]==sys.argv[1]]
print(json.dumps(a[0]) if a else "")' "$1"; }
is_away() { [ -n "$(away_of "$1")" ]; }
not_away() { [ -z "$(away_of "$1")" ]; }
placed_on() { grep -E "\[hosts\] placed agent $1 on host " "$RUN/controller.log" | tail -1 | sed -E 's/.* on host ([^ ]+) .*/\1/'; }
policy() { # agent field
    ctl_get /cli/hosts/placements | python3 -I -c '
import json,sys
a=[x for x in json.load(sys.stdin)["agents"] if x["agent"]==sys.argv[1]]
print((a[0].get(sys.argv[2]) or "") if a else "")' "$1" "$2"
}
traced_n() { grep -c "\"event\":\"$2\"" "$1" 2>/dev/null || true; }
# User entries in the laptop's transcripts that carry `text` (the fake
# agent's reply echoes the prompt).
prompts() { grep -rh '"role":"user"' "$ROOT/lt/home/.claude/projects" 2>/dev/null | grep -c "$1" || true; }

wait_for() { # seconds command…: until the command succeeds
    local n=$(( $1 * 10 )); shift
    for _ in $(seq 1 "$n"); do "$@" && return 0; sleep 0.1; done
    return 1
}

start_ctl || exit 1
wait_for 30 test -s "$ROOT/ctl/home/.k2/daemon.token" || { echo "laptop-away: no controller token" >&2; exit 1; }
CTL_TOKEN=$(cat "$ROOT/ctl/home/.k2/daemon.token")
LT_PID=$(start_host laptop lt "$LT_PORT" "k2.kind=laptop")
wait_ready laptop "$LT_PID" "$LT_PORT" "$RUN/laptop.log" && wait_linked "$LT_PORT" || exit 1

# 1. notes on the laptop (the only host yet), pinned to it
mkdir -p "$ROOT/work-lt/notes"
c=$(post_json /cli/sessions/v2/spawn "{\"agent_name\":\"notes\",\"cwd\":\"$ROOT/work-lt/notes\",\"command\":\"claude\",\"args\":[\"--session-id\",\"$(uuidgen | tr A-Z a-z)\"]}")
EPOCH=$(grep -E "\[hosts\] placed agent notes on host " "$RUN/controller.log" | tail -1 | sed -E 's/.* epoch ([0-9]+).*/\1/')
check "notes, first placed on the laptop, is pinned to it (D5)" \
    "$([ "$c" = 200 ] && [ "$(placed_on notes)" = laptop ] && [ "$(policy notes mode)" = pinned ] && [ "$(policy notes pinned_host)" = laptop ] && echo 1)" \
    "spawn HTTP $c; on $(placed_on notes) epoch $EPOCH; mode $(policy notes mode) $(policy notes pinned_host)"

H1_PID=$(start_host host-1 h1 "$H1_PORT" "k2.kind=server")
wait_ready host-1 "$H1_PID" "$H1_PORT" "$RUN/host-1.log" && wait_linked "$H1_PORT" || exit 1
H1_TOKEN=$(cat "$ROOT/h1/home/.k2/daemon.token")

# 2. awake and idle: never away
sleep $((AWAY_AFTER * 3))
idle_away=$(traced_n "$RUN/controller.trace.jsonl" ctl.host_away)
check "an idle laptop that is awake stays linked: its pongs are signs of life" \
    "$([ "$idle_away" = 0 ] && not_away laptop && echo 1)" \
    "idle $((AWAY_AFTER * 3)) s with K2_HOST_AWAY_AFTER=$AWAY_AFTER; ctl.host_away $idle_away; away $(status | field away)"

# 3. the lid closes
t_stop=$(python3 -I -c 'import time; print(int(time.time()*1000))')
kill -STOP "$LT_PID"
wait_for $((AWAY_AFTER * 4 + 20)) is_away laptop
t_away=$(python3 -I -c 'import time; print(int(time.time()*1000))')
AWAY=$(away_of laptop)
status > "$RUN/status-away.json"
why=$(echo "$AWAY" | field why); since=$(echo "$AWAY" | field since_ms)
paused=$(field paused < "$RUN/status-away.json")
paused_ok=$(python3 -I -c '
import json,sys
p=json.load(open(sys.argv[1]))["paused"]
print(1 if p==[{"agent":"notes","host":"laptop","epoch":int(sys.argv[2] or -1)}] else 0)' "$RUN/status-away.json" "$EPOCH")
check "SIGSTOP: after K2_HOST_AWAY_AFTER of silence the laptop is away (silent, since it was last heard) and notes is paused there" \
    "$([[ $why == "silent for "* ]] && [ -n "$since" ] && [ "$since" -le "$t_stop" ] && [ $((t_away - since)) -ge $((AWAY_AFTER * 1000)) ] && [ "$paused_ok" = 1 ] && [ "$(placed_on notes)" = laptop ] && echo 1)" \
    "away $AWAY; last heard $((t_stop - since)) ms before SIGSTOP, seen away $((t_away - t_stop)) ms after it; paused $paused"

# 4. messages from host-1: held, not sent, not placed elsewhere
A1=$(msg_from_h1 "lid closed one" false)
A2=$(msg_from_h1 "lid closed two" true)
M1=$(echo "$A1" | field msgId); M2=$(echo "$A2" | field msgId)
sleep $((AWAY_AFTER * 2))
R1=$(outbox_row "$M1"); R2=$(outbox_row "$M2")
echo "$R1" > "$RUN/outbox-held-1.json"; echo "$R2" > "$RUN/outbox-held-2.json"
held_ok() { # answer row
    [ "$(echo "$1" | field success)" = True ] && [ "$(echo "$1" | field held.host)" = laptop ] && [ "$(echo "$1" | field held.agent)" = notes ] \
        && [ -n "$(echo "$1" | field held.awaySinceMs)" ] && echo "$1" | field hint | grep -q '^held: notes is paused on host laptop' \
        && [ "$(echo "$2" | field state)" = queued ] && [ "$(echo "$2" | field attempts)" = 0 ] && [ "$(echo "$2" | field held.host)" = laptop ]
}
check "k2 msg notes from host-1 (with and without --wake): accepted and held for the laptop; nothing sent, nothing placed on host-1" \
    "$(held_ok "$A1" "$R1" && held_ok "$A2" "$R2" && [ "$(placed_on notes)" = laptop ] && [ "$(traced_n "$RUN/controller.trace.jsonl" ctl.msg_held)" -ge 2 ] && echo 1)" \
    "answers $(echo "$A1" | field held) / $(echo "$A2" | field delivery); rows $(echo "$R1" | field state)/$(echo "$R1" | field attempts) $(echo "$R2" | field state)/$(echo "$R2" | field attempts); notes on $(placed_on notes)"

# 5. the lid opens
kill -CONT "$LT_PID"
wait_for 60 not_away laptop
wait_for 60 sh -c "[ \"\$(curl -s 'http://127.0.0.1:$CTL_PORT/cli/hosts/outbox?token=$CTL_TOKEN&msg_id=$M2' | grep -c '\"state\":\"delivered\"')\" = 1 ] && [ \"\$(curl -s 'http://127.0.0.1:$CTL_PORT/cli/hosts/outbox?token=$CTL_TOKEN&msg_id=$M1' | grep -c '\"state\":\"delivered\"')\" = 1 ]"
R1=$(outbox_row "$M1"); R2=$(outbox_row "$M2")
echo "$R1" > "$RUN/outbox-final-1.json"; echo "$R2" > "$RUN/outbox-final-2.json"
adopted=$(grep -c "host laptop: agent notes Adopted" "$RUN/controller.log" || true)
p1=$(prompts "lid closed one"); p2=$(prompts "lid closed two")
acks=$(grep -cE "($M1|$M2) Delivered by host" "$RUN/controller.log" || true)
check "SIGCONT: the laptop links again still running notes (adopted, same epoch); both messages delivered once, on their first send" \
    "$([ "$adopted" -ge 1 ] && [ "$(placed_on notes)" = laptop ] && [ "$(echo "$R1" | field state)" = delivered ] && [ "$(echo "$R1" | field attempts)" = 1 ] && [ "$(echo "$R2" | field state)" = delivered ] && [ "$(echo "$R2" | field attempts)" = 1 ] && [ "$p1" = 1 ] && [ "$p2" = 1 ] && [ "$(status | field paused)" = "[]" ] && echo 1)" \
    "adopted $adopted; rows $(echo "$R1" | field state)/$(echo "$R1" | field attempts) $(echo "$R2" | field state)/$(echo "$R2" | field attempts); prompts $p1/$p2; delivered acks $acks; placements of notes $(grep -c 'placed agent notes on host' "$RUN/controller.log")"

grep -hE '\[hosts\] .*(away|linked|unlinked|Adopted|placed agent)|\[delivery\] .*held' "$RUN/controller.log" > "$RUN/presence.log" 2>/dev/null || true
rm -rf "$RUN/bin"
echo "evidence: $RUN"
if [ "$FAILS" = 0 ]; then echo "laptop-away: PASS"; else echo "laptop-away: $FAILS check(s) failed"; exit 1; fi
