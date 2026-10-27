#!/usr/bin/env bash
# ptyd S3 on real processes (docs/prd/progress/ptyd-s3.md): a
# K2_ROLE=controller and host-1 (K2_ROLE=host, K2_PTYD=1: agent CLIs run
# under k2-ptyd), each under its own throwaway HOME, k2-fake-agent as
# `claude`. host-1's daemon is killed with SIGKILL mid-turn (its CLI is
# ptyd's child, not the daemon's) and started again with the same HOME: it
# adopts the CLI ptyd kept, and the controller adopts the placement.
#
# The agent is a registered workspace's chat, driven by
# `POST /cli/workspace/msg` (the outbox), as a user or another agent would.
#
# Checks:
#   1. notes wakes on host-1 and answers; its CLI runs under ptyd
#   2. a long turn starts on a message; host-1's daemon is SIGKILLed
#      mid-turn and the CLI outlives it
#   3. host-1 restarts with the same HOME and adopts the CLI (host.adopt);
#      the controller adopts the placement at the same epoch, with no `gone`
#   4. the CLI survives: the same pid, never exited, never exec'd again
#   5. the turn's message is not failed: delivered, sent once
#   6. the agent answers it: the turn ends, the reply is in the transcript
#      and on the adopted session's screen, which also shows the output from
#      before the restart (the ring replayed)
#   7. a message after the restart is delivered once and answered
#
# Ports: RA_CTL_PORT (default 17491), RA_H1_PORT (17492); the script stops
# if one is taken. Evidence: lab-runs/host-restart-adopt/<UTC>/.

set -uo pipefail
REPO=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$REPO" || exit 1
BUILD=1
[ "${1:-}" = --no-build ] && BUILD=0
CTL_PORT=${RA_CTL_PORT:-17491}
H1_PORT=${RA_H1_PORT:-17492}
LONG_MS=${RA_LONG_TURN_MS:-8000}
for port in "$CTL_PORT" "$H1_PORT"; do
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "host-restart-adopt: port $port is taken; set RA_CTL_PORT/RA_H1_PORT" >&2
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
    ${slot[@]+"${slot[@]}"} cargo build -p k2-ptyd --bin k2-ptyd || exit 1
    ${slot[@]+"${slot[@]}"} cargo build -p k2-fake-agent || exit 1
fi
target=$(cargo metadata --format-version 1 --no-deps 2>/dev/null \
    | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
RUN="$REPO/lab-runs/host-restart-adopt/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN/bin"
cp "$target/debug/k2-daemon" "$target/debug/k2-ptyd" "$target/debug/k2-fake-agent" "$RUN/bin/" || exit 1
DAEMON="$RUN/bin/k2-daemon"
FAKE="$RUN/bin/k2-fake-agent"

# A short root: ptyd's socket path must fit in sun_path (104 bytes on macOS).
ROOT=$(mktemp -d "${LAB_SHORT_TMP:-/tmp}/k2ra-XXXXXX") || exit 1
ROOT=$(cd "$ROOT" && pwd -P)
mkdir -p "$ROOT"/{ctl,h1}/{home,tmp,run} "$ROOT/shim" "$ROOT/work-h1"
head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$ROOT/bootstrap-token"
"$FAKE" --install-shims "$ROOT/shim" || exit 1
CTL_PID="" H1_PID=""
WS="$ROOT/work-h1/notes"
FT="$RUN/fake-agent.trace.jsonl"

down() {
    local pid i
    for pid in "$H1_PID" "$CTL_PID"; do
        [ -n "$pid" ] || continue
        kill "$pid" 2>/dev/null || true
        for i in $(seq 1 25); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
        kill -9 "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    # ptyd runs from a copy under host-1's HOME, its CLIs from the shim dir.
    [ -n "${ROOT:-}" ] && pkill -9 -f "$ROOT/" 2>/dev/null || true
    [ -n "${CLI_PID:-}" ] && kill -9 "$CLI_PID" 2>/dev/null || true
    rm -rf "$ROOT"
}
trap down EXIT

FAILS=0
check() { # name ok detail
    local line="PASS $1 -- $3"
    if [ "$2" != 1 ]; then line="FAIL $1 -- $3"; FAILS=$((FAILS + 1)); fi
    echo "$line" | tee -a "$RUN/checks.txt"
}

wait_for() { # seconds command…: until the command succeeds
    local n=$(( $1 * 10 )); shift
    for _ in $(seq 1 "$n"); do "$@" && return 0; sleep 0.1; done
    return 1
}

wait_ready() { # name pid port log
    local i phase=""
    for i in $(seq 1 300); do
        if ! kill -0 "$2" 2>/dev/null; then
            echo "host-restart-adopt: $1 exited during boot: $(tail -3 "$4" | tr '\n' ' ')" >&2
            return 1
        fi
        phase=$(curl -s --connect-timeout 1 "http://127.0.0.1:$3/boot-status" 2>/dev/null \
            | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("phase",""))' 2>/dev/null || true)
        [ "$phase" = ready ] && return 0
        sleep 0.2
    done
    echo "host-restart-adopt: $1 never reached boot phase ready in 60 s (last: '$phase')" >&2
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
    echo "host-restart-adopt: host on :$1 never linked (linked=$linked)" >&2
    return 1
}

start_ctl() {
    env -i PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
        HOME="$ROOT/ctl/home" K2_HOME="$ROOT/ctl/home/.k2" \
        TMPDIR="$ROOT/ctl/tmp" XDG_RUNTIME_DIR="$ROOT/ctl/run" K2_RUNTIME_DIR= \
        USER="$(id -un)" LANG=en_US.UTF-8 \
        K2_ROLE=controller K2_HOSTS=1 K2_HOST_BOOTSTRAP_TOKEN_FILE="$ROOT/bootstrap-token" K2_HOST_AUTH=enroll \
        K2_HOST_MOVE_AFTER=600 \
        K2_DAEMON_PORT="$CTL_PORT" K2_TRACE_FILE="$RUN/controller.trace.jsonl" \
        "$DAEMON" >>"$RUN/controller.log" 2>&1 &
    CTL_PID=$!
    wait_ready controller "$CTL_PID" "$CTL_PORT" "$RUN/controller.log"
}

start_host() { # prints the pid. The first start mints the host's one-time token.
    if [ ! -s "$ROOT/h1-token" ]; then
        curl -s -X POST -H 'Content-Type: application/json' "http://127.0.0.1:$CTL_PORT/cli/hosts/enroll-tokens?token=$CTL_TOKEN" \
            -d '{"ttl_secs": 600}' | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["token"])' > "$ROOT/h1-token"
    fi
    env -i PATH="$ROOT/shim:/usr/bin:/bin:/usr/sbin:/sbin" \
        HOME="$ROOT/h1/home" K2_HOME="$ROOT/h1/home/.k2" \
        TMPDIR="$ROOT/h1/tmp" XDG_RUNTIME_DIR="$ROOT/h1/run" K2_RUNTIME_DIR= \
        USER="$(id -un)" LANG=en_US.UTF-8 \
        K2_ROLE=host K2_CONTROLLER_URL="ws://127.0.0.1:$CTL_PORT" \
        K2_HOST_AUTH=enroll K2_HOST_ENROLL_TOKEN_FILE="$ROOT/h1-token" K2_HOST_NAME=host-1 \
        K2_WORK_ROOT="$ROOT/work-h1" \
        K2_PTYD=1 K2_PTYD_BIN="$RUN/bin/k2-ptyd" \
        K2_DAEMON_PORT="$H1_PORT" K2_TRACE_FILE="$RUN/host-1.trace.jsonl" \
        K2_TEST_AGENT_SHIM_DIR="$ROOT/shim" K2_FAKE_AGENT_TRACE="$FT" K2_FAKE_AGENT_HOST=host-1 \
        K2_FAKE_AGENT_TURN_MS="${LAB_TURN_MS:-200}" K2_FAKE_AGENT_SEED="${LAB_SEED:-0}" \
        "$DAEMON" >>"$RUN/host-1.log" 2>&1 &
    echo $!
}

post_json() { # path json → HTTP code; the body in $RUN/post.out
    curl -s -o "$RUN/post.out" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        "http://127.0.0.1:$CTL_PORT$1?token=$CTL_TOKEN" -d "$2"
}

msg() { # text wake → the message id ("" when refused); the answer in msgs.jsonl
    local code
    code=$(curl -s -o "$RUN/post.out" -w '%{http_code}' -X POST "http://127.0.0.1:$CTL_PORT/cli/workspace/msg?token=$CTL_TOKEN" \
        --data-urlencode "workspace=$WS" --data-urlencode "text=$1" --data-urlencode "from=lab-ra" --data-urlencode "wake=$2")
    { printf '{"http":%s,"answer":' "$code"; cat "$RUN/post.out"; echo '}'; } >> "$RUN/msgs.jsonl"
    python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1])).get("msgId") or "")' "$RUN/post.out" 2>/dev/null
}

state_of() { # msg id → its outbox row as "state attempts reason"
    curl -s "http://127.0.0.1:$CTL_PORT/cli/hosts/outbox?token=$CTL_TOKEN&msg_id=$1" | python3 -I -c '
import json,sys
m=(json.load(sys.stdin).get("message") or {})
print(m.get("state",""), m.get("attempts",""), (m.get("reason") or "-").replace(" ","_"))'
}
is_state() { [ "$(state_of "$1" | cut -d' ' -f1)" = "$2" ]; }

fake_events() { # event → one JSON per line
    python3 -I -c '
import json,sys
for l in open(sys.argv[1]):
    e=json.loads(l)
    if e.get("event")==sys.argv[2]: print(json.dumps(e))' "$FT" "$1" 2>/dev/null
}
count() { fake_events "$1" | wc -l | tr -d ' '; }
more_than() { [ "$(count "$1")" -gt "$2" ]; }
traced() { [ -f "$1" ] && grep -q "\"event\":\"$2\"" "$1"; }
transcript() { find "$ROOT/h1/home/.claude/projects" -name "$SESSION.jsonl" 2>/dev/null | head -1; }
in_transcript() { local t; t=$(transcript); [ -n "$t" ] && grep -q "$1" "$t"; }
screen() { # the adopted session's screen, from host-1's own daemon
    curl -s "http://127.0.0.1:$H1_PORT/cli/terminal/read?token=$H1_TOKEN&id=$PANE&lines=400" \
        | python3 -I -c 'import json,sys; print("\n".join(json.load(sys.stdin).get("lines") or []))' 2>/dev/null
}
on_screen() { screen | grep -q "$1"; }
# ctl.adopt events for the agent after line $1 of the controller trace: "decision…"
adopts_after() {
    tail -n +"$(( $1 + 1 ))" "$RUN/controller.trace.jsonl" 2>/dev/null | python3 -I -c '
import json,sys
for l in sys.stdin:
    e=json.loads(l)
    if e.get("event")=="ctl.adopt" and e.get("agent_id")==sys.argv[1]: print((e.get("attrs") or {}).get("decision"))' "$AGENT"
}

# ── the workspace ──────────────────────────────────────────────────────
git_in() { # dir args…
    local d=$1; shift
    env -i PATH=/usr/bin:/bin HOME="$ROOT/ctl/home" GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null \
        GIT_AUTHOR_NAME=lab GIT_AUTHOR_EMAIL=lab@example.invalid GIT_COMMITTER_NAME=lab GIT_COMMITTER_EMAIL=lab@example.invalid \
        git -C "$d" "$@"
}
mkdir -p "$WS"
git_in "$WS" init -q -b main
printf '# notes\n' > "$WS/README.md"
git_in "$WS" add -A && git_in "$WS" commit -q -m first

start_ctl || exit 1
wait_for 30 test -s "$ROOT/ctl/home/.k2/daemon.token" || { echo "host-restart-adopt: no controller token" >&2; exit 1; }
CTL_TOKEN=$(cat "$ROOT/ctl/home/.k2/daemon.token")
H1_PID=$(start_host)
wait_ready host-1 "$H1_PID" "$H1_PORT" "$RUN/host-1.log" && wait_linked "$H1_PORT" || exit 1
H1_TOKEN=$(cat "$ROOT/h1/home/.k2/daemon.token")

c=$(post_json /cli/projects/add-from-path "{\"path\":\"$WS\",\"seed_wiki\":false,\"seed_agents_md\":false}")
AGENT=$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1])).get("id",""))' "$RUN/post.out" 2>/dev/null)
[ "$c" = 200 ] && [ -n "$AGENT" ] || { echo "host-restart-adopt: add-from-path HTTP $c $(cat "$RUN/post.out")" >&2; exit 1; }
c=$(post_json /cli/projects/update "{\"id\":\"$AGENT\",\"agentEnabled\":1}")
[ "$c" = 200 ] || { echo "host-restart-adopt: update HTTP $c $(cat "$RUN/post.out")" >&2; exit 1; }
echo "agent $AGENT (workspace $WS)" | tee "$RUN/agent.txt"

# 1. wakes on host-1 under ptyd, and answers
m1=$(msg "first before-restart-marker" true)
wait_for 60 more_than cli.turn.end 0
EXEC=$(fake_events cli.exec | tail -1)
CLI_PID=$(echo "$EXEC" | python3 -I -c 'import json,sys; print(json.loads(sys.stdin.read() or "{}").get("pid",""))')
SESSION=$(echo "$EXEC" | python3 -I -c 'import json,sys; print(json.loads(sys.stdin.read() or "{}").get("session_id",""))')
PANE=$(echo "$EXEC" | python3 -I -c 'import json,sys; print(json.loads(sys.stdin.read() or "{}").get("pane",""))')
parent=$(ps -o ppid= -p "$CLI_PID" 2>/dev/null | tr -d ' ')
parent_cmd=$(ps -o command= -p "$parent" 2>/dev/null)
wait_for 30 is_state "$m1" delivered
check "notes wakes on host-1 and answers; its CLI is ptyd's child, not the daemon's" \
    "$([ -n "$m1" ] && is_state "$m1" delivered && [ -n "$CLI_PID" ] && [ "$parent" != "$H1_PID" ] && echo "$parent_cmd" | grep -q k2-ptyd && echo 1)" \
    "msg $m1 $(state_of "$m1"); CLI pid $CLI_PID, parent $parent ($parent_cmd); daemon $H1_PID; pane $PANE"
wait_for 30 on_screen "before-restart-marker"
screen > "$RUN/screen-before.txt"

# 2. a long turn; SIGKILL host-1's daemon mid-turn
starts=$(count cli.turn.start)
m2=$(msg "second [[turn-ms=$LONG_MS]] mid-turn-marker" false)
wait_for 30 more_than cli.turn.start "$starts"
ends=$(count cli.turn.end)
epoch_before=$(grep -E "\[hosts\] placed agent $AGENT on host host-1" "$RUN/controller.log" | tail -1 | sed -E 's/.* epoch ([0-9]+).*/\1/')
ctl_lines=$(wc -l < "$RUN/controller.trace.jsonl" | tr -d ' ')
kill -9 "$H1_PID"
wait "$H1_PID" 2>/dev/null
OLD_H1=$H1_PID H1_PID=""
wait_for 10 sh -c "! kill -0 $OLD_H1 2>/dev/null"
check "a long turn starts on a message; host-1's daemon is SIGKILLed mid-turn and the CLI outlives it" \
    "$([ -n "$m2" ] && ! kill -0 "$OLD_H1" 2>/dev/null && kill -0 "$CLI_PID" 2>/dev/null && [ "$(count cli.turn.end)" = "$ends" ] && echo 1)" \
    "msg $m2; daemon $OLD_H1 killed; CLI $CLI_PID alive: $(kill -0 "$CLI_PID" 2>/dev/null && echo yes || echo no); turn ends $(count cli.turn.end) (before $ends)"

# 3. host-1 restarts with the same HOME and adopts the CLI
H1_PID=$(start_host)
wait_ready host-1 "$H1_PID" "$H1_PORT" "$RUN/host-1.log" && wait_linked "$H1_PORT" || exit 1
H1_TOKEN=$(cat "$ROOT/h1/home/.k2/daemon.token")
wait_for 10 sh -c "[ -n \"\$(tail -n +$((ctl_lines + 1)) '$RUN/controller.trace.jsonl' | grep ctl.adopt)\" ]"
decisions=$(adopts_after "$ctl_lines" | tr '\n' ' ')
epoch_after=$(grep -E "adopted agent $AGENT session" "$RUN/host-1.log" | tail -1 | sed -E 's/.* epoch ([0-9]+).*/\1/')
check "host-1 restarts and adopts the CLI; the controller adopts the placement at the same epoch, no gone" \
    "$(traced "$RUN/host-1.trace.jsonl" host.adopt && [ "$decisions" = "adopted " ] && [ -n "$epoch_before" ] && [ "$epoch_after" = "$epoch_before" ] && echo 1)" \
    "host.adopt: $(grep -c '"event":"host.adopt"' "$RUN/host-1.trace.jsonl" 2>/dev/null); controller decisions after the restart: ${decisions:-none}; epoch $epoch_before → $epoch_after; $(grep -E '\[ptyd\] (adopted|ended)' "$RUN/host-1.log" | tail -2 | tr '\n' ' ')"

# 4–6. the turn ends; the message is delivered, not failed; the agent answered it
wait_for $(( LONG_MS / 1000 + 30 )) more_than cli.turn.end "$ends"
wait_for 20 in_transcript "reply #2"
sleep 2   # a cut would land now: the controller has the turn's Stop
st2=$(state_of "$m2")
execs=$(count cli.exec); exits=$(count cli.exit)
check "the CLI survives: the same pid, never exited, never exec'd again" \
    "$(kill -0 "$CLI_PID" 2>/dev/null && [ "$execs" = 1 ] && [ "$exits" = 0 ] && echo 1)" \
    "CLI $CLI_PID alive: $(kill -0 "$CLI_PID" 2>/dev/null && echo yes || echo no); cli.exec $execs, cli.exit $exits"
check "the turn's message is not failed: delivered, sent once" \
    "$([ "$(echo "$st2" | cut -d' ' -f1)" = delivered ] && [ "$(echo "$st2" | cut -d' ' -f2)" = 1 ] && echo 1)" \
    "msg $m2: $st2"
wait_for 20 on_screen "reply #2"
screen > "$RUN/screen-after.txt"
check "the agent answers it: the turn ends, the reply is in the transcript and on the adopted screen, with the output from before the restart" \
    "$(more_than cli.turn.end "$ends" && in_transcript "reply #2" && on_screen "reply #2" && on_screen "before-restart-marker" && echo 1)" \
    "turn ends $(count cli.turn.end); transcript $(transcript | sed "s|$ROOT|<root>|"); screen: $(grep -c . "$RUN/screen-after.txt") lines"

# 7. a message after the restart
m3=$(msg "third after-restart-marker" false)
wait_for 30 is_state "$m3" delivered
wait_for 30 in_transcript "reply #3"
st3=$(state_of "$m3")
check "a message after the restart is delivered once and answered" \
    "$([ "$(echo "$st3" | cut -d' ' -f1)" = delivered ] && [ "$(echo "$st3" | cut -d' ' -f2)" = 1 ] && in_transcript "reply #3" && [ "$(count cli.exec)" = 1 ] && echo 1)" \
    "msg $m3: $st3; cli.exec $(count cli.exec)"

grep -E '\[(delivery|hosts)\]' "$RUN/controller.log" > "$RUN/controller-hosts.log" || true
grep -E '\[(ptyd|hosts/adopt|hosts/link)\]' "$RUN/host-1.log" > "$RUN/host-1-adopt.log" || true
echo "evidence: ${RUN#$REPO/}"
if [ "$FAILS" = 0 ]; then echo "host-restart-adopt: PASS"; exit 0; fi
echo "host-restart-adopt: $FAILS check(s) failed"
exit 1
