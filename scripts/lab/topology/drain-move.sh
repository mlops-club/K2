#!/usr/bin/env bash
# drain-move (rc1 kind-app P4 drain demo) on three real processes: a
# K2_ROLE=controller, host-1 and host-2 (K2_ROLE=host, each its own
# throwaway HOME and disk, K2_WORK_ROOT=<root>/work-h1 and work-h2, enrolled
# with W5 one-time tokens), k2-fake-agent as `claude`.
#
# host-1 is drained for good (SIGTERM, as a pool's scale-down or node
# maintenance does) and never comes back. Before the fix the drained agent
# waited for host-1 forever and its messages with it. Now a message is a
# need: the agent resumes on host-2 at once, with its home and session, and
# the message is delivered once. K2_HOST_MOVE_AFTER is 600 s here, so the
# backstop (unit-tested) can't be what moved it.
#
# Checks:
#   1. notes wakes on host-1 (a message, no app); its turn writes MEMORY.md
#   2. SIGTERM host-1: it saves notes' home, releases it and exits; the
#      controller holds notes for resume, placed nowhere
#   3. a message for notes: the controller resumes it on host-2 now (not
#      after K2_HOST_MOVE_AFTER), which restores the store's newest snapshot
#      (fetched > 0) and resumes the same session (--resume)
#   4. host-2 has notes' memory from host-1, and answers the message: its
#      turn runs to its end (the idle reaper, ticking every second, never
#      cuts it: K2-d), and the message is delivered once (one delivered
#      ack, once as a prompt in the transcript)
#
# Ports: DM_CTL_PORT (default 17486), DM_H1_PORT (17487), DM_H2_PORT (17488).
# Evidence: lab-runs/drain-move/<UTC>/.

set -uo pipefail
REPO=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$REPO" || exit 1
BUILD=1
[ "${1:-}" = --no-build ] && BUILD=0
CTL_PORT=${DM_CTL_PORT:-17486}
H1_PORT=${DM_H1_PORT:-17487}
H2_PORT=${DM_H2_PORT:-17488}
MOVE_AFTER=${DM_MOVE_AFTER:-600}
for port in "$CTL_PORT" "$H1_PORT" "$H2_PORT"; do
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "drain-move: port $port is taken ($(lsof -nP -iTCP:"$port" -sTCP:LISTEN | awk 'NR==2{print $1, $2}')); set DM_CTL_PORT/DM_H1_PORT/DM_H2_PORT" >&2
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
RUN="$REPO/lab-runs/drain-move/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN/bin"
cp "$target/debug/k2-daemon" "$target/debug/k2-fake-agent" "$RUN/bin/" || exit 1
DAEMON="$RUN/bin/k2-daemon"
FAKE="$RUN/bin/k2-fake-agent"

# A short root: ptyd's socket path must fit in sun_path (104 bytes on macOS).
ROOT=$(mktemp -d "${LAB_SHORT_TMP:-/tmp}/k2dm-XXXXXX") || exit 1
ROOT=$(cd "$ROOT" && pwd -P)
mkdir -p "$ROOT"/{ctl,h1,h2}/{home,tmp,run} "$ROOT/shim" "$ROOT/work-h1" "$ROOT/work-h2"
head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$ROOT/bootstrap-token"
"$FAKE" --install-shims "$ROOT/shim" || exit 1
CTL_PID="" H1_PID="" H2_PID=""
WS="$ROOT/work-h1/notes"   # where the controller registered it; on host-2 it is work-h2/notes
WS2="$ROOT/work-h2/notes"
FT="$RUN/fake-agent.trace.jsonl"

# Every process under `pid`, deepest first.
tree_of() {
    local c
    for c in $(pgrep -P "$1" 2>/dev/null); do tree_of "$c"; done
    echo "$1"
}

down() {
    local pid i
    for pid in "$H1_PID" "$H2_PID" "$CTL_PID"; do
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
            echo "drain-move: $1 exited during boot: $(tail -3 "$4" | tr '\n' ' ')" >&2
            return 1
        fi
        phase=$(curl -s --connect-timeout 1 "http://127.0.0.1:$3/boot-status" 2>/dev/null \
            | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("phase",""))' 2>/dev/null || true)
        [ "$phase" = ready ] && return 0
        sleep 0.2
    done
    echo "drain-move: $1 never reached boot phase ready in 60 s (last: '$phase')" >&2
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
    echo "drain-move: host on :$1 never linked (linked=$linked)" >&2
    return 1
}

start_ctl() {
    env -i PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
        HOME="$ROOT/ctl/home" K2_HOME="$ROOT/ctl/home/.k2" \
        TMPDIR="$ROOT/ctl/tmp" XDG_RUNTIME_DIR="$ROOT/ctl/run" K2_RUNTIME_DIR= \
        USER="$(id -un)" LANG=en_US.UTF-8 \
        K2_ROLE=controller K2_HOSTS=1 K2_HOST_BOOTSTRAP_TOKEN_FILE="$ROOT/bootstrap-token" K2_HOST_AUTH=enroll \
        K2_HOST_MOVE_AFTER="$MOVE_AFTER" K2SO_ACTIVE_REAPER_TICK_SECS=1 K2SO_ACTIVE_REAPER_GRACE_MS=1000 \
        K2_DAEMON_PORT="$CTL_PORT" K2_TRACE_FILE="$RUN/controller.trace.jsonl" \
        "$DAEMON" >>"$RUN/controller.log" 2>&1 &
    CTL_PID=$!
    wait_ready controller "$CTL_PID" "$CTL_PORT" "$RUN/controller.log"
}

start_host() { # name dir port; prints the pid. A first start mints the host's one-time token.
    if [ ! -s "$ROOT/$2-token" ]; then
        curl -s -X POST -H 'Content-Type: application/json' "http://127.0.0.1:$CTL_PORT/cli/hosts/enroll-tokens?token=$CTL_TOKEN" \
            -d '{"ttl_secs": 600}' | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["token"])' > "$ROOT/$2-token"
    fi
    env -i PATH="$ROOT/shim:/usr/bin:/bin:/usr/sbin:/sbin" \
        HOME="$ROOT/$2/home" K2_HOME="$ROOT/$2/home/.k2" \
        TMPDIR="$ROOT/$2/tmp" XDG_RUNTIME_DIR="$ROOT/$2/run" K2_RUNTIME_DIR= \
        USER="$(id -un)" LANG=en_US.UTF-8 \
        K2_ROLE=host K2_CONTROLLER_URL="ws://127.0.0.1:$CTL_PORT" \
        K2_HOST_AUTH=enroll K2_HOST_ENROLL_TOKEN_FILE="$ROOT/$2-token" K2_HOST_NAME="$1" \
        K2_WORK_ROOT="$ROOT/work-$2" K2_HOST_DRAIN_SECS=60 \
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

msg() { # text wake → HTTP code; the body in $RUN/post.out (and appended to msgs.jsonl)
    local code
    code=$(curl -s -o "$RUN/post.out" -w '%{http_code}' -X POST "http://127.0.0.1:$CTL_PORT/cli/workspace/msg?token=$CTL_TOKEN" \
        --data-urlencode "workspace=$WS" --data-urlencode "text=$1" --data-urlencode "from=lab-dm" --data-urlencode "wake=$2")
    { printf '{"text":%s,"http":%s,"answer":' "$(python3 -I -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$1")" "$code"; cat "$RUN/post.out"; echo '}'; } >> "$RUN/msgs.jsonl"
    echo "$code"
}

placed_on() { grep -E "\[hosts\] placed agent $AGENT on host " "$RUN/controller.log" | tail -1 | sed -E 's/.* on host ([^ ]+) .*/\1/'; }
placed_epoch() { grep -E "\[hosts\] placed agent $AGENT on host " "$RUN/controller.log" | tail -1 | sed -E 's/.* epoch ([0-9]+).*/\1/'; }

# host.home_saved events of a host with trigger turn: "count seq sent_objects hashed files" of the last.
turn_saves() {
    python3 -I -c '
import json,sys
ev=[json.loads(l) for l in open(sys.argv[1]) if "host.home_saved" in l]
ev=[e for e in ev if (e.get("attrs") or {}).get("trigger")=="turn"]
a=ev[-1]["attrs"] if ev else {}
print(len(ev), a.get("seq","none"), a.get("sent_objects","none"), a.get("hashed","none"), a.get("files","none"))' "$RUN/$1.trace.jsonl" 2>/dev/null || echo "0 none none none none"
}
saves_with() { python3 -I -c '
import json,sys
print(sum(1 for l in open(sys.argv[1]) if "host.home_saved" in l and json.loads(l).get("attrs",{}).get("trigger")==sys.argv[2]))' "$RUN/$1.trace.jsonl" "$2" 2>/dev/null || echo 0; }
# The last host.home_restored on a host: "seq fetched".
last_restore() {
    python3 -I -c '
import json,sys
ev=[json.loads(l) for l in open(sys.argv[1]) if "host.home_restored" in l]
print("%s %s" % (ev[-1]["attrs"]["seq"], ev[-1]["attrs"]["fetched"]) if ev else "none 0")' "$RUN/$1.trace.jsonl" 2>/dev/null || echo "none 0"
}
# The fake agent's execs in a work root: one JSON per line.
execs_in() { # work dir
    python3 -I -c '
import json,sys
for l in open(sys.argv[1]):
    e=json.loads(l)
    if e.get("event")=="cli.exec" and ((e.get("attrs") or {}).get("cwd") or "").startswith(sys.argv[2]):
        print(json.dumps(e))' "$FT" "$1" 2>/dev/null
}
turn_starts() { grep -c '"event":"cli.turn.start"' "$FT" 2>/dev/null || true; }
turn_ends() { grep -c '"event":"cli.turn.end"' "$FT" 2>/dev/null || true; }

memory_of() { find "$ROOT/$1/home/.claude/projects" -path '*-notes/memory/MEMORY.md' 2>/dev/null | head -1; }
transcript_of() { find "$ROOT/$1/home/.claude/projects" -path '*-notes/*' -name "$SESSION.jsonl" 2>/dev/null | head -1; }
sha() { [ -f "$1" ] && shasum -a 256 "$1" | cut -d' ' -f1; }

wait_for() { # seconds command…: until the command succeeds
    local n=$(( $1 * 10 )); shift
    for _ in $(seq 1 "$n"); do "$@" && return 0; sleep 0.1; done
    return 1
}
has_line() { [ -f "$1" ] && grep -q "$2" "$1"; }
traced() { [ -f "$1" ] && grep -q "\"event\":\"$2\"" "$1"; }
saves_over() { [ "$(turn_saves "$1" | cut -d' ' -f1)" -gt "$2" ]; }
more_than() { [ "$("$1")" -gt "$2" ]; }

git_in() { # dir args…
    local d=$1; shift
    env -i PATH=/usr/bin:/bin HOME="$ROOT/ctl/home" GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null \
        GIT_AUTHOR_NAME=lab GIT_AUTHOR_EMAIL=lab@example.invalid GIT_COMMITTER_NAME=lab GIT_COMMITTER_EMAIL=lab@example.invalid \
        git -C "$d" "$@"
}
ws_state() { # dir: HEAD, history, porcelain status, and the files' hashes (no .git internals)
    git_in "$1" rev-parse HEAD
    git_in "$1" log --format='%H %s'
    git_in "$1" status --porcelain
    (cd "$1" && find . -path ./.git -prune -o -type f -print | LC_ALL=C sort | xargs shasum -a 256)
}

# ── the workspace ──────────────────────────────────────────────────────
mkdir -p "$WS"
git_in "$WS" init -q -b main
printf '# notes\n' > "$WS/README.md"
git_in "$WS" add -A && git_in "$WS" commit -q -m first
printf 'dirty on host-1\n' > "$WS/wip.txt"

start_ctl || exit 1
wait_for 30 test -s "$ROOT/ctl/home/.k2/daemon.token" || { echo "drain-move: no controller token" >&2; exit 1; }
CTL_TOKEN=$(cat "$ROOT/ctl/home/.k2/daemon.token")
H1_PID=$(start_host host-1 h1 "$H1_PORT")
wait_ready host-1 "$H1_PID" "$H1_PORT" "$RUN/host-1.log" && wait_linked "$H1_PORT" || exit 1

c=$(post_json /cli/projects/add-from-path "{\"path\":\"$WS\",\"seed_wiki\":false,\"seed_agents_md\":false}")
AGENT=$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1])).get("id",""))' "$RUN/post.out" 2>/dev/null)
[ "$c" = 200 ] && [ -n "$AGENT" ] || { echo "drain-move: add-from-path HTTP $c $(cat "$RUN/post.out")" >&2; exit 1; }
c=$(post_json /cli/projects/update "{\"id\":\"$AGENT\",\"agentEnabled\":1}")
[ "$c" = 200 ] || { echo "drain-move: update HTTP $c $(cat "$RUN/post.out")" >&2; exit 1; }
echo "agent $AGENT (workspace $WS)" | tee "$RUN/agent.txt"

# 1. notes on host-1, by a message only
c=$(msg "first [[remember: turn one]]" true)
wait_for 60 has_line "$(memory_of h1)" "turn one"
SESSION=$(execs_in "$ROOT/work-h1" | tail -1 | python3 -I -c 'import json,sys; print(json.loads(sys.stdin.read() or "{}").get("session_id",""))')
check "notes wakes on host-1 by a message (no app); its turn writes MEMORY.md" \
    "$([ "$c" = 200 ] && [ "$(placed_on)" = host-1 ] && has_line "$(memory_of h1)" "turn one" && [ -n "$SESSION" ] && echo 1)" \
    "msg HTTP $c; on $(placed_on); session $SESSION"

H2_PID=$(start_host host-2 h2 "$H2_PORT")
wait_ready host-2 "$H2_PID" "$H2_PORT" "$RUN/host-2.log" && wait_linked "$H2_PORT" || exit 1

# 2. drain host-1 for good
kill -TERM "$H1_PID"
wait_for 90 sh -c "! kill -0 $H1_PID 2>/dev/null"
gone=$(kill -0 "$H1_PID" 2>/dev/null && echo 0 || echo 1)
H1_PID=""
ctl_get /cli/hosts/status > "$RUN/status-after-drain.json"
resuming=$(python3 -I -c 'import json,sys; print(sys.argv[2] in (json.load(open(sys.argv[1])).get("resuming") or []))' "$RUN/status-after-drain.json" "$AGENT")
ctl_get /cli/hosts/home/status > "$RUN/home-status-drain.json"
newest() { python3 -I -c 'import json,sys; s=[x for x in json.load(open(sys.argv[1]))["snapshots"] if x["agent"]==sys.argv[2]]; print(s[0]["seq"] if s else "none")' "$1" "$AGENT"; }
SEQ_DRAIN=$(newest "$RUN/home-status-drain.json")
check "SIGTERM host-1: it saved notes' home, released it and exited; the controller holds notes for resume" \
    "$([ "$gone" = 1 ] && traced "$RUN/host-1.trace.jsonl" host.drain_release && [ "$(saves_with host-1 release)" -ge 1 ] && [ "$resuming" = True ] && [ "$SEQ_DRAIN" != none ] && echo 1)" \
    "exited $gone; release saves $(saves_with host-1 release); resuming $resuming; store's newest snapshot $SEQ_DRAIN"

# 3. a message is a need: notes resumes on host-2 now
t_msg=$(date +%s)
c=$(msg "second [[turn-ms=8000]] [[remember: after the drain]]" false)
MSG_ID=$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1])).get("msgId",""))' "$RUN/post.out" 2>/dev/null)
wait_for 60 sh -c "grep -q 'placed agent $AGENT on host host-2' '$RUN/controller.log'"
t_placed=$(date +%s)
wait_for 30 traced "$RUN/host-2.trace.jsonl" host.home_restored
read -r rseq rfetched <<< "$(last_restore host-2)"
EX2=$(execs_in "$ROOT/work-h2" | tail -1)
resumed2=$(echo "$EX2" | python3 -I -c '
import json,sys
e=json.loads(sys.stdin.read() or "{}"); a=e.get("attrs") or {}
print(1 if a.get("resumed") and e.get("session_id")==sys.argv[1] and "--resume" in (a.get("argv") or []) else 0)' "$SESSION")
drained_move=$(grep '"event":"ctl.move_decision"' "$RUN/controller.trace.jsonl" 2>/dev/null | grep -c '"drained":true')
check "a message for notes resumes it on host-2 at once (no K2_HOST_MOVE_AFTER wait); host-2 restores the newest snapshot and resumes the same session" \
    "$([ "$c" = 200 ] && [ "$drained_move" -ge 1 ] && [ "$(placed_on)" = host-2 ] && [ $((t_placed - t_msg)) -lt "$MOVE_AFTER" ] && [ "$rseq" = "$SEQ_DRAIN" ] && [ "$rfetched" -gt 0 ] && [ "$resumed2" = 1 ] && echo 1)" \
    "msg HTTP $c ($MSG_ID); move decisions (drained) $drained_move; on $(placed_on) $((t_placed - t_msg)) s after the message; restored seq $rseq (store $SEQ_DRAIN) fetched $rfetched; exec $(echo "$EX2" | head -c 240)"

# 4. its memory came along; the 8 s turn runs to its end; delivered once
wait_for 60 has_line "$(memory_of h2)" "after the drain"
M2=$(memory_of h2); T2=$(transcript_of h2)
cp "$T2" "$RUN/transcript-host-2.jsonl" 2>/dev/null
outbox_row() { curl -s "http://127.0.0.1:$CTL_PORT/cli/hosts/outbox?token=$CTL_TOKEN&msg_id=$MSG_ID"; }
wait_for 30 sh -c "curl -s 'http://127.0.0.1:$CTL_PORT/cli/hosts/outbox?token=$CTL_TOKEN&msg_id=$MSG_ID' | grep -q '\"state\":\"delivered\"'"
outbox_row > "$RUN/outbox-row.json"
read -r st attempts <<< "$(python3 -I -c 'import json,sys; m=json.load(open(sys.argv[1])).get("message") or {}; print(m.get("state"), m.get("attempts"))' "$RUN/outbox-row.json")"
# User entries only: the fake agent's reply echoes the prompt.
prompts=$(grep '"role":"user"' "$T2" 2>/dev/null | grep -c "after the drain" || true)
acks=$(grep -c "$MSG_ID Delivered by host" "$RUN/controller.log" 2>/dev/null || true)
cut=$(grep -c "firing close for project=$AGENT" "$RUN/controller.log" 2>/dev/null || true)
check "host-2 has notes' memory from host-1 and answers: the turn ends (never reaped mid-turn), and the message is delivered once" \
    "$(has_line "$M2" "turn one" && has_line "$M2" "after the drain" && [ "$st" = delivered ] && [ "$acks" = 1 ] && [ "$prompts" = 1 ] && [ "$cut" = 0 ] && echo 1)" \
    "memory $(tr '\n' '|' < "$M2" 2>/dev/null); outbox $st after $attempts send(s) (a send before the resumed session is live is answered 'not yet', nothing written), $acks delivered ack(s); prompt in the transcript $prompts time(s); reaper closes of notes $cut"

ctl_get /cli/hosts/home/status > "$RUN/home-status-final.json"
grep -hE '\[hosts\] .*(drained|resumes on|placed agent|move)' "$RUN/controller.log" > "$RUN/move.log" 2>/dev/null || true
grep -hE '\[active-reaper\]' "$RUN/controller.log" > "$RUN/reaper.log" 2>/dev/null || true
rm -rf "$RUN/bin"
echo "evidence: $RUN"
if [ "$FAILS" = 0 ]; then echo "drain-move: PASS"; else echo "drain-move: $FAILS check(s) failed"; exit 1; fi
