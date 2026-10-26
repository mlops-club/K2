#!/usr/bin/env bash
# P3 T2 turn-end snapshots and S10 on three real processes: a
# K2_ROLE=controller, host-1 and host-2 (K2_ROLE=host, each its own
# throwaway HOME and its own disk: K2_WORK_ROOT=<root>/work-h1 and
# <root>/work-h2, enrolled with W5 one-time tokens), k2-fake-agent as
# `claude`. A host is killed with SIGKILL mid-turn (no drain, nothing
# saved at the end); its agent moves to the other host with the home and
# conversation its last finished turn left, and only the turn in progress is
# lost.
#
# The agent is a registered workspace's chat, driven the way a user or
# another agent would: `POST /cli/workspace/msg` (the outbox, a wake).
#
# Checks:
#   1. notes wakes on host-1; its first turn ends and host-1 saves its home
#      at the turn's end (host.home_saved, trigger turn)
#   2. a second turn (the workspace edited meanwhile): saved at its end
#      too, as an increment (only what changed sent; the hash cache reads
#      only the changed files)
#   3. a third, long turn starts and changes the workspace; host-1 is
#      SIGKILLed with its agent CLI mid-turn: no drain, no release save
#   4. the controller moves notes once host-1 has been lost for
#      K2_HOST_MOVE_AFTER (ctl.move_decision); it resumes on host-2 at a new
#      epoch, which first restores the last turn's snapshot (fetched > 0),
#      with --resume and the same session
#   5. host-2 has the home the second turn left: the workspace byte for
#      byte (the in-progress turn's change absent), MEMORY.md and the
#      transcript; the lost turn is the only thing missing
#   6. a message after the move lands on host-2, and the agent remembers
#      turns one and two there
#   7. host-1 restarts: notes stays on host-2, one live CLI (I1)
#
# TS_TURN_SECS sets the hosts' K2_HOME_TURN_SECS (default: unset, every turn
# end); TS_TURN_SECS=off is the control: no turn-end saves, so checks 1, 2
# and 4-6 fail (the moved agent finds no snapshot).
#
# Ports: TS_CTL_PORT (default 17483), TS_H1_PORT (17484), TS_H2_PORT (17485);
# the script stops if one is taken.
# Evidence: lab-runs/turn-snapshots/<UTC>/.

set -uo pipefail
REPO=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$REPO" || exit 1
BUILD=1
[ "${1:-}" = --no-build ] && BUILD=0
CTL_PORT=${TS_CTL_PORT:-17483}
H1_PORT=${TS_H1_PORT:-17484}
H2_PORT=${TS_H2_PORT:-17485}
MOVE_AFTER=${TS_MOVE_AFTER:-3}
for port in "$CTL_PORT" "$H1_PORT" "$H2_PORT"; do
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "turn-snapshots: port $port is taken ($(lsof -nP -iTCP:"$port" -sTCP:LISTEN | awk 'NR==2{print $1, $2}')); set TS_CTL_PORT/TS_H1_PORT/TS_H2_PORT" >&2
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
RUN="$REPO/lab-runs/turn-snapshots/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN/bin"
cp "$target/debug/k2-daemon" "$target/debug/k2-fake-agent" "$RUN/bin/" || exit 1
DAEMON="$RUN/bin/k2-daemon"
FAKE="$RUN/bin/k2-fake-agent"

# A short root: ptyd's socket path must fit in sun_path (104 bytes on macOS).
ROOT=$(mktemp -d "${LAB_SHORT_TMP:-/tmp}/k2ts-XXXXXX") || exit 1
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
    [ -n "${ROOT:-}" ] && pkill -9 -f "$ROOT/" 2>/dev/null || true
    rm -rf "$ROOT"
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
            echo "turn-snapshots: $1 exited during boot: $(tail -3 "$4" | tr '\n' ' ')" >&2
            return 1
        fi
        phase=$(curl -s --connect-timeout 1 "http://127.0.0.1:$3/boot-status" 2>/dev/null \
            | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("phase",""))' 2>/dev/null || true)
        [ "$phase" = ready ] && return 0
        sleep 0.2
    done
    echo "turn-snapshots: $1 never reached boot phase ready in 60 s (last: '$phase')" >&2
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
    echo "turn-snapshots: host on :$1 never linked (linked=$linked)" >&2
    return 1
}

start_ctl() {
    env -i PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
        HOME="$ROOT/ctl/home" K2_HOME="$ROOT/ctl/home/.k2" \
        TMPDIR="$ROOT/ctl/tmp" XDG_RUNTIME_DIR="$ROOT/ctl/run" K2_RUNTIME_DIR= \
        USER="$(id -un)" LANG=en_US.UTF-8 \
        K2_ROLE=controller K2_HOSTS=1 K2_HOST_BOOTSTRAP_TOKEN_FILE="$ROOT/bootstrap-token" K2_HOST_AUTH=enroll \
        K2_HOST_MOVE_AFTER="$MOVE_AFTER" \
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
        K2_WORK_ROOT="$ROOT/work-$2" K2_HOST_DRAIN_SECS=60 K2_HOME_TURN_SECS="${TS_TURN_SECS:-}" \
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
        --data-urlencode "workspace=$WS" --data-urlencode "text=$1" --data-urlencode "from=lab-ts" --data-urlencode "wake=$2")
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
printf 'one\n' > "$WS/lib.txt"
git_in "$WS" add -A && git_in "$WS" commit -q -m first
printf 'two (dirty)\n' > "$WS/lib.txt"

start_ctl || exit 1
wait_for 30 test -s "$ROOT/ctl/home/.k2/daemon.token" || { echo "turn-snapshots: no controller token" >&2; exit 1; }
CTL_TOKEN=$(cat "$ROOT/ctl/home/.k2/daemon.token")
H1_PID=$(start_host host-1 h1 "$H1_PORT")
wait_ready host-1 "$H1_PID" "$H1_PORT" "$RUN/host-1.log" && wait_linked "$H1_PORT" || exit 1

c=$(post_json /cli/projects/add-from-path "{\"path\":\"$WS\",\"seed_wiki\":false,\"seed_agents_md\":false}")
AGENT=$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1])).get("id",""))' "$RUN/post.out" 2>/dev/null)
[ "$c" = 200 ] && [ -n "$AGENT" ] || { echo "turn-snapshots: add-from-path HTTP $c $(cat "$RUN/post.out")" >&2; exit 1; }
c=$(post_json /cli/projects/update "{\"id\":\"$AGENT\",\"agentEnabled\":1}")
[ "$c" = 200 ] || { echo "turn-snapshots: update HTTP $c $(cat "$RUN/post.out")" >&2; exit 1; }
echo "agent $AGENT (workspace $WS)" | tee "$RUN/agent.txt"

# 1. the first turn, on host-1; saved at its end
c=$(msg "first [[remember: turn one]]" true)
wait_for 60 has_line "$(memory_of h1)" "turn one"
wait_for 30 saves_over host-1 0
read -r n1 seq1 sent1 hashed1 files1 <<< "$(turn_saves host-1)"
SESSION=$(execs_in "$ROOT/work-h1" | tail -1 | python3 -I -c 'import json,sys; print(json.loads(sys.stdin.read() or "{}").get("session_id",""))')
check "notes wakes on host-1; its first turn ends and host-1 saves its home at the turn's end" \
    "$([ "$c" = 200 ] && [ "$(placed_on)" = host-1 ] && has_line "$(memory_of h1)" "turn one" && [ "${n1:-0}" -ge 1 ] && [ -n "$SESSION" ] && echo 1)" \
    "msg HTTP $c; on $(placed_on); turn saves $n1, last seq $seq1 ($files1 files, $hashed1 read, $sent1 objects sent); session $SESSION"

# host-2 joins; notes stays where it is
H2_PID=$(start_host host-2 h2 "$H2_PORT")
wait_ready host-2 "$H2_PID" "$H2_PORT" "$RUN/host-2.log" && wait_linked "$H2_PORT" || exit 1

# 2. a second turn, the workspace edited meanwhile; saved at its end, as an increment
printf 'turn two edit\n' > "$WS/turn2.txt"
before=$(turn_saves host-1 | cut -d' ' -f1)
c=$(msg "second [[remember: turn two]]" false)
wait_for 60 has_line "$(memory_of h1)" "turn two"
# Turn one's save is done: the next save is turn two's Stop.
wait_for 30 saves_over host-1 "$before"
read -r n2 seq2 sent2 hashed2 files2 <<< "$(turn_saves host-1)"
ctl_get /cli/hosts/home/status > "$RUN/home-status-2.json"
stored_seq=$(python3 -I -c 'import json,sys; s=[x for x in json.load(open(sys.argv[1]))["snapshots"] if x["agent"]==sys.argv[2]]; print(s[0]["seq"] if s else "")' "$RUN/home-status-2.json" "$AGENT")
check "a second turn (the workspace edited) is saved at its end, as an increment: only what changed is read and sent" \
    "$([ "$c" = 200 ] && [ "${n2:-0}" -gt "${n1:-0}" ] && [ "$seq2" = "$stored_seq" ] && [ "$sent2" -ge 1 ] && [ "$sent2" -lt "$files2" ] && [ "$hashed2" -lt "$files2" ] && echo 1)" \
    "msg HTTP $c; turn saves $n2; last seq $seq2 (the store's newest: $stored_seq): $files2 files, $hashed2 read, $sent2 objects sent"
M2=$(memory_of h1); T2=$(transcript_of h1)
MEM2_SHA=$(sha "$M2"); MEM2_BYTES=$(wc -c < "$M2" | tr -d ' ')
ws_state "$WS" > "$RUN/workspace-after-turn-two.txt"
EPOCH1=$(placed_epoch)

# 3. a long third turn starts and changes the workspace; SIGKILL host-1 and its CLI
starts=$(turn_starts)
ends=$(turn_ends)
c=$(msg "third [[turn-ms=60000]] [[remember: turn three]]" false)
wait_for 60 more_than turn_starts "$starts"
printf 'turn three, in progress\n' > "$WS/turn3-wip.txt"
H1_TREE=$(tree_of "$H1_PID" | tr '\n' ' ')
kill -9 $H1_TREE 2>/dev/null
wait "$H1_PID" 2>/dev/null
t_kill=$(date -u +%Y-%m-%dT%H:%M:%S)
# SIGKILL lands asynchronously: wait for the processes to be gone.
wait_for 10 sh -c "for p in $H1_TREE; do kill -0 \$p 2>/dev/null && exit 1; done; exit 0"
alive=0
for p in $H1_TREE; do kill -0 "$p" 2>/dev/null && alive=$((alive + 1)); done
H1_PID=""
check "a long third turn started; host-1 and its agent CLI are SIGKILLed mid-turn: no drain, no release save" \
    "$([ "$c" = 200 ] && [ "$alive" = 0 ] && [ "$(turn_ends)" = "$ends" ] && ! traced "$RUN/host-1.trace.jsonl" host.drain_start && [ "$(saves_with host-1 release)" = 0 ] && echo 1)" \
    "msg HTTP $c; killed $H1_TREE at $t_kill, $alive alive; turn ends $(turn_ends) (before $ends); release saves $(saves_with host-1 release)"

# 4. moved after K2_HOST_MOVE_AFTER; resumes on host-2, which restores the last turn's snapshot
wait_for 90 sh -c "grep -q 'placed agent $AGENT on host host-2' '$RUN/controller.log'"
wait_for 30 traced "$RUN/host-2.trace.jsonl" host.home_restored
wait_for 30 sh -c "[ -n \"\$(find '$ROOT/h2/home/.claude/projects' -path '*-notes/*' -name '$SESSION.jsonl' 2>/dev/null)\" ]"
read -r rseq rfetched <<< "$(last_restore host-2)"
EX2=$(execs_in "$ROOT/work-h2" | tail -1)
resumed2=$(echo "$EX2" | python3 -I -c '
import json,sys
e=json.loads(sys.stdin.read() or "{}"); a=e.get("attrs") or {}
print(1 if a.get("resumed") and e.get("session_id")==sys.argv[1] and "--resume" in (a.get("argv") or []) else 0)' "$SESSION")
moved=$(grep -c "\"event\":\"ctl.move_decision\"" "$RUN/controller.trace.jsonl" 2>/dev/null || true)
EPOCH2=$(placed_epoch)
check "the controller moves notes after host-1 is lost for ${MOVE_AFTER}s; host-2 restores the last turn's snapshot (fetched > 0) and resumes the same session at a new epoch" \
    "$([ "$moved" -ge 1 ] && [ "$(placed_on)" = host-2 ] && [ "$EPOCH2" -gt "$EPOCH1" ] && [ "$rseq" = "$seq2" ] && [ "$rfetched" -gt 0 ] && [ "$resumed2" = 1 ] && echo 1)" \
    "move decisions $moved; on $(placed_on) epoch $EPOCH2 (was $EPOCH1); restored seq $rseq (last turn save $seq2) fetched $rfetched; exec $(echo "$EX2" | head -c 300)"

# 5. host-2 has the home the second turn left; only the turn in progress is lost
ws_state "$WS2" > "$RUN/workspace-on-host-2.txt"
M2h2=$(memory_of h2); T2h2=$(transcript_of h2)
mem_prefix=$(head -c "$MEM2_BYTES" "$M2h2" 2>/dev/null | shasum -a 256 | cut -d' ' -f1)
tr_two=$(grep -c "turn two" "$T2h2" 2>/dev/null || true)
check "host-2 has the second turn's home byte for byte (workspace, MEMORY.md, transcript); the killed turn's change is not there" \
    "$(cmp -s "$RUN/workspace-after-turn-two.txt" "$RUN/workspace-on-host-2.txt" && [ ! -e "$WS2/turn3-wip.txt" ] && [ -f "$WS2/turn2.txt" ] && [ "$mem_prefix" = "$MEM2_SHA" ] && [ "$tr_two" -ge 1 ] && echo 1)" \
    "workspace $(cmp -s "$RUN/workspace-after-turn-two.txt" "$RUN/workspace-on-host-2.txt" && echo same || echo DIFFERENT); turn3-wip.txt $([ -e "$WS2/turn3-wip.txt" ] && echo PRESENT || echo absent); MEMORY.md (first $MEM2_BYTES bytes) $mem_prefix vs $MEM2_SHA; transcript mentions turn two $tr_two time(s)"

# 6. a message after the move lands on host-2, with the memory of turns one and two
c=$(msg "fourth [[remember: turn four]]" false)
wait_for 60 has_line "$(memory_of h2)" "turn four"
M4=$(memory_of h2)
check "a message after the move lands on host-2, which remembers turns one and two" \
    "$([ "$c" = 200 ] && has_line "$M4" "turn one" && has_line "$M4" "turn two" && has_line "$M4" "turn four" && echo 1)" \
    "msg HTTP $c; memory $(tr '\n' '|' < "$M4" 2>/dev/null)"

# 7. host-1 restarts: notes stays on host-2, one live CLI
H1_PID=$(start_host host-1 h1 "$H1_PORT")
wait_ready host-1 "$H1_PID" "$H1_PORT" "$RUN/host-1.log" && wait_linked "$H1_PORT" || exit 1
most=0
for _ in $(seq 1 20); do
    live=0
    for pid in $( (execs_in "$ROOT/work-h1"; execs_in "$ROOT/work-h2") | python3 -I -c '
import json,sys
for l in sys.stdin:
    print(json.loads(l).get("pid",""))'); do
        [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null && live=$((live + 1))
    done
    [ "$live" -gt "$most" ] && most=$live
    sleep 0.5
done
check "host-1 restarts: notes stays on host-2, one live agent CLI (I1)" \
    "$([ "$(placed_on)" = host-2 ] && [ "$most" = 1 ] && echo 1)" \
    "on $(placed_on); at most $most live CLI(s) over 10 s"

ctl_get /cli/hosts/home/status > "$RUN/home-status-final.json"
grep -hE '\[hosts/(home|turn_save|drain)\]' "$RUN"/host-*.log > "$RUN/home.log" 2>/dev/null || true
grep -hE '\[hosts\] .*(lost|moves|placed agent)' "$RUN/controller.log" > "$RUN/move.log" 2>/dev/null || true
rm -rf "$RUN/bin"
echo "evidence: $RUN"
if [ "$FAILS" = 0 ]; then echo "turn-snapshots: PASS"; else echo "turn-snapshots: $FAILS check(s) failed"; exit 1; fi
