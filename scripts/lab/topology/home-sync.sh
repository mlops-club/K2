#!/usr/bin/env bash
# P3 T1 home sync on three real processes: a K2_ROLE=controller, host-1 and
# host-2 (K2_ROLE=host, each its own throwaway HOME, K2_WORK_ROOT set,
# enrolled with W5 one-time tokens, so the home transfers authenticate with
# the hosts' signed nonces), k2-fake-agent as `claude`. An agent's home survives a drain to another
# host, then a sleep and a move back.
#
# Checks:
#   1. notes is placed on host-1 and its first turn ("remember …") writes
#      MEMORY.md and its transcript under host-1's HOME; its workspace is a
#      git repo with two commits, a dirty file and an untracked file
#   2. SIGTERM host-1: it drains, saves notes' home (host.home_saved), says
#      released, and exits by itself; the controller holds snapshot 1
#   3. the workspace is deleted (host-2 is another machine) and notes is
#      woken with --resume while host-1 is down: it lands on host-2, which
#      restores snapshot 1 first (host.home_restored)
#   4. on host-2: the workspace byte for byte (git HEAD, history, dirty and
#      untracked state), MEMORY.md and the transcript as host-1 left them;
#      the CLI resumed the same session, and its new turn appended to memory
#   5. notes sleeps (v2 close): host-2 saves snapshot 2 before it closes, and
#      the close reply says so (home.seq = 2)
#   6. host-1 comes back, host-2 goes: notes wakes on host-1, which still
#      held snapshot 1 and now restores 2 over it: host-2's memory line is
#      there
#
# Ports: HS_CTL_PORT (default 17480), HS_H1_PORT (17481), HS_H2_PORT (17482);
# the script stops if one is taken.
# Evidence: lab-runs/home-sync/<UTC>/.

set -uo pipefail
REPO=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$REPO" || exit 1
BUILD=1
[ "${1:-}" = --no-build ] && BUILD=0
CTL_PORT=${HS_CTL_PORT:-17480}
H1_PORT=${HS_H1_PORT:-17481}
H2_PORT=${HS_H2_PORT:-17482}
for port in "$CTL_PORT" "$H1_PORT" "$H2_PORT"; do
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "home-sync: port $port is taken ($(lsof -nP -iTCP:"$port" -sTCP:LISTEN | awk 'NR==2{print $1, $2}')); set HS_CTL_PORT/HS_H1_PORT/HS_H2_PORT" >&2
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
RUN="$REPO/lab-runs/home-sync/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN/bin"
cp "$target/debug/k2-daemon" "$target/debug/k2-fake-agent" "$RUN/bin/" || exit 1
DAEMON="$RUN/bin/k2-daemon"
FAKE="$RUN/bin/k2-fake-agent"

# A short root: ptyd's socket path must fit in sun_path (104 bytes on macOS).
ROOT=$(mktemp -d "${LAB_SHORT_TMP:-/tmp}/k2hs-XXXXXX") || exit 1
ROOT=$(cd "$ROOT" && pwd -P)
mkdir -p "$ROOT"/{ctl,h1,h2}/{home,tmp,run} "$ROOT/shim" "$ROOT/work"
head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$ROOT/bootstrap-token"
"$FAKE" --install-shims "$ROOT/shim" || exit 1
CTL_PID="" H1_PID="" H2_PID=""
WS="$ROOT/work/notes"
SESSION=$(uuidgen | tr A-Z a-z)

down() {
    local pid i
    for pid in "$H1_PID" "$H2_PID" "$CTL_PID"; do
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
    local line="PASS $1 -- $3"
    if [ "$2" != 1 ]; then line="FAIL $1 -- $3"; FAILS=$((FAILS + 1)); fi
    echo "$line" | tee -a "$RUN/checks.txt"
}

wait_ready() { # name pid port log
    local i phase=""
    for i in $(seq 1 300); do
        if ! kill -0 "$2" 2>/dev/null; then
            echo "home-sync: $1 exited during boot: $(tail -3 "$4" | tr '\n' ' ')" >&2
            return 1
        fi
        phase=$(curl -s --connect-timeout 1 "http://127.0.0.1:$3/boot-status" 2>/dev/null \
            | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("phase",""))' 2>/dev/null || true)
        [ "$phase" = ready ] && return 0
        sleep 0.2
    done
    echo "home-sync: $1 never reached boot phase ready in 60 s (last: '$phase')" >&2
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
    echo "home-sync: host on :$1 never linked (linked=$linked)" >&2
    return 1
}

start_ctl() {
    env -i PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
        HOME="$ROOT/ctl/home" K2_HOME="$ROOT/ctl/home/.k2" \
        TMPDIR="$ROOT/ctl/tmp" XDG_RUNTIME_DIR="$ROOT/ctl/run" K2_RUNTIME_DIR= \
        USER="$(id -un)" LANG=en_US.UTF-8 \
        K2_ROLE=controller K2_HOSTS=1 K2_HOST_BOOTSTRAP_TOKEN_FILE="$ROOT/bootstrap-token" K2_HOST_AUTH=enroll \
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
        K2_WORK_ROOT="$ROOT/work" K2_HOST_DRAIN_SECS=60 \
        K2_DAEMON_PORT="$3" K2_TRACE_FILE="$RUN/$1.trace.jsonl" \
        K2_TEST_AGENT_SHIM_DIR="$ROOT/shim" K2_FAKE_AGENT_TRACE="$RUN/fake-agent.trace.jsonl" \
        K2_FAKE_AGENT_TURN_MS="${LAB_TURN_MS:-200}" K2_FAKE_AGENT_SEED="${LAB_SEED:-0}" \
        "$DAEMON" >>"$RUN/$1.log" 2>&1 &
    echo $!
}

ctl_get() { curl -s "http://127.0.0.1:$CTL_PORT$1?token=$CTL_TOKEN"; }

post() { # path json → HTTP code; the body in $RUN/post.out
    curl -s -o "$RUN/post.out" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        "http://127.0.0.1:$CTL_PORT$1?token=$CTL_TOKEN" -d "$2"
}

spawn() { # args (a JSON array) → HTTP code
    post /cli/sessions/v2/spawn "{\"agent_name\":\"notes\",\"cwd\":\"$WS\",\"command\":\"claude\",\"args\":$1}"
}

placed_on() { grep -E "\[hosts\] placed agent notes on host " "$RUN/controller.log" | tail -1 | sed -E 's/.* on host ([^ ]+) .*/\1/'; }

# The fake agent's files under a host's HOME (the same layout as Claude's).
memory_of() { find "$ROOT/$1/home/.claude/projects" -path '*/memory/MEMORY.md' 2>/dev/null | head -1; }
transcript_of() { find "$ROOT/$1/home/.claude/projects" -name "$SESSION.jsonl" 2>/dev/null | head -1; }
sha() { [ -f "$1" ] && shasum -a 256 "$1" | cut -d' ' -f1; }

wait_for() { # seconds command…: until the command succeeds
    local n=$(( $1 * 10 )); shift
    for _ in $(seq 1 "$n"); do "$@" && return 0; sleep 0.1; done
    return 1
}
has_line() { [ -f "$1" ] && grep -q "$2" "$1"; }
traced() { [ -f "$1" ] && grep -q "\"event\":\"$2\"" "$1"; }

git_ws() {
    env -i PATH=/usr/bin:/bin HOME="$ROOT/ctl/home" GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null \
        GIT_AUTHOR_NAME=lab GIT_AUTHOR_EMAIL=lab@example.invalid GIT_COMMITTER_NAME=lab GIT_COMMITTER_EMAIL=lab@example.invalid \
        git -C "$WS" "$@"
}
ws_state() { # HEAD, history, porcelain status, and the files' hashes (no .git internals)
    git_ws rev-parse HEAD
    git_ws log --format='%H %s'
    git_ws status --porcelain
    (cd "$WS" && find . -path ./.git -prune -o -type f -print | LC_ALL=C sort | xargs shasum -a 256)
}

# ── the workspace ──────────────────────────────────────────────────────
mkdir -p "$WS"
git_ws init -q -b main
printf '# notes\n' > "$WS/README.md"
printf 'one\n' > "$WS/lib.txt"
git_ws add -A && git_ws commit -q -m first
printf 'two\n' > "$WS/lib.txt"
git_ws commit -q -am second
printf 'three (dirty)\n' > "$WS/lib.txt"
printf 'not added\n' > "$WS/untracked.md"

start_ctl || exit 1
wait_for 30 test -s "$ROOT/ctl/home/.k2/daemon.token" || { echo "home-sync: no controller token" >&2; exit 1; }
CTL_TOKEN=$(cat "$ROOT/ctl/home/.k2/daemon.token")
H1_PID=$(start_host host-1 h1 "$H1_PORT")
wait_ready host-1 "$H1_PID" "$H1_PORT" "$RUN/host-1.log" && wait_linked "$H1_PORT" || exit 1
H2_PID=$(start_host host-2 h2 "$H2_PORT")
wait_ready host-2 "$H2_PID" "$H2_PORT" "$RUN/host-2.log" && wait_linked "$H2_PORT" || exit 1

# 1. notes on host-1, one turn
c=$(spawn "[\"--session-id\",\"$SESSION\",\"remember before the drain\"]")
wait_for 30 has_line "$(memory_of h1)" "before the drain"
M1=$(memory_of h1); T1=$(transcript_of h1)
check "notes placed on host-1; its first turn wrote MEMORY.md and the transcript there" \
    "$([ "$c" = 200 ] && [ "$(placed_on)" = host-1 ] && has_line "$M1" "before the drain" && [ -f "$T1" ] && echo 1)" \
    "HTTP $c, on $(placed_on); memory ${M1:-none}; transcript ${T1:-none}"
MEM1_SHA=$(sha "$M1"); TR1_SHA=$(sha "$T1")
ws_state > "$RUN/workspace-on-host-1.txt"

# 2. drain host-1
kill -TERM "$H1_PID"
wait_for 90 sh -c "! kill -0 $H1_PID 2>/dev/null"
gone=$(kill -0 "$H1_PID" 2>/dev/null && echo 0 || echo 1)
wait "$H1_PID" 2>/dev/null; H1_PID=""
ctl_get /cli/hosts/home/status > "$RUN/home-status-1.json"
seq1=$(python3 -I -c 'import json,sys; s=[x for x in json.load(open(sys.argv[1]))["snapshots"] if x["agent"]=="notes"]; print(s[0]["seq"] if s else "")' "$RUN/home-status-1.json")
check "SIGTERM host-1: it saved notes' home, released it and exited by itself; the controller holds snapshot 1" \
    "$([ "$gone" = 1 ] && traced "$RUN/host-1.trace.jsonl" host.home_saved && traced "$RUN/host-1.trace.jsonl" host.drain_release && [ "$seq1" = 1 ] && echo 1)" \
    "exited $gone; status $(cat "$RUN/home-status-1.json")"

# 3. another machine: no workspace; notes wakes with --resume while host-1 is down
rm -rf "$WS"
c=$(spawn "[\"--resume\",\"$SESSION\",\"remember after the move\"]")
wait_for 30 has_line "$(memory_of h2)" "after the move"
check "notes wakes on host-2 (host-1 down), which restores snapshot 1 before the spawn" \
    "$([ "$c" = 200 ] && [ "$(placed_on)" = host-2 ] && traced "$RUN/host-2.trace.jsonl" host.home_restored && echo 1)" \
    "HTTP $c $(head -c 300 "$RUN/post.out"); on $(placed_on)"

# 4. byte for byte, and the same conversation
ws_state > "$RUN/workspace-on-host-2.txt"
M2=$(memory_of h2); T2=$(transcript_of h2)
first_part=$(head -c "$(wc -c < "$M1")" "$M2" | shasum -a 256 | cut -d' ' -f1)
resumed=$(python3 -I -c '
import json,sys
ev=[json.loads(l) for l in open(sys.argv[1])]
print(sum(1 for e in ev if e.get("event")=="cli.exec" and (e.get("attrs") or {}).get("resumed") and e.get("session_id")==sys.argv[2]))' \
    "$RUN/fake-agent.trace.jsonl" "$SESSION")
check "host-2 has the workspace byte for byte (git HEAD, history, dirty, untracked), MEMORY.md and the transcript; same session resumed" \
    "$(cmp -s "$RUN/workspace-on-host-1.txt" "$RUN/workspace-on-host-2.txt" && [ "$first_part" = "$MEM1_SHA" ] && has_line "$M2" "after the move" && [ -f "$T2" ] && [ "${resumed:-0}" -ge 1 ] && echo 1)" \
    "workspace $(cmp -s "$RUN/workspace-on-host-1.txt" "$RUN/workspace-on-host-2.txt" && echo same || echo DIFFERENT); MEMORY.md (host-1 part) $first_part vs $MEM1_SHA; transcript ${T2:-none} (host-1's sha $TR1_SHA); resumed execs $resumed"

# 5. notes sleeps: host-2 saves before it closes
c=$(post /cli/sessions/v2/close '{"agent_name":"notes","force":true}')
home_seq=$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1])).get("home",{}).get("seq",""))' "$RUN/post.out" 2>/dev/null)
check "notes sleeps (v2 close): host-2 saved snapshot 2 first, and the close reply says so" \
    "$([ "$c" = 200 ] && [ "$home_seq" = 2 ] && echo 1)" "HTTP $c $(cat "$RUN/post.out")"

# 6. host-1 back, host-2 gone: notes wakes on host-1, which restores 2 over its 1
H1_PID=$(start_host host-1 h1 "$H1_PORT")
wait_ready host-1 "$H1_PID" "$H1_PORT" "$RUN/host-1.log" && wait_linked "$H1_PORT" || exit 1
kill -TERM "$H2_PID"; wait_for 90 sh -c "! kill -0 $H2_PID 2>/dev/null"; wait "$H2_PID" 2>/dev/null; H2_PID=""
c=$(spawn "[\"--resume\",\"$SESSION\"]")
wait_for 30 has_line "$(memory_of h1)" "after the move"
restored2=$(grep -c '"event":"host.home_restored"' "$RUN/host-1.trace.jsonl" 2>/dev/null)
check "notes wakes on host-1 again, with host-2's turn in its memory (snapshot 2 over 1)" \
    "$([ "$c" = 200 ] && [ "$(placed_on)" = host-1 ] && has_line "$(memory_of h1)" "after the move" && [ "${restored2:-0}" -ge 1 ] && echo 1)" \
    "HTTP $c; on $(placed_on); memory $(tr '\n' '|' < "$(memory_of h1)")"

ctl_get /cli/hosts/home/status > "$RUN/home-status-final.json"
grep -hE '\[hosts/(home|drain)\]' "$RUN"/host-*.log > "$RUN/home-sync.log" 2>/dev/null || true
rm -rf "$RUN/bin"
echo "evidence: $RUN"
if [ "$FAILS" = 0 ]; then echo "home-sync: PASS"; else echo "home-sync: $FAILS check(s) failed"; exit 1; fi
