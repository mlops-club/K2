#!/usr/bin/env bash
# fs-relay on two real processes: a K2_ROLE=controller (its own work root,
# K2_WORK_ROOT=<root>/work-ctl) and host-1 (K2_ROLE=host, its own disk,
# K2_WORK_ROOT=<root>/work-h1), k2-fake-agent as `claude`. The app's file
# browser and git panel, called through the controller with the
# controller's paths (work-ctl/notes), show and change the agent's
# workspace on the host's disk (work-h1/notes), never the controller's copy.
# The controller decides host-1's work root (K2_HOST_WORK_ROOTS); the
# workspace is bound to host-1 by the spawn the controller asked for.
# fs-relay is off by default: the controller turns it on (K2_FS_RELAY=1).
#
# Checks:
#   1. host-1 links with the k2.work_root label (it is on its own disk)
#   2. notes is placed on host-1, its workspace a git repo there (two
#      commits, a changed file, an untracked file); the controller's own
#      work-ctl/notes holds only a decoy
#   3. file browser: read-dir and read-file of work-ctl/notes show the
#      host's files, every path in the controller's namespace
#   4. git panel: changes and diff-file show the host's working tree
#   5. writes: write-file, stage-all and commit through the controller land
#      in the host's repository (its HEAD), the controller's copy untouched
#   6. a path in no agent's workspace stays on the controller's own disk
#   7. traces: ctl.fs_relay on the controller, host.fs_served on the host
#
#   scripts/lab/topology/fs-relay.sh [--no-build]
# Ports: FR_CTL_PORT (default 17490), FR_H1_PORT (17491).
# Evidence: lab-runs/fs-relay/<UTC>/.

set -uo pipefail
REPO=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$REPO" || exit 1
BUILD=1
[ "${1:-}" = --no-build ] && BUILD=0
CTL_PORT=${FR_CTL_PORT:-17490}
H1_PORT=${FR_H1_PORT:-17491}
for port in "$CTL_PORT" "$H1_PORT"; do
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "fs-relay: port $port is taken; set FR_CTL_PORT/FR_H1_PORT" >&2
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
RUN="$REPO/lab-runs/fs-relay/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN/bin"
cp "$target/debug/k2-daemon" "$target/debug/k2-fake-agent" "$RUN/bin/" || exit 1
DAEMON="$RUN/bin/k2-daemon"
FAKE="$RUN/bin/k2-fake-agent"

# A short root: ptyd's socket path must fit in sun_path (104 bytes on macOS).
ROOT=$(mktemp -d "${LAB_SHORT_TMP:-/tmp}/k2fr-XXXXXX") || exit 1
ROOT=$(cd "$ROOT" && pwd -P)
mkdir -p "$ROOT"/{ctl,h1}/{home,tmp,run} "$ROOT/shim" "$ROOT/work-ctl" "$ROOT/work-h1"
head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$ROOT/bootstrap-token"
"$FAKE" --install-shims "$ROOT/shim" || exit 1
CTL_PID="" H1_PID=""
WS_CTL="$ROOT/work-ctl/notes"   # the app's path (the controller's namespace)
WS_H1="$ROOT/work-h1/notes"     # the agent's workspace, on host-1's disk

down() {
    local pid i
    for pid in "$H1_PID" "$CTL_PID"; do
        [ -n "$pid" ] || continue
        kill "$pid" 2>/dev/null || true
        for i in $(seq 1 25); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
        kill -9 "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    [ -n "${ROOT:-}" ] && pkill -9 -f "$ROOT/" 2>/dev/null || true
    [ -n "${ROOT:-}" ] && rm -rf "$ROOT"
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
            echo "fs-relay: $1 exited during boot: $(tail -3 "$4" | tr '\n' ' ')" >&2
            return 1
        fi
        phase=$(curl -s --connect-timeout 1 "http://127.0.0.1:$3/boot-status" 2>/dev/null \
            | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("phase",""))' 2>/dev/null || true)
        [ "$phase" = ready ] && return 0
        sleep 0.2
    done
    echo "fs-relay: $1 never reached boot phase ready (last: '$phase')" >&2
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
    echo "fs-relay: host on :$1 never linked (linked=$linked)" >&2
    return 1
}

git_in() { # dir args…
    local d=$1; shift
    env -i PATH=/usr/bin:/bin HOME="$ROOT/ctl/home" GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null \
        GIT_AUTHOR_NAME=lab GIT_AUTHOR_EMAIL=lab@example.invalid GIT_COMMITTER_NAME=lab GIT_COMMITTER_EMAIL=lab@example.invalid \
        git -C "$d" "$@"
}

# ── the agent's workspace, on host-1's disk; the controller's decoy ─────
mkdir -p "$WS_H1" "$WS_CTL"
git_in "$WS_H1" init -q -b main
git_in "$WS_H1" config user.name lab && git_in "$WS_H1" config user.email lab@example.invalid
printf '# notes\n' > "$WS_H1/README.md"
printf 'one\n' > "$WS_H1/lib.txt"
git_in "$WS_H1" add -A && git_in "$WS_H1" commit -q -m first
printf 'two\n' > "$WS_H1/lib.txt"
git_in "$WS_H1" commit -q -am second
printf 'three (changed by the agent)\n' > "$WS_H1/lib.txt"
printf 'not added\n' > "$WS_H1/untracked.md"
printf 'the controller has only this\n' > "$WS_CTL/controller-copy.md"
mkdir -p "$ROOT/work-ctl/other" && printf 'mine\n' > "$ROOT/work-ctl/other/on-the-controller.md"

env -i PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
    HOME="$ROOT/ctl/home" K2_HOME="$ROOT/ctl/home/.k2" \
    TMPDIR="$ROOT/ctl/tmp" XDG_RUNTIME_DIR="$ROOT/ctl/run" K2_RUNTIME_DIR= \
    USER="$(id -un)" LANG=en_US.UTF-8 \
    K2_ROLE=controller K2_HOSTS=1 K2_HOST_BOOTSTRAP_TOKEN_FILE="$ROOT/bootstrap-token" K2_HOST_AUTH=enroll \
    K2_WORK_ROOT="$ROOT/work-ctl" K2_HOST_WORK_ROOTS="host-1=$ROOT/work-h1" K2_FS_RELAY=1 \
    K2_DAEMON_PORT="$CTL_PORT" K2_TRACE_FILE="$RUN/controller.trace.jsonl" \
    "$DAEMON" >>"$RUN/controller.log" 2>&1 &
CTL_PID=$!
wait_ready controller "$CTL_PID" "$CTL_PORT" "$RUN/controller.log" || exit 1
for i in $(seq 1 150); do [ -s "$ROOT/ctl/home/.k2/daemon.token" ] && break; sleep 0.2; done
CTL_TOKEN=$(cat "$ROOT/ctl/home/.k2/daemon.token")
curl -s -X POST -H 'Content-Type: application/json' "http://127.0.0.1:$CTL_PORT/cli/hosts/enroll-tokens?token=$CTL_TOKEN" \
    -d '{"ttl_secs": 600}' | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["token"])' > "$ROOT/h1-token"
env -i PATH="$ROOT/shim:/usr/bin:/bin:/usr/sbin:/sbin" \
    HOME="$ROOT/h1/home" K2_HOME="$ROOT/h1/home/.k2" \
    TMPDIR="$ROOT/h1/tmp" XDG_RUNTIME_DIR="$ROOT/h1/run" K2_RUNTIME_DIR= \
    USER="$(id -un)" LANG=en_US.UTF-8 \
    K2_ROLE=host K2_CONTROLLER_URL="ws://127.0.0.1:$CTL_PORT" \
    K2_HOST_AUTH=enroll K2_HOST_ENROLL_TOKEN_FILE="$ROOT/h1-token" K2_HOST_NAME=host-1 \
    K2_WORK_ROOT="$ROOT/work-h1" \
    K2_DAEMON_PORT="$H1_PORT" K2_TRACE_FILE="$RUN/host-1.trace.jsonl" \
    K2_TEST_AGENT_SHIM_DIR="$ROOT/shim" K2_FAKE_AGENT_TRACE="$RUN/fake-agent.trace.jsonl" \
    "$DAEMON" >>"$RUN/host-1.log" 2>&1 &
H1_PID=$!
wait_ready host-1 "$H1_PID" "$H1_PORT" "$RUN/host-1.log" && wait_linked "$H1_PORT" || exit 1

enc() { python3 -I -c 'import sys,urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"; }
get() { # path query → body in $RUN/get.out; prints the HTTP code
    curl -s -o "$RUN/get.out" -w '%{http_code}' "http://127.0.0.1:$CTL_PORT$1?$2&token=$CTL_TOKEN"
}
post() { # path json → body in $RUN/post.out; prints the HTTP code
    curl -s -o "$RUN/post.out" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        "http://127.0.0.1:$CTL_PORT$1?token=$CTL_TOKEN" -d "$2"
}
js() { python3 -I -c "import json,sys; d=json.load(open(sys.argv[1])); print($1)" "$2" 2>/dev/null; }

# 1.
curl -s "http://127.0.0.1:$CTL_PORT/cli/hosts/status?token=$CTL_TOKEN" > "$RUN/status.json"
label=$(js 'next((l for h in d["hosts"] for l in h.get("labels",[]) if l.startswith("k2.work_root=")), "")' "$RUN/status.json")
check "host-1 links with k2.work_root (its own disk)" "$([ "$label" = "k2.work_root=$ROOT/work-h1" ] && echo 1)" "label: ${label:-none}"

# 2.
c=$(post /cli/sessions/v2/spawn "{\"agent_name\":\"notes\",\"cwd\":\"$WS_CTL\",\"command\":\"claude\",\"args\":[\"--session-id\",\"$(uuidgen | tr A-Z a-z)\"]}")
on=$(grep -E "\[hosts\] placed agent notes on host " "$RUN/controller.log" | tail -1 | sed -E 's/.* on host ([^ ]+) .*/\1/')
check "notes placed on host-1, its workspace a git repo on host-1's disk; the controller holds only a decoy" \
    "$([ "$c" = 200 ] && [ "$on" = host-1 ] && [ -d "$WS_H1/.git" ] && [ ! -d "$WS_CTL/.git" ] && echo 1)" "HTTP $c, on ${on:-nothing}"

# 3. the file browser
c1=$(get /cli/fs/read-dir "path=$(enc "$WS_CTL")"); cp "$RUN/get.out" "$RUN/read-dir.json"
names=$(js '" ".join(sorted(e["name"] for e in d))' "$RUN/read-dir.json")
paths_ok=$(python3 -I -c 'import json,sys; d=json.load(open(sys.argv[1])); print(1 if d and all(e["path"].startswith(sys.argv[2] + "/") for e in d) else 0)' "$RUN/read-dir.json" "$WS_CTL" 2>/dev/null)
c2=$(get /cli/fs/read-file "path=$(enc "$WS_CTL/lib.txt")"); cp "$RUN/get.out" "$RUN/read-file.json"
check "file browser: work-ctl/notes through the controller shows host-1's files, paths in the controller's namespace" \
    "$([ "$c1" = 200 ] && echo " $names " | grep -q ' README.md lib.txt untracked.md ' && ! echo "$names" | grep -q controller-copy && [ "$paths_ok" = 1 ] && [ "$c2" = 200 ] && grep -q 'changed by the agent' "$RUN/read-file.json" && echo 1)" \
    "read-dir HTTP $c1: $names (paths ok: $paths_ok); read-file HTTP $c2: $(head -c 120 "$RUN/read-file.json")"

# 4. the git panel
c1=$(get /cli/git/changes "path=$(enc "$WS_CTL")"); cp "$RUN/get.out" "$RUN/changes.json"
c2=$(get /cli/git/diff-file "path=$(enc "$WS_CTL")&file_path=lib.txt"); cp "$RUN/get.out" "$RUN/diff-file.json"
check "git panel: changes and diff-file show host-1's working tree" \
    "$([ "$c1" = 200 ] && grep -q lib.txt "$RUN/changes.json" && grep -q untracked.md "$RUN/changes.json" && [ "$c2" = 200 ] && grep -q 'changed by the agent' "$RUN/diff-file.json" && echo 1)" \
    "changes HTTP $c1: $(head -c 200 "$RUN/changes.json"); diff-file HTTP $c2"

# 5. writes and a commit
c1=$(post /cli/fs/write-file "{\"path\":\"$WS_CTL/from-the-app.md\",\"content\":\"edited in the app\\n\"}")
c2=$(post /cli/git/stage-all "{\"path\":\"$WS_CTL\"}")
c3=$(post /cli/git/commit "{\"path\":\"$WS_CTL\",\"message\":\"through the controller\"}")
head=$(git_in "$WS_H1" log -1 --format=%s 2>/dev/null)
check "writes: write-file, stage-all and commit through the controller land in host-1's repository; the controller's copy untouched" \
    "$([ "$c1" = 200 ] && [ "$c2" = 200 ] && [ "$c3" = 200 ] && [ "$head" = "through the controller" ] && [ -f "$WS_H1/from-the-app.md" ] && [ ! -e "$WS_CTL/from-the-app.md" ] && echo 1)" \
    "write HTTP $c1, stage HTTP $c2, commit HTTP $c3 ($(head -c 160 "$RUN/post.out")); host-1 HEAD: '$head'; controller copy: $(ls "$WS_CTL" | tr '\n' ' ')"

# 6. no agent's workspace: the controller's own disk
c1=$(get /cli/fs/read-dir "path=$(enc "$ROOT/work-ctl/other")"); cp "$RUN/get.out" "$RUN/read-dir-local.json"
check "a path in no agent's workspace stays on the controller's own disk" \
    "$([ "$c1" = 200 ] && grep -q on-the-controller.md "$RUN/read-dir-local.json" && echo 1)" "HTTP $c1: $(head -c 160 "$RUN/read-dir-local.json")"

# 7. traces
relays=$(grep -c '"event":"ctl.fs_relay"' "$RUN/controller.trace.jsonl" 2>/dev/null)
served=$(grep -c '"event":"host.fs_served"' "$RUN/host-1.trace.jsonl" 2>/dev/null)
check "traces: ctl.fs_relay on the controller, host.fs_served on host-1, one per relayed request" \
    "$([ "${relays:-0}" -ge 7 ] && [ "$relays" = "$served" ] && echo 1)" "relayed $relays, served $served"

echo "evidence: $RUN"
rm -rf "$RUN/bin"
if [ "$FAILS" = 0 ]; then echo "fs-relay: PASS"; else echo "fs-relay: $FAILS check(s) failed"; exit 1; fi
