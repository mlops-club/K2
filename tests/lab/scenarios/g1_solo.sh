#!/usr/bin/env bash
# G1 on solo (PRD "K2 Distributed Runtime", gate G1): boot the solo daemon
# with the fake agent and K2_TRACE_FILE, run S1 and a CLI crash (S7), and
# print the flow checker's verdict for the spawn, hook and crash flows.
#
#   tests/lab/scenarios/g1_solo.sh            # the G1 trace check
#   tests/lab/scenarios/g1_solo.sh --bench 9  # spawn latency, trace off vs on
#
# Reuses the lab unchanged: scripts/lab/lib/toolchain.sh,
# scripts/lab/topology/solo.sh, tests/lab/labkit. solo.sh starts the daemon
# under `env -i`, so the daemon binary it is given is a two-line wrapper
# that sets K2_TRACE_FILE and execs the real binary (same pid).
# Output: lab-runs/G1-trace/<ts>/ (trace.jsonl, flows.json, logs).
set -uo pipefail

LAB_REPO=$(cd "$(dirname "$0")/../../.." && pwd)
export LAB_REPO
BENCH=0
case "${1:-}" in
    --bench) BENCH=${2:?--bench needs a run count} ;;
    "") ;;
    *) echo "usage: $0 [--bench N]" >&2; exit 2 ;;
esac

RUN="$LAB_REPO/lab-runs/G1-trace/$(date -u +%Y-%m-%dT%H-%M-%SZ)"
mkdir -p "$RUN/bin"
say() { echo "g1: $*"; }

# shellcheck source=../../../scripts/lab/lib/toolchain.sh
source "$LAB_REPO/scripts/lab/lib/toolchain.sh"
# shellcheck source=../../../scripts/lab/topology/solo.sh
source "$LAB_REPO/scripts/lab/topology/solo.sh"

say "building k2-daemon and k2-fake-agent (build.log)"
if ! lab_toolchain "$LAB_REPO" >>"$RUN/build.log" 2>&1 \
    || ! (cd "$LAB_REPO" && cargo build -p k2-daemon --bin k2-daemon -p k2-fake-agent) >>"$RUN/build.log" 2>&1; then
    say "build failed: $RUN/build.log"
    exit 1
fi
cp "$LAB_REPO/target/debug/k2-daemon" "$LAB_REPO/target/debug/k2-fake-agent" "$RUN/bin/"
LAB_FAKE_AGENT_BIN="$RUN/bin/k2-fake-agent"
export LAB_FAKE_AGENT_BIN

# One solo daemon; $1 = phase dir, $2 = trace file or "" (trace off).
up() {
    local dir=$1 trace=$2
    mkdir -p "$dir"
    if [ -n "$trace" ]; then
        printf '#!/bin/sh\nK2_TRACE_FILE=%s exec %s "$@"\n' "$trace" "$RUN/bin/k2-daemon" > "$dir/k2-daemon"
        chmod +x "$dir/k2-daemon"
        LAB_DAEMON_BIN="$dir/k2-daemon"
    else
        LAB_DAEMON_BIN="$RUN/bin/k2-daemon"
    fi
    export LAB_DAEMON_BIN
    topo_up "$dir" || { say "solo did not come up ($dir/daemon.log)"; return 1; }
    LAB_CRASH_CWD="$LAB_HOME/work/lab-crash"
    mkdir -p "$LAB_CRASH_CWD"
    git -C "$LAB_CRASH_CWD" init -q
    printf '%s' "$LAB_TOKEN" > "$dir/token"
    chmod 600 "$dir/token"
}

kit() {
    local dir=$1
    shift
    python3 -I "$LAB_REPO/tests/lab/scenarios/g1_solo.py" --port "$LAB_PORT" --token-file "$dir/token" \
        --home "$LAB_HOME" --cwd "$LAB_WORKSPACE" --crash-cwd "$LAB_CRASH_CWD" \
        --daemon-trace "$dir/daemon.trace.jsonl" --fake-trace "$LAB_FAKE_TRACE" \
        --flows-dir "$LAB_REPO/docs/prd/flows" --out "$dir/out" "$@"
}

trap 'topo_down' EXIT

if [ "$BENCH" -gt 0 ]; then
    rc=0
    for phase in off on; do
        dir="$RUN/bench-$phase"
        trace=""
        [ "$phase" = on ] && trace="$dir/daemon.trace.jsonl"
        up "$dir" "$trace" || exit 1
        say "S1 x$BENCH, trace $phase"
        kit "$dir" --bench "$BENCH" | tee "$dir/bench.txt" || rc=1
        topo_down
    done
    python3 -I - "$RUN/bench-off/out/bench.json" "$RUN/bench-on/out/bench.json" <<'EOF' || rc=1
import json, sys
off, on = (json.load(open(p))["median"] for p in sys.argv[1:3])
delta = (on - off) / off * 100
print("g1: spawn_to_banner_ms median: trace off %.1f, trace on %.1f, %+.1f%% (NFR1: within 5%%)" % (off, on, delta))
sys.exit(0 if delta <= 5 else 1)
EOF
    exit $rc
fi

dir="$RUN/solo"
up "$dir" "$dir/daemon.trace.jsonl" || exit 1
say "solo daemon on port $LAB_PORT, K2_TRACE_FILE=$dir/daemon.trace.jsonl"
kit "$dir"
rc=$?
say "bundle: $dir/out (trace.jsonl, flows.json)"
exit $rc
