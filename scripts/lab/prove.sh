#!/usr/bin/env bash
# The K2 lab (PRD "K2 Distributed Runtime", section 3): one command that
# brings up a topology, runs the parity suites and the gate's scenarios,
# checks the invariants that apply, and writes an evidence bundle.
#
#   scripts/lab/prove.sh <gate> --topology <solo|container|fleet|kind>
#                        [--chaos seed=N] [--parity full|none] [--record-baseline]
#                        [--linux-gate hold|run]
#
# Implemented: G0 on solo; G2 on solo and fleet (docs/prd/progress/lab-g2.md).
# Anything else exits 2. LAB_FLEET_BACKEND=procs (default) or compose picks
# how fleet runs.
#
# Writes lab-runs/<gate>/<UTC timestamp>/ (open report.html first):
#   report.html, report.json   pass/fail matrix, machine-readable twin
#   parity/                    one log (and result JSON) per existing suite
#   scenarios/S1.*             log, result, trace, the client's screen (.cast)
#   scenarios/S2..S9.*         G2: result and log per scenario; messages.jsonl
#   traces/, samples.jsonl     G2: daemon and fake-agent traces, the fleet monitor
#   chaos.log                  G2 with --chaos: what was killed, when
#   flows.json                 the PRD flows against the trace
#   invariants.txt, metrics.csv, topology/, demo.cast (when asciinema is installed)
#
# Exit 0 only when every required item passed. --parity none is for
# iterating on scenarios: parity then shows NOT RUN and the gate fails.
# --record-baseline stores this run's parity results as the I5 baseline
# (tests/lab/baselines/<gate>-<topology>.json); commit it deliberately.
# --linux-gate run (default) builds scripts/lab/docker/gate.Dockerfile and
# runs test-gate-linux.sh in it AFTER the local suites (never alongside
# another heavy build); hold never touches Docker and reports it NOT RUN.
# CPU budget: CARGO_BUILD_JOBS=3 and 3 test threads unless set; Docker jobs
# take the shared lock $HOME/.cache/k2-dist-docker.lock (one at a time).
#
# Touches nothing outside the repo, lab-runs/ and temp dirs: the daemon and
# every suite run under throwaway HOMEs; Rust comes from PATH or is installed
# under lab-runs/.toolchain; the Linux gate runs in Docker with named volumes.
set -uo pipefail

usage() { sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2; }

GATE="" TOPOLOGY="" CHAOS="" PARITY=full RECORD_BASELINE=0 LAB_LINUX_GATE=run
while [ $# -gt 0 ]; do
    case "$1" in
        --topology) TOPOLOGY=${2:-}; shift 2 ;;
        --topology=*) TOPOLOGY=${1#*=}; shift ;;
        --chaos) CHAOS=${2:-}; shift 2 ;;
        --chaos=*) CHAOS=${1#*=}; shift ;;
        --parity) PARITY=${2:-}; shift 2 ;;
        --parity=*) PARITY=${1#*=}; shift ;;
        --record-baseline) RECORD_BASELINE=1; shift ;;
        --linux-gate) LAB_LINUX_GATE=${2:-}; shift 2 ;;
        --linux-gate=*) LAB_LINUX_GATE=${1#*=}; shift ;;
        -h|--help) usage ;;
        -*) echo "prove.sh: unknown option $1" >&2; usage ;;
        *) [ -z "$GATE" ] && GATE=$1 || { echo "prove.sh: unexpected argument $1" >&2; usage; }; shift ;;
    esac
done
[ -n "$GATE" ] && [ -n "$TOPOLOGY" ] || usage
case "$PARITY" in full|none) ;; *) echo "prove.sh: --parity is full or none" >&2; exit 2 ;; esac
case "$LAB_LINUX_GATE" in hold|run) ;; *) echo "prove.sh: --linux-gate is hold or run" >&2; exit 2 ;; esac
export LAB_LINUX_GATE
export LAB_TEST_THREADS=${LAB_TEST_THREADS:-3}
case "$CHAOS" in ""|seed=[0-9]*) ;; *) echo "prove.sh: --chaos takes seed=N" >&2; exit 2 ;; esac
# Under chaos a host is gone often; the outbox's gone grace (default 120 s)
# would outlast the scenario waits, so chaos runs set it to 30 s on every
# daemon (K2_DELIVERY_GONE_GRACE_SECS) and S8 waits grace + 30 s.
[ -n "$CHAOS" ] && export LAB_GONE_GRACE_S=${LAB_GONE_GRACE_S:-30}
# Scenarios tell chaos's own kills (chaos.log) from the daemons'.
if [ -n "$CHAOS" ]; then export LAB_CHAOS=$CHAOS; else unset LAB_CHAOS; fi
# LAB_HOST_PTYD=1: the hosts run their agents under k2-ptyd (K2_PTYD=1).
if [ "${LAB_HOST_PTYD:-0}" = 1 ] && [ "${LAB_FLEET_BACKEND:-procs}" = compose ]; then
    echo "prove.sh: LAB_HOST_PTYD=1 is for the procs backend (the fleet image has no k2-ptyd)" >&2; exit 2
fi
case "$TOPOLOGY" in solo|container|fleet|kind) ;; *) echo "prove.sh: unknown topology '$TOPOLOGY' (solo, container, fleet, kind)" >&2; exit 2 ;; esac
case "$GATE/$TOPOLOGY" in
    G0/solo|G2/solo|G2/fleet) ;;
    *) echo "prove.sh: $GATE on topology $TOPOLOGY is not implemented (G0 solo; G2 solo, fleet)" >&2; exit 2 ;;
esac
if [ "$RECORD_BASELINE" = 1 ] && { [ "$PARITY" != full ] || [ "$GATE/$TOPOLOGY" != G0/solo ]; }; then
    echo "prove.sh: --record-baseline needs G0 --topology solo --parity full" >&2; exit 2
fi

LAB_REPO=$(cd "$(dirname "$0")/../.." && pwd)
cd "$LAB_REPO" || exit 2
LAB_SHA=$(git rev-parse HEAD)
TS=$(date -u +%Y-%m-%dT%H-%M-%SZ)
RUN="$LAB_REPO/lab-runs/$GATE/$TS"
mkdir -p "$RUN/parity" "$RUN/scenarios" "$RUN/topology"
export LAB_REPO LAB_SHA RUN
LAB_TMP=$(mktemp -d "${TMPDIR:-/tmp}/k2lab-run-XXXXXX")
# I5 compares the parity suites with the pre-change baseline. The suites
# don't depend on the gate or the topology, so every gate uses G0's.
BASELINE="tests/lab/baselines/G0-solo.json"
NOTES=()
say() { printf '%s lab: %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$RUN/prove.log"; }

# Keep the machine-wide cargo slot's own knobs (K2_DIST_PRIORITY, K2_DIST_SLOTS):
# only ~/.cache/k2-dist-cargo-slot.sh reads them; suites and the daemon start
# under env -i anyway.
for v in $(env | grep -oE '^K2(SO)?_[A-Z0-9_]+' | grep -vxE 'K2_DIST_(PRIORITY|SLOTS)' || true); do unset "$v"; done

# run.json: merge key=value (value is JSON when it parses as JSON).
run_set() {
    python3 -I - "$RUN/run.json" "$@" <<'PY'
import json, os, sys
path, kv = sys.argv[1], sys.argv[2:]
doc = json.load(open(path)) if os.path.exists(path) else {}
for i in range(0, len(kv), 2):
    try:
        doc[kv[i]] = json.loads(kv[i + 1])
    except ValueError:
        doc[kv[i]] = kv[i + 1]
json.dump(doc, open(path, "w"), indent=2)
PY
}
json_list() { python3 -I -c 'import json,sys; print(json.dumps(sys.argv[1:]))' "$@"; }

run_set gate "$GATE" topology "$TOPOLOGY" sha "$LAB_SHA" branch "$(git rev-parse --abbrev-ref HEAD)" \
    crates_tree "$(git rev-parse HEAD:crates)" repo "$LAB_REPO" parity "$PARITY" chaos "${CHAOS:-}" \
    started "$(date -u +%Y-%m-%dT%H:%M:%SZ)" args "$(json_list "$GATE" --topology "$TOPOLOGY" ${CHAOS:+--chaos "$CHAOS"} --parity "$PARITY")" \
    baseline "$BASELINE" recorded_baseline "$([ $RECORD_BASELINE = 1 ] && echo true || echo false)"

if [ -n "$(git status --porcelain --untracked-files=no -- . ':!lab-runs')" ]; then
    NOTES+=("The working tree has uncommitted changes. Solo and the local suites ran on the working tree; the Linux gate ran on the committed sha $LAB_SHA.")
fi
[ -n "$CHAOS" ] && NOTES+=("--chaos: K2_DELIVERY_GONE_GRACE_SECS=$LAB_GONE_GRACE_S on every daemon (the outbox gone rule; default 120 s).")
if [ -n "$CHAOS" ] && [ "$GATE" = G0 ]; then
    NOTES+=("--chaos $CHAOS recorded; G0 has no chaos scenarios (chaos applies to S3 to S10).")
fi

source "$LAB_REPO/scripts/lab/lib/toolchain.sh"
source "$LAB_REPO/scripts/lab/lib/parity.sh"
if [ "$GATE" = G2 ]; then
    # Both G2 topologies run under the fleet manager (layout = topology).
    export LAB_FLEET_LAYOUT=$TOPOLOGY
    source "$LAB_REPO/scripts/lab/topology/fleet.sh"
    run_set fleet_backend "${LAB_FLEET_BACKEND:-procs}"
else
    source "$LAB_REPO/scripts/lab/topology/$TOPOLOGY.sh"
fi

GATE_PID="" GATE_NOT_RUN="" MONITOR_PID="" CHAOS_PID=""
cleanup() {
    for p in "$CHAOS_PID" "$MONITOR_PID"; do
        [ -n "$p" ] && kill "$p" 2>/dev/null && wait "$p" 2>/dev/null
    done
    topo_down 2>/dev/null
    if [ -n "$GATE_PID" ] && kill -0 "$GATE_PID" 2>/dev/null; then
        docker rm -f "k2-lab-gate-$$" >/dev/null 2>&1
        kill "$GATE_PID" 2>/dev/null
    fi
    rm -rf "$LAB_TMP"
}
trap cleanup EXIT
trap 'say "interrupted"; exit 130' INT TERM

finish() {
    local rc
    run_set finished "$(date -u +%Y-%m-%dT%H:%M:%SZ)" notes "$(json_list "${NOTES[@]+"${NOTES[@]}"}")"
    python3 -I tests/lab/lab.py report --run-dir "$RUN" | tee -a "$RUN/prove.log"
    rc=${PIPESTATUS[0]}
    say "bundle: ${RUN#$LAB_REPO/}/ (open report.html first)"
    exit "$rc"
}

# ── G2: scenarios S2 to S9, chaos, invariants (topology up) ─────────────
g2_scenarios() {
    local only="S2,S3,S4,S5,S6,S7,S8,S9"
    # S10 (P3 T2, a lost host's agent moves) is opt-in: LAB_S10=1.
    [ "${LAB_S10:-0}" = 1 ] && only="$only,S10"
    say "fleet monitor: samples.jsonl (live daemons and agent CLIs every 0.5 s)"
    python3 -I tests/lab/lab.py fleet monitor --state "$LAB_FLEET_STATE" --out "$RUN/samples.jsonl" &
    MONITOR_PID=$!
    say "scenario S2: hook events land on the right agent"
    python3 -I tests/lab/lab.py g2 --state "$LAB_FLEET_STATE" --out "$RUN/scenarios" --only S2 | tee -a "$RUN/prove.log"
    if [ -n "$CHAOS" ]; then
        say "chaos ${CHAOS}: on for S3 to S9 (chaos.log)"
        python3 -I tests/lab/lab.py chaos --state "$LAB_FLEET_STATE" --seed "${CHAOS#seed=}" --out "$RUN/chaos.log" \
            >>"$RUN/prove.log" 2>&1 &
        CHAOS_PID=$!
        export LAB_CHAOS_LOG="$RUN/chaos.log"
    fi
    say "scenarios S3 to S9"
    # Under chaos the set repeats in rounds for at least LAB_CHAOS_MIN_S (180 s),
    # so chaos acts even where the scenarios are quick (solo).
    python3 -I tests/lab/lab.py g2 --state "$LAB_FLEET_STATE" --out "$RUN/scenarios" --only "${only#S2,}" \
        ${CHAOS:+--min-seconds "${LAB_CHAOS_MIN_S:-180}"} | tee -a "$RUN/prove.log"
    if [ -n "$CHAOS_PID" ]; then
        say "chaos: off (it restores anything it took down)"
        kill "$CHAOS_PID" 2>/dev/null
        wait "$CHAOS_PID" 2>/dev/null
        CHAOS_PID=""
    fi
    say "settle: waiting for every outbox row to be final, then snapshotting them"
    python3 -I tests/lab/lab.py collect --state "$LAB_FLEET_STATE" --out "$RUN/outbox-final" | tee -a "$RUN/prove.log"
    kill "$MONITOR_PID" 2>/dev/null
    wait "$MONITOR_PID" 2>/dev/null
    MONITOR_PID=""
    topo_metrics | sed 's/^/after_scenarios_/' >>"$RUN/metrics.csv"
    say "invariants I1 to I4"
    python3 -I tests/lab/lab.py invariants --run-dir "$RUN" --layout "$TOPOLOGY" | tee -a "$RUN/prove.log"
}

# ── Node for the must-not-break suite (22.18+) ──────────────────────────
LAB_NODE_DIR=""
for n in "$(command -v node 2>/dev/null)" /opt/homebrew/bin/node /usr/local/bin/node "$HOME"/.nvm/versions/node/*/bin/node; do
    [ -x "$n" ] || continue
    if "$n" -e 'const [a,b]=process.versions.node.split(".").map(Number); process.exit(a>22||(a===22&&b>=18)?0:1)' 2>/dev/null; then
        LAB_NODE_DIR=$(dirname "$n"); break
    fi
done
export LAB_NODE_DIR

# ── Build today's daemon and the fake agent from this checkout ──────────
say "gate $GATE, topology $TOPOLOGY, sha ${LAB_SHA:0:12}, run ${RUN#$LAB_REPO/}"
if ! lab_toolchain "$LAB_REPO" >>"$RUN/build.log" 2>&1; then
    say "no Rust toolchain (see build.log)"
    NOTES+=("Build failed: no Rust toolchain (build.log). Nothing ran.")
    [ "$PARITY" = none ] && parity_none
    finish
fi
say "building k2-daemon, k2-fake-agent, and k2-node + k2-provider-fakes (tests/cli needs them) (rust: $LAB_RUST_SOURCE, $(rustc --version))"
# Two commands: `--bin k2-daemon` would limit EVERY selected package to a
# bin of that name, leaving the others unbuilt.
if ! { lab_cargo build -p k2-daemon --bin k2-daemon >>"$RUN/build.log" 2>&1 \
        && lab_cargo build -p k2-fake-agent -p k2-node -p k2-provider-fakes --bins >>"$RUN/build.log" 2>&1 \
        && { [ "${LAB_HOST_PTYD:-0}" != 1 ] || lab_cargo build -p k2-ptyd --bin k2-ptyd >>"$RUN/build.log" 2>&1; }; }; then
    say "build failed (see build.log)"
    NOTES+=("Build failed (build.log). Nothing ran.")
    finish
fi
# The suites rebuild target/debug/k2-daemon (some with other features), so
# the topology and must-not-break use a copy taken now.
mkdir -p "$LAB_TMP/bin" "$LAB_TMP/shim"
cp target/debug/k2-daemon target/debug/k2-fake-agent "$LAB_TMP/bin/" || { say "built binaries missing"; finish; }
for b in k2-node k2-provider-fakes; do
    [ -x "target/debug/$b" ] || { say "target/debug/$b was not built (see build.log)"; NOTES+=("target/debug/$b was not built; tests/cli scripts that need it will fail."); }
done
LAB_DAEMON_BIN="$LAB_TMP/bin/k2-daemon"
if [ "${LAB_HOST_PTYD:-0}" = 1 ]; then
    cp target/debug/k2-ptyd "$LAB_TMP/bin/" || { say "k2-ptyd missing"; finish; }
    export LAB_PTYD_BIN="$LAB_TMP/bin/k2-ptyd" LAB_HOST_PTYD=1
    NOTES+=("LAB_HOST_PTYD=1: hosts run agents under k2-ptyd (K2_PTYD=1, K2_PTYD_BIN); k2-ptyd left after the run: ptyd-leftovers.txt.")
fi
LAB_FAKE_AGENT_BIN="$LAB_TMP/bin/k2-fake-agent"
"$LAB_FAKE_AGENT_BIN" --install-shims "$LAB_TMP/shim"
LAB_SHIM_DIR="$LAB_TMP/shim"
# The suites' own cargo builds queue in the machine-wide slot too.
LAB_CARGO_SHIM_DIR="$LAB_TMP/cargo-shim"
lab_cargo_shim "$LAB_CARGO_SHIM_DIR"
export LAB_DAEMON_BIN LAB_FAKE_AGENT_BIN LAB_SHIM_DIR LAB_CARGO_SHIM_DIR

# ── Topology up, scenario S1 ────────────────────────────────────────────
say "topology $TOPOLOGY: up"
echo "metric,value,unit,note" >"$RUN/metrics.csv"
if topo_up "$RUN/topology" 2>"$RUN/topology/up.err"; then
    topo_describe "$RUN/topology"
    topo_metrics | sed 's/^daemon_rss,/daemon_rss_idle,/' >>"$RUN/metrics.csv"
    printf '%s' "$LAB_TOKEN" >"$LAB_TMP/token"
    s1=(python3 -I tests/lab/lab.py s1 --port "$LAB_PORT" --token-file "$LAB_TMP/token" --home "$LAB_HOME"
        --cwd "$LAB_WORKSPACE" --out "$RUN/scenarios" --fake-trace "$LAB_FAKE_TRACE")
    say "scenario S1: spawn, attach, type, see output"
    if command -v asciinema >/dev/null 2>&1; then
        asciinema rec -q --overwrite -c "$(printf '%q ' "${s1[@]}")" "$RUN/demo.cast" | tee -a "$RUN/prove.log"
    else
        "${s1[@]}" | tee -a "$RUN/prove.log"
        run_set demo_reason "asciinema is not installed on this machine; scenarios/S1.cast has the client's screen (asciicast v2, one frame per grid update)"
    fi
    python3 -I - "$RUN/scenarios/S1.json" >>"$RUN/metrics.csv" <<'PY'
import json, sys
m = json.load(open(sys.argv[1])).get("metrics", {})
notes = {"spawn_to_banner_ms": "POST spawn to the agent's banner on the client's grid (S1)",
         "type_to_reply_ms": "typed prompt to the reply on the grid, includes the fake turn (S1)"}
for k, note in notes.items():
    print("%s,%s,ms,%s" % (k, m.get(k, "NA"), note))
PY
    topo_metrics | sed 's/^daemon_rss,/daemon_rss_after_s1,/' >>"$RUN/metrics.csv"
    if [ "$GATE" = G2 ]; then
        g2_scenarios
    elif [ "$PARITY" = full ]; then
        say "parity: tier 1 against the solo daemon"
        parity_tier1
    fi
else
    say "topology $TOPOLOGY failed to start: $(tail -1 "$RUN/topology/up.err")"
    python3 -I - "$RUN/scenarios/S1.json" "$(tail -3 "$RUN/topology/up.err" | tr '\n' ' ')" <<'PY'
import json, sys
json.dump({"scenario": "S1", "title": "Spawn, attach, type, see output", "status": "not_run",
           "reason": "topology failed to start: " + sys.argv[2], "checks": []}, open(sys.argv[1], "w"), indent=2)
PY
    [ "$PARITY" = full ] && [ "$GATE" = G0 ] && result_json tier1 "tests/behavior-test-tier1.sh" not_run "the solo daemon did not start" "" - 0
fi
topo_down
say "topology $TOPOLOGY: down"
# G2: tier 1 against today's solo daemon, exactly as at G0 (I5 compares it).
if [ "$GATE" = G2 ] && [ "$PARITY" = full ]; then
    say "parity: tier 1 against a solo daemon (scripts/lab/topology/solo.sh, as at G0)"
    (
        source "$LAB_REPO/scripts/lab/topology/solo.sh"
        trap topo_down EXIT
        mkdir -p "$RUN/topology/tier1-solo"
        if topo_up "$RUN/topology/tier1-solo" 2>"$RUN/topology/tier1-solo/up.err"; then
            parity_tier1
        else
            result_json tier1 "tests/behavior-test-tier1.sh" not_run "the solo daemon did not start: $(tail -1 "$RUN/topology/tier1-solo/up.err")" "" - 0
        fi
    )
fi

# ── The other parity suites ─────────────────────────────────────────────
if [ "$PARITY" = full ]; then
    say "parity: tests/behavior-test-tier3.sh"
    parity_tier3
    say "parity: tests/contract/run_sandbox.sh (must-not-break)"
    parity_contract
    say "parity: tests/cli/*.sh, one at a time"
    parity_cli | tee -a "$RUN/prove.log"
    # Last, on its own: the gate is a full cold-or-warm build of k2-core and
    # k2-daemon tests, and the Mac is shared (one heavy build at a time).
    say "parity: Linux gate: $([ "$LAB_LINUX_GATE" = run ] && echo "scripts/test-gate-linux.sh in Docker, 3 CPUs, after taking the shared Docker lock (log: parity/linux-gate.log)" || echo "held by --linux-gate hold, NOT RUN")"
    parity_linux_gate_start
    parity_linux_gate_finish
else
    parity_none
fi

# ── Flows, baseline, report ─────────────────────────────────────────────
FLOW_TRACE="$RUN/scenarios/S1.trace.jsonl"
if [ "$GATE" = G2 ] && [ -d "$RUN/traces" ]; then
    # Every trace of the run in one file: the lab's S1 events, each
    # daemon's and each host's fake agents.
    FLOW_TRACE="$RUN/traces/all.jsonl"
    cat "$RUN/scenarios/S1.trace.jsonl" "$RUN"/traces/*.daemon.jsonl "$RUN"/traces/*.fake.jsonl 2>/dev/null >"$FLOW_TRACE"
fi
python3 -I tests/lab/lab.py flows --flows-dir docs/prd/flows --trace "$FLOW_TRACE" \
    --out "$RUN/flows.json" >>"$RUN/prove.log" 2>&1 \
    || python3 -I tests/lab/lab.py flows --flows-dir docs/prd/flows --out "$RUN/flows.json" >>"$RUN/prove.log" 2>&1
if [ "$RECORD_BASELINE" = 1 ]; then
    if python3 -I tests/lab/lab.py baseline --run-dir "$RUN" --out "$LAB_REPO/$BASELINE" >>"$RUN/prove.log" 2>&1; then
        say "recorded the I5 baseline: $BASELINE (commit it deliberately)"
    else
        say "could not record a baseline: $(tail -1 "$RUN/prove.log")"
        run_set recorded_baseline false
    fi
fi
finish
