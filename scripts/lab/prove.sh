#!/usr/bin/env bash
# The K2 lab (PRD "K2 Distributed Runtime", section 3): one command that
# brings up a topology, runs the parity suites and the gate's scenarios,
# checks the invariants that apply, and writes an evidence bundle.
#
#   scripts/lab/prove.sh <gate> --topology <solo|container|fleet|kind>
#                        [--chaos seed=N] [--parity full|none] [--record-baseline]
#
# Wave 1 implements gate G0 on topology solo. Anything else exits 2.
#
# Writes lab-runs/<gate>/<UTC timestamp>/ (open report.html first):
#   report.html, report.json   pass/fail matrix, machine-readable twin
#   parity/                    one log (and result JSON) per existing suite
#   scenarios/S1.*             log, result, trace, the client's screen (.cast)
#   flows.json                 the PRD flows against the trace (expected only in wave 1)
#   invariants.txt, metrics.csv, topology/, demo.cast (when asciinema is installed)
#
# Exit 0 only when every required item passed. --parity none is for
# iterating on scenarios: parity then shows NOT RUN and the gate fails.
# --record-baseline stores this run's parity results as the I5 baseline
# (tests/lab/baselines/<gate>-<topology>.json); commit it deliberately.
#
# Touches nothing outside the repo, lab-runs/ and temp dirs: the daemon and
# every suite run under throwaway HOMEs; Rust comes from PATH or is installed
# under lab-runs/.toolchain; the Linux gate runs in Docker with named volumes.
set -uo pipefail

usage() { sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2; }

GATE="" TOPOLOGY="" CHAOS="" PARITY=full RECORD_BASELINE=0
while [ $# -gt 0 ]; do
    case "$1" in
        --topology) TOPOLOGY=${2:-}; shift 2 ;;
        --topology=*) TOPOLOGY=${1#*=}; shift ;;
        --chaos) CHAOS=${2:-}; shift 2 ;;
        --chaos=*) CHAOS=${1#*=}; shift ;;
        --parity) PARITY=${2:-}; shift 2 ;;
        --parity=*) PARITY=${1#*=}; shift ;;
        --record-baseline) RECORD_BASELINE=1; shift ;;
        -h|--help) usage ;;
        -*) echo "prove.sh: unknown option $1" >&2; usage ;;
        *) [ -z "$GATE" ] && GATE=$1 || { echo "prove.sh: unexpected argument $1" >&2; usage; }; shift ;;
    esac
done
[ -n "$GATE" ] && [ -n "$TOPOLOGY" ] || usage
case "$PARITY" in full|none) ;; *) echo "prove.sh: --parity is full or none" >&2; exit 2 ;; esac
case "$CHAOS" in ""|seed=[0-9]*) ;; *) echo "prove.sh: --chaos takes seed=N" >&2; exit 2 ;; esac
case "$TOPOLOGY" in solo|container|fleet|kind) ;; *) echo "prove.sh: unknown topology '$TOPOLOGY' (solo, container, fleet, kind)" >&2; exit 2 ;; esac
if [ "$GATE" != G0 ] || [ "$TOPOLOGY" != solo ]; then
    echo "prove.sh: $GATE on topology $TOPOLOGY is not implemented in wave 1 (only: prove.sh G0 --topology solo)" >&2
    exit 2
fi
if [ "$RECORD_BASELINE" = 1 ] && [ "$PARITY" != full ]; then
    echo "prove.sh: --record-baseline needs --parity full" >&2; exit 2
fi

LAB_REPO=$(cd "$(dirname "$0")/../.." && pwd)
cd "$LAB_REPO" || exit 2
LAB_SHA=$(git rev-parse HEAD)
TS=$(date -u +%Y-%m-%dT%H-%M-%SZ)
RUN="$LAB_REPO/lab-runs/$GATE/$TS"
mkdir -p "$RUN/parity" "$RUN/scenarios" "$RUN/topology"
export LAB_REPO LAB_SHA RUN
LAB_TMP=$(mktemp -d "${TMPDIR:-/tmp}/k2lab-run-XXXXXX")
BASELINE="tests/lab/baselines/$GATE-$TOPOLOGY.json"
NOTES=()
say() { printf '%s lab: %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$RUN/prove.log"; }

for v in $(env | grep -oE '^K2(SO)?_[A-Z0-9_]+' || true); do unset "$v"; done

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

if [ -n "$(git status --porcelain -- . ':!lab-runs')" ]; then
    NOTES+=("The working tree has uncommitted changes. Solo and the local suites ran on the working tree; the Linux gate ran on the committed sha $LAB_SHA.")
fi
if [ -n "$CHAOS" ]; then
    NOTES+=("--chaos $CHAOS recorded; G0 has no chaos scenarios (chaos applies to S3 to S10).")
fi

source "$LAB_REPO/scripts/lab/lib/toolchain.sh"
source "$LAB_REPO/scripts/lab/lib/parity.sh"
source "$LAB_REPO/scripts/lab/topology/$TOPOLOGY.sh"

GATE_PID="" GATE_NOT_RUN=""
cleanup() {
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
say "building k2-daemon and k2-fake-agent (rust: $LAB_RUST_SOURCE, $(rustc --version))"
if ! cargo build -p k2-daemon --bin k2-daemon -p k2-fake-agent >>"$RUN/build.log" 2>&1; then
    say "build failed (see build.log)"
    NOTES+=("Build failed (build.log). Nothing ran.")
    finish
fi
# The suites rebuild target/debug/k2-daemon (some with other features), so
# the topology and must-not-break use a copy taken now.
mkdir -p "$LAB_TMP/bin" "$LAB_TMP/shim"
cp target/debug/k2-daemon target/debug/k2-fake-agent "$LAB_TMP/bin/"
LAB_DAEMON_BIN="$LAB_TMP/bin/k2-daemon"
LAB_FAKE_AGENT_BIN="$LAB_TMP/bin/k2-fake-agent"
"$LAB_FAKE_AGENT_BIN" --install-shims "$LAB_TMP/shim"
LAB_SHIM_DIR="$LAB_TMP/shim"
export LAB_DAEMON_BIN LAB_FAKE_AGENT_BIN LAB_SHIM_DIR

# ── Linux gate first: it runs in Docker while the rest goes on here ─────
if [ "$PARITY" = full ]; then
    say "parity: starting scripts/test-gate-linux.sh in Docker (log: parity/linux-gate.log)"
    parity_linux_gate_start
fi

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
    if [ "$PARITY" = full ]; then
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
    [ "$PARITY" = full ] && result_json tier1 "tests/behavior-test-tier1.sh" not_run "the solo daemon did not start" "" - 0
fi
topo_down
say "topology $TOPOLOGY: down"

# ── The other parity suites ─────────────────────────────────────────────
if [ "$PARITY" = full ]; then
    say "parity: tests/behavior-test-tier3.sh"
    parity_tier3
    say "parity: tests/contract/run_sandbox.sh (must-not-break)"
    parity_contract
    say "parity: tests/cli/*.sh, one at a time"
    parity_cli | tee -a "$RUN/prove.log"
    say "parity: waiting for the Linux gate"
    parity_linux_gate_finish
else
    parity_none
fi

# ── Flows, baseline, report ─────────────────────────────────────────────
python3 -I tests/lab/lab.py flows --flows-dir docs/prd/flows --trace "$RUN/scenarios/S1.trace.jsonl" \
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
