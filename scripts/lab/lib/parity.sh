#!/usr/bin/env bash
# Parity (PRD section 3, rule 1): run the existing suites UNCHANGED and record
# each one's result separately under $RUN/parity/. Sourced by prove.sh.
#
# Every suite gets a throwaway HOME and TMPDIR, no K2_* / K2SO_* variables,
# and the fake agent first on PATH (a belt: no suite can reach a real agent
# CLI, the real ~/.k2 or ~/.claude). A suite that cannot run here is
# recorded as not_run WITH its reason; nothing is skipped silently.
#
# Each suite writes $RUN/parity/<id>.json:
#   {"suite", "title", "status": pass|fail|not_run, "reason", "log",
#    "exit", "seconds", "items": [{"name", "status", "log", "skips"}]}

# Needs: LAB_REPO, RUN, LAB_SHIM_DIR, LAB_DAEMON_BIN, LAB_SHA.

# result_json <id> <title> <status> <reason> <log> <exit> <seconds> [items-file]
result_json() {
    python3 -I - "$@" <<'PY'
import json, sys
sid, title, status, reason, log, code, secs = sys.argv[1:8]
items = []
if len(sys.argv) > 8 and sys.argv[8]:
    with open(sys.argv[8]) as fh:
        for line in fh:
            name, st, lg, skips = line.rstrip("\n").split("\t")
            items.append({"name": name, "status": st, "log": lg, "skips": int(skips)})
out = {"suite": sid, "title": title, "status": status, "reason": reason or None, "log": log or None,
       "exit": int(code) if code not in ("", "-") else None, "seconds": int(secs), "items": items}
import os
with open(os.path.join(os.environ["RUN"], "parity", sid + ".json"), "w") as fh:
    json.dump(out, fh, indent=2)
PY
}

# fail_reason <exit> <log>: empty on success, else the log's summary lines.
fail_reason() {
    [ "$1" -eq 0 ] && return
    if [ ! -s "$2" ]; then
        echo "exit $1 with no output"
    else
        grep -aE 'FAIL|Passed:|Failed:|PASS:|error' "$2" | tail -3 | sed 's/\x1b\[[0-9;]*m//g' | tr '\n' ' '
        echo "(exit $1)"
    fi
}

# A clean environment for one suite: prints env -i arguments.
suite_env() {
    local h=$1
    mkdir -p "$h/home" "$h/tmp"
    printf '%s\n' \
        "HOME=$h/home" "TMPDIR=$h/tmp" "USER=$(id -un)" "LANG=en_US.UTF-8" "TERM=dumb" \
        "PATH=$LAB_SHIM_DIR:$LAB_NODE_DIR:${CARGO_HOME:+$CARGO_HOME/bin:}/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin" \
        ${CARGO_HOME:+"CARGO_HOME=$CARGO_HOME"} ${RUSTUP_HOME:+"RUSTUP_HOME=$RUSTUP_HOME"} \
        ${OPENSSL_ROOT_DIR:+"OPENSSL_ROOT_DIR=$OPENSSL_ROOT_DIR"} \
        ${CARGO_BUILD_JOBS:+"CARGO_BUILD_JOBS=$CARGO_BUILD_JOBS"}
}

# run_clean <log> <cwd> <deadline-seconds> <cmd...>: run under a clean env
# with its own temp dirs. A deadline only stops a hung suite (it is then a
# failure, exit 124); nothing is judged on time. Returns the exit code.
run_clean() {
    local log=$1 cwd=$2 deadline=$3; shift 3
    local h; h=$(mktemp -d "${TMPDIR:-/tmp}/k2lab-suite-XXXXXX")
    local envs=(); while IFS= read -r l; do envs+=("$l"); done < <(suite_env "$h")
    ( cd "$cwd" && exec env -i "${envs[@]}" "$@" ) >"$log" 2>&1 &
    local pid=$! rc
    # The watchdog holds no stdout/stderr (a caller piping our output, e.g.
    # through tee, would otherwise wait for its sleep), and its sleep dies
    # with it.
    ( sleep "$deadline"; kill -TERM "$pid" 2>/dev/null; sleep 5; kill -KILL "$pid" 2>/dev/null ) >/dev/null 2>&1 </dev/null &
    local dog=$!
    wait "$pid"; rc=$?
    pkill -P "$dog" 2>/dev/null; kill "$dog" 2>/dev/null; wait "$dog" 2>/dev/null
    if [ $rc -eq 143 ] || [ $rc -eq 137 ]; then
        echo "lab: stopped after ${deadline}s (hung?)" >>"$log"; rc=124
    fi
    # Anything the suite left running under its sandbox.
    pkill -9 -f "$h/" 2>/dev/null || true
    rm -rf "$h"
    return $rc
}

# ── tests/cli/*.sh ───────────────────────────────────────────────────────
# One script at a time: 27 of them rebuild k2-daemon, some with other
# features, so running them in parallel would race on target/debug.
#
# A script that exits non-zero after printing its OWN refusal to run here
# (not a check failing) is recorded not_run with that line as the reason:
# the disposable-Linux-box guard, a tool "required for this test", or a
# build-box script called without its arguments. Anything else non-zero is
# a failure. Either way the suite is not green.
cli_not_run_reason() {
    local log=$1 line
    line=$(grep -m1 -aE '^refusing: set K2_DISPOSABLE_BOX=1' "$log") \
        && { echo "refuses outside a disposable Linux box: $line"; return; }
    line=$(grep -m1 -aE '^FAIL: .* is required for this test' "$log") \
        && { echo "a tool this test needs is missing here: ${line#FAIL: }"; return; }
    line=$(grep -m1 -aE 'line [0-9]+: 1: ' "$log") \
        && { echo "build-box script, needs arguments (${line##*: 1: }); not runnable from this harness"; return; }
    return 1
}

parity_cli() {
    local dir="$RUN/parity/cli" items="$RUN/parity/cli.items" t0=$SECONDS f name rc st skips why
    local fails=0 notrun=0 n=0
    mkdir -p "$dir"; : >"$items"; : >"$dir/not-run.txt"
    for f in "$LAB_REPO"/tests/cli/*.sh; do
        name=$(basename "$f" .sh)
        case "$name" in _*) continue ;; esac
        n=$((n + 1))
        run_clean "$dir/$name.log" "$LAB_REPO" 900 bash "$f"; rc=$?
        if [ $rc -eq 0 ]; then
            st=pass
        elif why=$(cli_not_run_reason "$dir/$name.log"); then
            st=not_run; notrun=$((notrun + 1)); printf '%s: %s\n' "$name" "$why" >>"$dir/not-run.txt"
        else
            st=fail; fails=$((fails + 1))
        fi
        skips=$(grep -ciE '(^|[^a-z])skip(ped)?([^a-z]|$)' "$dir/$name.log" || true)
        printf '%s\t%s\t%s\t%s\n' "$name" "$st" "parity/cli/$name.log" "${skips:-0}" >>"$items"
        printf '  cli %-48s %s\n' "$name" "$st"
    done
    local status=pass reason=""
    if [ $fails -gt 0 ]; then
        status=fail; reason="$fails of $n scripts failed"
        [ $notrun -gt 0 ] && reason+="; $notrun could not run here (parity/cli/not-run.txt)"
    elif [ $notrun -gt 0 ]; then
        status=not_run; reason="$notrun of $n scripts could not run here (parity/cli/not-run.txt); the rest passed"
    fi
    result_json cli "tests/cli/*.sh ($n scripts)" "$status" "$reason" "parity/cli/not-run.txt" "$fails" $((SECONDS - t0)) "$items"
    rm -f "$items"
}

# ── behavior tiers ───────────────────────────────────────────────────────
parity_tier3() {
    local t0=$SECONDS rc
    run_clean "$RUN/parity/tier3.log" "$LAB_REPO" 1800 bash tests/behavior-test-tier3.sh; rc=$?
    result_json tier3 "tests/behavior-test-tier3.sh" "$([ $rc -eq 0 ] && echo pass || echo fail)" \
        "$(fail_reason $rc "$RUN/parity/tier3.log")" "parity/tier3.log" $rc $((SECONDS - t0))
}

# Tier 1 needs a running daemon it finds through $HOME/.k2so/heartbeat.port.
# It runs against the solo sandbox daemon when that file is there.
parity_tier1() {
    local t0=$SECONDS rc
    if [ ! -s "$LAB_HOME/.k2so/heartbeat.port" ]; then
        local why="tier 1 finds its daemon only through \$HOME/.k2so/heartbeat.port (and drives cli/k2so); today's daemon writes \$HOME/.k2/ (no .k2so/ in the sandbox: $(ls -A "$LAB_HOME" | tr '\n' ' ')), so the script cannot target a sandbox daemon without an edit"
        echo "$why" >"$RUN/parity/tier1.log"
        result_json tier1 "tests/behavior-test-tier1.sh" not_run "$why" "parity/tier1.log" - 0
        return
    fi
    ( cd "$LAB_REPO" && env -i HOME="$LAB_HOME" PATH="$LAB_SHIM_DIR:/usr/bin:/bin:/usr/sbin:/sbin" \
        TEST_WORKSPACE="$LAB_WORKSPACE" bash tests/behavior-test-tier1.sh ) >"$RUN/parity/tier1.log" 2>&1; rc=$?
    result_json tier1 "tests/behavior-test-tier1.sh (against the solo daemon)" \
        "$([ $rc -eq 0 ] && echo pass || echo fail)" "$(fail_reason $rc "$RUN/parity/tier1.log")" \
        "parity/tier1.log" $rc $((SECONDS - t0))
}

# ── must-not-break ───────────────────────────────────────────────────────
# Its own runner boots a throwaway daemon from the given binary. Node 22.18+
# (LAB_NODE_DIR, found by prove.sh) runs the TypeScript suite as is.
parity_contract() {
    local t0=$SECONDS rc
    if [ -z "$LAB_NODE_DIR" ]; then
        local why="no node 22.18+ on this machine (tests/contract/run_sandbox.sh needs it)"
        echo "$why" >"$RUN/parity/contract.log"
        result_json contract "tests/contract/must-not-break.ts" not_run "$why" "parity/contract.log" - 0
        return
    fi
    run_clean "$RUN/parity/contract.log" "$LAB_REPO" 1800 bash tests/contract/run_sandbox.sh "$LAB_DAEMON_BIN"; rc=$?
    result_json contract "tests/contract/must-not-break.ts (run_sandbox.sh, node $("$LAB_NODE_DIR/node" --version))" \
        "$([ $rc -eq 0 ] && echo pass || echo fail)" \
        "$( { fail_reason $rc "$RUN/parity/contract.log"; n=$(grep -c '^  --  ' "$RUN/parity/contract.log"); \
              [ "${n:-0}" -gt 0 ] && echo "$n check(s) the suite itself reports as not run here: $(grep -m1 '^  --  ' "$RUN/parity/contract.log" | sed 's/.*(not run: //; s/)$//')"; } | tr '\n' ' ')" \
        "parity/contract.log" $rc $((SECONDS - t0))
}

# ── scripts/test-gate-linux.sh <sha>, in Docker ──────────────────────────
# Owner decision (2026-10-24): unchanged, as root, in a Linux container on
# this Mac's Docker. The repo is mounted read-only (the worktree and its git
# dir, so `git archive <sha>` works); the target dir and
# cargo registry live in named volumes so later runs are warm. Nothing from
# $HOME is mounted. The gate's workdir is fresh each run; its `target` is a
# symlink into the volume (the gate removes the link, not the cache).
GATE_IMAGE_TAG=k2-lab-gate:1
GATE_TARGET_VOL=k2-lab-gate-target
GATE_REGISTRY_VOL=k2-lab-gate-cargo-registry

# LAB_LINUX_GATE=hold (prove.sh --linux-gate hold):
# never touch Docker; the gate is NOT RUN with LAB_LINUX_GATE_HOLD_REASON.
LAB_LINUX_GATE_HOLD_REASON="held by --linux-gate hold: no gate container was started (the gate needs Docker with 20 GB free in its VM); run with --linux-gate run"

parity_linux_gate_start() {
    local log="$RUN/parity/linux-gate.log"
    mkdir -p "$RUN/topology"
    if [ "${LAB_LINUX_GATE:-hold}" != run ]; then
        echo "$LAB_LINUX_GATE_HOLD_REASON" >"$log"
        GATE_PID=""; GATE_NOT_RUN=$LAB_LINUX_GATE_HOLD_REASON
        return
    fi
    if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
        echo "docker is not available" >"$log"
        GATE_PID=""; GATE_NOT_RUN="Docker is not available on this machine"
        return
    fi
    GATE_NOT_RUN=""
    GATE_T0=$SECONDS
    local common wt
    wt=$LAB_REPO
    common=$(cd "$LAB_REPO" && cd "$(git rev-parse --git-common-dir)" && pwd)
    (
        set -o pipefail
        docker build -q -t "$GATE_IMAGE_TAG" -f "$LAB_REPO/scripts/lab/docker/gate.Dockerfile" "$LAB_REPO/scripts/lab/docker" \
            >"$RUN/parity/linux-gate.image.log" 2>&1 || { echo "lab: building the gate image failed" ; cat "$RUN/parity/linux-gate.image.log"; exit 3; }
        {
            echo "image: $GATE_IMAGE_TAG ($(docker image inspect "$GATE_IMAGE_TAG" --format '{{.Id}}'))"
            echo "base: $(grep -m1 '^FROM' "$LAB_REPO/scripts/lab/docker/gate.Dockerfile" | cut -d' ' -f2)"
            echo "sha: $LAB_SHA"
            echo "docker: $(docker version --format '{{.Server.Version}}') ($(docker info --format '{{.NCPU}} CPU, {{.MemTotal}} bytes'))"
            echo "volumes: $GATE_TARGET_VOL (target), $GATE_REGISTRY_VOL (cargo registry)"
        } >"$RUN/topology/linux-gate.txt"
        # Docker Desktop cannot mount at a target under /Users, so the repo
        # sits at /src/wt and its git dir at /src/git; GIT_DIR/GIT_WORK_TREE
        # let the gate's `git rev-parse` and `git archive <sha>` work there.
        exec docker run --rm --name "k2-lab-gate-$$" \
            --cap-add NET_ADMIN --cpus "${CARGO_BUILD_JOBS:-4}" -e K2_GATE_THREADS="${LAB_TEST_THREADS:-4}" \
            -e GIT_DIR=/src/git -e GIT_WORK_TREE=/src/wt \
            -e GIT_CONFIG_COUNT=1 -e GIT_CONFIG_KEY_0=safe.directory -e GIT_CONFIG_VALUE_0='*' \
            -v "$wt:/src/wt:ro" -v "$common:/src/git:ro" \
            -v "$GATE_TARGET_VOL:/cache/target" -v "$GATE_REGISTRY_VOL:/usr/local/cargo/registry" \
            "$GATE_IMAGE_TAG" \
            bash -c 'rm -rf /gate/work && mkdir -p /gate/work && ln -s /cache/target /gate/work/target \
                     && exec bash /src/wt/scripts/test-gate-linux.sh "$0" /gate/work' "$LAB_SHA"
    ) >"$log" 2>&1 &
    GATE_PID=$!
}

parity_linux_gate_finish() {
    local log="$RUN/parity/linux-gate.log" rc title="scripts/test-gate-linux.sh $LAB_SHA (Docker, root)"
    if [ -n "$GATE_NOT_RUN" ]; then
        result_json linux-gate "$title" not_run "$GATE_NOT_RUN" "parity/linux-gate.log" - 0
        return
    fi
    wait "$GATE_PID"; rc=$?
    local secs=$((SECONDS - GATE_T0))
    if grep -q '^GATE GREEN' "$log"; then
        result_json linux-gate "$title" pass "" "parity/linux-gate.log" $rc $secs
    elif grep -q '^refusing to run' "$log"; then
        result_json linux-gate "$title" not_run "the gate refused to start: $(grep -m1 '^refusing to run' "$log")" "parity/linux-gate.log" $rc $secs
    else
        result_json linux-gate "$title" fail "$(grep -E '^GATE RED|^lab:' "$log" | head -2 | tr '\n' ' ')exit $rc" "parity/linux-gate.log" $rc $secs
    fi
}

parity_none() {
    local s why="--parity none (iteration only): parity was not run, so the gate cannot pass"
    for s in linux-gate cli tier3 tier1 contract; do
        result_json "$s" "$s" not_run "$why" "" - 0
    done
}
