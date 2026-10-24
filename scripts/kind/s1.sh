#!/usr/bin/env bash
# Scenario S1 (spawn, attach, type, see output) against K2 on kind k2lab,
# through the port-forward, with the fake agent. TEST ONLY: needs
# `scripts/kind/up.sh --fake` (or `--dist --fake`) first; never runs a real
# agent.
#
#   scripts/kind/s1.sh            # → lab-runs/kind/<UTC time>/S1.{json,log,cast,trace.jsonl}
#   scripts/kind/s1.sh --dist     # → lab-runs/kind-dist/<UTC time>/
#
# S1 (tests/lab/labkit/s1.py, unchanged) drives the daemon over HTTP and
# WebSocket on 127.0.0.1:<port>, the app's own routes. It also reads the
# agent's transcript and the fake agent's trace from local files; here they
# live in the pod that runs agents (HOME, /work), so a copy loop mirrors
# them into a local dir while S1 runs. The agent starts in /work/lab-agent,
# a path with no symlinks on either side, so the pod and S1 agree on
# Claude's project slug for it.
#
# --dist (distributed variant, k2-kind-dist-fake on 7442) runs, in order:
#   S1/                   S1 through the controller
#   placement.txt         a spawned agent's process is in k2-host-0 and not
#                         in k2-controller-0 (a /proc scan of each pod)
#   after-host-restart/   delete pod k2-host-0, wait for it, S1 again
# and passes only when all three do.
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
dist=0
for a in "$@"; do
    case "$a" in
        --dist) dist=1 ;;
        -h|--help) sed -n '2,23p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "s1.sh: unknown argument $a (see --help)" >&2; exit 2 ;;
    esac
done
kind_variant_from_flags 1 "$dist"
kind_kubeconfig
cd "$KIND_REPO"
port=$(cat "$KIND_STATE/port" 2>/dev/null) || { echo "s1.sh: run scripts/kind/up.sh$KIND_FLAGS first" >&2; exit 1; }

out="$KIND_REPO/lab-runs/kind$([ "$dist" = 1 ] && echo -dist)/$(date -u +%Y-%m-%dT%H-%M-%SZ)"
mirror=$(mktemp -d "${TMPDIR:-/tmp}/k2-kind-s1-XXXXXX")
mkdir -p "$out"
copier=""
# shellcheck disable=SC2329  # called from the EXIT trap
cleanup() {
    [ -n "$copier" ] && kill "$copier" 2>/dev/null
    rm -rf "$mirror"
}
trap cleanup EXIT

ak() { kc -n "$KIND_NS" exec "$KIND_AGENT_POD" -c "$KIND_AGENT_CONTAINER" -- "$@"; }

# The token goes to S1 as a 0600 file in the temp dir, removed on exit.
(umask 077; kc -n "$KIND_NS" exec "$KIND_POD" -c k2-daemon -- cat /var/lib/k2/.k2/daemon.token > "$mirror/token")

home_rel=${KIND_AGENT_HOME#/}
copy_once() {
    rm -rf "$mirror/stage" && mkdir -p "$mirror/stage"
    ak tar -C / -cf - "$home_rel/.claude/projects" work/fake-agent.trace.jsonl 2>/dev/null \
        | tar -C "$mirror/stage" -xf - 2>/dev/null || true
    if [ -d "$mirror/stage/$home_rel/.claude/projects" ]; then
        mkdir -p "$mirror/home/.claude"
        rm -rf "$mirror/home/.claude/projects.new"
        mv "$mirror/stage/$home_rel/.claude/projects" "$mirror/home/.claude/projects.new"
        rm -rf "$mirror/home/.claude/projects"
        mv "$mirror/home/.claude/projects.new" "$mirror/home/.claude/projects"
    fi
    # The trace last, by rename: once S1 sees the turn end there, the
    # transcript copied in the same pass is already in place.
    if [ -f "$mirror/stage/work/fake-agent.trace.jsonl" ]; then
        mv "$mirror/stage/work/fake-agent.trace.jsonl" "$mirror/home/fake-agent.trace.jsonl"
    fi
}

run_s1() {  # run_s1 <out dir> → S1's exit status
    local dir=$1 rc=0
    mkdir -p "$dir"
    rm -rf "${mirror:?}/home" && mkdir -p "$mirror/home"
    # Start each run from an empty trace, as the solo topology does.
    ak sh -c ': > /work/fake-agent.trace.jsonl'
    ( while :; do copy_once; sleep 0.5; done ) &
    copier=$!
    say "S1 against http://127.0.0.1:$port (namespace $KIND_NS, agents in $KIND_AGENT_POD)"
    python3 -I tests/lab/lab.py s1 --port "$port" --token-file "$mirror/token" --home "$mirror/home" \
        --cwd /work/lab-agent --out "$dir" --fake-trace "$mirror/home/fake-agent.trace.jsonl" || rc=$?
    kill "$copier" 2>/dev/null; wait "$copier" 2>/dev/null || true; copier=""
    return "$rc"
}

# Agent processes in a pod: the fake agent's binary, or a process named
# like an agent CLI. /proc only (the images have no procps).
agent_procs() {  # agent_procs <pod> <container>
    kc -n "$KIND_NS" exec "$1" -c "$2" -- sh -c '
        for p in /proc/[0-9]*; do
            e=$(readlink "$p/exe" 2>/dev/null); c=$(cat "$p/comm" 2>/dev/null)
            case "$e:$c" in
                */k2-fake-agent:*|*:claude|*:codex) echo "pid ${p#/proc/} comm=$c exe=$e" ;;
            esac
        done' 2>/dev/null || true
}

api() {  # api <path> <json body> → response body (token from the file, never on argv)
    PORT=$port TOKEN_FILE=$mirror/token python3 -I -c '
import json, os, sys, urllib.error, urllib.parse, urllib.request
tok = open(os.environ["TOKEN_FILE"]).read().strip()
url = "http://127.0.0.1:%s%s?%s" % (os.environ["PORT"], sys.argv[1], urllib.parse.urlencode({"token": tok}))
req = urllib.request.Request(url, data=sys.argv[2].encode(), method="POST", headers={"Content-Type": "application/json"})
try:
    with urllib.request.urlopen(req, timeout=60) as r:
        print(r.status, r.read().decode("utf-8", "replace")[:400])
except urllib.error.HTTPError as e:
    print(e.code, e.read().decode("utf-8", "replace")[:400])' "$1" "$2"
}

placement() {  # → 0 when the agent runs in the host pod and not in the controller pod
    local name session resp host ctl i
    name="lab-place-$(python3 -I -c 'import uuid; print(uuid.uuid4().hex[:8])')"
    session=$(python3 -I -c 'import uuid; print(uuid.uuid4())')
    resp=$(api /cli/sessions/v2/spawn "$(python3 -I -c 'import json,sys; print(json.dumps({"agent_name": sys.argv[1], "cwd": "/work/lab-agent", "command": "claude", "args": ["--session-id", sys.argv[2]], "cols": 120, "rows": 32}))' "$name" "$session")")
    echo "spawn $name -> $resp"
    host=""
    for i in $(seq 1 30); do
        host=$(agent_procs "$KIND_AGENT_POD" "$KIND_AGENT_CONTAINER")
        [ -n "$host" ] && break
        sleep 1
    done
    ctl=$(agent_procs "$KIND_POD" k2-daemon)
    echo "agent processes in $KIND_AGENT_POD (after ${i}s):"; printf '  %s\n' "${host:-(none)}"
    echo "agent processes in $KIND_POD:"; printf '  %s\n' "${ctl:-(none)}"
    echo "close $name -> $(api /cli/sessions/v2/close "$(python3 -I -c 'import json,sys; print(json.dumps({"agent_name": sys.argv[1], "force": True, "clear_index": True}))' "$name")")"
    if [ -n "$host" ] && [ -z "$ctl" ]; then echo "PASS placement"; else echo "FAIL placement"; return 1; fi
}

describe() {  # describe <dir>
    {
        echo "topology: kind k2lab, namespace $KIND_NS, via kubectl port-forward 127.0.0.1:$port -> svc/$KIND_SVC"
        echo "agents: $KIND_IMAGE in $KIND_AGENT_POD (image id $(docker image inspect -f '{{.Id}}' "$KIND_IMAGE" 2>/dev/null))"
        for pod in $KIND_POD $([ "$KIND_AGENT_POD" != "$KIND_POD" ] && echo "$KIND_AGENT_POD"); do
            echo "pod $pod: image $(kc -n "$KIND_NS" get pod "$pod" -o jsonpath='{.spec.containers[0].image} {.status.containerStatuses[0].imageID} started {.status.startTime}')"
        done
        echo "daemon_version: $(kc -n "$KIND_NS" exec "$KIND_POD" -c k2-daemon -- k2-daemon --version 2>&1 | head -1)"
        echo "sha: $(git rev-parse HEAD)"
    } > "$1/topology.txt"
    curl -s --connect-timeout 2 "http://127.0.0.1:$port/boot-status" > "$1/boot-status.json" || true
}

if [ "$dist" = 0 ]; then
    rc=0
    run_s1 "$out" || rc=$?
    describe "$out"
    say "evidence: ${out#"$KIND_REPO"/}/"
    exit "$rc"
fi

s1a=0 place=0 s1b=0
run_s1 "$out/S1" || s1a=$?
describe "$out"
say "placement: is the agent in $KIND_AGENT_POD, not $KIND_POD?"
placement > "$out/placement.txt" 2>&1 || place=$?
cat "$out/placement.txt" >&2
say "deleting pod $KIND_AGENT_POD, then S1 again"
kc -n "$KIND_NS" delete pod "$KIND_AGENT_POD" --wait=true >&2
kc -n "$KIND_NS" rollout status statefulset/k2-host --timeout=300s >&2
kc -n "$KIND_NS" get pod "$KIND_AGENT_POD" -o jsonpath='{.metadata.uid} started {.status.startTime}{"\n"}' > "$out/host-after-restart.txt"
run_s1 "$out/after-host-restart" || s1b=$?
{
    echo "S1                    $([ $s1a = 0 ] && echo PASS || echo FAIL)"
    echo "placement             $([ $place = 0 ] && echo PASS || echo FAIL)"
    echo "S1 after host restart $([ $s1b = 0 ] && echo PASS || echo FAIL)"
} | tee "$out/summary.txt"
say "evidence: ${out#"$KIND_REPO"/}/"
[ $s1a = 0 ] && [ $place = 0 ] && [ $s1b = 0 ]
