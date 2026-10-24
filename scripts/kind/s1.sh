#!/usr/bin/env bash
# Scenario S1 (spawn, attach, type, see output) against K2 on kind k2lab,
# through the port-forward, with the fake agent. TEST ONLY: needs
# `scripts/kind/up.sh --fake` first; never runs a real agent.
#
#   scripts/kind/s1.sh            # → lab-runs/kind/<UTC time>/S1.{json,log,cast,trace.jsonl}
#
# S1 (tests/lab/labkit/s1.py, unchanged) drives the daemon over HTTP and
# WebSocket on 127.0.0.1:<port>, the app's own routes. It also reads the
# agent's transcript and the fake agent's trace from local files; here they
# live in the pod (HOME=/var/lib/k2, /work), so a copy loop mirrors them
# into a local dir while S1 runs. The agent starts in /work/lab-agent, a
# path with no symlinks on either side, so the pod and S1 agree on Claude's
# project slug for it.
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
kind_variant fake
kind_kubeconfig
cd "$KIND_REPO"
port=$(cat "$KIND_STATE/port" 2>/dev/null) || { echo "s1.sh: run scripts/kind/up.sh --fake first" >&2; exit 1; }

out="$KIND_REPO/lab-runs/kind/$(date -u +%Y-%m-%dT%H-%M-%SZ)"
mirror=$(mktemp -d "${TMPDIR:-/tmp}/k2-kind-s1-XXXXXX")
mkdir -p "$out" "$mirror/home" "$mirror/stage"
copier=""
# shellcheck disable=SC2329  # called from the EXIT trap
cleanup() {
    [ -n "$copier" ] && kill "$copier" 2>/dev/null
    rm -rf "$mirror"
}
trap cleanup EXIT

# The token goes to S1 as a 0600 file in the temp dir, removed on exit.
(umask 077; kc -n "$KIND_NS" exec "$KIND_POD" -c k2-daemon -- cat /var/lib/k2/.k2/daemon.token > "$mirror/token")

# Start each run from an empty trace, as the solo topology does.
kc -n "$KIND_NS" exec "$KIND_POD" -c k2-daemon -- sh -c ': > /work/fake-agent.trace.jsonl'

copy_once() {
    rm -rf "$mirror/stage" && mkdir -p "$mirror/stage"
    kc -n "$KIND_NS" exec "$KIND_POD" -c k2-daemon -- \
        tar -C / -cf - var/lib/k2/.claude/projects work/fake-agent.trace.jsonl 2>/dev/null \
        | tar -C "$mirror/stage" -xf - 2>/dev/null || true
    if [ -d "$mirror/stage/var/lib/k2/.claude/projects" ]; then
        mkdir -p "$mirror/home/.claude"
        rm -rf "$mirror/home/.claude/projects.new"
        mv "$mirror/stage/var/lib/k2/.claude/projects" "$mirror/home/.claude/projects.new"
        rm -rf "$mirror/home/.claude/projects"
        mv "$mirror/home/.claude/projects.new" "$mirror/home/.claude/projects"
    fi
    # The trace last, by rename: once S1 sees the turn end there, the
    # transcript copied in the same pass is already in place.
    if [ -f "$mirror/stage/work/fake-agent.trace.jsonl" ]; then
        mv "$mirror/stage/work/fake-agent.trace.jsonl" "$mirror/home/fake-agent.trace.jsonl"
    fi
}
( while :; do copy_once; sleep 0.5; done ) &
copier=$!

say "S1 against http://127.0.0.1:$port (namespace $KIND_NS)"
rc=0
python3 -I tests/lab/lab.py s1 --port "$port" --token-file "$mirror/token" --home "$mirror/home" \
    --cwd /work/lab-agent --out "$out" --fake-trace "$mirror/home/fake-agent.trace.jsonl" || rc=$?
kill "$copier" 2>/dev/null; wait "$copier" 2>/dev/null || true; copier=""
{
    echo "topology: kind k2lab, namespace $KIND_NS, image $KIND_IMAGE, via kubectl port-forward 127.0.0.1:$port"
    echo "image_id: $(docker image inspect -f '{{.Id}}' "$KIND_IMAGE" 2>/dev/null)"
    echo "pod_image_id: $(kc -n "$KIND_NS" get pod "$KIND_POD" -o jsonpath='{.status.containerStatuses[0].imageID}')"
    echo "daemon_version: $(kc -n "$KIND_NS" exec "$KIND_POD" -c k2-daemon -- k2-daemon --version 2>&1 | head -1)"
    echo "sha: $(git rev-parse HEAD)"
} > "$out/topology.txt"
curl -s --connect-timeout 2 "http://127.0.0.1:$port/boot-status" > "$out/boot-status.json" || true
say "evidence: ${out#"$KIND_REPO"/}/"
exit "$rc"
