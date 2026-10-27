#!/bin/sh
# TEST ONLY (k2-kind-dist-fake): k2-daemon under a small supervisor the
# lab's kind backend drives (scripts/kind/fleet_kind.py), as compose's
# `docker compose kill` / `start` drive a container:
#
#   $HOME/.k2-lab/hold         while it exists, the daemon is not (re)started
#   $HOME/.k2-lab/daemon.pid   the running daemon's pid (removed when it exits)
#   $HOME/.k2-lab/traces/      K2_TRACE_FILE and K2_FAKE_AGENT_TRACE
#
# The lab kills the daemon itself (kubectl exec kill -KILL|-TERM <pid>) after
# writing the hold file, and removes the hold file to start it again: a
# down-time the lab chooses, with no kubelet restart back-off, the same pod
# and the same volumes. A daemon that exits with no hold file is started
# again at once. SIGTERM to the pod (kubectl delete) goes to the daemon,
# which drains as it would without this script; then the script exits.
set -u
lab="$HOME/.k2-lab"
mkdir -p "$lab/traces"
rm -f "$lab/daemon.pid"
child=""
stop() {
    if [ -n "$child" ]; then
        kill -TERM "$child" 2>/dev/null
        wait "$child"
    fi
    rm -f "$lab/daemon.pid"
    exit 0
}
trap stop TERM INT
while :; do
    while [ -e "$lab/hold" ]; do sleep 0.2; done
    "$@" &
    child=$!
    echo "$child" > "$lab/daemon.pid"
    wait "$child"
    rm -f "$lab/daemon.pid"
    child=""
    echo "supervise: $1 exited at $(date -u +%H:%M:%SZ)$( [ -e "$lab/hold" ] && echo ', held by the lab')" >&2
    sleep 0.2
done
