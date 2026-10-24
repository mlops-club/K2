#!/usr/bin/env bash
# Keep 127.0.0.1:<port> forwarded to the K2 pod on k2lab. Started in the
# background, in its own session, by up.sh and forward.sh (kind_start_forward
# in lib.sh); stopped by down.sh and forward.sh.
#
#   scripts/kind/port-forward.sh <agents|fake|dist|dist-fake>
#
# `kubectl port-forward` exits when its pod goes away (a restart, an OOM
# kill, a new image), so this runs it again. It can also stay alive while
# every connection fails; a health probe on /boot-status catches that and
# restarts it. Every start, restart and signal is logged with a timestamp
# (appended to port-forward.log), so a dead forward leaves a reason.
set -uo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
kind_variant "${1:?usage: port-forward.sh <agents|fake|dist|dist-fake>}" || exit 2

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') [port-forward $$] $*" >&2; }
child="" watcher=""
stop() {  # stop <signal>
    log "stopping on SIG$1"
    [ -n "$child" ] && kill "$child" 2>/dev/null
    [ -n "$watcher" ] && kill "$watcher" 2>/dev/null
    exit 0
}
for sig in TERM INT HUP QUIT; do
    # shellcheck disable=SC2064  # expand $sig now, once per trap
    trap "stop $sig" "$sig"
done
trap 'log "exiting (status $?)"' EXIT
log "started for $KIND_NS svc/$KIND_SVC on 127.0.0.1:$KIND_PORT (parent $PPID)"

# The watcher probes /boot-status every PROBE_EVERY seconds and kills
# kubectl after PROBE_FAILS failures in a row (it is alive but forwards
# nothing); the loop itself blocks on kubectl, so an exit restarts at once.
# A probe fails only when /boot-status doesn't answer; a booting daemon does.
PROBE_EVERY=10 PROBE_FAILS=3
watch_health() {  # watch_health <kubectl pid>
    local fails=0
    while sleep "$PROBE_EVERY" && kill -0 "$1" 2>/dev/null; do
        if curl -s -o /dev/null --connect-timeout 3 --max-time 5 "http://127.0.0.1:$KIND_PORT/boot-status"; then
            fails=0
        else
            fails=$((fails + 1))
            if [ "$fails" -ge "$PROBE_FAILS" ]; then
                log "/boot-status unreachable $fails times in a row; restarting kubectl"
                kill "$1" 2>/dev/null
                return
            fi
        fi
    done
}
while :; do
    # kubectl itself, not the kc function: a function in the background is a
    # subshell, and killing that would orphan kubectl (and keep the port).
    kubectl --kubeconfig "$KIND_KUBECONFIG" -n "$KIND_NS" port-forward --address 127.0.0.1 \
        "svc/$KIND_SVC" "$KIND_PORT:7420" &
    child=$!
    ( trap - TERM INT HUP QUIT EXIT; watch_health "$child" ) &
    watcher=$!
    wait "$child" 2>/dev/null       # a trapped signal interrupts this wait
    rc=$?
    kill "$watcher" 2>/dev/null; wait "$watcher" 2>/dev/null
    log "kubectl port-forward exited ($rc); reconnecting in 2 s"
    child="" watcher=""
    sleep 2
done
