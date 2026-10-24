#!/usr/bin/env bash
# The port-forward to K2 on kind k2lab, on its own: no build, no apply.
#
#   scripts/kind/forward.sh [--dist] [--fake]            # (re)start it, wait for the daemon
#   scripts/kind/forward.sh [--dist] [--fake] --status   # is it running and answering?
#
# Restarting stops the old loop (and any kubectl it orphaned) and starts a
# new one in its own session; the app reconnects by itself. The log,
# appended across restarts, is $TMPDIR/k2-kind/<variant>/port-forward.log.
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
fake=0 dist=0 status=0
for a in "$@"; do
    case "$a" in
        --fake) fake=1 ;;
        --dist) dist=1 ;;
        --status) status=1 ;;
        -h|--help) sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "forward.sh: unknown argument $a (see --help)" >&2; exit 2 ;;
    esac
done
kind_variant_from_flags "$fake" "$dist"
base="http://127.0.0.1:$KIND_PORT"

if [ "$status" = 1 ]; then
    pid=$(cat "$KIND_STATE/port-forward.pid" 2>/dev/null || true)
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        echo "loop: running (pid $pid)"
    else
        echo "loop: not running"
    fi
    echo "kubectl: $(pgrep -f -- "--kubeconfig $KIND_KUBECONFIG -n $KIND_NS port-forward" | tr '\n' ' ')"
    echo "boot-status: $(curl -s --connect-timeout 2 --max-time 5 "$base/boot-status" \
        | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("phase","?"))' 2>/dev/null || echo unreachable)"
    echo "log ($KIND_STATE/port-forward.log), last lines:"
    tail -5 "$KIND_STATE/port-forward.log" 2>/dev/null | sed 's/^/  /'
    exit 0
fi

kind_kubeconfig
kind_start_forward
kind_wait_ready
say "forwarding $base → $KIND_NS svc/$KIND_SVC (pid $(cat "$KIND_STATE/port-forward.pid"))"
