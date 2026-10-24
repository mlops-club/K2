#!/usr/bin/env bash
# Take K2 off the kind cluster k2lab (what scripts/kind/up.sh put there).
#
#   scripts/kind/down.sh [--fake] [--keep-data]
#
# Stops the port-forward and deletes the namespace (k2-kind, or k2-kind-fake
# with --fake), which deletes the pod and its volume: K2's state and the
# Claude login with it. --keep-data stops the port-forward and scales the
# StatefulSet to zero instead, keeping the volume for the next up.sh.
# Local images (k2-daemon:dev, k2-agents:*) stay in Docker; remove them with
# `docker image rm` if you want the space back.
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"

variant=agents keep=0
for a in "$@"; do
    case "$a" in
        --fake) variant=fake ;;
        --keep-data) keep=1 ;;
        -h|--help) sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "down.sh: unknown argument $a (see --help)" >&2; exit 2 ;;
    esac
done
kind_variant "$variant"
kind_stop_forward
kind_kubeconfig
if [ "$keep" = 1 ]; then
    if kc get namespace "$KIND_NS" >/dev/null 2>&1; then
        kc -n "$KIND_NS" scale statefulset/k2-solo --replicas=0 >&2
        say "scaled $KIND_NS/k2-solo to 0; the volume is kept"
    fi
else
    kc delete namespace "$KIND_NS" --ignore-not-found --wait=true --timeout=300s >&2
    say "deleted namespace $KIND_NS (pod, service and volume)"
fi
rm -f "$KIND_STATE/port"
