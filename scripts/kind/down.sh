#!/usr/bin/env bash
# Take K2 off the kind cluster k2lab (what scripts/kind/up.sh put there).
#
#   scripts/kind/down.sh [--dist] [--fake] [--keep-data]
#
# Removes the Mac's door (the proxy container, or the fallback
# port-forward) and deletes the namespace (k2-kind; k2-kind-fake
# with --fake; k2-kind-dist or k2-kind-dist-fake with --dist), which deletes
# the pods and their volumes: K2's state and the Claude logins with them.
# --keep-data removes the door and scales the StatefulSets to zero
# instead, keeping the volumes for the next up.sh.
# Local images (k2-daemon:dev, k2-agents:*) stay in Docker; remove them with
# `docker image rm` if you want the space back.
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"

fake=0 dist=0 keep=0
for a in "$@"; do
    case "$a" in
        --fake) fake=1 ;;
        --dist) dist=1 ;;
        --keep-data) keep=1 ;;
        -h|--help) sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "down.sh: unknown argument $a (see --help)" >&2; exit 2 ;;
    esac
done
kind_variant_from_flags "$fake" "$dist"
kind_stop_forward
kind_stop_proxy
kind_kubeconfig
if [ "$keep" = 1 ]; then
    if kc get namespace "$KIND_NS" >/dev/null 2>&1; then
        for pair in $KIND_ROLLOUT; do
            kc -n "$KIND_NS" scale "statefulset/${pair%%=*}" --replicas=0 >&2
        done
        say "scaled the StatefulSets in $KIND_NS to 0; the volumes are kept"
    fi
else
    kc delete namespace "$KIND_NS" --ignore-not-found --wait=true --timeout=300s >&2
    say "deleted namespace $KIND_NS (pods, services, volumes, Secret)"
fi
rm -f "$KIND_STATE/port"
