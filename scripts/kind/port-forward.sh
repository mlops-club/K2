#!/usr/bin/env bash
# Keep 127.0.0.1:<port> forwarded to the K2 pod on k2lab. Started in the
# background by up.sh, stopped by down.sh. `kubectl port-forward` exits when
# its pod goes away (a restart, a new image), so this runs it again.
#
#   scripts/kind/port-forward.sh <agents|fake|dist|dist-fake>
set -uo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
kind_variant "${1:?usage: port-forward.sh <agents|fake|dist|dist-fake>}" || exit 2
trap 'kill "$child" 2>/dev/null; exit 0' TERM INT
child=""
while :; do
    kc -n "$KIND_NS" port-forward --address 127.0.0.1 "svc/$KIND_SVC" "$KIND_PORT:7420" &
    child=$!
    wait "$child"
    echo "$(date +%H:%M:%S) port-forward exited ($?); reconnecting" >&2
    sleep 2
done
