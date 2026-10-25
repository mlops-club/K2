#!/usr/bin/env bash
# The Mac's door to K2 on kind k2lab, on its own: no build of K2, no apply.
#
#   scripts/kind/forward.sh [--dist] [--fake]                  # (re)create the proxy container, wait for the daemon
#   scripts/kind/forward.sh [--dist] [--fake] --port-forward   # fallback: the kubectl port-forward loop instead
#   scripts/kind/forward.sh [--dist] [--fake] --status         # what is serving the port, and does it answer?
#
# The proxy (deploy/kind/proxy, container <namespace>-proxy) relays
# 127.0.0.1:<port> to the K2 Service's NodePort; Docker restarts it. Either
# door replaces the other. The app reconnects by itself.
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
fake=0 dist=0 mode=proxy
for a in "$@"; do
    case "$a" in
        --fake) fake=1 ;;
        --dist) dist=1 ;;
        --status) mode=status ;;
        --port-forward) mode=port-forward ;;
        -h|--help) sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "forward.sh: unknown argument $a (see --help)" >&2; exit 2 ;;
    esac
done
kind_variant_from_flags "$fake" "$dist"
base="http://127.0.0.1:$KIND_PORT"

if [ "$mode" = status ]; then
    echo "proxy $KIND_PROXY: $(docker inspect -f '{{.State.Status}} (restarts {{.RestartCount}}, started {{.State.StartedAt}}) {{join .Args " "}}' "$KIND_PROXY" 2>/dev/null || echo "not present")"
    pid=$(cat "$KIND_STATE/port-forward.pid" 2>/dev/null || true)
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        echo "port-forward loop: running (pid $pid, log $KIND_STATE/port-forward.log)"
    else
        echo "port-forward loop: not running"
    fi
    echo "listening on 127.0.0.1:$KIND_PORT: $(lsof -nP -iTCP:"$KIND_PORT" -sTCP:LISTEN 2>/dev/null | awk 'NR>1 {print $1 " pid " $2}' | sort -u | tr '\n' ' ')"
    echo "boot-status: $(curl -s --connect-timeout 2 --max-time 5 "$base/boot-status" \
        | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("phase","?"))' 2>/dev/null || echo unreachable)"
    exit 0
fi

kind_kubeconfig
if [ "$mode" = proxy ]; then
    docker image inspect "$KIND_PROXY_IMAGE" >/dev/null 2>&1 \
        || DOCKER_BUILDKIT=1 docker build -q -t "$KIND_PROXY_IMAGE" "$KIND_REPO/deploy/kind/proxy" >/dev/null
    kind_start_proxy
    door="proxy $KIND_PROXY → $KIND_PROXY_TARGET:$KIND_NODEPORT"
else
    kind_stop_proxy
    kind_start_forward
    door="port-forward loop (pid $(cat "$KIND_STATE/port-forward.pid"))"
fi
kind_wait_ready
say "$base → $KIND_NS via $door"
