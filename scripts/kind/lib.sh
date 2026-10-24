# Shared settings for scripts/kind/*.sh (sourced, not run).
#
# Only ever the kind cluster k2lab, only ever through an explicit kubeconfig
# written by `kind get kubeconfig`: the current kube context is never read
# or changed.

KIND_CLUSTER=k2lab
KIND_REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

# Variant: "agents" (k2-agents:dev, the owner's) or "fake" (k2-agents:fake,
# test only). Each has its own namespace, port and state dir.
kind_variant() {
    case "$1" in
        agents)
            KIND_NS=k2-kind KIND_IMAGE=k2-agents:dev KIND_OVERLAY=deploy/kind/agents
            KIND_DOCKERFILE=deploy/kind/Dockerfile.agents KIND_CONTEXT=deploy/kind
            KIND_PORT=${K2_KIND_PORT:-7440} ;;
        fake)
            KIND_NS=k2-kind-fake KIND_IMAGE=k2-agents:fake KIND_OVERLAY=deploy/kind/fake
            KIND_DOCKERFILE=deploy/kind/Dockerfile.fake KIND_CONTEXT=.
            KIND_PORT=${K2_KIND_PORT:-7441} ;;
        *) echo "unknown variant $1" >&2; return 2 ;;
    esac
    KIND_VARIANT=$1
    KIND_STATE="${TMPDIR:-/tmp}/k2-kind/$KIND_VARIANT"
    KIND_KUBECONFIG="$KIND_STATE/kubeconfig"
    KIND_POD=k2-solo-0
    mkdir -p "$KIND_STATE"
    chmod 700 "$KIND_STATE"
}

kind_kubeconfig() {
    if ! kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER"; then
        echo "kind cluster $KIND_CLUSTER does not exist (kind get clusters)" >&2
        return 1
    fi
    kind get kubeconfig --name "$KIND_CLUSTER" > "$KIND_KUBECONFIG"
    chmod 600 "$KIND_KUBECONFIG"
}

kc() { kubectl --kubeconfig "$KIND_KUBECONFIG" "$@"; }

say() { printf '%s kind: %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

# Stop the port-forward loop this variant started, if any.
kind_stop_forward() {
    local pid
    pid=$(cat "$KIND_STATE/port-forward.pid" 2>/dev/null) || return 0
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        pkill -P "$pid" 2>/dev/null || true
        kill "$pid" 2>/dev/null || true
    fi
    rm -f "$KIND_STATE/port-forward.pid"
}
