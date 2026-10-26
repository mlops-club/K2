# shellcheck shell=bash disable=SC2034  # sourced; the CAPS_* vars are for the callers
# Shared settings for scripts/kind/caps/*.sh (T3 k8s-capabilities, sourced).
#
# Only the kind cluster k2lab, only through an explicit kubeconfig written by
# `kind get kubeconfig`; never the current kube context (`default` is the
# owner's real k3s cluster). Only namespace k2-p4-caps: never k2-kind,
# k2-kind-dist or their proxies, never agent-sandbox-system.

CAPS_CLUSTER=k2lab
CAPS_NS=k2-p4-caps
CAPS_REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
CAPS_STATE="${TMPDIR:-/tmp}/k2-kind/caps"
CAPS_KUBECONFIG="$CAPS_STATE/kubeconfig"
# An image already on k2lab's nodes (the kind-app worker's fake host
# image): perl and curl, no pull. Used for probes only.
CAPS_PROBE_IMAGE=${CAPS_PROBE_IMAGE:-k2-agents:dist-fake}

caps_kubeconfig() {
    mkdir -p "$CAPS_STATE" && chmod 700 "$CAPS_STATE"
    kind get clusters 2>/dev/null | grep -qx "$CAPS_CLUSTER" \
        || { echo "kind cluster $CAPS_CLUSTER does not exist" >&2; return 1; }
    kind get kubeconfig --name "$CAPS_CLUSTER" > "$CAPS_KUBECONFIG"
    chmod 600 "$CAPS_KUBECONFIG"
}

kc() { kubectl --kubeconfig "$CAPS_KUBECONFIG" "$@"; }
kn() { kubectl --kubeconfig "$CAPS_KUBECONFIG" -n "$CAPS_NS" "$@"; }
say() { printf '%s caps: %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

# The namespace, Pod Security "restricted" like the declared end state.
caps_namespace() {
    kc apply -f - >&2 <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: $CAPS_NS
  labels:
    k2.dev/test: k8s-capabilities
    pod-security.kubernetes.io/enforce: restricted
    pod-security.kubernetes.io/warn: restricted
EOF
}
