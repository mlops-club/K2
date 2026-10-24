# shellcheck shell=bash disable=SC2034  # sourced; the KIND_* vars are for the callers
# Shared settings for scripts/kind/*.sh (sourced, not run).
#
# Only ever the kind cluster k2lab, only ever through an explicit kubeconfig
# written by `kind get kubeconfig`: the current kube context is never read
# or changed.

KIND_CLUSTER=k2lab
KIND_REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

# Variant (each has its own namespace, port and state dir):
#   agents      one pod, k2-agents:dev (the owner's)            k2-kind            7440
#   fake        one pod, k2-agents:fake (test only)             k2-kind-fake       7441
#   dist        controller + host, host on k2-agents:dev        k2-kind-dist       7443
#   dist-fake   controller + host, host on k2-agents:fake       k2-kind-dist-fake  7442
# KIND_POD is where the daemon token lives (the pod the app talks to);
# KIND_AGENT_POD is where agents run. KIND_ROLLOUT pairs each StatefulSet
# with the image whose id rolls it.
kind_variant() {
    case "$1" in
        agents|dist)
            KIND_IMAGE=k2-agents:dev KIND_DOCKERFILE=deploy/kind/Dockerfile.agents KIND_CONTEXT=deploy/kind ;;
        fake|dist-fake)
            KIND_IMAGE=k2-agents:fake KIND_DOCKERFILE=deploy/kind/Dockerfile.fake KIND_CONTEXT=. ;;
        *) echo "unknown variant $1" >&2; return 2 ;;
    esac
    case "$1" in
        agents) KIND_NS=k2-kind KIND_OVERLAY=deploy/kind/agents KIND_PORT=${K2_KIND_PORT:-7440} KIND_FLAGS="" ;;
        fake) KIND_NS=k2-kind-fake KIND_OVERLAY=deploy/kind/fake KIND_PORT=${K2_KIND_PORT:-7441} KIND_FLAGS=" --fake" ;;
        dist) KIND_NS=k2-kind-dist KIND_OVERLAY=deploy/kind/dist KIND_PORT=${K2_KIND_PORT:-7443} KIND_FLAGS=" --dist" ;;
        dist-fake) KIND_NS=k2-kind-dist-fake KIND_OVERLAY=deploy/kind/dist-fake KIND_PORT=${K2_KIND_PORT:-7442} KIND_FLAGS=" --dist --fake" ;;
    esac
    case "$1" in
        agents|fake)
            KIND_DIST=0 KIND_SVC=k2-solo KIND_POD=k2-solo-0
            KIND_AGENT_POD=k2-solo-0 KIND_AGENT_CONTAINER=k2-daemon KIND_AGENT_HOME=/var/lib/k2
            KIND_ROLLOUT="k2-solo=$KIND_IMAGE" KIND_LOAD="$KIND_IMAGE" ;;
        dist|dist-fake)
            KIND_DIST=1 KIND_SVC=k2-controller KIND_POD=k2-controller-0
            KIND_AGENT_POD=k2-host-0 KIND_AGENT_CONTAINER=k2-host KIND_AGENT_HOME=/var/lib/k2-host
            KIND_ROLLOUT="k2-controller=k2-daemon:dev k2-host=$KIND_IMAGE" KIND_LOAD="k2-daemon:dev $KIND_IMAGE" ;;
    esac
    KIND_VARIANT=$1
    KIND_STATE="${TMPDIR:-/tmp}/k2-kind/$KIND_VARIANT"
    KIND_KUBECONFIG="$KIND_STATE/kubeconfig"
    mkdir -p "$KIND_STATE"
    chmod 700 "$KIND_STATE"
}

# Variant from the common flags: kind_variant_from_flags <fake 0|1> <dist 0|1>
kind_variant_from_flags() {
    case "$1$2" in
        00) kind_variant agents ;;
        10) kind_variant fake ;;
        01) kind_variant dist ;;
        11) kind_variant dist-fake ;;
    esac
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
    local pid _
    pid=$(cat "$KIND_STATE/port-forward.pid" 2>/dev/null) || pid=""
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        for _ in $(seq 1 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    fi
    rm -f "$KIND_STATE/port-forward.pid"
    # Any kubectl port-forward left on this variant's own kubeconfig (an
    # interrupted up.sh can orphan one); nothing else uses that path.
    pkill -f -- "--kubeconfig $KIND_KUBECONFIG -n $KIND_NS port-forward" 2>/dev/null || true
    for _ in $(seq 1 50); do
        pgrep -f -- "--kubeconfig $KIND_KUBECONFIG -n $KIND_NS port-forward" >/dev/null || break
        sleep 0.1
    done
}

# Start the port-forward loop (port-forward.sh) for this variant, after
# stopping any old one. It runs in its own session (setsid), so a signal to
# whatever started it (a terminal closing, a tool cleaning up its process
# group) never reaches it; only kind_stop_forward stops it. The log is
# appended, never overwritten.
kind_start_forward() {
    kind_stop_forward
    if lsof -nP -iTCP:"$KIND_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "127.0.0.1:$KIND_PORT is already in use; set K2_KIND_PORT to a free port" >&2
        return 1
    fi
    python3 -I -c 'import os, sys; os.setsid(); os.execv(sys.argv[1], sys.argv[1:])' \
        "$KIND_REPO/scripts/kind/port-forward.sh" "$KIND_VARIANT" \
        </dev/null >>"$KIND_STATE/port-forward.log" 2>&1 &
    echo $! > "$KIND_STATE/port-forward.pid"     # execv keeps the pid
    echo "$KIND_PORT" > "$KIND_STATE/port"
}

# Wait until /boot-status through the forward says phase=ready.
kind_wait_ready() {
    local phase="" _
    for _ in $(seq 1 120); do
        phase=$(curl -s --connect-timeout 1 "http://127.0.0.1:$KIND_PORT/boot-status" 2>/dev/null \
            | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("phase",""))' 2>/dev/null || true)
        [ "$phase" = ready ] && return 0
        sleep 0.5
    done
    echo "http://127.0.0.1:$KIND_PORT/boot-status never said phase=ready (last: '$phase'); see $KIND_STATE/port-forward.log" >&2
    return 1
}
