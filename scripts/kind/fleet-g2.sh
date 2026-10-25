#!/usr/bin/env bash
# TEST ONLY. The lab's G2 scenario clients against distributed K2 on kind
# with N agent hosts (k2-kind-dist-fake; the fake agent is every CLI):
#
#   scripts/kind/up.sh --dist --fake --hosts=2          # first, once
#   scripts/kind/fleet-g2.sh [--only S1,S2,...] [--chaos seed=N] [--kill=process|pod]
#                            [--lab-src=REF|DIR] [--out=DIR] [--keep-runner]
#
# 1. builds k2-lab-runner:dist-fake (deploy/kind/Dockerfile.lab-runner: the
#    fake host image plus python3 and kubectl) when missing or when the
#    host image changed, and loads it into k2lab (Docker lock held);
# 2. makes the runner pod in k2-kind-dist-fake (ServiceAccount
#    k2-lab-runner, deploy/kind/dist-fake/lab.yaml) on the controller's node,
#    with the controller's and every host's HOME volume and k2-work mounted
#    (scripts/kind/fleet_kind.py has the layout);
# 3. copies the lab in: tests/lab and cli/ from --lab-src (a git ref,
#    default k2-dist/lab-g2, exported with git archive; or a directory),
#    and scripts/kind/fleet_kind.py, fleet-g2-runner.sh from this tree;
# 4. runs scripts/kind/fleet-g2-runner.sh there: S1 (other hosts held),
#    S2, then the rest (under --chaos seed=N, chaos kills CLIs, hosts and
#    the controller: kubectl exec kill, or with --kill=pod kubectl delete
#    pod), the outbox collection and invariants I1 to I4;
# 5. copies /lab/run back to --out (default lab-runs/kind-g2-<UTC time>/)
#    and deletes the runner pod (--keep-runner keeps it).
#
# Default --only: S1,S2,S3,S4,S5,S6,S7 (S8 and S9 once W3 merges). Only
# ever namespace k2-kind-dist-fake. Prints no token and no password.
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
only=S1,S2,S3,S4,S5,S6,S7 chaos="" kill_mode=process lab_src=k2-dist/lab-g2 out="" keep=0
for a in "$@"; do
    case "$a" in
        --only=*) only=${a#--only=} ;;
        --chaos=seed=*) chaos=${a#--chaos=seed=} ;;
        --kill=process|--kill=pod) kill_mode=${a#--kill=} ;;
        --lab-src=*) lab_src=${a#--lab-src=} ;;
        --out=*) out=${a#--out=} ;;
        --keep-runner) keep=1 ;;
        -h|--help) sed -n '2,29p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "fleet-g2.sh: unknown argument $a (see --help)" >&2; exit 2 ;;
    esac
done
case "$chaos" in ""|[0-9]*) ;; *) echo "fleet-g2.sh: --chaos=seed=N" >&2; exit 2 ;; esac
kind_variant dist-fake
kind_kubeconfig
cd "$KIND_REPO"
[ "$KIND_NS" = k2-kind-dist-fake ] || { echo "fleet-g2.sh: refusing namespace $KIND_NS" >&2; exit 1; }
RUNNER=k2-lab-runner RUNNER_IMAGE=k2-lab-runner:dist-fake
out=${out:-$KIND_REPO/lab-runs/kind-g2-$(date -u +%Y%m%dT%H%M%SZ)}
mkdir -p "$out"
out=$(cd "$out" && pwd)

# ── the fleet as deployed ───────────────────────────────────────────────
hosts=$(kc -n "$KIND_NS" get pods -l app.kubernetes.io/name=k2-host -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort -t- -k3 -n | paste -sd, -)
replicas=$(kc -n "$KIND_NS" get statefulset k2-host -o jsonpath='{.spec.replicas}')
[ -n "$hosts" ] && [ "$(echo "$hosts" | tr ',' '\n' | grep -c .)" = "$replicas" ] \
    || { echo "fleet-g2.sh: k2-host has $replicas replica(s) but pods '$hosts': run scripts/kind/up.sh --dist --fake --hosts=N first" >&2; exit 1; }
kc -n "$KIND_NS" get configmap k2-lab-supervise >/dev/null 2>&1 \
    || { echo "fleet-g2.sh: $KIND_NS has no lab supervisor (deploy/kind/dist-fake/lab.yaml): run scripts/kind/up.sh --dist --fake --hosts=N" >&2; exit 1; }
kc -n "$KIND_NS" rollout status statefulset/k2-controller --timeout=300s >&2
kc -n "$KIND_NS" rollout status statefulset/k2-host --timeout=300s >&2
say "fleet: controller k2-controller-0, hosts $hosts"

# ── 1. the runner image ─────────────────────────────────────────────────
base_id=$(docker image inspect -f '{{.Id}}' "$KIND_IMAGE")
if [ "$(docker image inspect -f '{{index .Config.Labels "k2.dev/base-id"}}' "$RUNNER_IMAGE" 2>/dev/null)" != "$base_id" ]; then
    DOCKER_LOCK="$HOME/.cache/k2-dist-docker.lock"
    mkdir -p "$HOME/.cache"
    until mkdir "$DOCKER_LOCK" 2>/dev/null; do say "waiting for $DOCKER_LOCK"; sleep 20; done
    trap 'rmdir "$DOCKER_LOCK" 2>/dev/null' EXIT
    say "building $RUNNER_IMAGE (deploy/kind/Dockerfile.lab-runner FROM $KIND_IMAGE)"
    DOCKER_BUILDKIT=1 docker build -q -f deploy/kind/Dockerfile.lab-runner -t "$RUNNER_IMAGE" \
        --build-arg "BASE_IMAGE=$KIND_IMAGE" --label "k2.dev/base-id=$base_id" deploy/kind >/dev/null
    say "loading $RUNNER_IMAGE into kind cluster $KIND_CLUSTER"
    kind load docker-image "$RUNNER_IMAGE" --name "$KIND_CLUSTER" >&2
    rmdir "$DOCKER_LOCK"; trap - EXIT
fi

# ── 2. the runner pod ───────────────────────────────────────────────────
kc -n "$KIND_NS" delete pod "$RUNNER" --ignore-not-found --wait=true >&2
python3 -I - "$RUNNER" "$RUNNER_IMAGE" "$hosts" "$(docker image inspect -f '{{.Id}}' "$RUNNER_IMAGE")" <<'PY' | kc -n "$KIND_NS" apply -f - >&2
import json, sys
name, image, hosts, image_id = sys.argv[1], sys.argv[2], sys.argv[3].split(","), sys.argv[4]
pods = ["k2-controller-0"] + hosts
vols = [{"name": "work", "persistentVolumeClaim": {"claimName": "k2-work"}},
        {"name": "run", "emptyDir": {}}, {"name": "repo", "emptyDir": {}}, {"name": "tmp", "emptyDir": {}}]
mounts = [{"name": "work", "mountPath": "/var/lib/k2/work"}, {"name": "run", "mountPath": "/lab/run"},
          {"name": "repo", "mountPath": "/lab/repo"}, {"name": "tmp", "mountPath": "/tmp"}]
for i, pod in enumerate(pods):
    vols.append({"name": "home-%d" % i, "persistentVolumeClaim": {"claimName": "data-" + pod}})
    mounts.append({"name": "home-%d" % i, "mountPath": "/lab/homes/" + pod})
print(json.dumps({
    "apiVersion": "v1", "kind": "Pod",
    "metadata": {"name": name, "labels": {"app.kubernetes.io/name": name, "k2.dev/deploy": "kind-dist-fake"},
                 "annotations": {"k2.dev/image-id": image_id}},
    "spec": {
        "serviceAccountName": "k2-lab-runner",
        "terminationGracePeriodSeconds": 1,
        "restartPolicy": "Never",
        # Every volume is ReadWriteOnce on local-path: the controller's node.
        "affinity": {"podAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [{
            "labelSelector": {"matchLabels": {"app.kubernetes.io/name": "k2-controller"}},
            "topologyKey": "kubernetes.io/hostname"}]}},
        "securityContext": {"runAsNonRoot": True, "runAsUser": 10000, "runAsGroup": 0,
                            "seccompProfile": {"type": "RuntimeDefault"}},
        "containers": [{
            "name": "runner", "image": image, "imagePullPolicy": "Never",
            "command": ["sleep", "infinity"],
            "env": [{"name": "HOME", "value": "/lab/run/home"}],
            "resources": {"requests": {"cpu": "250m", "memory": "256Mi"}, "limits": {"memory": "1Gi"}},
            "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
            "volumeMounts": mounts}],
        "volumes": vols}}))
PY
kc -n "$KIND_NS" wait --for=condition=Ready "pod/$RUNNER" --timeout=180s >&2
cleanup() {
    if [ "$keep" = 0 ]; then
        kc -n "$KIND_NS" delete pod "$RUNNER" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

# ── 3. the lab, copied in ───────────────────────────────────────────────
stage=$(mktemp -d "${TMPDIR:-/tmp}/k2-kind-fleet-g2.XXXXXX")
if [ -d "$lab_src" ]; then
    (cd "$lab_src" && tar -cf - tests/lab cli) | tar -xf - -C "$stage"
    lab_rev="dir $(cd "$lab_src" && pwd) ($(git -C "$lab_src" rev-parse --short HEAD 2>/dev/null || echo '?'))"
else
    git archive "$lab_src" tests/lab cli | tar -xf - -C "$stage"
    lab_rev="$lab_src $(git rev-parse --short "$lab_src")"
fi
mkdir -p "$stage/scripts/kind"
cp scripts/kind/fleet_kind.py scripts/kind/fleet-g2-runner.sh "$stage/scripts/kind/"
find "$stage" -name __pycache__ -prune -exec rm -rf {} +
tar -C "$stage" -cf - . | kc -n "$KIND_NS" exec -i "$RUNNER" -- tar -xf - -C /lab/repo
rm -rf "$stage"
say "lab: tests/lab and cli/ from $lab_rev; fleet_kind.py from $(git rev-parse --short HEAD)"

# ── 4. run ──────────────────────────────────────────────────────────────
rc=0
kc -n "$KIND_NS" exec "$RUNNER" -- env NS="$KIND_NS" HOSTS="$hosts" ONLY="$only" CHAOS="$chaos" KILL="$kill_mode" \
    CHAOS_MIN_S="${LAB_CHAOS_MIN_S:-180}" bash /lab/repo/scripts/kind/fleet-g2-runner.sh || rc=$?

# ── 5. the evidence ─────────────────────────────────────────────────────
kc -n "$KIND_NS" exec "$RUNNER" -- tar -C /lab/run --exclude=./home -cf - . | tar -xf - -C "$out"
{
    echo "lab: $lab_rev"
    echo "kind: $(git rev-parse HEAD) ($(git rev-parse --abbrev-ref HEAD)), namespace $KIND_NS, hosts $hosts, kill=$kill_mode${chaos:+, chaos seed=$chaos}"
    for img in "$KIND_BASE_IMAGE" "$KIND_IMAGE" "$RUNNER_IMAGE"; do
        echo "image $img: $(docker image inspect -f '{{.Id}} {{index .Config.Labels "org.opencontainers.image.revision"}}' "$img" 2>/dev/null)"
    done
} > "$out/kind.txt"
say "evidence: $out (rc=$rc)"
exit $rc
