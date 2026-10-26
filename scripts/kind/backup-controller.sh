#!/usr/bin/env bash
# Back up (or restore) the owner's controller volume inside the cluster.
#
#   scripts/kind/backup-controller.sh                  # back up → prints the backup PVC's name
#   scripts/kind/backup-controller.sh --restore=PVC    # put that backup back
#
# In k2-kind-dist only. The controller is scaled to 0 for the copy, so its
# SQLite database is closed (a consistent copy), then back to 1.
# - backup: a Job tars data-k2-controller-0 into a new PVC
#   k2-backup-controller-<UTC stamp> (local-path, made on the same node),
#   with the archive's sha256 and file count next to it. Nothing leaves the
#   cluster: no copy to the Mac, git or a log; only sizes and the checksum
#   are printed.
# - restore: a Job checks the archive's sha256, empties data-k2-controller-0
#   and untars the archive into it. Then the controller is scaled to 1;
#   pair it with the images it was backed up with (owner-roll-wave3.sh
#   --help, rollback).
# Only k2lab, through an explicit kubeconfig.
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
restore=
for a in "$@"; do
    case "$a" in
        --restore=*) restore=${a#--restore=} ;;
        -h|--help) sed -n '2,19p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "backup-controller.sh: unknown argument $a" >&2; exit 2 ;;
    esac
done
kind_variant dist
kind_kubeconfig
kd() { kubectl --kubeconfig "$KIND_KUBECONFIG" -n "$KIND_NS" "$@"; }
node=$(kubectl --kubeconfig "$KIND_KUBECONFIG" get pv "$(kd get pvc data-k2-controller-0 -o jsonpath='{.spec.volumeName}')" \
    -o jsonpath='{.spec.nodeAffinity.required.nodeSelectorTerms[0].matchExpressions[0].values[0]}')
image=$(kd get statefulset k2-controller -o jsonpath='{.spec.template.spec.containers[0].image}')   # has tar and sha256sum
job() {  # job <name> <backup pvc> <script>
    kd delete job "$1" --ignore-not-found --wait=true >/dev/null
    kd apply -f - >&2 <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $1
  labels: {k2.dev/deploy: kind-dist, k2.dev/backup: controller}
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    spec:
      restartPolicy: Never
      nodeName: $node
      securityContext: {runAsNonRoot: true, runAsUser: 10000, runAsGroup: 0, fsGroup: 10000, seccompProfile: {type: RuntimeDefault}}
      containers:
        - name: tar
          image: $image
          imagePullPolicy: Never
          command: [sh, -ec, $(python3 -I -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$3")]
          securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
          volumeMounts:
            - {name: data, mountPath: /data}
            - {name: backup, mountPath: /backup}
      volumes:
        - name: data
          persistentVolumeClaim: {claimName: data-k2-controller-0}
        - name: backup
          persistentVolumeClaim: {claimName: $2}
EOF
    if ! kd wait --for=condition=complete "job/$1" --timeout=600s >&2; then
        kd logs "job/$1" 2>&1 | tail -5 >&2
        return 1
    fi
    kd logs "job/$1"
}
scale_controller() {
    kd scale statefulset k2-controller --replicas="$1" >&2
    if [ "$1" = 0 ]; then
        kd wait --for=delete pod/k2-controller-0 --timeout=180s >&2 || true
    else
        kd rollout status statefulset/k2-controller --timeout=600s >&2
    fi
}

if [ -z "$restore" ]; then
    name="k2-backup-controller-$(date -u +%Y%m%d-%H%M%S)"
    size=$(kd get pvc data-k2-controller-0 -o jsonpath='{.spec.resources.requests.storage}')
    kd apply -f - >&2 <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: $name
  labels: {k2.dev/deploy: kind-dist, k2.dev/backup: controller}
spec:
  accessModes: [ReadWriteOnce]
  resources: {requests: {storage: $size}}
EOF
    say "scaling the controller to 0 for a consistent copy"
    scale_controller 0
    trap 'scale_controller 1' EXIT
    out=$(job "$name" "$name" 'cd /data && tar -czf /backup/controller.tar.gz . && cd /backup && sha256sum controller.tar.gz > controller.tar.gz.sha256 && tar -tzf controller.tar.gz | wc -l > files.count && echo "archive $(du -h controller.tar.gz | cut -f1), $(cat files.count) entries, sha256 $(cut -c1-16 controller.tar.gz.sha256)…"')
    trap - EXIT
    scale_controller 1
    say "backup: $out"
    echo "$name"
else
    kd get pvc "$restore" >/dev/null || { echo "backup-controller.sh: no backup PVC $restore" >&2; exit 1; }
    say "restoring data-k2-controller-0 from $restore (the controller is scaled to 0)"
    scale_controller 0
    trap 'scale_controller 1' EXIT
    out=$(job "restore-$(date -u +%H%M%S)" "$restore" 'cd /backup && sha256sum -c controller.tar.gz.sha256 >/dev/null && find /data -mindepth 1 -delete && tar -xzf controller.tar.gz -C /data && echo "restored $(find /data | wc -l) paths"')
    trap - EXIT
    scale_controller 1
    say "restore: $out"
fi
