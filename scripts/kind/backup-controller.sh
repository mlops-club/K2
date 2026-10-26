#!/usr/bin/env bash
# Back up (or restore) the owner's controller volume inside the cluster.
#
#   scripts/kind/backup-controller.sh [--leave-down]   # back up → prints the backup PVC's name
#   scripts/kind/backup-controller.sh --restore=PVC    # put that backup back
#   ... [--fake]                                       # the test variant k2-kind-dist-fake
#
# In k2-kind-dist (or k2-kind-dist-fake with --fake) only. The controller is scaled to 0 for the copy, so its
# SQLite database is closed (a consistent copy), then back to 1.
# - backup: a Job tars data-k2-controller-0 into a new PVC
#   k2-backup-controller-<UTC stamp> (local-path, made on the same node),
#   with the archive's sha256 and file count next to it. Nothing leaves the
#   cluster: no copy to the Mac, git or a log; only sizes and the checksum
#   are printed. --leave-down leaves the controller at 0 after a good
#   backup (the roll that follows brings it up on its new image; after a
#   `kind load` the old tag already points at the new one on the nodes).
# - restore: a Job checks the archive's sha256, empties data-k2-controller-0
#   and untars the archive into it (through a fresh directory the Job owns,
#   then moved up: the volume root's owner and mode are not the Job's to set). Then the controller is scaled to
#   1. In enroll mode (K2_HOST_AUTH=enroll) the restored database may not
#   know the hosts' keys any more: each host gets a fresh one-time token
#   (enroll.py, on a pipe) and its pod is restarted, since a host reads its
#   token file only at boot. Pair a restore with the images the backup was
#   made with (owner-roll-wave3.sh --help, rollback).
# Only k2lab, through an explicit kubeconfig.
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
restore= leave_down=0 fake=0
for a in "$@"; do
    case "$a" in
        --restore=*) restore=${a#--restore=} ;;
        --leave-down) leave_down=1 ;;
        --fake) fake=1 ;;
        -h|--help) sed -n '2,19p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "backup-controller.sh: unknown argument $a" >&2; exit 2 ;;
    esac
done
kind_variant_from_flags "$fake" 1
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
      # The node of data-k2-controller-0's local-path volume, through the
      # scheduler (nodeName would bypass it, and the new backup PVC, which
      # binds on first consumer, would never bind).
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - {key: kubernetes.io/hostname, operator: In, values: [$node]}
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
    # A failed Job never completes: poll for either, with a deadline.
    local i ok="" failed=""
    for i in $(seq 1 120); do
        ok=$(kd get job "$1" -o jsonpath='{.status.succeeded}' 2>/dev/null || true)
        failed=$(kd get job "$1" -o jsonpath='{.status.failed}' 2>/dev/null || true)
        [ -n "$ok$failed" ] && break
        sleep 5
    done
    if [ "${ok:-0}" -ge 1 ] 2>/dev/null; then
        kd logs "job/$1"
        return 0
    fi
    echo "backup-controller.sh: job $1 $( [ -n "$failed" ] && echo failed || echo "did not finish in 600 s"):" >&2
    kd logs "job/$1" 2>&1 | tail -8 >&2
    return 1
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
    if [ "$leave_down" = 1 ]; then say "the controller stays at 0 (--leave-down)"; else scale_controller 1; fi
    say "backup: $out"
    echo "$name"
else
    kd get pvc "$restore" >/dev/null || { echo "backup-controller.sh: no backup PVC $restore" >&2; exit 1; }
    say "restoring data-k2-controller-0 from $restore (the controller is scaled to 0)"
    scale_controller 0
    trap 'scale_controller 1' EXIT
    out=$(job "restore-$(date -u +%H%M%S)" "$restore" 'cd /backup && sha256sum -c controller.tar.gz.sha256 >/dev/null && find /data -mindepth 1 -delete && mkdir /data/.k2-restore && tar -xzf controller.tar.gz -C /data/.k2-restore --no-same-owner && for e in /data/.k2-restore/* /data/.k2-restore/.[!.]* /data/.k2-restore/..?*; do [ -e "$e" ] || [ -L "$e" ] || continue; mv "$e" /data/; done && rmdir /data/.k2-restore && echo "restored $(find /data | wc -l) paths"')
    trap - EXIT
    scale_controller 1
    say "restore: $out"
    if kd get statefulset k2-controller -o jsonpath='{.spec.template.spec.containers[0].env}' | grep -q '"K2_HOST_AUTH","value":"enroll"\|"value":"enroll","name":"K2_HOST_AUTH"'; then
        n=$(kd get statefulset k2-host -o jsonpath='{.spec.replicas}')
        hosts=$(for i in $(seq 0 $(( ${n:-1} - 1 ))); do printf 'k2-host-%s\n' "$i"; done | paste -sd, -)
        say "enroll mode: fresh one-time tokens for $hosts, then their pods restart"
        kind_wait_ready || exit 1
        kd exec "$KIND_POD" -c k2-daemon -- cat /var/lib/k2/.k2/daemon.token \
            | python3 -I "$KIND_REPO/scripts/kind/enroll.py" mint --kubeconfig "$KIND_KUBECONFIG" --namespace "$KIND_NS" \
                --url "http://127.0.0.1:$KIND_PORT" --hosts "$hosts" --deploy-label "kind-$KIND_VARIANT" >&2
        # The host reads its token file only at boot (a daemon gap, reported to dist-link).
        for h in ${hosts//,/ }; do kd delete pod "$h" --wait=false >&2; done
        kd rollout status statefulset/k2-host --timeout=600s >&2
    fi
fi
