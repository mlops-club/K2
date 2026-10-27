#!/usr/bin/env bash
# Copy one workspaces volume onto another, inside the cluster: the fs-relay
# migration's data step (docs/prd/progress/fs-relay.md, "The owner's K2 at
# rc3"). Forward, k2-work (shared) → a host's own work-k2-host-N; back, the
# reverse; or a backup copy onto a new claim.
#
#   scripts/kind/work-copy.sh --ns=NS --from=PVC --to=PVC [--mirror] [--go]
#                             [--allow-mounted=POD[,POD]] [--owner]
#   scripts/kind/work-copy.sh --ns=NS --from=PVC --backup [--go]
#
# Dry run (the default) changes nothing. It shows both claims (size, the
# node their volume is on, the pods that mount them) and the copy pod it
# would run, and says whether --go would go ahead.
#
# --go runs one pod, on the node the volumes are on (the scheduler follows
# the volumes), with the namespace's own daemon image, as K2's own pods run
# (uid 10000, gid 0, fsGroup 10000, the "restricted" pod security the
# namespaces enforce): it copies
# FROM onto TO with tar (modes, times, links kept), then compares the two
# trees file by file (sha256 of every regular file, and every link's
# target), and fails unless they match. The pod is removed after.
# --mirror first empties TO (for the copy back, where TO holds a stale
# copy): take a --backup of TO before.
# --backup creates the claim k2-backup-work-<UTC> (FROM's size) and copies
# FROM onto it.
#
# Refused with --go: the two volumes on different nodes; any running pod
# mounting TO (stop it first; --allow-mounted names pods that may, such as
# the controller when it doesn't write there); the owner's namespace
# k2-kind-dist without --owner.
#
# Only k2lab, through an explicit kubeconfig; never the current context.
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
ns="" from="" to="" go=0 mirror=0 backup=0 owner=0 allow=""
for a in "$@"; do
    case "$a" in
        --ns=*) ns=${a#--ns=} ;;
        --from=*) from=${a#--from=} ;;
        --to=*) to=${a#--to=} ;;
        --go) go=1 ;;
        --mirror) mirror=1 ;;
        --backup) backup=1 ;;
        --owner) owner=1 ;;
        --allow-mounted=*) allow=${a#--allow-mounted=} ;;
        -h|--help) sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "work-copy.sh: unknown argument $a (see --help)" >&2; exit 2 ;;
    esac
done
[ -n "$ns" ] && [ -n "$from" ] || { echo "work-copy.sh: --ns and --from are required" >&2; exit 2; }
[ "$backup" = 1 ] || [ -n "$to" ] || { echo "work-copy.sh: --to (or --backup) is required" >&2; exit 2; }
[ "$backup" = 0 ] || [ -z "$to" ] || { echo "work-copy.sh: --backup makes its own --to" >&2; exit 2; }
if [ "$ns" = k2-kind-dist ] && [ "$owner" = 0 ]; then
    echo "work-copy.sh: k2-kind-dist is the owner's: only with --owner, when the coordinator says so" >&2; exit 2
fi
case "$ns" in k2-kind|k2-kind-dist-fake) echo "work-copy.sh: refusing namespace $ns" >&2; exit 2 ;; esac
KIND_STATE="${TMPDIR:-/tmp}/k2-kind/work-copy"
KIND_KUBECONFIG="$KIND_STATE/kubeconfig"
mkdir -p "$KIND_STATE" && chmod 700 "$KIND_STATE"
kind_kubeconfig
kn() { kc -n "$ns" "$@"; }

# A claim's size, the node its volume is on, and the running pods that mount it.
claim_size() { kn get pvc "$1" -o jsonpath='{.spec.resources.requests.storage}'; }
claim_node() {
    local pv
    pv=$(kn get pvc "$1" -o jsonpath='{.spec.volumeName}')
    [ -n "$pv" ] || { echo "(not bound yet)"; return; }
    kc get pv "$pv" -o jsonpath='{.spec.nodeAffinity.required.nodeSelectorTerms[0].matchExpressions[0].values[0]}'
}
mounted_by() {
    kn get pods -o json | python3 -I -c '
import json, sys
claim = sys.argv[1]
for p in json.load(sys.stdin)["items"]:
    if p["status"].get("phase") not in ("Running", "Pending"):
        continue
    if any((v.get("persistentVolumeClaim") or {}).get("claimName") == claim for v in p["spec"].get("volumes", [])):
        print(p["metadata"]["name"])' "$1"
}

kn get pvc "$from" >/dev/null || { echo "work-copy.sh: no claim $from in $ns" >&2; exit 1; }
if [ "$backup" = 1 ]; then
    to="k2-backup-work-$(date -u +%Y%m%d-%H%M%S)"
fi
image=$(kn get statefulset k2-controller -o jsonpath='{.spec.template.spec.containers[0].image}')
from_node=$(claim_node "$from")
say "from $from ($(claim_size "$from"), on $from_node; mounted by: $(mounted_by "$from" | tr '\n' ' '))"
if [ "$backup" = 1 ]; then
    say "to $to: a new claim of $(claim_size "$from")"
    to_node=$from_node
else
    kn get pvc "$to" >/dev/null || { echo "work-copy.sh: no claim $to in $ns" >&2; exit 1; }
    to_node=$(claim_node "$to")
    say "to $to ($(claim_size "$to"), on $to_node; mounted by: $(mounted_by "$to" | tr '\n' ' '))"
fi
say "copy pod: image $image, $( [ "$mirror" = 1 ] && echo "TO emptied first, then " )tar FROM → TO, then compared file by file"

problems=()
if [ "$backup" = 0 ] && [ "$from_node" != "$to_node" ] && [ "$to_node" != "(not bound yet)" ]; then
    problems+=("the volumes are on different nodes ($from_node, $to_node)")
fi
if [ "$backup" = 0 ]; then
    for p in $(mounted_by "$to"); do
        case ",$allow," in *",$p,"*) ;; *) problems+=("$p mounts $to (stop it first, or --allow-mounted=$p)") ;; esac
    done
fi
if [ "${#problems[@]}" -gt 0 ]; then
    for p in "${problems[@]}"; do say "would refuse: $p"; done
    [ "$go" = 1 ] && { echo "work-copy.sh: refused" >&2; exit 1; }
fi
if [ "$go" = 0 ]; then
    say "dry run: nothing changed (--go to copy)"
    exit 0
fi

if [ "$backup" = 1 ]; then
    kn apply -f - >&2 <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: $to
  labels: {k2.dev/backup-of: $from}
spec:
  accessModes: [ReadWriteOnce]
  resources: {requests: {storage: $(claim_size "$from")}}
EOF
fi
pod="k2-work-copy-$(date -u +%H%M%S)"
script='set -eu
if [ "$MIRROR" = 1 ]; then find /to -mindepth 1 -maxdepth 1 ! -name lost+found -exec rm -rf {} +; fi
# The top entries, not "." (the volume root is not ours to retime or chmod).
(cd /from && find . -mindepth 1 -maxdepth 1 ! -name lost+found -print0 | tar cf - --null -T -) | (cd /to && tar xpf -)
sums() { cd "$1" && find . -path ./lost+found -prune -o \( -type f -o -type l \) -print | LC_ALL=C sort | while IFS= read -r f; do
  if [ -L "$f" ]; then printf "link %s -> %s\n" "$f" "$(readlink "$f")"; else printf "file %s %s\n" "$f" "$(sha256sum < "$f" | cut -d" " -f1)"; fi; done; }
sums /from > /tmp/from.sums; sums /to > /tmp/to.sums
if cmp -s /tmp/from.sums /tmp/to.sums; then echo "COPY-OK $(wc -l < /tmp/from.sums) entries"; else echo COPY-DIFFERS; diff /tmp/from.sums /tmp/to.sums | head -20; exit 1; fi'
kn apply -f - >&2 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $pod
  labels: {k2.dev/work-copy: "1"}
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 10000
    runAsGroup: 0
    fsGroup: 10000
    seccompProfile: {type: RuntimeDefault}
  containers:
    - name: copy
      image: $image
      imagePullPolicy: Never
      securityContext:
        allowPrivilegeEscalation: false
        capabilities: {drop: [ALL]}
      command: [sh, -c, $(python3 -I -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$script")]
      env: [{name: MIRROR, value: "$mirror"}]
      volumeMounts:
        - {name: from, mountPath: /from, readOnly: true}
        - {name: to, mountPath: /to}
  volumes:
    - {name: from, persistentVolumeClaim: {claimName: $from, readOnly: true}}
    - {name: to, persistentVolumeClaim: {claimName: $to}}
EOF
trap 'kn delete pod "$pod" --ignore-not-found --wait=false >/dev/null 2>&1' EXIT
phase=""
for _ in $(seq 1 360); do
    phase=$(kn get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    case "$phase" in Succeeded|Failed) break ;; esac
    sleep 5
done
kn logs "$pod" -c copy 2>/dev/null | tail -25
[ "$phase" = Succeeded ] || { echo "work-copy.sh: the copy pod ended $phase" >&2; exit 1; }
say "copied $from → $to"
[ "$backup" = 1 ] && echo "$to"
exit 0
