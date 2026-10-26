#!/usr/bin/env bash
# T3 acceptance item 3 (mail): Stalwart as declared in
# deploy/kubernetes/core/mail.yaml (kind-sized: a 1 GiB volume, no
# LoadBalancer, since kind has none: D7) answers on its management API
# inside the cluster.
#
#   scripts/kind/caps/mail-check.sh [--keep]
#
# In namespace k2-p4-caps: StatefulSet k2-mail and Service k2-mail (:8080),
# image stalwartlabs/stalwart:v0.16.25 (pulled by the kind node). From a
# probe pod, as the controller would reach it:
#   1. GET http://k2-mail:8080/healthz/ready → 200;
#   2. Stalwart is configured, not in bootstrap mode. Stalwart 0.16 with no
#      config file (mail.yaml as declared) starts in bootstrap mode: a
#      temporary admin in its log, a "recovery" listener on 8080, and an
#      unauthenticated /jmap/session (200). Checked from its log, which is
#      grepped for the marker only and never printed, and by
#      GET /jmap/session without credentials, which must be 401.
# No admin password is read or printed. --keep leaves k2-mail running.
set -euo pipefail
# shellcheck source=scripts/kind/caps/lib.sh
source "$(dirname "$0")/lib.sh"
keep=0
for a in "$@"; do
    case "$a" in
        --keep) keep=1 ;;
        -h|--help) sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "mail-check.sh: unknown argument $a" >&2; exit 2 ;;
    esac
done
caps_kubeconfig
caps_namespace
fails=0
ok() { printf 'ok   %s\n' "$*"; }
bad() { printf 'FAIL %s\n' "$*"; fails=$((fails + 1)); }
cleanup() {
    kn delete pod mail-probe --ignore-not-found --wait=false >/dev/null 2>&1 || true
    if [ "$keep" = 0 ]; then
        kn delete statefulset k2-mail --ignore-not-found --wait=true >/dev/null 2>&1 || true
        kn delete service k2-mail --ignore-not-found >/dev/null 2>&1 || true
        kn delete pvc data-k2-mail-0 --ignore-not-found --wait=false >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

say "k2-mail (stalwartlabs/stalwart:v0.16.25) in $CAPS_NS"
kn apply -f - >&2 <<'EOF'
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: k2-mail
  labels: {k2.dev/test: k8s-capabilities}
spec:
  serviceName: k2-mail
  replicas: 1
  selector:
    matchLabels: {app.kubernetes.io/name: k2-mail}
  template:
    metadata:
      labels: {app.kubernetes.io/name: k2-mail, k2.dev/role: mail}
    spec:
      securityContext:
        seccompProfile: {type: RuntimeDefault}
      containers:
        - name: stalwart
          image: stalwartlabs/stalwart:v0.16.25
          ports:
            - {name: smtp, containerPort: 25}
            - {name: submissions, containerPort: 465}
            - {name: imaps, containerPort: 993}
            - {name: mgmt, containerPort: 8080}
          resources:
            requests: {cpu: 100m, memory: 256Mi}
            limits: {memory: 1Gi}
          securityContext:
            allowPrivilegeEscalation: false
          volumeMounts:
            - {name: data, mountPath: /opt/stalwart}
  volumeClaimTemplates:
    - metadata: {name: data}
      spec:
        accessModes: [ReadWriteOnce]
        resources: {requests: {storage: 1Gi}}
---
apiVersion: v1
kind: Service
metadata:
  name: k2-mail
  labels: {k2.dev/test: k8s-capabilities}
spec:
  selector: {app.kubernetes.io/name: k2-mail}
  ports:
    - {name: mgmt, port: 8080, targetPort: mgmt}
EOF
if kn rollout status statefulset/k2-mail --timeout=600s >&2; then
    ok "k2-mail ready on $(kn get pod k2-mail-0 -o jsonpath='{.spec.nodeName}') ($(kn get pod k2-mail-0 -o jsonpath='{.status.containerStatuses[0].imageID}' | sed 's/.*@//' | cut -c1-19))"
else
    bad "k2-mail not ready: $(kn get pod k2-mail-0 -o jsonpath='{.status.containerStatuses[0].state}' 2>/dev/null | cut -c1-200)"
fi

kn apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: mail-probe
  labels: {k2.dev/test: k8s-capabilities}
spec:
  terminationGracePeriodSeconds: 1
  securityContext: {runAsNonRoot: true, runAsUser: 10000, runAsGroup: 0, seccompProfile: {type: RuntimeDefault}}
  containers:
    - name: c
      image: $CAPS_PROBE_IMAGE
      imagePullPolicy: Never
      command: [sleep, "600"]
      resources: {requests: {cpu: 10m, memory: 16Mi}, limits: {memory: 64Mi}}
      securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
EOF
kn wait --for=condition=Ready pod/mail-probe --timeout=120s >/dev/null
code() { kn exec mail-probe -- curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://k2-mail:8080$1" 2>/dev/null || echo 000; }
c=000
for _ in $(seq 1 60); do c=$(code /healthz/ready); [ "$c" = 200 ] && break; sleep 2; done
[ "$c" = 200 ] && ok "GET k2-mail:8080/healthz/ready -> 200" || bad "GET /healthz/ready -> $c"
if kn logs k2-mail-0 2>/dev/null | grep -q "server.bootstrap-mode"; then
    bad "Stalwart is in bootstrap mode (no configuration): a temporary admin is in its log and 8080 is a recovery listener"
else
    ok "Stalwart is configured (not in bootstrap mode)"
fi
c=$(code /jmap/session)
[ "$c" = 401 ] && ok "GET k2-mail:8080/jmap/session without credentials -> 401" || bad "GET /jmap/session without credentials -> $c (expected 401)"

echo
if [ "$fails" = 0 ]; then echo "PASS mail: Stalwart answers on its management API inside the cluster"; else echo "FAIL mail: $fails check(s)"; exit 1; fi
