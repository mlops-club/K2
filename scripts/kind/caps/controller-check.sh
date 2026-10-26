#!/usr/bin/env bash
# T3 acceptance item 5: a K2 controller on Kubernetes reports each capability
# as delegated, and its drivers reach what they are delegated to.
#
#   scripts/kind/caps/controller-check.sh [--image=k2-daemon:caps] [--keep]
#
# In namespace k2-p4-caps, shaped like deploy/kubernetes/core/controller.yaml:
#   - the RBAC of deploy/kubernetes/core/rbac.yaml (ServiceAccount
#     k2-controller with its namespace Role);
#   - a host bootstrap token in Secret k2-secrets (made from stdin, never
#     printed);
#   - StatefulSet k2-controller (K2_ROLE=controller) with the end state's
#     capability settings: Postgres through postgresql/k2-pg, Apps on
#     Kubernetes, mail at http://k2-mail:8080, Sew at its k2-edge URL, the
#     front door listening;
#   - backends: postgresql k2-pg (pg-smoke.sh --keep) and k2-mail
#     (mail-check.sh --keep), started here when missing.
# Then GET /boot-status (the full view: from inside the controller pod with
# its own owner token, read from its file there, never printed; callers
# without a token get the reduced "compat" view, which has no capabilities):
#   - capabilities.{postgres,mail,sew,apps,front}.state == delegated;
#   - drivers ok for postgres, apps, mail and front;
#   - sew's driver is not ok: there is no Sew image (noted, not a failure).
# The controller and its RBAC are deleted at the end (--keep keeps them);
# k2-pg and k2-mail are left as they were.
set -euo pipefail
# shellcheck source=scripts/kind/caps/lib.sh
source "$(dirname "$0")/lib.sh"
image=k2-daemon:caps keep=0
for a in "$@"; do
    case "$a" in
        --image=*) image=${a#--image=} ;;
        --keep) keep=1 ;;
        -h|--help) sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "controller-check.sh: unknown argument $a" >&2; exit 2 ;;
    esac
done
caps_kubeconfig
caps_namespace
fails=0
ok() { printf 'ok   %s\n' "$*"; }
bad() { printf 'FAIL %s\n' "$*"; fails=$((fails + 1)); }
note() { printf 'note %s\n' "$*"; }
cleanup() {
    kn delete pod ctl-probe --ignore-not-found --wait=false >/dev/null 2>&1 || true
    if [ "$keep" = 0 ]; then
        kn delete statefulset k2-controller --ignore-not-found --wait=true >/dev/null 2>&1 || true
        kn delete service k2-controller configmap k2-controller --ignore-not-found >/dev/null 2>&1 || true
        kn delete pvc data-k2-controller-0 --ignore-not-found --wait=false >/dev/null 2>&1 || true
        kn delete -f "$CAPS_REPO/deploy/kubernetes/core/rbac.yaml" --ignore-not-found >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

echo "== backends"
kn get postgresql k2-pg >/dev/null 2>&1 || "$(dirname "$0")/pg-smoke.sh" --keep >/dev/null 2>&1 || true
[ "$(kn get postgresql k2-pg -o jsonpath='{.status.PostgresClusterStatus}' 2>/dev/null)" = Running ] \
    && ok "postgresql k2-pg Running" || bad "postgresql k2-pg not Running"
kn get statefulset k2-mail >/dev/null 2>&1 || "$(dirname "$0")/mail-check.sh" --keep >/dev/null 2>&1 || true
kn rollout status statefulset/k2-mail --timeout=300s >/dev/null 2>&1 && ok "k2-mail ready" || bad "k2-mail not ready"

echo "== the controller ($image)"
kn apply -f "$CAPS_REPO/deploy/kubernetes/core/rbac.yaml" >&2
if ! kn get secret k2-secrets >/dev/null 2>&1; then
    python3 -I -c 'import base64,json,secrets,sys
print(json.dumps({"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
    "metadata": {"name": "k2-secrets", "labels": {"k2.dev/test": "k8s-capabilities"}},
    "data": {"host-enroll-token": base64.b64encode(secrets.token_urlsafe(32).encode()).decode()}}))' \
        | kn create -f - >&2
fi
kn apply -f - >&2 <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: k2-controller
  labels: {k2.dev/test: k8s-capabilities}
data:
  HOME: /var/lib/k2
  K2_HOME: /var/lib/k2/.k2
  K2_ROLE: controller
  K2_LISTEN: lan
  K2_DAEMON_PORT: "7420"
  K2_HOSTS: "1"
  K2_HOST_BOOTSTRAP_TOKEN_FILE: /run/secrets/k2/host-enroll-token
  K2_CAP_POSTGRES: kubernetes
  K2_POSTGRES_CLUSTER: k2-pg
  K2_POSTGRES_HOST: k2-pg
  K2_POSTGRES_DB: k2
  K2_POSTGRES_USER_FILE: /run/secrets/pg/username
  K2_CAP_MAIL: external
  K2_STALWART_URL: http://k2-mail:8080
  K2_STALWART_ALLOWED_IPS: 10.244.0.0/16        # kind's pod CIDR: where the controller calls from
  K2_CAP_SEW: external
  K2_SEW_MGMT_URL: http://k2-sew.k2-edge:9090
  K2_CAP_APPS: kubernetes
  K2_CAP_FRONT: listen
  K2_TUNNEL_LEASE: "off"
  K2_RELAY_LIST_URL: http://127.0.0.1:9/disabled
  K2_DNS_API_BASE: http://127.0.0.1:9
---
apiVersion: v1
kind: Service
metadata:
  name: k2-controller
  labels: {k2.dev/test: k8s-capabilities}
spec:
  selector: {app.kubernetes.io/name: k2-controller}
  ports: [{name: api, port: 7420, targetPort: 7420}]
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: k2-controller
  labels: {k2.dev/test: k8s-capabilities}
spec:
  serviceName: k2-controller
  replicas: 1
  selector:
    matchLabels: {app.kubernetes.io/name: k2-controller}
  template:
    metadata:
      labels: {app.kubernetes.io/name: k2-controller, k2.dev/role: controller}
    spec:
      serviceAccountName: k2-controller
      terminationGracePeriodSeconds: 10
      securityContext:
        runAsNonRoot: true
        runAsUser: 10000
        runAsGroup: 0
        fsGroup: 10000
        seccompProfile: {type: RuntimeDefault}
      containers:
        - name: k2-daemon
          image: $image
          imagePullPolicy: Never
          envFrom: [{configMapRef: {name: k2-controller}}]
          ports: [{name: api, containerPort: 7420}]
          startupProbe:
            httpGet: {path: /boot-status, port: api}
            periodSeconds: 5
            timeoutSeconds: 5
            failureThreshold: 60
          resources:
            requests: {cpu: 100m, memory: 256Mi}
            limits: {memory: 1Gi}
          securityContext:
            allowPrivilegeEscalation: false
            capabilities: {drop: [ALL]}
          volumeMounts:
            - {name: data, mountPath: /var/lib/k2}
            - {name: k2-secrets, mountPath: /run/secrets/k2, readOnly: true}
            - {name: pg, mountPath: /run/secrets/pg, readOnly: true}
      volumes:
        - name: k2-secrets
          secret: {secretName: k2-secrets}
        - name: pg
          secret: {secretName: k2.k2-pg.credentials.postgresql.acid.zalan.do}
  volumeClaimTemplates:
    - metadata: {name: data}
      spec:
        accessModes: [ReadWriteOnce]
        resources: {requests: {storage: 1Gi}}
EOF
if kn rollout status statefulset/k2-controller --timeout=600s >&2; then
    ok "k2-controller ready ($(kn get pod k2-controller-0 -o jsonpath='{.spec.nodeName}'))"
else
    bad "k2-controller not ready"; kn logs k2-controller-0 --tail=20 2>&1 | grep -viE "token|password|secret" | tail -10
fi

echo "== /boot-status"
kn apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ctl-probe
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
kn wait --for=condition=Ready pod/ctl-probe --timeout=120s >/dev/null
# The drivers answer within one check of boot; wait until every one has.
caps=""
for _ in $(seq 1 30); do
    caps=$(kn exec k2-controller-0 -c k2-daemon -- sh -c \
            'curl -fsS --max-time 5 "http://127.0.0.1:7420/boot-status?token=$(cat /var/lib/k2/.k2/daemon.token)"' 2>/dev/null \
        | python3 -I -c 'import json,sys; print(json.dumps(json.load(sys.stdin).get("capabilities", {})))' 2>/dev/null || true)
    python3 -I -c 'import json,sys; c=json.loads(sys.argv[1] or "{}"); sys.exit(0 if c and all("driver" in c.get(f, {}) for f in ("postgres","mail","sew","apps","front")) else 1)' "$caps" && break
    sleep 2
done
printf '%s\n' "$caps" > "$CAPS_REPO/lab-runs/caps/boot-status-capabilities.json" 2>/dev/null || true
eval "$(python3 -I - "$caps" <<'PY'
import json, shlex, sys
c = json.loads(sys.argv[1] or "{}")
for f in ("postgres", "mail", "sew", "apps", "front", "vault"):
    e = c.get(f, {}); d = e.get("driver") or {}
    print("S_%s=%s M_%s=%s D_%s=%s T_%s=%s" % (f, shlex.quote(e.get("state", "missing")), f, shlex.quote(e.get("mode", "")),
          f, shlex.quote({True: "ok", False: "notok"}.get(d.get("ok"), "none")), f, shlex.quote(d.get("detail", ""))))
PY
)"
for f in postgres mail sew apps front; do
    s=S_$f m=M_$f
    [ "${!s}" = delegated ] && ok "$f: delegated (${!m})" || bad "$f: state ${!s} (expected delegated)"
done
for f in postgres apps mail front; do
    d=D_$f t=T_$f
    [ "${!d}" = ok ] && ok "$f driver: ${!t}" || bad "$f driver: ${!d} (${!t})"
done
[ "$D_sew" = notok ] && note "sew driver not ok, as expected with no Sew image: $T_sew" \
    || bad "sew driver: $D_sew ($T_sew); expected not ok (no Sew in this cluster)"

echo
if [ "$fails" = 0 ]; then echo "PASS controller: every capability delegated; drivers reach Postgres, the App RBAC, mail and the front door"; else echo "FAIL controller: $fails check(s)"; exit 1; fi
