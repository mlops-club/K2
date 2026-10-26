#!/usr/bin/env bash
# T3 decision 4, the security fix: Stalwart from deploy/kubernetes/core/
# mail.yaml is never open and never logs a credential.
#
#   scripts/kind/caps/mail-check.sh [--keep]
#
# In namespace k2-p4-caps: the Secret k2-mail-recovery (64 hex, generated
# here from stdin when missing, never printed), mail.yaml as it is in git
# (StatefulSet, Services, the controller's mail Role), and the mail-ingress
# NetworkPolicy from network-policies.yaml. Then:
#   1. no temporary admin in Stalwart's log (grepped for the banner; the log
#      is never printed);
#   2. from a pod labelled as the controller: without credentials, a
#      management call (x:Bootstrap/get) is 401;
#   3. the recovery credential (from the Secret, through secretKeyRef)
#      authenticates; a wrong password is 401;
#   4. from a pod that is not the controller, port 8080 is unreachable;
#   5. /etc/stalwart and /var/lib/stalwart are writable by Stalwart (uid 2000).
# Leaving bootstrap mode is the controller's job (its mail supervisor runs
# x:Bootstrap/set, then recovery-off); here bootstrap mode is reported as
# "setup pending". --keep leaves Stalwart running.
set -euo pipefail
# shellcheck source=scripts/kind/caps/lib.sh
source "$(dirname "$0")/lib.sh"
keep=0
for a in "$@"; do
    case "$a" in
        --keep) keep=1 ;;
        -h|--help) sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "mail-check.sh: unknown argument $a" >&2; exit 2 ;;
    esac
done
caps_kubeconfig
caps_namespace
fails=0
ok() { printf 'ok   %s\n' "$*"; }
bad() { printf 'FAIL %s\n' "$*"; fails=$((fails + 1)); }
note() { printf 'note %s\n' "$*"; }
cleanup() {
    kn delete pod mail-as-controller mail-as-other --ignore-not-found --wait=false >/dev/null 2>&1 || true
    if [ "$keep" = 0 ]; then
        kn delete -f "$CAPS_REPO/deploy/kubernetes/core/mail.yaml" --ignore-not-found --wait=true >/dev/null 2>&1 || true
        kn delete networkpolicy mail-ingress --ignore-not-found >/dev/null 2>&1 || true
        kn delete pvc data-k2-mail-0 --ignore-not-found --wait=false >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

say "Secret k2-mail-recovery, mail.yaml and mail-ingress in $CAPS_NS"
if ! kn get secret k2-mail-recovery >/dev/null 2>&1; then
    python3 -I -c 'import base64,json,secrets
print(json.dumps({"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
    "metadata": {"name": "k2-mail-recovery", "labels": {"k2.dev/test": "k8s-capabilities"}},
    "data": {"password": base64.b64encode(secrets.token_hex(32).encode()).decode()}}))' | kn create -f - >/dev/null
fi
kn apply -f "$CAPS_REPO/deploy/kubernetes/core/mail.yaml" >&2
python3 -I - "$CAPS_REPO/deploy/kubernetes/core/network-policies.yaml" <<'PY' | kn apply -f - >&2
import re, sys
for doc in re.split(r"(?m)^---\s*$", open(sys.argv[1]).read()):
    if re.search(r"(?m)^  name: mail-ingress\s*$", doc):
        print(doc)
PY
if kn rollout status statefulset/k2-mail --timeout=600s >&2; then
    ok "k2-mail ready on $(kn get pod k2-mail-0 -o jsonpath='{.spec.nodeName}') (uid $(kn exec k2-mail-0 -- id -u))"
else
    bad "k2-mail not ready: $(kn get pod k2-mail-0 -o jsonpath='{.status.containerStatuses[0].state}' 2>/dev/null | cut -c1-200)"
fi

echo "== 1. no temporary admin in the log"
banner=$(kn logs k2-mail-0 2>/dev/null | grep -c "Use these credentials" || true)
[ "$banner" = 0 ] && ok "no temporary-admin banner in Stalwart's log (the recovery credential comes from the Secret)" \
    || bad "Stalwart logged a temporary admin ($banner banner(s))"

probe() {  # probe <name> <role label>
    kn apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $1
  labels: {k2.dev/test: k8s-capabilities, k2.dev/role: $2}
spec:
  terminationGracePeriodSeconds: 1
  securityContext: {runAsNonRoot: true, runAsUser: 10000, runAsGroup: 0, seccompProfile: {type: RuntimeDefault}}
  containers:
    - name: c
      image: $CAPS_PROBE_IMAGE
      imagePullPolicy: Never
      command: [sleep, "600"]
      env:
        - name: PW
          valueFrom: {secretKeyRef: {name: k2-mail-recovery, key: password}}
      resources: {requests: {cpu: 10m, memory: 16Mi}, limits: {memory: 64Mi}}
      securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
EOF
}
probe mail-as-controller controller
probe mail-as-other app
kn wait --for=condition=Ready pod/mail-as-controller pod/mail-as-other --timeout=120s >/dev/null
api() {  # api <pod> <curl auth args...> → HTTP code of an x:Bootstrap/get call
    # Retries while there is no connection at all (a just-started pod).
    local pod=$1 code="" i; shift
    for i in 1 2 3 4 5; do
        code=$(kn exec "$pod" -- sh -c 'curl -s -o /dev/null -w "%{http_code}" --max-time 5 "$@" -H "Content-Type: application/json" \
            -d "{\"using\":[\"urn:ietf:params:jmap:core\"],\"methodCalls\":[[\"x:Bootstrap/get\",{},\"0\"]]}" http://k2-mail:8080/jmap/' _ "$@" 2>/dev/null || true)
        [ "${code:-000}" != 000 ] && break
        sleep 2
    done
    echo "${code:-000}"
}

echo "== 2. 3. the management API from the controller"
c=$(api mail-as-controller)
[ "$c" = 401 ] && ok "no credentials: x:Bootstrap/get -> 401" || bad "no credentials: x:Bootstrap/get -> $c (expected 401)"
c=$(kn exec mail-as-controller -- sh -c 'curl -s -o /dev/null -w "%{http_code}" --max-time 5 -u "admin:$PW" http://k2-mail:8080/jmap/session' 2>/dev/null || echo 000)
[ "$c" = 200 ] && ok "the recovery credential from the Secret authenticates (/jmap/session 200)" || bad "recovery credential: /jmap/session -> $c"
c=$(api mail-as-controller -u admin:wrong-password)
[ "$c" = 401 ] && ok "a wrong password -> 401" || bad "a wrong password -> $c (expected 401)"

echo "== 4. port 8080 from a pod that is not the controller"
c=$(kn exec mail-as-other -- curl -s -o /dev/null -w '%{http_code}' --max-time 4 http://k2-mail:8080/healthz/ready 2>/dev/null || true)
[ "${c:-000}" = 000 ] && ok "unreachable from a non-controller pod (mail-ingress)" || bad "a non-controller pod reached 8080 (HTTP $c)"

echo "== 5. the volume"
kn exec k2-mail-0 -- sh -c 'touch /etc/stalwart/.k2-probe /var/lib/stalwart/.k2-probe && rm /etc/stalwart/.k2-probe /var/lib/stalwart/.k2-probe' \
    && ok "/etc/stalwart and /var/lib/stalwart writable by Stalwart" || bad "the volume is not writable by Stalwart"
if kn logs k2-mail-0 2>/dev/null | grep -q "server.bootstrap-mode"; then
    note "Stalwart is in bootstrap mode: setup pending (the controller's mail supervisor completes it), credentials required meanwhile"
else
    ok "Stalwart is configured (normal mode)"
fi

echo
if [ "$fails" = 0 ]; then echo "PASS mail: no temporary admin, no unauthenticated management, 8080 for the controller only"; else echo "FAIL mail: $fails check(s)"; exit 1; fi
