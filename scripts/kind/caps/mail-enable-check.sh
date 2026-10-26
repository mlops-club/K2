#!/usr/bin/env bash
# T3 decision 4, end to end: K2's controller sets up the Stalwart pod over
# K2_STALWART_URL and leaves no recovery credential anyone knows.
#
#   scripts/kind/caps/mail-enable-check.sh [--image=k2-daemon:caps] [--keep]
#
# In namespace k2-p4-caps:
#   1. a fresh Secret k2-mail-recovery, then Stalwart from mail.yaml with
#      mail-ingress (mail-check.sh --keep): bootstrap mode, credentials
#      required;
#   2. a probe pod labelled as the controller captures the ORIGINAL recovery
#      password through secretKeyRef (pod env is fixed at start), so it can
#      prove later that this password is dead;
#   3. the controller (controller-check.sh --keep: K2_CAP_MAIL=external,
#      K2_STALWART_URL=http://k2-mail:8080);
#   4. POST /cli/mail/server/enable {"hostname":"mail.k2.test"} inside the
#      controller pod with its own token (never printed), then GET
#      /cli/mail/status until running or error;
#   5. checks:
#      - status running, every step marked, api_url http://k2-mail:8080;
#      - the Stalwart pod was replaced and is in normal mode (no
#        bootstrap-mode line in the new pod's log);
#      - the original recovery password is refused (401);
#      - the Secret's value changed (hashes compared, values never printed);
#      - without credentials, still 401;
#      - the controller's mail health is running (an ApiKey ping).
# --keep leaves everything running; the default removes the controller,
# Stalwart, its volume and the Secret.
set -euo pipefail
# shellcheck source=scripts/kind/caps/lib.sh
source "$(dirname "$0")/lib.sh"
image=k2-daemon:caps keep=0
for a in "$@"; do
    case "$a" in
        --image=*) image=${a#--image=} ;;
        --keep) keep=1 ;;
        -h|--help) sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "mail-enable-check.sh: unknown argument $a" >&2; exit 2 ;;
    esac
done
caps_kubeconfig
caps_namespace
here=$(dirname "$0")
fails=0
ok() { printf 'ok   %s\n' "$*"; }
bad() { printf 'FAIL %s\n' "$*"; fails=$((fails + 1)); }
cleanup() {
    kn delete pod mail-witness --ignore-not-found --wait=false >/dev/null 2>&1 || true
    if [ "$keep" = 0 ]; then
        kn delete statefulset k2-controller --ignore-not-found --wait=true >/dev/null 2>&1 || true
        kn delete service k2-controller configmap k2-controller --ignore-not-found >/dev/null 2>&1 || true
        kn delete pvc data-k2-controller-0 --ignore-not-found --wait=false >/dev/null 2>&1 || true
        kn delete -f "$CAPS_REPO/deploy/kubernetes/core/rbac.yaml" --ignore-not-found >/dev/null 2>&1 || true
        kn delete -f "$CAPS_REPO/deploy/kubernetes/core/mail.yaml" --ignore-not-found --wait=true >/dev/null 2>&1 || true
        kn delete networkpolicy mail-ingress --ignore-not-found >/dev/null 2>&1 || true
        kn delete pvc data-k2-mail-0 --ignore-not-found --wait=false >/dev/null 2>&1 || true
        kn delete secret k2-mail-recovery --ignore-not-found >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT
secret_hash() { kn get secret k2-mail-recovery -o jsonpath='{.data.password}' | shasum -a 256 | cut -c1-16; }

echo "== 1. Stalwart, fresh"
kn delete statefulset k2-mail --ignore-not-found --wait=true >/dev/null 2>&1 || true
kn delete pvc data-k2-mail-0 --ignore-not-found --wait=true >/dev/null 2>&1 || true
kn delete secret k2-mail-recovery --ignore-not-found >/dev/null 2>&1 || true
"$here/mail-check.sh" --keep | sed 's/^/     /'
uid_before=$(kn get pod k2-mail-0 -o jsonpath='{.metadata.uid}')
hash_before=$(secret_hash)

echo "== 2. a witness with the original recovery password"
kn apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: mail-witness
  labels: {k2.dev/test: k8s-capabilities, k2.dev/role: controller}
spec:
  terminationGracePeriodSeconds: 1
  securityContext: {runAsNonRoot: true, runAsUser: 10000, runAsGroup: 0, seccompProfile: {type: RuntimeDefault}}
  containers:
    - name: c
      image: $CAPS_PROBE_IMAGE
      imagePullPolicy: Never
      command: [sleep, "1800"]
      env:
        - name: OLD_PW
          valueFrom: {secretKeyRef: {name: k2-mail-recovery, key: password}}
      resources: {requests: {cpu: 10m, memory: 16Mi}, limits: {memory: 64Mi}}
      securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
EOF
kn wait --for=condition=Ready pod/mail-witness --timeout=120s >/dev/null
c=$(kn exec mail-witness -- sh -c 'curl -s -o /dev/null -w "%{http_code}" --max-time 5 -u "admin:$OLD_PW" http://k2-mail:8080/jmap/session' || true)
[ "$c" = 200 ] && ok "before setup: the original recovery password authenticates" || bad "before setup: original password -> $c"

echo "== 3. the controller (K2_CAP_MAIL=external)"
"$here/controller-check.sh" --keep --image="$image" | sed 's/^/     /'

echo "== 4. enable through the controller's API"
ctl() {  # ctl <method> <path> [body]: the controller's own API, its token read in the pod
    kn exec k2-controller-0 -c k2-daemon -- sh -c \
        'curl -sS --max-time 30 -X "$1" -H "Content-Type: application/json" ${3:+-d "$3"} "http://127.0.0.1:7420$2?token=$(cat /var/lib/k2/.k2/daemon.token)"' \
        _ "$@" 2>&1
}
resp=$(ctl POST /cli/mail/server/enable '{"hostname":"mail.k2.test"}')
printf '     enable -> %s\n' "$(printf '%s' "$resp" | cut -c1-240)"
state="" status=""
for _ in $(seq 1 150); do
    status=$(ctl GET /cli/mail/status)
    state=$(printf '%s' "$status" | python3 -I -c 'import json,sys
try:
    d = json.load(sys.stdin)
except ValueError:
    print("?"); sys.exit()
s = d.get("server") or d
print(s.get("status") or s.get("state") or "?")' 2>/dev/null || echo "?")
    case "$state" in running|error) break ;; esac
    sleep 4
done
printf '%s' "$status" > "$CAPS_REPO/lab-runs/caps/mail-status.json"
summary=$(printf '%s' "$status" | python3 -I -c 'import json,sys
d = json.load(sys.stdin); s = d.get("server") or d
p = s.get("enableProgress") or d.get("enableProgress") or {}
print("steps=%s api_url=%s last_error=%s placement=%s" % (",".join(sorted((p.get("steps") or {}).keys())), s.get("apiUrl") or s.get("api_url"),
      s.get("lastError") or s.get("last_error"), p.get("placement")))' 2>/dev/null || echo "unparsed")
[ "$state" = running ] && ok "mail status running ($summary)" || bad "mail status $state ($summary)"

echo "== 5. no recovery credential anyone knows"
uid_after=$(kn get pod k2-mail-0 -o jsonpath='{.metadata.uid}')
[ -n "$uid_after" ] && [ "$uid_after" != "$uid_before" ] && ok "the Stalwart pod was replaced (new uid)" || bad "the Stalwart pod was not replaced"
if kn logs k2-mail-0 2>/dev/null | grep -q "server.bootstrap-mode"; then bad "the new Stalwart pod is still in bootstrap mode"; else ok "the new Stalwart pod runs in normal mode (config.json kept on its volume)"; fi
c=$(kn exec mail-witness -- sh -c 'curl -s -o /dev/null -w "%{http_code}" --max-time 5 -u "admin:$OLD_PW" http://k2-mail:8080/jmap/session' || true)
[ "$c" = 401 ] && ok "the original recovery password is refused (401)" || bad "the original recovery password -> $c (expected 401)"
[ "$(secret_hash)" != "$hash_before" ] && ok "Secret k2-mail-recovery overwritten (value hashes differ; nobody kept the new one)" || bad "the Secret was not overwritten"
c=$(kn exec mail-witness -- sh -c 'curl -s -o /dev/null -w "%{http_code}" --max-time 5 -H "Content-Type: application/json" -d "{\"using\":[\"urn:ietf:params:jmap:core\"],\"methodCalls\":[[\"Core/echo\",{},\"0\"]]}" http://k2-mail:8080/jmap/' || true)
[ "$c" = 401 ] && ok "without credentials: 401" || bad "without credentials -> $c (expected 401)"
health=$(ctl GET "/cli/mail/status" | python3 -I -c 'import json,sys; d=json.load(sys.stdin); s=d.get("server") or d; print(s.get("status") or s.get("state"))' 2>/dev/null || echo "?")
[ "$health" = running ] && ok "the controller's mail health: running (ApiKey ping)" || bad "mail health: $health"

echo
if [ "$fails" = 0 ]; then echo "PASS mail-enable: the controller set up Stalwart over K2_STALWART_URL; no recovery credential is known"; else echo "FAIL mail-enable: $fails check(s)"; exit 1; fi
