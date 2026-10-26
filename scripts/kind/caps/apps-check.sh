#!/usr/bin/env bash
# T3 acceptance item 2, S12 parity on kind: an App on Kubernetes
# (K2_CAP_APPS=kubernetes, decisions 2 and 3) next to its workspace's
# database through the operator (decision 1).
#
#   scripts/kind/caps/apps-check.sh [--image=k2-daemon:caps] [--keep]
#
# Namespace k2-p4-apps (made here, deleted at the end unless --keep), with
# namespace k2's Pod Security:
#   1. postgresql k2-pg; a controller like controller.yaml with Apps and
#      Postgres delegated (rbac.yaml); a neighbour pod (not the controller);
#   2. a workspace, its database (POST /cli/db/create, through k2-pg), and
#      an App folder in it (a small node server);
#   3. POST /cli/app/install {app, workspace}, then /cli/app/deploy {app, folder}:
#      the App's instance is
#      a Deployment, a Service and a NetworkPolicy; its init container
#      fetched the Seed from the controller with its token (sha256 checked);
#      the App runs as uid 20001 on the pinned node image, /app read-only;
#      the controller's health gate passed through the Service and the
#      controller reaches the App;
#   4. the policy: the App reaches its database (k2-pg:5432) and the
#      controller, and nothing else in the cluster (the neighbour); the
#      neighbour can't reach the App; the workspace's database login works
#      from a pod and /cli/db lists the database;
#   5. POST /cli/app/uninstall: the Deployment, Service and NetworkPolicy
#      are gone. The database is the workspace's (App databases are S21):
#      it stays, and db-check.sh proves its drop.
# Nothing secret is printed: tokens and DSNs stay in pods or pipes.
set -euo pipefail
export CAPS_NS=k2-p4-apps
# shellcheck source=scripts/kind/caps/lib.sh
source "$(dirname "$0")/lib.sh"
image=k2-daemon:caps keep=0
for a in "$@"; do
    case "$a" in
        --image=*) image=${a#--image=} ;;
        --keep) keep=1 ;;
        -h|--help) sed -n '2,28p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "apps-check.sh: unknown argument $a" >&2; exit 2 ;;
    esac
done
caps_kubeconfig
fails=0
ok() { printf 'ok   %s\n' "$*"; }
bad() { printf 'FAIL %s\n' "$*"; fails=$((fails + 1)); }
note() { printf 'note %s\n' "$*"; }
cleanup() {
    if [ "$keep" = 0 ]; then
        kn delete postgresql k2-pg --ignore-not-found --wait=true >/dev/null 2>&1 || true
        kc delete namespace "$CAPS_NS" --ignore-not-found --wait=true >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

ctl() {  # ctl <method> <path> [json body]: the controller's own API, its token read in the pod
    kn exec k2-controller-0 -c k2-daemon -- sh -c \
        'curl -sS --max-time 600 -X "$1" -H "Content-Type: application/json" ${3:+-d "$3"} "http://127.0.0.1:7420$2$(case "$2" in *\?*) echo "&";; *) echo "?";; esac)token=$(cat /var/lib/k2/.k2/daemon.token)"' \
        _ "$@" 2>&1 || true
}
field() { python3 -I -c 'import json,sys
try: d = json.load(sys.stdin)
except ValueError: print(""); sys.exit()
for k in sys.argv[1].split("."):
    d = d.get(k) if isinstance(d, dict) else None
print("" if d is None else (json.dumps(d) if isinstance(d, (dict, list)) else d))' "$1"; }
reach() {  # reach <pod> <container> <host> <port>: "open" or why not, from that pod (node's net)
    kn exec "$1" -c "$2" -- node -e 'const s=require("net").connect({host:process.argv[1],port:+process.argv[2]});
s.setTimeout(4000);s.on("connect",()=>{console.log("open");s.destroy()});s.on("timeout",()=>{console.log("timeout");s.destroy()});
s.on("error",e=>console.log(e.code||e.message))' "$3" "$4" 2>&1 | tail -1 || true
}

echo "== 1. namespace $CAPS_NS, postgresql k2-pg, the controller ($image), a database client"
caps_namespace
kn apply -f - >&2 <<EOF
apiVersion: acid.zalan.do/v1
kind: postgresql
metadata:
  name: k2-pg
  labels: {k2.dev/test: k8s-capabilities}
spec:
  teamId: k2
  numberOfInstances: 1
  postgresql: {version: "17"}
  users:
    k2: [createdb, createrole]
  databases:
    k2: k2
  volume: {size: 1Gi}
  resources:
    requests: {cpu: 100m, memory: 256Mi}
    limits: {cpu: "1", memory: 768Mi}
EOF
kn apply -f "$CAPS_REPO/deploy/kubernetes/core/rbac.yaml" >&2
if ! kn get secret k2-secrets >/dev/null 2>&1; then
    python3 -I -c 'import base64,json,secrets
print(json.dumps({"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
    "metadata": {"name": "k2-secrets", "labels": {"k2.dev/test": "k8s-capabilities"}},
    "data": {"host-enroll-token": base64.b64encode(secrets.token_urlsafe(32).encode()).decode()}}))' | kn create -f - >&2
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
  K2_CAP_APPS: kubernetes
  K2_CONTROLLER_SERVICE: k2-controller
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
      volumes:
        - name: k2-secrets
          secret: {secretName: k2-secrets}
  volumeClaimTemplates:
    - metadata: {name: data}
      spec:
        accessModes: [ReadWriteOnce]
        resources: {requests: {storage: 1Gi}}
---
apiVersion: v1
kind: Pod
metadata:
  name: db-client
  labels: {k2.dev/test: k8s-capabilities}
spec:
  terminationGracePeriodSeconds: 1
  securityContext: {runAsNonRoot: true, runAsUser: 10000, runAsGroup: 0, seccompProfile: {type: RuntimeDefault}}
  containers:
    - name: c
      image: $image
      imagePullPolicy: Never
      command: [sleep, "3600"]
      resources: {requests: {cpu: 10m, memory: 32Mi}, limits: {memory: 128Mi}}
      securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
EOF
st=""
for _ in $(seq 1 90); do
    st=$(kn get postgresql k2-pg -o jsonpath='{.status.PostgresClusterStatus}' 2>/dev/null || true)
    [ "$st" = Running ] && break
    sleep 5
done
[ "$st" = Running ] && ok "postgresql k2-pg Running" || bad "postgresql k2-pg: ${st:-no status}"
if kn rollout status statefulset/k2-controller --timeout=180s >&2; then
    ok "k2-controller ready"
else
    bad "k2-controller not ready"
    kn logs k2-controller-0 -c k2-daemon --tail=30 2>&1 | grep -viE "token|password|secret" | tail -12 | sed 's/^/     /'
    echo; echo "FAIL apps: the controller did not start"; exit 1
fi
kn wait --for=condition=Ready pod/db-client --timeout=120s >/dev/null && ok "database client pod ready" || bad "database client pod not ready"

echo "== 2. a workspace, its database, an App folder"
r=$(ctl POST /cli/db/server/enable '{}')
[ "$(printf '%s' "$r" | field state)" = running ] && ok "/cli/db on postgresql/k2-pg" || bad "db enable: $(printf '%s' "$r" | cut -c1-200)"
W=/var/lib/k2/work/appcheck
kn exec -i k2-controller-0 -c k2-daemon -- sh -c "mkdir -p $W/notes && cd $W && git init -q . 2>/dev/null; cat > $W/notes/manifest.json; cat > $W/notes/server.js <<'JS'
const http = require('http');
http.createServer((q, s) => {
  if (q.url === '/healthz') { s.writeHead(200); return s.end('ok'); }
  s.writeHead(200, {'content-type': 'text/plain'});
  s.end('hello from ' + process.env.K2_APP_ID + ' uid=' + process.getuid() + ' cwd=' + process.cwd());
}).listen(+process.env.PORT);
JS" <<'JSON'
{"schema": 1, "kind": "app", "id": "notes", "name": "Notes", "version": "1.0.0", "caps": [],
 "host": {"mode": "cmd", "start": ["node", "server.js"], "health": {"path": "/healthz"}, "drainSecs": 2}}
JSON
r=$(ctl POST /cli/projects/create "{\"name\":\"appcheck\",\"path\":\"$W\"}")
r=$(ctl POST /cli/db/create '{"project":"appcheck"}')
DB=$(printf '%s' "$r" | field name)
[ -n "$DB" ] && ok "workspace database $DB (through postgresql/k2-pg)" || bad "db create: $(printf '%s' "$r" | sed 's/:[^:@]*@/:***@/' | cut -c1-200)"

echo "== 3. install the App"
# install registers the App; the deploy (its own route) packs the folder's
# Seed and starts the instance.
show() { python3 -I -c 'import json,sys
try: d=json.load(sys.stdin)
except ValueError: d={"raw": sys.stdin.read()[:300]}
print("     " + sys.argv[1] + " -> " + json.dumps({k: d.get(k) for k in ("ok","state","error","code","message","deployment","hint") if k in d})[:400])' "$1"; }
ctl POST /cli/app/install "{\"app\":\"notes\",\"workspace\":\"appcheck\"}" | show install
ctl POST /cli/app/deploy "{\"app\":\"notes\",\"folder\":\"$W/notes\",\"wait\":300}" | show deploy
inst=""
for _ in $(seq 1 60); do
    inst=$(kn get deploy -l k2.dev/app=notes -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    [ -n "$inst" ] && [ "$(kn get deploy "$inst" -o jsonpath='{.status.availableReplicas}' 2>/dev/null)" = 1 ] && break
    sleep 5
done
if [ -z "$inst" ]; then
    bad "no Deployment for the App"
    ctl GET "/cli/app/inspect?app=notes" | cut -c1-600 | sed 's/^/     /'
    kn logs k2-controller-0 -c k2-daemon --tail=40 2>&1 | grep -iE "apps|k8s|seed" | grep -viE "token|password|secret" | tail -10 | sed 's/^/     /'
    echo; echo "FAIL apps: $fails check(s)"; exit 1
fi
ok "Deployment $inst available"
kn get service "$inst" >/dev/null 2>&1 && ok "Service $inst" || bad "no Service $inst"
kn get networkpolicy "$inst" >/dev/null 2>&1 && ok "NetworkPolicy $inst" || bad "no NetworkPolicy $inst"
pod=$(kn get pod -l "k2.dev/instance=$inst" -o jsonpath='{.items[0].metadata.name}')
seedlog=$(kn logs "$pod" -c seed 2>&1 | tail -1)
case "$seedlog" in *"sha256 ok"*) ok "init container: $seedlog" ;; *) bad "init container: $seedlog" ;; esac
img=$(kn get pod "$pod" -o jsonpath='{.spec.containers[0].image}')
case "$img" in docker.io/library/node@sha256:*) ok "the pinned node image, no per-App image ($img)" ;; *) bad "image $img" ;; esac
port=$(kn get service "$inst" -o jsonpath='{.spec.ports[0].port}')
body=$(kn exec k2-controller-0 -c k2-daemon -- curl -s --max-time 5 "http://$inst:$port/")
case "$body" in "hello from notes uid=20001 cwd=/app") ok "the controller reaches the App: '$body'" ;; *) bad "from the controller: '$body'" ;; esac
w=$(kn exec "$pod" -c app -- sh -c 'touch /app/x 2>&1 || true' | tail -1)
case "$w" in *"Read-only"*) ok "/app is read-only in the App" ;; *) bad "/app writable? '$w'" ;; esac
state=$(ctl GET "/cli/app/inspect?app=notes" | field runtime.state)
note "App state per the controller: ${state:-?}"

echo "== 4. the App's policy, and its database"
# A neighbour that is not the controller, on the App's own (pinned) image,
# with a listener on 8081, so "can't reach" is never a closed port.
kn apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: neighbour
  labels: {k2.dev/test: k8s-capabilities, k2.dev/role: app}
spec:
  terminationGracePeriodSeconds: 1
  securityContext: {runAsNonRoot: true, runAsUser: 10000, runAsGroup: 0, seccompProfile: {type: RuntimeDefault}}
  containers:
    - name: c
      image: $img
      command: [node, -e, 'require("http").createServer((q,s)=>s.end("neighbour")).listen(8081)']
      ports: [{containerPort: 8081}]
      resources: {requests: {cpu: 10m, memory: 32Mi}, limits: {memory: 128Mi}}
      securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
EOF
kn wait --for=condition=Ready pod/neighbour --timeout=180s >/dev/null && ok "neighbour pod ready (node, :8081)" || bad "neighbour pod not ready"
nip=$(kn get pod neighbour -o jsonpath='{.status.podIP}')
r=$(kn exec k2-controller-0 -c k2-daemon -- curl -s --max-time 4 "http://$nip:8081/" || true)
[ "$r" = neighbour ] && ok "baseline: the neighbour answers on 8081 (from the controller)" || bad "baseline: the neighbour does not answer ('$r')"
r=$(reach "$pod" app k2-pg 5432); [ "$r" = open ] && ok "the App reaches its database (k2-pg:5432)" || bad "App -> k2-pg:5432: $r"
r=$(reach "$pod" app k2-controller 7420); [ "$r" = open ] && ok "the App reaches the controller (7420)" || bad "App -> controller: $r"
r=$(reach "$pod" app "$nip" 8081); [ "$r" != open ] && ok "the App can't reach the neighbour ($r)" || bad "the App reached the neighbour"
r=$(reach neighbour c "$inst" "$port"); [ "$r" != open ] && ok "the neighbour can't reach the App ($r)" || bad "the neighbour reached the App"
login=$(ctl GET "/cli/db/dsn?project=appcheck" | field dsn | kn exec -i db-client -- sh -c 'read -r DSN; psql "$DSN" -tAX -c "SELECT current_user" 2>&1' || true)
case "$login" in ${DB}_writer_user) ok "the workspace's database login works ($login)" ;; *) bad "database login: $login" ;; esac
listed=$(ctl GET "/cli/db/list?project=appcheck" | python3 -I -c 'import json,sys
d=json.load(sys.stdin); print(" ".join(x.get("name","") for x in (d.get("databases") or d.get("items") or [])))' 2>/dev/null || true)
case " $listed " in *" $DB "*) ok "/cli/db lists $DB" ;; *) bad "/cli/db list: '$listed'" ;; esac

echo "== 5. uninstall"
r=$(ctl POST /cli/app/uninstall '{"app":"notes"}')
gone=""
for _ in $(seq 1 30); do
    n=$(kn get deploy,service,networkpolicy -l k2.dev/app=notes --no-headers 2>/dev/null | wc -l | tr -d ' ')
    [ "$n" = 0 ] && gone=1 && break
    sleep 2
done
[ -n "$gone" ] && ok "Deployment, Service and NetworkPolicy gone" || bad "left after uninstall: $(kn get deploy,service,networkpolicy -l k2.dev/app=notes --no-headers 2>/dev/null | awk '{print $1}' | xargs)"
[ -n "$(kn exec k2-pg-0 -c postgres -- psql -U postgres -tAX -c "SELECT 1 FROM pg_database WHERE datname='$DB'" 2>/dev/null)" ] \
    && note "the workspace's database stays (it is the workspace's; App databases are S21)" || bad "the workspace's database went with the App"

echo
if [ "$fails" = 0 ]; then echo "PASS apps: an App on Kubernetes beside its workspace database (S12 parity)"; else echo "FAIL apps: $fails check(s)"; exit 1; fi
