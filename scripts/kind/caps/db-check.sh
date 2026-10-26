#!/usr/bin/env bash
# T3 decision 1: per-workspace databases (/cli/db) through the Postgres
# operator, end to end on k2lab, in a fresh namespace.
#
#   scripts/kind/caps/db-check.sh [--image=k2-daemon:caps] [--keep]
#
# Namespace k2-p4-db (made here, deleted at the end unless --keep), with
# namespace k2's Pod Security:
#   1. postgresql k2-pg (one Spilo pod, Postgres 17) and a controller shaped
#      like deploy/kubernetes/core/controller.yaml with only Postgres
#      delegated (K2_CAP_POSTGRES=kubernetes) and rbac.yaml's RBAC;
#   2. POST /cli/db/server/enable: running on the operator's cluster, no unit;
#   3. two workspaces (POST /cli/projects/create), A and B, each with
#      POST /cli/db/create:
#      - postgresql/k2-pg gains a preparedDatabases entry per database, and
#        the operator's Secrets for its owner and writer users exist;
#      - in Postgres (as the superuser, in the Spilo pod): the database is
#        owned by {db}_owner_user, both users are NOINHERIT, the owner user
#        is a member of the writer user, _k2_migrations is the owner's;
#   4. POST /cli/db/migrate for A (a migration file in A's folder): the table
#      is the owner user's;
#   5. with A's agent DSN (GET /cli/db/dsn; it travels from the controller
#      pod to a client pod over stdin, never printed), over TLS to the
#      Service: insert and read work, CREATE TABLE is refused, and K2's
#      _k2_migrations can't be altered (it is the migrator's);
#   6. B's agent cannot connect to A's database (PUBLIC's default CONNECT
#      is revoked at create, and the operator's syncs leave it so);
#   7. POST /cli/db/drop for A: the entry leaves postgresql/k2-pg, the
#      database is gone, and an operator sync does not bring it back.
# No password is printed: the controller's token and the DSNs stay in pods.
set -euo pipefail
export CAPS_NS=k2-p4-db
# shellcheck source=scripts/kind/caps/lib.sh
source "$(dirname "$0")/lib.sh"
image=k2-daemon:caps keep=0
for a in "$@"; do
    case "$a" in
        --image=*) image=${a#--image=} ;;
        --keep) keep=1 ;;
        -h|--help) sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "db-check.sh: unknown argument $a" >&2; exit 2 ;;
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
        'curl -sS --max-time 120 -X "$1" -H "Content-Type: application/json" ${3:+-d "$3"} "http://127.0.0.1:7420$2$(case "$2" in *\?*) echo "&";; *) echo "?";; esac)token=$(cat /var/lib/k2/.k2/daemon.token)"' \
        _ "$@" 2>&1 || true
}
field() { python3 -I -c 'import json,sys
try: d = json.load(sys.stdin)
except ValueError: print(""); sys.exit()
for k in sys.argv[1].split("."):
    d = d.get(k) if isinstance(d, dict) else None
print("" if d is None else d)' "$1"; }
su_sql() {  # su_sql <db> <sql>: as postgres inside the Spilo pod (local socket)
    kn exec k2-pg-0 -c postgres -- psql -U postgres -d "$1" -tAX -c "$2" 2>&1 || true
}
as_agent() {  # as_agent <workspace> <db> <sql>: that workspace's agent DSN, into the client pod over stdin
    ctl GET "/cli/db/dsn?project=$1" | field dsn \
        | kn exec -i db-client -- sh -c 'read -r DSN; [ -n "$DSN" ] || { echo "no DSN"; exit 1; }
            DB=${DSN##*/}; DB=${DB%%\?*}; [ -n "$2" ] && DSN=$(printf "%s" "$DSN" | sed "s#/$DB?#/$2?#")
            psql "$DSN" -tAX -v ON_ERROR_STOP=1 -c "$1" 2>&1' _ "$3" "$2" || true
}

echo "== 1. namespace $CAPS_NS, postgresql k2-pg, the controller ($image)"
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
st=""
for _ in $(seq 1 90); do
    st=$(kn get postgresql k2-pg -o jsonpath='{.status.PostgresClusterStatus}' 2>/dev/null || true)
    [ "$st" = Running ] && break
    sleep 5
done
[ "$st" = Running ] && ok "postgresql k2-pg Running" || bad "postgresql k2-pg: ${st:-no status}"
kn apply -f "$CAPS_REPO/deploy/kubernetes/core/rbac.yaml" >&2
# A controller runs agents on hosts only (K2_HOSTS); none join here, but
# boot needs the setting and the hosts' bootstrap token (from stdin, never
# printed), as controller-check.sh.
if ! kn get secret k2-secrets >/dev/null 2>&1; then
    python3 -I -c 'import base64,json,secrets
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
  K2_TUNNEL_LEASE: "off"
  K2_RELAY_LIST_URL: http://127.0.0.1:9/disabled
  K2_DNS_API_BASE: http://127.0.0.1:9
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
  labels: {k2.dev/test: k8s-capabilities, k2.dev/role: app}
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
if kn rollout status statefulset/k2-controller --timeout=180s >&2; then
    ok "k2-controller ready"
else
    bad "k2-controller not ready: $(kn get pod k2-controller-0 -o jsonpath='{.status.containerStatuses[0].state}' 2>/dev/null | cut -c1-200)"
    kn logs k2-controller-0 -c k2-daemon --tail=40 2>&1 | grep -viE "token|password|secret" | tail -15 | sed 's/^/     /'
    kn logs k2-controller-0 -c k2-daemon --previous --tail=40 2>/dev/null | grep -viE "token|password|secret" | tail -10 | sed 's/^/     prev /'
    echo; echo "FAIL db: the controller did not start"; exit 1
fi
kn wait --for=condition=Ready pod/db-client --timeout=120s >/dev/null && ok "client pod ready (psql $(kn exec db-client -- psql --version | awk '{print $3}'))" || bad "client pod not ready"

echo "== 2. enable"
r=$(ctl POST /cli/db/server/enable '{}')
[ "$(printf '%s' "$r" | field state)" = running ] && [ "$(printf '%s' "$r" | field cluster)" = k2-pg ] \
    && ok "running on postgresql/k2-pg (Postgres $(printf '%s' "$r" | field installedMajor)), no unit" \
    || bad "enable: $(printf '%s' "$r" | cut -c1-300)"

echo "== 3. two workspaces, a database each"
for w in a b; do
    kn exec k2-controller-0 -c k2-daemon -- mkdir -p "/var/lib/k2/work/dbcheck-$w/.k2/db/migrations" || bad "mkdir in the controller"
    ctl POST /cli/projects/create "{\"name\":\"dbcheck-$w\",\"path\":\"/var/lib/k2/work/dbcheck-$w\"}" >/dev/null
done
A="" B=""
for w in a b; do
    r=$(ctl POST /cli/db/create "{\"project\":\"dbcheck-$w\"}")
    name=$(printf '%s' "$r" | field name)
    if [ "$w" = a ]; then A=$name; else B=$name; fi
    user=$(printf '%s' "$r" | python3 -I -c 'import json,sys,urllib.parse as u
try: print(u.urlsplit(json.load(sys.stdin)["dsn"]).username or "")
except Exception: print("")')
    host=$(printf '%s' "$r" | python3 -I -c 'import json,sys,urllib.parse as u
try: p = u.urlsplit(json.load(sys.stdin)["dsn"]); print("%s:%s %s" % (p.hostname, p.port, p.query))
except Exception: print("")')
    if [ -n "$name" ] && [ "$user" = "${name}_writer_user" ]; then
        ok "workspace $w: database $name, agent $user at $host"
    else
        bad "workspace $w: create -> $(printf '%s' "$r" | sed 's/:[^:@]*@/:***@/' | cut -c1-300)"
    fi
done
if [ -z "$A" ] || [ -z "$B" ]; then echo; echo "FAIL db: no database was created ($fails check(s))"; exit 1; fi
entries=$(kn get postgresql k2-pg -o jsonpath='{.spec.preparedDatabases}' | python3 -I -c 'import json,sys; print(" ".join(sorted(json.load(sys.stdin))))')
[ "$entries" = "$(printf '%s\n%s\n' "$A" "$B" | sort | xargs)" ] && ok "postgresql/k2-pg declares both: $entries" || bad "preparedDatabases: $entries"
for u in "${A}_owner_user" "${A}_writer_user"; do
    s="$(printf '%s' "$u" | tr _ -).k2-pg.credentials.postgresql.acid.zalan.do"
    kn get secret "$s" >/dev/null 2>&1 && ok "the operator's Secret for $u" || bad "no Secret $s"
done
[ "$(su_sql postgres "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname='$A'")" = "${A}_owner_user" ] \
    && ok "$A is owned by ${A}_owner_user (K2's migrator)" || bad "$A owner: $(su_sql postgres "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname='$A'")"
inh=$(su_sql postgres "SELECT string_agg(rolname||'='||rolinherit::text, ' ' ORDER BY rolname) FROM pg_roles WHERE rolname IN ('${A}_owner_user','${A}_writer_user')")
[ "$inh" = "${A}_owner_user=false ${A}_writer_user=false" ] && ok "both logins NOINHERIT" || bad "inherit: $inh"
[ "$(su_sql postgres "SELECT pg_has_role('${A}_owner_user','${A}_writer_user','MEMBER')")" = t ] \
    && ok "the migrator is a member of the agent (SET ROLE)" || bad "no membership owner_user → writer_user"
[ "$(su_sql "$A" "SELECT tableowner FROM pg_tables WHERE tablename='_k2_migrations'")" = "${A}_owner_user" ] \
    && ok "K2's _k2_migrations is the migrator's" || bad "_k2_migrations owner: $(su_sql "$A" "SELECT tableowner FROM pg_tables WHERE tablename='_k2_migrations'")"

echo "== 4. migrate A"
kn exec -i k2-controller-0 -c k2-daemon -- sh -c 'cat > /var/lib/k2/work/dbcheck-a/.k2/db/migrations/0001_notes.sql' <<'SQL'
CREATE TABLE notes (id serial PRIMARY KEY, body text NOT NULL);
SQL
r=$(ctl POST /cli/db/migrate '{"project":"dbcheck-a"}')
owner=$(su_sql "$A" "SELECT tableowner FROM pg_tables WHERE tablename='notes'")
[ "$owner" = "${A}_owner_user" ] && ok "migration applied as the migrator (notes owned by $owner)" || bad "migrate: $(printf '%s' "$r" | cut -c1-300); notes owner '$owner'"

echo "== 5. A's agent, from a pod, over TLS"
out="$(as_agent dbcheck-a "" "INSERT INTO notes (body) VALUES ('hello')") $(as_agent dbcheck-a "" "SELECT count(*) FROM notes")"
out="$out $(as_agent dbcheck-a "" "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")"
[ "$out" = "INSERT 0 1 1 t" ] && ok "insert and read as the agent, TLS on" || bad "agent: $out"
out=$(as_agent dbcheck-a "" "CREATE TABLE t (x int)")
printf '%s' "$out" | grep -q "permission denied" && ok "the agent cannot CREATE TABLE" || bad "CREATE TABLE as the agent: $out"
# The agent has DML on K2's tables (as on a host; the seal is for guests),
# never their shape: they are the migrator's.
out=$(as_agent dbcheck-a "" "ALTER TABLE _k2_migrations ADD COLUMN x int")
printf '%s' "$out" | grep -q "must be owner" && ok "the agent cannot alter K2's _k2_migrations (the migrator's)" || bad "agent altering _k2_migrations: $out"

echo "== 6. B's agent and A's database"
# Only a database's own logins connect (sql/ops.rs connect_lockdown_sql,
# host and Kubernetes alike). B's create came after A's: the operator
# synced every prepared database then, and PUBLIC's CONNECT stayed away.
out=$(as_agent dbcheck-b "$A" "SELECT 1")
printf '%s' "$out" | grep -q "permission denied for database" && ok "B's agent cannot connect to $A" || bad "B's agent connecting to $A: $out"
acl=$(su_sql postgres "SELECT has_database_privilege('public', '$A', 'CONNECT')")
[ "$acl" = f ] && ok "PUBLIC has no CONNECT on $A after the operator's syncs" || bad "PUBLIC CONNECT on $A: $acl"

echo "== 7. drop A"
r=$(ctl POST /cli/db/drop '{"project":"dbcheck-a","yes":true}')
[ "$(printf '%s' "$r" | field dropped)" = "$A" ] && ok "dropped $A" || bad "drop: $(printf '%s' "$r" | cut -c1-300)"
kn get postgresql k2-pg -o jsonpath='{.spec.preparedDatabases}' | grep -q "\"$A\"" && bad "$A still declared" || ok "$A is no longer declared"
# The forget patch was itself a spec change: the operator syncs on it.
sleep 30
[ -z "$(su_sql postgres "SELECT 1 FROM pg_database WHERE datname='$A'")" ] && ok "$A is gone after an operator sync" || bad "$A exists again"
[ -n "$(su_sql postgres "SELECT 1 FROM pg_database WHERE datname='$B'")" ] && ok "$B is untouched" || bad "$B is missing"

echo
if [ "$fails" = 0 ]; then echo "PASS db: /cli/db creates, migrates, serves and drops workspace databases through postgresql/k2-pg"; else echo "FAIL db: $fails check(s)"; exit 1; fi
