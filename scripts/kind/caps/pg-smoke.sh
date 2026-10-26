#!/usr/bin/env bash
# T3: the Zalando postgres-operator path on k2lab, before any K2 code uses it.
#
#   scripts/kind/caps/pg-smoke.sh [--keep]
#
# In namespace k2-p4-caps:
#   1. postgresql k2-pg (deploy/kubernetes/core/postgres.yaml's shape, kind
#      sized): the operator makes the cluster and the Secret for user k2;
#   2. patch in a plain user and database (spec.users, spec.databases), as
#      rbac.yaml's comment describes for Apps: its Secret appears, it logs in;
#   3. patch in a preparedDatabases entry: the operator makes <db>_owner and
#      <db>_writer roles with Secrets; both log in, the writer can write a
#      table the owner made in the default schema.
# Logins run psql in a client pod; passwords reach it only through
# secretKeyRef, never printed. --keep leaves k2-pg running for the next
# steps (T3 uses it); the default deletes it.
set -euo pipefail
# shellcheck source=scripts/kind/caps/lib.sh
source "$(dirname "$0")/lib.sh"
keep=0
for a in "$@"; do
    case "$a" in
        --keep) keep=1 ;;
        -h|--help) sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "pg-smoke.sh: unknown argument $a" >&2; exit 2 ;;
    esac
done
caps_kubeconfig
caps_namespace
SPILO=ghcr.io/zalando/spilo-18:4.1-p2     # cached on k2lab's nodes; has psql
fails=0
ok() { printf 'ok   %s\n' "$*"; }
bad() { printf 'FAIL %s\n' "$*"; fails=$((fails + 1)); }
secret_of() { printf '%s.k2-pg.credentials.postgresql.acid.zalan.do' "${1//_/-}"; }
wait_secret() {  # wait_secret <user> [seconds]
    local s; s=$(secret_of "$1")
    for _ in $(seq 1 "${2:-180}"); do kn get secret "$s" >/dev/null 2>&1 && return 0; sleep 1; done
    return 1
}
login() {  # login <user> <db> <sql>: run psql as <user> in a throwaway pod; prints psql's output
    local user=$1 db=$2 sql=$3 name="pg-login-$RANDOM"
    python3 -I - "$name" "$SPILO" "$(secret_of "$user")" "$db" "$sql" <<'PY' | kn apply -f - >/dev/null
import json, sys
name, image, secret, db, sql = sys.argv[1:]
print(json.dumps({"apiVersion": "v1", "kind": "Pod",
    "metadata": {"name": name, "labels": {"k2.dev/test": "pg-smoke"}},
    "spec": {"restartPolicy": "Never", "terminationGracePeriodSeconds": 1,
             "securityContext": {"runAsNonRoot": True, "runAsUser": 101, "runAsGroup": 101,
                                 "seccompProfile": {"type": "RuntimeDefault"}},
             "containers": [{"name": "psql", "image": image, "imagePullPolicy": "IfNotPresent",
                 "command": ["sh", "-c", 'psql "host=k2-pg port=5432 dbname=$DB user=$PGUSER sslmode=require" -q -v ON_ERROR_STOP=1 -tAc "$SQL"'],
                 "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
                 "env": [{"name": "DB", "value": db}, {"name": "SQL", "value": sql},
                         {"name": "PGUSER", "valueFrom": {"secretKeyRef": {"name": secret, "key": "username"}}},
                         {"name": "PGPASSWORD", "valueFrom": {"secretKeyRef": {"name": secret, "key": "password"}}}],
                 "resources": {"requests": {"cpu": "10m", "memory": "32Mi"}, "limits": {"memory": "128Mi"}}}]}}))
PY
    kn wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$name" --timeout=120s >/dev/null 2>&1 || true
    kn logs "$name" 2>&1 | tail -3
    local phase; phase=$(kn get pod "$name" -o jsonpath='{.status.phase}')
    kn delete pod "$name" --wait=false >/dev/null 2>&1
    [ "$phase" = Succeeded ]
}
cleanup() {
    kn delete pod -l k2.dev/test=pg-smoke --ignore-not-found --wait=false >/dev/null 2>&1 || true
    if [ "$keep" = 0 ]; then kn delete postgresql k2-pg --ignore-not-found >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

echo "== 1. postgresql k2-pg"
kn apply -f - >&2 <<'EOF'
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
for _ in $(seq 1 300); do
    st=$(kn get postgresql k2-pg -o jsonpath='{.status.PostgresClusterStatus}' 2>/dev/null)
    [ "$st" = Running ] && break
    sleep 2
done
[ "$st" = Running ] && ok "k2-pg Running ($(kn get pod -l cluster-name=k2-pg -o jsonpath='{.items[0].spec.nodeName}'))" || bad "k2-pg status: ${st:-none}"
wait_secret k2 && ok "Secret $(secret_of k2)" || bad "no Secret for k2"
out=$(login k2 k2 "select current_user || ' ' || version()") && ok "k2 logs in: ${out:0:60}" || bad "k2 login: $out"

echo "== 2. a plain user and database patched in"
kn patch postgresql k2-pg --type merge -p '{"spec":{"users":{"app_smoke":[]},"databases":{"app_smoke":"app_smoke"}}}' >&2
wait_secret app_smoke && ok "Secret $(secret_of app_smoke)" || bad "no Secret for app_smoke"
out=""
for _ in 1 2 3 4 5 6; do out=$(login app_smoke app_smoke "select current_user || '@' || current_database()") && break; sleep 5; done
[ "$out" = "app_smoke@app_smoke" ] && ok "app_smoke logs in to app_smoke" || bad "app_smoke login: $out"

echo "== 3. a prepared database: owner and writer roles"
kn patch postgresql k2-pg --type merge -p '{"spec":{"preparedDatabases":{"ws_smoke":{"defaultUsers":true,"schemas":{"data":{}}}}}}' >&2
for r in ws_smoke_owner_user ws_smoke_writer_user; do
    wait_secret "$r" && ok "Secret $(secret_of "$r")" || bad "no Secret for $r"
done
out=""
for _ in 1 2 3 4 5 6; do out=$(login ws_smoke_owner_user ws_smoke "set client_min_messages to warning; create table if not exists data.t(x int); select 'owner made data.t'") && break; sleep 5; done
[ "$out" = "owner made data.t" ] && ok "ws_smoke_owner_user created data.t" || bad "owner: $out"
out=$(login ws_smoke_writer_user ws_smoke "insert into data.t values (1); select count(*) from data.t") \
    && [ "$out" -ge 1 ] 2>/dev/null && ok "ws_smoke_writer_user wrote to data.t (count $out)" || bad "writer: $out"

echo
if [ "$fails" = 0 ]; then echo "PASS pg-smoke: the operator makes users, databases and prepared-database roles that log in"; else echo "FAIL pg-smoke: $fails check(s)"; exit 1; fi
