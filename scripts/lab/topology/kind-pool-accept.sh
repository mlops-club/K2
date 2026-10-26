#!/usr/bin/env bash
# P4 T1 kind acceptance: the controller's Kubernetes provisioner scales the
# host pool on kind k2lab (docs/prd/progress/k8s-provisioner.md).
#
#   scripts/lab/topology/kind-pool-accept.sh <daemon image> <fake agents image>
#
# A fresh namespace (K2_P4_NS, default k2-p4-pool; never k2-kind*), applied
# from the manifests below (modelled on deploy/kubernetes/core, which this
# never edits): a controller with K2_PROVISIONER=kubernetes, the RBAC of
# core/rbac.yaml plus D8's patch on the Secret k2-host-enroll, signed
# enrollment (K2_HOST_AUTH=enroll); the StatefulSet k2-host at 0 replicas
# (the provisioner mints and scales every host), K2_HOST_MAX_AWAKE=2, each
# host its own volume (no shared k2-work) at /var/lib/k2 with home sync on
# (K2_WORK_ROOT=/var/lib/k2/work; each agent its own workspace under it),
# readiness = k2-daemon --host-health (D10); the PDB maxUnavailable 1. Fake
# agents only; each wake's first turn is "remember …" (MEMORY.md).
#
# Checks:
#   1. the controller runs the kubernetes provisioner; 0 hosts
#   2. six wakes end with 3 host pods (MAX_HOSTS=3, max_awake=2), each
#      host enrolled by key (GET /cli/hosts/keys), every wake answered 200
#   3. a seventh wake is refused naming K2_PROVISIONER_MAX_HOSTS
#   4. readiness is k2-daemon --host-health: every host pod Ready
#   5. kubectl get pdb: one disruption allowed
#   6. after some agents sleep, the idle window drains the highest ordinal
#      (its agent released: asleep) and the pool shrinks back to 1 pod
#      - the drained agent's home: its host saved a snapshot at the released
#        epoch before the pool scaled the host away; woken again, the agent
#        lands on another pod, which restores that snapshot first (fetching
#        its objects) and has its memory
#   7. I1: no agent ever placed on two hosts at once (controller log)
#
# K2_P4_REAL=1: the variant with the real agents image (Claude Code in the
# host image, kind-app's Dockerfile.agents): one wake of a plain `sh`
# terminal (not an agent CLI) makes the provisioner create k2-host-0; the
# pod enrolls and turns Ready; `claude` is installed and resolvable on its
# PATH. Nothing runs `claude` (ground rule: no real agent CLIs from scripts).
#
# Only k2lab, through an explicit kubeconfig; tokens are read into files
# (0600) under the run dir's private tmp, never printed.
set -uo pipefail
REPO=$(cd "$(dirname "$0")/../../.." && pwd)
DAEMON_IMAGE=${1:?daemon image, e.g. k2-daemon:k8s-accept}
AGENTS_IMAGE=${2:?fake agents image, e.g. k2-agents:k8s-accept-fake}
NS=${K2_P4_NS:-k2-p4-pool}
case "$NS" in k2-kind*) echo "kind-pool-accept: refusing namespace $NS (k2-kind* belongs to others)" >&2; exit 2 ;; esac
IDLE_AFTER=${K2_P4_IDLE_AFTER:-45}
RUN="$REPO/lab-runs/k8s-provisioner/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN"
PRIV=$(mktemp -d "${TMPDIR:-/tmp}/k2-p4-XXXXXX"); chmod 700 "$PRIV"
KCFG="$PRIV/k2lab.kubeconfig"
kind get kubeconfig --name k2lab > "$KCFG" || exit 1
chmod 600 "$KCFG"
kc() { kubectl --kubeconfig "$KCFG" "$@"; }
kn() { kc -n "$NS" "$@"; }
say() { printf '%s %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "$RUN/log.txt" >&2; }
FAILS=0
check() { # name ok detail
    local line="PASS $1 -- $3"
    if [ "$2" != 1 ]; then line="FAIL $1 -- $3"; FAILS=$((FAILS + 1)); fi
    echo "$line" | tee -a "$RUN/checks.txt"
}
PF_PID=""
cleanup() {
    [ -n "$PF_PID" ] && kill "$PF_PID" 2>/dev/null
    rm -rf "$PRIV"
}
trap cleanup EXIT

say "namespace $NS (fresh), images $DAEMON_IMAGE / $AGENTS_IMAGE, idle window ${IDLE_AFTER}s"
kc delete namespace "$NS" --ignore-not-found --wait=true >/dev/null
kc create namespace "$NS" >/dev/null || exit 1
kc label namespace "$NS" k2.dev/deploy=p4-pool-accept >/dev/null
# The bootstrap token (unused in enroll mode, required by the controller's
# config): random, from stdin, never printed.
head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$PRIV/bootstrap"
kn create secret generic k2-secrets --from-file=host-enroll-token="$PRIV/bootstrap" >/dev/null || exit 1
# Pre-created and empty: RBAC lets the controller patch it by name only.
kn create secret generic k2-host-enroll >/dev/null || exit 1

cat > "$RUN/manifests.yaml" <<EOF
apiVersion: v1
kind: ServiceAccount
metadata: {name: k2-controller}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: k2-controller}
rules:
  # deploy/kubernetes/core/rbac.yaml (the host pool part)
  - {apiGroups: ["apps"], resources: ["statefulsets"], resourceNames: ["k2-host"], verbs: ["get", "watch", "patch"]}
  - {apiGroups: ["apps"], resources: ["statefulsets/scale"], resourceNames: ["k2-host"], verbs: ["get", "patch", "update"]}
  - {apiGroups: [""], resources: ["pods"], verbs: ["get", "list", "watch"]}
  - {apiGroups: [""], resources: ["secrets"], verbs: ["get"]}
  # D8: mint per ordinal into the pre-created Secret
  - {apiGroups: [""], resources: ["secrets"], resourceNames: ["k2-host-enroll"], verbs: ["patch"]}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: k2-controller}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: k2-controller}
subjects: [{kind: ServiceAccount, name: k2-controller}]
---
apiVersion: v1
kind: Service
metadata: {name: k2-controller}
spec:
  selector: {app.kubernetes.io/name: k2-controller}
  ports: [{name: api, port: 7420, targetPort: api}]
---
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: k2-controller}
spec:
  serviceName: k2-controller
  replicas: 1
  selector: {matchLabels: {app.kubernetes.io/name: k2-controller}}
  template:
    metadata: {labels: {app.kubernetes.io/name: k2-controller}}
    spec:
      serviceAccountName: k2-controller
      securityContext: {runAsNonRoot: true, runAsUser: 10000, runAsGroup: 0, fsGroup: 10000, seccompProfile: {type: RuntimeDefault}}
      containers:
        - name: k2-daemon
          image: $DAEMON_IMAGE
          imagePullPolicy: Never
          env:
            - {name: HOME, value: /var/lib/k2}
            - {name: K2_HOME, value: /var/lib/k2/.k2}
            - {name: K2_ROLE, value: controller}
            - {name: K2_LISTEN, value: lan}
            - {name: K2_DAEMON_PORT, value: "7420"}
            - {name: K2_HOSTS, value: "1"}
            - {name: K2_HOST_BOOTSTRAP_TOKEN_FILE, value: /run/secrets/k2/host-enroll-token}
            - {name: K2_HOST_AUTH, value: enroll}
            - {name: K2_PROVISIONER, value: kubernetes}
            - {name: K2_PROVISIONER_HOST_STATEFULSET, value: k2-host}
            - {name: K2_PROVISIONER_MIN_HOSTS, value: "1"}
            - {name: K2_PROVISIONER_MAX_HOSTS, value: "3"}
            - {name: K2_PROVISIONER_IDLE_AFTER, value: "$IDLE_AFTER"}
            - {name: K2_PROVISIONER_WAIT, value: "240"}
          ports: [{name: api, containerPort: 7420}]
          startupProbe: {httpGet: {path: /boot-status, port: api}, periodSeconds: 5, timeoutSeconds: 5, failureThreshold: 60}
          readinessProbe: {httpGet: {path: /boot-status, port: api}, periodSeconds: 10, timeoutSeconds: 5}
          resources: {requests: {cpu: 200m, memory: 384Mi}, limits: {memory: 1536Mi}}
          securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
          volumeMounts:
            - {name: data, mountPath: /var/lib/k2}
            - {name: k2-secrets, mountPath: /run/secrets/k2, readOnly: true}
      volumes:
        - {name: k2-secrets, secret: {secretName: k2-secrets}}
  volumeClaimTemplates:
    - metadata: {name: data}
      spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 2Gi}}}
---
apiVersion: v1
kind: ServiceAccount
metadata: {name: k2-host}
automountServiceAccountToken: false
---
apiVersion: v1
kind: Service
metadata: {name: k2-host}
spec:
  clusterIP: None
  selector: {app.kubernetes.io/name: k2-host}
---
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: k2-host}
spec:
  serviceName: k2-host
  replicas: 0                                  # the provisioner scales it
  podManagementPolicy: Parallel
  selector: {matchLabels: {app.kubernetes.io/name: k2-host}}
  template:
    metadata: {labels: {app.kubernetes.io/name: k2-host}}
    spec:
      serviceAccountName: k2-host
      automountServiceAccountToken: false
      terminationGracePeriodSeconds: 60
      securityContext: {runAsNonRoot: true, runAsUser: 10000, runAsGroup: 0, fsGroup: 10000, seccompProfile: {type: RuntimeDefault}}
      containers:
        - name: k2-host
          image: $AGENTS_IMAGE
          imagePullPolicy: Never
          env:
            - {name: HOME, value: /var/lib/k2}
            - {name: K2_HOME, value: /var/lib/k2/.k2}
            - {name: K2_ROLE, value: host}
            - {name: K2_WORK_ROOT, value: /var/lib/k2/work}   # home sync (P3 T1): off without it
            - {name: K2_DAEMON_PORT, value: "7420"}
            - {name: K2_CONTROLLER_URL, value: "ws://k2-controller:7420"}
            - name: K2_HOST_NAME
              valueFrom: {fieldRef: {fieldPath: metadata.name}}
            - {name: K2_HOST_AUTH, value: enroll}
            - {name: K2_HOST_ENROLL_TOKEN_FILE, value: "/run/secrets/k2-enroll/\$(K2_HOST_NAME)"}
            - {name: K2_HOST_LABELS, value: "pool=default"}
            - {name: K2_HOST_MAX_AWAKE, value: "2"}
          readinessProbe:
            exec: {command: [/usr/bin/k2-daemon, --host-health]}   # D10
            periodSeconds: 5
            timeoutSeconds: 5
          resources: {requests: {cpu: 100m, memory: 256Mi}, limits: {memory: 1Gi}}
          securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
          volumeMounts:
            - {name: data, mountPath: /var/lib/k2}
            - {name: k2-host-enroll, mountPath: /run/secrets/k2-enroll, readOnly: true}
      volumes:
        - name: k2-host-enroll
          secret: {secretName: k2-host-enroll, optional: true, defaultMode: 0440}
  volumeClaimTemplates:
    - metadata: {name: data}
      spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 1Gi}}}
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: {name: k2-host}
spec:
  maxUnavailable: 1
  selector: {matchLabels: {app.kubernetes.io/name: k2-host}}
EOF
kn apply -f "$RUN/manifests.yaml" > "$RUN/apply.txt" || exit 1
say "waiting for the controller"
kn rollout status statefulset/k2-controller --timeout=300s >/dev/null || { kn describe pod k2-controller-0 > "$RUN/controller-describe.txt"; exit 1; }

# The controller's API on a local port; its owner token into a 0600 file.
PORT=${K2_P4_PORT:-17460}
kn port-forward svc/k2-controller "$PORT:7420" > "$RUN/port-forward.log" 2>&1 &
PF_PID=$!
for i in $(seq 1 100); do curl -s -o /dev/null "http://127.0.0.1:$PORT/boot-status" && break; sleep 0.3; done
kn exec k2-controller-0 -- cat /var/lib/k2/.k2/daemon.token > "$PRIV/token" 2>/dev/null; chmod 600 "$PRIV/token"
ctl() { curl -s --max-time "${2:-30}" "http://127.0.0.1:$PORT$1?token=$(cat "$PRIV/token")"; }
mkdir -p "$PRIV/sessions"
spawn() { # agent [args JSON] → "HTTP body". Each agent its own workspace under the
    # hosts' work root; a first wake starts a conversation with one turn.
    local args=${2:-}
    if [ -z "$args" ]; then
        uuidgen | tr A-Z a-z > "$PRIV/sessions/$1"
        args="[\"--session-id\",\"$(cat "$PRIV/sessions/$1")\",\"remember $1 before the scale-down\"]"
    fi
    curl -s --max-time 300 -o "$PRIV/spawn.out" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        "http://127.0.0.1:$PORT/cli/sessions/v2/spawn?token=$(cat "$PRIV/token")" \
        -d "{\"agent_name\":\"$1\",\"cwd\":\"/var/lib/k2/work/$1\",\"command\":\"claude\",\"args\":$args}"
    echo " $(head -c 300 "$PRIV/spawn.out")"
}
memory() { # pod agent: the agent's MEMORY.md there (the fake agent's, Claude's layout)
    kn exec "$1" -- sh -c "cat /var/lib/k2/.claude/projects/*-work-$2/memory/MEMORY.md" 2>/dev/null
}
close() { curl -s --max-time 60 -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
    "http://127.0.0.1:$PORT/cli/sessions/v2/close?token=$(cat "$PRIV/token")" -d "{\"agent_name\":\"$1\",\"force\":true}"; }
replicas() { kn get statefulset k2-host -o jsonpath='{.spec.replicas}'; }
pods() { kn get pods -l app.kubernetes.io/name=k2-host --no-headers 2>/dev/null | awk '{print $1}' | sort | tr '\n' ' '; }

# 1.
ctl /cli/hosts/status > "$RUN/status-0.json"
check "the controller runs K2_PROVISIONER=kubernetes; the pool starts at 0 hosts" \
    "$(python3 -I -c 'import json,sys; s=json.load(open(sys.argv[1])); print(1 if s.get("provisioner")=="kubernetes" and s["hosts"]==[] else 0)' "$RUN/status-0.json" 2>/dev/null)" \
    "$(cat "$RUN/status-0.json") replicas $(replicas)"

if [ "${K2_P4_REAL:-0}" = 1 ]; then
    term() { # a plain shell terminal: the wake that makes the provisioner act
        curl -s --max-time 300 -o "$PRIV/spawn.out" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
            "http://127.0.0.1:$PORT/cli/sessions/v2/spawn?token=$(cat "$PRIV/token")" \
            -d "{\"agent_name\":\"$1\",\"cwd\":\"/var/lib/k2\",\"command\":\"sh\",\"args\":[]}"
        echo " $(head -c 300 "$PRIV/spawn.out")"
    }
    r=$(term t1)
    kn wait --for=condition=Ready pod -l app.kubernetes.io/name=k2-host --timeout=180s > "$RUN/wait-ready.txt" 2>&1
    ctl /cli/hosts/keys > "$RUN/keys.json"
    check "real agents image: a wake makes the provisioner create k2-host-0, enrolled by key and linked" \
        "$(echo "$r" | grep -q '^200 ' && [ "$(replicas)" = 1 ] && grep -q '"k2-host-0"' "$RUN/keys.json" && echo 1)" "wake $r; replicas $(replicas); pods $(pods)"
    ready=$(kn get pod k2-host-0 -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')
    check "the provisioned pod is Ready through k2-daemon --host-health" "$([ "$ready" = True ] && echo 1)" "Ready=$ready"
    have=$(kn exec k2-host-0 -- sh -c 'p=$(command -v claude) && test -x "$p" && echo "$p"' 2>&1)
    img=$(kn get pod k2-host-0 -o jsonpath='{.spec.containers[0].image}')
    check "claude is installed and resolvable on the host's PATH (not run)" "$(echo "$have" | grep -q '/claude$' && echo 1)" "$have in $img"
    kn logs k2-controller-0 > "$RUN/controller.log" 2>&1
    kn get statefulset,pods -o wide > "$RUN/pool.txt"
    echo "evidence: $RUN"
    if [ "$FAILS" = 0 ]; then echo "kind-pool-accept (real agents): PASS"; else echo "kind-pool-accept (real agents): $FAILS check(s) failed"; exit 1; fi
    exit 0
fi

# 2. six wakes
codes=""
for n in 1 2 3 4 5 6; do
    say "wake a$n (replicas $(replicas))"
    r=$(spawn "a$n"); codes="$codes $r" ; echo "a$n: $r" >> "$RUN/wakes.txt"
done
kn get statefulset,pods -o wide > "$RUN/pool-after-wakes.txt"
ctl /cli/hosts/keys > "$RUN/keys.json"
ctl /cli/hosts/status > "$RUN/status-6.json"
ok=$(python3 -I - "$RUN/keys.json" "$RUN/status-6.json" "$RUN/wakes.txt" <<'PY'
import json,sys
keys=json.load(open(sys.argv[1])).get("keys",[])
st=json.load(open(sys.argv[2]))
wakes=open(sys.argv[3]).read().splitlines()
names={k["host_name"] for k in keys if not k.get("revoked_ms")}
hosts={h["host_id"] for h in st["hosts"]}
want={"k2-host-0","k2-host-1","k2-host-2"}
print(1 if names>=want and hosts==want and all(" 200 " in w for w in wakes) else 0)
PY
)
check "six wakes end with 3 host pods, each enrolled by key" "$ok" \
    "replicas $(replicas); pods $(pods); linked $(python3 -I -c 'import json,sys; print([h["host_id"] for h in json.load(open(sys.argv[1]))["hosts"]])' "$RUN/status-6.json"); keys $(python3 -I -c 'import json,sys; print(sorted(k["host_name"] for k in json.load(open(sys.argv[1])).get("keys",[])))' "$RUN/keys.json"); wakes: $(tr '\n' ';' < "$RUN/wakes.txt" | cut -c1-400)"

# 3. at the maximum
r=$(spawn a7)
check "a seventh wake is refused naming K2_PROVISIONER_MAX_HOSTS" \
    "$(echo "$r" | grep -q '^503 .*K2_PROVISIONER_MAX_HOSTS=3' && echo 1)" "$r; replicas $(replicas)"

# 4. readiness by --host-health. A host links (and takes its wake) before
# its next probe runs: wait for the probes, then read the conditions.
kn wait --for=condition=Ready pod -l app.kubernetes.io/name=k2-host --timeout=180s > "$RUN/wait-ready.txt" 2>&1
ready=$(kn get pods -l app.kubernetes.io/name=k2-host -o jsonpath='{range .items[*]}{.metadata.name}={.status.conditions[?(@.type=="Ready")].status} {end}')
probe=$(kn get pod k2-host-0 -o jsonpath='{.spec.containers[0].readinessProbe.exec.command}')
check "every host pod Ready through k2-daemon --host-health (D10)" \
    "$([ "$(echo "$ready" | tr ' ' '\n' | grep -c '=True')" = 3 ] && echo "$probe" | grep -q -- --host-health && echo 1)" "$ready; probe $probe"

# 5. PDB
kn get pdb k2-host > "$RUN/pdb.txt"
allowed=$(kn get pdb k2-host -o jsonpath='{.status.disruptionsAllowed}')
check "kubectl get pdb: one disruption allowed" "$([ "$allowed" = 1 ] && echo 1)" "$(tr '\n' ' ' < "$RUN/pdb.txt")"

# 6. scale down: a3, a4 (k2-host-1) and a6 sleep; k2-host-2 keeps a5, which
# fits elsewhere: the idle window drains k2-host-2 (a5 released), then
# k2-host-1 (empty); the pool is back to MIN_HOSTS=1.
for a in a3 a4 a6; do echo "close $a: $(close $a)" >> "$RUN/sleeps.txt"; done
say "waiting for the idle window to shrink the pool (idle ${IDLE_AFTER}s, ticks every 30 s)"
for i in $(seq 1 120); do [ "$(replicas)" = 1 ] && [ "$(pods)" = "k2-host-0 " ] && break; sleep 5; done
kn get statefulset,pods,pvc -o wide > "$RUN/pool-after-idle.txt"
kn logs k2-controller-0 > "$RUN/controller.log" 2>&1
ctl /cli/hosts/status > "$RUN/status-idle.json"
a5=$(grep -E "\[hosts\] (draining k2-host-2|placed agent a5)|unplace a5|agent a5" "$RUN/controller.log" | tail -3 | tr '\n' ';')
check "after the idle window: drained highest first, back to 1 pod; the drained host's agent asleep" \
    "$([ "$(replicas)" = 1 ] && [ "$(pods)" = "k2-host-0 " ] && grep -q 'draining k2-host-2 for a scale-down: releasing 1 agent' "$RUN/controller.log" \
        && grep -q 'scaled k2-host 3 -> 2' "$RUN/controller.log" && grep -q 'scaled k2-host 2 -> 1' "$RUN/controller.log" \
        && python3 -I -c 'import json,sys; s=json.load(open(sys.argv[1])); sys.exit(0 if [h["host_id"] for h in s["hosts"]]==["k2-host-0"] else 1)' "$RUN/status-idle.json" && echo 1)" \
    "replicas $(replicas); pods $(pods); $(grep -E 'scaled k2-host|draining' "$RUN/controller.log" | sed -E 's/^[^ ]+ //' | tr '\n' ';')"
# The drained agent's home. a5 was released by k2-host-2's drain: its close
# saved a snapshot at the released epoch, committed before the scale-down.
ctl /cli/hosts/home/status > "$RUN/home-status.json"
read -r rel_epoch snap_seq snap_epoch snap_line scaled_line <<< "$(python3 -I - "$RUN/controller.log" <<'PY'
import re,sys
rel=seq=sep=snap_at=scaled_at="-"
for i,line in enumerate(open(sys.argv[1], errors="replace")):
    m=re.search(r"\[hosts\] placed agent a5 on host k2-host-2 epoch (\d+)", line)
    if m: rel=m.group(1)
    m=re.search(r"\[hosts/home\] a5: snapshot (\d+) from k2-host-2 at epoch (\d+)", line)
    if m: seq,sep,snap_at=m.group(1),m.group(2),i
    if "scaled k2-host 3 -> 2" in line and scaled_at=="-": scaled_at=i
print(rel,seq,sep,snap_at,scaled_at)
PY
)"
saved=$([ "$rel_epoch" != - ] && [ "$snap_epoch" = "$rel_epoch" ] && [ "$snap_line" != - ] && [ "$scaled_line" != - ] && [ "$snap_line" -lt "$scaled_line" ] && echo 1)
# Woken again: k2-host-2 is gone; a1 sleeps so a5 fits on k2-host-0, which
# restores the snapshot first, fetching its objects.
echo "close a1: $(close a1)" >> "$RUN/sleeps.txt"
r5=$(spawn a5 "[\"--resume\",\"$(cat "$PRIV/sessions/a5")\",\"remember a5 after the scale-down\"]")
for i in $(seq 1 60); do memory k2-host-0 a5 | grep -q "after the scale-down" && break; sleep 1; done
kn logs k2-host-0 > "$RUN/k2-host-0.log" 2>&1
kn logs k2-controller-0 > "$RUN/controller-after.log" 2>&1
mem=$(memory k2-host-0 a5)
restored=$(grep -E "\[hosts/home\] a5: restored snapshot $snap_seq \(([1-9][0-9]*) objects" "$RUN/k2-host-0.log" | tail -1)
moved=$(grep -c "placed agent a5 on host k2-host-0" "$RUN/controller-after.log")
check "home snapshot of the drained host's agents: saved at the released epoch before the scale-down, restored on another pod" \
    "$([ "$saved" = 1 ] && echo "$r5" | grep -q '^200 ' && [ "$moved" -ge 1 ] && [ -n "$restored" ] \
        && echo "$mem" | grep -q "a5 before the scale-down" && echo "$mem" | grep -q "a5 after the scale-down" && echo 1)" \
    "a5 released at epoch $rel_epoch; snapshot $snap_seq at epoch $snap_epoch (log line $snap_line, scale-down line $scaled_line); store: $(head -c 300 "$RUN/home-status.json"); wake $r5; on k2-host-0: ${restored:-no restore}; memory: $(echo "$mem" | tr '\n' '|')"

# 7. I1
i1=$(python3 -I - "$RUN/controller.log" <<'PY'
import re,sys,collections
# No agent is re-placed in this run: each was placed exactly once, on one host.
placed=collections.defaultdict(set)
for line in open(sys.argv[1], errors="replace"):
    m=re.search(r"\[hosts\] placed agent (\S+) on host (\S+)", line)
    if m:
        placed[m.group(1)].add(m.group(2))
bad={a:sorted(h) for a,h in placed.items() if len(h)!=1}
print("ok" if placed and not bad else bad or "nothing placed")
PY
)
check "I1: no agent placed on two hosts at once" "$([ "$i1" = ok ] && echo 1)" "$i1"

kn get events --sort-by=.lastTimestamp > "$RUN/events.txt" 2>&1
echo "evidence: $RUN"
if [ "$FAILS" = 0 ]; then echo "kind-pool-accept: PASS"; else echo "kind-pool-accept: $FAILS check(s) failed"; exit 1; fi
