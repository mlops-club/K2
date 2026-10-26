#!/usr/bin/env bash
# T3 acceptance item 1: does k2lab's CNI (kindnet) enforce NetworkPolicy?
#
#   scripts/kind/caps/netpol-check.sh [--keep]
#
# In namespace k2-p4-caps: a tiny HTTP server pod and three clients:
# "allowed" on another node, "denied" on the server's node, "denied-x" on
# another node (pod affinity), so both same-node and cross-node traffic is
# tested.
#   1. baseline: both clients reach the server (so a later failure means
#      the policy, not the network);
#   2. default-deny ingress for the server plus an allow rule for the
#      "allowed" client: allowed reaches it, denied times out;
#   3. the policies removed: both reach it again (enforcement follows the
#      objects, both ways).
# Exits 0 only when all three hold. If step 2's denied client gets through,
# kindnet does not enforce NetworkPolicy here: FAIL, and nothing is changed
# on the cluster to work around it. The pods are deleted at the end
# (--keep keeps them).
set -euo pipefail
# shellcheck source=scripts/kind/caps/lib.sh
source "$(dirname "$0")/lib.sh"
keep=0
for a in "$@"; do
    case "$a" in
        --keep) keep=1 ;;
        -h|--help) sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "netpol-check.sh: unknown argument $a" >&2; exit 2 ;;
    esac
done
caps_kubeconfig
caps_namespace
fails=0
ok() { printf 'ok   %s\n' "$*"; }
bad() { printf 'FAIL %s\n' "$*"; fails=$((fails + 1)); }

pod() {  # pod <name> <role> <same|other|any: node vs the server> <command...>
    local name=$1 role=$2 where=$3; shift 3
    python3 -I - "$name" "$role" "$where" "$CAPS_PROBE_IMAGE" "$@" <<'PY' | kn apply -f - >&2
import json, sys
name, role, where, image, cmd = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5:]
term = [{"labelSelector": {"matchLabels": {"role": "server"}}, "topologyKey": "kubernetes.io/hostname"}]
affinity = {"same": {"podAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": term}},
            "other": {"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": term}}}.get(where, {})
print(json.dumps({"apiVersion": "v1", "kind": "Pod",
    "metadata": {"name": name, "labels": {"k2.dev/test": "netpol", "role": role}},
    "spec": {"terminationGracePeriodSeconds": 1, "restartPolicy": "Always", "affinity": affinity,
             "securityContext": {"runAsNonRoot": True, "runAsUser": 10000, "runAsGroup": 0,
                                 "seccompProfile": {"type": "RuntimeDefault"}},
             "containers": [{"name": "c", "image": image, "imagePullPolicy": "Never", "command": cmd,
                             "resources": {"requests": {"cpu": "10m", "memory": "16Mi"}, "limits": {"memory": "64Mi"}},
                             "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}}}]}}))
PY
}
cleanup() {
    kn delete networkpolicy -l k2.dev/test=netpol --ignore-not-found >/dev/null 2>&1 || true
    [ "$keep" = 1 ] || kn delete pod -l k2.dev/test=netpol --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

say "pods in $CAPS_NS (image $CAPS_PROBE_IMAGE)"
pod netpol-server server any perl -MIO::Socket::INET -e '
    $| = 1; my $s = IO::Socket::INET->new(LocalPort => 8080, Listen => 16, ReuseAddr => 1) or die "listen: $!";
    while (my $c = $s->accept) { my $l = <$c>; print $c "HTTP/1.0 200 OK\r\nContent-Length: 3\r\n\r\nok\n"; close $c }'
kn wait --for=condition=Ready pod/netpol-server --timeout=120s >/dev/null
pod netpol-allowed allowed other sleep 3600
pod netpol-denied denied same sleep 3600
pod netpol-denied-x denied other sleep 3600
kn wait --for=condition=Ready pod -l k2.dev/test=netpol --timeout=120s >/dev/null
ip=$(kn get pod netpol-server -o jsonpath='{.status.podIP}')
for p in netpol-server netpol-allowed netpol-denied netpol-denied-x; do
    printf '     %-15s on %s\n' "$p" "$(kn get pod "$p" -o jsonpath='{.spec.nodeName}')"
done

probe() {  # probe <client pod> → "reach" or "blocked"
    if kn exec "$1" -- curl -fsS --max-time 4 "http://$ip:8080/" >/dev/null 2>&1; then echo reach; else echo blocked; fi
}
expect() {  # expect <what> <client> <reach|blocked> [tries]
    local got i
    for i in $(seq 1 "${4:-5}"); do
        got=$(probe "$2")
        [ "$got" = "$3" ] && { ok "$1: $2 -> server $got"; return 0; }
        sleep 2
    done
    bad "$1: $2 -> server $got (expected $3)"
}

echo "== 1. baseline, no policy"
expect baseline netpol-allowed reach
expect baseline netpol-denied reach
expect baseline netpol-denied-x reach

echo "== 2. default-deny ingress for the server, allow role=allowed"
kn apply -f - >&2 <<'EOF'
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: netpol-server-deny
  labels: {k2.dev/test: netpol}
spec:
  podSelector: {matchLabels: {role: server}}
  policyTypes: [Ingress]
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: netpol-server-allow
  labels: {k2.dev/test: netpol}
spec:
  podSelector: {matchLabels: {role: server}}
  policyTypes: [Ingress]
  ingress:
    - from: [{podSelector: {matchLabels: {role: allowed}}}]
      ports: [{port: 8080}]
EOF
expect "with policy" netpol-denied blocked 10
expect "with policy" netpol-denied-x blocked 10
expect "with policy" netpol-allowed reach 10

echo "== 3. policies removed"
kn delete networkpolicy -l k2.dev/test=netpol >/dev/null
expect "after removal" netpol-denied reach 10
expect "after removal" netpol-denied-x reach 10
expect "after removal" netpol-allowed reach

echo
if [ "$fails" = 0 ]; then
    echo "PASS netpol: kindnet enforces NetworkPolicy on $CAPS_CLUSTER, same node and across nodes (deny, allow, and removal)"
else
    echo "FAIL netpol: $fails check(s); see above. If the denied client got through, kindnet does not enforce NetworkPolicy here."
    exit 1
fi
