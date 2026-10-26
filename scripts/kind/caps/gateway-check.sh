#!/usr/bin/env bash
# T3 acceptance item 4: the front door through Gateway API, TLS passthrough,
# as declared in deploy/kubernetes/core/gateway.yaml (D7 (a): Envoy Gateway
# in its own namespace, reversible).
#
#   scripts/kind/caps/gateway-check.sh [--keep] [--uninstall]
#
# 1. Envoy Gateway v1.9.2 by Helm into namespace envoy-gateway-system with
#    --skip-crds; only Envoy's own CRDs from gateway-crds-helm (Gateway API
#    CRDs disabled there), rendered and applied server-side: Helm can't
#    store that chart's release (its Secret would exceed 1 MiB). The render
#    is checked to hold no gateway.networking.k8s.io object, so k2lab's
#    Gateway API v1.6.3 CRDs are left as they are. GatewayClass k2-p4-eg. No cloud-provider-kind: the check runs
#    from inside the cluster, through the gateway's ClusterIP.
# 2. In k2-p4-caps: Gateway k2 and TLSRoute k2-front as in gateway.yaml
#    (listener TLS :443 Passthrough, hostname *.k2.test), the backend a
#    stand-in for the controller's front door: `openssl s_server` with its
#    own certificate for front.k2.test.
# 3. From a client pod: `openssl s_client -servername front.k2.test` to the
#    gateway sees the BACKEND's certificate (CN front.k2.test: the gateway
#    did not terminate TLS) and a line round-trips. SNI nope.example.org
#    gets no route.
# The k2-p4-caps objects are deleted at the end (--keep keeps them).
# --uninstall removes Envoy Gateway, its CRDs and the GatewayClass, and
# exits (step 1 undone).
set -euo pipefail
# shellcheck source=scripts/kind/caps/lib.sh
source "$(dirname "$0")/lib.sh"
keep=0 uninstall=0
for a in "$@"; do
    case "$a" in
        --keep) keep=1 ;;
        --uninstall) uninstall=1 ;;
        -h|--help) sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "gateway-check.sh: unknown argument $a" >&2; exit 2 ;;
    esac
done
caps_kubeconfig
EG_VERSION=1.9.2 EG_NS=envoy-gateway-system CLASS=k2-p4-eg
h() { helm --kubeconfig "$CAPS_KUBECONFIG" "$@"; }
eg_crds() {  # Envoy Gateway's own CRDs only, rendered (stdout)
    h template eg-crds oci://docker.io/envoyproxy/gateway-crds-helm --version "$EG_VERSION" \
        --set crds.gatewayAPI.enabled=false --set crds.envoyGateway.enabled=true 2>/dev/null \
        | python3 -I -c 'import re,sys
docs = re.split(r"(?m)^---\s*$", sys.stdin.read())
print("\n---\n".join(d.strip("\n") for d in docs if re.search(r"(?m)^kind:", d)))'
}

if [ "$uninstall" = 1 ]; then
    kc delete gatewayclass "$CLASS" --ignore-not-found >&2
    h uninstall eg -n "$EG_NS" --ignore-not-found >&2 || true
    eg_crds | kc delete --ignore-not-found -f - >&2 || true
    kc delete namespace "$EG_NS" --ignore-not-found --wait=false >&2
    say "Envoy Gateway removed (the Gateway API CRDs were never touched)"
    exit 0
fi
caps_namespace
fails=0
ok() { printf 'ok   %s\n' "$*"; }
bad() { printf 'FAIL %s\n' "$*"; fails=$((fails + 1)); }
cleanup() {
    kn delete pod gw-client --ignore-not-found --wait=false >/dev/null 2>&1 || true
    if [ "$keep" = 0 ]; then
        kn delete tlsroute k2-front --ignore-not-found >/dev/null 2>&1 || true
        kn delete gateway k2 --ignore-not-found >/dev/null 2>&1 || true
        kn delete pod front-stand-in --ignore-not-found --wait=false >/dev/null 2>&1 || true
        kn delete service front-stand-in --ignore-not-found >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

echo "== 1. Envoy Gateway $EG_VERSION in $EG_NS (Gateway API CRDs untouched)"
crd_before=$(kc get crd gateways.gateway.networking.k8s.io -o jsonpath='{.metadata.resourceVersion} {.metadata.annotations.gateway\.networking\.k8s\.io/bundle-version}')
crds=$(eg_crds)
n_eg=$(printf '%s\n' "$crds" | grep -c '^kind: CustomResourceDefinition' || true)
if printf '%s\n' "$crds" | grep -qE '^  name: [a-z.]*gateway\.networking\.k8s\.io$'; then
    bad "the rendered CRDs include gateway.networking.k8s.io objects; not applying"; exit 1
fi
printf '%s\n' "$crds" | kc apply --server-side --force-conflicts -f - >/dev/null
ok "$n_eg Envoy Gateway CRDs applied (none of Gateway API's)"
h upgrade --install eg oci://docker.io/envoyproxy/gateway-helm --version "$EG_VERSION" \
    -n "$EG_NS" --create-namespace --skip-crds --wait --timeout 10m >/dev/null
crd_after=$(kc get crd gateways.gateway.networking.k8s.io -o jsonpath='{.metadata.resourceVersion} {.metadata.annotations.gateway\.networking\.k8s\.io/bundle-version}')
[ "$crd_before" = "$crd_after" ] && ok "Gateway API CRDs unchanged ($crd_after)" || bad "Gateway API CRD changed: $crd_before -> $crd_after"
kc -n "$EG_NS" rollout status deploy/envoy-gateway --timeout=300s >&2 && ok "envoy-gateway controller ready" || bad "envoy-gateway not ready"
# kind has no LoadBalancer (and D7 adds none for this check): the class's
# Envoy Service is ClusterIP, so the Gateway gets an address and is
# Programmed. k3s keeps the default (ServiceLB).
kc apply -f - >&2 <<EOF
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: EnvoyProxy
metadata:
  name: $CLASS
  namespace: $EG_NS
  labels: {k2.dev/test: k8s-capabilities}
spec:
  provider:
    type: Kubernetes
    kubernetes:
      envoyService: {type: ClusterIP}
---
apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata:
  name: $CLASS
  labels: {k2.dev/test: k8s-capabilities}
spec:
  controllerName: gateway.envoyproxy.io/gatewayclass-controller
  parametersRef:
    group: gateway.envoyproxy.io
    kind: EnvoyProxy
    name: $CLASS
    namespace: $EG_NS
EOF
for _ in $(seq 1 60); do
    acc=$(kc get gatewayclass "$CLASS" -o jsonpath='{.status.conditions[?(@.type=="Accepted")].status}')
    [ "$acc" = True ] && break; sleep 2
done
[ "$acc" = True ] && ok "GatewayClass $CLASS accepted" || bad "GatewayClass $CLASS not accepted"

echo "== 2. Gateway k2 and TLSRoute k2-front in $CAPS_NS (gateway.yaml, passthrough)"
kn apply -f - >&2 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: front-stand-in
  labels: {k2.dev/test: k8s-capabilities, app: front-stand-in}
spec:
  terminationGracePeriodSeconds: 1
  securityContext: {runAsNonRoot: true, runAsUser: 10000, runAsGroup: 0, seccompProfile: {type: RuntimeDefault}}
  containers:
    - name: tls
      image: $CAPS_PROBE_IMAGE
      imagePullPolicy: Never
      command: [sh, -c]
      args:
        - |
          cd /tmp
          openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 1 \
            -subj /CN=front.k2.test -addext subjectAltName=DNS:front.k2.test -keyout k.pem -out c.pem 2>/dev/null
          exec openssl s_server -quiet -accept 8443 -cert c.pem -key k.pem -rev
      ports: [{containerPort: 8443}]
      resources: {requests: {cpu: 10m, memory: 16Mi}, limits: {memory: 64Mi}}
      securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
---
apiVersion: v1
kind: Service
metadata:
  name: front-stand-in
  labels: {k2.dev/test: k8s-capabilities}
spec:
  selector: {app: front-stand-in}
  ports: [{port: 443, targetPort: 8443}]
---
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: k2
  labels: {k2.dev/test: k8s-capabilities}
spec:
  gatewayClassName: $CLASS
  listeners:
    - name: k2-tls
      protocol: TLS
      port: 443
      hostname: "*.k2.test"
      tls:
        mode: Passthrough
      allowedRoutes:
        kinds: [{kind: TLSRoute}]
---
apiVersion: gateway.networking.k8s.io/v1
kind: TLSRoute
metadata:
  name: k2-front
  labels: {k2.dev/test: k8s-capabilities}
spec:
  parentRefs:
    - name: k2
      sectionName: k2-tls
  hostnames:
    - "*.k2.test"
  rules:
    - backendRefs:
        - name: front-stand-in
          port: 443
EOF
kn wait --for=condition=Ready pod/front-stand-in --timeout=120s >/dev/null
for _ in $(seq 1 90); do
    prog=$(kn get gateway k2 -o jsonpath='{.status.conditions[?(@.type=="Programmed")].status}')
    [ "$prog" = True ] && break; sleep 2
done
[ "$prog" = True ] && ok "Gateway k2 programmed" || bad "Gateway k2 not programmed: $(kn get gateway k2 -o jsonpath='{.status.conditions}' | cut -c1-300)"
rt=$(kn get tlsroute k2-front -o jsonpath='{.status.parents[0].conditions[?(@.type=="Accepted")].status}')
[ "$rt" = True ] && ok "TLSRoute k2-front accepted" || bad "TLSRoute k2-front not accepted"
gw_svc=$(kc -n "$EG_NS" get svc -l "gateway.envoyproxy.io/owning-gateway-name=k2,gateway.envoyproxy.io/owning-gateway-namespace=$CAPS_NS" -o jsonpath='{.items[0].metadata.name}')
[ -n "$gw_svc" ] && ok "gateway Service $EG_NS/$gw_svc" || bad "no Service for gateway k2"

echo "== 3. passthrough from a client pod"
kn apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: gw-client
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
kn wait --for=condition=Ready pod/gw-client --timeout=120s >/dev/null
target="$gw_svc.$EG_NS.svc.cluster.local:443"
tls() {  # tls <sni>: subject seen and the reply to "k2 front door"
    kn exec gw-client -- sh -c 'printf "k2 front door\n" | timeout 8 openssl s_client -connect "$1" -servername "$2" -quiet -verify_quiet 2>/dev/null | head -1; \
        echo | timeout 8 openssl s_client -connect "$1" -servername "$2" 2>/dev/null | openssl x509 -noout -subject 2>/dev/null' _ "$target" "$1" 2>/dev/null || true
}
out=""
for _ in $(seq 1 15); do out=$(tls front.k2.test); case "$out" in *front.k2.test*) break ;; esac; sleep 2; done
reply=$(printf '%s\n' "$out" | head -1) subject=$(printf '%s\n' "$out" | grep -i subject || true)
case "$subject" in *CN*=*front.k2.test*) ok "SNI front.k2.test through the gateway: the backend's own certificate ($subject)" ;; *) bad "SNI front.k2.test: certificate subject '${subject:-none}'" ;; esac
[ "$reply" = "rood tnorf 2k" ] && ok "a line round-trips through the TLS stream (s_server -rev answered '$reply')" || bad "round trip: '${reply:-nothing}'"
other=$(tls nope.example.org)
case "$other" in *front.k2.test*) bad "SNI nope.example.org reached the backend" ;; *) ok "SNI nope.example.org: no route (nothing reached the backend)" ;; esac

echo
if [ "$fails" = 0 ]; then echo "PASS gateway: TLS passthrough through Gateway API (Envoy Gateway $EG_VERSION) to the front door stand-in"; else echo "FAIL gateway: $fails check(s)"; exit 1; fi
