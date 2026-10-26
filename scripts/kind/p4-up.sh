#!/usr/bin/env bash
# TEST ONLY (P4 T4): the host pool on kind, namespace k2-kind-p4, port 7444.
#
#   scripts/kind/p4-up.sh [--daemon-image=k2-daemon:rc1] [--fake-src=../rc1-src]
#
# 1. tags --daemon-image (a build with the pool and home sync: wave3 rc1
#    or later) as k2-daemon:p4; nothing is compiled here except the fake
#    agent (deploy/kind/Dockerfile.fake, from --fake-src's
#    tests/fakes/k2-fake-agent, under the Docker lock and a cargo slot);
# 2. up.sh --p4 (deploy/kind/p4: no shared k2-work, hosts on different
#    worker nodes, the end state's PDB) with the pool
#    (K2_PROVISIONER=kubernetes, MIN = MAX = 2), signed enrollment,
#    K2_PTYD=1 and home sync (K2_WORK_ROOT=/var/lib/k2/work);
# 3. checks: two hosts Running on two different nodes, each linked by its
#    key, with its own data and work volumes; the PDB allows one
#    disruption.
# The app login's password, when up.sh sets one, goes only to
# $KIND_STATE/owner-password (0600); it is never printed.
#
# Only k2lab, through an explicit kubeconfig; only namespace k2-kind-p4.
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
daemon_image=k2-daemon:rc1 fake_src="$KIND_REPO/../rc1-src"
for a in "$@"; do
    case "$a" in
        --daemon-image=*) daemon_image=${a#--daemon-image=} ;;
        --fake-src=*) fake_src=${a#--fake-src=} ;;
        -h|--help) sed -n '2,21p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "p4-up.sh: unknown argument $a" >&2; exit 2 ;;
    esac
done
kind_variant p4
kind_kubeconfig
[ "$KIND_NS" = k2-kind-p4 ] || { echo "p4-up.sh: refusing namespace $KIND_NS" >&2; exit 1; }
docker image inspect "$daemon_image" >/dev/null 2>&1 || { echo "p4-up.sh: no image $daemon_image" >&2; exit 1; }
[ -d "$fake_src/tests/fakes/k2-fake-agent" ] || { echo "p4-up.sh: no tests/fakes/k2-fake-agent under $fake_src" >&2; exit 1; }
docker tag "$daemon_image" k2-daemon:p4
say "k2-daemon:p4 = $daemon_image ($(docker image inspect -f '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$daemon_image" | cut -c1-9))"
K2_KIND_FAKE_SRC=$fake_src "$KIND_REPO/scripts/kind/up.sh" --p4 --hosts=2 --max-hosts=2 --host-auth=enroll \
    --ptyd=1 --provisioner=kubernetes --work-root=/var/lib/k2/work --password-file="$KIND_STATE/owner-password"

kn() { kubectl --kubeconfig "$KIND_KUBECONFIG" -n "$KIND_NS" "$@"; }
fails=0
ok() { printf 'ok   %s\n' "$*"; }
bad() { printf 'FAIL %s\n' "$*"; fails=$((fails + 1)); }
n0=$(kn get pod k2-host-0 -o jsonpath='{.spec.nodeName}')
n1=$(kn get pod k2-host-1 -o jsonpath='{.spec.nodeName}')
[ -n "$n0" ] && [ -n "$n1" ] && [ "$n0" != "$n1" ] && ok "k2-host-0 on $n0, k2-host-1 on $n1" || bad "hosts on '$n0' and '$n1'"
for h in k2-host-0 k2-host-1; do
    kn exec "$h" -c k2-host -- sh -c 'curl -fsS --max-time 3 http://127.0.0.1:7420/cli/hosts/status | grep -q "\"linked\":true"' \
        && ok "$h linked" || bad "$h not linked"
    [ "$(kn exec "$h" -c k2-host -- stat -c %a /var/lib/k2-host/.k2/host-key.pem 2>/dev/null)" = 600 ] \
        && ok "$h: its key, 0600" || bad "$h: no key (0600)"
    for c in data work; do
        kn get pvc "$c-$h" >/dev/null 2>&1 && ok "$h: its own $c volume" || bad "$h: no $c-$h"
    done
done
kn get pvc k2-work >/dev/null 2>&1 && bad "a shared k2-work exists" || ok "no shared k2-work"
[ "$(kn get pdb k2-host -o jsonpath='{.status.disruptionsAllowed}')" = 1 ] && ok "PDB k2-host allows one disruption" || bad "PDB k2-host: $(kn get pdb k2-host -o jsonpath='{.status.disruptionsAllowed}') allowed"
echo
[ "$fails" = 0 ] && echo "PASS p4-up: k2-kind-p4 on 127.0.0.1:$KIND_PORT" || { echo "FAIL p4-up: $fails check(s)"; exit 1; }
