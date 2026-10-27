#!/usr/bin/env bash
# TEST ONLY. Signed host enrollment on kind (k2-kind-dist-fake, deployed with
# `scripts/kind/up.sh --dist --fake --hosts=N --host-auth=enroll`):
#
#   scripts/kind/enroll-check.sh [--host=k2-host-1] [--no-restore]
#
# 1. every host is enrolled and linked; its key is $HOME/.k2/host-key.pem
#    on its own volume, mode 0600;
# 2. delete <host>'s pod: it relinks by its key with no new token (the
#    Secret k2-host-enroll is unchanged, same key fingerprint, no new
#    ctl.host_enrolled in the controller's trace, a new ctl.host_admitted);
# 3. revoke <host> (POST /cli/hosts/keys/revoke): its live link is cut and
#    it is refused (revoked_host_key in its /cli/hosts/status last_error),
#    also after its pod restarts;
# 4. unless --no-restore: <host> gets a new key and a new one-time token
#    and enrolls again, so the fleet is whole for the next run.
#
# Prints key fingerprints, never a token.
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
host=k2-host-1 restore=1
for a in "$@"; do
    case "$a" in
        --host=*) host=${a#--host=} ;;
        --no-restore) restore=0 ;;
        -h|--help) sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "enroll-check.sh: unknown argument $a (see --help)" >&2; exit 2 ;;
    esac
done
kind_variant dist-fake
kind_kubeconfig
cd "$KIND_REPO"
base="http://127.0.0.1:$KIND_PORT"
fails=0
ok() { printf 'ok   %s\n' "$*"; }
bad() { printf 'FAIL %s\n' "$*"; fails=$((fails + 1)); }
owner() { kc -n "$KIND_NS" exec k2-controller-0 -c k2-daemon -- cat /var/lib/k2/.k2/daemon.token; }
enroll() { owner | python3 -I scripts/kind/enroll.py "$@" --url "$base"; }
hosts=$(kc -n "$KIND_NS" get pods -l app.kubernetes.io/name=k2-host -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort -t- -k3 -n | paste -sd, -)
auth=$(kc -n "$KIND_NS" get statefulset k2-controller \
    -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="K2_HOST_AUTH")].value}')
[ "$auth" = enroll ] || { echo "enroll-check.sh: $KIND_NS runs K2_HOST_AUTH=${auth:-token}; deploy with up.sh --dist --fake --host-auth=enroll" >&2; exit 1; }
case ",$hosts," in *",$host,"*) ;; *) echo "enroll-check.sh: no pod $host (hosts: $hosts)" >&2; exit 1 ;; esac

host_status() {  # host_status <pod> → the host's own /cli/hosts/status (no token)
    kc -n "$KIND_NS" exec "$1" -c k2-host -- curl -sS --max-time 3 http://127.0.0.1:7420/cli/hosts/status 2>/dev/null || echo '{}'
}
field() { python3 -I -c 'import json,sys; d=json.loads(sys.argv[1] or "{}"); v=d.get(sys.argv[2]); print("" if v is None else v)' "$1" "$2"; }
fingerprint() {  # fingerprint <host> → its key's fingerprint on the controller ("" if none)
    enroll keys | awk -v h="$1" '$1 == h && $0 !~ /REVOKED/ {print $2}'
}
ctl_events() {  # ctl_events <event> <host> → count in the controller's trace
    kc -n "$KIND_NS" exec k2-controller-0 -c k2-daemon -- sh -c \
        'grep "\"event\":\"$1\"" "$HOME/.k2-lab/traces/daemon.jsonl" 2>/dev/null | grep -c "\"$2\"" || true' _ "$1" "$2"
}
wait_until() {  # wait_until <seconds> <command...>
    local end=$((SECONDS + $1)); shift
    until "$@"; do [ $SECONDS -lt $end ] || return 1; sleep 1; done
}
linked() { [ "$(field "$(host_status "$1")" linked)" = True ]; }
refused_revoked() { case "$(field "$(host_status "$1")" last_error)" in *revoked_host_key*) return 0 ;; esac; return 1; }

# ── 1. enrolled ─────────────────────────────────────────────────────────
echo "== 1. every host enrolled, linked, key on its own volume (0600)"
enroll keys
for h in ${hosts//,/ }; do
    fp=$(fingerprint "$h")
    [ -n "$fp" ] && ok "$h enrolled, key $fp" || bad "$h has no active key on the controller"
    linked "$h" && ok "$h linked" || bad "$h not linked: $(field "$(host_status "$h")" last_error)"
    mode=$(kc -n "$KIND_NS" exec "$h" -c k2-host -- stat -c '%a %U' /var/lib/k2-host/.k2/host-key.pem 2>/dev/null || echo missing)
    [ "${mode%% *}" = 600 ] && ok "$h key file /var/lib/k2-host/.k2/host-key.pem mode $mode" || bad "$h key file: $mode"
done

# ── 2. pod restart: relinks by key, no new token ───────────────────────
echo "== 2. delete pod $host: it relinks by its key, with no new token"
fp_before=$(fingerprint "$host")
secret_rv=$(kc -n "$KIND_NS" get secret k2-host-enroll -o jsonpath='{.metadata.resourceVersion}')
enrolled_before=$(ctl_events ctl.host_enrolled "$host")
admitted_before=$(ctl_events ctl.host_admitted "$host")
kc -n "$KIND_NS" delete pod "$host" --wait=true >/dev/null
kc -n "$KIND_NS" wait --for=condition=Ready "pod/$host" --timeout=180s >/dev/null \
    && ok "$host back and ready (linked)" || bad "$host not ready again"
[ "$(kc -n "$KIND_NS" get secret k2-host-enroll -o jsonpath='{.metadata.resourceVersion}')" = "$secret_rv" ] \
    && ok "Secret k2-host-enroll unchanged (resourceVersion $secret_rv): no new token" || bad "the Secret changed"
fp_after=$(fingerprint "$host")
[ -n "$fp_after" ] && [ "$fp_after" = "$fp_before" ] && ok "same key $fp_after" || bad "key $fp_before -> $fp_after"
[ "$(ctl_events ctl.host_enrolled "$host")" = "$enrolled_before" ] \
    && ok "no new ctl.host_enrolled for $host ($enrolled_before before and after)" || bad "a new enrollment was traced"
admitted_after=$(ctl_events ctl.host_admitted "$host")
[ "$admitted_after" -gt "$admitted_before" ] \
    && ok "ctl.host_admitted for $host: $admitted_before -> $admitted_after (signed hello)" || bad "no new ctl.host_admitted"

# ── 3. revoke: refused ──────────────────────────────────────────────────
echo "== 3. revoke $host: refused, also after a restart"
enroll revoke --host "$host" --reason "kind enroll-check"
wait_until 30 refused_revoked "$host" \
    && ok "$host refused: $(field "$(host_status "$host")" last_error | cut -c1-120)" || bad "$host not refused after revoke"
linked "$host" && bad "$host still linked" || ok "$host not linked"
kc -n "$KIND_NS" delete pod "$host" --wait=true >/dev/null
kc -n "$KIND_NS" wait --for=jsonpath='{.status.phase}'=Running "pod/$host" --timeout=180s >/dev/null || true
wait_until 60 refused_revoked "$host" \
    && ok "after a pod restart, $host is still refused: $(field "$(host_status "$host")" last_error | cut -c1-120)" \
    || bad "$host not refused after its restart"
linked "$host" && bad "$host linked after its restart" || ok "$host not linked after its restart"

# ── 4. restore ──────────────────────────────────────────────────────────
if [ "$restore" = 1 ]; then
    echo "== 4. restore $host: a new key and a new one-time token"
    kc -n "$KIND_NS" exec "$host" -c k2-host -- rm -f /var/lib/k2-host/.k2/host-key.pem
    owner | python3 -I scripts/kind/enroll.py mint --kubeconfig "$KIND_KUBECONFIG" --namespace "$KIND_NS" \
        --url "$base" --hosts "$hosts" --renew "$host" --deploy-label kind-dist-fake
    kc -n "$KIND_NS" delete pod "$host" --wait=true >/dev/null
    kc -n "$KIND_NS" wait --for=condition=Ready "pod/$host" --timeout=180s >/dev/null \
        && ok "$host enrolled again with a new key $(fingerprint "$host") and linked" || bad "$host did not enroll again"
fi

echo
if [ "$fails" = 0 ]; then echo "PASS enroll-check ($hosts; $host relinked by key, then revoked and refused)"; else echo "FAIL enroll-check: $fails check(s)"; exit 1; fi
