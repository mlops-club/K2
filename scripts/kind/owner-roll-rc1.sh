#!/usr/bin/env bash
# The owner's second roll: k2-kind-dist (port 7443) from wave3 853157fee
# onto the release candidate k2-dist/wave3 c59acf7e0 (images :rc1, built and
# verified by dist-link), with home sync (K2_WORK_ROOT on the hosts, P3 T1)
# and turn-end snapshots (P3 T2), and a pool that may grow to two hosts.
#
#   scripts/kind/owner-roll-rc1.sh [--dry-run]     (the default)
#   scripts/kind/owner-roll-rc1.sh --go [--skip-rehearsal]
#
# --dry-run changes nothing anywhere: no scale, no token, no tag, no apply.
# It
#   1. snapshots k2-kind-dist read-only (as owner-roll-wave3.sh: pods,
#      PVC uids, the host's Claude login as present/size/mtime/onboarding,
#      never its contents), and the images the pods run (the StatefulSets'
#      k2.dev/image-id), which are the rollback images;
#   2. renders the roll (up.sh --render-only: 1 host, enroll, K2_PTYD=1,
#      K2_PROVISIONER=kubernetes with MIN=1 MAX=2, K2_WORK_ROOT=/var/lib/k2/work,
#      --login-from=k2-host-0);
#   3. checks the render:
#      (a) k2-host keeps its name, serviceName and claim template, so
#          k2-host-0 mounts data-k2-host-0 (the owner's Claude login) again;
#      (b) the pool: MIN_HOSTS=1, MAX_HOSTS=2, replicas 1;
#      (c) the NodePort (30743) and the controller's claim are unchanged;
#          the proxy answers on 7443; every object is in k2-kind-dist;
#      (d) K2_WORK_ROOT=/var/lib/k2/work on k2-host, where k2-work is mounted;
#      (e) the login copy: init container login-seed mounts data-k2-host-0
#          read-only on its own mount only (a pod-level readOnly would make
#          k2-host-0's own HOME read-only), and nothing else mounts it;
#   4. kubectl diff of the render against the live namespace;
#   5. whether the :rc1 daemon image exists and is c59acf7e0's build.
#
# How the login reaches k2-host-1: the init container login-seed (up.sh
# --login-from=k2-host-0). When a host other than k2-host-0 starts with no
# .claude/.credentials.json of its own, it copies k2-host-0's (0600) and
# .claude.json from data-k2-host-0, mounted read-only, onto its own volume.
# Inside the cluster only: the volumes are on the controller's node (the
# hosts' podAffinity), nothing is read out, printed or put in a Secret. A
# host that has a login keeps it (its own refreshes stand). Home sync can't
# carry it: its secret scrub leaves credentials out on purpose.
#
# --go (only when the coordinator says so) also
#   6. tags the running images (by id) k2-daemon:dist-wave3 and
#      k2-agents:dist-wave3, the rollback;
#   7. rehearses on k2-kind-dist-fake: k2-daemon:rc1 as :dist-fake, up.sh
#      with the same flags; a dummy login (not a credential) on its
#      k2-host-0 when it has none; the second host (below) gets it; then
#      enroll-check (k2-host-1), app-check, reconnect-check. A failed
#      rehearsal stops here, with k2-kind-dist untouched;
#   8. backs up the controller's volume in the cluster (backup-controller.sh);
#   9. rolls k2-kind-dist: k2-daemon:rc1 as :dist, up.sh with the flags
#      above (never --reset-password);
#  10. checks: the PVCs from before keep their uids; the login file is
#      unchanged (size, mtime) with onboarding done; k2-host-0 linked by its
#      key (0600) with K2_WORK_ROOT; app-check, reconnect-check
#      --restart-live, owner-proof.py on k2-host-0 (one short prompt);
#      then the second host: a token for k2-host-1 (enroll.py mint, as the
#      pool does before it scales), k2-host at 2, k2-host-1 linked, its
#      login-seed log "copied", its login file present (0600) with
#      onboarding done, and back to 1 replica (its volume, key and login
#      stay for the pool's next scale-up).
# Rollback (automatic if a check in 9-10 fails): k2-host to 1, the
# -wave3 images back to :dist, backup-controller.sh --restore, up.sh with
# wave3's flags (MIN = MAX = 1, no K2_WORK_ROOT, no login-seed), and
# data-k2-host-1 deleted when this roll made it (it holds a copy of the
# login); 7443 and app-check again.
#
# Only k2lab, through an explicit kubeconfig; never the current context. No
# credential is read out of a pod or printed: files are checked in place.
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
mode=dry-run rehearse=1
for a in "$@"; do
    case "$a" in
        --dry-run) mode=dry-run ;;
        --go) mode=go ;;
        --skip-rehearsal) rehearse=0 ;;
        -h|--help) sed -n '2,68p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "owner-roll-rc1.sh: unknown argument $a" >&2; exit 2 ;;
    esac
done
kind_variant dist
kind_kubeconfig
NS=$KIND_NS PORT=$KIND_PORT
ROLL_FLAGS=(--dist --host-auth=enroll --ptyd=1 --provisioner=kubernetes --max-hosts=2
    --work-root=/var/lib/k2/work --login-from=k2-host-0)
PREV_FLAGS=(--dist --host-auth=enroll --ptyd=1 --provisioner=kubernetes)   # wave3's roll
RC_COMMIT=${RC_COMMIT:-c59acf7e02af4b2237e175546f6b1f7c76073068}   # k2-dist/wave3, the release candidate
RC_DAEMON=${RC_DAEMON:-k2-daemon:rc1}                             # dist-link's verified build
OUT="$KIND_STATE/owner-roll-rc1"
mkdir -p "$OUT" && chmod 700 "$OUT"
kd() { kubectl --kubeconfig "$KIND_KUBECONFIG" -n "$NS" "$@"; }
fails=0
ok() { printf 'ok   %s\n' "$*"; }
bad() { printf 'FAIL %s\n' "$*"; fails=$((fails + 1)); }
note() { printf 'note %s\n' "$*"; }

snapshot() {  # snapshot <file>: read-only facts, no file contents
    {
        echo "# pods"
        kd get pods -o custom-columns=NAME:.metadata.name,UID:.metadata.uid,IMAGE:.status.containerStatuses[0].imageID --no-headers
        echo "# pvcs"
        # The deployment's own PVCs (a roll adds its k2-backup-controller-* one).
        kd get pvc -o custom-columns=NAME:.metadata.name,UID:.metadata.uid,SIZE:.spec.resources.requests.storage --no-headers \
            | grep -v '^k2-backup-' | awk '{print $1, $2, $3}'   # widths follow the longest name
        echo "# statefulsets"
        kd get statefulset -o custom-columns=NAME:.metadata.name,REPLICAS:.spec.replicas,SERVICE:.spec.serviceName --no-headers
        echo "# host login (present size mtime; onboarding flag)"
        kd exec k2-host-0 -c k2-host -- sh -c '
            for f in /var/lib/k2-host/.claude/.credentials.json /var/lib/k2-host/.claude.json; do
                if [ -s "$f" ]; then echo "$f present $(stat -c "%s %Y" "$f")"; else echo "$f MISSING"; fi
            done
            grep -q "\"hasCompletedOnboarding\": *true" /var/lib/k2-host/.claude.json 2>/dev/null && echo "onboarding done" || echo "onboarding NOT done"' 2>&1
        echo "# controller state"
        kd exec k2-controller-0 -c k2-daemon -- sh -c '
            for f in connect-users.json connect-sessions.json hosts; do
                [ -e "/var/lib/k2/.k2/$f" ] && echo "$f present" || echo "$f MISSING"
            done' 2>&1
        echo "# secrets"
        kd get secret k2-host-enroll >/dev/null 2>&1 && echo "k2-host-enroll present" || echo "k2-host-enroll absent"
        echo "# images (k2.dev/image-id)"
        for sts in k2-controller k2-host; do
            echo "$sts $(kd get statefulset "$sts" -o jsonpath='{.spec.template.metadata.annotations.k2\.dev/image-id}')"
        done
    } > "$1"
}

echo "== 1. before-snapshot of $NS (read-only)"
snapshot "$OUT/before.txt"
sed 's/^/     /' "$OUT/before.txt"
grep -q "credentials.json present" "$OUT/before.txt" && ok "the host's Claude login is present" || bad "no Claude login on k2-host-0"
grep -q "onboarding done" "$OUT/before.txt" && ok "onboarding done" || bad "onboarding not done"

echo "== 2. render the roll"
"$KIND_REPO/scripts/kind/up.sh" "${ROLL_FLAGS[@]}" --render-only="$OUT/render.yaml" 2>&1 | sed 's/^/     /'

echo "== 3. the hard requirements on the render"
python3 -I - "$OUT/render.yaml" "$KIND_KUBECONFIG" "$NS" <<'PY' > "$OUT/requirements.txt"
import json, subprocess, sys
render, kubeconfig, ns = sys.argv[1:4]
text = subprocess.run(["kubectl", "--kubeconfig", kubeconfig, "create", "--dry-run=client", "-o", "json", "-f", render],
                      capture_output=True, text=True, check=True).stdout
docs, i, dec = [], 0, json.JSONDecoder()
while i < len(text):              # one JSON object per document, back to back
    if text[i].isspace():
        i += 1
        continue
    d, i = dec.raw_decode(text, i)
    docs.extend(d["items"] if d.get("kind") == "List" else [d])
def find(kind, name):
    return next(d for d in docs if d["kind"] == kind and d["metadata"]["name"] == name)
live = json.loads(subprocess.run(["kubectl", "--kubeconfig", kubeconfig, "-n", ns, "get", "statefulset", "k2-host", "-o", "json"],
                                 capture_output=True, text=True, check=True).stdout)
host = find("StatefulSet", "k2-host")
def claims(s):
    return [(c["metadata"]["name"], c["spec"].get("accessModes"), c["spec"]["resources"]["requests"]["storage"]) for c in s["spec"]["volumeClaimTemplates"]]
def mount(s):
    c = next(c for c in s["spec"]["template"]["spec"]["containers"] if c["name"] == "k2-host")
    return [m["mountPath"] for m in c["volumeMounts"] if m["name"] == "data"]
same = claims(host) == claims(live) and host["spec"]["serviceName"] == live["spec"]["serviceName"] and mount(host) == mount(live)
print(("ok  " if same else "FAIL") + " (a) k2-host keeps its claim template %s, serviceName %s and mount %s: k2-host-0 reuses data-k2-host-0" % (claims(host), host["spec"]["serviceName"], mount(host)))
ctl = find("StatefulSet", "k2-controller")
env = {e["name"]: e.get("value") for c in ctl["spec"]["template"]["spec"]["containers"] for e in c.get("env", [])}
floor = env.get("K2_PROVISIONER") == "kubernetes" and env.get("K2_PROVISIONER_MIN_HOSTS") == "1" and env.get("K2_PROVISIONER_MAX_HOSTS") == "2" and host["spec"]["replicas"] == 1
print(("ok  " if floor else "FAIL") + " (b) K2_PROVISIONER=%s, MIN_HOSTS=%s, MAX_HOSTS=%s; k2-host replicas %s" % (
    env.get("K2_PROVISIONER"), env.get("K2_PROVISIONER_MIN_HOSTS"), env.get("K2_PROVISIONER_MAX_HOSTS"), host["spec"]["replicas"]))
stray = ["%s/%s" % (d["kind"], d["metadata"]["name"]) for d in docs if d["kind"] != "Namespace" and d["metadata"].get("namespace") != ns]
print(("ok  " if not stray else "FAIL") + " every object is in %s (an object without one lands in the kubeconfig's default namespace)%s" % (
    ns, "" if not stray else ": " + ", ".join(stray)))
np = find("Service", "k2-controller-nodeport")
ports = [p.get("nodePort") for p in np["spec"]["ports"]]
claims_ctl = claims(ctl)
print(("ok  " if ports == [30743] else "FAIL") + " (c) the proxy's NodePort stays %s; the controller keeps claim %s (the app's login)" % (ports, claims_ctl))
hspec = host["spec"]["template"]["spec"]
hc = next(c for c in hspec["containers"] if c["name"] == "k2-host")
henv = {e["name"]: e.get("value") for e in hc.get("env", [])}
work = [m["mountPath"] for m in hc["volumeMounts"] if m["name"] == "work"]
wr = henv.get("K2_WORK_ROOT") == "/var/lib/k2/work" and work == ["/var/lib/k2/work"]
print(("ok  " if wr else "FAIL") + " (d) K2_WORK_ROOT=%s on k2-host; k2-work mounted at %s" % (henv.get("K2_WORK_ROOT"), work))
vols = {v["name"]: v for v in hspec.get("volumes", [])}
src = vols.get("login-src", {}).get("persistentVolumeClaim", {})
inits = hspec.get("initContainers", [])
seed = next((c for c in inits if c["name"] == "login-seed"), None)
seed_mount = [m for m in (seed or {}).get("volumeMounts", []) if m["name"] == "login-src"]
others = [c["name"] for c in hspec["containers"] + inits if c["name"] != "login-seed" and any(m["name"] == "login-src" for m in c.get("volumeMounts", []))]
login = (src.get("claimName") == "data-k2-host-0" and not src.get("readOnly") and seed is not None
         and [m.get("readOnly") for m in seed_mount] == [True] and not others)
print(("ok  " if login else "FAIL") + " (e) login-seed mounts %s read-only on its own mount (pod-level readOnly %s); other mounts of it: %s" % (
    src.get("claimName"), bool(src.get("readOnly")), others or "none"))
PY
cat "$OUT/requirements.txt"
fails=$((fails + $(grep -c '^FAIL' "$OUT/requirements.txt" || true)))
c=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:$PORT/boot-status" || true)
[ "$c" = 200 ] && ok "(c) the proxy answers on 127.0.0.1:$PORT now" || bad "(c) 127.0.0.1:$PORT -> ${c:-no answer}"

echo "== 4. kubectl diff against $NS (server-side dry run)"
set +e
kd diff -f "$OUT/render.yaml" > "$OUT/diff.txt" 2>&1
drc=$?
set -e
case "$drc" in
    0) note "no difference" ;;
    1) ok "diff written: $OUT/diff.txt ($(grep -c '^[-+][^-+]' "$OUT/diff.txt") changed lines)" ;;
    *) bad "kubectl diff failed (exit $drc): $(tail -3 "$OUT/diff.txt")" ;;
esac

echo "== 5. the release candidate's image"
rc_rev=$(docker image inspect -f '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$RC_DAEMON" 2>/dev/null || true)
if [ -z "$rc_rev" ]; then
    note "$RC_DAEMON is not built yet (dist-link); --go needs it"
    [ "$mode" = dry-run ] || bad "no $RC_DAEMON"
elif [ "$rc_rev" = "$RC_COMMIT" ]; then
    ok "$RC_DAEMON is ${RC_COMMIT:0:9}'s build"
else
    bad "$RC_DAEMON is ${rc_rev:0:9}'s build, not ${RC_COMMIT:0:9}"
fi

if [ "$mode" = dry-run ]; then
    echo
    if [ "$fails" = 0 ]; then echo "DRY RUN OK: nothing was changed; --go rolls it"; else echo "DRY RUN: $fails problem(s); do not --go"; exit 1; fi
    exit 0
fi

# ── --go ────────────────────────────────────────────────────────────────
[ "$fails" = 0 ] || { echo "owner-roll-rc1.sh: the dry-run checks failed; not rolling" >&2; exit 1; }
OWNER_PW=${K2_KIND_PASSWORD_FILE:-$HOME/.local/state/k2-kind/dist/owner-password}
FAKE_PW=${K2_KIND_FAKE_PASSWORD_FILE:-${TMPDIR:-/tmp}/k2-kind/dist-fake/owner-password}
[ -r "$OWNER_PW" ] || { echo "owner-roll-rc1.sh: no owner password file at $OWNER_PW" >&2; exit 1; }

# second_host <namespace> <port> [keep]: a token for k2-host-1 (as the pool
# mints before it scales), k2-host at 2, k2-host-1 linked with k2-host-0's
# login copied in, then back to 1 (unless keep). Facts only, never contents.
second_host() {
    local kcfg=$KIND_KUBECONFIG ns=$1 port=$2 keep=${3:-} log
    k() { kubectl --kubeconfig "$kcfg" -n "$ns" "$@"; }
    k exec k2-controller-0 -c k2-daemon -- cat /var/lib/k2/.k2/daemon.token \
        | python3 -I "$KIND_REPO/scripts/kind/enroll.py" mint --kubeconfig "$kcfg" --namespace "$ns" \
            --url "http://127.0.0.1:$port" --hosts k2-host-0,k2-host-1 >&2 || { bad "$ns: mint k2-host-1"; return; }
    k scale statefulset k2-host --replicas=2 >&2
    k rollout status statefulset/k2-host --timeout=600s >&2 || { bad "$ns: k2-host-1 not ready"; return; }
    log=$(k logs k2-host-1 -c login-seed 2>&1 | tail -1)
    case "$log" in *"copied k2-host-0's login"*|*"has its own login"*) ok "$ns: k2-host-1 $log" ;; *) bad "$ns: k2-host-1 $log" ;; esac
    k exec k2-host-1 -c k2-host -- sh -c 'curl -fsS --max-time 3 http://127.0.0.1:7420/cli/hosts/status | grep -q "\"linked\":true"' \
        && ok "$ns: k2-host-1 linked" || bad "$ns: k2-host-1 not linked"
    [ "$(k exec k2-host-1 -c k2-host -- stat -c %a /var/lib/k2-host/.claude/.credentials.json 2>/dev/null)" = 600 ] \
        && ok "$ns: k2-host-1 has a login file, 0600" || bad "$ns: k2-host-1 has no login file (0600)"
    k exec k2-host-1 -c k2-host -- grep -q '"hasCompletedOnboarding": *true' /var/lib/k2-host/.claude.json \
        && ok "$ns: k2-host-1 onboarding done" || bad "$ns: k2-host-1 onboarding not done"
    [ -n "$keep" ] && return
    k scale statefulset k2-host --replicas=1 >&2
    k wait --for=delete pod/k2-host-1 --timeout=120s >&2 || true
}

echo "== 6. the running images, for the rollback"
for pair in k2-controller=k2-daemon k2-host=k2-agents; do
    id=$(sed -n "s/^${pair%%=*} //p" "$OUT/before.txt")
    case "$id" in sha256:*) ;; *) echo "owner-roll-rc1.sh: no image id for ${pair%%=*}" >&2; exit 1 ;; esac
    docker image inspect "$id" >/dev/null 2>&1 || { echo "owner-roll-rc1.sh: ${pair%%=*}'s image $id is not on this Mac" >&2; exit 1; }
    docker tag "$id" "${pair#*=}:dist-wave3"
    ok "${pair#*=}:dist-wave3 = ${id:7:12} (what ${pair%%=*} runs)"
done

if [ "$rehearse" = 1 ]; then
    echo "== 7. rehearse on k2-kind-dist-fake"
    docker tag "$RC_DAEMON" k2-daemon:dist-fake
    "$KIND_REPO/scripts/kind/up.sh" --fake "${ROLL_FLAGS[@]}" >&2 && ok "rehearsal: rolled" || bad "rehearsal: up.sh"
    # A dummy login on the test host-0 when it has none: not a credential.
    kubectl --kubeconfig "$KIND_KUBECONFIG" -n k2-kind-dist-fake exec k2-host-0 -c k2-host -- sh -c '
        [ -e /var/lib/k2-host/.claude/.credentials.json ] && exit 0
        umask 077; mkdir -p /var/lib/k2-host/.claude
        echo "{\"dummy\":\"rehearsal, not a credential\"}" > /var/lib/k2-host/.claude/.credentials.json
        [ -e /var/lib/k2-host/.claude.json ] || echo "{\"hasCompletedOnboarding\": true}" > /var/lib/k2-host/.claude.json' >&2
    second_host k2-kind-dist-fake 7442 keep
    "$KIND_REPO/scripts/kind/enroll-check.sh" >&2 && ok "rehearsal: enroll-check (k2-host-1)" || bad "rehearsal: enroll-check"
    kubectl --kubeconfig "$KIND_KUBECONFIG" -n k2-kind-dist-fake scale statefulset k2-host --replicas=1 >&2
    "$KIND_REPO/scripts/kind/app-check.sh" --dist --fake --password-file="$FAKE_PW" >&2 && ok "rehearsal: app-check" || bad "rehearsal: app-check"
    "$KIND_REPO/scripts/kind/reconnect-check.sh" --dist --fake --password-file="$FAKE_PW" >&2 && ok "rehearsal: reconnect-check" || bad "rehearsal: reconnect-check"
    [ "$fails" = 0 ] || { echo; echo "STOPPED: the rehearsal failed; k2-kind-dist is untouched"; exit 1; }
fi

echo "== 8. back up the controller's volume (inside the cluster)"
docker tag "$RC_DAEMON" k2-daemon:dist
backup=$("$KIND_REPO/scripts/kind/backup-controller.sh" --leave-down) || {
    docker tag k2-daemon:dist-wave3 k2-daemon:dist
    echo "STOPPED: the backup failed; k2-kind-dist was not rolled" >&2; exit 1; }
echo "$backup" > "$OUT/backup-pvc"
ok "backup PVC $backup"
had_host1=0
kd get pvc data-k2-host-1 >/dev/null 2>&1 && had_host1=1

rollback() {
    echo "== ROLLBACK"
    kd scale statefulset k2-host --replicas=1 >&2 || true
    docker tag k2-daemon:dist-wave3 k2-daemon:dist
    docker tag k2-agents:dist-wave3 k2-agents:dist
    "$KIND_REPO/scripts/kind/backup-controller.sh" --restore="$backup" >&2 || echo "FAIL the restore" >&2
    "$KIND_REPO/scripts/kind/up.sh" "${PREV_FLAGS[@]}" >&2 || echo "FAIL up.sh ${PREV_FLAGS[*]}" >&2
    if [ "$had_host1" = 0 ] && kd get pvc data-k2-host-1 >/dev/null 2>&1; then
        kd delete pvc data-k2-host-1 --wait=true >&2 && echo "rollback: deleted data-k2-host-1 (a copy of the login)"
    fi
    c=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:$PORT/boot-status" || true)
    echo "rollback: 127.0.0.1:$PORT -> ${c:-no answer}"
    "$KIND_REPO/scripts/kind/app-check.sh" --dist --password-file="$OWNER_PW" >&2 \
        && echo "rollback: app-check --dist PASS" || echo "rollback: app-check --dist FAIL"
}

echo "== 9. roll $NS"
if ! "$KIND_REPO/scripts/kind/up.sh" "${ROLL_FLAGS[@]}" >&2; then
    bad "up.sh ${ROLL_FLAGS[*]}"; rollback; exit 1
fi

echo "== 10. after"
snapshot "$OUT/after.txt"
section() { sed -n "/^$1/,/^#/p" "$2" | grep -v '^#'; }
missing=$(section "# pvcs" "$OUT/before.txt" | grep -vxF -f <(section "# pvcs" "$OUT/after.txt") || true)
[ -z "$missing" ] && ok "the PVCs from before keep their uids" || bad "changed PVCs: $missing"
[ "$(section "# host login" "$OUT/after.txt")" = "$(section "# host login" "$OUT/before.txt")" ] \
    && ok "unchanged: k2-host-0's login" || bad "changed: k2-host-0's login (see $OUT/before.txt and after.txt)"
[ "$(kd get statefulset k2-host -o jsonpath='{.spec.replicas}')" = 1 ] && ok "k2-host at 1 replica" || bad "k2-host replicas $(kd get statefulset k2-host -o jsonpath='{.spec.replicas}')"
kd exec k2-host-0 -c k2-host -- sh -c 'curl -fsS --max-time 3 http://127.0.0.1:7420/cli/hosts/status | grep -q "\"linked\":true"' \
    && ok "k2-host-0 linked" || bad "k2-host-0 not linked"
[ "$(kd exec k2-host-0 -c k2-host -- stat -c %a /var/lib/k2-host/.k2/host-key.pem 2>/dev/null)" = 600 ] \
    && ok "host key 0600 on its volume" || bad "host key missing or not 0600"
[ "$(kd exec k2-host-0 -c k2-host -- printenv K2_WORK_ROOT 2>/dev/null)" = /var/lib/k2/work ] \
    && ok "k2-host-0: K2_WORK_ROOT=/var/lib/k2/work" || bad "k2-host-0: no K2_WORK_ROOT"
"$KIND_REPO/scripts/kind/app-check.sh" --dist --password-file="$OWNER_PW" >&2 && ok "app-check --dist" || bad "app-check --dist"
"$KIND_REPO/scripts/kind/reconnect-check.sh" --dist --restart-live --password-file="$OWNER_PW" >&2 && ok "reconnect-check --dist --restart-live" || bad "reconnect-check"
python3 -I "$KIND_REPO/scripts/kind/owner-proof.py" --port "$PORT" --password-file "$OWNER_PW" \
    --kubeconfig "$KIND_KUBECONFIG" --namespace "$NS" | tee "$OUT/owner-proof.txt" | sed 's/^/     /'
grep -q "^PASS owner-proof" "$OUT/owner-proof.txt" && ok "the owner's path (owner-proof.py)" || bad "the owner's path (owner-proof.py)"
[ "$fails" = 0 ] && second_host "$NS" "$PORT"
c=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:$PORT/boot-status" || true)
[ "$c" = 200 ] && ok "127.0.0.1:$PORT answers" || bad "127.0.0.1:$PORT -> ${c:-no answer}"
echo
if [ "$fails" = 0 ]; then
    echo "ROLLED: k2-kind-dist on rc1 ${RC_COMMIT:0:9} (backup PVC $backup kept; rollback images :dist-wave3)"
else
    echo "ROLL: $fails check(s) failed; rolling back"; rollback; exit 1
fi
