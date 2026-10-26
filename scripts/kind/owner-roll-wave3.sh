#!/usr/bin/env bash
# The owner's end-state roll: k2-kind-dist (port 7443) onto k2-dist/wave3,
# with the host pool (K2_PROVISIONER=kubernetes), signed host enrollment
# (K2_HOST_AUTH=enroll), agent PTYs in k2-ptyd (K2_PTYD=1) and the real
# Claude Code agents image (k2-agents:dist).
#
#   scripts/kind/owner-roll-wave3.sh [--dry-run]     (the default)
#   scripts/kind/owner-roll-wave3.sh --go [--skip-rehearsal]
#
# --dry-run changes nothing anywhere. It
#   1. snapshots k2-kind-dist read-only: pods, image ids, the PVCs' uids,
#      the host's Claude login (present, size, mtime, onboarding flag: never
#      its contents), the controller's login and host state files, and
#      whether the Secret k2-host-enroll exists;
#   2. renders what the roll applies (up.sh --render-only: 1 host, enroll,
#      K2_PTYD=1, K2_PROVISIONER=kubernetes with MIN = MAX = 1);
#   3. checks the hard requirements on the render:
#      (a) StatefulSet k2-host keeps its name, serviceName and claim template
#          ("data", same size and access modes), so k2-host-0 mounts the
#          same PVC data-k2-host-0 at the same /var/lib/k2-host;
#      (b) the pool's floor and ceiling are 1: never zero hosts, and never a
#          second host whose idle scale-down would strand agent homes (home
#          sync, P3 T1, is not in wave3 yet);
#      (c) the NodePort behind the proxy (30743) and the controller's
#          volume (the app's login) are unchanged; the proxy answers on 7443;
#   4. runs `kubectl diff` (server-side dry run) of the render against the
#      live namespace and keeps it in $KIND_STATE/owner-roll/.
#
# --go (only when the coordinator says so) also
#   5. keeps the current images as k2-daemon:dist-prev / k2-agents:dist-prev;
#   6. builds k2-daemon:dist from a detached checkout of $WAVE3_COMMIT
#      ($WAVE3_SRC, default ../wave3-src) and rehearses the exact roll on
#      the test variant k2-kind-dist-fake with 2 hosts (enroll-check,
#      app-check and reconnect-check there); a failed rehearsal stops here,
#      with k2-kind-dist untouched;
#   7. backs up the controller's volume inside the cluster
#      (scripts/kind/backup-controller.sh: a PVC k2-backup-controller-<UTC>);
#   8. rolls k2-kind-dist: scripts/kind/up.sh --dist --host-auth=enroll
#      --ptyd=1 --provisioner=kubernetes (never --reset-password: the app's
#      login stays). The owner's app reconnects once, when the controller
#      rolls;
#   9. checks afterwards: the same PVC uids; the same login file (size,
#      mtime) with onboarding done; k2-host-0 linked by its key (0600);
#      replicas 1; app-check.sh --dist; reconnect-check.sh --dist
#      --restart-live; then the owner's path (scripts/kind/owner-proof.py):
#      the app login on 7443, one short prompt that wakes a Claude Code
#      agent on k2-host-0 and is answered in its transcript, a sleep and a
#      second wake into the same session.
# Rollback (automatic if a check in 8–9 fails): the -prev images back to
# :dist, backup-controller.sh --restore=<the backup PVC>, then
# scripts/kind/up.sh --dist (the pool off, token auth); 7443 and app-check
# are checked again. By hand, the same three commands.
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
        -h|--help) sed -n '2,50p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "owner-roll-wave3.sh: unknown argument $a" >&2; exit 2 ;;
    esac
done
kind_variant dist
kind_kubeconfig
NS=$KIND_NS PORT=$KIND_PORT
ROLL_FLAGS=(--dist --host-auth=enroll --ptyd=1 --provisioner=kubernetes)
OUT="$KIND_STATE/owner-roll"
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
        kd get pvc -o custom-columns=NAME:.metadata.name,UID:.metadata.uid,SIZE:.spec.resources.requests.storage --no-headers
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
floor = env.get("K2_PROVISIONER") == "kubernetes" and env.get("K2_PROVISIONER_MIN_HOSTS") == "1" and env.get("K2_PROVISIONER_MAX_HOSTS") == "1"
print(("ok  " if floor else "FAIL") + " (b) K2_PROVISIONER=%s, MIN_HOSTS=%s, MAX_HOSTS=%s; k2-host replicas %s" % (
    env.get("K2_PROVISIONER"), env.get("K2_PROVISIONER_MIN_HOSTS"), env.get("K2_PROVISIONER_MAX_HOSTS"), host["spec"]["replicas"]))
np = find("Service", "k2-controller-nodeport")
ports = [p.get("nodePort") for p in np["spec"]["ports"]]
claims_ctl = claims(ctl)
print(("ok  " if ports == [30743] else "FAIL") + " (c) the proxy's NodePort stays %s; the controller keeps claim %s (the app's login)" % (ports, claims_ctl))
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

if [ "$mode" = dry-run ]; then
    echo
    if [ "$fails" = 0 ]; then echo "DRY RUN OK: nothing was changed; --go rolls it"; else echo "DRY RUN: $fails problem(s); do not --go"; exit 1; fi
    exit 0
fi

# ── --go ────────────────────────────────────────────────────────────────
[ "$fails" = 0 ] || { echo "owner-roll-wave3.sh: the dry-run checks failed; not rolling" >&2; exit 1; }
WAVE3_COMMIT=${WAVE3_COMMIT:-853157feeccabdc01f59481cb4c29b45151bebb2}   # k2-dist/wave3, the coordinator's go
WAVE3_SRC=${WAVE3_SRC:-$KIND_REPO/../wave3-src}
OWNER_PW=${K2_KIND_PASSWORD_FILE:-$HOME/.local/state/k2-kind/dist/owner-password}
FAKE_PW=${K2_KIND_FAKE_PASSWORD_FILE:-${TMPDIR:-/tmp}/k2-kind/dist-fake/owner-password}
[ -r "$OWNER_PW" ] || { echo "owner-roll-wave3.sh: no owner password file at $OWNER_PW" >&2; exit 1; }
if [ ! -e "$WAVE3_SRC/.git" ]; then
    git -C "$KIND_REPO" worktree add --detach "$WAVE3_SRC" "$WAVE3_COMMIT" >&2
fi
[ "$(git -C "$WAVE3_SRC" rev-parse HEAD)" = "$WAVE3_COMMIT" ] \
    || { echo "owner-roll-wave3.sh: $WAVE3_SRC is not at $WAVE3_COMMIT" >&2; exit 1; }
say "wave3 source: $WAVE3_SRC at ${WAVE3_COMMIT:0:9}"

echo "== 5. keep the current images for the rollback"
docker tag k2-daemon:dist k2-daemon:dist-prev
docker tag k2-agents:dist k2-agents:dist-prev
ok "k2-daemon:dist-prev and k2-agents:dist-prev"

echo "== 6. build from wave3 and rehearse on k2-kind-dist-fake"
K2_KIND_BUILD_MEMORY=${K2_KIND_BUILD_MEMORY:-6g} CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-1} \
    "$KIND_REPO/scripts/kind/build-daemon.sh" "$WAVE3_SRC" k2-daemon:dist >&2
if [ "$rehearse" = 1 ]; then
    docker tag k2-daemon:dist k2-daemon:dist-fake
    K2_KIND_DAEMON_SRC="$WAVE3_SRC" "$KIND_REPO/scripts/kind/up.sh" --dist --fake --hosts=2 \
        --host-auth=enroll --ptyd=1 --provisioner=kubernetes >&2 && ok "rehearsal: rolled" || bad "rehearsal: up.sh"
    "$KIND_REPO/scripts/kind/enroll-check.sh" >&2 && ok "rehearsal: enroll-check" || bad "rehearsal: enroll-check"
    "$KIND_REPO/scripts/kind/app-check.sh" --dist --fake --password-file="$FAKE_PW" >&2 && ok "rehearsal: app-check" || bad "rehearsal: app-check"
    "$KIND_REPO/scripts/kind/reconnect-check.sh" --dist --fake --password-file="$FAKE_PW" >&2 && ok "rehearsal: reconnect-check" || bad "rehearsal: reconnect-check"
    [ "$fails" = 0 ] || { echo; echo "STOPPED: the rehearsal failed; k2-kind-dist is untouched"; exit 1; }
fi

echo "== 7. back up the controller's volume (inside the cluster)"
# --leave-down: the nodes' k2-daemon:dist is already wave3's (the
# rehearsal's kind load); the roll below brings the controller up on it.
backup=$("$KIND_REPO/scripts/kind/backup-controller.sh" --leave-down) || { echo "STOPPED: the backup failed; k2-kind-dist was not rolled" >&2; exit 1; }
echo "$backup" > "$OUT/backup-pvc"
ok "backup PVC $backup"

rollback() {
    echo "== ROLLBACK"
    docker tag k2-daemon:dist-prev k2-daemon:dist
    docker tag k2-agents:dist-prev k2-agents:dist
    "$KIND_REPO/scripts/kind/backup-controller.sh" --restore="$backup" >&2 || echo "FAIL the restore" >&2
    "$KIND_REPO/scripts/kind/up.sh" --dist >&2 || echo "FAIL up.sh --dist" >&2
    c=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:$PORT/boot-status" || true)
    echo "rollback: 127.0.0.1:$PORT -> ${c:-no answer}"
    "$KIND_REPO/scripts/kind/app-check.sh" --dist --password-file="$OWNER_PW" >&2 \
        && echo "rollback: app-check --dist PASS" || echo "rollback: app-check --dist FAIL"
}

echo "== 8. roll $NS"
if ! K2_KIND_DAEMON_SRC="$WAVE3_SRC" "$KIND_REPO/scripts/kind/up.sh" "${ROLL_FLAGS[@]}" >&2; then
    bad "up.sh ${ROLL_FLAGS[*]}"; rollback; exit 1
fi

echo "== 9. after"
snapshot "$OUT/after.txt"
for what in "# pvcs" "# host login"; do
    b=$(sed -n "/^$what/,/^#/p" "$OUT/before.txt" | grep -v '^#')
    a=$(sed -n "/^$what/,/^#/p" "$OUT/after.txt" | grep -v '^#')
    [ "$a" = "$b" ] && ok "unchanged: ${what#\# }" || bad "changed: ${what#\# } (see $OUT/before.txt and after.txt)"
done
[ "$(kd get statefulset k2-host -o jsonpath='{.spec.replicas}')" = 1 ] && ok "k2-host at 1 replica" || bad "k2-host replicas $(kd get statefulset k2-host -o jsonpath='{.spec.replicas}')"
kd exec k2-host-0 -c k2-host -- sh -c 'curl -fsS --max-time 3 http://127.0.0.1:7420/cli/hosts/status | grep -q "\"linked\":true"' \
    && ok "k2-host-0 linked" || bad "k2-host-0 not linked"
[ "$(kd exec k2-host-0 -c k2-host -- stat -c %a /var/lib/k2-host/.k2/host-key.pem 2>/dev/null)" = 600 ] \
    && ok "host key 0600 on its volume" || bad "host key missing or not 0600"
"$KIND_REPO/scripts/kind/app-check.sh" --dist --password-file="$OWNER_PW" >&2 && ok "app-check --dist" || bad "app-check --dist"
"$KIND_REPO/scripts/kind/reconnect-check.sh" --dist --restart-live --password-file="$OWNER_PW" >&2 && ok "reconnect-check --dist --restart-live" || bad "reconnect-check"
python3 -I "$KIND_REPO/scripts/kind/owner-proof.py" --port "$PORT" --password-file "$OWNER_PW" \
    --kubeconfig "$KIND_KUBECONFIG" --namespace "$NS" | tee "$OUT/owner-proof.txt" | sed 's/^/     /'
grep -q "^PASS owner-proof" "$OUT/owner-proof.txt" && ok "the owner's path (owner-proof.py)" || bad "the owner's path (owner-proof.py)"
c=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:$PORT/boot-status" || true)
[ "$c" = 200 ] && ok "127.0.0.1:$PORT answers" || bad "127.0.0.1:$PORT -> ${c:-no answer}"
echo
if [ "$fails" = 0 ]; then
    echo "ROLLED: k2-kind-dist on wave3 ${WAVE3_COMMIT:0:9} (backup PVC $backup kept)"
else
    echo "ROLL: $fails check(s) failed; rolling back"; rollback; exit 1
fi
