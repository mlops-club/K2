#!/usr/bin/env bash
# TEST ONLY: rehearse the owner's fs-relay migration (docs/prd/progress/
# fs-relay.md, "The owner's K2 at rc3") on the rehearsal variant
# (up.sh --rehearse: namespace k2-p4-rehearse, port 7445), never on the
# owner's k2-kind-dist.
#
#   scripts/kind/fs-relay-migrate-rehearse.sh [--daemon-image=k2-daemon:p4]
#                                             [--fake-src=PATH]
#
# 1. The owner's shape: a controller and one host sharing k2-work, home
#    sync on the host (K2_WORK_ROOT), fs-relay off; two workspaces seeded
#    on k2-work (alpha, a git repository with an agent; beta, plain files).
# 2. Backup: k2-work copied onto k2-backup-work-<UTC> (work-copy.sh --backup).
# 3. Migrate: the host stopped and its StatefulSet deleted (its PVCs stay),
#    the roll with --host-work=own --work-size=<k2-work's size> --fs-relay,
#    the host stopped again, k2-work copied onto work-k2-host-0
#    (work-copy.sh), the host started.
# 4. ON: an agent spawned in alpha binds it to the host; a file written in
#    the host's copy reads through the controller (not on the controller's
#    k2-work); a commit through the controller lands in the host's
#    repository; beta's seeded files read through the relay as they were.
# 5. Roll back: the host stopped, work-k2-host-0 copied back over k2-work
#    (--mirror), its StatefulSet deleted, the roll without --host-work=own
#    and --fs-relay; what was written while ON is on k2-work, read through
#    the controller with the relay off.
# Checks print ok/FAIL; the last line is "rehearsal: PASS" or the failures.
# Evidence (the renders, the checks) in $KIND_STATE/rehearsal-<UTC>.
# Each run starts from a fresh k2-p4-rehearse (an earlier one is deleted
# first) and leaves it up for a look; delete it with
# kubectl delete namespace k2-p4-rehearse when done.
#
# Only k2lab, through an explicit kubeconfig; only namespace k2-p4-rehearse.
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
daemon_image=k2-daemon:p4 fake_src="${K2_KIND_FAKE_SRC:-$KIND_REPO}"
for a in "$@"; do
    case "$a" in
        --daemon-image=*) daemon_image=${a#--daemon-image=} ;;
        --fake-src=*) fake_src=${a#--fake-src=} ;;
        -h|--help) sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "fs-relay-migrate-rehearse.sh: unknown argument $a" >&2; exit 2 ;;
    esac
done
kind_variant rehearse
[ "$KIND_NS" = k2-p4-rehearse ] || { echo "refusing namespace $KIND_NS" >&2; exit 1; }
kind_kubeconfig
kn() { kc -n "$KIND_NS" "$@"; }
docker image inspect "$daemon_image" >/dev/null 2>&1 || { echo "no image $daemon_image" >&2; exit 1; }
docker tag "$daemon_image" k2-daemon:rehearse
# A fresh start: the namespace of an earlier run goes (its claims with it).
if kc get namespace "$KIND_NS" >/dev/null 2>&1; then
    say "deleting the earlier $KIND_NS"
    kc delete namespace "$KIND_NS" --wait=true --timeout=300s >/dev/null
fi
RUN="$KIND_STATE/rehearsal-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN"
fails=0
ok() { printf 'ok   %s\n' "$*" | tee -a "$RUN/checks.txt"; }
bad() { printf 'FAIL %s\n' "$*" | tee -a "$RUN/checks.txt"; fails=$((fails + 1)); }
check() { if [ "$1" = 1 ]; then ok "$2"; else bad "$2"; fi; }
W=/var/lib/k2/work
UP=(env K2_KIND_FAKE_SRC="$fake_src" "$KIND_REPO/scripts/kind/up.sh" --rehearse --hosts=1 --work-root="$W" --password-file="$KIND_STATE/owner-password")

# The controller's API from inside its pod, with its owner token (never printed).
api() {  # api METHOD PATH [JSON] → "<code> <body>"
    kn exec k2-controller-0 -c k2-daemon -- sh -c '
        t=$(cat /var/lib/k2/.k2/daemon.token)
        if [ "$1" = GET ]; then
            curl -s -o /tmp/b -w "%{http_code}" -H "Authorization: Bearer $t" "http://127.0.0.1:7420$2"
        else
            curl -s -o /tmp/b -w "%{http_code}" -H "Authorization: Bearer $t" -H "Content-Type: application/json" -X POST --data "$3" "http://127.0.0.1:7420$2"
        fi; printf " "; cat /tmp/b' sh "$1" "$2" "${3:-}"
}
code() { printf '%s' "$1" | cut -d' ' -f1; }
q() { python3 -I -c 'import sys,urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"; }
host_sh() { kn exec k2-host-0 -c k2-host -- sh -c "$1"; }
ctl_sh() { kn exec k2-controller-0 -c k2-daemon -- sh -c "$1"; }
wait_linked() {
    for _ in $(seq 1 60); do
        host_sh 'curl -fsS --max-time 3 http://127.0.0.1:7420/cli/hosts/status' 2>/dev/null | grep -q '"linked":true' && return 0
        sleep 5
    done
    return 1
}
stop_host() { kn scale statefulset k2-host --replicas=0 >/dev/null; kn wait --for=delete pod/k2-host-0 --timeout=180s >/dev/null 2>&1 || true; }
start_host() { kn scale statefulset k2-host --replicas=1 >/dev/null; kn rollout status statefulset/k2-host --timeout=300s >/dev/null; wait_linked; }

# ── 1. the owner's shape ───────────────────────────────────────────────
"${UP[@]}" --render-only="$RUN/shared.yaml" >/dev/null 2>&1
"${UP[@]}" >"$RUN/up-shared.log" 2>&1 || { bad "up.sh (shared shape): see $RUN/up-shared.log"; exit 1; }
check "$(kn get pod k2-host-0 -o json | grep -q '"claimName": "k2-work"' && echo 1)" "the host mounts the shared k2-work"
wait_linked && ok "k2-host-0 linked" || bad "k2-host-0 not linked"
ctl_sh "set -e; mkdir -p $W/alpha $W/beta/docs; cd $W/alpha; printf 'seeded alpha\n' > readme.md
    git init -q -b main . && git -c user.name=r -c user.email=r@x.invalid add -A && git -c user.name=r -c user.email=r@x.invalid commit -qm seed
    printf 'beta one\n' > $W/beta/one.txt; printf 'beta two\n' > $W/beta/docs/two.txt"
r=$(api POST /cli/projects/add-from-path "{\"path\":\"$W/alpha\"}"); check "$( [ "$(code "$r")" = 200 ] && echo 1)" "alpha added ($(code "$r"))"
r=$(api POST /cli/projects/add-without-git "{\"path\":\"$W/beta\"}"); check "$( [ "$(code "$r")" = 200 ] && echo 1)" "beta added ($(code "$r"))"
seeded=$(ctl_sh "cd $W && find alpha beta -path alpha/.git -prune -o -type f -print | sort | xargs sha256sum")
echo "$seeded" > "$RUN/seeded.sums"

# ── 2. backup ──────────────────────────────────────────────────────────
backup=$("$KIND_REPO/scripts/kind/work-copy.sh" --ns="$KIND_NS" --from=k2-work --backup --go 2>"$RUN/backup.log" | tail -1)
check "$( [ -n "$backup" ] && kn get pvc "$backup" >/dev/null 2>&1 && echo 1)" "k2-work backed up onto ${backup:-?}"

# ── 3. migrate ─────────────────────────────────────────────────────────
size=$(kn get pvc k2-work -o jsonpath='{.spec.resources.requests.storage}')
stop_host
kn delete statefulset k2-host --wait=true >/dev/null
"${UP[@]}" --host-work=own --work-size="$size" --fs-relay --render-only="$RUN/own.yaml" >/dev/null 2>&1
diff "$RUN/shared.yaml" "$RUN/own.yaml" > "$RUN/shared-to-own.diff" || true
"${UP[@]}" --host-work=own --work-size="$size" --fs-relay >"$RUN/up-own.log" 2>&1 || bad "up.sh (own volumes, fs-relay): see $RUN/up-own.log"
check "$(kn get pvc work-k2-host-0 -o jsonpath='{.spec.resources.requests.storage}' 2>/dev/null | grep -qx "$size" && echo 1)" "work-k2-host-0 made, $size"
stop_host
"$KIND_REPO/scripts/kind/work-copy.sh" --ns="$KIND_NS" --from=k2-work --to=work-k2-host-0 --go >"$RUN/copy-forward.log" 2>&1 \
    && ok "k2-work copied onto work-k2-host-0 ($(grep -o 'COPY-OK.*' "$RUN/copy-forward.log"))" || bad "the forward copy: see $RUN/copy-forward.log"
start_host && ok "k2-host-0 linked on its own volume" || bad "k2-host-0 not linked after the migration"
check "$(kn exec k2-controller-0 -c k2-daemon -- env | grep -qx 'K2_FS_RELAY=1' && echo 1)" "the controller has K2_FS_RELAY=1"

# ── 4. ON ──────────────────────────────────────────────────────────────
# Every seeded file, unchanged (K2 adds its own .k2/ markers on its own).
host_sums=$(host_sh "cd $W && find alpha beta -path alpha/.git -prune -o -type f -print | sort | xargs sha256sum")
check "$(missing=$(comm -23 <(printf '%s\n' "$seeded" | sort) <(printf '%s\n' "$host_sums" | sort)); [ -z "$missing" ] && echo 1)" "every seeded file is on the host's copy, unchanged"
r=$(api POST /cli/sessions/v2/spawn "{\"agent_name\":\"alpha\",\"cwd\":\"$W/alpha\",\"command\":\"claude\",\"args\":[],\"cols\":80,\"rows\":24}")
check "$( [ "$(code "$r")" = 200 ] && echo 1)" "an agent spawned in alpha ($(code "$r"))"
host_sh "printf 'written on the host\n' > $W/alpha/on-host.md"
r=$(api GET "/cli/fs/read-file?path=$(q "$W/alpha/on-host.md")")
check "$( [ "$(code "$r")" = 200 ] && grep -q 'written on the host' <<<"$r" && echo 1)" "read through the controller: the host's file ($(code "$r"))"
check "$(ctl_sh "test -e $W/alpha/on-host.md" || echo 1)" "not on the controller's k2-work"
api POST /cli/git/stage-all "{\"path\":\"$W/alpha\"}" >/dev/null
r=$(api POST /cli/git/commit "{\"path\":\"$W/alpha\",\"message\":\"while on\"}")
check "$( [ "$(code "$r")" = 200 ] && echo 1)" "commit through the controller ($(code "$r") $(printf '%s' "$r" | cut -c5-90))"
check "$(host_sh "git -C $W/alpha log --oneline" | grep -q 'while on' && echo 1)" "the commit is in the host's repository"
r=$(api POST /cli/sessions/v2/spawn "{\"agent_name\":\"beta\",\"cwd\":\"$W/beta\",\"command\":\"claude\",\"args\":[],\"cols\":80,\"rows\":24}")
r=$(api GET "/cli/fs/read-file?path=$(q "$W/beta/docs/two.txt")")
check "$( [ "$(code "$r")" = 200 ] && grep -q 'beta two' <<<"$r" && echo 1)" "beta's seeded file reads through the relay ($(code "$r"))"

# ── 5. roll back ───────────────────────────────────────────────────────
stop_host
"$KIND_REPO/scripts/kind/work-copy.sh" --ns="$KIND_NS" --from=work-k2-host-0 --to=k2-work --mirror --allow-mounted=k2-controller-0 --go >"$RUN/copy-back.log" 2>&1 \
    && ok "work-k2-host-0 copied back over k2-work ($(grep -o 'COPY-OK.*' "$RUN/copy-back.log"))" || bad "the copy back: see $RUN/copy-back.log"
kn delete statefulset k2-host --wait=true >/dev/null
"${UP[@]}" >"$RUN/up-back.log" 2>&1 || bad "up.sh (back to shared): see $RUN/up-back.log"
wait_linked && ok "k2-host-0 linked on the shared k2-work again" || bad "k2-host-0 not linked after the rollback"
check "$(kn get pod k2-host-0 -o json | grep -q '"claimName": "k2-work"' && echo 1)" "the host mounts k2-work again"
check "$(kn exec k2-controller-0 -c k2-daemon -- env | grep -q 'K2_FS_RELAY' || echo 1)" "the controller has no K2_FS_RELAY"
r=$(api GET "/cli/fs/read-file?path=$(q "$W/alpha/on-host.md")")
check "$( [ "$(code "$r")" = 200 ] && grep -q 'written on the host' <<<"$r" && echo 1)" "what was written while ON reads from k2-work, relay off ($(code "$r"))"
check "$(ctl_sh "git -C $W/alpha log --oneline" | grep -q 'while on' && echo 1)" "the commit made while ON is in k2-work's repository"
check "$(kn get pvc work-k2-host-0 "$backup" >/dev/null 2>&1 && echo 1)" "work-k2-host-0 and $backup kept (for a second try, or a restore)"

echo "evidence: $RUN"
if [ "$fails" = 0 ]; then echo "rehearsal: PASS"; else echo "rehearsal: $fails check(s) failed"; exit 1; fi
