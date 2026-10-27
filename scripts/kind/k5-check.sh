#!/usr/bin/env bash
# TEST ONLY (K5): the app's file browser and git panel, through the
# controller, work on the agent's own host volume (fs-relay;
# docs/prd/progress/fs-relay.md, "K5: turning fs-relay on, on kind").
#
#   scripts/kind/p4-up.sh --daemon-image=<fs-relay build> --fake-src=<its tree> --fs-relay
#   scripts/kind/k5-check.sh [--host-away] [--off-after]
#
# The runbook's end-to-end check, in order, on k2-kind-p4 (port 7444):
#   1. both sides set: K2_FS_RELAY=1 and K2_WORK_ROOT on the controller;
#      each host linked with k2.work_root in its labels;
#   2. an agent on a host (app-check.sh --p4), then a file written in that
#      host's pod;
#   3. read-dir and read-file through the controller show the host's file;
#      the controller's own volume doesn't have it;
#   4. write-file through the controller lands on the host only;
#   5. git through the controller on the host: info, changes, stage,
#      stage-all, commit; the host's `git log` and the controller's
#      snapshot agree. Then the same in a linked worktree
#      (create-worktree → .worktrees/<branch>). Before it, as the app does:
#      the workspace added through the controller (add-from-path, as the
#      app's Add Workspace; its project id names the worktree). The host
#      fills in a missing git identity at start (wave3 85cf7e2f6); nothing
#      is set by hand;
#   6. refusals: `..` (400 bad_path), an undeclared key (400
#      undeclared_key), a link out of the workspace (403
#      outside_work_root);
#   7. --host-away: the host's pod deleted → 503 host_away, never the
#      controller's copy; back, the file is there again;
#   8. --off-after: p4-up.sh again without --fs-relay → read-dir lists the
#      controller's own volume. It rolls the controller: last, opt-in.
#
# The app's login (the owner password p4-up.sh wrote, 0600) gives a session
# token, as the app gets one; the token stays in a 0600 file. Fake agents
# only. Only k2lab, through an explicit kubeconfig; only k2-kind-p4.
set -uo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
host_away=0 off_after=0 daemon_image="" fake_src=""
for a in "$@"; do
    case "$a" in
        --host-away) host_away=1 ;;
        --off-after) off_after=1 ;;
        --daemon-image=*) daemon_image=${a#--daemon-image=} ;;   # for --off-after's p4-up.sh
        --fake-src=*) fake_src=${a#--fake-src=} ;;
        -h|--help) sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "k5-check.sh: unknown argument $a" >&2; exit 2 ;;
    esac
done
kind_variant p4
kind_kubeconfig
NS=$KIND_NS PORT=$KIND_PORT
WS=/var/lib/k2/work/demo
kn() { kc -n "$NS" "$@"; }
PRIV=$(mktemp -d "${TMPDIR:-/tmp}/k2-k5-XXXXXX"); chmod 700 "$PRIV"
trap 'rm -rf "$PRIV"' EXIT
fails=0
ok() { printf 'ok   %s\n' "$*"; }
bad() { printf 'FAIL %s\n' "$*"; fails=$((fails + 1)); }
check() { if [ "$1" = 1 ]; then ok "$2"; else bad "$2"; fi; }

# api METHOD PATH [JSON body] → "<status> <body>" (the session token on the query, as the app sends it).
api() {
    PORT=$PORT TOKF="$PRIV/token" python3 -I - "$@" <<'PY'
import json, os, sys, urllib.error, urllib.parse, urllib.request
method, path = sys.argv[1], sys.argv[2]
body = sys.argv[3].encode() if len(sys.argv) > 3 else None
tok = open(os.environ["TOKF"]).read().strip()
sep = "&" if "?" in path else "?"
req = urllib.request.Request("http://127.0.0.1:%s%s%stoken=%s" % (os.environ["PORT"], path, sep, urllib.parse.quote(tok)),
                             data=body, method=method, headers={"Content-Type": "application/json"} if body else {})
try:
    with urllib.request.urlopen(req, timeout=60) as r:
        print(r.status, r.read().decode("utf-8", "replace"))
except urllib.error.HTTPError as e:
    print(e.code, e.read().decode("utf-8", "replace"))
except OSError as e:
    print(0, str(e))
PY
}
q() { python3 -I -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe="/"))' "$1"; }
js() { python3 -I -c 'import json, sys; print(json.dumps(dict(zip(sys.argv[1::2], sys.argv[2::2]))))' "$@"; }
code() { printf '%s' "${1%% *}"; }
body() { printf '%s' "${1#* }"; }

# The app's login.
PWF="$KIND_STATE/owner-password"
[ -r "$PWF" ] || { echo "k5-check.sh: no $PWF (run p4-up.sh first)" >&2; exit 1; }
PWF=$PWF PORT=$PORT TOKF="$PRIV/token" python3 -I - <<'PY' || { echo "k5-check.sh: login failed" >&2; exit 1; }
import json, os, urllib.request
pw = open(os.environ["PWF"]).read().strip()
req = urllib.request.Request("http://127.0.0.1:%s/cli/auth/login" % os.environ["PORT"],
                             data=json.dumps({"username": "owner", "password": pw}).encode(),
                             headers={"Content-Type": "application/json"}, method="POST")
del pw
tok = json.load(urllib.request.urlopen(req, timeout=30))["token"]
fd = os.open(os.environ["TOKF"], os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
os.write(fd, tok.encode()); os.close(fd)
PY
ok "login as owner -> a session token (in a 0600 file, not shown)"

echo "== 1. both sides are set"
cenv=$(kn exec k2-controller-0 -c k2-daemon -- env | grep -E '^(K2_FS_RELAY|K2_WORK_ROOT)=' | sort | tr '\n' ' ')
check "$( [ "$cenv" = "K2_FS_RELAY=1 K2_WORK_ROOT=/var/lib/k2/work " ] && echo 1)" "controller env: ${cenv:-none}"
r=$(api GET /cli/hosts/status)
hs=$(body "$r" | python3 -I -c 'import json,sys; d=json.load(sys.stdin); print(" ".join("%s:%s" % (h["host_id"], "work_root" if any(l.startswith("k2.work_root=/var/lib/k2/work") for l in h.get("labels", [])) else "no-label") for h in d.get("hosts", [])))' 2>/dev/null)
check "$( [ -n "$hs" ] && ! grep -q no-label <<<"$hs" && echo 1)" "hosts linked with k2.work_root=/var/lib/k2/work: ${hs:-none} ($(code "$r"))"

echo "== 2. an agent on a host"
ac=$("$KIND_REPO/scripts/kind/app-check.sh" --p4 --password-file="$PWF" 2>&1)
H=$(sed -n 's/.*the shell ran in \(k2-host-[0-9]*\).*/\1/p' <<<"$ac" | tail -1)
check "$( [ -n "$H" ] && echo 1)" "app-check --p4: the agent ran on ${H:-no host} ($(tail -1 <<<"$ac" | cut -c1-120))"
[ -n "$H" ] || { echo "k5-check.sh: no host to check against" >&2; exit 1; }
kn exec "$H" -c k2-host -- sh -c "mkdir -p $WS && echo on-host > $WS/k5.md"
check "$(kn exec "$H" -c k2-host -- cat $WS/k5.md 2>/dev/null | grep -qx on-host && echo 1)" "wrote $WS/k5.md in $H's pod"

echo "== 3. the file browser reads the host"
r=$(api GET "/cli/fs/read-dir?path=$(q $WS)")
check "$( [ "$(code "$r")" = 200 ] && grep -q '"k5.md"' <<<"$(body "$r")" && echo 1)" "read-dir $WS through the controller lists k5.md ($(code "$r"))"
r=$(api GET "/cli/fs/read-file?path=$(q $WS/k5.md)")
check "$( [ "$(code "$r")" = 200 ] && grep -q 'on-host' <<<"$(body "$r")" && echo 1)" "read-file k5.md returns on-host ($(code "$r"))"
check "$(kn exec k2-controller-0 -c k2-daemon -- test -e $WS/k5.md || echo 1)" "the controller's volume has no $WS/k5.md"

echo "== 4. writes land on the host"
r=$(api POST /cli/fs/write-file "$(js path $WS/k5b.md content written-through-the-controller)")
check "$( [ "$(code "$r")" = 200 ] && echo 1)" "write-file k5b.md through the controller ($(code "$r") $(body "$r" | cut -c1-80))"
check "$(kn exec "$H" -c k2-host -- cat $WS/k5b.md 2>/dev/null | grep -qx written-through-the-controller && echo 1)" "$H has k5b.md"
check "$(kn exec k2-controller-0 -c k2-daemon -- test -e $WS/k5b.md || echo 1)" "the controller's volume has no k5b.md"

echo "== 5. the git panel works on the host"
git_flow() { # <repo path> <label> <file> <message>
    local p=$1 l=$2 f=$3 m=$4 r
    r=$(api GET "/cli/git/info?path=$(q "$p")")
    check "$( [ "$(code "$r")" = 200 ] && echo 1)" "$l: git info ($(code "$r") $(body "$r" | cut -c1-100))"
    r=$(api GET "/cli/git/changes?path=$(q "$p")")
    check "$( [ "$(code "$r")" = 200 ] && grep -q "$f" <<<"$(body "$r")" && echo 1)" "$l: git changes lists $f ($(code "$r"))"
    r=$(api POST /cli/git/stage "$(js path "$p" filePath "$f")")
    check "$( [ "$(code "$r")" = 200 ] && echo 1)" "$l: stage $f ($(code "$r") $(body "$r" | cut -c1-80))"
    check "$(kn exec "$H" -c k2-host -- git -C "$p" diff --cached --name-only 2>/dev/null | grep -qx "$f" && echo 1)" "$l: $f staged in $H's index"
    r=$(api POST /cli/git/stage-all "$(js path "$p")")
    check "$( [ "$(code "$r")" = 200 ] && echo 1)" "$l: stage-all ($(code "$r"))"
    r=$(api POST /cli/git/commit "$(js path "$p" message "$m")")
    check "$( [ "$(code "$r")" = 200 ] && echo 1)" "$l: commit '$m' ($(code "$r") $(body "$r" | cut -c1-100))"
    local hl head snap
    hl=$(kn exec "$H" -c k2-host -- git -C "$p" log -1 --format='%H %s' 2>/dev/null)
    check "$( [ "${hl#* }" = "$m" ] && echo 1)" "$l: $H's git log -1: ${hl:0:12} ${hl#* }"
    head=${hl%% *}
    r=$(api GET "/cli/git/snapshot?path=$(q "$p")")
    snap=$(body "$r")
    check "$( [ "$(code "$r")" = 200 ] && [ -n "$head" ] && grep -q "${head:0:7}" <<<"$snap" && echo 1)" "$l: the controller's snapshot shows HEAD ${head:0:12} ($(code "$r"))"
    check "$(kn exec k2-controller-0 -c k2-daemon -- test -e "$p/.git" || echo 1)" "$l: no repository at $p on the controller's volume"
}
# The workspace, added as the app's Add Workspace does (relayed to the host).
r=$(api POST /cli/projects/add-from-path "$(js path $WS)")
PID=$(body "$r" | python3 -I -c 'import json,sys; d=json.load(sys.stdin); print(d.get("id") or (d.get("project") or {}).get("id") or "")' 2>/dev/null)
check "$( [ "$(code "$r")" = 200 ] && [ -n "$PID" ] && echo 1)" "add-from-path $WS through the controller: project ${PID:-none} ($(code "$r") $(body "$r" | cut -c1-80))"
# The host's git identity: filled in at its start, never set here.
gid=$(kn exec "$H" -c k2-host -- sh -c 'printf "%s <%s>" "$(git config --global user.name)" "$(git config --global user.email)"')
check "$( [ "$gid" != " <>" ] && [ -n "$gid" ] && echo 1)" "$H has a git identity from its start: ${gid:-none}"
if [ "$(kn exec "$H" -c k2-host -- sh -c "test -e $WS/.git && echo y")" != y ]; then
    r=$(api POST /cli/git/init "$(js path $WS)")
    check "$( [ "$(code "$r")" = 200 ] && echo 1)" "git init $WS through the controller ($(code "$r") $(body "$r" | cut -c1-80))"
else
    ok "$WS is a repository on $H already (up.sh made demo)"
fi
git_flow $WS "workspace" k5.md k5
BR=k5-wt
r=$(api POST /cli/git/create-worktree "$(js projectPath $WS branch $BR projectId "$PID")")
WT=$(body "$r" | python3 -I -c 'import json,sys
d = json.load(sys.stdin)
w = d["worktree"] if isinstance(d.get("worktree"), dict) else {}
print(next((v for k in ("path", "worktreePath", "worktree_path") for v in (d.get(k), w.get(k)) if isinstance(v, str) and v), ""))' 2>/dev/null)
[ -n "$WT" ] || WT=$WS/.worktrees/$BR
check "$( [ "$(code "$r")" = 200 ] && echo 1)" "create-worktree $BR ($(code "$r") $(body "$r" | cut -c1-120))"
check "$(kn exec "$H" -c k2-host -- test -e "$WT/.git" && echo 1)" "the linked worktree $WT is on $H"
r=$(api POST /cli/fs/write-file "$(js path "$WT/k5-wt.md" content in-the-worktree)")
check "$( [ "$(code "$r")" = 200 ] && echo 1)" "write-file in the worktree ($(code "$r"))"
git_flow "$WT" "worktree $BR" k5-wt.md k5-wt
r=$(api GET "/cli/git/worktrees?path=$(q $WS)")
check "$( [ "$(code "$r")" = 200 ] && grep -q "$BR" <<<"$(body "$r")" && echo 1)" "git worktrees lists $BR ($(code "$r"))"

echo "== 6. refusals"
r=$(api GET "/cli/fs/read-file?path=$(q $WS/../../../etc/passwd)")
check "$( [ "$(code "$r")" = 400 ] && grep -q bad_path <<<"$(body "$r")" && echo 1)" "read-file $WS/../../../etc/passwd -> $(code "$r") $(body "$r" | cut -c1-100)"
r=$(api GET "/cli/fs/read-file?path=$(q $WS/k5.md)&mode=x")
check "$( [ "$(code "$r")" = 400 ] && grep -q undeclared_key <<<"$(body "$r")" && echo 1)" "an undeclared key (mode=x) -> $(code "$r") $(body "$r" | cut -c1-100)"
kn exec "$H" -c k2-host -- sh -c "ln -sfn /etc $WS/etc-link"
r=$(api GET "/cli/fs/read-file?path=$(q $WS/etc-link/passwd)")
check "$( [ "$(code "$r")" = 403 ] && grep -q outside_work_root <<<"$(body "$r")" && echo 1)" "a link out of the workspace (etc-link/passwd) -> $(code "$r") $(body "$r" | cut -c1-100)"
r=$(api POST /cli/git/stage "$(js path $WS filePath ../../../etc/passwd)")
check "$( [ "$(code "$r")" -ge 400 ] && ! grep -q 'root:' <<<"$(body "$r")" && echo 1)" "git stage of a file outside the repo (../../../etc/passwd) -> $(code "$r") $(body "$r" | cut -c1-100)"
kn exec "$H" -c k2-host -- rm -f "$WS/etc-link"

if [ "$host_away" = 1 ]; then
    echo "== 7. the host is away"
    kn delete pod "$H" --wait=false >/dev/null
    away=""
    for _ in $(seq 1 40); do
        r=$(api GET "/cli/fs/read-dir?path=$(q $WS)")
        [ "$(code "$r")" = 503 ] && { away=$r; break; }
        sleep 0.5
    done
    check "$( [ -n "$away" ] && grep -q host_away <<<"$(body "$away")" && echo 1)" "while $H is away: read-dir -> ${away:-no 503 seen: $(code "$r")} $(body "${away:-$r}" | cut -c1-80)"
    kn wait --for=condition=Ready "pod/$H" --timeout=300s >/dev/null 2>&1
    back=""
    for _ in $(seq 1 60); do
        r=$(api GET "/cli/fs/read-dir?path=$(q $WS)")
        [ "$(code "$r")" = 200 ] && grep -q '"k5.md"' <<<"$(body "$r")" && { back=1; break; }
        sleep 1
    done
    check "$back" "$H back: read-dir lists k5.md again ($(code "$r"))"
fi

if [ "$off_after" = 1 ]; then
    echo "== 8. off is the old behaviour"
    "$KIND_REPO/scripts/kind/p4-up.sh" ${daemon_image:+--daemon-image="$daemon_image"} ${fake_src:+--fake-src="$fake_src"} >"$PRIV/p4-up-off.log" 2>&1 \
        && ok "p4-up.sh without --fs-relay" || bad "p4-up.sh without --fs-relay ($(tail -1 "$PRIV/p4-up-off.log"))"
    cenv=$(kn exec k2-controller-0 -c k2-daemon -- env | grep -cE '^K2_FS_RELAY=')
    check "$( [ "$cenv" = 0 ] && echo 1)" "the controller's env has no K2_FS_RELAY"
    # The controller's own volume: its copy of the folder, or none at all
    # (then "No such file or directory"), never the host's files.
    r=$(api GET "/cli/fs/read-dir?path=$(q $WS)")
    check "$( { [ "$(code "$r")" = 200 ] && ! grep -q '"k5.md"' <<<"$(body "$r")"; } || { [ "$(code "$r")" = 400 ] && grep -q 'No such file' <<<"$(body "$r")"; } && echo 1)" \
        "read-dir $WS answers from the controller's own volume, not the host's ($(code "$r") $(body "$r" | cut -c1-80))"
fi

echo
if [ "$fails" = 0 ]; then echo "PASS k5-check: the file browser and git panel work on the agent's host ($H)"; else echo "FAIL k5-check: $fails check(s)"; exit 1; fi
