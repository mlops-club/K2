# shellcheck shell=bash
# Topology "kind" (gate G4, P4 T4): distributed K2 on the kind cluster k2lab
# with the host pool, sourced by prove.sh. TEST ONLY namespaces
# (k2-kind-dist-fake, k2-kind-p4); k2lab only, through an explicit
# kubeconfig (`kind get kubeconfig --name k2lab`), never the current context.
#
# The kind scripts live on the kind-app worker's branch (k2-dist/kind-p4):
# LAB_KIND_REPO (default ../kind-p4 next to this checkout). This tree gives
# the scenarios (tests/lab, cli/) and the fake agent; the daemon is an image
# (LAB_KIND_DAEMON_IMAGE, default k2-daemon:rc1), recorded in run.json with
# its revision label. Nothing is compiled here except the fake agent's image.
#
# Settings:
#   LAB_KIND_WORK=shared|own   shared: the hosts share the k2-work volume
#                              with the controller (the owner's shape);
#                              own: each host its own work volume
#                              (K2_WORK_ROOT, home sync), no shared one
#   LAB_KIND_POOL=1:3          the pool's MIN:MAX (K2_PROVISIONER=kubernetes)
#   LAB_KIND_START_HOSTS=2     hosts running when the scenarios start (S3,
#                              S8 and S10 need two); above MIN, they are
#                              added as the pool adds one: a token minted
#                              first, then the replicas raised
#   LAB_HOST_MOVE_AFTER=20     the controller's K2_HOST_MOVE_AFTER (S10)
#
# kind_g4 runs, in order:
#   1. up.sh --dist --fake with the pool, enroll, K2_PTYD=1, K2_WORK_ROOT
#      (and --host-work=own) in k2-kind-dist-fake;
#   2. fleet-g2.sh (pool mode): S1 to S10 and I1 to I4 in the cluster, from
#      the lab runner pod; its scenarios, invariants and traces become this
#      run's;
#   3. kind checks as scenarios K1 to K6: enroll-check (relink, revoke,
#      restore), app-check (the app's path), the home move between two
#      nodes and the drain demo (k2-kind-p4: hosts on different nodes, no
#      shared volume), and rows PENDING on work not merged yet: a drained
#      agent's move (k2-dist/drain-move) and the controller's workspace
#      routes on the agent's host (fs/git relay).

LAB_KIND_REPO=${LAB_KIND_REPO:-$(cd "$LAB_REPO/.." && pwd)/kind-p4}
LAB_KIND_DAEMON_IMAGE=${LAB_KIND_DAEMON_IMAGE:-k2-daemon:rc1}
LAB_KIND_WORK=${LAB_KIND_WORK:-shared}
LAB_KIND_POOL=${LAB_KIND_POOL:-1:3}
LAB_HOST_MOVE_AFTER=${LAB_HOST_MOVE_AFTER:-20}
LAB_KIND_START_HOSTS=${LAB_KIND_START_HOSTS:-2}
KIND_NS_FLEET=k2-kind-dist-fake
KIND_NS_P4=k2-kind-p4
LAB_KIND_KUBECONFIG=""

kind_kc() { kubectl --kubeconfig "$LAB_KIND_KUBECONFIG" "$@"; }

kind_preflight() {
    local s
    case "$LAB_KIND_WORK" in shared|own) ;; *) echo "kind: LAB_KIND_WORK is shared or own" >&2; return 1 ;; esac
    case "$LAB_KIND_POOL" in [1-9]:[1-9]) ;; *) echo "kind: LAB_KIND_POOL is MIN:MAX" >&2; return 1 ;; esac
    for s in up.sh fleet-g2.sh enroll-check.sh app-check.sh p4-up.sh drain-demo.sh home-move-check.sh; do
        [ -x "$LAB_KIND_REPO/scripts/kind/$s" ] || { echo "kind: no $LAB_KIND_REPO/scripts/kind/$s (LAB_KIND_REPO: a checkout of k2-dist/kind-p4)" >&2; return 1; }
    done
    kind get clusters 2>/dev/null | grep -qx k2lab || { echo "kind: no kind cluster k2lab" >&2; return 1; }
    LAB_KIND_KUBECONFIG="$LAB_TMP/kubeconfig"
    kind get kubeconfig --name k2lab >"$LAB_KIND_KUBECONFIG" && chmod 600 "$LAB_KIND_KUBECONFIG"
    docker image inspect "$LAB_KIND_DAEMON_IMAGE" >/dev/null 2>&1 || { echo "kind: no image $LAB_KIND_DAEMON_IMAGE" >&2; return 1; }
}

# kind_result <id> <title> <status pass|fail|not_run> <reason> [log]: a
# scenario JSON from a kind check's output (its ok/PASS/FAIL lines as checks).
kind_result() {
    python3 -I - "$RUN/scenarios/$1.json" "$1" "$2" "$3" "$4" "${5:-}" <<'PY'
import json, os, re, sys
out, sid, title, status, reason, log = sys.argv[1:7]
checks = []
if log and os.path.isfile(log):
    for line in open(log, errors="replace"):
        m = re.match(r"^\s*(ok|PASS|FAIL|NOTE|note)\s+(.*)$", line.rstrip())
        if m and not m.group(2).startswith(("app path", "drain-demo", "home-move-check", "p4-up")):
            name, _, detail = m.group(2).partition(" -- ")
            checks.append({"name": name[:300], "ok": m.group(1) not in ("FAIL",), "detail": detail[:500],
                           "note": m.group(1).lower() == "note"})
doc = {"scenario": sid, "title": title, "status": status, "checks": checks}
if reason:
    doc["reason"] = reason
json.dump(doc, open(out, "w"), indent=2)
PY
}

kind_check() { # id title log cmd...: run a kind check; PASS when it exits 0
    local id=$1 title=$2 log=$3 rc; shift 3
    "$@" >"$log" 2>&1; rc=$?
    if [ "$rc" = 0 ]; then kind_result "$id" "$title" pass "" "$log"
    else kind_result "$id" "$title" fail "exit $rc ($(basename "$log"))" "$log"; fi
    say "$id $title: $([ "$rc" = 0 ] && echo PASS || echo "FAIL (exit $rc)")"
}

topo_up() { # <dir>: the fleet namespace, as the run asks
    local dir=$1 min=${LAB_KIND_POOL%%:*} max=${LAB_KIND_POOL#*:} want have
    kind_preflight || return 1
    # A claim template can't change on a live StatefulSet: switching
    # between shared and own work volumes recreates k2-host (PVCs stay;
    # fleet-g2's fresh step deletes them).
    want=$([ "$LAB_KIND_WORK" = own ] && echo "data work" || echo "data")
    have=$(kind_kc -n "$KIND_NS_FLEET" get statefulset k2-host -o jsonpath='{.spec.volumeClaimTemplates[*].metadata.name}' 2>/dev/null || true)
    if [ -n "$have" ] && [ "$have" != "$want" ]; then
        kind_kc -n "$KIND_NS_FLEET" delete statefulset k2-host --wait=true >>"$dir/up.log" 2>&1
    fi
    docker tag "$LAB_KIND_DAEMON_IMAGE" k2-daemon:dist-fake
    K2_KIND_FAKE_SRC=$LAB_REPO "$LAB_KIND_REPO/scripts/kind/up.sh" --dist --fake --hosts="$min" --max-hosts="$max" \
        --host-work="$LAB_KIND_WORK" --move-after="$LAB_HOST_MOVE_AFTER" --host-auth=enroll --ptyd=1 \
        --provisioner=kubernetes --work-root=/var/lib/k2/work \
        --password-file="${TMPDIR:-/tmp}/k2-kind/dist-fake/owner-password" >>"$dir/up.log" 2>&1 || return 1
    local n hosts
    n=$(kind_kc -n "$KIND_NS_FLEET" get statefulset k2-host -o jsonpath='{.spec.replicas}')
    if [ "${n:-0}" -lt "$LAB_KIND_START_HOSTS" ]; then
        hosts=$(for i in $(seq 0 $(( LAB_KIND_START_HOSTS - 1 ))); do echo "k2-host-$i"; done | paste -sd, -)
        kind_kc -n "$KIND_NS_FLEET" exec k2-controller-0 -c k2-daemon -- cat /var/lib/k2/.k2/daemon.token \
            | python3 -I "$LAB_KIND_REPO/scripts/kind/enroll.py" mint --kubeconfig "$LAB_KIND_KUBECONFIG" --namespace "$KIND_NS_FLEET" \
                --url http://127.0.0.1:7442 --hosts "$hosts" --deploy-label kind-dist-fake >>"$dir/up.log" 2>&1 || return 1
        kind_kc -n "$KIND_NS_FLEET" scale statefulset k2-host --replicas="$LAB_KIND_START_HOSTS" >>"$dir/up.log" 2>&1
        kind_kc -n "$KIND_NS_FLEET" rollout status statefulset/k2-host --timeout=600s >>"$dir/up.log" 2>&1 || return 1
    fi
}

topo_describe() { # <dir>
    local dir=$1
    {
        echo "kind repo: $LAB_KIND_REPO ($(git -C "$LAB_KIND_REPO" rev-parse HEAD) $(git -C "$LAB_KIND_REPO" rev-parse --abbrev-ref HEAD))"
        for img in "$LAB_KIND_DAEMON_IMAGE" k2-daemon:dist-fake k2-agents:dist-fake; do
            echo "image $img: $(docker image inspect -f '{{.Id}} revision={{index .Config.Labels "org.opencontainers.image.revision"}} kind={{index .Config.Labels "k2.dev/kind-revision"}}' "$img" 2>/dev/null)"
        done
        echo "work: $LAB_KIND_WORK; pool MIN:MAX $LAB_KIND_POOL; K2_HOST_MOVE_AFTER=$LAB_HOST_MOVE_AFTER"
    } >"$dir/versions.txt"
    kind_kc -n "$KIND_NS_FLEET" get pods,pvc,statefulset -o wide >"$dir/objects.txt" 2>&1
    kind_kc -n "$KIND_NS_FLEET" get statefulset -o yaml >"$dir/statefulsets.yaml" 2>&1
}

topo_metrics() {
    local rss
    rss=$(kind_kc -n "$KIND_NS_FLEET" exec k2-controller-0 -c k2-daemon -- sh -c \
        'awk "/^VmRSS/{print \$2}" /proc/$(cat /var/lib/k2/.k2-lab/daemon.pid)/status' 2>/dev/null)
    echo "daemon_rss,${rss:-NA},kB,the controller in $KIND_NS_FLEET"
}

topo_down() { :; }   # the deployments stay (the kind scripts own them; down.sh removes them)

# fleet-g2's fresh step deletes the controller's data and the app login
# with it: make the login again, or give it a new password (in the same
# 0600 file), without redeploying. The token and password stay off every argv.
kind_owner_login() { # <password file>
    local pwf=$1 tok
    tok=$(kind_kc -n "$KIND_NS_FLEET" exec k2-controller-0 -c k2-daemon -- cat /var/lib/k2/.k2/daemon.token) || return 1
    TOK=$tok PWF=$pwf python3 -I - <<'PY'
import json, os, secrets, string, urllib.request
tok, pwf = os.environ["TOK"], os.environ["PWF"]
def call(path, body=None):
    req = urllib.request.Request("http://127.0.0.1:7442%s?token=%s" % (path, tok),
                                 data=None if body is None else json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)
pw = "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(24))
if any(u.get("username") == "owner" for u in call("/cli/users").get("users", [])):
    steps = (("/cli/users/set-password", {"username": "owner", "password": pw}),)
else:
    steps = (("/cli/users/add", {"username": "owner", "password": pw}),
             ("/cli/users/set-role", {"username": "owner", "role": "owner"}))
for path, body in steps:
    if not call(path, body).get("success"):
        raise SystemExit("kind_owner_login: %s failed" % path)
fd = os.open(pwf, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
os.write(fd, (pw + "\n").encode()); os.close(fd)
os.chmod(pwf, 0o600)
PY
}

# G4 on kind: everything after topo_up (see the header).
kind_g4() {
    local fleet="$RUN/fleet" w
    export LAB_S10=1 LAB_HOST_MOVE_AFTER LAB_HOME_SYNC=1
    say "kind: S1 to S10 and I1 to I4 through fleet-g2.sh (pool mode, $KIND_NS_FLEET, work $LAB_KIND_WORK)"
    "$LAB_KIND_REPO/scripts/kind/fleet-g2.sh" --only=S1,S2,S3,S4,S5,S6,S7,S8,S9,S10 --lab-src="$LAB_REPO" --out="$fleet" \
        >"$RUN/topology/fleet-g2.log" 2>&1
    say "kind: fleet-g2 exit $? (topology/fleet-g2.log)"
    # Its results are this run's: S1 sits in its own directory there.
    [ -f "$fleet/scenarios/S1/S1.json" ] && cp "$fleet/scenarios/S1/S1.json" "$RUN/scenarios/S1.json"
    for f in "$fleet"/scenarios/S1/S1.*; do [ -e "$f" ] && cp "$f" "$RUN/scenarios/"; done
    for f in "$fleet"/scenarios/S*.json "$fleet"/scenarios/S*.log "$fleet"/scenarios/messages.jsonl; do
        [ -e "$f" ] && cp "$f" "$RUN/scenarios/"
    done
    for f in invariants.json samples.jsonl hosts-status-start.json hosts-status-end.json; do
        [ -e "$fleet/$f" ] && cp "$fleet/$f" "$RUN/"
    done
    [ -d "$fleet/traces" ] && cp -R "$fleet/traces" "$RUN/traces"
    [ -d "$fleet/outbox-final" ] && cp -R "$fleet/outbox-final" "$RUN/outbox-final"
    [ -f "$fleet/kind.txt" ] && cp "$fleet/kind.txt" "$RUN/topology/fleet-kind.txt"
    python3 -I - "$RUN/invariants.json" >"$RUN/invariants.txt" 2>/dev/null <<'PY' || true
import json, sys
for r in json.load(open(sys.argv[1])):
    print("%s %s: %s" % (r["id"], r["status"], r.get("reason", "")))
PY

    say "kind: K1 enroll-check, K2 app-check ($KIND_NS_FLEET)"
    if kind_kc -n "$KIND_NS_FLEET" get pod k2-host-1 >/dev/null 2>&1; then
        kind_check K1 "Signed enrollment: relink by key, revoke, restore" "$RUN/topology/K1-enroll-check.log" \
            "$LAB_KIND_REPO/scripts/kind/enroll-check.sh"
    else
        kind_result K1 "Signed enrollment: relink by key, revoke, restore" not_run \
            "no k2-host-1 in $KIND_NS_FLEET at this point (the pool runs $(kind_kc -n "$KIND_NS_FLEET" get statefulset k2-host -o jsonpath='{.spec.replicas}') host(s))"
    fi
    kind_owner_login "${TMPDIR:-/tmp}/k2-kind/dist-fake/owner-password" >"$RUN/topology/K2-login.log" 2>&1 \
        || say "kind: could not make the app login again (K2 will fail)"
    kind_check K2 "The desktop app's path: login, spawn, attach with a session token" "$RUN/topology/K2-app-check.log" \
        "$LAB_KIND_REPO/scripts/kind/app-check.sh" --dist --fake --password-file="${TMPDIR:-/tmp}/k2-kind/dist-fake/owner-password"

    say "kind: K3 home move and K4 drain demo ($KIND_NS_P4: two hosts on different nodes, no shared volume)"
    kind_kc delete namespace "$KIND_NS_P4" --ignore-not-found --wait=true >>"$RUN/topology/p4-up.log" 2>&1
    if "$LAB_KIND_REPO/scripts/kind/p4-up.sh" --daemon-image="$LAB_KIND_DAEMON_IMAGE" --fake-src="$LAB_REPO" \
            --pool=2:2 --move-after="$LAB_HOST_MOVE_AFTER" >>"$RUN/topology/p4-up.log" 2>&1; then
        kind_check K3 "An agent's home moves between hosts on two nodes (no shared volume)" "$RUN/topology/K3-home-move.log" \
            "$LAB_KIND_REPO/scripts/kind/home-move-check.sh" --move-wait=300
        "$LAB_KIND_REPO/scripts/kind/drain-demo.sh" >"$RUN/topology/K4-drain-demo.log" 2>&1
        w=$(grep -c '^FAIL' "$RUN/topology/K4-drain-demo.log" || true)
        first4=$(grep -E '^(PASS|FAIL) ' "$RUN/topology/K4-drain-demo.log" | head -4 | grep -c '^PASS' || true)
        if [ "$w" = 0 ]; then
            kind_result K4 "Drain a node mid-conversation: the agent continues on another host" pass "" "$RUN/topology/K4-drain-demo.log"
        elif [ "$first4" != 4 ]; then
            kind_result K4 "Drain a node mid-conversation: the agent continues on another host" fail \
                "the drain itself failed (checks 1-4: $first4 of 4 pass)" "$RUN/topology/K4-drain-demo.log"
        else
            # Checks 1-4 (the drain itself) passed; 5-7 need k2-dist/drain-move.
            kind_result K4 "Drain a node mid-conversation: the agent continues on another host" not_run \
                "PENDING k2-dist/drain-move: a drained agent moves only when its own host returns (rc1); the drain itself: $(grep -cE '^PASS' "$RUN/topology/K4-drain-demo.log") check(s) pass, $w wait for drain-move" \
                "$RUN/topology/K4-drain-demo.log"
        fi
        say "K4 drain demo: $(grep -cE '^PASS' "$RUN/topology/K4-drain-demo.log") pass, $w fail"
    else
        kind_result K3 "An agent's home moves between hosts on two nodes (no shared volume)" fail "p4-up.sh failed (topology/p4-up.log)"
        kind_result K4 "Drain a node mid-conversation: the agent continues on another host" fail "p4-up.sh failed (topology/p4-up.log)"
    fi
    kind_result K5 "A workspace made through the app is on the agent's host; the app shows the agent's files" not_run \
        "PENDING the fs/git relay (dist-link, after rc1): /cli/fs, /cli/git, project create/add and App deploy-from-folder use the controller's own disk; home-move-check.sh shows the gap (docs/prd/progress/kind-p4.md on k2-dist/kind-p4)"
    kind_result K6 "The pool scales a host down and its agents continue on the others" not_run \
        "PENDING k2-dist/drain-move: a scale-down releases its host's agents like a drain"
}
