#!/usr/bin/env bash
# TEST ONLY: ptyd as a sidecar container of the host pod (ptyd S3,
# docs/prd/progress/ptyd-s3.md, "The sidecar"), on the rehearsal variant
# (up.sh --rehearse: namespace k2-p4-rehearse, port 7445), never on the
# owner's k2-kind-dist.
#
#   scripts/kind/ptyd-sidecar-check.sh [--daemon-image=k2-daemon:p4]
#                                      [--fake-src=PATH]
#
# A controller and one host (k2-fake-agent as `claude`) with --ptyd=1: the
# host pod runs k2-ptyd as a native sidecar (an init container with
# restartPolicy Always) beside the daemon's container k2-host, sharing the
# K2 home, the workspaces, /tmp and the process namespace. Then:
#   1. the host pod has the sidecar; the agent's CLI is ptyd's child, in the
#      sidecar's container, and the agent answers a first message
#   2. a long turn starts on a second message; the daemon's container is
#      killed (SIGKILL to its main process and the daemon, from the
#      sidecar: a crash, no drain); the kubelet restarts that container
#      only
#   3. the CLI survives (same pid, never exited); the restarted daemon
#      adopts it (host.adopt), the controller adopts the placement at the
#      same epoch (ctl.adopt adopted, no gone)
#   4. the turn's message is not failed (delivered, sent once), and the
#      agent answers it (the turn ends; the reply in its transcript)
#   5. a message after the restart is delivered once and answered; the
#      sidecar itself never restarted
# Checks print ok/FAIL; the last line is "sidecar-check: PASS" or the
# failures. Evidence in $KIND_STATE/sidecar-<UTC>. Each run starts from a
# fresh k2-p4-rehearse (an earlier one is deleted first) and leaves it up
# for a look; delete it with kubectl delete namespace k2-p4-rehearse.
#
# Only k2lab, through an explicit kubeconfig; only namespace k2-p4-rehearse.
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
daemon_image=k2-daemon:p4 fake_src="${K2_KIND_FAKE_SRC:-$KIND_REPO}" long_ms=20000
for a in "$@"; do
    case "$a" in
        --daemon-image=*) daemon_image=${a#--daemon-image=} ;;
        --fake-src=*) fake_src=${a#--fake-src=} ;;
        -h|--help) sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "ptyd-sidecar-check.sh: unknown argument $a" >&2; exit 2 ;;
    esac
done
kind_variant rehearse
[ "$KIND_NS" = k2-p4-rehearse ] || { echo "refusing namespace $KIND_NS" >&2; exit 1; }
kind_kubeconfig
kn() { kc -n "$KIND_NS" "$@"; }
docker image inspect "$daemon_image" >/dev/null 2>&1 || { echo "no image $daemon_image" >&2; exit 1; }
docker tag "$daemon_image" k2-daemon:rehearse
if kc get namespace "$KIND_NS" >/dev/null 2>&1; then
    say "deleting the earlier $KIND_NS"
    kc delete namespace "$KIND_NS" --wait=true --timeout=300s >/dev/null
fi
RUN="$KIND_STATE/sidecar-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN"
fails=0
ok() { printf 'ok   %s\n' "$*" | tee -a "$RUN/checks.txt"; }
bad() { printf 'FAIL %s\n' "$*" | tee -a "$RUN/checks.txt"; fails=$((fails + 1)); }
check() { if [ "$1" = 1 ]; then ok "$2"; else bad "$2"; fi; }
W=/var/lib/k2/work
WS=$W/notes
H=/var/lib/k2-host
FT=$H/.k2-lab/traces/fake.jsonl

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
body() { printf '%s' "$1" | cut -d' ' -f2-; }
msg() {  # text wake → the message id
    kn exec k2-controller-0 -c k2-daemon -- sh -c '
        t=$(cat /var/lib/k2/.k2/daemon.token)
        curl -s -H "Authorization: Bearer $t" -X POST "http://127.0.0.1:7420/cli/workspace/msg" \
            --data-urlencode "workspace=$1" --data-urlencode "text=$2" --data-urlencode "from=sidecar-check" --data-urlencode "wake=$3"' \
        sh "$WS" "$1" "$2" | tee -a "$RUN/msgs.jsonl" | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("msgId") or "")' 2>/dev/null
}
state_of() {  # msg id → "state attempts"
    body "$(api GET "/cli/hosts/outbox?msg_id=$1")" | python3 -I -c '
import json,sys
m=(json.load(sys.stdin).get("message") or {})
print(m.get("state",""), m.get("attempts",""))' 2>/dev/null
}
ptyd_sh() { kn exec k2-host-0 -c k2-ptyd -- sh -c "$1"; }
host_sh() { kn exec k2-host-0 -c k2-host -- sh -c "$1"; }
ctl_sh() { kn exec k2-controller-0 -c k2-daemon -- sh -c "$1"; }
fake_count() { ptyd_sh "grep -c '\"event\":\"$1\"' $FT 2>/dev/null || true" | tr -d ' \r'; }
wait_for() { # seconds command…: until the command succeeds
    local n=$(( $1 / 2 )); shift
    for _ in $(seq 1 "$n"); do "$@" && return 0; sleep 2; done
    return 1
}
more_than() { [ "$(fake_count "$1")" -gt "$2" ]; }
linked() { host_sh 'curl -fsS --max-time 3 http://127.0.0.1:7420/cli/hosts/status' 2>/dev/null | grep -q '"linked":true'; }
is_state() { [ "$(state_of "$1" | cut -d' ' -f1)" = "$2" ]; }
in_transcript() { ptyd_sh "grep -rl '$1' $H/.claude/projects 2>/dev/null | grep -q ." ; }
restarts() { kn get pod k2-host-0 -o jsonpath="{.status.$1[?(@.name==\"$2\")].restartCount}"; }
# A pid's parent (the shared process namespace: every container's
# processes are visible from the sidecar).
ppid_of() { ptyd_sh "awk '/^PPid:/{print \$2}' /proc/$1/status" | tr -d '\r'; }
# The CLI's pid, from the fake agent's own cli.exec trace.
cli_pid() { ptyd_sh "grep '\"event\":\"cli.exec\"' $FT | tail -1" | python3 -I -c 'import json,sys; print(json.loads(sys.stdin.read() or "{}").get("pid",""))'; }

# ── up ─────────────────────────────────────────────────────────────────
K2_KIND_FAKE_SRC="$fake_src" "$KIND_REPO/scripts/kind/up.sh" --rehearse --hosts=1 --ptyd=1 --work-root="$W" \
    --password-file="$KIND_STATE/owner-password" >"$RUN/up.log" 2>&1 || { bad "up.sh: see $RUN/up.log"; exit 1; }
wait_for 120 linked || { bad "k2-host-0 never linked"; exit 1; }
kn get pod k2-host-0 -o json > "$RUN/host-pod.json"
sidecar=$(python3 -I -c '
import json,sys
p=json.load(open(sys.argv[1]))
ic=[c for c in p["spec"].get("initContainers",[]) if c["name"]=="k2-ptyd"]
print("yes" if ic and ic[0].get("restartPolicy")=="Always" and p["spec"].get("shareProcessNamespace") else "no")' "$RUN/host-pod.json")
check "$( [ "$sidecar" = yes ] && host_sh 'printenv K2_PTYD_SIDECAR' | grep -qx 1 && echo 1)" "the host pod runs k2-ptyd as a native sidecar, the process namespace shared, the daemon in sidecar mode"

# ── 1. the agent, under the sidecar's ptyd ─────────────────────────────
ctl_sh "set -e; mkdir -p $WS; cd $WS; printf '# notes\n' > README.md
    git init -q -b main . && git -c user.name=r -c user.email=r@x.invalid add -A && git -c user.name=r -c user.email=r@x.invalid commit -qm seed"
r=$(api POST /cli/projects/add-from-path "{\"path\":\"$WS\",\"seed_wiki\":false,\"seed_agents_md\":false}")
agent=$(body "$r" | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("id",""))' 2>/dev/null)
r=$(api POST /cli/projects/update "{\"id\":\"$agent\",\"agentEnabled\":1}")
echo "agent $agent" > "$RUN/agent.txt"
m1=$(msg "first before-restart-marker" true)
wait_for 120 more_than cli.turn.end 0 || true
wait_for 30 is_state "$m1" delivered || true
cli=$(cli_pid)
cli_parent=$(ppid_of "$cli")
parent_cmd=$(ptyd_sh "tr '\\0' ' ' < /proc/$cli_parent/cmdline" | tr -d '\r')
check "$( [ -n "$m1" ] && is_state "$m1" delivered && [ -n "$cli" ] && echo "$parent_cmd" | grep -q 'k2-ptyd --serve' && echo 1)" \
    "the agent answers a first message; its CLI (pid $cli) is the sidecar's ptyd's child ($parent_cmd)"

# ── 2. a long turn; kill the daemon's container mid-turn ───────────────
starts=$(fake_count cli.turn.start)
m2=$(msg "second [[turn-ms=$long_ms]] mid-turn-marker" false)
wait_for 60 more_than cli.turn.start "$starts" || true
ends=$(fake_count cli.turn.end)
host_restarts=$(restarts containerStatuses k2-host)
sidecar_restarts=$(restarts initContainerStatuses k2-ptyd)
ctl_lines=$(ctl_sh 'wc -l < /var/lib/k2/.k2-lab/traces/daemon.jsonl' | tr -d ' \r')
# The daemon (supervise.sh writes its pid) and the container's main process,
# supervise.sh itself (the daemon's parent).
daemon=$(ptyd_sh "cat $H/.k2-lab/daemon.pid" | tr -d '\r')
main=$(ppid_of "$daemon")
ptyd_sh "kill -9 $main $daemon" || true
say "killed the daemon's container: supervise.sh $main, k2-daemon $daemon (SIGKILL)"
restarted() { [ "$(restarts containerStatuses k2-host)" -gt "$host_restarts" ]; }
wait_for 120 restarted || true
check "$(restarted && [ "$(fake_count cli.turn.end)" = "$ends" ] && echo 1)" \
    "a long turn started; the daemon's container was killed mid-turn and restarted by the kubelet (restarts $host_restarts → $(restarts containerStatuses k2-host))"

# ── 3. the CLI survives and is adopted ─────────────────────────────────
wait_for 120 linked || true
ptyd_sh "test -d /proc/$cli" && alive=yes || alive=no
adopted=$(ptyd_sh "grep -c '\"event\":\"host.adopt\"' $H/.k2-lab/traces/daemon.jsonl 2>/dev/null || true" | tr -d ' \r')
decisions=$(ctl_sh "tail -n +$((ctl_lines + 1)) /var/lib/k2/.k2-lab/traces/daemon.jsonl" | python3 -I -c '
import json,sys
for l in sys.stdin:
    e=json.loads(l)
    if e.get("event")=="ctl.adopt" and e.get("agent_id")==sys.argv[1]: print((e.get("attrs") or {}).get("decision"))' "$agent" | tr '\n' ' ')
check "$( [ "$alive" = yes ] && [ "${adopted:-0}" -ge 1 ] && [ "$decisions" = "adopted " ] && echo 1)" \
    "the CLI survives (pid $cli alive: $alive); the restarted daemon adopted it (host.adopt $adopted); the controller: ${decisions:-none}"

# ── 4. the turn's message: not failed, answered ────────────────────────
wait_for $(( long_ms / 1000 + 60 )) more_than cli.turn.end "$ends" || true
wait_for 30 in_transcript "reply #2" || true
sleep 3
st2=$(state_of "$m2")
check "$( [ "$st2" = "delivered 1" ] && more_than cli.turn.end "$ends" && in_transcript "reply #2" && echo 1)" \
    "the turn's message is not failed ($st2) and the agent answers it (turn ends $(fake_count cli.turn.end))"

# ── 5. after the restart ───────────────────────────────────────────────
m3=$(msg "third after-restart-marker" false)
wait_for 60 is_state "$m3" delivered || true
wait_for 60 in_transcript "reply #3" || true
st3=$(state_of "$m3")
check "$( [ "$st3" = "delivered 1" ] && in_transcript "reply #3" && [ "$(fake_count cli.exec)" = 1 ] && [ "$(fake_count cli.exit)" = 0 ] \
    && [ "$(restarts initContainerStatuses k2-ptyd)" = "$sidecar_restarts" ] && echo 1)" \
    "a message after the restart is delivered once and answered ($st3); one CLI exec, no exit; the sidecar never restarted"

kn get pod k2-host-0 -o wide > "$RUN/pods.txt" 2>&1 || true
kn logs k2-host-0 -c k2-host --previous > "$RUN/host-previous.log" 2>&1 || true
kn logs k2-host-0 -c k2-host > "$RUN/host.log" 2>&1 || true
say "evidence: $RUN"
if [ "$fails" = 0 ]; then echo "sidecar-check: PASS"; exit 0; fi
echo "sidecar-check: $fails check(s) failed"
exit 1
