#!/usr/bin/env bash
# TEST ONLY (P4 T4, design section 5 of docs/prd/progress/p4-plan.md): drain
# an agent's host mid-conversation on kind; the agent comes back on the
# other host with the same session and its memory. Namespace k2-kind-p4
# (scripts/kind/p4-up.sh), k2lab only, through an explicit kubeconfig.
#
#   scripts/kind/drain-demo.sh [--turn-ms=30000]
#
# The drain is scoped to k2-kind-p4. `kubectl drain` and `kubectl cordon`
# act on a whole node: on k2lab they would move other namespaces' pods (the
# owner's k2-kind-dist among them), so they are never used. Instead:
#   - "cordon": the node gets the label k2.dev/p4-drained=true, which only
#     k2-kind-p4's hosts refuse (deploy/kind/p4: a required node affinity);
#   - "drain": the host's pod is evicted through the Eviction API, which is
#     what `kubectl drain` calls: the PDB is honoured and the pod gets
#     SIGTERM and its grace period;
#   - "uncordon": the label goes.
# Nothing else on the node changes.
#
# Checks:
#   1. agent demo (a registered workspace's chat, the lab's fake agent) is
#      placed on a host (SRC); its first turn remembers a fact in MEMORY.md
#   2. a long turn ([[turn-ms]]) is in progress on SRC
#   3. the PDB allows one eviction; SRC's node is "cordoned" and SRC
#      evicted; SRC goes Pending (its local-path volumes are on that node)
#   4. the controller marks SRC draining (ctl.host_draining) and gets the
#      release; nothing else is placed on SRC
#   5. the next message places demo on the other host (DST) at a higher
#      epoch; DST restores the home SRC saved (host.home_restored,
#      fetched > 0) and the CLI resumes the same session (--resume)
#   6. on DST, MEMORY.md begins with SRC's bytes (sha256 equal) and holds
#      every fact: the long turn finished inside the drain window and
#      nothing was lost silently (I2)
#   7. SRC is Pending for the node affinity while cordoned; after
#      "uncordon" it runs again and relinks by its key (no new
#      enrollment); its trace has the drain (host.drain_start, the release
#      save, host.drain_done); demo stays on DST with one live CLI (I1)
# Evidence: lab-runs/kind-p4/drain-<UTC>/ (checks, traces, logs, events).
# Prints no token: the controller is called inside its pod.
set -uo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
turn_ms=30000
for a in "$@"; do
    case "$a" in
        --turn-ms=*) turn_ms=${a#--turn-ms=} ;;
        -h|--help) sed -n '2,43p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "drain-demo.sh: unknown argument $a" >&2; exit 2 ;;
    esac
done
kind_variant p4
kind_kubeconfig || exit 1
[ "$KIND_NS" = k2-kind-p4 ] || { echo "drain-demo.sh: refusing namespace $KIND_NS" >&2; exit 1; }
LABEL=k2.dev/p4-drained
RUN="$KIND_REPO/lab-runs/kind-p4/drain-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN"
kn() { kubectl --kubeconfig "$KIND_KUBECONFIG" -n "$KIND_NS" "$@"; }
kc() { kubectl --kubeconfig "$KIND_KUBECONFIG" "$@"; }

FAILS=0
check() { # name ok detail
    local line="PASS $1 -- $3"
    if [ "$2" != 1 ]; then line="FAIL $1 -- $3"; FAILS=$((FAILS + 1)); fi
    echo "$line" | tee -a "$RUN/checks.txt"
}
wait_for() { # seconds command…
    local end=$(( $(date +%s) + $1 )); shift
    until "$@"; do [ "$(date +%s)" -lt "$end" ] || return 1; sleep 1; done
}

# The controller's API inside its pod, its token read there.
ctl() { # method path [form fields as k=v…]: each one --data-urlencode'd
    local m=$1 p=$2 kv args=(); shift 2
    for kv in "$@"; do args+=(--data-urlencode "$kv"); done
    kn exec k2-controller-0 -c k2-daemon -- sh -c \
        'm=$1 p=$2; shift 2; curl -sS --max-time 60 -X "$m" "$@" "http://127.0.0.1:7420$p?token=$(cat /var/lib/k2/.k2/daemon.token)"' \
        _ "$m" "$p" ${args[@]+"${args[@]}"} 2>&1
}
ctl_json() { # method path json
    kn exec k2-controller-0 -c k2-daemon -- sh -c \
        'curl -sS --max-time 60 -X "$1" -H "Content-Type: application/json" -d "$3" "http://127.0.0.1:7420$2?token=$(cat /var/lib/k2/.k2/daemon.token)"' \
        _ "$1" "$2" "$3" 2>&1
}
msg() { # text wake
    ctl POST /cli/workspace/msg "workspace=$WS" "text=$1" "from=drain-demo" "wake=$2"
}
field() { python3 -I -c 'import json,sys
try: d = json.load(sys.stdin)
except ValueError: sys.exit(0)
for k in sys.argv[1].split("."): d = d.get(k) if isinstance(d, dict) else None
print("" if d is None else d)' "$1"; }

ctl_log() { kn logs k2-controller-0 -c k2-daemon 2>/dev/null; }
placed_on() { ctl_log | grep -E "\[hosts\] placed agent $AGENT on host " | tail -1 | sed -E 's/.* on host ([^ ]+) .*/\1/'; }
placed_epoch() { ctl_log | grep -E "\[hosts\] placed agent $AGENT on host " | tail -1 | sed -E 's/.* epoch ([0-9]+).*/\1/'; }
trace() { # pod file: the lab trace (daemon.jsonl or fake.jsonl) on the pod's own volume
    local home=/var/lib/k2-host
    [ "$1" = k2-controller-0 ] && home=/var/lib/k2
    kn exec "$1" -c "$( [ "$1" = k2-controller-0 ] && echo k2-daemon || echo k2-host)" -- cat "$home/.k2-lab/traces/$2" 2>/dev/null
}
events() { # pod file event: matching lines
    trace "$1" "$2" | grep -F "\"event\":\"$3\""
}
count() { events "$@" | wc -l | tr -d ' '; }
memory() { # pod: MEMORY.md's path in the agent's Claude projects dir
    kn exec "$1" -c k2-host -- sh -c 'ls /var/lib/k2-host/.claude/projects/*-demo/memory/MEMORY.md 2>/dev/null | head -1'
}
mem_has() { local f; f=$(memory "$1"); [ -n "$f" ] && kn exec "$1" -c k2-host -- grep -q "$2" "$f"; }
mem_sha() { # pod [bytes]
    local f; f=$(memory "$1")
    [ -n "$f" ] || { echo none; return; }
    if [ -n "${2:-}" ]; then kn exec "$1" -c k2-host -- sh -c 'head -c "$1" "$2" | sha256sum' _ "$2" "$f" | cut -c1-64
    else kn exec "$1" -c k2-host -- sha256sum "$f" | cut -c1-64; fi
}
pod_phase() { kn get pod "$1" -o jsonpath='{.status.phase}' 2>/dev/null; }
pod_uid() { kn get pod "$1" -o jsonpath='{.metadata.uid}' 2>/dev/null; }
linked() { kn exec "$1" -c k2-host -- sh -c 'curl -fsS --max-time 3 http://127.0.0.1:7420/cli/hosts/status | grep -q "\"linked\":true"' 2>/dev/null; }

uncordon() { [ -n "${NODE:-}" ] && kc label node "$NODE" "$LABEL-" >/dev/null 2>&1; }
trap uncordon EXIT

# ── 0. preflight ───────────────────────────────────────────────────────
for h in k2-host-0 k2-host-1; do
    [ "$(pod_phase "$h")" = Running ] && linked "$h" || { echo "drain-demo.sh: $h is not running and linked (scripts/kind/p4-up.sh)" >&2; exit 1; }
done
if kc get nodes -l "$LABEL" -o name | grep -q .; then
    echo "drain-demo.sh: a node is already labelled $LABEL: $(kc get nodes -l "$LABEL" -o name | tr '\n' ' ')" >&2; exit 1
fi

# ── 1. the agent, its first turn ───────────────────────────────────────
WS=/var/lib/k2/work/demo
kn exec k2-controller-0 -c k2-daemon -- sh -c '
    set -e; rm -rf "$1"; mkdir -p "$1"; cd "$1"
    git init -q -b main; printf "# demo\n" > README.md
    git -c user.name=demo -c user.email=demo@example.invalid add -A
    git -c user.name=demo -c user.email=demo@example.invalid commit -q -m first' _ "$WS" \
    || { echo "drain-demo.sh: workspace" >&2; exit 1; }
r=$(ctl_json POST /cli/projects/add-from-path "{\"path\":\"$WS\",\"seed_wiki\":false,\"seed_agents_md\":false}")
AGENT=$(printf '%s' "$r" | field id)
[ -n "$AGENT" ] || { echo "drain-demo.sh: add-from-path: $(printf '%s' "$r" | cut -c1-300)" >&2; exit 1; }
ctl_json POST /cli/projects/update "{\"id\":\"$AGENT\",\"agentEnabled\":1}" > "$RUN/update.json"
echo "agent $AGENT (workspace $WS)" | tee "$RUN/agent.txt"

FACT="the drain demo's fact $(date -u +%H%M%S)"
c=$(msg "first [[remember: $FACT]]" true)
has_placement() { [ -n "$(placed_on)" ]; }
wait_for 120 has_placement || true
SRC=$(placed_on)
case "$SRC" in k2-host-0) DST=k2-host-1 ;; k2-host-1) DST=k2-host-0 ;; *) DST= ;; esac
wait_for 120 mem_has "$SRC" "$FACT" || true
SESSION=$(events "$SRC" fake.jsonl cli.exec | tail -1 | field session_id)
EPOCH1=$(placed_epoch)
check "demo is placed on a host and its first turn remembers a fact in MEMORY.md" \
    "$([ -n "$DST" ] && mem_has "$SRC" "$FACT" && [ -n "$SESSION" ] && echo 1)" \
    "on ${SRC:-nowhere} (epoch $EPOCH1), session $SESSION; msg: $(printf '%s' "$c" | cut -c1-120)"
[ -n "$DST" ] || exit 1
NODE=$(kn get pod "$SRC" -o jsonpath='{.spec.nodeName}')
SRC_UID=$(pod_uid "$SRC")

# ── 2. a long turn in progress ─────────────────────────────────────────
starts=$(count "$SRC" fake.jsonl cli.turn.start)
c=$(msg "second [[turn-ms=$turn_ms]] [[remember: the long turn finished]]" false)
wait_for 60 sh -c "[ \$(kubectl --kubeconfig '$KIND_KUBECONFIG' -n '$KIND_NS' exec '$SRC' -c k2-host -- cat /var/lib/k2-host/.k2-lab/traces/fake.jsonl 2>/dev/null | grep -c '\"event\":\"cli.turn.start\"') -gt $starts ]" || true
ends=$(count "$SRC" fake.jsonl cli.turn.end)
check "a long turn (${turn_ms} ms) is in progress on $SRC" \
    "$([ "$(count "$SRC" fake.jsonl cli.turn.start)" -gt "$starts" ] && [ "$(count "$SRC" fake.jsonl cli.turn.start)" -gt "$ends" ] && echo 1)" \
    "turn starts $(count "$SRC" fake.jsonl cli.turn.start) (before $starts), ends $ends; msg: $(printf '%s' "$c" | cut -c1-120)"
MEM1_BYTES=$(kn exec "$SRC" -c k2-host -- wc -c "$(memory "$SRC")" | awk '{print $1}')
MEM1_SHA=$(mem_sha "$SRC")

# ── 3. cordon (scoped), evict ──────────────────────────────────────────
allowed=$(kn get pdb k2-host -o jsonpath='{.status.disruptionsAllowed}')
kc label node "$NODE" "$LABEL=true" >/dev/null
t_evict=$(date -u +%Y-%m-%dT%H:%M:%SZ)
kn create --raw "/api/v1/namespaces/$KIND_NS/pods/$SRC/eviction" -f - > "$RUN/eviction.json" 2>&1 <<EOF
{"apiVersion":"policy/v1","kind":"Eviction","metadata":{"name":"$SRC","namespace":"$KIND_NS"}}
EOF
erc=$?
# The old pod drains (up to its grace); the new one can't be scheduled.
wait_for 180 sh -c "[ \"\$(kubectl --kubeconfig '$KIND_KUBECONFIG' -n '$KIND_NS' get pod '$SRC' -o jsonpath='{.metadata.uid}' 2>/dev/null)\" != '$SRC_UID' ]" || true
wait_for 60 sh -c "[ \"\$(kubectl --kubeconfig '$KIND_KUBECONFIG' -n '$KIND_NS' get pod '$SRC' -o jsonpath='{.status.phase}' 2>/dev/null)\" = Pending ]" || true
t_gone=$(date -u +%Y-%m-%dT%H:%M:%SZ)
check "the PDB allows one eviction; $NODE is cordoned for k2-kind-p4 only and $SRC evicted; it comes back Pending" \
    "$([ "$allowed" = 1 ] && [ "$erc" = 0 ] && [ "$(pod_uid "$SRC")" != "$SRC_UID" ] && [ "$(pod_phase "$SRC")" = Pending ] && echo 1)" \
    "disruptionsAllowed $allowed; eviction rc $erc at $t_evict; old pod gone by $t_gone; now $(pod_phase "$SRC")"

# ── 4. the controller saw the drain ────────────────────────────────────
draining=$(trace k2-controller-0 daemon.jsonl | grep -F '"event":"ctl.host_draining"' | grep -c "$SRC")
check "the controller marked $SRC draining and got the release; demo isn't placed there again" \
    "$([ "$draining" -ge 1 ] && [ "$(placed_on)" = "$SRC" ] && [ "$(placed_epoch)" = "$EPOCH1" ] && echo 1)" \
    "ctl.host_draining for $SRC: $draining; last placement $(placed_on) epoch $(placed_epoch)"
ctl_log | grep -E "\[hosts\].*($SRC|$AGENT)" | tail -20 > "$RUN/controller-drain.log"

# ── 5. the next message: placed on DST, home restored, same session ───
c=$(msg "third [[remember: after the drain]]" true)
wait_for 120 sh -c "kubectl --kubeconfig '$KIND_KUBECONFIG' -n '$KIND_NS' logs k2-controller-0 -c k2-daemon | grep -q 'placed agent $AGENT on host $DST'" || true
wait_for 120 mem_has "$DST" "after the drain" || true
EPOCH2=$(placed_epoch)
restored=$(events "$DST" daemon.jsonl host.home_restored | tail -1)
fetched=$(printf '%s' "$restored" | field attrs.fetched)
EX=$(events "$DST" fake.jsonl cli.exec | tail -1)
resumed=$(printf '%s' "$EX" | python3 -I -c '
import json,sys
e=json.loads(sys.stdin.read() or "{}"); a=e.get("attrs") or {}
print(1 if e.get("session_id")==sys.argv[1] and "--resume" in (a.get("argv") or []) else 0)' "$SESSION")
check "the next message places demo on $DST at a higher epoch; $DST restores $SRC's home and resumes the same session" \
    "$([ "$(placed_on)" = "$DST" ] && [ "${EPOCH2:-0}" -gt "${EPOCH1:-0}" ] && [ "${fetched:-0}" -gt 0 ] && [ "$resumed" = 1 ] && echo 1)" \
    "on $(placed_on) epoch $EPOCH2 (was $EPOCH1); restored fetched=${fetched:-none}; exec session $(printf '%s' "$EX" | field session_id) resume=$resumed; msg: $(printf '%s' "$c" | cut -c1-100)"

# ── 6. the memory, byte for byte, and nothing lost ─────────────────────
prefix=$(mem_sha "$DST" "$MEM1_BYTES")
check "on $DST, MEMORY.md begins with $SRC's (sha256 equal) and holds every fact: the long turn finished in the drain window (I2)" \
    "$([ "$prefix" = "$MEM1_SHA" ] && mem_has "$DST" "$FACT" && mem_has "$DST" "the long turn finished" && mem_has "$DST" "after the drain" && echo 1)" \
    "first $MEM1_BYTES bytes $prefix vs $MEM1_SHA; facts: one $(mem_has "$DST" "$FACT" && echo yes || echo NO), long turn $(mem_has "$DST" "the long turn finished" && echo yes || echo NO), after $(mem_has "$DST" "after the drain" && echo yes || echo NO)"

# ── 7. Pending while cordoned; uncordon; relink by key; one live CLI ───
reason=$(kn get events --field-selector "involvedObject.name=$SRC,reason=FailedScheduling" -o jsonpath='{.items[-1:].message}' 2>/dev/null)
pending_ok=$([ "$(pod_phase "$SRC")" = Pending ] && printf '%s' "$reason" | grep -q "affinity" && echo 1)
enrolled_before=$(trace k2-controller-0 daemon.jsonl | grep -cF '"event":"ctl.host_enrolled"')
uncordon
NODE=
wait_for 300 sh -c "[ \"\$(kubectl --kubeconfig '$KIND_KUBECONFIG' -n '$KIND_NS' get pod '$SRC' -o jsonpath='{.status.phase}')\" = Running ]" || true
wait_for 180 linked "$SRC" || true
enrolled_after=$(trace k2-controller-0 daemon.jsonl | grep -cF '"event":"ctl.host_enrolled"')
drain_start=$(count "$SRC" daemon.jsonl host.drain_start)
drain_done=$(count "$SRC" daemon.jsonl host.drain_done)
release_saves=$(events "$SRC" daemon.jsonl host.home_saved | grep -c '"trigger":"release"')
sleep 10
live_src=$(kn exec "$SRC" -c k2-host -- sh -c 'pgrep -f k2-fake-agent | wc -l' 2>/dev/null | tr -d ' ')
check "$SRC was Pending for the affinity while cordoned; uncordoned it relinks by its key; its trace has the drain; demo stays on $DST, no CLI on $SRC (I1)" \
    "$([ "$pending_ok" = 1 ] && linked "$SRC" && [ "$enrolled_after" = "$enrolled_before" ] && [ "$drain_start" -ge 1 ] && [ "$drain_done" -ge 1 ] && [ "$release_saves" -ge 1 ] && [ "$(placed_on)" = "$DST" ] && [ "${live_src:-1}" = 0 ] && echo 1)" \
    "Pending: $(printf '%s' "$reason" | cut -c1-120); enrollments $enrolled_before -> $enrolled_after; drain_start $drain_start drain_done $drain_done release saves $release_saves; on $(placed_on); fake CLIs on $SRC: $live_src"

# ── evidence ───────────────────────────────────────────────────────────
for p in k2-controller-0 k2-host-0 k2-host-1; do
    trace "$p" daemon.jsonl > "$RUN/$p.trace.jsonl"
    [ "$p" = k2-controller-0 ] || trace "$p" fake.jsonl > "$RUN/$p.fake.jsonl"
done
ctl_log > "$RUN/controller.log"
kn get events --sort-by=.lastTimestamp > "$RUN/events.txt" 2>&1
kn get pods,pvc,pdb -o wide > "$RUN/objects.txt" 2>&1
echo "evidence: $RUN"
if [ "$FAILS" = 0 ]; then echo "PASS drain-demo"; else echo "FAIL drain-demo: $FAILS check(s)"; exit 1; fi
