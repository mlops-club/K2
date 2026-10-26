#!/usr/bin/env bash
# TEST ONLY (P4 T4): an agent's home moves between two hosts on different
# kind nodes with no shared workspaces volume (deploy/kind/p4, namespace
# k2-kind-p4, scripts/kind/p4-up.sh). The move is rc1's lost-host move
# (S10, K2_HOST_MOVE_AFTER, default 300 s): the agent's host daemon and its
# CLI are killed under the lab's supervise hold (no drain, so the drained-
# agent gap that k2-dist/drain-move fixes is not in the way).
#
#   scripts/kind/home-move-check.sh [--move-wait=600]
#
# Checks:
#   1. the hosts run on different nodes; neither has a k2-work claim
#   2. agent "move" (a registered workspace's chat) is placed on SRC; its
#      first turn remembers a fact; a file the agent "wrote" in its
#      workspace on SRC and a second turn: saved at the turn's end
#   3. SRC's daemon and the agent CLI are killed and held down
#   4. the controller moves the agent (ctl.move_decision) to DST, on the
#      other node, at a higher epoch; DST restores the home (fetched > 0)
#      and resumes the same session
#   5. on DST: the workspace file, MEMORY.md (SRC's bytes first) and the
#      transcript; a message after the move lands there
#   6. SRC released: it relinks, the agent stays on DST, one live CLI (I1)
# And reports, not as pass/fail, what the controller sees without a shared
# volume: its copy of the workspace (the files the app browses) against
# the agent's on DST.
# Evidence: lab-runs/kind-p4/home-move-<UTC>/. Prints no token.
set -uo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
move_wait=600
for a in "$@"; do
    case "$a" in
        --move-wait=*) move_wait=${a#--move-wait=} ;;
        -h|--help) sed -n '2,28p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "home-move-check.sh: unknown argument $a" >&2; exit 2 ;;
    esac
done
kind_variant p4
kind_kubeconfig || exit 1
[ "$KIND_NS" = k2-kind-p4 ] || { echo "home-move-check.sh: refusing namespace $KIND_NS" >&2; exit 1; }
RUN="$KIND_REPO/lab-runs/kind-p4/home-move-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN"
kn() { kubectl --kubeconfig "$KIND_KUBECONFIG" -n "$KIND_NS" "$@"; }
FAILS=0
check() {
    local line="PASS $1 -- $3"
    if [ "$2" != 1 ]; then line="FAIL $1 -- $3"; FAILS=$((FAILS + 1)); fi
    echo "$line" | tee -a "$RUN/checks.txt"
}
note() { echo "NOTE $*" | tee -a "$RUN/checks.txt"; }
wait_for() { local end=$(( $(date +%s) + $1 )); shift; until "$@"; do [ "$(date +%s)" -lt "$end" ] || return 1; sleep 2; done; }
field() { python3 -I -c 'import json,sys
try: d = json.load(sys.stdin)
except ValueError: sys.exit(0)
for k in sys.argv[1].split("."): d = d.get(k) if isinstance(d, dict) else None
print("" if d is None else d)' "$1"; }
hx() { kn exec "$1" -c k2-host -- "${@:2}"; }

ctl() { # method path [form fields]
    local m=$1 p=$2 kv args=(); shift 2
    for kv in "$@"; do args+=(--data-urlencode "$kv"); done
    kn exec k2-controller-0 -c k2-daemon -- sh -c \
        'm=$1 p=$2; shift 2; curl -sS --max-time 60 -X "$m" "$@" "http://127.0.0.1:7420$p?token=$(cat /var/lib/k2/.k2/daemon.token)"' \
        _ "$m" "$p" ${args[@]+"${args[@]}"} 2>&1
}
ctl_json() {
    kn exec k2-controller-0 -c k2-daemon -- sh -c \
        'curl -sS --max-time 60 -X "$1" -H "Content-Type: application/json" -d "$3" "http://127.0.0.1:7420$2?token=$(cat /var/lib/k2/.k2/daemon.token)"' \
        _ "$1" "$2" "$3" 2>&1
}
msg() { ctl POST /cli/workspace/msg "workspace=$WS" "text=$1" "from=home-move-check" "wake=$2"; }
ctl_log() { kn logs k2-controller-0 -c k2-daemon 2>/dev/null; }
placed_on() { ctl_log | grep -E "\[hosts\] placed agent $AGENT on host " | tail -1 | sed -E 's/.* on host ([^ ]+) .*/\1/'; }
placed_epoch() { ctl_log | grep -E "\[hosts\] placed agent $AGENT on host " | tail -1 | sed -E 's/.* epoch ([0-9]+).*/\1/'; }
trace() { # pod file
    if [ "$1" = k2-controller-0 ]; then kn exec "$1" -c k2-daemon -- cat "/var/lib/k2/.k2-lab/traces/$2" 2>/dev/null
    else hx "$1" cat "/var/lib/k2-host/.k2-lab/traces/$2" 2>/dev/null; fi
}
events() { trace "$1" "$2" | grep -F "\"event\":\"$3\""; }
count() { events "$@" | wc -l | tr -d ' '; }
# The last cli.exec of this agent's workspace on a host (other agents may
# run there too: a lost host's agents all move).
exec_of() { events "$1" fake.jsonl cli.exec | python3 -I -c '
import json,sys
last=""
for l in sys.stdin:
    if ((json.loads(l).get("attrs") or {}).get("cwd") or "").startswith(sys.argv[1]): last=l.strip()
print(last)' "$WS"; }
memory() { hx "$1" sh -c 'ls /var/lib/k2-host/.claude/projects/*-"$1"/memory/MEMORY.md 2>/dev/null | head -1' _ "$NAME"; }
mem_has() { local f; f=$(memory "$1"); [ -n "$f" ] && hx "$1" grep -q "$2" "$f"; }
mem_sha() { local f; f=$(memory "$1"); [ -n "$f" ] || { echo none; return; }
    if [ -n "${2:-}" ]; then hx "$1" sh -c 'head -c "$1" "$2" | sha256sum' _ "$2" "$f" | cut -c1-64; else hx "$1" sha256sum "$f" | cut -c1-64; fi; }
linked() { hx "$1" sh -c 'curl -fsS --max-time 3 http://127.0.0.1:7420/cli/hosts/status | grep -q "\"linked\":true"' 2>/dev/null; }
# Live fake-agent CLIs in a pod (the image has no ps/pgrep: /proc). Our own
# sh is skipped: its command line holds the pattern too.
clis() { hx "$1" sh -c 'n=0; for p in /proc/[0-9]*; do [ "${p#/proc/}" = $$ ] && continue; case "$(tr "\0" " " < $p/cmdline 2>/dev/null)" in /opt/k2-fake/shim/*|*/k2-fake-agent\ *) n=$((n+1));; esac; done; echo $n' 2>/dev/null; }
ws_state() { # pod container dir: files and their hashes, git HEAD (no .git internals)
    kn exec "$1" -c "$2" -- sh -c 'cd "$1" 2>/dev/null || { echo "(no directory)"; exit 0; }
        git rev-parse HEAD 2>/dev/null || echo "(no commit)"
        find . -path ./.git -prune -o -path ./.k2 -prune -o -type f -print | LC_ALL=C sort | while read -r f; do echo "$(sha256sum "$f" | cut -c1-16) $f"; done' _ "$3" 2>&1
}

# ── 1. the layout ───────────────────────────────────────────────────────
n0=$(kn get pod k2-host-0 -o jsonpath='{.spec.nodeName}'); n1=$(kn get pod k2-host-1 -o jsonpath='{.spec.nodeName}')
claims=$(kn get pvc -o name | tr '\n' ' ')
check "the hosts run on different nodes, each with its own volumes; no shared k2-work" \
    "$([ -n "$n0" ] && [ -n "$n1" ] && [ "$n0" != "$n1" ] && ! kn get pvc k2-work >/dev/null 2>&1 && linked k2-host-0 && linked k2-host-1 && echo 1)" \
    "k2-host-0 on $n0, k2-host-1 on $n1; claims: $claims"

# ── 2. the agent, two turns on SRC ─────────────────────────────────────
NAME=move$(date -u +%H%M%S)   # a new workspace (and agent) per run
WS=/var/lib/k2/work/$NAME
kn exec k2-controller-0 -c k2-daemon -- sh -c '
    set -e; rm -rf "$1"; mkdir -p "$1"; cd "$1"
    git init -q -b main; printf "# move\n" > README.md
    git -c user.name=check -c user.email=check@example.invalid add -A
    git -c user.name=check -c user.email=check@example.invalid commit -q -m first' _ "$WS" || exit 1
r=$(ctl_json POST /cli/projects/add-from-path "{\"path\":\"$WS\",\"seed_wiki\":false,\"seed_agents_md\":false}")
AGENT=$(printf '%s' "$r" | field id)
[ -n "$AGENT" ] || { echo "home-move-check.sh: add-from-path: $(printf '%s' "$r" | cut -c1-300)" >&2; exit 1; }
ctl_json POST /cli/projects/update "{\"id\":\"$AGENT\",\"agentEnabled\":1}" > "$RUN/update.json"
echo "agent $AGENT (workspace $WS)" | tee "$RUN/agent.txt"
FACT="home-move fact $(date -u +%H%M%S)"
c1=$(msg "first [[remember: $FACT]]" true)
has_placement() { [ -n "$(placed_on)" ]; }
wait_for 120 has_placement || true
SRC=$(placed_on)
case "$SRC" in k2-host-0) DST=k2-host-1 ;; k2-host-1) DST=k2-host-0 ;; *) DST= ;; esac
[ -n "$DST" ] || { check "the agent is placed" 0 "msg: $(printf '%s' "$c1" | cut -c1-200)"; exit 1; }
wait_for 120 mem_has "$SRC" "$FACT" || true
ws_state k2-controller-0 k2-daemon "$WS" > "$RUN/workspace-controller-start.txt"
ws_state "$SRC" k2-host "$WS" > "$RUN/workspace-src-first-turn.txt"
# What the agent changed in its workspace (the fake agent writes only
# MEMORY.md; the lab edits the workspace the same way, from outside).
hx "$SRC" sh -c 'printf "written on %s\n" "$1" > "$2/notes.txt"' _ "$SRC" "$WS"
saves=$(events "$SRC" daemon.jsonl host.home_saved | grep -c '"trigger":"turn"')
c2=$(msg "second [[remember: the second turn]]" false); echo "second: $c2" >> "$RUN/msgs.txt"
more_saves() { [ "$(events "$SRC" daemon.jsonl host.home_saved | grep -c '"trigger":"turn"')" -gt "$saves" ]; }
wait_for 120 more_saves || true
wait_for 60 mem_has "$SRC" "the second turn" || true
SESSION=$(exec_of "$SRC" | field session_id)
EPOCH1=$(placed_epoch)
MEM_BYTES=$(hx "$SRC" wc -c "$(memory "$SRC")" | awk '{print $1}')
MEM_SHA=$(mem_sha "$SRC")
ws_state "$SRC" k2-host "$WS" > "$RUN/workspace-src.txt"
check "the agent runs on $SRC ($( [ "$SRC" = k2-host-0 ] && echo "$n0" || echo "$n1")); two turns, the second saved at its end with the workspace file" \
    "$(mem_has "$SRC" "$FACT" && mem_has "$SRC" "the second turn" && more_saves && [ -n "$SESSION" ] && grep -q notes.txt "$RUN/workspace-src.txt" && echo 1)" \
    "session $SESSION, epoch $EPOCH1; turn saves $(events "$SRC" daemon.jsonl host.home_saved | grep -c '"trigger":"turn"') (before $saves)"

# ── 3. SRC lost: its daemon and the CLI killed, held down ──────────────
# The daemon first (held down by supervise.sh), then what outlives it: with
# K2_PTYD=1 the CLIs run under k2-ptyd, which keeps them on purpose. A lost
# node takes all of it. Our own sh is skipped (its command line matches).
hx "$SRC" sh -c 'touch "$HOME/.k2-lab/hold"
    d=$(cat "$HOME/.k2-lab/daemon.pid")
    [ -n "$d" ] && kill -9 "$d"
    for p in /proc/[0-9]*; do
        [ "${p#/proc/}" = $$ ] && continue
        case "$(tr "\0" " " < $p/cmdline 2>/dev/null)" in *k2-ptyd*|/opt/k2-fake/shim/*|*/k2-fake-agent\ *) kill -9 "${p#/proc/}" 2>/dev/null;; esac
    done' >> "$RUN/kill.txt" 2>&1
t_kill=$(date -u +%H:%M:%S)
not_linked() { ! linked "$SRC"; }
wait_for 30 not_linked || true
check "$SRC's daemon and the agent CLI are killed and held down (no drain)" \
    "$(not_linked && [ "$(clis "$SRC")" = 0 ] && echo 1)" "at $t_kill; CLIs left on $SRC: $(clis "$SRC")"

# ── 4. the move ────────────────────────────────────────────────────────
on_dst() { [ "$(placed_on)" = "$DST" ]; }
c3=$(msg "third [[remember: after the move]]" true); echo "third: $c3" >> "$RUN/msgs.txt"
wait_for "$move_wait" on_dst || true
wait_for 120 mem_has "$DST" "after the move" || true
EPOCH2=$(placed_epoch)
moves=$(count k2-controller-0 daemon.jsonl ctl.move_decision)
fetched=$(events "$DST" daemon.jsonl host.home_restored | tail -1 | field attrs.fetched)
EX=$(exec_of "$DST")
resumed=$(printf '%s' "$EX" | python3 -I -c '
import json,sys
e=json.loads(sys.stdin.read() or "{}"); a=e.get("attrs") or {}
print(1 if e.get("session_id")==sys.argv[1] and "--resume" in (a.get("argv") or []) else 0)' "$SESSION")
check "the controller moves the agent to $DST on the other node at a higher epoch; $DST restores the home and resumes the same session" \
    "$(on_dst && [ "$moves" -ge 1 ] && [ "${EPOCH2:-0}" -gt "${EPOCH1:-0}" ] && [ "${fetched:-0}" -gt 0 ] && [ "$resumed" = 1 ] && echo 1)" \
    "move decisions $moves; on $(placed_on) epoch $EPOCH2 (was $EPOCH1); fetched ${fetched:-none}; exec session $(printf '%s' "$EX" | field session_id) resume=$resumed"

# ── 5. the home on DST ─────────────────────────────────────────────────
ws_state "$DST" k2-host "$WS" > "$RUN/workspace-dst.txt"
prefix=$(mem_sha "$DST" "$MEM_BYTES")
check "on $DST: the workspace file, MEMORY.md (SRC's bytes first), the facts; the message after the move landed there" \
    "$(grep -q notes.txt "$RUN/workspace-dst.txt" && [ "$prefix" = "$MEM_SHA" ] && mem_has "$DST" "$FACT" && mem_has "$DST" "the second turn" && mem_has "$DST" "after the move" && echo 1)" \
    "notes.txt $(grep -q notes.txt "$RUN/workspace-dst.txt" && echo present || echo ABSENT); MEMORY.md first $MEM_BYTES bytes $prefix vs $MEM_SHA"

# ── 6. SRC back ────────────────────────────────────────────────────────
hx "$SRC" rm -f /var/lib/k2-host/.k2-lab/hold 2>/dev/null
wait_for 120 linked "$SRC" || true
sleep 10
check "$SRC released: it relinks, the agent stays on $DST, one live CLI (I1)" \
    "$(linked "$SRC" && on_dst && [ "$(clis "$SRC")" = 0 ] && [ "$(clis "$DST")" -ge 1 ] && echo 1)" \
    "on $(placed_on); CLIs: $SRC $(clis "$SRC"), $DST $(clis "$DST")"

# ── what the controller sees, with no shared volume ─────────────────────
ws_state k2-controller-0 k2-daemon "$WS" > "$RUN/workspace-controller-end.txt"
if grep -q notes.txt "$RUN/workspace-controller-end.txt"; then
    note "the controller's copy of $WS has the agent's notes.txt"
else
    note "the controller's copy of $WS (what the app browses) does not have the agent's notes.txt: $(tr '\n' ' ' < "$RUN/workspace-controller-end.txt")"
fi
if grep -q README.md "$RUN/workspace-src-first-turn.txt"; then
    note "the workspace the controller registered (README.md, commit) was on $SRC at the first turn"
else
    note "the workspace the controller registered (README.md, its commit) never reached $SRC: there it is $(tr '\n' ' ' < "$RUN/workspace-src-first-turn.txt")"
fi

for p in k2-controller-0 k2-host-0 k2-host-1; do
    trace "$p" daemon.jsonl > "$RUN/$p.trace.jsonl"
    [ "$p" = k2-controller-0 ] || trace "$p" fake.jsonl > "$RUN/$p.fake.jsonl"
done
ctl_log > "$RUN/controller.log"
kn get pods,pvc -o wide > "$RUN/objects.txt" 2>&1
echo "evidence: $RUN"
if [ "$FAILS" = 0 ]; then echo "PASS home-move-check"; else echo "FAIL home-move-check: $FAILS check(s)"; exit 1; fi
