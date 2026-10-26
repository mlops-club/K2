#!/bin/bash
# TEST ONLY. Runs inside the lab runner pod (scripts/kind/fleet-g2.sh copies
# it to /lab/repo/scripts/kind and execs it): the lab's G2 sequence of
# scripts/lab/prove.sh (g2_scenarios) against the kind fleet, through
# scripts/kind/fleet_kind.py. Environment from fleet-g2.sh:
#
#   NS, HOSTS (k2-host-0,k2-host-1,...), ONLY (S1,S2,...), CHAOS (seed or
#   empty), KILL (process|pod), CHAOS_MIN_S
#
# Writes everything to /lab/run: fleet.json, scenarios/, traces/,
# samples.jsonl, chaos.log, outbox-final/, invariants.json, hosts-status*.json.
set -uo pipefail
RUN=/lab/run
KIND_PY=(python3 -I /lab/repo/scripts/kind/fleet_kind.py)
LAB=("${KIND_PY[@]}" lab)
STATE=$RUN/fleet.json
export LAB_KIND_REPO=/lab/repo HOME=/lab/run/home
mkdir -p "$RUN/scenarios" "$RUN/traces" "$HOME"
say() { printf '%s fleet-g2: %s\n' "$(date -u +%H:%M:%SZ)" "$*" | tee -a "$RUN/fleet-g2.log" >&2; }
bg=()
cleanup() {
    for p in "${bg[@]}"; do kill -TERM "$p" 2>/dev/null; done
    wait 2>/dev/null
    [ -f "$STATE" ] && "${LAB[@]}" fleet down --state "$STATE" >/dev/null 2>&1
}
trap cleanup EXIT

# Rows of this run only: the outboxes live on the pods' volumes and keep
# earlier runs' rows (judged by those runs).
START_MS=$(python3 -I -c 'import time; print(int(time.time() * 1000))')
LIVE=${LIVE:-$HOSTS}   # pool mode: the hosts running now (HOSTS: every one the pool may make)
"${KIND_PY[@]}" plan --state "$STATE" --run-dir "$RUN" --namespace "$NS" --hosts "$HOSTS" --kill "$KILL" ${POOL:+--pool "$POOL"} \
    | tee -a "$RUN/fleet-g2.log" || exit 1
# This run's traces only: the files live on the pods' volumes and are
# appended to across runs (writers use O_APPEND, so truncating is safe).
python3 -I - "$STATE" <<'PY'
import json, sys
st = json.load(open(sys.argv[1]))
for p in [p["trace"] for p in st["procs"].values()] + [h["fake_trace"] for h in st["hosts"]]:
    open(p, "a").close()
    open(p, "r+").truncate(0)
PY
"${KIND_PY[@]}" forward --state "$STATE" &
bg+=($!)
ports=$(python3 -I -c 'import json,sys; print(" ".join("%s=%s" % (n, p["port"]) for n, p in json.load(open(sys.argv[1]))["procs"].items()))' "$STATE")
for pair in $ports; do
    case ",controller,$LIVE," in *",${pair%%=*},"*) ;; *) say "${pair%%=*}: not running (the pool makes it when it needs it)"; continue ;; esac
    for _ in $(seq 1 300); do
        curl -fsS --max-time 2 "http://127.0.0.1:${pair#*=}/boot-status" 2>/dev/null | grep -q '"phase":"ready"' && break
        sleep 0.5
    done
    say "${pair%%=*} on 127.0.0.1:${pair#*=}: $(curl -fsS --max-time 2 "http://127.0.0.1:${pair#*=}/boot-status" | head -c 120)"
done
ctl_port=$(python3 -I -c 'import json,sys; st=json.load(open(sys.argv[1])); print(st["procs"]["controller"]["port"])' "$STATE")
hosts_status() { curl -fsS --max-time 5 "http://127.0.0.1:$ctl_port/cli/hosts/status" > "$RUN/$1" 2>/dev/null; }
for _ in $(seq 1 120); do
    hosts_status hosts-status-start.json
    n=$(python3 -I -c 'import json,sys; print(sum(1 for h in json.load(open(sys.argv[1])).get("hosts", []) if h.get("connected", True)))' "$RUN/hosts-status-start.json" 2>/dev/null || echo 0)
    [ "$n" -ge "$(echo "$LIVE" | tr ',' '\n' | grep -c .)" ] && break
    sleep 1
done
say "controller /cli/hosts/status: $(head -c 600 "$RUN/hosts-status-start.json")"

say "fleet monitor: samples.jsonl"
"${LAB[@]}" fleet monitor --state "$STATE" --out "$RUN/samples.jsonl" &
bg+=($!)

rc=0
has() { case ",$ONLY," in *",$1,"*) return 0 ;; esac; return 1; }

if has S1; then
    # S1 is a one-host scenario (it reads the transcript in one --home): the
    # other hosts are held down while it runs, so the controller places it
    # on the first.
    first=${LIVE%%,*}
    others=$(echo "$LIVE" | tr ',' '\n' | tail -n +2)
    # (lab.py's `fleet` takes the proc right after the action.)
    for h in $others; do
        "${LAB[@]}" fleet kill "$h" --state "$STATE" 2>&1 | tee -a "$RUN/fleet-g2.log"
        [ "${PIPESTATUS[0]}" = 0 ] || { say "could not hold $h for S1"; exit 1; }
    done
    eval "$(python3 -I - "$STATE" "$first" <<'PY'
import json, shlex, sys
st, first = json.load(open(sys.argv[1])), sys.argv[2]
ctl = st["procs"]["controller"]
h = next(h for h in st["hosts"] if h["name"] == first)
for k, v in {"S1_PORT": ctl["port"], "S1_TOKEN_FILE": ctl["home"] + "/.k2/daemon.token", "S1_HOME": h["home"],
             "S1_CWD": st["work"] + "/lab-agent", "S1_TRACE": h["fake_trace"]}.items():
    print("%s=%s" % (k, shlex.quote(str(v))))
PY
)"
    mkdir -p "$S1_CWD" && git -C "$S1_CWD" init -q
    say "S1 on $first (others held: ${others//$'\n'/ })"
    "${LAB[@]}" s1 --port "$S1_PORT" --token-file "$S1_TOKEN_FILE" --home "$S1_HOME" --cwd "$S1_CWD" \
        --out "$RUN/scenarios/S1" --fake-trace "$S1_TRACE" 2>&1 | tee -a "$RUN/fleet-g2.log"
    [ "${PIPESTATUS[0]}" = 0 ] || rc=1
    for h in $others; do
        "${LAB[@]}" fleet start "$h" --state "$STATE" 2>&1 | tee -a "$RUN/fleet-g2.log"
        [ "${PIPESTATUS[0]}" = 0 ] || { say "could not start $h again after S1"; exit 1; }
    done
fi

if has S2; then
    "${LAB[@]}" g2 --state "$STATE" --out "$RUN/scenarios" --only S2 2>&1 | tee -a "$RUN/fleet-g2.log"
    [ "${PIPESTATUS[0]}" = 0 ] || rc=1
fi
rest=$(echo "$ONLY" | tr ',' '\n' | grep -vx 'S1' | grep -vx 'S2' | paste -sd, -)
if [ -n "$rest" ]; then
    chaos_pid=""
    if [ -n "$CHAOS" ]; then
        say "chaos seed=$CHAOS: on for $rest (chaos.log, kill=$KILL)"
        "${LAB[@]}" chaos --state "$STATE" --seed "$CHAOS" --out "$RUN/chaos.log" &
        chaos_pid=$!
        bg+=("$chaos_pid")
    fi
    "${LAB[@]}" g2 --state "$STATE" --out "$RUN/scenarios" --only "$rest" \
        ${CHAOS:+--min-seconds "${CHAOS_MIN_S:-180}"} 2>&1 | tee -a "$RUN/fleet-g2.log"
    [ "${PIPESTATUS[0]}" = 0 ] || rc=1
    if [ -n "$chaos_pid" ]; then
        say "chaos: off (it restores anything it took down)"
        kill -TERM "$chaos_pid"; wait "$chaos_pid"
    fi
fi

hosts_status hosts-status-end.json
say "controller /cli/hosts/status at the end: $(head -c 1500 "$RUN/hosts-status-end.json")"
"${LAB[@]}" collect --state "$STATE" --out "$RUN/outbox-all" 2>&1 | tee -a "$RUN/fleet-g2.log"
# outbox-final/ (what invariants I2 reads): the rows created since this run
# started; outbox-all/ keeps every row as listed.
python3 -I - "$RUN" "$START_MS" <<'PY' | tee -a "$RUN/fleet-g2.log"
import json, os, sys
run, start = sys.argv[1], int(sys.argv[2])
os.makedirs(os.path.join(run, "outbox-final"), exist_ok=True)
for f in sorted(os.listdir(os.path.join(run, "outbox-all"))):
    doc = json.load(open(os.path.join(run, "outbox-all", f)))
    if isinstance(doc, dict) and isinstance(doc.get("messages"), list):
        before = len(doc["messages"])
        doc["messages"] = [m for m in doc["messages"] if (m.get("created_ms") or 0) >= start]
        print("outbox %s: %d row(s) of this run, %d earlier" % (f[:-5], len(doc["messages"]), before - len(doc["messages"])))
    json.dump(doc, open(os.path.join(run, "outbox-final", f), "w"), indent=2)
PY
# Traces under the names labkit/invariants.py reads.
python3 -I - "$STATE" "$RUN/traces" <<'PY'
import json, shutil, sys
st, out = json.load(open(sys.argv[1])), sys.argv[2]
for name, p in st["procs"].items():
    shutil.copyfile(p["trace"], "%s/%s.daemon.jsonl" % (out, name))
for h in st["hosts"]:
    shutil.copyfile(h["fake_trace"], "%s/%s.fake.jsonl" % (out, h["name"]))
PY
say "invariants I1 to I4"
"${LAB[@]}" invariants --run-dir "$RUN" --layout fleet 2>&1 | tee -a "$RUN/fleet-g2.log"
python3 -I - "$RUN" <<'PY' | tee -a "$RUN/fleet-g2.log"
import glob, json, os, sys
run = sys.argv[1]
s1 = os.path.join(run, "scenarios", "S1", "S1.json")
if os.path.isfile(s1):
    print("S1  %-5s %s" % (json.load(open(s1)).get("status", "?").upper(), "One agent through the controller (lab.py s1)"))
for f in sorted(glob.glob(os.path.join(run, "scenarios", "S*.json"))):
    d = json.load(open(f))
    print("%-3s %-5s %s" % (d.get("scenario"), d.get("status", "?").upper(), d.get("title", "")))
PY
say "done (rc=$rc)"
exit $rc
