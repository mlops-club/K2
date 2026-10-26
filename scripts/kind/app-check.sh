#!/usr/bin/env bash
# Check K2 on kind the way the desktop app uses it, with no agent CLI:
#
#   scripts/kind/app-check.sh [--dist] [--fake] [--p4] --password-file=PATH
#
# 1. POST /cli/auth/login as the app login (K2_KIND_USER, default "owner";
#    the password read from PATH) → a session token, as the app gets one
# 2. with that session token, never the owner token:
#    POST /cli/sessions/v2/spawn a plain `sh` in <work>/demo that prints
#    "K2CHECK host=<hostname> uid=<uid> cwd=<cwd>" and then waits. With
#    --fake the spawn guard allows only the shims, so it spawns `claude`
#    (the fake agent) with the marker as its --session-id and waits for its
#    banner instead.
# 3. WS /cli/sessions/grid?session=…&token=<session> (the app's terminal
#    view) until that line is on the grid
# 4. /proc in the agents pod and in the app's pod: where does it run?
# 5. POST /cli/sessions/v2/close
#
# Passes when the line arrives through the door and the shell runs in the
# agents pod (k2-host-0 with --dist; with --p4, k2-kind-p4, whichever host
# the controller chose) and, with --dist, not in the controller. Prints no
# password and no token.
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
fake=0 dist=0 p4=0 password_file=""
for a in "$@"; do
    case "$a" in
        --fake) fake=1 ;;
        --dist) dist=1 ;;
        --p4) p4=1 ;;
        --password-file=*) password_file=${a#--password-file=} ;;
        -h|--help) sed -n '2,19p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "app-check.sh: unknown argument $a (see --help)" >&2; exit 2 ;;
    esac
done
[ -r "$password_file" ] || { echo "app-check.sh: --password-file=PATH (readable) is required" >&2; exit 2; }
if [ "$p4" = 1 ]; then kind_variant p4; fake=1; else kind_variant_from_flags "$fake" "$dist"; fi
kind_kubeconfig
cd "$KIND_REPO"
marker=$(python3 -I -c 'import uuid; print(uuid.uuid4())')   # also the fake agent's --session-id
proc_scan() {  # proc_scan <pod> <container> → lines for processes whose argv has the marker
    kc -n "$KIND_NS" exec "$1" -c "$2" -- env M="$marker" sh -c '
        for p in /proc/[0-9]*; do
            a=$(tr "\0" " " < "$p/cmdline" 2>/dev/null) || continue
            case "$a" in *"$M"*) echo "pid ${p#/proc/} $a" ;; esac
        done' 2>/dev/null | cut -c1-160 || true
}

# Steps 1-3 and 5 in Python (labkit's WebSocket client); it pauses after the
# line is seen so the shell can be found in /proc, then closes it.
# Two FIFOs, one per direction: with one, Python could read back its own
# "scan" before the shell saw it, and the scan never ran.
fifo=$(mktemp -u "${TMPDIR:-/tmp}/k2-app-check-XXXXXX"); mkfifo "$fifo" "$fifo.back"
trap 'rm -f "$fifo" "$fifo.back"' EXIT
exec 3<>"$fifo"     # Python → shell; read-write: neither side blocks on open, and reads can time out
exec 4<>"$fifo.back"   # shell → Python
PORT=$KIND_PORT USER_NAME=${K2_KIND_USER:-owner} PASSWORD_FILE=$password_file \
    WORK=$KIND_WORK MARKER=$marker FAKE=$fake LABKIT="$KIND_REPO/tests/lab" FIFO=$fifo python3 -I - <<'PY' &
import json, os, sys, time, urllib.error, urllib.parse, urllib.request
sys.path.insert(0, os.environ["LABKIT"])
from labkit import ws
from labkit.s1 import Grid
port, marker = int(os.environ["PORT"]), os.environ["MARKER"]
def call(path, body, token=None):
    q = "?" + urllib.parse.urlencode({"token": token}) if token else ""
    req = urllib.request.Request("http://127.0.0.1:%d%s%s" % (port, path, q), data=json.dumps(body).encode(),
                                 method="POST", headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, {"error": e.read().decode("utf-8", "replace")[:300]}
def done(ok, msg):
    print(("PASS " if ok else "FAIL ") + msg, flush=True)
    open(os.environ["FIFO"], "w").write("go\n")   # let the shell side continue
    sys.exit(0 if ok else 1)

pw = open(os.environ["PASSWORD_FILE"]).read().strip()
st, d = call("/cli/auth/login", {"username": os.environ["USER_NAME"], "password": pw})
del pw
if st != 200 or not d.get("token"):
    done(False, "login as %s -> HTTP %s %s" % (os.environ["USER_NAME"], st, d.get("error", "")))
tok = d["token"]
print("ok   login as %s -> session token (%d chars)" % (d.get("username"), len(tok)), flush=True)
agent = "kind-app-check-" + marker[-8:]
if os.environ["FAKE"] == "1":
    # The fake images resolve only shims: the fake agent, marker as its session id.
    command, args, want = "claude", ["--session-id", marker], "k2-fake-agent (as claude)"
else:
    # No exec: the shell itself stays, so the marker stays in its argv for /proc.
    command, args, want = "sh", ["-c", 'echo "K2CHECK host=$(hostname) uid=$(id -u) cwd=$(pwd)"; sleep 300; : %s' % marker], "K2CHECK"
st, d = call("/cli/sessions/v2/spawn", {"agent_name": agent, "cwd": os.environ["WORK"] + "/demo",
                                        "command": command, "args": args, "cols": 120, "rows": 32}, tok)
print("%s spawn %s with the session token -> HTTP %s %s" % ("ok  " if st == 200 else "FAIL", command, st, json.dumps(d)[:200]), flush=True)
if st != 200:
    done(False, "spawn")
grid = Grid()
sock = ws.WebSocket("127.0.0.1", port, "/cli/sessions/grid?" + urllib.parse.urlencode(
    {"session": d["sessionId"], "token": tok}), timeout=30)
deadline, line = time.monotonic() + 30, ""
while time.monotonic() < deadline and want not in grid.text():
    sock.settimeout(max(0.1, deadline - time.monotonic()))
    kind, data = sock.recv()
    if kind == "close":
        break
    if kind == "text":
        msg = json.loads(data)
        if not grid.apply(msg) and msg.get("event") in ("error", "child_exit"):
            print("FAIL grid event: %s" % data[:300], flush=True)
            break
line = next((l for l in grid.text().splitlines() if want in l), "")
sock.close()
print("%s attach /cli/sessions/grid with the session token: %r" % ("ok  " if line else "FAIL", line), flush=True)
open(os.environ["FIFO"], "w").write("scan\n")       # the shell side scans /proc now
open(os.environ["FIFO"] + ".back").readline()      # ... and says when it is done (one line; the fifo never hits EOF)
st, d = call("/cli/sessions/v2/close", {"agent_name": agent, "force": True, "clear_index": True}, tok)
print("%s close -> HTTP %s %s" % ("ok  " if st == 200 else "FAIL", st, json.dumps(d)[:120]), flush=True)
sys.exit(0 if line and st == 200 else 1)
PY
py=$!
msg=""
read -r -t 120 msg <&3 || true
if [ "$msg" = scan ]; then
    if [ "$p4" = 1 ] || [ "$dist" = 1 ]; then
        # Any host: the controller places it (a pool or no shared volume
        # means it need not be k2-host-0).
        for h in $(kc -n "$KIND_NS" get pods -l app.kubernetes.io/name=k2-host -o jsonpath='{.items[*].metadata.name}'); do
            agents=$(proc_scan "$h" "$KIND_AGENT_CONTAINER")
            [ -n "$agents" ] && { KIND_AGENT_POD=$h; break; }
        done
    else
        agents=$(proc_scan "$KIND_AGENT_POD" "$KIND_AGENT_CONTAINER")
    fi
    echo "     /proc in $KIND_AGENT_POD ($(kc -n "$KIND_NS" get pod "$KIND_AGENT_POD" -o jsonpath='{.spec.nodeName}')): ${agents:-(none)}"
    ctl=""
    if [ "$KIND_AGENT_POD" != "$KIND_POD" ]; then
        ctl=$(proc_scan "$KIND_POD" k2-daemon)
        echo "     /proc in $KIND_POD: ${ctl:-(none)}"
    fi
    echo "done" >&4
fi
rc=0
wait "$py" || rc=$?
if [ "$rc" = 0 ] && [ -n "${agents:-}" ] && [ -z "${ctl:-}" ]; then
    echo "PASS app path: login, spawn and attach with a session token; the shell ran in $KIND_AGENT_POD"
else
    echo "FAIL app path (python rc=$rc, in $KIND_AGENT_POD: ${agents:+yes}, in $KIND_POD: ${ctl:+yes})"
    exit 1
fi
