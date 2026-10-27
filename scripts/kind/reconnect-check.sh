#!/usr/bin/env bash
# Restart K2's pods on kind and check that a client reconnects the way the
# desktop app does, with the SAME session token and no new sign-in:
#
#   scripts/kind/reconnect-check.sh [--dist] [--fake] --password-file=PATH [--timeout=SECONDS]
#
# 1. POST /cli/auth/login (K2_KIND_USER, default "owner") → a session token
# 2. WS /cli/sessions/events?path=&token=<session> → the "hello" frame
# 3. kubectl delete the pods (k2-controller-0 and k2-host-0 with --dist,
#    k2-solo-0 otherwise); the socket must close
# 4. every second, reopen the events socket with the same token until it
#    says hello again (a 401/403 handshake = the token died: FAIL)
# 5. /boot-status shows a new instanceId, GET /cli/auth/whoami with the
#    token is 200, and (--dist) the host is linked again
#
# Restarting pods interrupts whoever uses them: only the test variants
# (--fake) unless --restart-live is given too. Prints no password and no
# token. Default timeout 300 s.
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
fake=0 dist=0 live=0 password_file="" timeout=300
for a in "$@"; do
    case "$a" in
        --fake) fake=1 ;;
        --dist) dist=1 ;;
        --restart-live) live=1 ;;
        --password-file=*) password_file=${a#--password-file=} ;;
        --timeout=*) timeout=${a#--timeout=} ;;
        -h|--help) sed -n '2,19p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "reconnect-check.sh: unknown argument $a (see --help)" >&2; exit 2 ;;
    esac
done
[ -r "$password_file" ] || { echo "reconnect-check.sh: --password-file=PATH (readable) is required" >&2; exit 2; }
if [ "$fake" = 0 ] && [ "$live" = 0 ]; then
    echo "reconnect-check.sh: this restarts the pods; without --fake it needs --restart-live" >&2
    exit 2
fi
kind_variant_from_flags "$fake" "$dist"
kind_kubeconfig
cd "$KIND_REPO"
if [ "$KIND_DIST" = 1 ]; then pods="k2-controller-0 k2-host-0"; else pods="k2-solo-0"; fi

PORT=$KIND_PORT USER_NAME=${K2_KIND_USER:-owner} PASSWORD_FILE=$password_file TIMEOUT=$timeout \
    KC="$KIND_KUBECONFIG" NS="$KIND_NS" PODS="$pods" DIST="$KIND_DIST" LABKIT="$KIND_REPO/tests/lab" \
    python3 -I - <<'PY'
import json, os, socket, subprocess, sys, time, urllib.error, urllib.parse, urllib.request
sys.path.insert(0, os.environ["LABKIT"])
from labkit import ws
port, timeout = int(os.environ["PORT"]), float(os.environ["TIMEOUT"])
base = "http://127.0.0.1:%d" % port

def http(method, path, body=None, token=None, t=10):
    q = "?" + urllib.parse.urlencode({"token": token}) if token else ""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path + q, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=t) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, {}
    except (OSError, ValueError):
        return None, {}

def fail(msg):
    print("FAIL " + msg, flush=True)
    sys.exit(1)

def open_events(tok):
    """The events socket with this token → (socket, hello) or raises."""
    sock = ws.WebSocket("127.0.0.1", port, "/cli/sessions/events?" + urllib.parse.urlencode(
        {"path": "", "token": tok}), timeout=5)
    sock.settimeout(5)
    kind, data = sock.recv()
    if kind != "text":
        sock.close()
        raise ws.WsError("first frame: %s" % kind)
    msg = json.loads(data)
    if msg.get("kind") != "hello":
        sock.close()
        raise ws.WsError("first frame is %r, not hello" % msg.get("kind"))
    return sock, msg

pw = open(os.environ["PASSWORD_FILE"]).read().strip()
st, d = http("POST", "/cli/auth/login", {"username": os.environ["USER_NAME"], "password": pw})
del pw
if st != 200 or not d.get("token"):
    fail("login as %s -> HTTP %s" % (os.environ["USER_NAME"], st))
tok = d["token"]
print("ok   login as %s -> session token (%d chars)" % (d.get("username"), len(tok)), flush=True)
_, boot = http("GET", "/boot-status", token=tok)
before = boot.get("instanceId")
sock, hello = open_events(tok)
print("ok   events socket open, hello received (instance %s)" % before, flush=True)

pods = os.environ["PODS"].split()
subprocess.run(["kubectl", "--kubeconfig", os.environ["KC"], "-n", os.environ["NS"], "delete", "pod",
                *pods, "--wait=false"], check=True, stdout=subprocess.DEVNULL)
t0 = time.monotonic()
print("ok   deleted %s" % " ".join(pods), flush=True)

# The open socket must notice: a close frame, EOF or a reset.
sock.settimeout(1)
closed_at = None
while time.monotonic() - t0 < 120:
    try:
        kind, _ = sock.recv()
        if kind == "close":
            closed_at = time.monotonic()
            break
    except socket.timeout:
        continue
    except (OSError, ws.WsError):
        closed_at = time.monotonic()
        break
sock.close()
if closed_at is None:
    fail("the events socket stayed open 120 s after the pods were deleted")
print("ok   events socket closed after %.1f s" % (closed_at - t0), flush=True)

attempts, last_err, hello2 = 0, "", None
while time.monotonic() - t0 < timeout:
    attempts += 1
    try:
        s2, hello2 = open_events(tok)
        s2.close()
        break
    except ws.WsError as e:
        last_err = str(e)
        if " 401 " in last_err or " 403 " in last_err:
            fail("the server refused the same token after the restart: %s" % last_err[:200])
    except (OSError, ValueError) as e:
        last_err = "%s: %s" % (type(e).__name__, e)
    time.sleep(1)
if hello2 is None:
    fail("no hello with the same token within %d s (%d tries; last: %s)" % (timeout, attempts, last_err[:200]))
t_events = time.monotonic() - t0
print("ok   events socket reopened with the SAME token, hello after %.1f s (%d tries)" % (t_events, attempts), flush=True)

_, boot = http("GET", "/boot-status", token=tok)
after = boot.get("instanceId")
if not after or after == before:
    fail("/boot-status instanceId %s -> %s: did the daemon restart?" % (before, after))
print("ok   /boot-status instance %s -> %s, phase %s" % (before, after, boot.get("phase")), flush=True)
st, who = http("GET", "/cli/auth/whoami", token=tok)
if st != 200:
    fail("GET /cli/auth/whoami with the same token -> HTTP %s" % st)
print("ok   whoami with the same token -> 200 (%s)" % who.get("username"), flush=True)

if os.environ["DIST"] == "1":
    linked = None
    while time.monotonic() - t0 < timeout:
        _, hs = http("GET", "/cli/hosts/status")
        if any(h.get("host_name") for h in hs.get("hosts", [])):
            linked = time.monotonic() - t0
            break
        time.sleep(1)
    if linked is None:
        fail("the host did not link again within %d s" % timeout)
    print("ok   host linked again after %.1f s" % linked, flush=True)

print("PASS reconnect: the same session token reopened /cli/sessions/events %.1f s after the restart" % t_events)
PY
