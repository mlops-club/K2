#!/usr/bin/env python3
"""The owner's path, end to end, on k2-kind-dist (owner-roll-wave3.sh step 9).

Through 127.0.0.1:<port> with the owner's app login (the password file
up.sh wrote; read here, never printed), the routes the app and `k2 msg` use
(tests/lab/labkit/w1.py's S4 wake flow):

  1. POST /cli/auth/login                        → the app's session
  2. a probe workspace: a git folder on the shared k2-work volume (made in
     the controller pod), POST /cli/projects/add-from-path, then
     /cli/projects/update agentEnabled=1 (a sleeping agent)
  3. POST /cli/workspace/msg wake=true with one short prompt: the agent
     (Claude Code, on the host the pool placed it) wakes and answers
  4. the reply is in Claude's transcript on k2-host-0's volume
     (~/.claude/projects/*/<session>.jsonl): an assistant line with the
     prompt's unique marker. Only counts and the session id are printed
  4b. with --home-snapshot (home sync, rc1 on): before the sleep, the
     controller's home store has a snapshot of this agent
     (GET /cli/hosts/home/status, owner token read in the controller pod):
     made at the turn's end (P3 T2), since nothing else saves it before
     step 5. Its seq, files and bytes are printed
  5. POST /cli/sessions/v2/close (sleep), then a second short message wakes
     it again and is answered in the same session

    python3 -I scripts/kind/owner-proof.py --port 7443 --password-file F \\
        --kubeconfig K --namespace k2-kind-dist [--user owner] [--home-snapshot]

Prints one line per check; exits 0 only if all pass. The agent is left
asleep and the probe workspace registered (k2-roll-proof-<tag>).
"""
import argparse, json, subprocess, sys, time, urllib.error, urllib.parse, urllib.request, uuid

a = argparse.ArgumentParser()
a.add_argument("--port", type=int, required=True)
a.add_argument("--password-file", required=True)
a.add_argument("--kubeconfig", required=True)
a.add_argument("--namespace", required=True)
a.add_argument("--user", default="owner")
a.add_argument("--home-snapshot", action="store_true")
args = a.parse_args()
tag = uuid.uuid4().hex[:8]
fails = 0


def check(ok, what, detail=""):
    global fails
    print(("ok   " if ok else "FAIL ") + what + (" — " + detail if detail else ""), flush=True)
    if not ok:
        fails += 1
    return ok


def http(path, body=None, form=None, token=None, timeout=120):
    q = "?" + urllib.parse.urlencode({"token": token}) if token else ""
    if form is not None:
        data, ctype = urllib.parse.urlencode(form).encode(), "application/x-www-form-urlencoded"
    else:
        data, ctype = json.dumps(body or {}).encode(), "application/json"
    req = urllib.request.Request("http://127.0.0.1:%d%s%s" % (args.port, path, q), data=data, method="POST",
                                 headers={"Content-Type": ctype})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8", "replace")
            status = r.status
    except urllib.error.HTTPError as e:
        raw, status = e.read().decode("utf-8", "replace"), e.code
    try:
        return status, json.loads(raw or "{}")
    except ValueError:
        return status, {"raw": raw[:300]}


def pod(name, container, script, *argv):
    r = subprocess.run(["kubectl", "--kubeconfig", args.kubeconfig, "-n", args.namespace, "exec", name, "-c", container,
                        "--", "sh", "-c", script, "_", *argv], capture_output=True, text=True)
    return r.returncode, r.stdout.strip()


def replies(marker):
    """(session id, assistant lines with the marker) on k2-host-0's volume; never the text."""
    rc, out = pod("k2-host-0", "k2-host", '''
        for f in $(grep -rl -- "$1" /var/lib/k2-host/.claude/projects 2>/dev/null); do
            n=$(grep -- "$1" "$f" | grep -c '"type":"assistant"')
            echo "$(basename "$f" .jsonl) $n"
        done''', marker)
    best = None
    for line in out.splitlines():
        sid, n = line.rsplit(" ", 1)
        if int(n) > 0:
            best = (sid, int(n))
    return best


def wait_reply(marker, deadline=240):
    end = time.time() + deadline
    while time.time() < end:
        r = replies(marker)
        if r:
            return r
        time.sleep(5)
    return None


# 1.
pw = open(args.password_file).read().strip()
st, d = http("/cli/auth/login", {"username": args.user, "password": pw})
token = d.get("token") or d.get("sessionToken") or (d.get("session") or {}).get("token")
if not check(st == 200 and bool(token), "the owner's app login through 127.0.0.1:%d" % args.port, "HTTP %s" % st):
    sys.exit(1)

# 2.
path = "/var/lib/k2/work/k2-roll-proof-%s" % tag
rc, _ = pod("k2-controller-0", "k2-daemon", 'mkdir -p "$1" && git init -q "$1"', path)
st, d = http("/cli/projects/add-from-path", {"path": path, "seed_wiki": False, "seed_agents_md": False}, token=token)
pid = d.get("id")
check(rc == 0 and st == 200 and bool(pid), "probe workspace registered", "%s, HTTP %s" % (path, st))
st, _ = http("/cli/projects/update", {"id": pid, "agentEnabled": 1}, token=token)
check(st == 200, "its agent enabled (asleep)", "HTTP %s" % st)

# 3. + 4.
m1 = "K2ROLL-%s-A" % tag
st, d = http("/cli/workspace/msg", form={"workspace": path, "text": "Reply with the single word %s and nothing else." % m1,
                                        "from": "owner-roll", "wake": "true"}, token=token, timeout=300)
check(st == 200 and d.get("success") is True and d.get("woke") is True, "msg with wake: delivered, the agent woke",
      "HTTP %s success=%s woke=%s" % (st, d.get("success"), d.get("woke")))
r1 = wait_reply(m1)
check(bool(r1), "Claude Code answered on k2-host-0 (its transcript has the reply)",
      "session %s, %d assistant line(s) with the marker" % r1 if r1 else "no reply within 240 s")

# 4b.
if args.home_snapshot and r1:
    snap, end = None, time.time() + 120
    while time.time() < end and not snap:
        rc, out = pod("k2-controller-0", "k2-daemon",
                      'curl -s "http://127.0.0.1:7420/cli/hosts/home/status?token=$(cat /var/lib/k2/.k2/daemon.token)"')
        try:
            snap = next((s for s in json.loads(out).get("snapshots", []) if s.get("agent") == pid), None)
        except ValueError:
            snap = None
        if not snap:
            time.sleep(3)
    check(bool(snap), "a turn-end home snapshot of the agent is in the controller's store (before any sleep)",
          "seq %s, %s file(s), %s bytes, from %s" % (snap.get("seq"), snap.get("files"), snap.get("bytes"),
                                                     snap.get("host") or snap.get("host_id")) if snap else "none within 120 s")

# 5.
st, d = http("/cli/sessions/v2/close", {"agent_name": pid, "force": True}, token=token)
check(st == 200 and d.get("closed") is True, "slept (POST /cli/sessions/v2/close)", "HTTP %s closed=%s" % (st, d.get("closed")))
m2 = "K2ROLL-%s-B" % tag
st, d = http("/cli/workspace/msg", form={"workspace": path, "text": "Reply with the single word %s and nothing else." % m2,
                                        "from": "owner-roll", "wake": "true"}, token=token, timeout=300)
check(st == 200 and d.get("success") is True and d.get("woke") is True, "a second msg woke it again",
      "HTTP %s success=%s woke=%s" % (st, d.get("success"), d.get("woke")))
r2 = wait_reply(m2)
check(bool(r2) and bool(r1) and r2[0] == r1[0], "answered again, in the same session (resumed)",
      "session %s" % (r2[0] if r2 else None))
http("/cli/sessions/v2/close", {"agent_name": pid, "force": True}, token=token)

print("\n%s owner-proof: %s" % ("PASS" if fails == 0 else "FAIL", "the owner's path works end to end" if fails == 0 else "%d check(s)" % fails))
sys.exit(1 if fails else 0)
