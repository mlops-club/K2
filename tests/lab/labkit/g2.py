"""Gate G2 scenario clients (PRD section 3): S2 to S9 against a running
topology described by a fleet state file (labkit/fleet.py).

The client talks to the controller's URL only (on solo: the daemon), as the
desktop app does. Where a scenario needs an agent's own machine (the real
`cli/k2 msg`, an inbox compose from a host), it uses that host's loopback
API and HOME, as the agent CLI would. Evidence is what the system and the
agents recorded: daemon traces, the fake agents' traces and transcripts,
outbox listings. Waits have deadlines only to stop a hung run; no check
asserts a duration.

    lab.py g2 --state F --out DIR --only S2,S3,S4,S5,S6,S7,S8,S9

Each scenario writes DIR/<S>.json ({"scenario", "title", "status", "checks":
[{name, ok, detail}], ...}) and DIR/<S>.log. Messages the lab sends are
appended to DIR/messages.jsonl (the I2 ledger).
"""

import json
import os
import shutil
import signal
import subprocess
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid

from . import fleet as fleetlib
from . import s1
from . import ws as wslib

DEADLINE_S = 90
HTTP_TIMEOUT_S = 120
REPLY_MARKER = s1.REPLY_MARKER
BANNER = "k2-fake-agent (as claude)"
HOOKS = ("SessionStart", "UserPromptSubmit", "Stop")
REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

TITLES = {
    "S2": "Hook events land on the right agent",
    "S3": "k2 msg between agents on different hosts",
    "S4": "Message wakes a sleeping agent",
    "S5": "Heartbeat fires on a sleeping agent",
    "S6": "Inbox delivery survives restarts",
    "S7": "CLI killed mid-turn, next need resumes",
    "S8": "Host killed mid-turn, nothing lost silently",
    "S9": "Controller restart, agents keep running",
}
FLEET_ONLY = ("S8", "S9")


def now_iso():
    return fleetlib._now_iso()


def wait_until(fn, deadline=DEADLINE_S, poll=0.1):
    end = time.monotonic() + deadline
    while True:
        v = fn()
        if v or time.monotonic() >= end:
            return v
        time.sleep(poll)


class Scenario:
    """One scenario's checks, log and result file."""

    def __init__(self, sid, out):
        self.sid, self.out = sid, out
        self.checks, self.lines, self.info = [], [], {}

    def log(self, msg):
        self.lines.append("%s %s" % (now_iso(), msg))

    def check(self, name, ok, detail=""):
        self.checks.append({"name": name, "ok": bool(ok), "detail": detail})
        self.log("%s %s -- %s" % ("PASS" if ok else "FAIL", name, detail))
        print("%s %s: %s%s" % ("PASS" if ok else "FAIL", self.sid, name, (" -- " + detail) if detail else ""), flush=True)
        return bool(ok)

    def finish(self, reason=None, status=None):
        ok = bool(self.checks) and all(c["ok"] for c in self.checks)
        doc = {"scenario": self.sid, "title": TITLES.get(self.sid, self.sid),
               "status": status or ("pass" if ok else "fail"), "checks": self.checks, "info": self.info}
        if reason:
            doc["reason"] = reason
        with open(os.path.join(self.out, self.sid + ".json"), "w") as fh:
            json.dump(doc, fh, indent=2, default=str)
        with open(os.path.join(self.out, self.sid + ".log"), "w") as fh:
            fh.write("\n".join(self.lines) + "\n")
        print("%s %s" % (self.sid, doc["status"].upper()), flush=True)
        return doc


DAEMON_BACK_S = 90  # how long a client waits for a restarting daemon, as the app does


def daemon_down(status, text):
    """The daemon is not there to answer yet: connection refused (it is
    down: the request never reached it, so sending again cannot duplicate
    it) or 503 while it boots (`"state":"migrating"`)."""
    return (status == 0 and "refused" in text) or (status == 503 and '"migrating"' in text)


def send_waiting_for_daemon(req, log=None):
    """urlopen that waits (up to DAEMON_BACK_S) while the daemon is down or
    booting, like the app reconnecting; anything else is returned as is."""
    end = time.monotonic() + DAEMON_BACK_S
    waited = False
    while True:
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as r:
                status, text = r.status, r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            status, text = e.code, e.read().decode("utf-8", "replace")
        except OSError as e:
            status, text = 0, "request failed: %s" % e
        if not daemon_down(status, text) or time.monotonic() >= end:
            if waited and log:
                log("the daemon answered again (HTTP %s)" % status)
            return status, text
        if not waited and log:
            log("the daemon is down or booting (%s); waiting for it, as the app does" % text[:80])
        waited = True
        time.sleep(0.5)


class Client:
    """HTTP and WS to the topology, and its evidence."""

    def __init__(self, state_path, out):
        self.fleet = fleetlib.Fleet(state_path)
        self.out = out
        self.sc = None  # the running Scenario (for logging)
        self.tag = uuid.uuid4().hex[:6]

    # ── plumbing ────────────────────────────────────────────────────────
    def log(self, msg):
        if self.sc:
            self.sc.log(msg)

    def http(self, method, path, query=None, body=None, form=None, port=None, token=None):
        q = dict(query or {})
        q["token"] = token or self.fleet.token()
        url = "http://127.0.0.1:%d%s?%s" % (port or self.fleet.port, path, urllib.parse.urlencode(q))
        headers = {}
        if form is not None:
            data = urllib.parse.urlencode(form).encode()
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        elif body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        else:
            data = b"" if method == "POST" else None
        status, text = send_waiting_for_daemon(urllib.request.Request(url, data=data, method=method, headers=headers),
                                               self.log)
        self.log("%s :%s%s %s -> %s %s" % (method, port or self.fleet.port, path,
                                          json.dumps(form if form is not None else body), status, text[:500]))
        try:
            parsed = json.loads(text)
        except ValueError:
            parsed = {}
        return status, text, parsed if isinstance(parsed, dict) else {"_": parsed}

    def ledger(self, **entry):
        entry.setdefault("ts", now_iso())
        entry.setdefault("scenario", self.sc.sid if self.sc else None)
        with open(os.path.join(self.out, "messages.jsonl"), "a") as fh:
            fh.write(json.dumps(entry, default=str) + "\n")

    # ── agents ──────────────────────────────────────────────────────────
    def spawn(self, name, cwd, session_id=None):
        args = ["--session-id", session_id] if session_id else []
        st, text, resp = self.http("POST", "/cli/sessions/v2/spawn", body={
            "agent_name": name, "cwd": cwd, "command": "claude", "args": args, "cols": 100, "rows": 30})
        return st, resp

    def close(self, name):
        return self.http("POST", "/cli/sessions/v2/close", body={"agent_name": name, "force": True, "clear_index": True})

    def register_enabled(self, cwd):
        """A workspace agent that is on but asleep (no process)."""
        st, text, resp = self.http("POST", "/cli/projects/add-from-path",
                                   body={"path": cwd, "seed_wiki": False, "seed_agents_md": False})
        pid = resp.get("id")
        if st != 200 or not pid:
            return None, "add-from-path HTTP %s %s" % (st, text[:200])
        st, text, _ = self.http("POST", "/cli/projects/update", body={"id": pid, "agentEnabled": 1})
        if st != 200:
            return None, "update HTTP %s %s" % (st, text[:200])
        return pid, "project %s" % pid

    def msg(self, workspace, text, wake=True, sender="lab-g2", port=None, token=None):
        form = {"workspace": workspace, "text": text, "from": sender, "wake": "true" if wake else "false"}
        st, raw, resp = self.http("POST", "/cli/workspace/msg", form=form, port=port, token=token)
        self.ledger(msg_id=resp.get("msgId"), to=workspace, text=text, via=":%s/cli/workspace/msg" % (port or self.fleet.port),
                    http=st, response=resp or raw[:300])
        return st, raw, resp

    # ── evidence ────────────────────────────────────────────────────────
    def fake(self, event=None, session=None):
        return [e for e in self.fleet.fake_events(event) if session is None or e.get("session_id") == session]

    def exec_of(self, session):
        evs = self.fake("cli.exec", session)
        return evs[-1] if evs else None

    def transcripts(self, cwd):
        slug = s1.claude_project_slug(cwd)
        out = []
        for h in self.fleet.hosts():
            d = os.path.join(h["home"], ".claude", "projects", slug)
            if os.path.isdir(d):
                out += [(h["name"], os.path.join(d, f)) for f in sorted(os.listdir(d)) if f.endswith(".jsonl")]
        return out

    def prompts_with(self, cwd, needle):
        """[(host, session, prompt)] for every user prompt containing needle."""
        out = []
        for host, path in self.transcripts(cwd):
            sid = os.path.basename(path)[:-len(".jsonl")]
            for v in fleetlib.read_jsonl(path):
                c = (v.get("message") or {}).get("content")
                if v.get("type") == "user" and isinstance(c, str) and needle in c:
                    out.append((host, sid, c))
        return out

    def replied(self, cwd, needle):
        """True when a reply to a prompt containing needle is in a transcript."""
        for _, path in self.transcripts(cwd):
            for v in fleetlib.read_jsonl(path):
                for part in (v.get("message") or {}).get("content") or []:
                    if isinstance(part, dict) and REPLY_MARKER in part.get("text", "") and needle in part.get("text", ""):
                        return True
        return False

    def turn_events(self, session, event):
        return self.fake(event, session)

    def memory_file(self, host, cwd):
        return os.path.join(self.fleet.host(host)["home"], ".claude", "projects", s1.claude_project_slug(cwd),
                            "memory", "MEMORY.md")

    def placed_host(self, agent, since_ts=""):
        """The host the controller last placed `agent` on (fleet), or the one host (solo)."""
        if self.fleet.layout == "solo":
            return "local"
        evs = [e for e in self.fleet.daemon_events("controller", "ctl.place")
               if e.get("agent_id") == agent and (e.get("ts") or "") >= since_ts]
        return (evs[-1].get("attrs") or {}).get("host") if evs else None

    def outbox(self, proc=None, msg_id=None):
        p = self.fleet.proc(proc or self.fleet.st["controller"])
        st, text, resp = self.http("GET", "/cli/hosts/outbox", query={"msg_id": msg_id} if msg_id else None,
                                   port=p["port"], token=self.fleet.token(proc))
        return resp if st == 200 else None

    def outbox_state(self, msg_id, proc=None):
        resp = self.outbox(proc, msg_id) or {}
        return (resp.get("message") or {}).get("state")

    def wait_final(self, msg_id, deadline=DEADLINE_S):
        return wait_until(lambda: self.outbox_state(msg_id) in ("delivered", "failed") and self.outbox_state(msg_id),
                          deadline, 0.3) or self.outbox_state(msg_id)

    # ── grid ────────────────────────────────────────────────────────────
    def attach(self, session):
        end = time.monotonic() + DAEMON_BACK_S
        while True:
            try:
                return self._attach(session)
            except ConnectionRefusedError:
                if time.monotonic() >= end:
                    raise
                time.sleep(0.5)

    def _attach(self, session):
        sock = wslib.WebSocket("127.0.0.1", self.fleet.port, "/cli/sessions/grid?" + urllib.parse.urlencode(
            {"session": session, "token": self.fleet.token()}), timeout=DEADLINE_S)
        return sock, s1.Grid()

    def grid_wait(self, sock, grid, needle, deadline=DEADLINE_S):
        end = time.monotonic() + deadline
        while needle not in grid.text():
            left = end - time.monotonic()
            if left <= 0:
                self.log("deadline waiting for %r on the grid" % needle)
                return False
            sock.settimeout(left)
            try:
                kind, data = sock.recv()
            except (OSError, wslib.WsError) as e:
                self.log("grid stream error: %s" % e)
                return False
            if kind == "close":
                self.log("grid stream closed")
                return False
            if kind != "text":
                continue
            msg = json.loads(data)
            if not grid.apply(msg):
                self.log("ws event: %s" % data[:300])
                if msg.get("event") in ("error", "child_exit"):
                    return False
        return True

    def type(self, sock, text):
        """Paste, then Enter, as the app does. False when the stream is gone."""
        try:
            sock.send_text(json.dumps({"action": "input", "text": text}))
            time.sleep(0.15)
            sock.send_text(json.dumps({"action": "input", "text": "\r"}))
        except (OSError, wslib.WsError) as e:
            self.log("typing %r failed: %s" % (text, e))
            return False
        return True

    def start_agent(self, sc, name, cwd):
        """Spawn `name` in `cwd`, attach, wait for the banner. Returns
        (session_id, claude_session, host, sock, grid) or None."""
        csid = str(uuid.uuid4())
        since = now_iso()
        st, resp = self.spawn(name, cwd, csid)
        sid = resp.get("sessionId")
        if not sc.check("%s spawned through the controller" % name, st == 200 and sid,
                        "HTTP %s sessionId %s" % (st, sid) if st == 200 else "HTTP %s %s" % (st, json.dumps(resp)[:300])):
            return None
        try:
            sock, grid = self.attach(sid)
        except (OSError, wslib.WsError) as e:
            sc.check("%s attached (WS /cli/sessions/grid)" % name, False, str(e))
            return None
        ok = self.grid_wait(sock, grid, "session %s" % csid)
        host = self.placed_host(name, since)
        if not sc.check("%s is up: its banner on the grid" % name, ok, "host %s, claude session %s" % (host, csid)):
            sock.close()
            return None
        return sid, csid, host, sock, grid


# ── S2 ──────────────────────────────────────────────────────────────────

def s2(c, sc):
    names = ["s2-a-%s" % c.tag, "s2-b-%s" % c.tag]
    agents = {}
    try:
        for n in names:
            a = c.start_agent(sc, n, c.fleet.workspace(n))
            if not a:
                return
            agents[n] = a
        hosts = {n: a[2] for n, a in agents.items()}
        sc.info["hosts"] = hosts
        if c.fleet.layout == "fleet":
            sc.check("the two agents run on different hosts", len(set(hosts.values())) == 2, json.dumps(hosts))
        for n, (sid, csid, host, sock, grid) in agents.items():
            text = "s2 prompt for %s" % n
            c.type(sock, text)
            sc.check("%s answered a typed prompt" % n, c.grid_wait(sock, grid, "%s1: %s" % (REPLY_MARKER, text)), text)
        for n, (sid, csid, host, sock, grid) in agents.items():
            # The Stop hook runs after the reply; wait for it to finish.
            wait_until(lambda: any((e.get("attrs") or {}).get("hook") == "Stop"
                                   for e in c.fake("cli.hook.done", csid)), 30)
        hook_event = "ctl.hook" if c.fleet.layout == "fleet" else "host.agent_event"
        recorded = c.fleet.daemon_events(c.fleet.st["controller"], hook_event)
        sc.info["daemon_hook_event"] = hook_event
        for n, (sid, csid, host, sock, grid) in agents.items():
            fired = sorted({(e.get("attrs") or {}).get("hook") for e in c.fake("cli.hook.done", csid)
                            if (e.get("attrs") or {}).get("exit") == 0})
            sc.check("%s's CLI fired %s (exit 0)" % (n, "/".join(HOOKS)), set(HOOKS) <= set(fired),
                     "fired: %s (fake trace cli.hook.done, session %s)" % (fired, csid))

            def got(evs, hook):
                return [e for e in evs if (e.get("attrs") or {}).get("event") in (hook, _snake(hook))
                        or (e.get("attrs") or {}).get("hook") == hook]
            # ctl.hook names the session in attrs.pane; host.agent_event in session_id.
            mine = [e for e in recorded if sid in (e.get("session_id"), (e.get("attrs") or {}).get("pane"))]
            sc.info.setdefault("recorded", {})[n] = [(e.get("ts"), (e.get("attrs") or {}).get("event"), e.get("agent_id")) for e in mine]
            missing = [h for h in HOOKS if not got(mine, h)]
            sc.check("the controller recorded every hook of %s for its session" % n, not missing,
                     "missing %s; %d %s event(s) for session %s" % (missing or "none", len(mine), hook_event, sid))
            wrong = [e for e in mine if e.get("agent_id") not in (None, n)]
            sc.check("no hook of %s's session was attributed to another agent" % n, not wrong,
                     "%d misattributed: %s" % (len(wrong), [(e.get("agent_id"), (e.get("attrs") or {}).get("event")) for e in wrong][:5]))
    finally:
        for n, a in agents.items():
            a[3].close()
            c.close(n)


def _snake(hook):
    out = ""
    for ch in hook:
        out += ("_" + ch.lower()) if ch.isupper() and out else ch.lower()
    return out


# ── S3 ──────────────────────────────────────────────────────────────────

def k2_msg_as(c, host, to, text, sender):
    """The real cli/k2 msg, run in `host`'s environment (its HOME and daemon)."""
    h = c.fleet.host(host)
    p = c.fleet.proc(h["proc"])
    env = {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin", "HOME": h["home"],
           "K2_HOME": os.path.join(h["home"], ".k2"), "TMPDIR": p["env"]["TMPDIR"],
           "K2_PORT": str(p["port"])}
    r = subprocess.run([os.path.join(REPO, "cli", "k2"), "msg", to, text, "--from", sender],
                       env=env, capture_output=True, text=True, timeout=HTTP_TIMEOUT_S)
    out = (r.stdout + r.stderr).strip()
    msg_id = None
    for tok in out.replace(",", " ").split():
        if tok.strip('"{}').startswith("m_"):
            msg_id = tok.strip('"{}:')
    try:
        doc = json.loads(r.stdout)
        msg_id = doc.get("msgId") or msg_id
    except ValueError:
        doc = None
    c.ledger(msg_id=msg_id, to=to, text=text, via="cli/k2 msg on %s" % host, rc=r.returncode, response=doc or out[:300])
    return r.returncode, out, msg_id, doc


def workspace_agent(c, sc, name):
    """A workspace agent the way the app makes one: the workspace registered
    (its name is the dir name, `name`) and opened with ensure-pinned-chat.
    Returns (cwd, project id, chat tuple, host) or None."""
    cwd = c.fleet.workspace(name)
    pid, why = c.register_enabled(cwd)
    if not sc.check("workspace %s registered" % name, pid, why):
        return None
    a = open_chat(c, sc, cwd, "%s opened (ensure-pinned-chat)" % name)
    if not a:
        return None
    host = (c.exec_of(a[1]) or {}).get("host")
    return cwd, pid, a, host


def s3(c, sc):
    """k2 msg <workspace>, as agents address each other today (k2
    connections list): alice's CLI runs the real cli/k2 on alice's host."""
    alice, bob = "s3-alice-%s" % c.tag, "s3-bob-%s" % c.tag
    agents = {}
    try:
        for n in (alice, bob):
            w = workspace_agent(c, sc, n)
            if not w:
                return
            agents[n] = w
        ha, hb = agents[alice][3], agents[bob][3]
        sc.info["hosts"] = {alice: ha, bob: hb}
        if c.fleet.layout == "fleet":
            sc.check("alice and bob run on different hosts", ha and hb and ha != hb, "alice %s, bob %s" % (ha, hb))
        nonce = uuid.uuid4().hex[:8]
        text = "S3 hello bob from alice %s" % nonce
        rc, out, msg_id, doc = k2_msg_as(c, ha or c.fleet.hosts()[0]["name"], bob, text, alice)
        sc.info["k2_msg"] = {"rc": rc, "out": out[:500], "msg_id": msg_id}
        sc.check("cli/k2 msg %s on alice's host accepted it" % bob, rc == 0 and (doc or {}).get("success") is not False,
                 "rc %s: %s" % (rc, out[:300]))
        if c.fleet.layout == "fleet":
            state = c.wait_final(msg_id) if msg_id else None
            sc.check("the controller's outbox ends delivered", state == "delivered", "msg %s state %s" % (msg_id, state))
        bob_cwd = agents[bob][0]
        wait_until(lambda: c.replied(bob_cwd, nonce), DEADLINE_S)
        seen = c.prompts_with(bob_cwd, nonce)
        sc.check("bob's transcript has it exactly once, and bob answered", len(seen) == 1 and c.replied(bob_cwd, nonce),
                 "%d time(s): %s" % (len(seen), [(h, s) for h, s, _ in seen]))
        wrong = c.prompts_with(agents[alice][0], nonce)
        sc.check("it did not land on alice", not wrong, "%d time(s) in alice's transcripts" % len(wrong))
    finally:
        for n, w in agents.items():
            w[2][3].close()
            c.close(w[1])


# ── S4, S5: the W1 flows ────────────────────────────────────────────────

def w1_flows(c, out, flows):
    from . import w1
    os.makedirs(out, exist_ok=True)
    token_file = os.path.join(out, ".token")
    with open(token_file, "w") as fh:
        fh.write(c.fleet.token())
    os.chmod(token_file, 0o600)
    hosts = c.fleet.hosts()

    class A:
        port = c.fleet.port
        home = [h["home"] for h in hosts]
        workspace = c.fleet.st["work"]
        fake_trace = None
        only = ",".join(flows)
    A.out = os.path.join(out, "w1")
    # The fake traces of every host, merged, for w1's cli.exec evidence.
    merged = os.path.join(A.out, "fake.trace.jsonl")
    os.makedirs(A.out, exist_ok=True)

    run = w1.W1(A, c.fleet.token())

    def fake_events(event, session):
        return [e for e in c.fleet.fake_events(event) if e.get("session_id") == session]
    run.fake_events = fake_events
    run.fake_trace = merged  # non-empty: trace evidence on, read through fake_events
    run.run(flows)
    os.remove(token_file)
    return run


def s4_s5(c, out, which):
    flow_of = {"S4": "wake", "S5": "heartbeat"}
    run = w1_flows(c, out, [flow_of[s] for s in which])
    docs = {}
    for sid in which:
        sc = Scenario(sid, out)
        f = flow_of[sid]
        for ch in run.checks:
            if ch["flow"] == f:
                sc.checks.append({"name": ch["name"], "ok": ch["ok"], "detail": ch["detail"]})
        sc.info = run.flows.get(f, {})
        sc.lines = [l for l in run.log_lines]
        docs[sid] = sc.finish()
    return docs


# ── S6 ──────────────────────────────────────────────────────────────────

def inbox_files(cwd, title):
    hits = []
    for root, _, files in os.walk(os.path.join(cwd, ".k2")):
        for f in files:
            p = os.path.join(root, f)
            try:
                if title in open(p, errors="replace").read():
                    hits.append(p)
            except OSError:
                pass
    return sorted(hits)


def compose_inbox(c, host, to, title, sender):
    h = c.fleet.host(host)
    st, raw, resp = c.http("POST", "/cli/inbox/compose", form={
        "workspace": to, "title": title, "body": "sent by the lab (%s)" % c.sc.sid, "from": sender},
        port=c.fleet.proc(h["proc"])["port"], token=c.fleet.token(h["proc"]))
    c.ledger(msg_id=resp.get("msgId"), to=to, text=title, kind="inbox", via="inbox compose on %s" % host,
             http=st, response=resp or raw[:300])
    return st, raw, resp


def s6(c, sc):
    """Inbox compose to a workspace (by name) survives restarts."""
    alice, bob = "s6-alice-%s" % c.tag, "s6-bob-%s" % c.tag
    title = "S6 inbox item %s" % uuid.uuid4().hex[:8]
    agents = {}
    try:
        for n in (alice, bob):
            w = workspace_agent(c, sc, n)
            if not w:
                return
            agents[n] = w
        bob_cwd = agents[bob][0]
        if c.fleet.layout == "solo":
            st, raw, resp = compose_inbox(c, "local", bob, title, alice)
            files = wait_until(lambda: inbox_files(bob_cwd, title), 30)
            if not sc.check("the item is in bob's inbox", st == 200 and len(files) == 1, "HTTP %s; %s" % (st, files)):
                return
            before = open(files[0], "rb").read()
            with c.fleet.control():
                down = c.fleet.kill("solo", signal.SIGTERM, 30)
                sc.check("the daemon stopped (SIGTERM)", down, "pid gone=%s" % down)
                if not down:
                    c.fleet.kill("solo")
                c.fleet.start("solo")
            after = inbox_files(bob_cwd, title)
            sc.check("after the restart the item is still there, byte for byte",
                     len(after) == 1 and open(after[0], "rb").read() == before, "%s" % after)
            return

        ha, hb = agents[alice][3], agents[bob][3]
        sc.info["hosts"] = {alice: ha, bob: hb}
        if not sc.check("alice and bob run on different hosts", ha and hb and ha != hb, "alice %s, bob %s" % (ha, hb)):
            return
        with c.fleet.control():
            sc.check("bob's host %s killed (SIGKILL)" % hb, c.fleet.kill(hb), "")
            st, raw, resp = compose_inbox(c, ha, bob, title, alice)
            msg_id = resp.get("msgId")
            sc.info["msg_id"] = msg_id
            sc.check("inbox compose on alice's host is accepted while bob's host is down",
                     st == 200 and msg_id and resp.get("success") is not False, "HTTP %s %s" % (st, raw[:300]))
            down = c.fleet.kill("controller", signal.SIGTERM, 30)
            if not down:
                c.fleet.kill("controller")
            c.fleet.start("controller")
            sc.check("the controller restarted while the item waited", c.fleet.alive("controller"),
                     "stopped by SIGTERM: %s" % down)
            wait_until(lambda: (fleetlib.http_json(c.fleet.proc(ha)["port"], "/cli/hosts/status") or {}).get("linked"), 60)
            c.fleet.start(hb)
            sc.check("bob's host is back", c.fleet.alive(hb), "")
        # bob's next need: the app opens him again.
        agents[bob][2][3].close()
        b = open_chat(c, sc, bob_cwd, "bob's next need: the app opens him again")
        if b:
            agents[bob] = (bob_cwd, agents[bob][1], b, hb)
        state = c.wait_final(msg_id) if msg_id else None
        files = wait_until(lambda: inbox_files(bob_cwd, title), 30)
        sc.check("the controller's outbox ends delivered", state == "delivered", "msg %s state %s" % (msg_id, state))
        sc.check("the item is in bob's inbox exactly once", len(files) == 1, "%s" % files)
        # Read once alice's host answers (under --chaos it may be restarting).
        up = wait_until(lambda: c.outbox(ha, msg_id), 60, 1.0) or {}
        sc.check("alice's host's outbox (hop up) ends delivered", (up.get("message") or {}).get("state") == "delivered",
                 json.dumps(up.get("message"))[:300])
    finally:
        for n, w in agents.items():
            w[2][3].close()
            c.close(w[1])


# ── S7 ──────────────────────────────────────────────────────────────────

def open_chat(c, sc, cwd, what):
    """The app opening a workspace's agent: POST /cli/workspace/ensure-pinned-chat
    {project}. Returns (daemon session, claude session, reused, sock, grid) or None."""
    st, raw, resp = c.http("POST", "/cli/workspace/ensure-pinned-chat", body={"project": cwd})
    sid, csid = resp.get("sessionId"), resp.get("claudeSessionId")
    if not sc.check(what, st == 200 and sid and csid,
                    "HTTP %s session %s claude %s reused %s args %s" % (st, sid, csid, resp.get("reused"),
                                                                       json.dumps(resp.get("args")))):
        return None
    try:
        sock, grid = c.attach(sid)
    except (OSError, wslib.WsError) as e:
        sc.check("attach the agent's grid", False, str(e))
        return None
    if not sc.check("its banner is on the grid", c.grid_wait(sock, grid, "session %s" % csid), csid):
        sock.close()
        return None
    return sid, csid, resp.get("reused"), sock, grid


def ask_chat(c, sc, cwd, a, text, n=None, tries=3):
    """Type `text` into the open chat `a` and wait for its reply. When the
    CLI dies first (a chaos kill: the turn in progress is lost, PRD failure
    table), do what the user would: the next need reopens the agent
    (ensure-pinned-chat) and the prompt is typed again. Every retry is
    logged and counted in the scenario's info. Returns (ok, a)."""
    for attempt in range(1, tries + 1):
        sid, csid, reused, sock, grid = a
        want = "%s%s: %s" % (REPLY_MARKER, n, text) if n else text
        typed = c.type(sock, text)
        if typed and c.grid_wait(sock, grid, want if n else "%s" % text):
            if n or wait_until(lambda: c.replied(cwd, text)):
                return True, a
        if c.fleet.cli_alive((c.exec_of(csid) or {}).get("host"), (c.exec_of(csid) or {}).get("pid")):
            return False, a  # the CLI lives: a real failure, no retry
        sc.info["retries_after_cli_death"] = sc.info.get("retries_after_cli_death", 0) + 1
        sc.log("the CLI died before answering %r (attempt %d): the next need reopens it" % (text, attempt))
        sock.close()
        b = open_chat(c, sc, cwd, "the next need reopens the agent after its CLI died (attempt %d)" % attempt)
        if not b:
            return False, a
        a = b
        n = None  # the turn number restarts from the transcript; match the text only
    return False, a


def s7(c, sc):
    cwd = c.fleet.workspace("s7-%s" % c.tag)
    pid, why = c.register_enabled(cwd)
    if not sc.check("workspace registered", pid, why):
        return
    a = open_chat(c, sc, cwd, "the app opens the agent (ensure-pinned-chat)")
    if not a:
        return
    sid, csid, _, sock, grid = a
    sc.info.update(session=sid, claude_session=csid, project=pid)
    try:
        fact = "s7 fact %s" % uuid.uuid4().hex[:6]
        n1 = "s7 first [[remember: %s]]" % fact
        ok, a = ask_chat(c, sc, cwd, a, n1, 1)
        sid, csid, _, sock, grid = a
        if not sc.check("it answered and remembered", ok, n1):
            return
        wait_until(lambda: c.turn_events(csid, "cli.turn.end"), 30)
        ex = c.exec_of(csid)
        host, cli_pid = (ex or {}).get("host"), (ex or {}).get("pid")
        sc.info.update(host=host, cli_pid=cli_pid)
        ends = c.turn_events(csid, "cli.turn.end")
        mem_hash = (ends[-1].get("attrs") or {}).get("memory_sha256") if ends else None
        mem_file = c.memory_file(host, cwd) if host else ""
        mem_bytes = open(mem_file, "rb").read() if os.path.isfile(mem_file) else None
        sc.check("MEMORY.md holds the fact", mem_bytes is not None and fact.encode() in mem_bytes, mem_file)

        n2 = "s7 long %s [[turn-ms=60000]]" % uuid.uuid4().hex[:6]
        starts = len(c.turn_events(csid, "cli.turn.start"))
        c.type(sock, n2)
        started = wait_until(lambda: len(c.turn_events(csid, "cli.turn.start")) > starts, 30)
        if not sc.check("a long turn started", started, n2):
            return
        with c.fleet.control():
            try:
                c.fleet.kill_cli(cli_pid, host=host)
                killed = wait_until(lambda: not c.fleet.cli_alive(host, cli_pid), 10)
            except (fleetlib.FleetError, ProcessLookupError, TypeError) as e:
                killed = False
                c.log("kill failed: %s" % e)
        sc.check("the CLI was SIGKILLed mid-turn", killed and len(c.turn_events(csid, "cli.turn.end")) == len(ends),
                 "pid %s on %s" % (cli_pid, host))
        sock.close()
        sock = None
        # The daemon sees the child go; the next need is the app opening it again.
        time.sleep(1.0)  # let the host report the exit
        b = open_chat(c, sc, cwd, "the next need: the app opens the agent again")
        if not b:
            return
        sid2, csid2, reused, sock, grid = b
        sc.check("the next need resumed the same provider session", csid2 == csid,
                 "before %s, now %s (reused=%s)" % (csid, csid2, reused))
        resumed = [e for e in c.fake("cli.exec", csid) if (e.get("attrs") or {}).get("resumed")]
        sc.check("a new CLI was started with --resume", bool(resumed),
                 "argv %s" % json.dumps((resumed[-1].get("attrs") or {}).get("argv")) if resumed else "no resumed cli.exec for %s" % csid)
        if resumed:
            h = (resumed[-1].get("attrs") or {}).get("memory_sha256")
            sc.check("I3: the resumed CLI started with the same MEMORY.md (sha256)", h == mem_hash,
                     "before %s, at resume %s" % (mem_hash, h))
            rhost = resumed[-1].get("host")
            now_file = c.memory_file(rhost, cwd)
            now_bytes = open(now_file, "rb").read() if os.path.isfile(now_file) else None
            sc.check("MEMORY.md bytes unchanged by the crash and resume", now_bytes == mem_bytes,
                     "%s (resumed on %s; the crash was on %s)" % (now_file, rhost, host))
        n3 = "s7 after %s" % uuid.uuid4().hex[:6]
        c.type(sock, n3)
        sc.check("the resumed agent answers", wait_until(lambda: c.replied(cwd, n3)), n3)
    finally:
        if sock:
            sock.close()
        c.close(pid)


# ── S8 ──────────────────────────────────────────────────────────────────

def s8(c, sc):
    cwd = c.fleet.workspace("s8-%s" % c.tag)
    pid, why = c.register_enabled(cwd)
    if not sc.check("workspace registered", pid, why):
        return
    a = open_chat(c, sc, cwd, "the app opens the agent (ensure-pinned-chat)")
    if not a:
        return
    sid, csid, _, sock, grid = a
    try:
        n1 = "s8 first %s" % uuid.uuid4().hex[:6]
        c.type(sock, n1)
        if not sc.check("it answers", c.grid_wait(sock, grid, "%s1: %s" % (REPLY_MARKER, n1)), n1):
            return
        ex = c.exec_of(csid)
        host = (ex or {}).get("host")
        sc.info.update(host=host, session=sid, claude_session=csid, cli_pid=(ex or {}).get("pid"))
        n2 = "s8 long %s [[turn-ms=8000]]" % uuid.uuid4().hex[:6]
        starts = len(c.turn_events(csid, "cli.turn.start"))
        c.type(sock, n2)
        started = wait_until(lambda: len(c.turn_events(csid, "cli.turn.start")) > starts, 30)
        if not sc.check("a long turn started on %s" % host, started, n2):
            return
        sock.close()
        sock = None
        with c.fleet.control():
            t_term = now_iso()
            exited = c.fleet.kill(host, signal.SIGTERM, 120)
            t_exit = now_iso()
            sc.info.update(sigterm=t_term, exited=t_exit)
            sc.check("%s exited by itself after SIGTERM (drain)" % host, exited, "SIGTERM %s, gone %s" % (t_term, t_exit))
            if not exited:
                c.fleet.kill(host)
            ends = [e for e in c.turn_events(csid, "cli.turn.end") if (e.get("ts") or "") >= t_term]
            stops = [e for e in c.turn_events(csid, "cli.hook.done")
                     if (e.get("attrs") or {}).get("hook") == "Stop" and (e.get("ts") or "") >= t_term]
            sc.check("the turn in progress finished before the host exited (turn end and Stop hook)",
                     bool(ends) and bool(stops) and ends[0]["ts"] <= t_exit and stops[0]["ts"] <= t_exit,
                     "turn.end %s, Stop %s, host gone %s" % ([e["ts"] for e in ends][:1], [e["ts"] for e in stops][:1], t_exit))
            n3 = "s8-while-down-%s" % uuid.uuid4().hex[:6]
            st, raw, resp = c.msg(cwd, n3, wake=False)
            msg_id = resp.get("msgId")
            sc.check("a message sent while the host is down is accepted (or fails with a reason), not lost silently",
                     st == 200 and (resp.get("success") is True or resp.get("reason")), "HTTP %s %s" % (st, raw[:300]))
            t_restart = now_iso()
            c.fleet.start(host)
        resumed = wait_until(lambda: [e for e in c.fake("cli.exec", csid)
                                      if (e.get("attrs") or {}).get("resumed") and (e.get("ts") or "") >= t_restart], 60)
        sc.check("the replacement host resumed the agent that was awake (same session, no re-spawn by the lab)",
                 bool(resumed), "resumed cli.exec of %s after %s: %d" % (csid, t_restart, len(resumed or [])))
        wait_until(lambda: c.replied(cwd, n3), 60)
        seen3 = c.prompts_with(cwd, n3)
        state = c.wait_final(msg_id, 60) if msg_id else None
        sc.check("the message sent while it was down was delivered exactly once",
                 len(seen3) == 1 and c.replied(cwd, n3) and state in (None, "delivered"),
                 "in transcripts %d time(s); outbox state %s" % (len(seen3), state))
    finally:
        if sock:
            sock.close()
        c.close(pid)


# ── S9 ──────────────────────────────────────────────────────────────────

def s9(c, sc):
    name = "s9-%s" % c.tag
    cwd = c.fleet.workspace(name)
    a = c.start_agent(sc, name, cwd)
    if not a:
        return
    sid, csid, host, sock, grid = a
    ex = c.exec_of(csid)
    cli_pid = (ex or {}).get("pid")
    sc.info.update(host=host, session=sid, claude_session=csid, cli_pid=cli_pid)
    try:
        n1 = "s9 long %s [[turn-ms=6000]]" % uuid.uuid4().hex[:6]
        c.type(sock, n1)
        started = wait_until(lambda: c.turn_events(csid, "cli.turn.start"), 30)
        if not sc.check("a long turn started", started, ""):
            return
        sock.close()
        with c.fleet.control():
            t_down = now_iso()
            stopped = c.fleet.kill("controller", signal.SIGTERM, 30)
            if not stopped:
                c.fleet.kill("controller")
            ended = wait_until(lambda: c.turn_events(csid, "cli.turn.end"), 30)
            t_up_start = now_iso()
            c.fleet.start("controller")
            t_up = now_iso()
        sc.info.update(controller_down=t_down, controller_restarting=t_up_start, controller_up=t_up)
        sc.check("the agent finished its turn while the controller was down",
                 bool(ended) and t_down <= ended[0]["ts"] <= t_up_start,
                 "turn.end %s; controller down %s, restarted %s" % (ended[0]["ts"] if ended else None, t_down, t_up_start))
        execs = c.fake("cli.exec", csid)
        exits = [e for e in c.fake("cli.exit", csid)]
        alive = c.fleet.cli_alive(host, cli_pid)
        sc.check("the agent's CLI kept running (same pid, no exit, no new exec)",
                 alive and not exits and len(execs) == 1,
                 "pid %s alive=%s; cli.exit %d; cli.exec %d" % (cli_pid, alive, len(exits), len(execs)))
        adopted = wait_until(lambda: [e for e in c.fleet.daemon_events("controller", "ctl.adopt")
                                      if e.get("agent_id") == name and (e.get("ts") or "") >= t_up_start], 30)
        status = fleetlib.http_json(c.fleet.port, "/cli/hosts/status") or {}
        placements = {h.get("host_id"): h.get("placements") for h in status.get("hosts", [])}
        sc.check("the restarted controller re-adopted the agent (ctl.adopt; placement on %s)" % host,
                 bool(adopted) and (placements.get(host) or 0) >= 1,
                 "ctl.adopt %d; placements %s" % (len(adopted or []), placements))
        try:
            sock, grid = c.attach(sid)
            replay = c.grid_wait(sock, grid, "%s1: %s" % (REPLY_MARKER, n1), 30)
        except (OSError, wslib.WsError) as e:
            replay, sock = False, None
            c.log("reattach failed: %s" % e)
        sc.check("a client reattached through the controller sees the reply made while it was down", replay,
                 "session %s" % sid)
        if sock:
            n2 = "s9 after %s" % uuid.uuid4().hex[:6]
            typed = c.type(sock, n2)
            sc.check("typing reaches the same agent and is answered",
                     typed and c.grid_wait(sock, grid, "%s2: %s" % (REPLY_MARKER, n2), 30), n2 if typed else "the grid stream was closed")
        n3 = "s9-msg-%s" % uuid.uuid4().hex[:6]
        st, raw, resp = c.msg(name, n3, wake=False)
        wait_until(lambda: c.replied(cwd, n3), 60)
        sc.check("k2 msg reaches the same CLI (no re-spawn)",
                 c.replied(cwd, n3) and len(c.fake("cli.exec", csid)) == 1 and c.fleet.cli_alive(host, cli_pid),
                 "HTTP %s %s" % (st, raw[:200]))
    finally:
        if sock:
            sock.close()
        c.close(name)


SCENARIOS = {"S2": s2, "S3": s3, "S6": s6, "S7": s7, "S8": s8, "S9": s9}


def run(state_path, out, only, min_seconds=0):
    """Run `only` once into `out`; with min_seconds (a --chaos window), run
    the set in rounds (out/round-<k>/) until that long has passed, and write
    each scenario's verdict over all rounds to out/<S>.json: pass only if
    every round passed. The message ledger is always out/messages.jsonl."""
    if not min_seconds:
        return run_once(state_path, out, only, out)
    start, rounds, k = time.monotonic(), [], 0
    while k == 0 or time.monotonic() - start < min_seconds:
        k += 1
        rounds.append(run_once(state_path, os.path.join(out, "round-%d" % k), only, out))
    results = {}
    for sid in only:
        per = [(i + 1, r[sid]) for i, r in enumerate(rounds) if sid in r]
        statuses = {d["status"] for _, d in per}
        status = "n/a" if statuses == {"n/a"} else ("pass" if statuses <= {"pass", "n/a"} else "fail")
        checks = [{"name": "round %d: %s" % (i, d["status"].upper()), "ok": d["status"] in ("pass", "n/a"),
                   "detail": "; ".join("%s -- %s" % (c["name"], c["detail"][:200]) for c in d["checks"] if not c["ok"])
                   or "%d checks passed" % len(d["checks"])} for i, d in per]
        doc = {"scenario": sid, "title": TITLES.get(sid, sid), "status": status, "checks": checks,
               "rounds": len(per), "info": {"round_dirs": ["round-%d" % i for i, _ in per]}}
        if status == "n/a":
            doc["reason"] = per[0][1].get("reason")
        with open(os.path.join(out, sid + ".json"), "w") as fh:
            json.dump(doc, fh, indent=2, default=str)
        with open(os.path.join(out, sid + ".log"), "w") as fh:
            fh.write("%d round(s) under chaos; logs in round-<k>/%s.log\n" % (len(per), sid))
        print("%s %s over %d round(s)" % (sid, status.upper(), len(per)), flush=True)
        results[sid] = doc
    return results


def run_once(state_path, out, only, ledger_dir):
    os.makedirs(out, exist_ok=True)
    c = Client(state_path, ledger_dir)
    results = {}
    w1_set = [s for s in only if s in ("S4", "S5")]
    for sid in only:
        if sid in ("S4", "S5"):
            if w1_set:
                results.update(s4_s5(c, out, w1_set))
                w1_set = []
            continue
        sc = Scenario(sid, out)
        c.sc = sc
        if sid in FLEET_ONLY and c.fleet.layout != "fleet":
            results[sid] = sc.finish(reason="applies to fleet only (PRD scenario matrix: - on solo)", status="n/a")
            continue
        try:
            SCENARIOS[sid](c, sc)
        except Exception as e:  # report, never hang or hide
            sc.log(traceback.format_exc())
            sc.check("scenario ran to the end", False, "%s: %s" % (type(e).__name__, e))
        c.fleet.reload()
        results[sid] = sc.finish()
        c.sc = None
    return results
