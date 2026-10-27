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

import contextlib
import json
import os
import shutil
import signal
import socket
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


def gone_wait_s():
    """A wait that outlasts the outbox's gone grace (LAB_GONE_GRACE_S, else
    the daemon's 120 s) by 30 s, so a message the gone rule settles is seen
    settled, not still queued."""
    try:
        grace = int(os.environ.get("LAB_GONE_GRACE_S") or 120)
    except ValueError:
        grace = 120
    return max(60, grace + 30)
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
    "S10": "Host lost mid-session, its agent moves with its last turn's home",
}
FLEET_ONLY = ("S8", "S9", "S10")


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


# Requests the app may simply send again when the daemon died while
# answering: they converge on one state (register, enable, open, close).
IDEMPOTENT = ("/cli/projects/add-from-path", "/cli/projects/update", "/cli/workspace/ensure-pinned-chat",
              "/cli/sessions/v2/close", "/cli/hosts/outbox", "/cli/hosts/status")


def daemon_down(status, text, method="POST", path=""):
    """The daemon is not there to answer yet: connection refused (it is
    down: the request never reached it, so sending again cannot duplicate
    it), 503 while it boots (`"state":"migrating"`), or, for a GET or an
    idempotent route only, a connection the daemon dropped while dying."""
    if (status == 0 and "refused" in text) or (status == 503 and '"migrating"' in text):
        return True
    # A controller with no host linked refuses a spawn explicitly; nothing
    # was placed, so asking again once a host is back is safe (--chaos).
    if status == 503 and "no agent host is connected" in text:
        return True
    # A host the controller still counts as lost (it has not linked back
    # yet, e.g. right after a controller restart): an idempotent request
    # converges once it links, so wait for it as for a booting daemon.
    if status == 502 and "is not connected" in text and path in IDEMPOTENT:
        return True
    dropped = status == 0 and ("closed connection" in text or "reset" in text or "Broken pipe" in text)
    return dropped and (method == "GET" or path in IDEMPOTENT)


def chaos_killed(host, pid):
    """The chaos.log entry in which --chaos itself SIGKILLed the agent CLI
    `pid` on `host`, or None (no chaos, or not chaos's doing)."""
    path = os.environ.get("LAB_CHAOS_LOG")
    if not (os.environ.get("LAB_CHAOS") and path and pid and os.path.isfile(path)):
        return None
    for e in fleetlib.read_jsonl(path):
        if e.get("kind") == "cli" and e.get("action") == "SIGKILL" and e.get("pid") == pid and e.get("host") == host:
            return e
    return None


def chaos_hit(host, pid, since, kinds=("cli", "host")):
    """chaos.log entries since `since` (ISO) in which --chaos itself
    SIGKILLed the agent CLI `pid` on `host`, or `host` itself (a host, or
    with kinds=("controller",) the controller)."""
    path = os.environ.get("LAB_CHAOS_LOG")
    if not (os.environ.get("LAB_CHAOS") and path and host and os.path.isfile(path)):
        return []
    return [e for e in fleetlib.read_jsonl(path)
            if (e.get("ts") or "") >= since and e.get("action") == "SIGKILL" and e.get("kind") in kinds
            and ((e.get("kind") == "cli" and e.get("host") == host and e.get("pid") == pid)
                 or (e.get("kind") != "cli" and e.get("target", e.get("host")) == host))]


def send_waiting_for_daemon(make_req, log=None, method="POST", path=""):
    """urlopen that waits (up to DAEMON_BACK_S) while the daemon is down or
    booting, like the app reconnecting; anything else is returned as is.
    `make_req()` builds the request on every attempt, so it carries the
    token the daemon has now (a restarted daemon issues a new one). A 403
    right after waiting is retried once with the new token."""
    end = time.monotonic() + DAEMON_BACK_S
    waited = refreshed = False
    while True:
        req = make_req()
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as r:
                status, text = r.status, r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            status, text = e.code, e.read().decode("utf-8", "replace")
        except OSError as e:
            status, text = 0, "request failed: %s" % e
        if waited and status == 403 and not refreshed:
            refreshed = True  # the token file may have changed under us once more
            time.sleep(0.5)
            continue
        if not daemon_down(status, text, method, path) or time.monotonic() >= end:
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
        self.projects = {}  # cwd -> project id (register_enabled)

    # ── plumbing ────────────────────────────────────────────────────────
    def log(self, msg):
        if self.sc:
            self.sc.log(msg)

    def http(self, method, path, query=None, body=None, form=None, port=None, token=None):
        q = dict(query or {})
        proc_of_port = next((n for n, p in self.fleet.st["procs"].items() if p["port"] == port), None)

        def url():
            q["token"] = token if token and not proc_of_port else self.fleet.token(proc_of_port)
            return "http://127.0.0.1:%d%s?%s" % (port or self.fleet.port, path, urllib.parse.urlencode(q))
        headers = {}
        if form is not None:
            data = urllib.parse.urlencode(form).encode()
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        elif body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        else:
            data = b"" if method == "POST" else None
        status, text = send_waiting_for_daemon(lambda: urllib.request.Request(url(), data=data, method=method,
                                                                              headers=headers),
                                               self.log, method, path)
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

    def spawn_args(self, name, cwd, args):
        st, text, resp = self.http("POST", "/cli/sessions/v2/spawn", body={
            "agent_name": name, "cwd": cwd, "command": "claude", "args": args, "cols": 100, "rows": 30})
        return st, resp if resp else {"_": text[:300]}

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
        self.projects[cwd] = pid
        return pid, "project %s" % pid

    def activate(self, cwd):
        """What the app does when it opens or focuses a workspace: POST
        /cli/projects/activate {projectId} stamps its need clock, so the
        active reaper leaves its chat alone for the interaction window
        (without it a chat counts as aged out and is closed ~30-60 s on,
        even mid-turn). Logged, never fatal (an older daemon: 404).
        LAB_NO_ACTIVATE=1 skips it: agents nobody opens in the app, driven
        only by messages (K2-d: a queued delivery must count as a need)."""
        pid = self.projects.get(cwd)
        if pid and os.environ.get("LAB_NO_ACTIVATE") == "1":
            self.log("LAB_NO_ACTIVATE=1: %s not activated (message-only agent)" % pid)
            return
        if pid:
            st, text, _ = self.http("POST", "/cli/projects/activate", body={"projectId": pid})
            if st != 200:
                self.log("activate %s: HTTP %s %s" % (pid, st, text[:120]))

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
        # The newest by ts: fake_events is in file order per host, so the
        # last one is host-2's latest even when host-1 exec'd it since.
        evs = self.fake("cli.exec", session)
        return max(evs, key=lambda e: e.get("ts", "")) if evs else None

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

    def traced_final(self, msg_id, direct=True):
        """A final state the controller traced without an outbox row: since
        the S4 wake fix it delivers some messages directly
        (`[delivery] … delivered directly`, `ctl.msg_accept` status delivered).
        `ctl.msg_accept` says delivered for every message a host hands up,
        queued ones too, so it counts only when `direct` (no outbox row) and
        the controller never sent the message down (`ctl.deliver`)."""
        evs = [e for e in self.fleet.daemon_events(self.fleet.st["controller"]) if e.get("msg_id") == msg_id]
        if any(e.get("event") == "delivery.failed" for e in evs):
            return "failed"
        if direct and not any(e.get("event") == "ctl.deliver" for e in evs) and any(
                e.get("event") == "ctl.msg_accept" and (e.get("attrs") or {}).get("status") == "delivered" for e in evs):
            return "delivered"
        return None

    def final_state(self, msg_id):
        resp = self.outbox(None, msg_id)
        st = ((resp or {}).get("message") or {}).get("state")
        if st in ("delivered", "failed"):
            return st
        # Direct only when the controller answered and has no row for it.
        return self.traced_final(msg_id, direct=resp is not None and st is None) or st

    def final_reason(self, msg_id):
        """Why the outbox row ended (a failure's reason), or None."""
        resp = self.outbox(None, msg_id) or {}
        return (resp.get("message") or {}).get("reason")

    def wait_final(self, msg_id, deadline=DEADLINE_S):
        return wait_until(lambda: self.final_state(msg_id) in ("delivered", "failed") and self.final_state(msg_id),
                          deadline, 0.3) or self.final_state(msg_id)

    # ── grid ────────────────────────────────────────────────────────────
    def reattach(self, sc, sid, sock):
        """The grid stream through the controller closed under us (a
        restart): wait until the controller answers and attach again, as the
        app does. Counted in the scenario's info. Returns (sock, grid) or
        (None, None)."""
        sc.info["reattached_after_stream_close"] = sc.info.get("reattached_after_stream_close", 0) + 1
        sc.log("the grid stream closed; reattaching once the controller answers")
        with contextlib.suppress(OSError):
            sock.close()
        wait_until(lambda: fleetlib.http_json(self.fleet.port, "/cli/hosts/status"), DAEMON_BACK_S, 0.5)
        try:
            return self.attach(sid)
        except (OSError, wslib.WsError) as e:
            self.log("reattach failed: %s" % e)
            return None, None

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
        frames = {}  # grid event -> count, for the evidence when it fails

        def missed(why):
            # What the stream did carry: no frame at all (the relay) or
            # frames without the needle (what the host rendered).
            tail = [r for r in grid.text().splitlines() if r.strip()][-6:]
            self.log("%s; grid frames applied %s; grid text tail %r" % (why, frames or "none", tail))
            return False

        while not grid.has(needle):
            left = end - time.monotonic()
            if left <= 0:
                return missed("deadline waiting for %r on the grid" % needle)
            sock.settimeout(left)
            try:
                kind, data = sock.recv()
            except (OSError, wslib.WsError) as e:
                return missed("grid stream error: %s" % e)
            if kind == "close":
                return missed("grid stream closed")
            if kind != "text":
                continue
            msg = json.loads(data)
            if grid.apply(msg):
                frames[msg.get("event")] = frames.get(msg.get("event"), 0) + 1
            else:
                self.log("ws event: %s" % data[:300])
                if msg.get("event") in ("error", "child_exit"):
                    return False
        return True

    def stream_end(self, sock, deadline):
        """How an open grid stream ends ("close", "error: …", an error event),
        or None when it is still open at the deadline (a hang)."""
        end = time.monotonic() + deadline
        while time.monotonic() < end:
            sock.settimeout(max(0.1, end - time.monotonic()))
            try:
                kind, data = sock.recv()
            except socket.timeout:
                return None
            except (OSError, wslib.WsError) as e:
                return "error: %s" % e
            if kind == "close":
                return "close"
            if kind == "text":
                ev = json.loads(data).get("event")
                if ev in ("error", "child_exit"):
                    return "event %s: %s" % (ev, data[:160])
        return None

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
           # compose keeps a daemon's env in the compose file: a tmp of our own.
           "K2_HOME": os.path.join(h["home"], ".k2"),
           "TMPDIR": p["env"].get("TMPDIR") or os.path.join(c.fleet.st["root"], "k2msg-tmp"),
           "K2_PORT": str(p["port"])}
    os.makedirs(env["TMPDIR"], exist_ok=True)
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
            if ha and ha == hb and os.environ.get("LAB_CHAOS"):
                # --chaos had the other host down at placement, so both went
                # to one host (correct placement). S3 needs the message to
                # cross hosts: alice's k2 msg runs on the other host instead.
                ha = next((h["name"] for h in c.fleet.hosts() if h["name"] != hb), None)
                sc.log("alice and bob both on %s (chaos): sending from %s instead" % (hb, ha))
                sc.info["sent_from"] = ha
            sc.check("alice and bob run on different hosts", ha and hb and ha != hb, "alice %s, bob %s" % (ha, hb))
        nonce = uuid.uuid4().hex[:8]
        text = "S3 hello bob from alice %s" % nonce
        rc, out, msg_id, doc = k2_msg_as(c, ha or c.fleet.hosts()[0]["name"], bob, text, alice)
        sc.info["k2_msg"] = {"rc": rc, "out": out[:500], "msg_id": msg_id}
        sc.check("cli/k2 msg %s on alice's host accepted it" % bob, rc == 0 and (doc or {}).get("success") is not False,
                 "rc %s: %s" % (rc, out[:300]))
        if c.fleet.layout == "fleet":
            state = c.wait_final(msg_id) if msg_id else None
            sc.check("the controller ends it delivered (outbox row, or traced direct delivery)", state == "delivered",
                     "msg %s state %s" % (msg_id, state))
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
    run.token_fn = c.fleet.token
    run.cli_alive_fn = c.fleet.cli_alive

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


def placed_host(c, agent):
    """The host holding `agent`'s placement (GET /cli/hosts/placements on
    the controller: last_host), or None."""
    st, _, resp = c.http("GET", "/cli/hosts/placements")
    p = next((x for x in resp.get("agents", []) if x.get("agent") == agent), {}) if st == 200 else {}
    return p.get("last_host") or p.get("pinned_host")


def inbox_on_host(c, host, cwd, title):
    """bob's inbox item as his host sees it: through the host when the
    hosts have their own disks (the item is delivered there, not to the
    controller's or the runner's copy), else read here."""
    files = c.fleet.host_files(host, os.path.join(cwd, ".k2"), title) if host else None
    return inbox_files(cwd, title) if files is None else files


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
        if ha == hb:
            # Both placed on one host (under --chaos the other may have been
            # down at placement). The item is composed on another host: the
            # sender is only a label; what S6 needs is bob's host down.
            ha = next((h["name"] for h in c.fleet.hosts() if h["name"] != hb), None)
            sc.log("alice and bob both on %s: composing from %s instead" % (hb, ha))
            sc.info["composed_on"] = ha
        if not sc.check("bob's host and the sending host differ", ha and hb and ha != hb, "send %s, bob %s" % (ha, hb)):
            return
        t_compose = now_iso()
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
        state = c.wait_final(msg_id, gone_wait_s()) if msg_id else None
        # Read through bob's host (delivery, not the fs relay, is under test).
        hb_now = placed_host(c, agents[bob][1]) or hb
        sc.info["bob_inbox_read_on"] = hb_now
        files = wait_until(lambda: inbox_on_host(c, hb_now, bob_cwd, title), 30 if state != "failed" else 3)
        # PRD failure table: when --chaos killed bob's CLI, or his host with
        # it, before the item landed (the host answers retry while no session
        # is live), the gone rule failing it with a reason is the right end;
        # then it is not in bob's inbox.
        csid = agents[bob][2][1]
        killed = [e for x in c.fake("cli.exec", csid) if (x.get("ts") or "") >= t_compose
                  for e in chaos_hit(x.get("host"), x.get("pid"), t_compose)]
        sc.info["s6_chaos_kills_of_bob"] = [(e["ts"], e.get("kind"), e.get("host"), e.get("pid")) for e in killed]
        lost_to_chaos = bool(killed) and state == "failed"
        sc.check("the controller's outbox ends delivered (or failed with a reason after --chaos killed bob's CLI or host)",
                 state == "delivered" or lost_to_chaos,
                 "msg %s state %s; chaos kills of bob's CLI or host %s" % (msg_id, state, sc.info["s6_chaos_kills_of_bob"]))
        sc.check("the item is in bob's inbox exactly once (none when it failed)",
                 len(files) == (0 if lost_to_chaos else 1), "%s" % files)
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
    {project}, after activating it as the app does (Client.activate).
    Returns (daemon session, claude session, reused, sock, grid) or None."""
    c.activate(cwd)
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
            if c.stream_end(sock, 1) is None:
                return False, a  # the CLI lives and the stream is open: a real failure, no retry
            sock, grid = c.reattach(sc, sid, sock)
            if not sock:
                return False, a
            a = (sid, csid, reused, sock, grid)
            if not wait_until(lambda: c.prompts_with(cwd, text), 5):
                # The prompt never reached the agent. Typing again can hit a
                # stream a second controller restart closed: reattach again
                # (the next attempt) rather than wait for a reply to nothing.
                if not c.type(sock, text) and attempt < tries:
                    sc.log("typing %r after the reattach failed too (attempt %d): reattaching again" % (text, attempt))
                    continue
            return bool(wait_until(lambda: c.replied(cwd, text), DEADLINE_S)), a
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
        started = False
        for attempt in (1, 2, 3):
            starts = len(c.turn_events(csid, "cli.turn.start"))
            c.type(sock, n2)
            started = wait_until(lambda: len(c.turn_events(csid, "cli.turn.start")) > starts, 30)
            if started or c.fleet.cli_alive(host, cli_pid):
                break
            # --chaos killed the CLI first: reopen it (the next need), type again.
            sc.info["retries_after_cli_death"] = sc.info.get("retries_after_cli_death", 0) + 1
            sock.close()
            a = open_chat(c, sc, cwd, "the next need reopens the agent before the long turn (attempt %d)" % attempt)
            if not a:
                return
            sid, csid, _, sock, grid = a
            ex = c.exec_of(csid)
            host, cli_pid = (ex or {}).get("host"), (ex or {}).get("pid")
            ends = c.turn_events(csid, "cli.turn.end")
            mem_hash = (ends[-1].get("attrs") or {}).get("memory_sha256") if ends else mem_hash
        if not sc.check("a long turn started", started, n2):
            return
        with c.fleet.control():
            try:
                c.fleet.kill_cli(cli_pid, host=host)
                killed = wait_until(lambda: not c.fleet.cli_alive(host, cli_pid), 10)
            except (fleetlib.FleetError, ProcessLookupError, TypeError) as e:
                # Already gone (--chaos got there first): the CLI died mid-turn
                # all the same, which is the state S7 needs.
                killed = not c.fleet.cli_alive(host, cli_pid)
                c.log("kill: %s; the CLI is %s" % (e, "already gone" if killed else "still alive"))
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
        ok, b = ask_chat(c, sc, cwd, b, n3)
        sock = b[3]
        sc.check("the resumed agent answers", ok, n3)
    finally:
        if sock:
            sock.close()
        c.close(pid)


# ── S8 ──────────────────────────────────────────────────────────────────

def s8(c, sc):
    """S8 has two halves; both must pass. crash: the host is SIGKILLed mid-turn
    (dist-link W3's definition): the controller reports it lost, refuses to
    place the agent elsewhere (I1), the grid answers an error, and when the
    host is back the agent spawns again resuming its conversation with the
    interrupted prompt in its transcript. drain: the PRD failure table's row
    for S8: SIGTERM drains (the turn's Stop before exit) and the replacement
    resumes the agents that were awake, delivering what waited."""
    s8_crash(c, sc)
    redo_on_chaos(c, sc, s8_drain, "S8 drain")


def s8_crash(c, sc):
    name = "s8c-%s" % c.tag
    cwd = c.fleet.workspace(name)
    a = c.start_agent(sc, name, cwd)
    if not a:
        return
    sid, csid, host, sock, grid = a
    sc.info["crash"] = {"host": host, "claude_session": csid}
    text = "s8 crash %s [[turn-ms=8000]]" % uuid.uuid4().hex[:6]
    try:
        c.type(sock, text)
        # Crash once the agent has the prompt: the turn started AND the
        # prompt is in its transcript (the CLI records it after its
        # UserPromptSubmit hook returns, which is slower while chaos restarts
        # the controller). What never reached the agent can't be "lost by it".
        if not sc.check("crash: a long turn started on %s and the agent recorded the prompt" % host,
                        wait_until(lambda: c.turn_events(csid, "cli.turn.start") and c.prompts_with(cwd, text), 30),
                        text):
            return
        with c.fleet.control():
            sc.check("crash: %s SIGKILLed mid-turn" % host, c.fleet.kill(host), "")
            how = c.stream_end(sock, 30)
            sc.check("crash: the open grid ends (error or close), never hangs", how is not None, how or "still open after 30 s")
            status = wait_until(lambda: (lambda st: st if host in (st or {}).get("lost", []) else None)(
                fleetlib.http_json(c.fleet.port, "/cli/hosts/status")), 30) or {}
            sc.check("crash: the controller reports %s lost" % host, host in status.get("lost", []),
                     json.dumps(status)[:200])
            st, resp = c.spawn_args(name, cwd, ["--resume", csid])
            sc.check("crash: spawning the agent while its host is lost is refused explicitly, not placed elsewhere (I1)",
                     st == 502 and "lost" in json.dumps(resp), "HTTP %s %s" % (st, json.dumps(resp)[:200]))
            c.fleet.start(host)
        status = wait_until(lambda: (lambda st: st if st and st.get("lost") == [] else None)(
            fleetlib.http_json(c.fleet.port, "/cli/hosts/status")), 60) or {}
        sc.check("crash: the host is back and nothing is lost", status.get("lost") == [], json.dumps(status)[:200])
        st, resp = c.spawn_args(name, cwd, ["--resume", csid])
        sc.check("crash: the agent spawns again, fresh (the dead placement was dropped)",
                 st == 200 and resp.get("reused") is False, "HTTP %s %s" % (st, json.dumps(resp)[:200]))
        seen = c.prompts_with(cwd, text)
        sc.check("crash: the interrupted prompt is in the transcript (not lost silently)", bool(seen),
                 "%s" % [(h, s_) for h, s_, _ in seen])
        if st == 200:
            try:
                sock2, grid2 = c.attach(resp["sessionId"])
                sc.check("crash: it resumed the same conversation",
                         c.grid_wait(sock2, grid2, "session %s · resumed" % csid, 30), csid)
                sock2.close()
            except (OSError, wslib.WsError) as e:
                sc.check("crash: it resumed the same conversation", False, str(e))
    finally:
        sock.close()
        c.close(name)


def s8_drain(c, sc, attempt=1):
    cwd = c.fleet.workspace("s8-%s%s" % (c.tag, "" if attempt == 1 else "-%d" % attempt))
    pid, why = c.register_enabled(cwd)
    if not sc.check("drain: workspace registered", pid, why):
        return
    a = open_chat(c, sc, cwd, "drain: the app opens the agent (ensure-pinned-chat)")
    if not a:
        return
    sid, csid, _, sock, grid = a
    try:
        n1 = "s8 first %s" % uuid.uuid4().hex[:6]
        c.type(sock, n1)
        if not sc.check("drain: it answers", c.grid_wait(sock, grid, "%s1: %s" % (REPLY_MARKER, n1)), n1):
            return
        ex = c.exec_of(csid)
        host = (ex or {}).get("host")
        sc.info.update(host=host, session=sid, claude_session=csid, cli_pid=(ex or {}).get("pid"))
        n2 = "s8 long %s [[turn-ms=8000]]" % uuid.uuid4().hex[:6]
        starts = len(c.turn_events(csid, "cli.turn.start"))
        c.type(sock, n2)
        started = wait_until(lambda: len(c.turn_events(csid, "cli.turn.start")) > starts, 30)
        if not sc.check("drain: a long turn started on %s" % host, started, n2):
            return
        sock.close()
        sock = None
        with c.fleet.control():
            t_term = now_iso()
            exited = c.fleet.kill(host, signal.SIGTERM, 120)
            t_exit = now_iso()
            sc.info.update(sigterm=t_term, exited=t_exit)
            sc.check("drain: %s exited by itself after SIGTERM (drain)" % host, exited, "SIGTERM %s, gone %s" % (t_term, t_exit))
            if not exited:
                c.fleet.kill(host)
            ends = [e for e in c.turn_events(csid, "cli.turn.end") if (e.get("ts") or "") >= t_term]
            stops = [e for e in c.turn_events(csid, "cli.hook.done")
                     if (e.get("attrs") or {}).get("hook") == "Stop" and (e.get("ts") or "") >= t_term]
            sc.check("drain: the turn in progress finished before the host exited (turn end and Stop hook)",
                     bool(ends) and bool(stops) and ends[0]["ts"] <= t_exit and stops[0]["ts"] <= t_exit,
                     "turn.end %s, Stop %s, host gone %s" % ([e["ts"] for e in ends][:1], [e["ts"] for e in stops][:1], t_exit))
            # Unplacing stops the agent: its CLI must not outlive the drain,
            # even when it runs under k2-ptyd (LAB_HOST_PTYD=1), which is
            # designed to outlive its daemon.
            cli_pid = (ex or {}).get("pid")
            gone = wait_until(lambda: not c.fleet.cli_alive(host, cli_pid), 10)
            ptyd_left = fleetlib.ptyd_pids(c.fleet.st) if c.fleet.st.get("host_ptyd") else "n/a (in-process)"
            sc.info["ptyd_after_drain"] = ptyd_left
            sc.check("drain: unplacing stopped the agent's CLI on %s (gone once the host exited)" % host, gone,
                     "pid %s on %s gone=%s; k2-ptyd processes of this run: %s" % (cli_pid, host, bool(gone), ptyd_left))
            n3 = "s8-while-down-%s" % uuid.uuid4().hex[:6]
            st, raw, resp = c.msg(cwd, n3, wake=False)
            msg_id = resp.get("msgId")
            sc.check("drain: a message sent while the host is down is accepted (or fails with a reason), not lost silently",
                     st == 200 and (resp.get("success") is True or resp.get("reason")), "HTTP %s %s" % (st, raw[:300]))
            # LAB_S8_RESUME_ELSEWHERE=1 (default under LAB_HOST_PTYD=1): a
            # need comes first, while the host is still down, and places the
            # agent the usual way, on the other host.
            # LAB_S8_NO_RETURN=1 (outbox's variant): the drained host is not
            # restarted and no app need comes; the waiting message alone
            # must resume the agent elsewhere (drain-move: a message the
            # outbox holds for a drained agent is a need).
            elsewhere = os.environ.get("LAB_S8_RESUME_ELSEWHERE", os.environ.get("LAB_HOST_PTYD", "0")) == "1"
            mode = "message" if os.environ.get("LAB_S8_NO_RETURN") == "1" else ("need" if elsewhere else "back")
            sc.info["s8_mode"] = mode
            if mode == "back":
                t_restart = now_iso()
                c.fleet.start(host)
        if mode == "message":
            t_restart = t_exit
            resumed = wait_until(lambda: [e for e in c.fake("cli.exec", csid)
                                          if e.get("host") not in (None, host) and (e.get("attrs") or {}).get("resumed")
                                          and (e.get("ts") or "") >= t_exit], gone_wait_s()) or []
            sc.info["resumed_on"] = resumed[0].get("host") if resumed else None
            sc.check("drain: with %s kept down and no app need, the waiting message alone resumed the agent on the other host"
                     " (same session, --resume)" % host,
                     bool(resumed) and "--resume" in ((resumed[0].get("attrs") or {}).get("argv") or []),
                     "resumed cli.exec of %s after %s: %s" % (csid, t_exit, json.dumps(resumed[:1])[:250]))
            wait_until(lambda: c.replied(cwd, n3), gone_wait_s())
            with c.fleet.control():
                c.fleet.start(host)  # only now, for the scenarios after this one
            s8_one_live(c, sc, csid, host)
        elif mode == "need":
            t_need = now_iso()
            b = open_chat(c, sc, cwd, "drain: a need (the app) opens the agent while %s is down" % host)
            if b:
                b[3].close()
                ex2 = c.exec_of(csid)
                sc.info["resumed_on"] = (ex2 or {}).get("host")
                sc.check("drain: the need resumed the agent on the other host while %s was down (same session)" % host,
                         b[1] == csid and bool(ex2) and ex2.get("host") not in (None, host)
                         and bool((ex2.get("attrs") or {}).get("resumed")) and (ex2.get("ts") or "") >= t_need,
                         "session %s -> %s; last cli.exec %s" % (csid, b[1], json.dumps(ex2)[:250]))
            with c.fleet.control():
                t_restart = now_iso()
                c.fleet.start(host)  # waits for its link
            s8_one_live(c, sc, csid, host)
        else:
            resumed = wait_until(lambda: [e for e in c.fake("cli.exec", csid)
                                          if (e.get("attrs") or {}).get("resumed") and (e.get("ts") or "") >= t_restart], gone_wait_s())
            sc.check("drain: the replacement host resumed the agent that was awake (same session, no re-spawn by the lab)",
                     bool(resumed), "resumed cli.exec of %s after %s: %d" % (csid, t_restart, len(resumed or [])))
        wait_until(lambda: c.replied(cwd, n3), gone_wait_s())
        seen3 = c.prompts_with(cwd, n3)
        state = c.wait_final(msg_id, gone_wait_s()) if msg_id else None
        # PRD failure table: when --chaos killed the resumed CLI before the
        # message landed, the gone rule failing it loudly is the right end.
        killed = [e for x in c.fake("cli.exec", csid) if (x.get("ts") or "") >= t_restart
                  for e in chaos_hit(x.get("host"), x.get("pid"), t_restart) if e.get("kind") == "cli"]
        # Before it landed (not in the transcript), or mid-turn (in it once,
        # unanswered: the turn in progress is lost with the CLI).
        lost_to_chaos = bool(killed) and state == "failed" and len(seen3) <= 1 and not c.replied(cwd, n3)
        sc.info["s8_msg_chaos_kills"] = [(e["ts"], e.get("host"), e.get("pid")) for e in killed]
        sc.check("drain: the message sent while it was down was delivered exactly once"
                 " (or failed with a reason after --chaos killed the resumed CLI)",
                 (len(seen3) == 1 and c.replied(cwd, n3) and state in (None, "delivered")) or lost_to_chaos,
                 "in transcripts %d time(s); outbox state %s; chaos kills of the resumed CLI %s"
                 % (len(seen3), state, sc.info["s8_msg_chaos_kills"]))
    finally:
        if sock:
            sock.close()
        c.close(pid)


def s8_one_live(c, sc, csid, host):
    """The controller's resume loop runs when the drained host links back:
    it must not place the agent a second time (one live CLI over 15 s)."""
    most, live = 0, []
    end = time.monotonic() + 15
    while time.monotonic() < end:
        live = [e for e in c.fake("cli.exec", csid) if c.fleet.cli_alive(e.get("host"), e.get("pid"))]
        most = max(most, len(live))
        time.sleep(0.5)
    sc.check("drain: once %s is back, one live CLI for the agent (not placed twice)" % host, most == 1,
             "at most %d live over 15 s; now %s" % (most, [(e.get("host"), e.get("pid")) for e in live]))


# ── S9 ──────────────────────────────────────────────────────────────────

def s9(c, sc):
    """The controller restarts under a running agent. S9 is about that
    agent's CLI surviving, so when --chaos itself SIGKILLed the CLI
    (chaos.log) the attempt shows nothing: it is redone with a fresh agent
    (at most 5 attempts, each one counted in info)."""
    redo_on_chaos(c, sc, s9_attempt, "S9")


def redo_on_chaos(c, sc, attempt_fn, what, attempts=5):
    """Run attempt_fn(c, sc, attempt) up to `attempts` times. An attempt is redone
    when it failed a check and chaos.log shows --chaos itself killed the
    attempt's agent CLI or its host (sc.info host / cli_pid), or, when the
    attempt failed its first check (a spawn), the controller: then what
    failed says nothing about the property under test. The dropped checks
    stay in the log and in info["redone_after_chaos"]."""
    for attempt in range(1, attempts + 1):
        mark, since = len(sc.checks), now_iso()
        sc.info.pop("cli_pid", None)
        sc.info.pop("host", None)  # set again by the attempt (S8: the crash half set them)
        sc.info.pop("no_redo_after", None)
        if attempt_fn(c, sc, attempt) == "redo":
            continue
        failed = [x["name"] for x in sc.checks[mark:] if not x["ok"]]
        hits = chaos_hit(sc.info.get("host"), sc.info.get("cli_pid"), since) if failed else []
        # From no_redo_after on, chaos is part of what the attempt checks
        # (S10 under LAB_S10_CHAOS): its hits there excuse nothing.
        hits = [e for e in hits if (e.get("ts") or "") < sc.info.get("no_redo_after", "~")]
        if failed and len(sc.checks) == mark + 1 and "spawned" in failed[0]:
            # The spawn itself was lost: --chaos killed the controller while
            # it was in flight (a spawn is never sent twice), so S9 never got
            # an agent to test.
            hits = chaos_hit("controller", None, since, kinds=("controller",))
        if not hits or attempt == attempts:
            return
        # --chaos killed this agent's CLI or its host: what failed after it
        # says nothing about the controller restart. Kept in log and info.
        sc.info.setdefault("redone_after_chaos", []).append(
            {"attempt": attempt, "failed": failed, "chaos": [(e["ts"], e.get("kind"), e.get("pid")) for e in hits]})
        sc.log("--chaos killed %s's agent (%s) during attempt %d; its failed checks %s are dropped; redoing %s"
               % (what, [(e["ts"], e.get("kind")) for e in hits], attempt, failed, what))
        del sc.checks[mark:]


def s9_attempt(c, sc, attempt):
    name = "s9-%s%s" % (c.tag, "" if attempt == 1 else "-%d" % attempt)
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
        if not started and c.fleet.cli_alive(host, cli_pid) and c.stream_end(sock, 1) is not None:
            sock, grid = c.reattach(sc, sid, sock)
            if sock:
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
        if attempt < 3 and not c.fleet.cli_alive(host, cli_pid):
            killed = chaos_killed(host, cli_pid)
            if killed:
                sc.info.setdefault("redone_after_chaos_cli_kill", []).append(
                    {"attempt": attempt, "pid": cli_pid, "host": host, "chaos": killed.get("ts")})
                sc.log("--chaos SIGKILLed this agent's CLI (pid %s on %s, %s): attempt %d shows nothing; redoing S9"
                       % (cli_pid, host, killed.get("ts"), attempt))
                return "redo"
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
            answered = typed and c.grid_wait(sock, grid, "%s2: %s" % (REPLY_MARKER, n2), 30)
            if not answered and c.stream_end(sock, 1) is not None:
                # The stream through the controller closed under us (--chaos
                # restarted it): reattach once it is back, as the app does,
                # and type again only if the prompt never reached the agent.
                sc.info["reattached_after_stream_close"] = sc.info.get("reattached_after_stream_close", 0) + 1
                sc.log("the grid stream closed while typing; reattaching once the controller answers")
                sock.close()
                sock = None
                wait_until(lambda: fleetlib.http_json(c.fleet.port, "/cli/hosts/status"), DAEMON_BACK_S, 0.5)
                try:
                    sock, grid = c.attach(sid)
                    if not wait_until(lambda: c.prompts_with(cwd, n2), 5):
                        typed = c.type(sock, n2)
                    answered = typed and bool(wait_until(lambda: c.replied(cwd, n2), 30))
                except (OSError, wslib.WsError) as e:
                    c.log("reattach failed: %s" % e)
            sc.check("typing reaches the same agent and is answered", answered,
                     n2 if typed else "the grid stream was closed")
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


# ── S10 ─────────────────────────────────────────────────────────────────

def move_after_s():
    """The controller's K2_HOST_MOVE_AFTER in the lab (fleet.host_move_after),
    or None (the daemon's own 300 s)."""
    v = fleetlib.host_move_after()
    try:
        return int(v) if v else None
    except ValueError:
        return None


def s10(c, sc):
    """P3 T2 (outbox's turn-end snapshots): a host SIGKILLed mid-session (no
    drain, nothing saved then). Its portable agent moves once the host has
    been lost for K2_HOST_MOVE_AFTER and resumes on the other host with the
    home its last finished turn saved (MEMORY.md, the transcript, the same
    session) and the conversation; the turn in progress is lost, and its
    message, never acked, is delivered again there. When the dead host
    returns, no second CLI appears (I1).

    The fleet's hosts share one work dir (LAB_HOME_SYNC), so the workspace
    half of the home is outbox's turn-snapshots.sh (a work root per host);
    S10 checks what each host keeps apart: its HOME (memory, transcript).
    Opt-in (LAB_S10=1): before T2 the agent waits for its host (W3)."""
    redo_on_chaos(c, sc, s10_attempt, "S10")


def s10_attempt(c, sc, attempt):
    after = move_after_s() or 300
    name = "s10-%s%s" % (c.tag, "" if attempt == 1 else "-%d" % attempt)
    cwd = c.fleet.workspace(name)
    pid, why = c.register_enabled(cwd)
    if not sc.check("workspace %s registered" % name, pid, why):
        return
    sock = None
    try:
        a = open_chat(c, sc, cwd, "the app opens the agent (ensure-pinned-chat)")
        if not a:
            return
        facts = ["s10 fact one %s" % uuid.uuid4().hex[:6], "s10 fact two %s" % uuid.uuid4().hex[:6]]
        for i, fact in enumerate(facts, 1):
            text = "s10 turn %d [[remember: %s]]" % (i, fact)
            ok, a = ask_chat(c, sc, cwd, a, text)
            if not sc.check("turn %d answered" % i, ok, text):
                return
        sid, csid, _, sock, grid = a
        ex = c.exec_of(csid)
        h1, cli_pid = (ex or {}).get("host"), (ex or {}).get("pid")
        sc.info.update(host=h1, cli_pid=cli_pid, claude_session=csid, agent=pid)
        # Turn two's end: its sha (the fake agent traces it) and host-1's
        # turn-end save of it (P3 T2: trigger turn).
        wait_until(lambda: len(c.turn_events(csid, "cli.turn.end")) >= 2, 30)
        end2 = c.turn_events(csid, "cli.turn.end")[-1]
        mem2 = (end2.get("attrs") or {}).get("memory_sha256")
        saves = wait_until(lambda: [e for e in c.fleet.daemon_events(h1, "host.home_saved")
                                    if e.get("agent_id") == pid and (e.get("attrs") or {}).get("trigger") == "turn"
                                    and (e.get("ts") or "") >= end2["ts"]], 30) or []
        sc.check("turn two's end saved the agent's home on %s (host.home_saved, trigger turn)" % h1, bool(saves),
                 json.dumps([e.get("attrs") for e in saves][-1:])[:300])
        places = [e for e in c.fleet.daemon_events("controller", "ctl.place") if e.get("agent_id") == pid]
        epoch1 = (places[-1].get("attrs") or {}).get("epoch") if places else None

        # Turn three: a long one, by message (the outbox), so its message
        # is not acked when the host dies.
        # 30 s: long enough to kill it mid-turn; its redelivered copy runs
        # the full 30 s again on the new host.
        n3 = "s10 turn 3 %s [[turn-ms=30000]] [[remember: s10 lost turn]]" % uuid.uuid4().hex[:6]
        starts = len(c.turn_events(csid, "cli.turn.start"))
        st, raw, resp = c.msg(cwd, n3, wake=True)
        msg_id = resp.get("msgId")
        if not sc.check("turn three (a message) started on %s and the agent recorded its prompt" % h1,
                        st == 200 and wait_until(lambda: len(c.turn_events(csid, "cli.turn.start")) > starts
                                                 and c.prompts_with(cwd, n3.split(" [[")[0]), 30),
                        "HTTP %s %s" % (st, raw[:200])):
            return
        sock.close()
        sock = None
        # The hold. Without LAB_S10_CHAOS, S10 holds the fleet from the kill
        # until the agent goes on on the other host: what it checks is the
        # lab's own host loss, not one --chaos adds on top. With it (under
        # --chaos), S10 holds the fleet only to kill h1 and to start it
        # again, and guards h1 in between; chaos kills the controller, the
        # other host and CLIs meanwhile, and the checks say what must still
        # hold: one move, the home intact, no message lost or doubled (a
        # message may end failed, with a reason, only after chaos hit).
        free = s10_chaos()
        hold = contextlib.nullcontext() if free else c.fleet.control()
        with hold:
            with c.fleet.control() if free else contextlib.nullcontext():
                t_kill = now_iso()
                killed = c.fleet.kill(h1)
                # Its CLI goes with it (the host's process tree), as outbox's
                # turn-snapshots.sh does.
                with contextlib.suppress(fleetlib.FleetError, ProcessLookupError, TypeError):
                    if c.fleet.cli_alive(h1, cli_pid):
                        c.fleet.kill_cli(cli_pid, host=h1)
                if free:
                    c.fleet.set_guard([h1])
                    sc.info["no_redo_after"] = t_kill
            sc.check("%s SIGKILLed with its CLI mid-turn (no drain, no release save)" % h1,
                     killed and wait_until(lambda: not c.fleet.cli_alive(h1, cli_pid), 10)
                     and len(c.turn_events(csid, "cli.turn.end")) == starts,
                     "pid %s on %s" % (cli_pid, h1))
            # Not before K2_HOST_MOVE_AFTER: the agent stays placed on the lost host.
            time.sleep(max(0, after / 2))
            early = [e for e in c.fleet.daemon_events("controller", "ctl.move_decision")
                     if e.get("agent_id") == pid and (e.get("ts") or "") >= t_kill]
            sc.check("no move before %s has been lost for K2_HOST_MOVE_AFTER (%d s)" % (h1, after), not early,
                     json.dumps(early)[:200])
            # A controller restart counts the loss again from its start.
            moves = wait_until(lambda: [e for e in c.fleet.daemon_events("controller", "ctl.move_decision")
                                        if e.get("agent_id") == pid and (e.get("ts") or "") >= t_kill],
                               after + (120 if free else 30), 0.5) or []
            mv = moves[0] if moves else {}
            lost_ms = (mv.get("attrs") or {}).get("lost_ms") or 0
            sc.check("the controller moves the agent once %s has been lost for %d s (ctl.move_decision)" % (h1, after),
                     bool(moves) and (mv.get("attrs") or {}).get("from") == h1 and lost_ms >= after * 1000,
                     "killed %s; %s" % (t_kill, json.dumps(mv)[:300]))
            # It resumes on the other host (the resume loop), at a new epoch,
            # restoring the last turn's snapshot first.
            resumed = wait_until(lambda: [e for e in c.fake("cli.exec", csid)
                                          if e.get("host") not in (None, h1) and (e.get("ts") or "") >= t_kill
                                          and (e.get("attrs") or {}).get("resumed")], 120 if free else 60) or []
            rx = resumed[0] if resumed else {}
            h2 = rx.get("host")
            sc.info["moved_to"] = h2
            ra = rx.get("attrs") or {}
            new_places = [e for e in c.fleet.daemon_events("controller", "ctl.place")
                          if e.get("agent_id") == pid and (e.get("ts") or "") >= t_kill
                          and (e.get("attrs") or {}).get("host") == h2]
            epoch2 = (new_places[0].get("attrs") or {}).get("epoch") if new_places else None
            sc.check("it resumes on the other host, same session, --resume, at a new epoch (no lab spawn)",
                     bool(rx) and "--resume" in (ra.get("argv") or []) and csid in (ra.get("argv") or [])
                     and bool(new_places) and epoch1 is not None and epoch2 is not None and epoch2 > epoch1,
                     "exec %s; epoch %s -> %s" % (json.dumps(rx)[:250], epoch1, epoch2))
            restored = [e for e in c.fleet.daemon_events(h2, "host.home_restored")
                        if e.get("agent_id") == pid and (e.get("ts") or "") >= t_kill] if h2 else []
            ra2 = (restored[0].get("attrs") or {}) if restored else {}
            sc.check("%s restored the last turn's snapshot before resuming (host.home_restored, fetched > 0)" % h2,
                     bool(restored) and (ra2.get("fetched") or 0) > 0 and restored[0]["ts"] <= rx.get("ts", ""),
                     json.dumps(ra2)[:200])
            # The home: the memory the resumed CLI started with is turn two's.
            sc.check("I3: the moved CLI started with turn two's MEMORY.md (sha256): the lost turn left nothing",
                     ra.get("memory_sha256") == mem2 and mem2 not in (None, "absent"),
                     "turn two %s, at resume %s" % (mem2, ra.get("memory_sha256")))
            # The conversation: turns one and two are in its transcript there.
            mine = [(h, s_) for h, s_, _ in c.prompts_with(cwd, "s10 turn 1 ") + c.prompts_with(cwd, "s10 turn 2 ")
                    if h == h2]
            sc.check("the conversation moved: turns one and two are in the transcript on %s" % h2,
                     len(mine) == 2 and all(s_ == csid for _, s_ in mine), "%s" % mine)
            # The lost turn's message is delivered again there, once, and answered.
            key3 = n3.split(" [[")[0]
            wait_until(lambda: c.replied(cwd, key3) and [p for p in c.prompts_with(cwd, key3) if p[0] == h2],
                       gone_wait_s() + (90 if free else 30))
            on2 = [p for p in c.prompts_with(cwd, key3) if p[0] == h2]
            state = c.wait_final(msg_id, gone_wait_s()) if msg_id else None
            why3 = c.final_reason(msg_id) if msg_id else None
            ok3 = len(on2) == 1 and c.replied(cwd, key3) and state == "delivered"
            hit3 = s10_hits(h2, t_kill) if free and not ok3 else []
            sc.check("turn three's message, never acked, is delivered again on %s exactly once and answered%s"
                     % (h2, " (or failed with a reason after --chaos hit)" if free else ""),
                     ok3 or (bool(hit3) and state == "failed" and bool(why3) and len(on2) <= 1),
                     "on %s %d time(s); outbox state %s %s; chaos %s"
                     % (h2, len(on2), state, why3 or "", [(e["ts"], e.get("kind"), e.get("target")) for e in hit3]))
            # K2-f: 'interrupted' means its turn never answered. A loud
            # failure for a turn that did answer is a false one.
            if state == "failed" and (why3 or "").startswith("interrupted"):
                sc.check("turn three's 'interrupted' failure is real: its turn never answered",
                         not c.replied(cwd, key3), why3)
            elsewhere = [p[0] for p in c.prompts_with(cwd, key3) if p[0] not in (h1, h2)]
            sc.check("turn three's message reached no third place (never doubled)",
                     not elsewhere and len([p for p in c.prompts_with(cwd, key3) if p[0] == h1]) == 1,
                     "%s" % [p[0] for p in c.prompts_with(cwd, key3)])
            mem_now = c.memory_file(h2, cwd) if h2 else ""
            mem_bytes = open(mem_now, "rb").read() if os.path.isfile(mem_now) else b""
            sc.check("MEMORY.md on %s holds turns one and two" % h2, all(f.encode() in mem_bytes for f in facts), mem_now)
            # The dead host returns: the agent stays where it moved (I1).
            with c.fleet.control() if free else contextlib.nullcontext():
                t_back = now_iso()
                c.fleet.start(h1)  # waits for its link
                if free:
                    c.fleet.clear_guard()
            most, live = 0, []
            end = time.monotonic() + 15
            while time.monotonic() < end:
                live = [e for e in c.fake("cli.exec", csid) if c.fleet.cli_alive(e.get("host"), e.get("pid"))]
                most = max(most, len(live))
                time.sleep(0.5)
            # Under chaos its CLI may be killed meanwhile (none live): never two.
            sc.check("I1: once %s is back, one live CLI for the agent, on %s (not adopted back, not placed twice)" % (h1, h2),
                     (most <= 1 if free else most == 1) and all(e.get("host") == h2 for e in live)
                     and c.placed_host(pid, t_back) in (None, h2),
                     "at most %d live over 15 s; now %s" % (most, [(e.get("host"), e.get("pid")) for e in live]))
            # And it goes on there.
            n4 = "s10 after %s" % uuid.uuid4().hex[:6]
            st, raw, resp = c.msg(cwd, n4, wake=False)  # it is live there: queued, with a msg id (I2)
            wait_until(lambda: c.replied(cwd, n4), DEADLINE_S)
            at4 = [p[0] for p in c.prompts_with(cwd, n4)]
            ok4 = c.replied(cwd, n4) and at4 == [h2]
            if free and not ok4:
                # chaos killed its CLI (or h2) first: answered once, or a
                # loud failure (the outbox row's, or the controller's own
                # answer when nothing placed it then: success false and a
                # reason). Never delivered-and-silent.
                hit4 = s10_hits(h2, t_back)
                mid4 = resp.get("msgId")
                state4 = c.wait_final(mid4, gone_wait_s()) if mid4 else (
                    "failed" if resp.get("success") is False else None)
                why4 = c.final_reason(mid4) if mid4 else resp.get("reason")
                ok4 = bool(hit4) and ((c.replied(cwd, n4) and len(at4) == 1)
                                      or (state4 == "failed" and bool(why4) and len(at4) <= 1))
                sc.info["n4_after_chaos"] = {"prompts": at4, "state": state4, "reason": why4,
                                             "chaos": [(e["ts"], e.get("kind"), e.get("target")) for e in hit4]}
            if free and not ok4 and state4 == "failed" and (why4 or "").startswith("interrupted"):
                sc.check("n4's 'interrupted' failure is real: its turn never answered", not c.replied(cwd, n4), why4)
            sc.check("a message after the move lands on %s and is answered" % h2, ok4,
                     "HTTP %s; prompts %s" % (st, at4))
            # One move, whatever chaos did meanwhile.
            all_moves = [e for e in c.fleet.daemon_events("controller", "ctl.move_decision")
                         if e.get("agent_id") == pid and (e.get("ts") or "") >= t_kill]
            sc.check("the agent moved once (one ctl.move_decision since the kill)", len(all_moves) == 1,
                     json.dumps([(e.get("ts"), (e.get("attrs") or {}).get("from")) for e in all_moves])[:300])
            if free:
                # The hold: from the kill until h1 is back (the guard).
                sc.info["hold"] = [t_kill, t_back]
                sc.info["chaos_in_hold"] = [(e["ts"], e.get("kind"), e.get("target") or e.get("host"))
                                            for e in s10_hits(None, t_kill) if e["ts"] <= t_back]
    finally:
        if s10_chaos():
            c.fleet.clear_guard()
        if sock:
            sock.close()
        c.close(pid)


def s10_chaos():
    """LAB_S10_CHAOS=1 under --chaos: chaos acts during S10's hold."""
    return os.environ.get("LAB_S10_CHAOS") == "1" and bool(os.environ.get("LAB_CHAOS"))


def s10_hits(host, since):
    """chaos.log SIGKILLs since `since`: of `host`, of any agent CLI on it,
    or of the controller (host None: every SIGKILL)."""
    path = os.environ.get("LAB_CHAOS_LOG")
    if not (path and os.path.isfile(path)):
        return []
    return [e for e in fleetlib.read_jsonl(path)
            if (e.get("ts") or "") >= since and e.get("action") == "SIGKILL"
            and (host is None or e.get("kind") == "controller" or e.get("host") == host or e.get("target") == host)]

SCENARIOS = {"S2": s2, "S3": s3, "S6": s6, "S7": s7, "S8": s8, "S9": s9, "S10": s10}


def run(state_path, out, only, min_seconds=0):
    """Run `only` once into `out`; with min_seconds (a --chaos window), run
    the set in rounds (out/round-<k>/) until that long has passed, and write
    each scenario's verdict over all rounds to out/<S>.json: pass only if
    every round passed. The message ledger is always out/messages.jsonl."""
    # LAB_G2_ROUNDS=N: exactly N rounds (a repro loop, e.g. S8 alone with
    # LAB_G2_ONLY=S8), with or without chaos.
    want = int(os.environ.get("LAB_G2_ROUNDS") or 0)
    if not min_seconds and not want:
        return run_once(state_path, out, only, out)
    start, rounds, k = time.monotonic(), [], 0
    while k == 0 or (k < want if want else time.monotonic() - start < min_seconds):
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
        if sid == "S10" and os.environ.get("LAB_S10") != "1":
            results[sid] = sc.finish(reason="opt-in: LAB_S10=1 (needs P3 T2 turn-end snapshots and home sync)", status="n/a")
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
