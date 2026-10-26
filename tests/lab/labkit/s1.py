"""Scenario S1: spawn, attach, type, see output (PRD section 3, gate G0).

Drives a running daemon the way the app does, through its real routes:

  1. POST /cli/sessions/v2/spawn  {agent_name, cwd, command: "claude",
     args: [--session-id, <id>]}
  2. WS   /cli/sessions/grid?session=<id>  (the app's terminal view: a
     snapshot, then deltas)
  3. wait for the agent's banner and prompt on the grid  (spawn latency)
  4. type on the same socket: {"action":"input","text":<text>}, then "\\r"
     150 ms later (paste, then Enter, the cadence K2 uses for agent CLIs)
  5. wait for "[k2-fake-agent] reply #1: <text>" on the grid  (turn latency)
  6. the agent's home has the transcript with that prompt
  7. POST /cli/sessions/v2/close  {agent_name, force, clear_index}

Writes into <out>: S1.log, S1.json (result), S1.trace.jsonl (the lab's own
events merged with the fake agent's), S1.cast (asciicast v2 of the screen
as the client saw it, one frame per grid update). Deadlines below only stop
a hung run; no check asserts a duration.
"""

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from . import ws as wslib

REPLY_MARKER = "[k2-fake-agent] reply #"
DEADLINE_S = 60  # per wait; only stops a hung run
COLS, ROWS = 120, 32


def _now_iso():
    t = time.time()
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + ".%03dZ" % int((t % 1) * 1000)


def claude_project_slug(path):
    """k2_core::chat_history::claude_project_hash."""
    canonical = os.path.realpath(path)
    return "".join(c if (c.isascii() and c.isalnum()) or c == "-" else "-" for c in canonical)


def _row_text(runs):
    return "".join(r.get("text", "") for r in runs).rstrip()


class Grid:
    """The client's view of a `/cli/sessions/grid` stream: scrollback plus
    the visible rows, rebuilt from `snapshot` and `delta` events."""

    def __init__(self):
        self.scrollback, self.rows, self.version = [], [], -1

    def apply(self, msg):
        ev, p = msg.get("event"), msg.get("payload") or {}
        if ev == "snapshot":
            self.scrollback = [_row_text(r) for r in p.get("scrollback", [])]
            self.rows = [_row_text(r) for r in p.get("grid", [])]
        elif ev == "delta":
            self.scrollback += [_row_text(r) for r in p.get("scrollbackAppended", [])]
            n = p.get("rows", len(self.rows))
            self.rows = (self.rows + [""] * n)[:n]
            for d in p.get("damagedRows", []):
                if 0 <= d["row"] < n:
                    self.rows[d["row"]] = _row_text(d["runs"])
        else:
            return False
        self.version = p.get("version", self.version)
        return True

    def text(self):
        return "\n".join(self.scrollback + self.rows)

    def has(self, needle):
        """`needle` is on the grid, also when a soft wrap splits it over two
        rows (a line that starts mid-row, after typed-ahead input, wraps at
        the terminal's width)."""
        t = self.text()
        return needle in t or needle in t.replace("\n", "")

    def screen(self):
        return "\n".join(self.rows).rstrip("\n")


class Run:
    def __init__(self, port, token, home, cwd, out, fake_trace):
        self.port, self.token, self.home, self.cwd, self.out = port, token, home, cwd, out
        self.fake_trace = fake_trace
        self.log_lines, self.events, self.checks, self.metrics = [], [], [], {}
        self.grid = Grid()
        self.cast = []
        self.t0 = time.monotonic()
        self.agent_name = "lab-s1-" + uuid.uuid4().hex[:8]
        self.claude_session = str(uuid.uuid4())
        self.session = None

    def log(self, msg):
        self.log_lines.append("%s %s" % (_now_iso(), msg))

    def event(self, name, **attrs):
        self.events.append({"ts": _now_iso(), "event": name, "src": "lab", "agent_id": self.agent_name,
                            "session_id": self.session, "attrs": attrs})

    def check(self, name, ok, detail=""):
        self.checks.append({"name": name, "ok": bool(ok), "detail": detail})
        self.log("%s %s %s" % ("PASS" if ok else "FAIL", name, detail))
        return ok

    def http(self, method, path, query=None, body=None):
        q = dict(query or {})
        q["token"] = self.token
        url = "http://127.0.0.1:%d%s?%s" % (self.port, path, urllib.parse.urlencode(q))
        data = json.dumps(body).encode() if body is not None else (b"" if method == "POST" else None)
        req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=DEADLINE_S) as r:
                return r.status, r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode("utf-8", "replace")

    def wait_for(self, sock, needle, what):
        """Apply grid events until `needle` is on the client's screen or in
        its scrollback. Returns seconds waited, or None on close, error or
        deadline."""
        start = time.monotonic()
        deadline = start + DEADLINE_S
        while not self.grid.has(needle):
            left = deadline - time.monotonic()
            if left <= 0:
                self.log("deadline waiting for %s" % what)
                return None
            sock.settimeout(left)
            try:
                kind, data = sock.recv()
            except (OSError, wslib.WsError) as e:
                self.log("stream error waiting for %s: %s" % (what, e))
                return None
            if kind == "close":
                self.log("stream closed waiting for %s" % what)
                return None
            if kind != "text":
                self.log("unexpected binary frame (%d bytes)" % len(data))
                continue
            msg = json.loads(data)
            if self.grid.apply(msg):
                frame = "\x1b[H\x1b[2J" + self.grid.screen().replace("\n", "\r\n")
                self.cast.append([round(time.monotonic() - self.t0, 4), "o", frame])
            else:
                self.log("ws event: %s" % data[:300])
                if msg.get("event") in ("error", "child_exit"):
                    return None
        return time.monotonic() - start

    def wait_trace(self, event):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.fake_trace and os.path.isfile(self.fake_trace):
                with open(self.fake_trace, encoding="utf-8") as fh:
                    if any('"%s"' % event in l and self.claude_session in l for l in fh):
                        return True
            time.sleep(0.05)
        self.log("fake agent trace never showed %s" % event)
        return False

    def run(self):
        os.makedirs(self.out, exist_ok=True)
        # 1. spawn
        t_spawn = time.monotonic()
        self.event("lab.spawn_request", command="claude")
        status, body = self.http("POST", "/cli/sessions/v2/spawn", body={
            "agent_name": self.agent_name, "cwd": self.cwd, "command": "claude",
            "args": ["--session-id", self.claude_session], "cols": COLS, "rows": ROWS})
        self.log("spawn -> %s %s" % (status, body[:500]))
        try:
            resp = json.loads(body)
        except ValueError:
            resp = {}
        self.session = resp.get("sessionId")
        if not self.check("spawn returns a session", status == 200 and self.session, "HTTP %s" % status):
            return self.finish()
        self.event("lab.spawned", reused=resp.get("reused"))
        self.check("spawn is fresh, not reused", resp.get("reused") is False, "reused=%r" % resp.get("reused"))

        # 2. attach
        try:
            sock = wslib.WebSocket("127.0.0.1", self.port, "/cli/sessions/grid?" + urllib.parse.urlencode(
                {"session": self.session, "token": self.token}), timeout=DEADLINE_S)
        except (OSError, wslib.WsError) as e:
            self.check("attach to /cli/sessions/grid", False, str(e))
            return self.finish()
        self.check("attach to /cli/sessions/grid", True)
        self.event("lab.attached")

        # 3. the agent is up: its banner, then its prompt
        banner = "k2-fake-agent (as claude) · session %s · new" % self.claude_session
        waited = self.wait_for(sock, banner, "the agent banner")
        if not self.check("the fake agent runs as claude (banner on the grid)", waited is not None, banner):
            sock.close()
            return self.finish()
        self.metrics["spawn_to_banner_ms"] = round((time.monotonic() - t_spawn) * 1000, 1)
        self.event("lab.banner_seen")
        prompt_seen = self.wait_for(sock, banner + "\n>", "the prompt")
        self.check("the agent shows its prompt", prompt_seen is not None)

        # 4. type, as the app does: paste, then Enter
        text = "hello from the lab " + uuid.uuid4().hex[:6]
        t_type = time.monotonic()
        sock.send_text(json.dumps({"action": "input", "text": text}))
        time.sleep(0.15)
        sock.send_text(json.dumps({"action": "input", "text": "\r"}))
        self.event("lab.typed", text=text)

        # 5. see output
        reply = "%s1: %s" % (REPLY_MARKER, text)
        waited = self.wait_for(sock, reply, "the reply")
        if self.check("the reply comes back on the grid", waited is not None, reply):
            self.metrics["type_to_reply_ms"] = round((time.monotonic() - t_type) * 1000, 1)
            self.event("lab.reply_seen")
        sock.close()
        # Let the turn finish (its Stop hook) before the agent is closed, so
        # the trace shows the whole turn. Informational; not a check.
        self.wait_trace("cli.turn.end")

        # 6. the agent's home
        transcript = os.path.join(self.home, ".claude", "projects", claude_project_slug(self.cwd),
                                  self.claude_session + ".jsonl")
        prompts = []
        if os.path.isfile(transcript):
            with open(transcript, encoding="utf-8") as fh:
                prompts = [v["message"]["content"] for v in map(json.loads, fh) if v.get("type") == "user"]
        self.check("transcript in the agent home has the prompt", prompts == [text],
                   "%s: %r" % (os.path.relpath(transcript, self.home), prompts))

        # 7. close
        status, body = self.http("POST", "/cli/sessions/v2/close",
                                 body={"agent_name": self.agent_name, "force": True, "clear_index": True})
        self.check("close is accepted", status == 200, "HTTP %s %s" % (status, body[:200]))
        self.event("lab.closed")
        return self.finish()

    def finish(self):
        fake = []
        if self.fake_trace and os.path.isfile(self.fake_trace):
            with open(self.fake_trace, encoding="utf-8") as fh:
                fake = [json.loads(l) for l in fh if l.strip()]
            fake = [e for e in fake if e.get("session_id") == self.claude_session]
        trace = sorted(self.events + fake, key=lambda e: e["ts"])
        result = {
            "scenario": "S1",
            "title": "Spawn, attach, type, see output",
            "status": "pass" if self.checks and all(c["ok"] for c in self.checks) else "fail",
            "agent_name": self.agent_name,
            "daemon_session": self.session,
            "claude_session": self.claude_session,
            "checks": self.checks,
            "metrics": self.metrics,
            "hooks_seen_by_agent": [e["attrs"] for e in fake if e["event"] == "cli.hook.done"],
            "trace": "S1.trace.jsonl",
            "trace_note": "lab and k2-fake-agent events only; the daemon emits no trace events until wave 2",
        }
        with open(os.path.join(self.out, "S1.json"), "w") as fh:
            json.dump(result, fh, indent=2)
        with open(os.path.join(self.out, "S1.log"), "w") as fh:
            fh.write("\n".join(self.log_lines) + "\n")
            fh.write("\n--- the client's screen at the end (scrollback + grid) ---\n")
            fh.write(self.grid.text() + "\n")
        with open(os.path.join(self.out, "S1.trace.jsonl"), "w") as fh:
            for e in trace:
                fh.write(json.dumps(e) + "\n")
        with open(os.path.join(self.out, "S1.cast"), "w") as fh:
            fh.write(json.dumps({"version": 2, "width": COLS, "height": ROWS, "timestamp": int(time.time()),
                                 "title": "k2 lab S1: the client's screen"}) + "\n")
            for ev in self.cast:
                fh.write(json.dumps(ev) + "\n")
        return result
