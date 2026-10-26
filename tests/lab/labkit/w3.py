"""W3 survive (dist-link): a client of a K2_ROLE=controller across a
controller restart (S9) and a host crash (S8), in phases the topology
script runs between its kills and restarts:

  start   spawn an agent through the controller, attach its grid, wait for
          the prompt, type a prompt (the fake agent's turn is then running)
          and write the state file. The caller then kills something.
  resume  after a controller restart: re-attach the same session through
          the controller and wait for the turn's reply. Checks the reply
          arrived, and that banner, prompt and reply are on the client's
          screen in order (no gap: the host's snapshot carries everything
          the agent printed while the controller was down).
          With --bytes (the host runs on k2-ptyd, W3b) the client also
          reads the session's raw bytes (/cli/sessions/bytes) up to the
          banner before the restart, keeps its offset, and after the
          restart reconnects from it: the ack shows no gap, the missed
          bytes hold the prompt echo and the reply, and what it got is
          byte for byte a fresh read from offset 0.
  close   close the agent (after the caller compared its pid).
  lost    after a host crash: the open grid ends with an error or a close
          (never a hang), the controller reports the host lost, and a spawn
          of the agent is refused with an explicit error (not placed
          elsewhere, I1).
  return  after the host is back: the controller dropped the dead
          placement (gone); the agent can be spawned again, resuming its
          conversation; the interrupted prompt is in the transcript.

Usage: w3.py PHASE --port P --token-file F --home H --cwd W --state S --out DIR
Prints PASS/FAIL lines; exits 0 only if every check of the phase passed.
Deadlines only stop a hung run; nothing asserts a duration.
"""

import argparse
import json
import os
import sys
import time
import urllib.parse
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from labkit import ws as wslib  # noqa: E402
from labkit.s1 import Grid, claude_project_slug  # noqa: E402

DEADLINE_S = 60


class Phase:
    def __init__(self, a):
        self.a = a
        self.token = open(a.token_file).read().strip()
        self.checks = []

    def check(self, name, ok, detail=""):
        self.checks.append({"name": name, "ok": bool(ok), "detail": detail})
        print("%s %s -- %s" % ("PASS" if ok else "FAIL", name, detail), flush=True)
        return ok

    def http(self, method, path, body=None):
        import urllib.request
        import urllib.error
        url = "http://127.0.0.1:%d%s?%s" % (self.a.port, path, urllib.parse.urlencode({"token": self.token}))
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=DEADLINE_S) as r:
                return r.status, r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode("utf-8", "replace")

    def attach(self, session):
        return wslib.WebSocket("127.0.0.1", self.a.port, "/cli/sessions/grid?" + urllib.parse.urlencode(
            {"session": session, "token": self.token}), timeout=DEADLINE_S)

    def open_bytes(self, session, offset):
        """A /cli/sessions/bytes client from `offset`: (socket, ack payload)
        or (None, the error message)."""
        try:
            sock = wslib.WebSocket("127.0.0.1", self.a.port, "/cli/sessions/bytes?" + urllib.parse.urlencode(
                {"session": session, "from": offset, "token": self.token}), timeout=DEADLINE_S)
        except (OSError, wslib.WsError) as e:
            return None, "upgrade: %s" % e
        kind, data = sock.recv()
        msg = json.loads(data) if kind == "text" else {}
        if msg.get("event") != "session:ack":
            sock.close()
            return None, "%s %s" % (kind, str(data)[:200])
        return sock, msg["payload"]

    def read_bytes(self, sock, until=None, count=None):
        """Binary chunks until `until` is in them or `count` bytes came.
        Returns (bytes, how it ended)."""
        got = b""
        deadline = time.monotonic() + DEADLINE_S
        while not ((until is not None and until in got) or (count is not None and len(got) >= count)):
            left = deadline - time.monotonic()
            if left <= 0:
                return got, "deadline"
            sock.settimeout(left)
            try:
                kind, data = sock.recv()
            except (OSError, wslib.WsError) as e:
                return got, "stream error: %s" % e
            if kind == "close":
                return got, "closed"
            if kind == "text":
                return got, "event: %s" % data[:200]
            got += data
        return got, "found"

    def wait_grid(self, sock, grid, needle):
        """Apply grid events until `needle` is on the client's screen. Returns
        (found, how it ended)."""
        deadline = time.monotonic() + DEADLINE_S
        while not grid.has(needle):
            left = deadline - time.monotonic()
            if left <= 0:
                return False, "deadline"
            sock.settimeout(left)
            try:
                kind, data = sock.recv()
            except (OSError, wslib.WsError) as e:
                return False, "stream error: %s" % e
            if kind == "close":
                return False, "closed"
            if kind != "text":
                continue
            msg = json.loads(data)
            if not grid.apply(msg) and msg.get("event") in ("error", "child_exit"):
                return False, "%s: %s" % (msg.get("event"), json.dumps(msg.get("payload"))[:200])
        return True, "found"

    def save(self, state):
        with open(self.a.state, "w") as fh:
            json.dump(state, fh)

    def load(self):
        with open(self.a.state) as fh:
            return json.load(fh)

    def transcript_prompts(self, cwd, claude_session):
        path = os.path.join(self.a.home, ".claude", "projects", claude_project_slug(cwd), claude_session + ".jsonl")
        if not os.path.isfile(path):
            return []
        with open(path, encoding="utf-8") as fh:
            return [v["message"]["content"] for v in map(json.loads, fh) if v.get("type") == "user"]

    # ── phases ────────────────────────────────────────────────────────────

    def start(self):
        agent = "w3-" + self.a.tag + "-" + uuid.uuid4().hex[:6]
        claude_session = str(uuid.uuid4())
        st, body = self.http("POST", "/cli/sessions/v2/spawn", {
            "agent_name": agent, "cwd": self.a.cwd, "command": "claude",
            "args": ["--session-id", claude_session], "cols": 120, "rows": 32})
        resp = json.loads(body) if st == 200 else {}
        if not self.check("spawn through the controller", st == 200, "HTTP %s %s" % (st, body[:200])):
            return
        session = resp["sessionId"]
        sock = self.attach(session)
        grid = Grid()
        banner = "k2-fake-agent (as claude) · session %s" % claude_session
        # The banner line ends " · new" (a fresh conversation), then the prompt.
        ok, how = self.wait_grid(sock, grid, banner + " · new\n>")
        if not self.check("the agent shows its prompt", ok, how):
            return
        state_bytes = {}
        if self.a.bytes:
            bsock, ack = self.open_bytes(session, 0)
            ok = bsock is not None
            if self.check("bytes: the controller relays the session's byte stream from offset 0", ok, str(ack)[:200]):
                got, how = self.read_bytes(bsock, until=banner.encode())
                bsock.close()
                self.check("bytes: the banner is in the raw bytes", how == "found", "%s after %d bytes" % (how, len(got)))
                state_bytes = {"bytes_offset": len(got), "bytes_seen": got.hex()}
        text = "w3 %s %s" % (self.a.tag, uuid.uuid4().hex[:6])
        sock.send_text(json.dumps({"action": "input", "text": text}))
        time.sleep(0.15)
        sock.send_text(json.dumps({"action": "input", "text": "\r"}))
        # The turn has started once the agent recorded the prompt (it prints
        # nothing until the reply). Only then hand over to the caller's kill.
        deadline = time.monotonic() + DEADLINE_S
        while text not in self.transcript_prompts(self.a.cwd, claude_session) and time.monotonic() < deadline:
            time.sleep(0.05)
        self.save(dict({"agent": agent, "session": session, "claude_session": claude_session, "text": text, "banner": banner}, **state_bytes))
        self.check("prompt typed; the turn is running (the prompt is in the transcript)",
                   text in self.transcript_prompts(self.a.cwd, claude_session), text)
        if self.a.keep_open:
            # Keep the grid open until the caller's kill ends it (lost phase
            # reads how it ended from this process's output).
            ok, how = self.wait_grid(sock, grid, "[k2-fake-agent] reply #1: " + text)
            print("GRID-END %s" % how, flush=True)

    def wait_linked(self):
        """Poll the controller's host status until a host is linked and none
        is lost. Returns the last status."""
        deadline = time.monotonic() + DEADLINE_S
        status = {}
        while time.monotonic() < deadline:
            st, body = self.http("GET", "/cli/hosts/status")
            status = json.loads(body) if st == 200 else {}
            if status.get("lost") == [] and status.get("hosts"):
                break
            time.sleep(0.2)
        return status

    def resume(self):
        s = self.load()
        status = self.wait_linked()
        self.check("the host relinked to the new controller and its placements were adopted",
                   status.get("lost") == [] and any(h.get("placements", 0) > 0 for h in status.get("hosts", [])),
                   json.dumps(status)[:200])
        sock = self.attach(s["session"])
        grid = Grid()
        reply = "[k2-fake-agent] reply #1: " + s["text"]
        ok, how = self.wait_grid(sock, grid, reply)
        self.check("after the controller restart the reply reaches the client", ok, how)
        screen = grid.text()
        idx = [screen.find(s["banner"]), screen.find(s["text"]), screen.find(reply)]
        self.check("banner, prompt and reply in order on the client's screen (no gap)",
                   all(i >= 0 for i in idx) and idx == sorted(idx), "positions %s" % idx)
        self.check("the prompt was recorded once in the agent's transcript",
                   self.transcript_prompts(self.a.cwd, s["claude_session"]) == [s["text"]],
                   repr(self.transcript_prompts(self.a.cwd, s["claude_session"])))
        sock.close()
        if self.a.bytes and "bytes_offset" in s:
            self.resume_bytes(s, reply)

    def resume_bytes(self, s, reply):
        off, seen = s["bytes_offset"], bytes.fromhex(s["bytes_seen"])
        bsock, ack = self.open_bytes(s["session"], off)
        if not self.check("bytes: after the controller restart the client reconnects from its offset", bsock is not None, str(ack)[:200]):
            return
        self.check("bytes: no gap (the ring still holds every byte after the offset)",
                   ack.get("fromOffset") == off and ack.get("currentFrontOffset") == off,
                   json.dumps(ack))
        missed, how = self.read_bytes(bsock, until=reply.encode())
        bsock.close()
        self.check("bytes: the missed bytes hold the prompt echo and the reply",
                   how == "found" and s["text"].encode() in missed, "%s, %d bytes" % (how, len(missed)))
        fresh_sock, _ = self.open_bytes(s["session"], 0)
        fresh, how = self.read_bytes(fresh_sock, count=len(seen) + len(missed)) if fresh_sock else (b"", "no stream")
        if fresh_sock:
            fresh_sock.close()
        self.check("bytes: exactly the session's bytes, once each, in order (before + missed == a fresh read from 0)",
                   seen + missed == fresh[:len(seen) + len(missed)],
                   "%d + %d bytes vs %d fresh (%s)" % (len(seen), len(missed), len(fresh), how))

    def close(self):
        s = self.load()
        st, body = self.http("POST", "/cli/sessions/v2/close", {"agent_name": s["agent"], "force": True, "clear_index": True})
        self.check("close after resume", st == 200 and '"closed":true' in body, "HTTP %s %s" % (st, body[:120]))

    def lost(self):
        s = self.load()
        st, body = self.http("GET", "/cli/hosts/status")
        status = json.loads(body) if st == 200 else {}
        self.check("the controller reports the host lost", status.get("lost") == [self.a.host_name], body[:200])
        st, body = self.http("POST", "/cli/sessions/v2/spawn", {
            "agent_name": s["agent"], "cwd": self.a.cwd, "command": "claude",
            "args": ["--resume", s["claude_session"]], "cols": 120, "rows": 32})
        self.check("spawning the agent while its host is lost is refused explicitly (I1)",
                   st == 502 and "lost" in body, "HTTP %s %s" % (st, body[:200]))
        sock = self.attach(s["session"])
        kind, data = sock.recv()
        self.check("attaching its grid while lost answers an error, not a hang",
                   kind == "text" and "is lost" in data, "%s %s" % (kind, (data or "")[:160]))

    def returned(self):
        s = self.load()
        status = self.wait_linked()
        self.check("the host linked again and nothing is lost", status.get("lost") == [], json.dumps(status)[:200])
        st, body = self.http("POST", "/cli/sessions/v2/spawn", {
            "agent_name": s["agent"], "cwd": self.a.cwd, "command": "claude",
            "args": ["--resume", s["claude_session"]], "cols": 120, "rows": 32})
        resp = json.loads(body) if st == 200 else {}
        self.check("the dead placement was dropped: the agent spawns again, fresh on the host",
                   st == 200 and resp.get("reused") is False, "HTTP %s %s" % (st, body[:200]))
        self.check("the interrupted prompt is in the transcript (not lost silently)",
                   s["text"] in self.transcript_prompts(self.a.cwd, s["claude_session"]),
                   repr(self.transcript_prompts(self.a.cwd, s["claude_session"])))
        if st == 200:
            sock = self.attach(resp["sessionId"])
            grid = Grid()
            ok, how = self.wait_grid(sock, grid, "session %s · resumed" % s["claude_session"])
            self.check("it resumed the same conversation", ok, how)
            sock.close()
            self.http("POST", "/cli/sessions/v2/close", {"agent_name": s["agent"], "force": True, "clear_index": True})


def main(argv):
    p = argparse.ArgumentParser(prog="w3.py")
    p.add_argument("phase", choices=["start", "resume", "close", "lost", "return"])
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--token-file", required=True)
    p.add_argument("--home", required=True)
    p.add_argument("--cwd", required=True)
    p.add_argument("--state", required=True)
    p.add_argument("--tag", default="x")
    p.add_argument("--host-name", default="host-1")
    p.add_argument("--keep-open", action="store_true")
    p.add_argument("--bytes", action="store_true", help="also check the raw byte stream (host on k2-ptyd)")
    a = p.parse_args(argv)
    ph = Phase(a)
    {"start": ph.start, "resume": ph.resume, "close": ph.close, "lost": ph.lost, "return": ph.returned}[a.phase]()
    return 0 if ph.checks and all(c["ok"] for c in ph.checks) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
