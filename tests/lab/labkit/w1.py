#!/usr/bin/env python3
"""W1: three app flows against a running k2-daemon, through its HTTP routes.

Written to be pointed later at a K2_ROLE=controller daemon whose agents run
on a separate K2_ROLE=host daemon; today it runs against one ordinary
daemon (the solo topology). Each flow gets its own fresh workspace (a git
repo under --workspace, registered with POST /cli/projects/add-from-path),
so the flows never share an agent.

  chat       the chat overlay: POST /cli/workspace/ensure-pinned-chat
             {project} -> a session; WS /cli/sessions/grid shows the fake
             agent's banner; a typed prompt is answered; a second ensure
             returns the same live session (reused).
  wake       S4, a message wakes a sleeping agent: the workspace's agent is
             enabled but has no process. POST /cli/workspace/msg (the
             `k2 msg` route) with wake=false is refused dormant_no_wake (it
             really is asleep); with wake on, the daemon spawns the agent
             and delivers. The agent then sleeps again (v2/close) and a
             second message resumes the same conversation.
  heartbeat  S5, a heartbeat fires on a sleeping agent: POST
             /cli/heartbeat/add (with WAKEUP.md instructions), then POST
             /cli/heartbeat/fire name=<hb> force=1; a session is spawned
             and the agent gets the wakeup prompt.

Evidence is what the agent itself recorded: the fake agent's transcript
under <home>/.claude/projects/<slug>/<session>.jsonl (the prompt and the
reply "[k2-fake-agent] reply #<n>: <prompt>") and its trace file
(--fake-trace, K2_FAKE_AGENT_TRACE: cli.exec with argv and resumed).

    python3 -I tests/lab/labkit/w1.py --port P --token-file F --home H \\
        --workspace W --out DIR [--fake-trace T] [--only chat,wake,heartbeat]

Writes DIR/W1.json {"status": "pass"|"fail", "checks": [{name, ok,
detail}], ...} and DIR/W1.log, prints one line per check, exits 0 only if
every check passes. Deadlines only stop a hung run; no check asserts a
duration.
"""

import argparse
import glob
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

if __package__ in (None, ""):
    # Run as a script: make `labkit` importable from tests/lab.
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from labkit import s1  # noqa: E402
    from labkit import ws as wslib  # noqa: E402
else:
    from . import s1  # noqa: E402
    from . import ws as wslib  # noqa: E402

DEADLINE_S = 90  # per wait; only stops a hung run
HTTP_TIMEOUT_S = 120  # msg/fire block while the agent wakes (readiness ceiling 20 s)
FLOWS = ("chat", "wake", "heartbeat")
BANNER = "k2-fake-agent (as claude)"
REPLY_MARKER = s1.REPLY_MARKER


def now_iso():
    return s1._now_iso()


def read_jsonl(path):
    out = []
    if path and os.path.isfile(path):
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        out.append(json.loads(line))
                    except ValueError:
                        pass
    return out


def pid_alive(pid):
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except (PermissionError, ValueError, TypeError):
        return True
    return True


def wait_until(fn, deadline=DEADLINE_S, poll=0.1):
    end = time.monotonic() + deadline
    while True:
        v = fn()
        if v or time.monotonic() >= end:
            return v
        time.sleep(poll)


class W1:
    def __init__(self, a, token):
        self.a, self.token = a, token
        self.port, self.home, self.out = a.port, a.home, a.out
        self.fake_trace = a.fake_trace
        self.checks, self.log_lines, self.flows = [], [], {}
        self.tag = uuid.uuid4().hex[:6]

    # ── plumbing ────────────────────────────────────────────────────────
    def log(self, msg):
        self.log_lines.append("%s %s" % (now_iso(), msg))

    def check(self, flow, name, ok, detail=""):
        full = "%s: %s" % (flow, name)
        self.checks.append({"name": full, "flow": flow, "ok": bool(ok), "detail": detail})
        self.log("%s %s %s" % ("PASS" if ok else "FAIL", full, detail))
        print("%s %s%s" % ("PASS" if ok else "FAIL", full, (" -- " + detail) if detail else ""), flush=True)
        return bool(ok)

    def http(self, method, path, query=None, body=None, form=None):
        """JSON body (`body`) or form-encoded body (`form`). Returns
        (status, text, parsed-json-or-{})."""
        q = dict(query or {})
        q["token"] = self.token
        url = "http://127.0.0.1:%d%s?%s" % (self.port, path, urllib.parse.urlencode(q))
        headers = {}
        if form is not None:
            data = urllib.parse.urlencode(form).encode()
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        elif body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        else:
            data = b"" if method == "POST" else None
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        shown = form if form is not None else body
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as r:
                status, text = r.status, r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            status, text = e.code, e.read().decode("utf-8", "replace")
        except OSError as e:
            status, text = 0, "request failed: %s" % e
        self.log("%s %s %s -> %s %s" % (method, path, json.dumps(shown), status, text[:600]))
        try:
            parsed = json.loads(text)
        except ValueError:
            parsed = {}
        return status, text, parsed if isinstance(parsed, dict) else {"_": parsed}

    def new_workspace(self, flow):
        path = os.path.join(os.path.realpath(self.a.workspace), "w1-%s-%s" % (flow, self.tag))
        os.makedirs(path, exist_ok=True)
        subprocess.run(["git", "init", "-q", path], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return os.path.realpath(path)

    def register(self, flow, path):
        status, text, resp = self.http("POST", "/cli/projects/add-from-path",
                                       body={"path": path, "seed_wiki": False, "seed_agents_md": False})
        pid = resp.get("id")
        ok = status == 200 and bool(pid) and not resp.get("needsGitInit")
        self.check(flow, "workspace registered (POST /cli/projects/add-from-path)", ok,
                   "HTTP %s project id %s" % (status, pid) if ok else "HTTP %s %s" % (status, text[:300]))
        return pid if ok else None

    def enable_agent(self, flow, pid):
        """The app's 'agent on' switch: projects.agent_enabled = 1. With no
        session saved and no process, that is a sleeping agent."""
        status, text, _ = self.http("POST", "/cli/projects/update", body={"id": pid, "agentEnabled": 1})
        return self.check(flow, "workspace agent enabled (POST /cli/projects/update agentEnabled=1)",
                          status == 200, "HTTP %s %s" % (status, text[:200]))

    def transcripts(self, cwd):
        d = os.path.join(self.home, ".claude", "projects", s1.claude_project_slug(cwd))
        return sorted(glob.glob(os.path.join(d, "*.jsonl")))

    def find_turn(self, cwd, needle, session=None):
        """The transcript turn whose user prompt contains `needle`:
        {session, prompt, replies, file}, or None."""
        for path in self.transcripts(cwd):
            sid = os.path.basename(path)[:-len(".jsonl")]
            if session and sid != session:
                continue
            rows = read_jsonl(path)
            for i, v in enumerate(rows):
                msg = v.get("message") or {}
                content = msg.get("content")
                if v.get("type") == "user" and isinstance(content, str) and needle in content:
                    replies = []
                    for w in rows[i + 1:]:
                        if w.get("type") == "user":
                            break
                        for part in (w.get("message") or {}).get("content") or []:
                            if isinstance(part, dict) and part.get("type") == "text":
                                replies.append(part.get("text", ""))
                    return {"session": sid, "prompt": content, "replies": replies,
                            "file": os.path.relpath(path, self.home)}
        return None

    def wait_turn_done(self, cwd, needle, session=None):
        """Wait for the prompt AND its reply (the turn ended) in the transcript."""
        def done():
            t = self.find_turn(cwd, needle, session)
            return t if t and any(REPLY_MARKER in r and needle in r for r in t["replies"]) else None
        return wait_until(done) or self.find_turn(cwd, needle, session)

    def fake_events(self, event, session):
        return [e for e in read_jsonl(self.fake_trace)
                if e.get("event") == event and e.get("session_id") == session]

    def trace_evidence(self, flow, session, resumed=None, label="the fake agent ran as claude for that session"):
        """Check the fake agent's own cli.exec for `session` (skipped when no
        --fake-trace was given)."""
        if not self.fake_trace:
            self.log("no --fake-trace: skipped %s" % label)
            return None
        evs = wait_until(lambda: [e for e in self.fake_events("cli.exec", session)
                                  if resumed is None or (e.get("attrs") or {}).get("resumed") == resumed], 10)
        ok = bool(evs) and all((e.get("attrs") or {}).get("as") == "claude" for e in evs)
        detail = ("argv %s" % json.dumps((evs[-1].get("attrs") or {}).get("argv"))) if evs else \
            "no cli.exec%s for session %s in %s" % ("" if resumed is None else " resumed=%s" % resumed,
                                                      session, self.fake_trace)
        self.check(flow, label + " (fake trace cli.exec)", ok, detail)
        return evs[-1] if evs else None

    # ── flow 1: chat overlay ────────────────────────────────────────────
    def flow_chat(self):
        f = "chat"
        info = self.flows.setdefault(f, {})
        cwd = self.new_workspace(f)
        info["workspace"] = cwd
        if not self.register(f, cwd):
            return
        body = {"project": cwd}
        info["ensure_body"] = body
        status, text, resp = self.http("POST", "/cli/workspace/ensure-pinned-chat", body=body)
        sid, csid = resp.get("sessionId"), resp.get("claudeSessionId")
        info.update(daemon_session=sid, claude_session=csid, ensure_response=resp)
        if not self.check(f, "ensure-pinned-chat returns a session", status == 200 and sid,
                          "HTTP %s sessionId %s claudeSessionId %s command %s %s" % (
                              status, sid, csid, resp.get("command"), json.dumps(resp.get("args")))
                          if status == 200 else "HTTP %s %s" % (status, text[:300])):
            return
        self.check(f, "the first ensure spawned a fresh session (reused=false)", resp.get("reused") is False,
                   "reused=%r" % resp.get("reused"))

        grid = s1.Grid()
        try:
            sock = wslib.WebSocket("127.0.0.1", self.port, "/cli/sessions/grid?" + urllib.parse.urlencode(
                {"session": sid, "token": self.token}), timeout=DEADLINE_S)
        except (OSError, wslib.WsError) as e:
            self.check(f, "attach WS /cli/sessions/grid", False, str(e))
            return
        self.check(f, "attach WS /cli/sessions/grid", True, "session=%s" % sid)
        banner = BANNER + (" · session %s" % csid if csid else "")
        try:
            seen = self.grid_wait(sock, grid, banner)
            if not self.check(f, "the fake agent's banner is on the grid", seen, banner):
                return
            text_in = "w1 chat %s" % uuid.uuid4().hex[:6]
            info["typed"] = text_in
            sock.send_text(json.dumps({"action": "input", "text": text_in}))
            time.sleep(0.15)
            sock.send_text(json.dumps({"action": "input", "text": "\r"}))
            reply = "%s1: %s" % (REPLY_MARKER, text_in)
            self.check(f, "a typed prompt is answered on the grid", self.grid_wait(sock, grid, reply), reply)
        finally:
            info["screen"] = grid.text()
            sock.close()

        turn = self.wait_turn_done(cwd, info["typed"], csid)
        self.check(f, "transcript in the agent home has the prompt and reply",
                   turn and turn["prompt"] == info["typed"] and any(reply in r for r in turn["replies"]),
                   json.dumps(turn) if turn else "no turn in %s" % self.transcripts(cwd))
        self.trace_evidence(f, csid)

        status, text, resp2 = self.http("POST", "/cli/workspace/ensure-pinned-chat", body=body)
        self.check(f, "a second ensure returns the same live session (reused=true)",
                   status == 200 and resp2.get("sessionId") == sid and resp2.get("reused") is True,
                   "HTTP %s sessionId %s reused %r" % (status, resp2.get("sessionId"), resp2.get("reused")))

    def grid_wait(self, sock, grid, needle):
        deadline = time.monotonic() + DEADLINE_S
        while needle not in grid.text():
            left = deadline - time.monotonic()
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

    # ── flow 2: a message wakes a sleeping agent (S4) ──────────────────
    def send_msg(self, info, key, cwd, text, wake):
        form = {"workspace": cwd, "text": text, "from": "lab-w1", "wake": "true" if wake else "false"}
        info.setdefault("msg_bodies", {})[key] = form
        status, raw, resp = self.http("POST", "/cli/workspace/msg", form=form)
        info.setdefault("msg_responses", {})[key] = resp or raw[:300]
        return status, raw, resp

    def flow_wake(self):
        f = "wake"
        info = self.flows.setdefault(f, {})
        cwd = self.new_workspace(f)
        info["workspace"] = cwd
        pid = self.register(f, cwd)
        if not pid or not self.enable_agent(f, pid):
            return

        # Asleep: no transcript, and the daemon itself says so.
        status, raw, resp = self.send_msg(info, "no_wake", cwd, "w1 probe %s" % self.tag, wake=False)
        self.check(f, "the agent is asleep: msg wake=false -> dormant_no_wake",
                   status == 200 and resp.get("success") is False and resp.get("reason") == "dormant_no_wake"
                   and not self.transcripts(cwd),
                   "HTTP %s %s; transcripts %s" % (status, raw[:200], self.transcripts(cwd)))

        text1 = "w1 wake %s" % uuid.uuid4().hex[:6]
        status, raw, resp = self.send_msg(info, "wake1", cwd, text1, wake=True)
        if not self.check(f, "msg (wake on) is delivered and woke the agent",
                          status == 200 and resp.get("success") is True and resp.get("woke") is True,
                          "HTTP %s %s" % (status, raw[:300])):
            return
        turn = self.wait_turn_done(cwd, text1)
        info["turn1"] = turn
        ok = bool(turn) and any(REPLY_MARKER in r and text1 in r for r in turn["replies"])
        self.check(f, "the woken agent got the message and answered (transcript)", ok,
                   json.dumps(turn) if turn else "no prompt containing %r in %s" % (text1, self.transcripts(cwd)))
        if not turn:
            return
        csid = turn["session"]
        ex = self.trace_evidence(f, csid, resumed=False, label="the wake spawned a new claude session")

        # Sleep again, as the active reaper / app dismiss does: v2/close on
        # the canonical slot (keyed by the bare project id). Then a message
        # wakes it and the same conversation resumes.
        close = {"agent_name": pid, "force": True}
        info["close_body"] = close
        status, raw, resp = self.http("POST", "/cli/sessions/v2/close", body=close)
        # The close kills the CLI (no clean cli.exit), so the evidence is
        # that the pid from its cli.exec is gone. Same machine only: on a
        # remote host the pid is not ours to probe, so this is skipped.
        pid_cli = (ex or {}).get("pid")
        gone = None
        if pid_cli:
            gone = wait_until(lambda: not pid_alive(pid_cli), 15)
        self.check(f, "the agent is put back to sleep (POST /cli/sessions/v2/close)",
                   status == 200 and resp.get("closed") is True and gone is not False,
                   "HTTP %s %s; agent pid %s gone=%s" % (status, raw[:200], pid_cli, gone))
        text2 = "w1 wake-again %s" % uuid.uuid4().hex[:6]
        status, raw, resp = self.send_msg(info, "wake2", cwd, text2, wake=True)
        self.check(f, "a second msg wakes it again", status == 200 and resp.get("success") is True
                   and resp.get("woke") is True, "HTTP %s %s" % (status, raw[:300]))
        turn2 = self.wait_turn_done(cwd, text2)
        info["turn2"] = turn2
        self.check(f, "the second wake resumed the same conversation (same session id, answered)",
                   bool(turn2) and turn2["session"] == csid
                   and any(REPLY_MARKER in r and text2 in r for r in turn2["replies"]),
                   json.dumps(turn2) if turn2 else "no prompt containing %r" % text2)
        if ex is not None:
            self.trace_evidence(f, csid, resumed=True, label="the second wake resumed it (--resume)")

    # ── flow 3: a heartbeat fires on a sleeping agent (S5) ─────────────
    def flow_heartbeat(self):
        f = "heartbeat"
        info = self.flows.setdefault(f, {})
        cwd = self.new_workspace(f)
        info["workspace"] = cwd
        pid = self.register(f, cwd)
        if not pid or not self.enable_agent(f, pid):
            return
        name = "w1-hb-%s" % self.tag
        marker = "w1 heartbeat wakeup %s" % uuid.uuid4().hex[:6]
        # Daily, 12 h from now: never due during the run, so only the
        # manual fire below can launch it.
        due = time.localtime(time.time() + 12 * 3600)
        add = {"project": cwd, "name": name, "frequency": "daily",
               "spec": json.dumps({"time": "%02d:%02d" % (due.tm_hour, due.tm_min)}),
               "instructions": marker}
        info["add_body"] = add
        status, raw, resp = self.http("POST", "/cli/heartbeat/add", body=add)
        if not self.check(f, "heartbeat added (POST /cli/heartbeat/add)", status == 200,
                          "HTTP %s %s" % (status, raw[:300])):
            return
        if not self.transcripts(cwd):
            self.log("heartbeat workspace has no transcript before the fire: the agent is asleep")
        fire = {"project": cwd, "name": name, "force": "1"}
        info["fire_body"] = fire
        status, raw, resp = self.http("POST", "/cli/heartbeat/fire", body=fire)
        info["fire_response"] = resp
        if not self.check(f, "fire succeeds and spawns a session",
                          status == 200 and resp.get("success") is True and bool(resp.get("terminalId")
                                                                                  or resp.get("targetSessionId")),
                          "HTTP %s decision %s branch %s session %s" % (
                              status, resp.get("decision"), resp.get("branch"),
                              resp.get("terminalId") or resp.get("targetSessionId"))
                          if status == 200 else "HTTP %s %s" % (status, raw[:300])):
            return
        turn = self.wait_turn_done(cwd, marker)
        info["turn"] = turn
        self.check(f, "the agent got the wakeup prompt and answered (transcript)",
                   bool(turn) and any(REPLY_MARKER in r and marker in r for r in turn["replies"]),
                   json.dumps(turn) if turn else "no prompt containing %r in %s" % (marker, self.transcripts(cwd)))
        if turn:
            self.trace_evidence(f, turn["session"], resumed=False,
                                label="the fire spawned a new claude session")

    # ── run ────────────────────────────────────────────────────────────
    def run(self, only):
        os.makedirs(self.out, exist_ok=True)
        for f in only:
            try:
                getattr(self, "flow_" + f)()
            except Exception as e:  # report, never hang or hide
                self.check(f, "flow ran to the end", False, "%s: %s" % (type(e).__name__, e))
        ok = bool(self.checks) and all(c["ok"] for c in self.checks)
        result = {"scenario": "W1", "title": "Chat overlay, message wake (S4), heartbeat fire (S5)",
                  "status": "pass" if ok else "fail", "port": self.port, "flows_run": list(only),
                  "checks": self.checks, "flows": self.flows}
        with open(os.path.join(self.out, "W1.json"), "w") as fh:
            json.dump(result, fh, indent=2)
        with open(os.path.join(self.out, "W1.log"), "w") as fh:
            fh.write("\n".join(self.log_lines) + "\n")
            screen = (self.flows.get("chat") or {}).get("screen")
            if screen:
                fh.write("\n--- chat: the client's screen at the end ---\n" + screen + "\n")
        print("W1 %s" % result["status"].upper(), flush=True)
        return 0 if ok else 1


def main(argv):
    p = argparse.ArgumentParser(prog="w1.py", description=__doc__.split("\n\n")[0])
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--token-file", required=True)
    p.add_argument("--home", required=True, help="the agents' HOME (transcripts under .claude/projects)")
    p.add_argument("--workspace", required=True, help="a dir to create the flows' workspaces under")
    p.add_argument("--out", required=True)
    p.add_argument("--fake-trace", default=None, help="K2_FAKE_AGENT_TRACE file (extra evidence)")
    p.add_argument("--only", default=",".join(FLOWS), help="comma list of %s" % ",".join(FLOWS))
    a = p.parse_args(argv)
    only = [f.strip() for f in a.only.split(",") if f.strip()]
    bad = [f for f in only if f not in FLOWS]
    if bad:
        p.error("unknown flow(s): %s" % ",".join(bad))
    with open(a.token_file) as fh:
        token = fh.read().strip()
    return W1(a, token).run(only)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
