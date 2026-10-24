#!/usr/bin/env python3
"""G1 on solo: the daemon's trace against the spawn, hook and crash flows.

Run by g1_solo.sh against a solo daemon started with K2_TRACE_FILE:

  1. S1 (labkit/s1.py, unchanged): spawn, attach, type, see output.
  2. S7, a CLI crash: spawn `claude --session-id X` under a `tab-*` agent of
     a registered workspace, type a long prompt, `kill -9` the CLI once its
     turn has started, wait for the daemon to mark the session dead, then
     the next need (an empty-command spawn of the same agent, as the app's
     tab visit does) resumes it: today's daemon replays `--resume X`.
  3. Merge the daemon's, the fake agent's and the lab's events into one
     trace and check the flows with labkit/flows.py.

A flow is checked on its solo steps: steps listed in NOT_IN_SOLO (no honest
emission point before remote hosts) are dropped and reported, never faked.
Each flow is checked on its scenario's events (the agent's own events plus
the fake agent's events for its provider session), so another agent's
events can never complete it.

Writes into --out: trace.jsonl (merged), flows.json (verdicts), S1/, S7.json.
Deadlines only stop a hung run; nothing asserts a duration.

    g1_solo.py --port P --token-file F --home H --cwd W --crash-cwd C
               --daemon-trace T --fake-trace T --flows-dir D [--flows-dir D2]
               --out DIR [--bench N]
"""

import argparse
import json
import os
import signal
import statistics
import sys
import time
import urllib.parse
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from labkit import flows, s1  # noqa: E402
from labkit import ws as wslib  # noqa: E402

DEADLINE_S = 60  # per wait; only stops a hung run
CRASH_TURN_MS = 30000  # the crash turn outlasts the kill by far

# Flow steps with no honest emission point in solo (see
# docs/prd/progress/trace.md, "Flow steps not in solo").
NOT_IN_SOLO = {
    "spawn": {2: "placement.choose: no placement before remote hosts (P2); "
                 "solo has one in-process host"},
}
# The flows G1 requires, and the scenario whose events prove each.
G1_FLOWS = (("spawn", "S1"), ("hook", "S1"), ("crash", "S7"))


def now_iso():
    return s1._now_iso()


def read_jsonl(path):
    if not os.path.isfile(path):
        return []
    return flows.read_trace(path)


def wait_until(what, fn, deadline=DEADLINE_S, poll=0.02):
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        v = fn()
        if v:
            return v
        time.sleep(poll)
    raise TimeoutError("deadline waiting for %s" % what)


class Crash:
    """Scenario S7: CLI killed mid-turn, the next need resumes it."""

    def __init__(self, a, token):
        self.a, self.token = a, token
        self.agent = "tab-" + str(uuid.uuid4())
        self.claude_session = str(uuid.uuid4())
        self.events, self.checks, self.log_lines = [], [], []

    def log(self, msg):
        self.log_lines.append("%s %s" % (now_iso(), msg))

    def event(self, name, **attrs):
        self.events.append({"ts": now_iso(), "event": name, "src": "lab", "agent_id": self.agent,
                            "session_id": self.claude_session, "attrs": attrs})

    def check(self, name, ok, detail=""):
        self.checks.append({"name": name, "ok": bool(ok), "detail": detail})
        self.log("%s %s %s" % ("PASS" if ok else "FAIL", name, detail))
        return ok

    def http(self, method, path, body=None):
        run = s1.Run(self.a.port, self.token, self.a.home, self.a.crash_cwd, self.a.out, None)
        return run.http(method, path, body=body)

    def fake(self, event, **match):
        for e in read_jsonl(self.a.fake_trace):
            if e.get("event") == event and e.get("session_id") == self.claude_session \
                    and all((e.get("attrs") or {}).get(k) == v for k, v in match.items()):
                return e
        return None

    def daemon(self, event, pane, **match):
        for e in read_jsonl(self.a.daemon_trace):
            at = e.get("attrs") or {}
            if e.get("event") == event and at.get("pane") == pane \
                    and all(at.get(k) == v for k, v in match.items()):
                return e
        return None

    def spawn(self, body):
        status, text = self.http("POST", "/cli/sessions/v2/spawn", body)
        self.log("spawn %s -> %s %s" % (json.dumps(body), status, text[:400]))
        try:
            return status, json.loads(text)
        except ValueError:
            return status, {}

    def run(self):
        try:
            return self._run()
        except TimeoutError as e:
            self.check("scenario finished", False, str(e))
            return self.result()

    def _run(self):
        a = self.a
        status, text = self.http("POST", "/cli/projects/add-from-path",
                                 {"path": a.crash_cwd, "seed_wiki": False, "seed_agents_md": False})
        self.log("add-from-path -> %s %s" % (status, text[:300]))
        if not self.check("workspace registered", status == 200, "HTTP %s" % status):
            return self.result()

        # 1. spawn, as the app opens an agent tab
        self.event("lab.spawn_request", command="claude")
        status, resp = self.spawn({"agent_name": self.agent, "cwd": a.crash_cwd, "command": "claude",
                                   "args": ["--session-id", self.claude_session,
                                            "--turn-ms", str(CRASH_TURN_MS)],
                                   "cols": s1.COLS, "rows": s1.ROWS})
        pane1 = resp.get("sessionId")
        if not self.check("spawn returns a session", status == 200 and pane1, "HTTP %s" % status):
            return self.result()
        exec1 = wait_until("cli.exec", lambda: self.fake("cli.exec", resumed=False))
        wait_until("SessionStart", lambda: self.fake("cli.hook", hook="SessionStart"))

        # 2. a turn starts; the CLI dies in it
        sock = wslib.WebSocket("127.0.0.1", a.port, "/cli/sessions/grid?" + urllib.parse.urlencode(
            {"session": pane1, "token": self.token}), timeout=DEADLINE_S)
        prompt = "a long task " + uuid.uuid4().hex[:6]
        sock.send_text(json.dumps({"action": "input", "text": prompt}))
        time.sleep(0.15)
        sock.send_text(json.dumps({"action": "input", "text": "\r"}))
        self.event("lab.typed", text=prompt)
        wait_until("cli.turn.start", lambda: self.fake("cli.turn.start"))
        pid = exec1.get("pid")
        os.kill(pid, signal.SIGKILL)
        self.event("lab.killed", pid=pid, signal="SIGKILL")
        self.log("killed the CLI, pid %s, mid-turn" % pid)
        sock.close()
        exit_ev = wait_until("ptyd.child_exit", lambda: self.daemon("ptyd.child_exit", pane1))
        self.check("the daemon saw the CLI die by signal 9",
                   (exit_ev.get("attrs") or {}).get("signal") == 9, json.dumps(exit_ev.get("attrs")))
        wait_until("host.agent_exit", lambda: self.daemon("host.agent_exit", pane1))
        self.check("the turn in progress never ended", self.fake("cli.turn.end") is None)

        # 3. the next need: the tab is visited again (empty command)
        self.event("lab.need")
        status, resp = self.spawn({"agent_name": self.agent, "cwd": a.crash_cwd,
                                   "cols": s1.COLS, "rows": s1.ROWS})
        pane2 = resp.get("sessionId")
        if not self.check("the need spawns a fresh session", status == 200 and pane2 and pane2 != pane1
                          and resp.get("reused") is False, "HTTP %s %s" % (status, json.dumps(resp)[:200])):
            return self.result()
        resumed = wait_until("resumed cli.exec", lambda: self.fake("cli.exec", resumed=True))
        self.check("the CLI resumes the same session id (I3)", resumed.get("session_id") == self.claude_session,
                   "argv %s" % (resumed.get("attrs") or {}).get("argv"))
        wait_until("host.agent_event after resume",
                   lambda: self.daemon("host.agent_event", pane2, hook="SessionStart"))
        self.event("lab.resumed")

        status, text = self.http("POST", "/cli/sessions/v2/close",
                                 {"agent_name": self.agent, "force": True, "clear_index": True})
        self.check("close is accepted", status == 200, "HTTP %s %s" % (status, text[:200]))
        return self.result()

    def result(self):
        ok = bool(self.checks) and all(c["ok"] for c in self.checks)
        return {"scenario": "S7", "title": "CLI killed mid-turn, next need resumes",
                "status": "pass" if ok else "fail", "agent_name": self.agent,
                "claude_session": self.claude_session, "checks": self.checks, "log": self.log_lines}


def solo_view(flow):
    """`flow` without its NOT_IN_SOLO steps, renumbered."""
    drop = NOT_IN_SOLO.get(flow["id"], {})
    steps = [dict(s) for s in flow["steps"] if s["n"] not in drop]
    for i, s in enumerate(steps, 1):
        s["n"] = i
    return dict(flow, steps=steps)


def scoped(events, agent, claude_sessions):
    """The scenario's events: its agent's, plus the fake agent's for its
    provider session(s)."""
    return [e for e in events if e.get("agent_id") == agent
            or (e.get("src") == "k2-fake-agent" and e.get("session_id") in claude_sessions)]


def check_g1(all_flows, events, scenarios):
    out = []
    for fid, sc in G1_FLOWS:
        flow = all_flows[fid]
        agent, sessions = scenarios[sc]
        r = flows.check(solo_view(flow), scoped(events, agent, sessions))
        drop = NOT_IN_SOLO.get(fid, {})
        kept = [s for s in flow["steps"] if s["n"] not in drop]
        steps = []
        for s, obs in zip(kept, r["steps"]):
            steps.append(dict(obs, n=s["n"]))
        for n, why in drop.items():
            s = flow["steps"][n - 1]
            steps.append({"n": n, "event": s["event"], "label": s["label"], "observed_ts": None,
                          "not_in_solo": why})
        steps.sort(key=lambda s: s["n"])
        first_missing = next((s["n"] for s in steps if s["observed_ts"] is None and "not_in_solo" not in s),
                             None)
        out.append({"flow": fid, "scenario": sc, "status": r["status"], "instance": r["instance"],
                    "steps": steps, "first_missing": first_missing,
                    "not_in_solo": sorted(drop)})
    return out


def run_once(a, token):
    os.makedirs(a.out, exist_ok=True)
    s1run = s1.Run(a.port, token, a.home, a.cwd, os.path.join(a.out, "S1"), a.fake_trace)
    s1res = s1run.run()
    crash = Crash(a, token)
    s7res = crash.run()
    with open(os.path.join(a.out, "S7.json"), "w") as fh:
        json.dump(s7res, fh, indent=2)
    return s1run, s1res, crash, s7res


def main(argv):
    p = argparse.ArgumentParser(prog="g1_solo.py")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--token-file", required=True)
    p.add_argument("--home", required=True)
    p.add_argument("--cwd", required=True)
    p.add_argument("--crash-cwd", required=True)
    p.add_argument("--daemon-trace", required=True)
    p.add_argument("--fake-trace", required=True)
    p.add_argument("--flows-dir", action="append", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--bench", type=int, default=0, help="only run S1 N times, print spawn_to_banner_ms")
    a = p.parse_args(argv)
    with open(a.token_file) as fh:
        token = fh.read().strip()

    if a.bench:
        ms = []
        for i in range(a.bench):
            r = s1.Run(a.port, token, a.home, a.cwd, os.path.join(a.out, "bench-%02d" % i), a.fake_trace).run()
            if r["status"] != "pass":
                print("bench run %d: S1 %s" % (i, r["status"]))
                return 1
            ms.append(r["metrics"]["spawn_to_banner_ms"])
        print("spawn_to_banner_ms median %.1f  runs %s" % (statistics.median(ms), " ".join("%.1f" % m for m in ms)))
        with open(os.path.join(a.out, "bench.json"), "w") as fh:
            json.dump({"spawn_to_banner_ms": ms, "median": statistics.median(ms)}, fh, indent=2)
        return 0

    s1run, s1res, crash, s7res = run_once(a, token)
    events = read_jsonl(a.daemon_trace) + read_jsonl(a.fake_trace) + s1run.events + crash.events
    events.sort(key=lambda e: e["ts"])
    with open(os.path.join(a.out, "trace.jsonl"), "w") as fh:
        for e in events:
            fh.write(json.dumps(e) + "\n")

    all_flows = {}
    for d in a.flows_dir:
        all_flows.update(flows.load_flows(d))
    scenarios = {"S1": (s1run.agent_name, {s1run.claude_session}),
                 "S7": (crash.agent, {crash.claude_session})}
    g1 = check_g1(all_flows, events, scenarios)
    # Every flow on the whole trace too, as prove.sh's checker sees it.
    whole = [flows.check(f, events) for f in all_flows.values()]
    with open(os.path.join(a.out, "flows.json"), "w") as fh:
        json.dump({"g1": g1, "whole_trace": whole}, fh, indent=2)

    print("S1 %s" % s1res["status"].upper())
    for c in s1res["checks"]:
        print("  %s %s" % ("ok  " if c["ok"] else "FAIL", c["name"]))
    print("S7 %s" % s7res["status"].upper())
    for c in s7res["checks"]:
        print("  %s %s" % ("ok  " if c["ok"] else "FAIL", c["name"]))
    print("G1 flows (solo steps):")
    for r in g1:
        print("  %-6s %-5s %s" % (r["flow"], r["status"].upper(),
                                  "" if r["status"] == "pass" else "first missing step %s" % r["first_missing"]))
        for s in r["steps"]:
            mark = "n/a " if "not_in_solo" in s else ("ok  " if s["observed_ts"] else "MISS")
            print("      %s %2d %-20s %s" % (mark, s["n"], s["event"],
                                             s.get("not_in_solo") or s["observed_ts"] or ""))
    print("All flows on the whole trace (informational):")
    for r in whole:
        print("  %-12s %s" % (r["flow"], r["status"]))
    ok = s1res["status"] == "pass" and s7res["status"] == "pass" and all(r["status"] == "pass" for r in g1)
    print("G1 solo trace: %s" % ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
