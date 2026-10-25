"""Invariants I1 to I4 (PRD section 3), judged after the scenarios from the
evidence a G2 run leaves in its run dir, never from the scenarios' own
checks:

  traces/<proc>.daemon.jsonl   each daemon's K2_TRACE_FILE (ctl.place, ...)
  traces/<host>.fake.jsonl     each host's fake agents (cli.exec with pid,
                               cwd, memory_sha256; cli.turn.end; cli.exit)
  samples.jsonl                the fleet monitor: live daemons and agent CLI
                               pids, every 0.5 s
  scenarios/messages.jsonl     the lab's ledger of every message it sent
  outbox-final/<proc>.json     each daemon's GET /cli/hosts/outbox at the end

An agent is keyed by its workspace (the CLI's cwd): every lab agent has its
own. Only agent sessions count: CLIs started in a PTY (they carry a pane,
K2_PANE_ID). Helpers a daemon runs itself (`codex app-server`, `cursor agent
stdio`) have none and are left out. `check_all(run_dir, layout)` returns one result per invariant:
{"id", "status": pass|fail|not_run, "reason", "diffs": [violations]}.
"""

import json
import os

from .fleet import read_jsonl

FINAL = ("delivered", "failed")


def _ts(e):
    return e.get("ts") or ""


def _load_dir(run_dir, suffix):
    d = os.path.join(run_dir, "traces")
    out = {}
    if os.path.isdir(d):
        for f in sorted(os.listdir(d)):
            if f.endswith(suffix):
                out[f[:-len(suffix)]] = read_jsonl(os.path.join(d, f))
    return out


class Evidence:
    def __init__(self, run_dir, layout):
        self.run_dir, self.layout = run_dir, layout
        self.daemon = _load_dir(run_dir, ".daemon.jsonl")
        fake = _load_dir(run_dir, ".fake.jsonl")
        self.fake = []
        for host, evs in fake.items():
            for e in evs:
                e.setdefault("host", host)
                self.fake.append(e)
        self.fake.sort(key=_ts)
        self.samples = read_jsonl(os.path.join(run_dir, "samples.jsonl"))
        self.ledger = read_jsonl(os.path.join(run_dir, "scenarios", "messages.jsonl"))
        self.outboxes = {}
        od = os.path.join(run_dir, "outbox-final")
        if os.path.isdir(od):
            for f in sorted(os.listdir(od)):
                if f.endswith(".json"):
                    with open(os.path.join(od, f)) as fh:
                        try:
                            self.outboxes[f[:-5]] = json.load(fh)
                        except ValueError:
                            self.outboxes[f[:-5]] = None
        # One record per agent CLI process.
        self.clis = {}
        for e in self.fake:
            pid = e.get("pid")
            if e.get("event") == "cli.exec" and pid and e.get("pane"):
                a = e.get("attrs") or {}
                self.clis[(e["host"], pid, _ts(e))] = {
                    "host": e["host"], "pid": pid, "start": _ts(e), "end": None, "cwd": a.get("cwd"),
                    "session": e.get("session_id"), "resumed": bool(a.get("resumed")),
                    "mem_start": a.get("memory_sha256"), "mem_last": a.get("memory_sha256"), "turn_ends": []}
        for e in self.fake:
            c = self._cli_of(e)
            if not c:
                continue
            a = e.get("attrs") or {}
            if e.get("event") == "cli.turn.end":
                c["turn_ends"].append(_ts(e))
                if a.get("memory_sha256"):
                    c["mem_last"] = a["memory_sha256"]
            elif e.get("event") == "cli.exit":
                c["end"] = _ts(e)
                if a.get("memory_sha256"):
                    c["mem_last"] = a["memory_sha256"]
        # A CLI that never wrote cli.exit (killed) ends at the first sample
        # that no longer shows its pid after one did; one no sample ever
        # showed lived less than a period: it ends where it started. One
        # still shown at the last sample is alive at the end (end None).
        for c in self.clis.values():
            if c["end"]:
                continue
            seen = False
            for s in self.samples:
                if s["ts"] < c["start"]:
                    continue
                if c["pid"] in s.get("clis", []):
                    seen = True
                elif seen:
                    c["end"] = s["ts"]
                    break
            if not seen:
                c["end"] = c["start"]

    def _cli_of(self, e):
        """The CLI record an event belongs to: same host and pid, the latest
        cli.exec at or before it."""
        best = None
        for (host, pid, start), c in self.clis.items():
            if host == e.get("host") and pid == e.get("pid") and start <= _ts(e):
                if best is None or start > best["start"]:
                    best = c
        return best


def _result(iid, diffs, ran, ok_reason, not_run_reason):
    if not ran:
        return {"id": iid, "status": "not_run", "reason": not_run_reason, "diffs": []}
    if diffs:
        return {"id": iid, "status": "fail", "reason": "%d violation(s)" % len(diffs), "diffs": diffs}
    return {"id": iid, "status": "pass", "reason": ok_reason, "diffs": []}


def check_i1(ev):
    """At most one live CLI per agent at any sample; placement epochs only grow."""
    diffs = []
    for s in ev.samples:
        by_cwd = {}
        for pid in s.get("clis", []):
            for c in ev.clis.values():
                if c["pid"] == pid and c.get("cwd") and c["start"] <= s["ts"] and (c["end"] is None or c["end"] >= s["ts"]):
                    by_cwd.setdefault(c["cwd"], set()).add((c["host"], pid))
        for cwd, live in sorted(by_cwd.items()):
            if len(live) > 1:
                diffs.append("%s: %d live CLIs for %s: %s" % (s["ts"], len(live), cwd, sorted(live)))
    places = []
    for proc, evs in ev.daemon.items():
        for e in evs:
            if e.get("event") == "ctl.place":
                ep = e.get("epoch") or (e.get("attrs") or {}).get("epoch")
                places.append((_ts(e), ep, e.get("agent_id"), (e.get("attrs") or {}).get("host")))
    places.sort()
    last = None
    for ts, ep, agent, host in places:
        if ep is None:
            diffs.append("%s: ctl.place of %s on %s carries no epoch" % (ts, agent, host))
            continue
        if last is not None and int(ep) <= int(last[1]):
            diffs.append("%s: epoch %s for %s on %s is not above %s (issued %s)" % (ts, ep, agent, host, last[1], last[0]))
        last = (ts, ep)
    ran = bool(ev.samples) and bool(ev.clis)
    return _result("I1", diffs, ran,
                   "%d samples of %d agent CLIs, never two live for one agent; %d placements, epochs strictly increasing" % (
                       len(ev.samples), len(ev.clis), len(places)),
                   "no monitor samples or no agent CLI ran")


def _outbox_rows(doc):
    if not doc:
        return []
    if "messages" in doc:
        return doc["messages"] or []
    return [doc["message"]] if doc.get("message") else []


def check_i2(ev):
    """Every message ends delivered or failed (with a reason). None vanish."""
    diffs = []
    ledger = [m for m in ev.ledger]
    if ev.layout == "solo":
        for m in ledger:
            r = m.get("response")
            r = r if isinstance(r, dict) else {}
            if m.get("http") not in (200, None) and not r:
                continue  # an HTTP error is a loud failure
            if r.get("success") is True:
                continue
            if r.get("success") is False and (r.get("reason") or r.get("error")):
                continue
            if m.get("rc") not in (None, 0) and m.get("response"):
                continue
            diffs.append("%s %s → %s: no final answer (%s)" % (m.get("scenario"), m.get("kind", "msg"), m.get("to"),
                                                                json.dumps(m.get("response"))[:200]))
        return _result("I2", diffs, bool(ledger), "%d message(s), each answered delivered or failed with a reason" % len(ledger),
                       "the lab sent no messages")

    rows = {}
    for proc, doc in ev.outboxes.items():
        hop = (doc or {}).get("hop")
        for r in _outbox_rows(doc):
            rows.setdefault(r.get("msg_id"), []).append((proc, hop, r))
    ids = {m.get("msg_id") for m in ledger if m.get("msg_id")} | set(rows)
    for proc, evs in ev.daemon.items():
        for e in evs:
            if e.get("msg_id") and (e.get("event") or "").startswith(("host.msg", "ctl.msg", "ctl.deliver", "host.deliver")):
                ids.add(e["msg_id"])
    ids.discard(None)
    for m in ledger:
        r = m.get("response") if isinstance(m.get("response"), dict) else {}
        if not m.get("msg_id") and r.get("success") is not False and m.get("http", 200) == 200 and m.get("rc", 0) == 0:
            diffs.append("%s %s → %s: accepted without a msg id, so it cannot be tracked" % (
                m.get("scenario"), m.get("kind", "msg"), m.get("to")))
    for mid in sorted(ids):
        seen = rows.get(mid, [])
        if not seen:
            diffs.append("%s: in no outbox at the end (vanished)" % mid)
            continue
        down = [r for p, hop, r in seen if hop == "down"]
        up = [r for p, hop, r in seen if hop == "up"]
        for p, hop, r in seen:
            if r.get("state") not in FINAL:
                diffs.append("%s: %s hop %s still %s (attempts %s)" % (mid, p, hop, r.get("state"), r.get("attempts")))
            elif r.get("state") == "failed" and not r.get("reason"):
                diffs.append("%s: %s hop %s failed with no reason" % (mid, p, hop))
        if up and not down and all(r.get("state") == "delivered" for r in up):
            diffs.append("%s: the controller accepted it (host hop up delivered) but has no row (vanished)" % mid)
    return _result("I2", diffs, bool(ids), "%d message(s), every outbox row final, failures with reasons" % len(ids),
                   "no message ids in the ledger, outboxes or traces")


def check_i3(ev):
    """A resumed CLI keeps the agent's provider session id and starts with
    the memory bytes the previous CLI left (sha256)."""
    diffs = []
    resumed = [c for c in ev.clis.values() if c["resumed"]]
    by_start = sorted(ev.clis.values(), key=lambda c: c["start"])
    for c in resumed:
        prev = [p for p in by_start if p["start"] < c["start"] and p["cwd"] == c["cwd"]]
        if not prev:
            diffs.append("%s pid %s on %s resumed %s with no earlier CLI for %s" % (c["start"], c["pid"], c["host"], c["session"], c["cwd"]))
            continue
        p = prev[-1]
        if p["session"] != c["session"]:
            diffs.append("%s: %s resumed session %s, its previous CLI had %s" % (c["start"], c["cwd"], c["session"], p["session"]))
        if p["mem_last"] != c["mem_start"]:
            diffs.append("%s: %s memory changed across the resume: %s (left by pid %s on %s) → %s (pid %s on %s)" % (
                c["start"], c["cwd"], p["mem_last"], p["pid"], p["host"], c["mem_start"], c["pid"], c["host"]))
    return _result("I3", diffs, bool(resumed), "%d resume(s), session id and MEMORY.md sha256 kept" % len(resumed),
                   "no agent was resumed in this run")


def check_i4(ev):
    """An agent's home is written by one host at a time: no agent has writes
    (turn ends, inbox items) from a host while a CLI of it lives on another."""
    diffs = []
    writes = []
    for c in ev.clis.values():
        for t in c["turn_ends"]:
            writes.append((t, c["cwd"], c["host"], "turn (pid %s)" % c["pid"]))
    for proc, evs in ev.daemon.items():
        for e in evs:
            if e.get("event") == "inbox.write":
                a = e.get("attrs") or {}
                cwd = a.get("cwd") or a.get("workspace") or a.get("path")
                if cwd:
                    writes.append((_ts(e), cwd, a.get("host") or proc, "inbox item"))
    for t, cwd, host, what in sorted(writes):
        for c in ev.clis.values():
            if c["host"] == host or not c["cwd"] or not cwd.startswith(c["cwd"]):
                continue
            if c["start"] <= t and (c["end"] is None or t <= c["end"]):
                diffs.append("%s: %s for %s written by %s while pid %s ran it on %s" % (t, what, c["cwd"], host, c["pid"], c["host"]))
    hosts = {c["host"] for c in ev.clis.values()}
    return _result("I4", diffs, bool(writes), "%d home write(s) by %d host(s), never two hosts at once" % (len(writes), len(hosts)),
                   "no agent home writes were recorded")


def check_all(run_dir, layout):
    ev = Evidence(run_dir, layout)
    return [check_i1(ev), check_i2(ev), check_i3(ev), check_i4(ev)]
