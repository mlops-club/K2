"""Flows as data, and the checker that matches a trace against them.

A flow (docs/prd/flows/<id>.json, schema tests/lab/flows.schema.json) is
one PRD sequence diagram: participants and ordered steps, each step naming
the trace `event` that proves it.

A trace is JSONL, one event per line (format: docs/prd/progress/lab.md,
"Trace format"):

    {"ts": "2026-11-14T09:12:03.123Z", "event": "host.spawn", "src": "k2-host",
     "agent_id": "...", "msg_id": null, "host": "host-1", "attrs": {...}}

`check(flow, events)` says whether the events contain the flow's steps in
order. Events are ordered by `ts` (stable, so equal stamps keep file
order). With a correlation key K, one instance is the events whose K is a
given value, plus events that carry no K at all (a CLI hook knows no
agent_id); the flow passes when some value of K yields every step in
order. A step matches an event with the same name whose fields include the
step's `match` (looked up at the top level, then under `attrs`).

Until the daemon emits trace events (wave 2), a trace holds only the lab's
and the fake agent's own events. A flow is then reported "expected_only":
its steps are listed, nothing is judged.
"""

import glob
import json
import os

STEP_KINDS = {"call", "reply", "self", "fail"}
# Emitters that are not the system under test: their events alone never
# make a flow checkable.
NON_SUT_SOURCES = {"lab", "k2-fake-agent"}


class FlowError(ValueError):
    pass


def validate(flow):
    """Raise FlowError unless `flow` has the schema's shape."""
    def need(cond, msg):
        if not cond:
            raise FlowError("%s: %s" % (flow.get("id", "?") if isinstance(flow, dict) else "?", msg))

    need(isinstance(flow, dict), "not an object")
    for k in ("id", "name", "proof", "source", "correlate", "participants", "steps"):
        need(k in flow, "missing %r" % k)
    need(isinstance(flow["id"], str) and flow["id"].isalnum() and flow["id"].islower(), "bad id")
    need(flow["correlate"] is None or isinstance(flow["correlate"], str), "correlate must be a string or null")
    need(isinstance(flow["proof"], list), "proof must be a list")
    parts = flow["participants"]
    need(isinstance(parts, list) and len(parts) >= 2, "needs two or more participants")
    ids = set()
    for p in parts:
        need(isinstance(p, dict) and all(isinstance(p.get(k), str) for k in ("id", "name", "role")),
             "participant needs id, name, role")
        need(p["id"] not in ids, "duplicate participant %r" % p["id"])
        ids.add(p["id"])
    steps = flow["steps"]
    need(isinstance(steps, list) and steps, "needs steps")
    for i, s in enumerate(steps, 1):
        need(isinstance(s, dict), "step %d is not an object" % i)
        need(s.get("n") == i, "step %d is numbered %r" % (i, s.get("n")))
        need(s.get("from") in ids and s.get("to") in ids, "step %d names an unknown participant" % i)
        need(s.get("kind") in STEP_KINDS, "step %d has kind %r" % (i, s.get("kind")))
        need(isinstance(s.get("label"), str), "step %d needs a label" % i)
        ev = s.get("event")
        need(isinstance(ev, str) and "." in ev and ev == ev.strip().lower(), "step %d has event %r" % (i, ev))
        need(isinstance(s.get("match", {}), dict), "step %d: match must be an object" % i)
    return flow


def load_flows(flows_dir):
    """Every flow in `flows_dir`, validated, keyed by id."""
    out = {}
    for path in sorted(glob.glob(os.path.join(flows_dir, "*.json"))):
        with open(path, encoding="utf-8") as fh:
            flow = validate(json.load(fh))
        if flow["id"] + ".json" != os.path.basename(path):
            raise FlowError("%s holds flow %r" % (path, flow["id"]))
        out[flow["id"]] = flow
    return out


def read_trace(path):
    """Events from a JSONL trace. A line that is not a JSON object with
    string `ts` and `event` is an error (fail loudly)."""
    events = []
    with open(path, encoding="utf-8") as fh:
        for n, line in enumerate(fh, 1):
            if not line.strip():
                continue
            try:
                ev = json.loads(line)
            except ValueError as e:
                raise FlowError("%s:%d: not JSON: %s" % (path, n, e))
            if not isinstance(ev, dict) or not isinstance(ev.get("ts"), str) or not isinstance(ev.get("event"), str):
                raise FlowError("%s:%d: an event needs string ts and event" % (path, n))
            events.append(ev)
    return events


def _field(ev, key):
    if key in ev:
        return ev[key]
    attrs = ev.get("attrs")
    if isinstance(attrs, dict):
        return attrs.get(key)
    return None


def _matches(step, ev):
    if ev.get("event") != step["event"]:
        return False
    return all(_field(ev, k) == v for k, v in step.get("match", {}).items())


def _walk(steps, events):
    """Greedy in-order match (earliest event per step; optimal for a
    subsequence). Returns one entry per step: the event index or None."""
    hits, j = [], 0
    for s in steps:
        found = None
        while j < len(events):
            if _matches(s, events[j]):
                found = j
                j += 1
                break
            j += 1
        hits.append(found)
        if found is None:
            hits.extend([None] * (len(steps) - len(hits)))
            break
    return hits


def sut_traced(events):
    """True when some event came from the system under test."""
    return any(ev.get("src") not in NON_SUT_SOURCES for ev in (events or []))


def check(flow, events):
    """Match `events` (or None: no trace) against `flow`.

    Returns {"flow", "status", "instance", "steps": [{n, event, label,
    observed_ts}], "first_missing"}. status is "pass", "fail" or
    "expected_only" (no trace, or no event from the system under test).
    """
    steps = flow["steps"]
    blank = [{"n": s["n"], "event": s["event"], "label": s["label"], "observed_ts": None} for s in steps]
    if events is None or not sut_traced(events):
        observed = blank
        if events:
            # Still show which steps the lab and the fake agent saw (first
            # sighting each, order not judged), for reading.
            ordered = sorted(events, key=lambda e: e["ts"])
            hits = [next((i for i, e in enumerate(ordered) if _matches(s, e)), None) for s in steps]
            observed = _observed(steps, ordered, hits)
        return {"flow": flow["id"], "status": "expected_only", "instance": None,
                "steps": observed, "first_missing": None}
    ordered = sorted(events, key=lambda e: e["ts"])
    key = flow["correlate"]
    if key is None:
        groups = {None: ordered}
    else:
        values = []
        for ev in ordered:
            v = ev.get(key)
            if v is not None and v not in values and any(_matches(s, ev) for s in steps):
                values.append(v)
        groups = {v: [ev for ev in ordered if ev.get(key) in (v, None)] for v in values}
        if not groups:
            groups = {None: [ev for ev in ordered if ev.get(key) is None]}
    best = None
    for value, evs in groups.items():
        hits = _walk(steps, evs)
        score = sum(h is not None for h in hits)
        if best is None or score > best[0]:
            best = (score, value, evs, hits)
    score, value, evs, hits = best
    observed = _observed(steps, evs, hits)
    missing = next((s["n"] for s, h in zip(steps, hits) if h is None), None)
    return {"flow": flow["id"], "status": "pass" if missing is None else "fail",
            "instance": value, "steps": observed, "first_missing": missing}


def _observed(steps, evs, hits):
    return [{"n": s["n"], "event": s["event"], "label": s["label"],
             "observed_ts": evs[h]["ts"] if h is not None else None}
            for s, h in zip(steps, hits)]
