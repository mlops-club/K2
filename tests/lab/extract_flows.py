#!/usr/bin/env python3
"""Extract the PRD's sequence diagrams into docs/prd/flows/<id>.json.

    python3 -I tests/lab/extract_flows.py            # write the files
    python3 -I tests/lab/extract_flows.py --check    # exit 1 if any differs

The diagrams live as JS data (`const FLOWS = {...}`) in
docs/prd/k2-distributed-runtime.html. This keeps the PRD's text (parts,
steps, labels, notes) and adds what a trace needs: an `event` name per step,
optional `match` fields, and the flow's correlation key. Those come from
EVENTS below, the one place to edit when wave 2 names its trace events.
The `prove` flow (the gate run itself) is the runner's own, not a flow the
system under test performs, so it is not extracted.
"""

import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
PRD = os.path.join(REPO, "docs", "prd", "k2-distributed-runtime.html")
OUT = os.path.join(REPO, "docs", "prd", "flows")

FLOW_IDS = ["spawn", "msg", "sleep", "ctlrestart", "hostdrain", "hook",
            "move", "install", "drain", "laptop", "localattach"]

# Per flow: the correlation key that ties one instance's events together
# (None: every event in the trace is one instance), and per step the event
# name a trace carries, optionally with fields the event must also have.
EVENTS = {
    "spawn": ("agent_id", [
        "api.spawn_request", "placement.choose", "host.spawn", "ptyd.open_session",
        "cli.exec", ("cli.hook", {"hook": "SessionStart"}), "host.agent_event", "client.stream"]),
    "msg": ("msg_id", [
        "cli.msg_send", "ctl.deliver", "outbox.append", "lifecycle.wake", "host.spawn",
        "host.ready", "host.inject", "cli.paste", "outbox.delivered", "cli.msg_result"]),
    "sleep": ("agent_id", [
        "lifecycle.scan", "host.release", "host.released", "provisioner.scale_down",
        "scheduler.due", "lifecycle.need", "provisioner.ensure_host", "host.spawn", "host.ready"]),
    "ctlrestart": ("host", [
        "ctl.sigterm", "host.link_down", "host.buffer", "ctl.start", "host.resume", "client.replay"]),
    "hostdrain": ("host", [
        "host.sigterm", "host.draining", "host.wait_turn", ("cli.hook", {"hook": "Stop"}),
        "host.released", "host.replaced", "host.enroll", "host.spawn"]),
    "hook": ("agent_id", [
        "cli.hook", "cell.owner_walk", "cell.owner_verdict", "cell.event", "host.agent_event"]),
    "move": ("agent_id", [
        "host.release", "host.released", "home.push", "placement.epoch_bump", "host.spawn",
        "home.fetch", "host.ready"]),
    "install": (None, [
        "owner.apply", "kube.apply", "kube.start_controller", "kube.start_hosts", "host.enroll",
        "owner.first_run"]),
    "drain": (None, [
        "kube.evict", "kube.schedule", "kube.pending", "ctl.host_away", "ctl.move_decision"]),
    "laptop": ("msg_id", [
        "host.heartbeat_stop", "ctl.host_away", "ctl.deliver", "outbox.queued", "host.reconnect",
        "outbox.drain", "outbox.delivered"]),
    "localattach": ("host", [
        "app.controller_unreachable", "host.local_attach", "host.local_list", "host.local_input",
        "cli.pty_write", "host.queue", "host.reconnect_flush"]),
}


def js_literal(src, i):
    """Parse the JS object/array/string/number literal at src[i] (the subset
    the PRD uses: unquoted keys, double-quoted strings, trailing commas).
    Returns (value, next index)."""
    def ws(i):
        while i < len(src):
            if src[i].isspace():
                i += 1
            elif src.startswith("//", i):
                i = src.index("\n", i)
            elif src.startswith("/*", i):
                i = src.index("*/", i) + 2
            else:
                break
        return i

    def val(i):
        i = ws(i)
        c = src[i]
        if c == '"':
            j = i + 1
            while src[j] != '"':
                j += 2 if src[j] == "\\" else 1
            return json.loads(src[i:j + 1]), j + 1
        if c == "{":
            obj, i = {}, ws(i + 1)
            while src[i] != "}":
                m = re.compile(r'"(?:[^"\\]|\\.)*"|[A-Za-z_$][\w$]*').match(src, i)
                if not m:
                    raise ValueError("bad key at %d: %r" % (i, src[i:i + 30]))
                key = json.loads(m.group()) if m.group().startswith('"') else m.group()
                i = ws(m.end())
                if src[i] != ":":
                    raise ValueError("expected ':' at %d" % i)
                obj[key], i = val(i + 1)
                i = ws(i)
                if src[i] == ",":
                    i = ws(i + 1)
            return obj, i + 1
        if c == "[":
            arr, i = [], ws(i + 1)
            while src[i] != "]":
                v, i = val(i)
                arr.append(v)
                i = ws(i)
                if src[i] == ",":
                    i = ws(i + 1)
            return arr, i + 1
        m = re.compile(r"-?\d+(\.\d+)?|true|false|null").match(src, i)
        if not m:
            raise ValueError("unsupported literal at %d: %r" % (i, src[i:i + 30]))
        return json.loads(m.group()), m.end()

    return val(i)


def prd_flows(html):
    start = html.index("const FLOWS = ") + len("const FLOWS = ")
    flows, _ = js_literal(html, start)
    return flows


def build(fid, f):
    correlate, events = EVENTS[fid]
    if len(events) != len(f["steps"]):
        raise ValueError("%s: %d steps in the PRD, %d events mapped" % (fid, len(f["steps"]), len(events)))
    steps = []
    for n, (s, ev) in enumerate(zip(f["steps"], events), 1):
        event, match = (ev if isinstance(ev, tuple) else (ev, {}))
        step = {"n": n, "from": s["f"], "to": s["t"], "kind": s.get("k", "call"),
                "label": s["x"], "event": event}
        if match:
            step["match"] = match
        step["say"] = s["say"]
        steps.append(step)
    return {
        "$schema": "../../../tests/lab/flows.schema.json",
        "id": fid,
        "name": f["name"],
        "note": f["note"],
        "proof": [p.strip() for p in f["proof"].split(",")],
        "source": "docs/prd/k2-distributed-runtime.html, FLOWS.%s" % fid,
        "correlate": correlate,
        "participants": [{"id": p[0], "name": p[1], "role": p[2]} for p in f["parts"]],
        "steps": steps,
    }


def render(doc):
    return json.dumps(doc, indent=2, ensure_ascii=False) + "\n"


def expected_files():
    with open(PRD, encoding="utf-8") as fh:
        flows = prd_flows(fh.read())
    missing = [fid for fid in FLOW_IDS if fid not in flows]
    if missing:
        raise ValueError("flows missing from the PRD: %s" % missing)
    extra = sorted(set(flows) - set(FLOW_IDS) - {"prove"})
    if extra:
        raise ValueError("PRD has flows this extractor does not know: %s" % extra)
    return {fid: render(build(fid, flows[fid])) for fid in FLOW_IDS}


def main(argv):
    check = "--check" in argv
    files = expected_files()
    bad = []
    for fid, text in files.items():
        path = os.path.join(OUT, fid + ".json")
        if check:
            try:
                with open(path, encoding="utf-8") as fh:
                    if fh.read() != text:
                        bad.append(path)
            except FileNotFoundError:
                bad.append(path)
        else:
            os.makedirs(OUT, exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(text)
    if check and bad:
        print("out of date with the PRD (run tests/lab/extract_flows.py):\n  " + "\n  ".join(bad))
        return 1
    print("%s %d flows in %s" % ("checked" if check else "wrote", len(files), os.path.relpath(OUT, REPO)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
