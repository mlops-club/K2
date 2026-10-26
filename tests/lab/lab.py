#!/usr/bin/env python3
"""The lab kit's command line, called by scripts/lab/prove.sh.

    lab.py s1 --port P --token-file F --home H --cwd W --out DIR [--fake-trace T]
    lab.py flows --flows-dir D [--trace T] --out FILE
    lab.py report --run-dir DIR
    lab.py baseline --run-dir DIR --out FILE
    lab.py fleet up|down|kill|start|monitor ...   (labkit/fleet.py)
    lab.py g2 --state F --out DIR [--only S2,...,S9]   (labkit/g2.py)
    lab.py collect --state F --out DIR      wait for final outbox rows, snapshot them
    lab.py invariants --run-dir DIR --layout fleet|solo   (labkit/invariants.py)
    lab.py chaos --state F --seed N --out chaos.log  (labkit/chaos.py; until SIGTERM)

Standard library only; run with `python3 -I`.
"""

import argparse
import json
import os
import signal
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from labkit import chaos, fleet, flows, g2, invariants, report, s1  # noqa: E402


def cmd_s1(a):
    with open(a.token_file) as fh:
        token = fh.read().strip()
    res = s1.Run(a.port, token, a.home, a.cwd, a.out, a.fake_trace).run()
    print("S1 %s" % res["status"].upper())
    for c in res["checks"]:
        print("  %s %s" % ("ok  " if c["ok"] else "FAIL", c["name"]))
    return 0 if res["status"] == "pass" else 1


def cmd_flows(a):
    events = flows.read_trace(a.trace) if a.trace else None
    results = [flows.check(f, events) for f in flows.load_flows(a.flows_dir).values()]
    with open(a.out, "w") as fh:
        json.dump(results, fh, indent=2)
    for r in results:
        print("  %-12s %s" % (r["flow"], r["status"]))
    return 0 if all(r["status"] in ("pass", "expected_only") for r in results) else 1


def cmd_report(a):
    rep = report.build(a.run_dir)
    report.write(a.run_dir, rep)
    print("report: %s (%s)" % (os.path.join(a.run_dir, "report.html"), rep["verdict"]))
    return 0 if rep["verdict"] == "pass" else 1


def cmd_baseline(a):
    doc = report.baseline_from(a.run_dir)
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    with open(a.out, "w") as fh:
        json.dump(doc, fh, indent=2)
        fh.write("\n")
    print("baseline: %s" % a.out)
    return 0


def cmd_fleet(a):
    if a.action == "up":
        st = fleet.up(a.layout, a.run_dir, a.state, a.daemon_bin, a.fake_bin)
        for name, p in st["procs"].items():
            print("fleet: %s (%s) pid %s port %s" % (name, p["role"], p["pid"], p["port"]))
        return 0
    st = fleet.load(a.state)
    if a.action == "down":
        fleet.down(st)
        return 0
    if a.action == "kill":
        ok = fleet.kill(st, a.proc, getattr(signal, "SIG" + a.signal), a.wait)
        print("fleet: %s %s" % (a.proc, "gone" if ok else "still running"))
        return 0 if ok else 1
    if a.action == "start":
        print("fleet: %s pid %s" % (a.proc, fleet.start(st, a.proc)))
        return 0
    if a.action == "monitor":
        fleet.monitor(a.state, a.out)
        return 0
    return 2


def cmd_g2(a):
    only = [s.strip() for s in a.only.split(",") if s.strip()]
    bad = [s for s in only if s not in g2.TITLES]
    if bad:
        print("lab.py g2: unknown scenario(s) %s" % ",".join(bad), file=sys.stderr)
        return 2
    res = g2.run(a.state, a.out, only, a.min_seconds)
    return 0 if all(r["status"] in ("pass", "n/a") for r in res.values()) else 1


def cmd_collect(a):
    """Wait (up to --deadline s, only to stop a hang) until every row of
    every daemon's outbox is delivered or failed, then write each listing to
    <out>/<proc>.json. Exit 0 when all were final."""
    import time
    st = fleet.load(a.state)
    os.makedirs(a.out, exist_ok=True)

    def listings():
        out = {}
        for name, p in st["procs"].items():
            try:
                with open(os.path.join(p["home"], ".k2", "daemon.token")) as fh:
                    tok = fh.read().strip()
            except OSError:
                tok = ""
            out[name] = fleet.http_json(p["port"], "/cli/hosts/outbox?token=" + tok, timeout=5)
        return out

    end = time.monotonic() + a.deadline
    while True:
        docs = listings()
        pending = [(n, m.get("msg_id"), m.get("state")) for n, d in docs.items() if d
                   for m in d.get("messages") or [] if m.get("state") not in ("delivered", "failed")]
        if not pending or time.monotonic() >= end:
            break
        time.sleep(1)
    for name, doc in docs.items():
        with open(os.path.join(a.out, name + ".json"), "w") as fh:
            json.dump(doc, fh, indent=2)
    print("collect: %d outbox row(s) not final%s" % (len(pending), (": %s" % pending[:5]) if pending else ""))
    return 0 if not pending else 1


def cmd_invariants(a):
    res = invariants.check_all(a.run_dir, a.layout)
    with open(os.path.join(a.run_dir, "invariants.json"), "w") as fh:
        json.dump(res, fh, indent=2)
    for r in res:
        print("%s %s: %s" % (r["id"], r["status"], r["reason"]))
        for d in r["diffs"][:10]:
            print("    " + d)
    return 0 if all(r["status"] == "pass" for r in res) else 1


def cmd_chaos(a):
    chaos.Chaos(a.state, a.seed, a.out).run()
    return 0


def main(argv):
    p = argparse.ArgumentParser(prog="lab.py")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("s1")
    s.add_argument("--port", type=int, required=True)
    s.add_argument("--token-file", required=True)
    s.add_argument("--home", required=True)
    s.add_argument("--cwd", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--fake-trace")
    s.set_defaults(fn=cmd_s1)
    f = sub.add_parser("flows")
    f.add_argument("--flows-dir", required=True)
    f.add_argument("--trace")
    f.add_argument("--out", required=True)
    f.set_defaults(fn=cmd_flows)
    r = sub.add_parser("report")
    r.add_argument("--run-dir", required=True)
    r.set_defaults(fn=cmd_report)
    b = sub.add_parser("baseline")
    b.add_argument("--run-dir", required=True)
    b.add_argument("--out", required=True)
    b.set_defaults(fn=cmd_baseline)
    fl = sub.add_parser("fleet")
    fl.add_argument("action", choices=["up", "down", "kill", "start", "monitor"])
    fl.add_argument("proc", nargs="?")
    fl.add_argument("--state", required=True)
    fl.add_argument("--layout", choices=["fleet", "solo"], default="fleet")
    fl.add_argument("--run-dir")
    fl.add_argument("--daemon-bin")
    fl.add_argument("--fake-bin")
    fl.add_argument("--signal", choices=["KILL", "TERM"], default="KILL")
    fl.add_argument("--wait", type=float, default=None, help="seconds to wait for the process to go")
    fl.add_argument("--out")
    fl.set_defaults(fn=cmd_fleet)
    g = sub.add_parser("g2")
    g.add_argument("--state", required=True)
    g.add_argument("--out", required=True)
    g.add_argument("--only", default=",".join(g2.TITLES))
    g.add_argument("--min-seconds", type=float, default=0,
                   help="repeat the set in rounds until this long has passed (the --chaos window)")
    g.set_defaults(fn=cmd_g2)
    co = sub.add_parser("collect")
    co.add_argument("--state", required=True)
    co.add_argument("--out", required=True)
    co.add_argument("--deadline", type=float, default=180)
    co.set_defaults(fn=cmd_collect)
    iv = sub.add_parser("invariants")
    iv.add_argument("--run-dir", required=True)
    iv.add_argument("--layout", choices=["fleet", "solo"], required=True)
    iv.set_defaults(fn=cmd_invariants)
    ch = sub.add_parser("chaos")
    ch.add_argument("--state", required=True)
    ch.add_argument("--seed", type=int, required=True)
    ch.add_argument("--out", required=True)
    ch.set_defaults(fn=cmd_chaos)
    a = p.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
