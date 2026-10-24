#!/usr/bin/env python3
"""The lab kit's command line, called by scripts/lab/prove.sh.

    lab.py s1 --port P --token-file F --home H --cwd W --out DIR [--fake-trace T]
    lab.py flows --flows-dir D [--trace T] --out FILE
    lab.py report --run-dir DIR
    lab.py baseline --run-dir DIR --out FILE

Standard library only; run with `python3 -I`.
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from labkit import flows, report, s1  # noqa: E402


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
    a = p.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
