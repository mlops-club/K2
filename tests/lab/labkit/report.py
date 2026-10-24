"""The evidence bundle's report: report.json, report.html, invariants.txt.

`build(run_dir)` reads what prove.sh wrote into the run dir:

    run.json             gate, topology, sha, times, args, baseline path
    parity/<suite>.json  one per suite (scripts/lab/lib/parity.sh)
    scenarios/<S>.json   one per scenario that ran (labkit/s1.py, ...)
    flows.json           the flow checker's results (labkit/flows.py)
    metrics.csv          metric,value,unit,note
    demo.cast            when asciinema recorded one

and decides the verdict: "pass" only when every REQUIRED item passed. A
required item that did not run is NOT RUN, shown in red, and fails the gate.
Nothing here skips: a missing file for a required item is NOT RUN with the
reason "no result written".
"""

import csv
import html
import json
import os

PARITY_SUITES = [
    ("linux-gate", "scripts/test-gate-linux.sh <sha> (Linux, as root)"),
    ("cli", "tests/cli/*.sh"),
    ("tier3", "tests/behavior-test-tier3.sh"),
    ("tier1", "tests/behavior-test-tier1.sh"),
    ("contract", "tests/contract/must-not-break.ts"),
]

# Scenario matrix (PRD section 3): which scenarios a gate runs on a topology,
# and whether each is required (filled dot) or only reported. G0 on solo:
# S1 and S12 are required from G0 on; S2 and S7 are in P0's proof list but
# the matrix requires them from G1.
SCENARIOS = {
    ("G0", "solo"): [
        ("S1", "Spawn, attach, type, see output", True),
        ("S12", "Postgres, Apps, mail, Sew, front door parity", True),
        ("S2", "Hook events land on the right agent", False),
        ("S7", "CLI killed mid-turn, next need resumes", False),
    ],
}
NOT_BUILT = "not implemented in wave 1 (the lab ships S1 only)"

INVARIANTS = [
    ("I1", "An agent has at most one live placement (epochs)"),
    ("I2", "Every message reaches a final state"),
    ("I3", "A resumed agent keeps its session id and memory files byte for byte"),
    ("I4", "An agent's home is written by one host at a time"),
    ("I5", "Solo mode output matches the pre-change baseline on every existing suite"),
]
# Why an invariant is not exercised on a topology at a gate.
INVARIANT_NA = {
    ("G0", "solo"): {
        "I1": "solo has one in-process host; placements and epochs arrive in P2",
        "I2": "no message delivery at G0 (S3, S4, S6 are G2)",
        "I3": "no scenario at G0 resumes an agent (S7 is G1, S10 G3); the fake agent's own tests check it",
        "I4": "solo has one host",
    },
}


def _load(path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _maybe(path):
    return _load(path) if os.path.isfile(path) else None


def compare_baseline(parity, baseline):
    """I5: every suite (and every cli script) has the baseline's status.
    Returns (status, reason, diffs)."""
    diffs = []
    for sid, _ in PARITY_SUITES:
        cur, base = parity.get(sid), baseline["suites"].get(sid)
        if base is None:
            diffs.append("%s: not in the baseline" % sid)
            continue
        if cur is None or cur["status"] == "not_run":
            diffs.append("%s: not run now" % sid)
            continue
        if cur["status"] != base["status"]:
            diffs.append("%s: %s, baseline %s" % (sid, cur["status"], base["status"]))
        base_items = base.get("items", {})
        cur_items = {i["name"]: i["status"] for i in cur.get("items", [])}
        for name in sorted(set(base_items) | set(cur_items)):
            if base_items.get(name) != cur_items.get(name):
                diffs.append("%s/%s: %s, baseline %s" % (sid, name, cur_items.get(name, "absent"),
                                                         base_items.get(name, "absent")))
    if any(d.endswith("not run now") for d in diffs):
        return "not_run", "a parity suite did not run, so it cannot be compared", diffs
    return ("pass", "", diffs) if not diffs else ("fail", "%d difference(s) from the baseline" % len(diffs), diffs)


def baseline_from(run_dir):
    """The baseline document for this run's parity results."""
    run = _load(os.path.join(run_dir, "run.json"))
    suites = {}
    for sid, _ in PARITY_SUITES:
        r = _maybe(os.path.join(run_dir, "parity", sid + ".json"))
        if r is None or r["status"] == "not_run":
            raise ValueError("cannot record a baseline: %s did not run" % sid)
        suites[sid] = {"status": r["status"], "items": {i["name"]: i["status"] for i in r.get("items", [])}}
    return {"gate": run["gate"], "topology": run["topology"], "sha": run["sha"],
            "crates_tree": run.get("crates_tree"), "recorded": run["started"], "suites": suites}


def build(run_dir):
    run = _load(os.path.join(run_dir, "run.json"))
    gate, topo = run["gate"], run["topology"]
    required = []

    parity = {}
    for sid, title in PARITY_SUITES:
        r = _maybe(os.path.join(run_dir, "parity", sid + ".json"))
        if r is None:
            r = {"suite": sid, "title": title, "status": "not_run", "reason": "no result written", "items": []}
        parity[sid] = r
        required.append({"kind": "parity", "id": sid, "title": r["title"], "status": r["status"],
                         "reason": r.get("reason")})

    scenarios = []
    for sid, title, req in SCENARIOS.get((gate, topo), []):
        r = _maybe(os.path.join(run_dir, "scenarios", sid + ".json"))
        if r is None:
            r = {"scenario": sid, "title": title, "status": "not_run",
                 "reason": NOT_BUILT if sid != "S1" else "no result written", "checks": []}
        r["required"] = req
        scenarios.append(r)
        if req:
            required.append({"kind": "scenario", "id": sid, "title": title, "status": r["status"],
                             "reason": r.get("reason") or _first_failed(r)})

    invariants = []
    na = INVARIANT_NA.get((gate, topo), {})
    for iid, title in INVARIANTS:
        if iid in na:
            invariants.append({"id": iid, "title": title, "status": "n/a", "reason": na[iid], "required": False})
            continue
        if iid != "I5":
            invariants.append({"id": iid, "title": title, "status": "not_run",
                               "reason": "no check built for this topology yet", "required": True})
            continue
        base_path = run.get("baseline")
        inv = {"id": iid, "title": title, "required": True, "baseline": base_path, "diffs": []}
        if run.get("recorded_baseline"):
            inv.update(status="not_run", reason="this run recorded the baseline (%s); I5 compares later runs to it" % base_path)
        elif not base_path or not os.path.isfile(os.path.join(run["repo"], base_path)):
            inv.update(status="not_run", reason="no baseline recorded for %s/%s (prove.sh --record-baseline)" % (gate, topo))
        else:
            base = _load(os.path.join(run["repo"], base_path))
            st, why, diffs = compare_baseline(parity, base)
            inv.update(status=st, diffs=diffs,
                       reason=(why + "; " if why else "") + "baseline %s recorded %s at %s (crates/ tree %s; this run %s)" % (
                           base_path, base["recorded"], base["sha"][:12], (base.get("crates_tree") or "?")[:12],
                           (run.get("crates_tree") or "?")[:12]))
        invariants.append(inv)
    for inv in invariants:
        if inv["required"]:
            required.append({"kind": "invariant", "id": inv["id"], "title": inv["title"],
                             "status": inv["status"], "reason": inv["reason"]})

    flows = _maybe(os.path.join(run_dir, "flows.json")) or []
    metrics = []
    mpath = os.path.join(run_dir, "metrics.csv")
    if os.path.isfile(mpath):
        with open(mpath, newline="") as fh:
            metrics = list(csv.DictReader(fh))
    demo = {"file": "demo.cast"} if os.path.isfile(os.path.join(run_dir, "demo.cast")) else {
        "file": None, "reason": run.get("demo_reason") or "asciinema is not installed; scenarios/S1.cast has the client's screen instead"}

    verdict = "pass" if required and all(r["status"] == "pass" for r in required) else "fail"
    return {
        "gate": gate, "topology": topo, "sha": run["sha"], "branch": run.get("branch"),
        "started": run["started"], "finished": run.get("finished"), "args": run.get("args"),
        "parity_mode": run.get("parity"), "chaos": run.get("chaos"),
        "verdict": verdict, "required": required,
        "parity": [parity[s] for s, _ in PARITY_SUITES], "scenarios": scenarios,
        "invariants": invariants, "flows": flows, "metrics": metrics, "demo": demo,
        "notes": run.get("notes", []),
    }


def _first_failed(r):
    for c in r.get("checks", []):
        if not c["ok"]:
            return "%s: %s" % (c["name"], c.get("detail", ""))
    return None


def invariants_text(rep):
    head = "  ".join("%s %s" % (i["id"], {"pass": "ok", "fail": "FAIL", "not_run": "NOT-RUN"}.get(i["status"], i["status"]))
                     for i in rep["invariants"])
    lines = [head, ""]
    for i in rep["invariants"]:
        lines.append("%s %s: %s" % (i["id"], i["status"], i.get("reason") or ""))
        lines += ["    " + d for d in i.get("diffs", [])]
    return "\n".join(lines) + "\n"


# ── HTML ────────────────────────────────────────────────────────────────

CSS = """
:root { --bg:#f8fafc; --panel:#fff; --ink:#0f172a; --muted:#475569; --line:#e2e8f0;
  --pass-bg:#d1fae5; --pass:#065f46; --fail-bg:#fee2e2; --fail:#991b1b; --na-bg:#f1f5f9; --na:#475569;
  --info-bg:#dbeafe; --info:#1e3a8a; }
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) { --bg:#0b1220; --panel:#111a2e; --ink:#e2e8f0;
  --muted:#94a3b8; --line:#1e293b; --pass-bg:#064e3b; --pass:#a7f3d0; --fail-bg:#7f1d1d; --fail:#fecaca;
  --na-bg:#1e293b; --na:#cbd5e1; --info-bg:#1e3a8a; --info:#bfdbfe; } }
* { box-sizing:border-box; }
body { margin:0; background:var(--bg); color:var(--ink); font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif; padding:24px 16px 64px; }
main { max-width:1100px; margin:0 auto; }
h1 { margin:0 0 4px; font-size:24px; } h2 { margin:32px 0 8px; font-size:18px; }
.meta { color:var(--muted); font-size:13px; } code { font:12.5px ui-monospace,Menlo,monospace; }
.verdict { display:inline-block; padding:6px 14px; border-radius:8px; font-weight:700; margin:12px 0; }
table { border-collapse:collapse; width:100%; background:var(--panel); border:1px solid var(--line); border-radius:8px; display:block; overflow-x:auto; }
th,td { border-bottom:1px solid var(--line); padding:6px 10px; text-align:left; vertical-align:top; }
th { font-size:12px; color:var(--muted); text-transform:uppercase; letter-spacing:.03em; }
.chip { display:inline-block; padding:1px 8px; border-radius:999px; font-size:12px; font-weight:600; white-space:nowrap; }
.pass { background:var(--pass-bg); color:var(--pass); } .fail, .not_run { background:var(--fail-bg); color:var(--fail); }
.na, .n\\/a, .reported { background:var(--na-bg); color:var(--na); } .expected_only { background:var(--info-bg); color:var(--info); }
details { margin:4px 0; } summary { cursor:pointer; color:var(--muted); }
.reason { color:var(--muted); font-size:13px; }
"""

LABEL = {"pass": "PASS", "fail": "FAIL", "not_run": "NOT RUN", "n/a": "n/a", "expected_only": "expected only"}


def _chip(status):
    cls = status.replace("/", "\\/") if status != "n/a" else "na"
    return '<span class="chip %s">%s</span>' % (html.escape(cls), LABEL.get(status, html.escape(status)))


def _e(s):
    return html.escape("" if s is None else str(s))


def _link(path, text=None):
    return '<a href="%s"><code>%s</code></a>' % (_e(path), _e(text or path))


def render_html(rep):
    out = []
    w = out.append
    w("<!doctype html><html lang='en'><head><meta charset='utf-8'>"
      "<meta name='viewport' content='width=device-width,initial-scale=1'>")
    w("<title>K2 lab %s %s</title><style>%s</style></head><body><main>" % (_e(rep["gate"]), _e(rep["topology"]), CSS))
    w("<h1>K2 lab · gate %s · topology %s</h1>" % (_e(rep["gate"]), _e(rep["topology"])))
    w("<div class='meta'>sha <code>%s</code> · branch %s · started %s · finished %s · parity %s · chaos %s</div>"
      % (_e(rep["sha"]), _e(rep.get("branch")), _e(rep["started"]), _e(rep.get("finished")),
         _e(rep.get("parity_mode")), _e(rep.get("chaos") or "off")))
    w("<div class='verdict %s'>Gate %s</div>" % ("pass" if rep["verdict"] == "pass" else "fail",
                                                  "PASSED" if rep["verdict"] == "pass" else "NOT PASSED"))
    for n in rep.get("notes", []):
        w("<p class='reason'>%s</p>" % _e(n))

    w("<h2>Required items</h2><table><tr><th>Kind</th><th>Item</th><th>Status</th><th>Detail</th></tr>")
    for r in rep["required"]:
        w("<tr><td>%s</td><td><b>%s</b> %s</td><td>%s</td><td class='reason'>%s</td></tr>"
          % (_e(r["kind"]), _e(r["id"]), _e(r["title"]), _chip(r["status"]), _e(r.get("reason"))))
    w("</table>")

    w("<h2>Parity: existing suites, unchanged</h2><table><tr><th>Suite</th><th>Status</th><th>Detail</th><th>Log</th></tr>")
    for p in rep["parity"]:
        detail = _e(p.get("reason"))
        items = p.get("items") or []
        if items:
            failed = [i for i in items if i["status"] != "pass"]
            skipped = [i for i in items if i.get("skips")]
            detail += "<details><summary>%d scripts: %d passed, %d failed; %d printed SKIP</summary><table>" % (
                len(items), len(items) - len(failed), len(failed), len(skipped))
            for i in sorted(items, key=lambda i: (i["status"] == "pass", i["name"])):
                detail += "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (
                    _e(i["name"]), _chip(i["status"]), "SKIP x%d" % i["skips"] if i.get("skips") else "", _link(i["log"], "log"))
            detail += "</table></details>"
        w("<tr><td><b>%s</b><br><span class='reason'>%s</span></td><td>%s</td><td class='reason'>%s</td><td>%s</td></tr>"
          % (_e(p["suite"]), _e(p["title"]), _chip(p["status"]), detail, _link(p["log"], "log") if p.get("log") else ""))
    w("</table>")

    w("<h2>Scenarios</h2><table><tr><th>Scenario</th><th>Required</th><th>Status</th><th>Checks</th><th>Files</th></tr>")
    for s in rep["scenarios"]:
        checks = "".join("<div>%s %s <span class='reason'>%s</span></div>" % (
            _chip("pass" if c["ok"] else "fail"), _e(c["name"]), _e(c.get("detail"))) for c in s.get("checks", []))
        if s.get("reason"):
            checks += "<div class='reason'>%s</div>" % _e(s["reason"])
        if s.get("metrics"):
            checks += "<div class='reason'>%s</div>" % _e(", ".join("%s=%s" % kv for kv in s["metrics"].items()))
        files = ""
        if s["status"] != "not_run":
            sid = s["scenario"]
            files = "<br>".join(_link("scenarios/%s.%s" % (sid, ext)) for ext in ("log", "trace.jsonl", "cast", "json"))
            if s.get("trace_note"):
                files += "<div class='reason'>%s</div>" % _e(s["trace_note"])
        w("<tr><td><b>%s</b> %s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
          % (_e(s["scenario"]), _e(s["title"]), "required" if s["required"] else _chip("reported"),
             _chip(s["status"]), checks, files))
    w("</table>")

    w("<h2>Invariants</h2><table><tr><th>Invariant</th><th>Status</th><th>Detail</th></tr>")
    for i in rep["invariants"]:
        diffs = ""
        if i.get("diffs"):
            diffs = "<details><summary>%d difference(s)</summary><pre>%s</pre></details>" % (
                len(i["diffs"]), _e("\n".join(i["diffs"])))
        w("<tr><td><b>%s</b> %s</td><td>%s</td><td class='reason'>%s%s</td></tr>"
          % (_e(i["id"]), _e(i["title"]), _chip(i["status"]), _e(i.get("reason")), diffs))
    w("</table>")

    w("<h2>Flows: the PRD's sequence diagrams as expected traces</h2>")
    w("<p class='reason'>Expected only until the daemon emits trace events (wave 2). Steps the lab or the fake agent "
      "saw are stamped for reading; nothing is judged.</p>")
    w("<table><tr><th>Flow</th><th>Status</th><th>Steps (event · label · observed)</th></tr>")
    for f in rep["flows"]:
        steps = "".join("<div><code>%d %s</code> %s %s</div>" % (
            s["n"], _e(s["event"]), _e(s["label"]),
            "<span class='chip pass'>seen %s</span>" % _e(s["observed_ts"]) if s["observed_ts"] else "") for s in f["steps"])
        w("<tr><td><b>%s</b><br>%s</td><td>%s</td><td><details><summary>%d steps</summary>%s</details></td></tr>"
          % (_e(f["flow"]), _link("../../../docs/prd/flows/%s.json" % f["flow"], "flows/%s.json" % f["flow"]),
             _chip(f["status"]), len(f["steps"]), steps))
    w("</table>")

    w("<h2>Metrics</h2><table><tr><th>Metric</th><th>Value</th><th>Unit</th><th>Note</th></tr>")
    for m in rep["metrics"]:
        w("<tr><td>%s</td><td>%s</td><td>%s</td><td class='reason'>%s</td></tr>"
          % (_e(m.get("metric")), _e(m.get("value")), _e(m.get("unit")), _e(m.get("note"))))
    w("</table>")

    w("<h2>Topology and demo</h2><p>%s · %s · %s · %s · %s</p>" % (
        _link("topology/"), _link("invariants.txt"), _link("metrics.csv"), _link("report.json"),
        _link(rep["demo"]["file"]) if rep["demo"]["file"] else "demo.cast: <span class='reason'>%s</span>" % _e(rep["demo"]["reason"])))
    w("</main></body></html>\n")
    return "".join(out)


def write(run_dir, rep):
    with open(os.path.join(run_dir, "report.json"), "w") as fh:
        json.dump(rep, fh, indent=2)
    with open(os.path.join(run_dir, "report.html"), "w") as fh:
        fh.write(render_html(rep))
    with open(os.path.join(run_dir, "invariants.txt"), "w") as fh:
        fh.write(invariants_text(rep))

