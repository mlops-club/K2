"""The report writer: verdict, NOT RUN handling, I5 against a baseline,
and the files it writes. Run: tests/lab/run_tests.sh"""

import json
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from labkit import report  # noqa: E402

SUITES = [s for s, _ in report.PARITY_SUITES]


class RunDir:
    """A fake run dir, as prove.sh leaves it."""

    def __init__(self, tmp, repo):
        self.d, self.repo = tmp, repo
        for sub in ("parity", "scenarios"):
            os.makedirs(os.path.join(tmp, sub), exist_ok=True)
        self.run = {"gate": "G0", "topology": "solo", "sha": "a" * 40, "crates_tree": "c" * 40,
                    "repo": repo, "started": "2026-10-24T10:00:00Z", "baseline": "baselines/G0-solo.json",
                    "recorded_baseline": False, "parity": "full"}
        self.save()

    def save(self):
        self._w("run.json", self.run)

    def _w(self, rel, doc):
        with open(os.path.join(self.d, rel), "w") as fh:
            json.dump(doc, fh)

    def parity(self, sid, status, items=None, reason=None):
        self._w("parity/%s.json" % sid, {"suite": sid, "title": sid, "status": status, "reason": reason,
                                         "log": "parity/%s.log" % sid,
                                         "items": [{"name": n, "status": s, "log": "x", "skips": 0}
                                                   for n, s in (items or {}).items()]})

    def all_parity(self, status="pass"):
        for s in SUITES:
            self.parity(s, status, items={"a": "pass", "b": "pass"} if s == "cli" else None)

    def scenario(self, sid, ok):
        self._w("scenarios/%s.json" % sid, {"scenario": sid, "title": sid, "status": "pass" if ok else "fail",
                                            "checks": [{"name": "c", "ok": ok, "detail": "d"}]})

    def baseline(self, cli_b="pass"):
        os.makedirs(os.path.join(self.repo, "baselines"), exist_ok=True)
        doc = {"sha": "b" * 40, "crates_tree": "c" * 40, "recorded": "2026-10-23T00:00:00Z",
               "suites": {s: {"status": "pass", "items": {"a": "pass", "b": cli_b} if s == "cli" else {}}
                          for s in SUITES}}
        with open(os.path.join(self.repo, "baselines", "G0-solo.json"), "w") as fh:
            json.dump(doc, fh)


def required(rep, rid):
    return next(r for r in rep["required"] if r["id"] == rid)


class Report(unittest.TestCase):
    def setUp(self):
        self._t = tempfile.TemporaryDirectory()
        self.addCleanup(self._t.cleanup)
        self.repo = os.path.join(self._t.name, "repo")
        os.makedirs(self.repo)
        self.r = RunDir(os.path.join(self._t.name, "run"), self.repo)
        os.makedirs(self.r.d, exist_ok=True)

    def test_nothing_written_is_all_not_run_and_fails(self):
        rep = report.build(self.r.d)
        self.assertEqual(rep["verdict"], "fail")
        for s in SUITES:
            self.assertEqual(required(rep, s)["status"], "not_run")
            self.assertEqual(required(rep, s)["reason"], "no result written")
        self.assertEqual(required(rep, "S1")["status"], "not_run")

    def test_required_set_for_g0_solo(self):
        rep = report.build(self.r.d)
        self.assertEqual([r["id"] for r in rep["required"]], SUITES + ["S1", "S12", "I5"])
        shown = [s["scenario"] for s in rep["scenarios"]]
        self.assertEqual(shown, ["S1", "S12", "S2", "S7"])
        self.assertEqual(required(rep, "S12")["reason"], report.NOT_BUILT)
        self.assertEqual([i["status"] for i in rep["invariants"]], ["n/a"] * 4 + ["not_run"])

    def test_one_not_run_suite_fails_the_gate_and_says_why(self):
        self.r.all_parity()
        self.r.parity("linux-gate", "not_run", reason="the gate refused to start: 9G free")
        self.r.scenario("S1", True)
        self.r.baseline()
        rep = report.build(self.r.d)
        self.assertEqual(rep["verdict"], "fail")
        self.assertEqual(required(rep, "linux-gate")["reason"], "the gate refused to start: 9G free")
        self.assertEqual(required(rep, "I5")["status"], "not_run")

    def test_i5_matches_the_baseline(self):
        self.r.all_parity()
        self.r.scenario("S1", True)
        self.r.baseline()
        rep = report.build(self.r.d)
        i5 = required(rep, "I5")
        self.assertEqual(i5["status"], "pass", i5)
        self.assertIn("baselines/G0-solo.json", i5["reason"])
        self.assertIn("bbbbbbbbbbbb", i5["reason"])
        # Everything required passed except S12, which wave 1 does not build.
        self.assertEqual([r["id"] for r in rep["required"] if r["status"] != "pass"], ["S12"])
        self.assertEqual(rep["verdict"], "fail")

    def test_i5_names_each_difference(self):
        self.r.all_parity()
        self.r.parity("cli", "pass", items={"a": "pass", "b": "pass", "c": "pass"})
        self.r.baseline(cli_b="fail")
        inv = next(i for i in report.build(self.r.d)["invariants"] if i["id"] == "I5")
        self.assertEqual(inv["status"], "fail")
        self.assertEqual(inv["diffs"], ["cli/b: pass, baseline fail", "cli/c: pass, baseline absent"])

    def test_i5_is_not_run_when_this_run_recorded_the_baseline(self):
        self.r.all_parity()
        self.r.baseline()
        self.r.run["recorded_baseline"] = True
        self.r.save()
        self.assertEqual(required(report.build(self.r.d), "I5")["status"], "not_run")

    def test_verdict_passes_only_when_every_required_item_passed(self):
        self.r.all_parity()
        self.r.scenario("S1", True)
        self.r.scenario("S12", True)
        self.r.baseline()
        self.assertEqual(report.build(self.r.d)["verdict"], "pass")
        self.r.scenario("S1", False)
        rep = report.build(self.r.d)
        self.assertEqual(rep["verdict"], "fail")
        self.assertEqual(required(rep, "S1")["reason"], "c: d")

    def test_baseline_from_lists_suites_that_did_not_run(self):
        self.r.all_parity()
        doc = report.baseline_from(self.r.d)
        self.assertEqual(doc["suites"]["cli"], {"status": "pass", "items": {"a": "pass", "b": "pass"}})
        self.assertEqual(doc["sha"], "a" * 40)
        self.assertEqual(doc["not_recorded"], {})
        self.r.parity("linux-gate", "not_run", reason="held: disk full")
        doc = report.baseline_from(self.r.d)
        self.assertNotIn("linux-gate", doc["suites"])
        self.assertEqual(doc["not_recorded"], {"linux-gate": "held: disk full"})
        for s in SUITES:
            self.r.parity(s, "not_run", reason="x")
        with self.assertRaisesRegex(ValueError, "no parity suite ran"):
            report.baseline_from(self.r.d)

    def test_i5_is_not_run_while_a_suite_is_missing_from_the_baseline(self):
        self.r.all_parity()
        self.r.parity("linux-gate", "not_run", reason="held: disk full")
        self.r.baseline()
        path = os.path.join(self.repo, "baselines", "G0-solo.json")
        with open(path) as fh:
            doc = json.load(fh)
        del doc["suites"]["linux-gate"]
        doc["not_recorded"] = {"linux-gate": "held: disk full"}
        with open(path, "w") as fh:
            json.dump(doc, fh)
        inv = next(i for i in report.build(self.r.d)["invariants"] if i["id"] == "I5")
        self.assertEqual(inv["status"], "not_run")
        self.assertIn("linux-gate: not in the baseline (held: disk full)", inv["reason"])
        self.assertIn("the others match", inv["reason"])

    def test_g2_required_sets_and_judged_invariants(self):
        self.r.run.update(gate="G2", topology="fleet")
        self.r.save()
        with open(os.path.join(self.r.d, "invariants.json"), "w") as fh:
            json.dump([{"id": "I1", "status": "pass", "reason": "ok", "diffs": []},
                       {"id": "I2", "status": "fail", "reason": "1 violation(s)", "diffs": ["m_1: still queued"]},
                       {"id": "I3", "status": "not_run", "reason": "no resume", "diffs": []},
                       {"id": "I4", "status": "pass", "reason": "ok", "diffs": []}], fh)
        self.r.scenario("S3", True)
        rep = report.build(self.r.d)
        self.assertEqual([r["id"] for r in rep["required"] if r["kind"] == "scenario"],
                         ["S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S12"])
        self.assertEqual(required(rep, "S3")["status"], "pass")
        self.assertEqual(required(rep, "S4")["reason"], "no result written")
        self.assertIn("no S12 client", required(rep, "S12")["reason"])
        self.assertEqual([required(rep, i)["status"] for i in ("I1", "I2", "I3", "I4")], ["pass", "fail", "not_run", "pass"])
        self.assertEqual(next(i for i in rep["invariants"] if i["id"] == "I2")["diffs"], ["m_1: still queued"])
        self.r.run.update(topology="solo")
        self.r.save()
        rep = report.build(self.r.d)
        self.assertNotIn("S8", [r["id"] for r in rep["required"]])
        self.assertIn("S8", [s["scenario"] for s in rep["scenarios"]])

    def test_write_produces_the_three_files(self):
        self.r.parity("cli", "fail", items={"a": "pass", "<b&c>": "fail"})
        self.r.scenario("S1", True)
        rep = report.build(self.r.d)
        report.write(self.r.d, rep)
        with open(os.path.join(self.r.d, "report.json")) as fh:
            self.assertEqual(json.load(fh)["verdict"], "fail")
        with open(os.path.join(self.r.d, "invariants.txt")) as fh:
            self.assertEqual(fh.readline(), "I1 n/a  I2 n/a  I3 n/a  I4 n/a  I5 NOT-RUN\n")
        with open(os.path.join(self.r.d, "report.html")) as fh:
            page = fh.read()
        self.assertIn("Gate NOT PASSED", page)
        self.assertIn("&lt;b&amp;c&gt;", page)
        self.assertNotIn("<b&c>", page)
        self.assertIn("NOT RUN", page)
        self.assertIn("asciinema is not installed", page)


if __name__ == "__main__":
    unittest.main()
