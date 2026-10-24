"""The flow checker against hand-written traces, and the flow files
against the PRD. Run: tests/lab/run_tests.sh"""

import copy
import json
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import extract_flows  # noqa: E402
from labkit import flows  # noqa: E402

FLOWS_DIR = os.path.join(extract_flows.REPO, "docs", "prd", "flows")


def ev(ts, event, src="k2-daemon", **kw):
    e = {"ts": "2026-11-14T09:12:%06.3fZ" % ts, "event": event, "src": src}
    e.update(kw)
    return e


def spawn_trace(agent="a1", t0=0.0, skip=None, swap=None):
    """A complete spawn flow for `agent`, as wave 2 would emit it."""
    names = ["api.spawn_request", "placement.choose", "host.spawn", "ptyd.open_session",
             "cli.exec", "cli.hook", "host.agent_event", "client.stream"]
    out = []
    for i, name in enumerate(names):
        if skip == i + 1:
            continue
        kw = {"agent_id": agent}
        src = "k2-daemon"
        if name.startswith("cli."):
            kw = {}  # the CLI knows no agent id
            src = "k2-fake-agent"
        if name == "cli.hook":
            kw["attrs"] = {"hook": "SessionStart"}
        out.append(ev(t0 + i, name, src=src, **kw))
    if swap:
        a, b = swap
        out[a]["ts"], out[b]["ts"] = out[b]["ts"], out[a]["ts"]
    return out


class FlowFiles(unittest.TestCase):
    def test_one_file_per_prd_flow_and_in_sync(self):
        loaded = flows.load_flows(FLOWS_DIR)
        self.assertEqual(sorted(loaded), sorted(extract_flows.FLOW_IDS))
        self.assertEqual(len(loaded), 11)
        for fid, text in extract_flows.expected_files().items():
            with open(os.path.join(FLOWS_DIR, fid + ".json"), encoding="utf-8") as fh:
                self.assertEqual(fh.read(), text, "%s.json is stale: run tests/lab/extract_flows.py" % fid)

    def test_steps_keep_the_prd_labels_in_order(self):
        spawn = flows.load_flows(FLOWS_DIR)["spawn"]
        self.assertEqual([s["label"] for s in spawn["steps"]][:3],
                         ["POST /cli/sessions/v2/spawn", "placement.choose(agent)",
                          "Spawn{agent, harness, resume_id, epoch}"])
        self.assertEqual(spawn["proof"], ["S1", "S2"])
        self.assertEqual(spawn["steps"][5]["match"], {"hook": "SessionStart"})

    def test_js_literal_parser(self):
        v, _ = extract_flows.js_literal('{ a: "x\\"y", b: [1, -2.5, true, null,], "c d": {e: "f"}, }', 0)
        self.assertEqual(v, {"a": 'x"y', "b": [1, -2.5, True, None], "c d": {"e": "f"}})

    def test_validate_rejects_bad_shapes(self):
        good = flows.load_flows(FLOWS_DIR)["hook"]
        flows.validate(copy.deepcopy(good))
        for mutate, msg in [
            (lambda f: f["steps"][0].update(to="nobody"), "unknown participant"),
            (lambda f: f["steps"][1].update(n=5), "numbered"),
            (lambda f: f["steps"][0].update(kind="shout"), "kind"),
            (lambda f: f["steps"][0].update(event="NoDot"), "event"),
            (lambda f: f.pop("correlate"), "correlate"),
            (lambda f: f["participants"].append(dict(f["participants"][0])), "duplicate"),
        ]:
            bad = copy.deepcopy(good)
            mutate(bad)
            with self.assertRaisesRegex(flows.FlowError, msg):
                flows.validate(bad)


class Checker(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.f = flows.load_flows(FLOWS_DIR)

    def test_no_trace_is_expected_only(self):
        r = flows.check(self.f["spawn"], None)
        self.assertEqual(r["status"], "expected_only")
        self.assertEqual(len(r["steps"]), 8)
        self.assertTrue(all(s["observed_ts"] is None for s in r["steps"]))

    def test_only_lab_and_fake_agent_events_is_expected_only(self):
        trace = [e for e in spawn_trace() if e["src"] == "k2-fake-agent"]
        r = flows.check(self.f["spawn"], trace)
        self.assertEqual(r["status"], "expected_only")
        # What was seen is still shown (step 5, cli.exec) for reading.
        self.assertIsNotNone(r["steps"][4]["observed_ts"])

    def test_complete_trace_passes(self):
        r = flows.check(self.f["spawn"], spawn_trace())
        self.assertEqual(r["status"], "pass")
        self.assertEqual(r["instance"], "a1")
        self.assertIsNone(r["first_missing"])
        self.assertTrue(all(s["observed_ts"] for s in r["steps"]))

    def test_missing_step_fails_and_names_it(self):
        r = flows.check(self.f["spawn"], spawn_trace(skip=3))
        self.assertEqual(r["status"], "fail")
        self.assertEqual(r["first_missing"], 3)

    def test_out_of_order_fails(self):
        r = flows.check(self.f["spawn"], spawn_trace(swap=(1, 2)))
        self.assertEqual(r["status"], "fail")

    def test_events_are_ordered_by_ts_not_file_order(self):
        trace = spawn_trace()
        r = flows.check(self.f["spawn"], list(reversed(trace)))
        self.assertEqual(r["status"], "pass")

    def test_match_fields_are_required(self):
        trace = spawn_trace()
        for e in trace:
            if e["event"] == "cli.hook":
                e["attrs"] = {"hook": "Stop"}
        r = flows.check(self.f["spawn"], trace)
        self.assertEqual((r["status"], r["first_missing"]), ("fail", 6))

    def test_correlation_keeps_agents_apart(self):
        # Agent a2's spawn interleaved with a1's, a1 missing a hop: a2's
        # events must not fill a1's gap; a2 alone passes.
        a1 = spawn_trace("a1", t0=0.0, skip=3)
        a2 = spawn_trace("a2", t0=0.5)
        r = flows.check(self.f["spawn"], a1 + a2)
        self.assertEqual((r["status"], r["instance"]), ("pass", "a2"))
        r = flows.check(self.f["spawn"], a1 + [e for e in a2 if e.get("agent_id") != "a2" or e["event"] == "host.spawn"])
        self.assertEqual(r["status"], "fail")

    def test_uncorrelated_flow_uses_the_whole_trace(self):
        names = [s["event"] for s in self.f["drain"]["steps"]]
        trace = [ev(i, n, src="kubernetes") for i, n in enumerate(names)]
        self.assertEqual(flows.check(self.f["drain"], trace)["status"], "pass")
        self.assertEqual(flows.check(self.f["drain"], trace[:-1])["first_missing"], 5)

    def test_read_trace_fails_loudly_on_bad_lines(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "t.jsonl")
            with open(p, "w") as fh:
                fh.write(json.dumps(ev(1, "host.spawn")) + "\n\n")
            self.assertEqual(len(flows.read_trace(p)), 1)
            for bad in ["not json\n", '{"event": "x.y"}\n', "[1]\n"]:
                with open(p, "w") as fh:
                    fh.write(bad)
                with self.assertRaises(flows.FlowError):
                    flows.read_trace(p)


if __name__ == "__main__":
    unittest.main()
