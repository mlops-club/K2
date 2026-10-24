"""g1_solo.py's flow logic on hand-written traces: the solo view of a flow,
per-scenario scoping, and the crash flow's order. No daemon needed.

    python3 -I -B tests/lab/scenarios/test_g1_solo.py
"""

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.dirname(HERE))

import g1_solo  # noqa: E402
from labkit import flows  # noqa: E402

FLOWS_DIR = os.path.join(HERE, "..", "..", "..", "docs", "prd", "flows")


def ev(t, event, src="k2-daemon", agent=None, session=None, **attrs):
    e = {"ts": "2026-10-24T12:00:%06.3fZ" % t, "event": event, "src": src, "attrs": attrs}
    if agent is not None:
        e["agent_id"] = agent
    if session is not None:
        e["session_id"] = session
    return e


def fake(t, event, session, **attrs):
    return ev(t, event, src="k2-fake-agent", session=session, **attrs)


def solo_spawn(agent, sess, t0=0.0):
    """What today's daemon and the fake agent write for one spawn."""
    return [
        ev(t0 + 0.001, "api.spawn_request", agent=agent, need=False),
        ev(t0 + 0.002, "host.spawn", agent=agent, session=sess, resume=False),
        ev(t0 + 0.003, "ptyd.open_session", agent=agent, session=sess),
        fake(t0 + 0.010, "cli.exec", sess, resumed=False),
        fake(t0 + 0.011, "cli.hook", sess, hook="SessionStart"),
        ev(t0 + 0.012, "cell.hook_received", agent=agent, session=sess, hook="SessionStart"),
        ev(t0 + 0.013, "cell.owner_walk", agent=agent, session=sess, hook="SessionStart"),
        ev(t0 + 0.014, "cell.owner_verdict", agent=agent, session=sess, hook="SessionStart"),
        ev(t0 + 0.015, "cell.event", agent=agent, session=sess, hook="SessionStart"),
        ev(t0 + 0.016, "host.agent_event", agent=agent, session=sess, hook="SessionStart"),
        fake(t0 + 0.017, "cli.hook.done", sess, hook="SessionStart", exit=0),
        ev(t0 + 0.020, "client.stream", agent=agent, session=sess),
    ]


def solo_crash(agent, sess):
    return solo_spawn(agent, sess) + [
        fake(1.000, "cli.turn.start", sess, turn=1),
        ev(1.100, "ptyd.child_exit", agent=agent, session=sess, signal=9, clean=False, code=None),
        ev(1.101, "host.agent_exit", agent=agent, session=sess),
        ev(2.000, "api.spawn_request", agent=agent, need=True),
        ev(2.001, "host.spawn", agent=agent, session=sess, resume=True),
        fake(2.010, "cli.exec", sess, resumed=True),
        fake(2.011, "cli.hook", sess, hook="SessionStart"),
        ev(2.012, "host.agent_event", agent=agent, session=sess, hook="SessionStart"),
    ]


class G1Checks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.flows = flows.load_flows(FLOWS_DIR)

    def g1(self, events, s1=("lab-s1-a", "s-1"), s7=("tab-x", "s-7")):
        scen = {"S1": (s1[0], {s1[1]}), "S7": (s7[0], {s7[1]})}
        return {r["flow"]: r for r in g1_solo.check_g1(self.flows, sorted(events, key=lambda e: e["ts"]), scen)}

    def test_solo_view_drops_placement_and_renumbers(self):
        v = g1_solo.solo_view(self.flows["spawn"])
        self.assertEqual([s["event"] for s in v["steps"]][:2], ["api.spawn_request", "host.spawn"])
        self.assertEqual([s["n"] for s in v["steps"]], list(range(1, 8)))
        flows.validate(v)

    def test_a_full_solo_run_passes_all_three(self):
        r = self.g1(solo_spawn("lab-s1-a", "s-1") + solo_crash("tab-x", "s-7"))
        self.assertEqual({k: v["status"] for k, v in r.items()}, {"spawn": "pass", "hook": "pass", "crash": "pass"})
        step2 = r["spawn"]["steps"][1]
        self.assertEqual((step2["n"], step2["observed_ts"]), (2, None))
        self.assertIn("P2", step2["not_in_solo"])
        self.assertIsNone(r["spawn"]["first_missing"])

    def test_a_clean_exit_is_not_a_crash(self):
        events = [e for e in solo_crash("tab-x", "s-7") if e["event"] != "ptyd.child_exit"]
        events.append(ev(1.100, "ptyd.child_exit", agent="tab-x", session="s-7", signal=None, clean=True, code=0))
        r = self.g1(solo_spawn("lab-s1-a", "s-1") + events)
        self.assertEqual((r["crash"]["status"], r["crash"]["first_missing"]), ("fail", 5))

    def test_a_fresh_start_after_the_crash_is_not_a_resume(self):
        events = [e for e in solo_crash("tab-x", "s-7")
                  if not (e["event"] == "cli.exec" and e["attrs"].get("resumed"))]
        events.append(fake(2.010, "cli.exec", "s-7", resumed=False))
        r = self.g1(solo_spawn("lab-s1-a", "s-1") + events)
        self.assertEqual((r["crash"]["status"], r["crash"]["first_missing"]), ("fail", 9))

    def test_another_agents_events_never_complete_a_flow(self):
        # S1's daemon events are missing; another agent's complete set and
        # unattributed daemon events must not stand in for them.
        own = [e for e in solo_spawn("lab-s1-a", "s-1") if e["src"] != "k2-daemon"]
        other = solo_spawn("someone-else", "s-other")
        stray = [dict(e, agent_id=None) for e in solo_spawn("lab-s1-a", "s-1") if e["src"] == "k2-daemon"]
        r = self.g1(own + other + stray + solo_crash("tab-x", "s-7"))
        # No daemon event of its own: "expected_only", which G1 never counts.
        self.assertEqual(r["spawn"]["status"], "expected_only")
        self.assertEqual(r["hook"]["status"], "expected_only")
        self.assertEqual(r["crash"]["status"], "pass")

    def test_hook_before_the_cli_posts_it_fails(self):
        events = solo_spawn("lab-s1-a", "s-1")
        for e in events:
            if e["event"] == "cli.hook":
                e["ts"] = "2026-10-24T12:00:00.019Z"  # after the daemon handled it
        r = self.g1(events + solo_crash("tab-x", "s-7"))
        self.assertEqual(r["hook"]["status"], "fail")


if __name__ == "__main__":
    unittest.main()
