"""Gate G2's checkers against hand-written evidence: invariants I1 to I4,
the chaos schedule, the fleet plan. Run: tests/lab/run_tests.sh"""

import json
import os
import sys
import tempfile
import unittest
import unittest.mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from labkit import chaos, fleet, g2, invariants  # noqa: E402


def ts(s):
    return "2026-11-14T09:12:%06.3fZ" % s


class Run:
    """A fake G2 run dir."""

    def __init__(self, d):
        self.d = d
        for sub in ("traces", "scenarios", "outbox-final"):
            os.makedirs(os.path.join(d, sub), exist_ok=True)

    def _append(self, rel, *events):
        with open(os.path.join(self.d, rel), "a") as fh:
            for e in events:
                fh.write(json.dumps(e) + "\n")

    def fake(self, host, t, event, pid, session="s1", **attrs):
        self._append("traces/%s.fake.jsonl" % host, {"ts": ts(t), "event": event, "src": "k2-fake-agent", "host": host,
                                                     "pid": pid, "pane": "p%s" % pid, "session_id": session, "attrs": attrs})

    def cli(self, host, pid, start, end=None, cwd="/w/a", session="s1", resumed=False, mem0="absent", mem1="absent",
            turn_at=None):
        self.fake(host, start, "cli.exec", pid, session, cwd=cwd, resumed=resumed, memory_sha256=mem0)
        if turn_at is not None:
            self.fake(host, turn_at, "cli.turn.end", pid, session, turn=1, memory_sha256=mem1)
        if end is not None:
            self.fake(host, end, "cli.exit", pid, session, code=0, memory_sha256=mem1)

    def daemon(self, proc, t, event, **kw):
        e = {"ts": ts(t), "event": event, "src": "k2-daemon"}
        e.update(kw)
        self._append("traces/%s.daemon.jsonl" % proc, e)

    def sample(self, t, clis):
        self._append("samples.jsonl", {"ts": ts(t), "procs": {}, "clis": clis})

    def ledger(self, **m):
        self._append("scenarios/messages.jsonl", m)

    def outbox(self, proc, hop, rows):
        with open(os.path.join(self.d, "outbox-final", proc + ".json"), "w") as fh:
            json.dump({"hop": hop, "counts": {}, "messages": rows}, fh)


def by_id(results):
    return {r["id"]: r for r in results}


class Invariants(unittest.TestCase):
    def setUp(self):
        self._t = tempfile.TemporaryDirectory()
        self.addCleanup(self._t.cleanup)
        self.r = Run(self._t.name)

    def check(self, layout="fleet"):
        return by_id(invariants.check_all(self.r.d, layout))

    def test_nothing_recorded_is_not_run(self):
        res = self.check()
        self.assertEqual({k: v["status"] for k, v in res.items()},
                         {"I1": "not_run", "I2": "not_run", "I3": "not_run", "I4": "not_run"})

    def test_i1_two_live_clis_for_one_agent_fail(self):
        self.r.cli("host-1", 100, 1.0, cwd="/w/a")
        self.r.cli("host-2", 200, 2.0, cwd="/w/a")
        self.r.sample(1.5, [100])
        self.r.sample(2.5, [100, 200])
        res = self.check()["I1"]
        self.assertEqual(res["status"], "fail")
        self.assertIn("2 live CLIs for /w/a", res["diffs"][0])

    def test_helper_clis_without_a_pane_are_not_agents(self):
        self.r.cli("host-1", 100, 1.0, cwd="/w/a")
        self.r._append("traces/host-2.fake.jsonl", {"ts": ts(1.2), "event": "cli.exec", "host": "host-2", "pid": 300,
                                                    "pane": None, "session_id": "x",
                                                    "attrs": {"cwd": "/w/a", "argv": ["app-server"]}})
        self.r.sample(1.5, [100, 300])
        self.assertEqual(self.check()["I1"]["status"], "pass")

    def test_i1_a_killed_cli_then_a_new_one_passes(self):
        self.r.cli("host-1", 100, 1.0, cwd="/w/a")  # killed: no cli.exit
        self.r.sample(1.5, [100])
        self.r.sample(2.0, [])
        self.r.cli("host-2", 200, 3.0, cwd="/w/a", resumed=True)
        self.r.sample(3.5, [200])
        self.r.daemon("controller", 1.0, "ctl.place", agent_id="a", attrs={"host": "host-1", "epoch": 7})
        self.r.daemon("controller", 3.0, "ctl.place", agent_id="a", attrs={"host": "host-2", "epoch": 8})
        self.assertEqual(self.check()["I1"]["status"], "pass")

    def test_i1_the_same_placement_traced_again_is_not_a_new_one(self):
        self.r.cli("host-1", 100, 1.0)
        self.r.sample(1.5, [100])
        self.r.daemon("controller", 1.0, "ctl.place", agent_id="a", attrs={"host": "host-1", "epoch": 8})
        self.r.daemon("controller", 2.0, "ctl.place", agent_id="a", attrs={"host": "host-1", "epoch": 8})
        self.r.daemon("controller", 3.0, "ctl.place", agent_id="a", attrs={"host": "host-1", "epoch": 9})
        self.assertEqual(self.check()["I1"]["status"], "pass")

    def test_i1_an_epoch_reissued_after_a_restart_fails(self):
        self.r.cli("host-1", 100, 1.0)
        self.r.sample(1.5, [100])
        self.r.daemon("controller", 1.0, "ctl.place", agent_id="a", attrs={"host": "host-1", "epoch": 8})
        self.r.daemon("controller", 5.0, "ctl.place", agent_id="b", attrs={"host": "host-1", "epoch": 8})
        res = self.check()["I1"]
        self.assertEqual(res["status"], "fail")
        self.assertIn("epoch 8 for b", res["diffs"][0])

    def test_i2_fleet_every_row_final_passes(self):
        self.r.ledger(scenario="S3", msg_id="m_1", to="bob", http=200, response={"success": True, "msgId": "m_1"})
        self.r.outbox("controller", "down", [{"msg_id": "m_1", "state": "delivered"}])
        self.r.outbox("host-1", "up", [{"msg_id": "m_1", "state": "delivered"}])
        self.assertEqual(self.check()["I2"]["status"], "pass")

    def test_i2_fleet_queued_vanished_and_silent_failures_fail(self):
        self.r.ledger(scenario="S3", msg_id="m_1", to="bob", http=200, response={"success": True})
        self.r.ledger(scenario="S3", msg_id="m_2", to="bob", http=200, response={"success": True})
        self.r.ledger(scenario="S3", msg_id="m_3", to="bob", http=200, response={"success": True})
        self.r.ledger(scenario="S7", to="/w/x", http=200, response={"success": True})
        self.r.outbox("controller", "down", [{"msg_id": "m_1", "state": "queued", "attempts": 0},
                                             {"msg_id": "m_3", "state": "failed", "reason": ""}])
        diffs = "\n".join(self.check()["I2"]["diffs"])
        self.assertIn("m_1: controller hop down still queued", diffs)
        self.assertIn("m_2: in no outbox at the end (vanished)", diffs)
        self.assertIn("m_3: controller hop down failed with no reason", diffs)
        self.assertIn("accepted without a msg id", diffs)

    def test_i2_a_direct_delivery_the_controller_traced_is_final(self):
        self.r.ledger(scenario="S3", msg_id="m_9", to="bob", http=200, response={"success": True})
        self.r.outbox("host-1", "up", [{"msg_id": "m_9", "state": "delivered"}])
        self.r.outbox("controller", "down", [])
        self.assertEqual(self.check()["I2"]["status"], "fail", "no row and no trace: vanished")
        self.r.daemon("controller", 1.0, "ctl.msg_accept", msg_id="m_9", attrs={"status": "delivered", "host": "host-1"})
        self.assertEqual(self.check()["I2"]["status"], "pass")

    def test_i2_solo_needs_an_answer_for_each_message(self):
        self.r.ledger(scenario="S3", to="bob", http=200, response={"success": True})
        self.r.ledger(scenario="S7", to="x", http=200, response={"success": False, "reason": "dormant_no_wake"})
        self.assertEqual(self.check("solo")["I2"]["status"], "pass")
        self.r.ledger(scenario="S6", kind="inbox", to="bob", http=200, response={"id": "s6-item", "filename": "s6-item.md"})
        self.assertEqual(self.check("solo")["I2"]["status"], "pass")
        self.r.ledger(scenario="S7", to="y", http=200, response={"success": False})
        self.assertEqual(self.check("solo")["I2"]["status"], "fail")

    def test_i3_resume_keeps_session_and_memory(self):
        self.r.cli("host-1", 100, 1.0, end=2.0, session="s", mem0="absent", mem1="h1", turn_at=1.5)
        self.r.cli("host-1", 101, 3.0, end=4.0, session="s", resumed=True, mem0="h1", mem1="h1")
        self.assertEqual(self.check()["I3"]["status"], "pass")

    def test_i3_memory_changed_or_session_changed_fails(self):
        self.r.cli("host-1", 100, 1.0, session="s", mem1="h1", turn_at=1.5)  # killed after a turn
        self.r.cli("host-2", 200, 3.0, session="s", resumed=True, mem0="absent")
        diffs = self.check()["I3"]["diffs"]
        self.assertEqual(len(diffs), 1)
        self.assertIn("memory changed across the resume: h1", diffs[0])
        self.r.cli("host-2", 201, 5.0, session="other", resumed=True, mem0="absent")
        self.assertTrue(any("resumed session other, its previous CLI had s" in d for d in self.check()["I3"]["diffs"]))

    def test_i4_writes_from_two_hosts_at_once_fail(self):
        self.r.cli("host-1", 100, 1.0, end=9.0, cwd="/w/a", turn_at=5.0)
        self.r.cli("host-2", 200, 2.0, end=3.0, cwd="/w/a", turn_at=2.5)
        res = self.check()["I4"]
        self.assertEqual(res["status"], "fail")
        self.assertIn("written by host-2 while pid 100 ran it on host-1", res["diffs"][0])

    def test_i4_one_host_after_the_other_passes(self):
        self.r.cli("host-1", 100, 1.0, end=2.0, cwd="/w/a", turn_at=1.5)
        self.r.cli("host-2", 200, 3.0, end=4.0, cwd="/w/a", turn_at=3.5)
        self.r.daemon("host-2", 3.7, "inbox.write", attrs={"cwd": "/w/a"})
        res = self.check()["I4"]
        self.assertEqual(res["status"], "pass", res)


class Chaos(unittest.TestCase):
    def test_schedule_is_a_function_of_the_seed(self):
        self.assertEqual(chaos.schedule(4117, 20), chaos.schedule(4117, 20))
        self.assertNotEqual(chaos.schedule(4117, 20), chaos.schedule(4118, 20))
        for wait, kroll, pick, down in chaos.schedule(1, 50):
            self.assertTrue(chaos.WAIT_S[0] <= wait <= chaos.WAIT_S[1])
            self.assertTrue(chaos.DOWN_S[0] <= down <= chaos.DOWN_S[1])

    def test_kinds_follow_the_weights(self):
        self.assertEqual([chaos.kind_of("fleet", r) for r in (0.0, 0.49, 0.5, 0.79, 0.8, 0.99)],
                         ["cli", "cli", "host", "host", "controller", "controller"])
        self.assertEqual([chaos.kind_of("solo", r) for r in (0.69, 0.7)], ["cli", "controller"])


class WaitingForTheDaemon(unittest.TestCase):
    def test_only_a_daemon_that_is_not_there_is_waited_for(self):
        self.assertTrue(g2.daemon_down(0, "request failed: [Errno 61] Connection refused"))
        self.assertTrue(g2.daemon_down(503, '{"state":"migrating","error":"daemon is completing first-boot migrations"}'))
        self.assertTrue(g2.daemon_down(503, '{"error":"no agent host is connected to this controller"}'))
        self.assertFalse(g2.daemon_down(503, '{"error":"something else"}'))
        self.assertFalse(g2.daemon_down(500, "boom"))

    def test_a_dropped_request_is_sent_again_only_when_that_is_safe(self):
        dropped = "request failed: Remote end closed connection without response"
        self.assertTrue(g2.daemon_down(0, dropped, "POST", "/cli/projects/add-from-path"))
        self.assertTrue(g2.daemon_down(0, dropped, "GET", "/cli/anything"))
        self.assertFalse(g2.daemon_down(0, dropped, "POST", "/cli/workspace/msg"), "k2 msg could be sent twice")
        self.assertFalse(g2.daemon_down(0, dropped, "POST", "/cli/inbox/compose"))

    def test_a_host_not_linked_back_yet_is_waited_for_on_idempotent_routes_only(self):
        lost = '{"error":"agent host host-1 is not connected (lost: its agents keep their placement there until it links again)"}'
        self.assertTrue(g2.daemon_down(502, lost, "POST", "/cli/sessions/v2/close"))
        self.assertFalse(g2.daemon_down(502, lost, "POST", "/cli/sessions/v2/spawn"), "S8 crash expects this 502")
        self.assertFalse(g2.daemon_down(400, '{"error":"spawn failed: no agent host is connected"}', "POST",
                                        "/cli/workspace/ensure-pinned-chat"), "a 400 is the daemon's answer, not a wait")


class ChaosKilled(unittest.TestCase):
    def test_only_a_cli_chaos_itself_killed_counts(self):
        with tempfile.TemporaryDirectory() as d:
            log = os.path.join(d, "chaos.log")
            with open(log, "w") as fh:
                fh.write(json.dumps({"ts": "t1", "kind": "cli", "host": "host-2", "pid": 3302, "action": "SIGKILL"}) + "\n")
                fh.write(json.dumps({"ts": "t2", "kind": "host", "action": "skipped"}) + "\n")
            env = {"LAB_CHAOS": "seed=7", "LAB_CHAOS_LOG": log}
            with unittest.mock.patch.dict(os.environ, env):
                self.assertEqual(g2.chaos_killed("host-2", 3302)["ts"], "t1")
                self.assertIsNone(g2.chaos_killed("host-1", 3302), "same pid on another host")
                self.assertIsNone(g2.chaos_killed("host-2", 9999))
            with unittest.mock.patch.dict(os.environ, {"LAB_CHAOS_LOG": log}):
                os.environ.pop("LAB_CHAOS", None)
                self.assertIsNone(g2.chaos_killed("host-2", 3302), "no chaos in this run")

    def test_chaos_hit_counts_the_agents_cli_or_its_host_since_a_time(self):
        with tempfile.TemporaryDirectory() as d:
            log = os.path.join(d, "chaos.log")
            with open(log, "w") as fh:
                for e in ({"ts": "2026-01-01T00:00:01Z", "kind": "cli", "host": "host-1", "pid": 7, "action": "SIGKILL"},
                          {"ts": "2026-01-01T00:00:02Z", "kind": "host", "target": "host-2", "pid": 9, "action": "SIGKILL"},
                          {"ts": "2026-01-01T00:00:03Z", "kind": "controller", "target": "controller", "action": "SIGKILL"},
                          {"ts": "2026-01-01T00:00:04Z", "kind": "host", "target": "host-2", "action": "started"}):
                    fh.write(json.dumps(e) + "\n")
            with unittest.mock.patch.dict(os.environ, {"LAB_CHAOS": "seed=1", "LAB_CHAOS_LOG": log}):
                self.assertEqual(len(g2.chaos_hit("host-1", 7, "2026-01-01T00:00:00Z")), 1)
                self.assertEqual(g2.chaos_hit("host-1", 7, "2026-01-01T00:00:02Z"), [], "before the attempt")
                self.assertEqual([e["kind"] for e in g2.chaos_hit("host-2", 5, "2026")], ["host"])
                self.assertEqual(g2.chaos_hit("controller", None, "2026"), [], "a controller restart is S9's own test")
                self.assertEqual([e["kind"] for e in g2.chaos_hit("controller", None, "2026", kinds=("controller",))],
                                 ["controller"], "asked for explicitly: a spawn lost in flight")


class Plan(unittest.TestCase):
    def test_fleet_plan_follows_the_env_contract(self):
        st = fleet.plan("fleet", "/r", "/run", "/bin/d", "/bin/f")
        self.assertEqual(sorted(st["procs"]), ["controller", "host-1", "host-2"])
        ctl, h2 = st["procs"]["controller"]["env"], st["procs"]["host-2"]["env"]
        self.assertEqual((ctl["K2_ROLE"], ctl["K2_HOSTS"]), ("controller", "1"))
        self.assertEqual(h2["K2_ROLE"], "host")
        self.assertEqual(h2["K2_CONTROLLER_URL"], "ws://127.0.0.1:%d" % st["procs"]["controller"]["port"])
        self.assertEqual(h2["K2_HOST_ENROLL_TOKEN_FILE"], ctl["K2_HOST_BOOTSTRAP_TOKEN_FILE"])
        self.assertEqual((h2["K2_HOST_NAME"], h2["K2_FAKE_AGENT_HOST"]), ("host-2", "host-2"))
        self.assertNotIn("K2_TEST_AGENT_SHIM_DIR", ctl, "the controller runs no agents")
        self.assertEqual(h2["K2_TRACE_FILE"], "/run/traces/host-2.daemon.jsonl")
        self.assertEqual(len({p["port"] for p in st["procs"].values()}), 3)
        self.assertEqual(len({p["home"] for p in st["procs"].values()}), 3)

    def test_ptyd_hosts_get_k2_ptyd_and_its_binary_the_controller_does_not(self):
        with unittest.mock.patch.dict(os.environ, {"LAB_HOST_PTYD": "1", "LAB_PTYD_BIN": "/b/k2-ptyd"}):
            st = fleet.plan("fleet", "/r", "/run", "/bin/d", "/bin/f")
        h1, ctl = st["procs"]["host-1"]["env"], st["procs"]["controller"]["env"]
        self.assertEqual((h1["K2_PTYD"], h1["K2_PTYD_BIN"]), ("1", "/b/k2-ptyd"))
        self.assertNotIn("K2_PTYD", ctl)
        self.assertTrue(st["host_ptyd"])
        with unittest.mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("LAB_HOST_PTYD", None)
            self.assertNotIn("K2_PTYD", fleet.plan("fleet", "/r", "/run", "/bin/d", "/bin/f")["procs"]["host-1"]["env"])

    def test_home_sync_sets_k2_work_root_on_hosts_and_defaults_to_resume_elsewhere(self):
        for env, on in (({"LAB_HOME_SYNC": "1"}, True), ({"LAB_S8_RESUME_ELSEWHERE": "1"}, True),
                        ({"LAB_S8_RESUME_ELSEWHERE": "1", "LAB_HOME_SYNC": "0"}, False), ({}, False)):
            with unittest.mock.patch.dict(os.environ, env):
                for k in ("LAB_HOME_SYNC", "LAB_S8_RESUME_ELSEWHERE"):
                    if k not in env:
                        os.environ.pop(k, None)
                st = fleet.plan("fleet", "/r", "/run", "/bin/d", "/bin/f")
            roots = {n: p["env"].get("K2_WORK_ROOT") for n, p in st["procs"].items()}
            if on:
                self.assertEqual(roots, {"controller": None, "host-1": "/r/work", "host-2": "/r/work"}, env)
            else:
                self.assertEqual(set(roots.values()), {None}, env)

    def test_exec_of_is_the_newest_cli_exec_by_ts_across_hosts(self):
        c = g2.Client.__new__(g2.Client)
        evs = [{"event": "cli.exec", "session_id": "s", "host": "host-1", "ts": "2026-10-26T00:00:09.000Z"},
               {"event": "cli.exec", "session_id": "s", "host": "host-2", "ts": "2026-10-26T00:00:05.000Z"}]
        c.fleet = unittest.mock.Mock(fake_events=lambda event=None: [e for e in evs if event in (None, e["event"])])
        self.assertEqual(c.exec_of("s")["host"], "host-1")
        self.assertIsNone(c.exec_of("other"))

    def test_a_hop_up_accept_is_final_only_for_a_direct_delivery(self):
        acc = {"event": "ctl.msg_accept", "msg_id": "m", "attrs": {"status": "delivered"}}
        dlv = {"event": "ctl.deliver", "msg_id": "m", "attrs": {"host": "host-1"}}
        bad = {"event": "delivery.failed", "msg_id": "m", "attrs": {"reason": "recipient_gone: x"}}
        c = g2.Client.__new__(g2.Client)
        for evs, row, resp_ok, want in (
                ([acc], None, True, "delivered"),          # direct: no row, never sent down
                ([acc], "queued", True, "queued"),         # queued: the row decides
                ([acc, dlv], None, True, None),            # sent down: not direct
                ([acc], None, False, None),                # controller down: can't tell
                ([acc, dlv, bad], "sent", True, "failed"),
                ([acc], "delivered", True, "delivered")):
            c.fleet = unittest.mock.Mock(st={"controller": "controller"}, daemon_events=lambda proc, event=None, evs=evs: evs)
            resp = ({"message": {"state": row}} if row else {"message": None}) if resp_ok else None
            c.outbox = lambda proc=None, msg_id=None, resp=resp: resp
            self.assertEqual(c.final_state("m"), want, (evs, row, resp_ok))

    def test_host_move_after_reaches_the_controller_only_and_is_20_s_under_s10(self):
        for env, want in (({}, ""), ({"LAB_S10": "1"}, "20"), ({"LAB_S10": "1", "LAB_HOST_MOVE_AFTER": "7"}, "7"),
                          ({"LAB_HOST_MOVE_AFTER": "9"}, "9")):
            with unittest.mock.patch.dict(os.environ, env):
                for k in ("LAB_S10", "LAB_HOST_MOVE_AFTER"):
                    if k not in env:
                        os.environ.pop(k, None)
                st = fleet.plan("fleet", "/r", "/run", "/bin/d", "/bin/f")
                self.assertEqual(g2.move_after_s(), int(want) if want else None)
            self.assertEqual(st["procs"]["controller"]["env"]["K2_HOST_MOVE_AFTER"], want, env)
            self.assertNotIn("K2_HOST_MOVE_AFTER", st["procs"]["host-1"]["env"])

    def test_open_chat_activates_a_registered_workspace_first_as_the_app_does(self):
        c = g2.Client.__new__(g2.Client)
        c.projects, calls = {"/w/a": "p1"}, []
        c.http = lambda method, path, body=None, **kw: calls.append((path, body)) or (500, "x", {})
        c.log = lambda msg: None
        sc = unittest.mock.Mock(check=lambda name, ok, detail="": ok)
        self.assertIsNone(g2.open_chat(c, sc, "/w/a", "opens"))
        self.assertEqual(calls[0], ("/cli/projects/activate", {"projectId": "p1"}))
        self.assertEqual(calls[1][0], "/cli/workspace/ensure-pinned-chat")
        calls.clear()
        g2.open_chat(c, sc, "/w/unregistered", "opens")
        self.assertEqual([p for p, _ in calls], ["/cli/workspace/ensure-pinned-chat"])

    def test_s10_is_fleet_only_and_opt_in(self):
        self.assertIn("S10", g2.FLEET_ONLY)
        self.assertIn("S10", g2.SCENARIOS)
        self.assertIn("S10", g2.TITLES)

    def test_gone_grace_reaches_every_daemon_only_when_the_lab_sets_it(self):
        with unittest.mock.patch.dict(os.environ, {"LAB_GONE_GRACE_S": "30"}):
            st = fleet.plan("fleet", "/r", "/run", "/bin/d", "/bin/f")
            self.assertEqual({p["env"]["K2_DELIVERY_GONE_GRACE_SECS"] for p in st["procs"].values()}, {"30"})
            self.assertEqual(g2.gone_wait_s(), 60)
        with unittest.mock.patch.dict(os.environ, {}):
            os.environ.pop("LAB_GONE_GRACE_S", None)
            self.assertEqual(g2.gone_wait_s(), 150)

    def test_one_process_never_writes_back_anothers_stale_pids(self):
        with tempfile.TemporaryDirectory() as d:
            st = fleet.plan("fleet", d, d, "/bin/d", "/bin/f")
            st["_path"] = os.path.join(d, "fleet.json")
            fleet.save(st)
            a, b = fleet.load(st["_path"]), fleet.load(st["_path"])  # two lab processes
            fleet.update_proc(a, "host-1", pid=111)
            fleet.update_proc(b, "controller", pid=222)  # b never saw host-1's new pid
            now = fleet.load(st["_path"])["procs"]
            self.assertEqual((now["host-1"]["pid"], now["controller"]["pid"]), (111, 222))

    def test_a_pid_that_is_not_this_runs_daemon_is_never_ours(self):
        st = fleet.plan("fleet", "/r", "/run", "/no/such/k2-daemon", "/bin/f")
        self.assertFalse(fleet.ours(st, os.getpid()), "this python process is alive but not the daemon")
        self.assertFalse(fleet.ours(st, None))

    def test_solo_plan_is_one_daemon_that_is_controller_and_host(self):
        st = fleet.plan("solo", "/r", "/run", "/bin/d", "/bin/f")
        self.assertEqual(list(st["procs"]), ["solo"])
        self.assertEqual(st["controller"], "solo")
        self.assertEqual([h["name"] for h in st["hosts"]], ["local"])
        self.assertNotIn("K2_ROLE", st["procs"]["solo"]["env"])


if __name__ == "__main__":
    unittest.main()
