"""--chaos seed=N (PRD section 3, point 5): kill a random CLI, host or
controller at seeded times while scenarios S3 to S9 run.

The schedule comes from the seed alone: before every step the runner draws,
in this order, the wait (4 to 12 s), the target kind, the pick among the
candidates and the downtime (1 to 5 s), whether or not the step can act. So
the same seed gives the same sequence of draws; what each draw hits depends
on what is running then, and chaos.log records that.

Targets: fleet: a CLI 50 %, a host 30 %, the controller 20 %. solo: a CLI
70 %, the daemon 30 %. A CLI is SIGKILLed (the daemon notices and the next
need resumes it, S7); a daemon is SIGKILLed and started again after the
downtime with the same HOME and port. CLI picks are made among the live
agent CLIs sorted by workspace, then pid.

A scenario that restarts processes itself holds the fleet's control lock;
chaos never waits for it: that step is logged as skipped. On SIGTERM chaos
finishes the step in progress (a downed daemon is started again) and exits.

    lab.py chaos --state F --seed N --out chaos.log
"""

import json
import random
import signal
import time

from . import fleet as fleetlib

WAIT_S = (4.0, 12.0)
DOWN_S = (1.0, 5.0)
KINDS = {"fleet": (("cli", 0.5), ("host", 0.3), ("controller", 0.2)),
         "solo": (("cli", 0.7), ("controller", 0.3))}


def schedule(seed, steps):
    """The first `steps` draws for a seed: [(wait_s, kind_roll, pick_roll,
    down_s)]. Pure; the lab's tests check it is a function of the seed."""
    rng = random.Random(seed)
    return [(rng.uniform(*WAIT_S), rng.random(), rng.random(), rng.uniform(*DOWN_S)) for _ in range(steps)]


def kind_of(layout, roll):
    acc = 0.0
    for kind, p in KINDS[layout]:
        acc += p
        if roll < acc:
            return kind
    return KINDS[layout][-1][0]


class Chaos:
    def __init__(self, state_path, seed, out_path):
        self.fleet = fleetlib.Fleet(state_path)
        self.seed, self.out_path = seed, out_path
        self.rng = random.Random(seed)
        self.stop = False

    def log(self, **e):
        e = dict({"ts": fleetlib._now_iso(), "seed": self.seed}, **e)
        with open(self.out_path, "a") as fh:
            fh.write(json.dumps(e) + "\n")

    def sleep(self, s):
        end = time.monotonic() + s
        while not self.stop and time.monotonic() < end:
            time.sleep(min(0.1, end - time.monotonic()))

    def live_clis(self):
        cwd_of = {}
        for e in self.fleet.fake_events("cli.exec"):
            cwd_of[(e.get("pid"))] = (e.get("attrs") or {}).get("cwd") or ""
        return sorted(((cwd_of.get(p, ""), p) for p in self.fleet.cli_pids()))

    def step(self, n, kind, pick, down):
        layout = self.fleet.layout
        self.fleet.reload()
        with self.fleet.control(wait=False) as held:
            if not held:
                self.log(step=n, kind=kind, action="skipped", why="a scenario holds the fleet")
                return
            if kind == "cli":
                clis = self.live_clis()
                if not clis:
                    self.log(step=n, kind=kind, action="skipped", why="no live agent CLI")
                    return
                cwd, pid = clis[int(pick * len(clis))]
                try:
                    self.fleet.kill_cli(pid)
                    self.log(step=n, kind=kind, target=cwd, pid=pid, action="SIGKILL", live=len(clis))
                except (fleetlib.FleetError, ProcessLookupError) as e:
                    self.log(step=n, kind=kind, target=cwd, pid=pid, action="SIGKILL", result="failed: %s" % e)
                return
            if kind == "host":
                names = sorted(h["proc"] for h in self.fleet.hosts())
                name = names[int(pick * len(names))]
            else:
                name = self.fleet.st["controller"]
            pid = self.fleet.proc(name)["pid"]
            gone = self.fleet.kill(name)
            self.log(step=n, kind=kind, target=name, pid=pid, action="SIGKILL", result="gone" if gone else "still running")
            # The downtime runs to its end even when asked to stop, so the
            # daemon is always started again.
            end = time.monotonic() + down
            while time.monotonic() < end:
                time.sleep(0.05)
            try:
                new = self.fleet.start(name)
                self.log(step=n, kind=kind, target=name, action="started", pid=new, down_s=round(down, 2))
            except fleetlib.FleetError as e:
                self.log(step=n, kind=kind, target=name, action="start failed", why=str(e))

    def run(self):
        def on_term(*_):
            self.stop = True
        signal.signal(signal.SIGTERM, on_term)
        layout = self.fleet.layout
        self.log(step=0, action="chaos on", layout=layout, kinds=KINDS[layout], wait_s=WAIT_S, down_s=DOWN_S)
        n = 0
        while not self.stop:
            n += 1
            wait, kroll, pick, down = (self.rng.uniform(*WAIT_S), self.rng.random(), self.rng.random(),
                                       self.rng.uniform(*DOWN_S))
            self.sleep(wait)
            if self.stop:
                break
            self.step(n, kind_of(layout, kroll), pick, down)
        # Anything still down (a failed start) is started before leaving.
        self.fleet.reload()
        for name in self.fleet.st["procs"]:
            if not self.fleet.alive(name):
                try:
                    self.fleet.start(name)
                    self.log(step=n, action="restored", target=name)
                except fleetlib.FleetError as e:
                    self.log(step=n, action="restore failed", target=name, why=str(e))
        self.log(step=n, action="chaos off")
