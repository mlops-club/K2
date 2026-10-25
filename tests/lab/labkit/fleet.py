"""The lab's process manager for gate G2: start, kill, restart and watch the
k2-daemon processes of a topology, and describe them in a state file every
other lab piece reads (scenarios, chaos, invariant checkers).

Layouts:

  fleet   one K2_ROLE=controller daemon and two K2_ROLE=host daemons
          (host-1, host-2), each under its own throwaway HOME, on free
          loopback ports; agents are k2-fake-agent children of the hosts
  solo    today's daemon (in-process agent host), named "solo"; it is both
          the controller (the client's URL) and the one host, "local"

Backend `procs` runs them on this machine. A daemon is started by a short
/bin/sh that backgrounds it and exits, so the daemon is never a child of a
Python process (a killed daemon can't linger as a zombie that `kill -0`
calls alive) and it outlives the `lab.py fleet up` that started it.

State file (JSON), written by `up` and updated by every start:

  {"layout", "backend", "root", "run_dir", "daemon_bin", "fake_bin", "shim",
   "work", "controller": <proc name>, "hosts": [{"name", "proc", "home",
   "fake_trace"}], "procs": {<name>: {"role", "port", "home", "env", "log",
   "trace", "pid", "starts"}}, "lock": <dir>}

Every daemon writes its K2_TRACE_FILE to <run_dir>/traces/<name>.daemon.jsonl
and its agents write theirs to <run_dir>/traces/<host>.fake.jsonl
(K2_FAKE_AGENT_HOST=<host>), both appended across restarts.

Kills are by pid only: the daemons' pids from the state file, and agent CLIs
whose executable is inside this run's shim dir.

    lab.py fleet up --layout fleet|solo --run-dir D --state F --daemon-bin B --fake-bin A
    lab.py fleet down --state F
    lab.py fleet kill --state F <proc> [--signal KILL|TERM]
    lab.py fleet start --state F <proc>
    lab.py fleet monitor --state F --out samples.jsonl     (until SIGTERM)
"""

import contextlib
import json
import os
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request

READY_DEADLINE_S = 90  # boot to "ready" and a host's link; only stops a hung start
STOP_GRACE_S = 10
MONITOR_PERIOD_S = 0.5
HOSTS = ("host-1", "host-2")


class FleetError(Exception):
    pass


def _now_iso():
    t = time.time()
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + ".%03dZ" % int((t % 1) * 1000)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def pid_alive(pid):
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def http_json(port, path, timeout=2):
    try:
        with urllib.request.urlopen("http://127.0.0.1:%d%s" % (port, path), timeout=timeout) as r:
            return json.loads(r.read() or b"{}")
    except (OSError, ValueError):
        return None


def load(state_path):
    with open(state_path, encoding="utf-8") as fh:
        st = json.load(fh)
    st["_path"] = state_path
    return st


def save(st):
    path = st["_path"]
    doc = {k: v for k, v in st.items() if not k.startswith("_")}
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=2)
    os.replace(tmp, path)


# ── Layouts ─────────────────────────────────────────────────────────────

def _base_env(root, name, home):
    return {
        "HOME": home,
        "TMPDIR": os.path.join(root, name, "tmp"),
        "XDG_RUNTIME_DIR": os.path.join(root, name, "run"),
        "K2_RUNTIME_DIR": "",
        "USER": os.environ.get("USER") or subprocess.run(["id", "-un"], capture_output=True, text=True).stdout.strip(),
        "LANG": "en_US.UTF-8",
    }


def _agent_env(st, host_name):
    """What a process that runs agents adds: the fake agent as every CLI."""
    return {
        "PATH": "%s:/usr/bin:/bin:/usr/sbin:/sbin" % st["shim"],
        "K2_TEST_AGENT_SHIM_DIR": st["shim"],
        "K2_FAKE_AGENT_TRACE": os.path.join(st["run_dir"], "traces", "%s.fake.jsonl" % host_name),
        "K2_FAKE_AGENT_HOST": host_name,
        "K2_FAKE_AGENT_TURN_MS": os.environ.get("LAB_TURN_MS", "300"),
        "K2_FAKE_AGENT_SEED": os.environ.get("LAB_SEED", "0"),
    }


def plan(layout, root, run_dir, daemon_bin, fake_bin):
    """The state for a new topology (nothing started yet)."""
    if layout not in ("fleet", "solo"):
        raise FleetError("unknown layout %r (fleet, solo)" % layout)
    st = {"layout": layout, "backend": "procs", "root": root, "run_dir": run_dir,
          "daemon_bin": daemon_bin, "fake_bin": fake_bin, "shim": os.path.join(root, "shim"),
          "work": os.path.join(root, "work"), "lock": os.path.join(root, "fleet.lock"), "procs": {}}
    traces = os.path.join(run_dir, "traces")

    def proc(name, role, extra):
        home = os.path.join(root, name, "home")
        port = free_port()
        env = _base_env(root, name, home)
        env.update(extra)
        env.update({"K2_DAEMON_PORT": str(port), "K2_TRACE_FILE": os.path.join(traces, "%s.daemon.jsonl" % name)})
        st["procs"][name] = {"role": role, "port": port, "home": home, "env": env,
                             "log": os.path.join(run_dir, "%s.log" % name),
                             "trace": env["K2_TRACE_FILE"], "pid": None, "starts": 0}

    if layout == "solo":
        extra = {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin"}
        extra.update(_agent_env(st, "local"))
        proc("solo", "all", extra)
        st["controller"] = "solo"
        st["hosts"] = [{"name": "local", "proc": "solo", "home": st["procs"]["solo"]["home"],
                        "fake_trace": extra["K2_FAKE_AGENT_TRACE"]}]
        return st

    token_file = os.path.join(root, "host-token")
    st["host_token_file"] = token_file
    proc("controller", "controller", {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
                                      "K2_HOME": os.path.join(root, "controller", "home", ".k2"),
                                      "K2_ROLE": "controller", "K2_HOSTS": "1",
                                      "K2_HOST_BOOTSTRAP_TOKEN_FILE": token_file})
    ctl_port = st["procs"]["controller"]["port"]
    st["controller"] = "controller"
    st["hosts"] = []
    for h in HOSTS:
        extra = _agent_env(st, h)
        extra.update({"K2_HOME": os.path.join(root, h, "home", ".k2"), "K2_ROLE": "host",
                      "K2_CONTROLLER_URL": "ws://127.0.0.1:%d" % ctl_port,
                      "K2_HOST_ENROLL_TOKEN_FILE": token_file, "K2_HOST_NAME": h})
        proc(h, "host", extra)
        st["hosts"].append({"name": h, "proc": h, "home": st["procs"][h]["home"],
                            "fake_trace": extra["K2_FAKE_AGENT_TRACE"]})
    return st


# ── Process control (backend procs) ─────────────────────────────────────

def _spawn_detached(p):
    """Start the daemon through a /bin/sh that backgrounds it and exits;
    returns the daemon's pid. fd 3 carries the pid back and is closed for
    the daemon, or the pipe would stay open as long as it runs."""
    os.makedirs(os.path.dirname(p["log"]), exist_ok=True)
    for d in (p["env"]["TMPDIR"], p["env"]["XDG_RUNTIME_DIR"], p["home"]):
        os.makedirs(d, exist_ok=True)
    with open(p["log"], "a") as log:
        log.write("\n===== lab: start %s at %s =====\n" % (p["role"], _now_iso()))
    out = subprocess.run(
        ["/bin/sh", "-c", 'exec 3>&1; "$1" 3>&- </dev/null >>"$0" 2>&1 & echo $! >&3', p["log"], p["bin"]],
        env=p["env"], stdout=subprocess.PIPE, text=True, check=True,
        # Its own cwd: helper CLIs a daemon starts (codex app-server, ...)
        # inherit it, and must not look like they share a workspace.
        cwd=os.path.dirname(p["home"]))
    return int(out.stdout.strip())


def wait_ready(st, name, deadline=READY_DEADLINE_S):
    p = st["procs"][name]
    end = time.monotonic() + deadline
    phase = None
    while time.monotonic() < end:
        if not pid_alive(p["pid"]):
            raise FleetError("%s (pid %s) exited during boot; log %s" % (name, p["pid"], p["log"]))
        phase = (http_json(p["port"], "/boot-status") or {}).get("phase")
        if phase == "ready":
            if p["role"] != "host":
                return
            if (http_json(p["port"], "/cli/hosts/status") or {}).get("linked") is True:
                return
        time.sleep(0.2)
    raise FleetError("%s never got ready in %d s (boot phase %r%s)" % (
        name, deadline, phase, ", host not linked" if p["role"] == "host" else ""))


def start(st, name, wait=True):
    p = st["procs"][name]
    if pid_alive(p["pid"]):
        raise FleetError("%s is already running (pid %s)" % (name, p["pid"]))
    p["bin"] = st["daemon_bin"]
    p["pid"] = _spawn_detached(p)
    p["starts"] += 1
    p["started_at"] = _now_iso()
    save(st)
    if wait:
        wait_ready(st, name)
    return p["pid"]


def kill(st, name, sig=signal.SIGKILL, wait_s=None):
    """Signal a daemon and wait for it to be gone. SIGKILL waits briefly;
    SIGTERM waits `wait_s` (a drain may take a while), then gives up and
    returns False (still running)."""
    p = st["procs"][name]
    pid = p["pid"]
    if not pid_alive(pid):
        return True
    os.kill(pid, sig)
    end = time.monotonic() + (wait_s if wait_s is not None else (5 if sig == signal.SIGKILL else STOP_GRACE_S))
    while time.monotonic() < end:
        if not pid_alive(pid):
            return True
        time.sleep(0.05)
    return not pid_alive(pid)


def agent_cli_pids(st):
    """Live agent CLIs of this run: processes whose executable is in its shim dir."""
    out = subprocess.run(["ps", "-axo", "pid=,command="], capture_output=True, text=True).stdout
    prefix = st["shim"] + "/"
    pids = []
    for line in out.splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) == 2 and parts[1].startswith(prefix):
            pids.append(int(parts[0]))
    return sorted(pids)


def kill_cli(st, pid, sig=signal.SIGKILL):
    if pid not in agent_cli_pids(st):
        raise FleetError("pid %s is not an agent CLI of this run" % pid)
    os.kill(pid, sig)


def down(st):
    for name, p in st["procs"].items():
        if pid_alive(p["pid"]):
            with contextlib.suppress(ProcessLookupError):
                os.kill(p["pid"], signal.SIGTERM)
    end = time.monotonic() + STOP_GRACE_S
    while time.monotonic() < end and any(pid_alive(p["pid"]) for p in st["procs"].values()):
        time.sleep(0.1)
    for p in st["procs"].values():
        if pid_alive(p["pid"]):
            with contextlib.suppress(ProcessLookupError):
                os.kill(p["pid"], signal.SIGKILL)
    for pid in agent_cli_pids(st):
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)


def up(layout, run_dir, state_path, daemon_bin, fake_bin):
    # Short, under /tmp: hosts nest runtime dirs (sockets) under it, and a
    # macOS socket path is at most 104 bytes.
    root = os.path.realpath(tempfile.mkdtemp(prefix="k2lab-%s-" % layout, dir="/tmp"))
    os.makedirs(os.path.join(run_dir, "traces"), exist_ok=True)
    st = plan(layout, root, os.path.abspath(run_dir), daemon_bin, fake_bin)
    st["_path"] = state_path
    os.makedirs(st["work"], exist_ok=True)
    subprocess.run([fake_bin, "--install-shims", st["shim"]], check=True)
    if layout == "fleet":
        with open(st["host_token_file"], "w") as fh:
            fh.write(os.urandom(24).hex())
        os.chmod(st["host_token_file"], 0o600)
    for h in st["hosts"]:
        open(h["fake_trace"], "a").close()
    save(st)
    start(st, st["controller"])
    for h in st["hosts"]:
        if h["proc"] != st["controller"]:
            start(st, h["proc"])
    return st


# ── Shared control lock (scenarios that restart processes vs chaos) ─────

@contextlib.contextmanager
def control(st, wait=True):
    """Hold the fleet's control lock. Scenarios wait for it; chaos asks with
    wait=False and gets False when a scenario holds it."""
    lock = st["lock"]
    while True:
        try:
            os.mkdir(lock)
            break
        except FileExistsError:
            try:
                holder = int(open(os.path.join(lock, "pid")).read().strip() or 0)
            except (OSError, ValueError):
                holder = 0
            if holder and not pid_alive(holder):
                with contextlib.suppress(OSError):
                    os.remove(os.path.join(lock, "pid"))
                    os.rmdir(lock)
                continue
            if not wait:
                yield False
                return
            time.sleep(0.1)
    with open(os.path.join(lock, "pid"), "w") as fh:
        fh.write(str(os.getpid()))
    try:
        yield True
    finally:
        with contextlib.suppress(OSError):
            os.remove(os.path.join(lock, "pid"))
            os.rmdir(lock)


# ── Monitor ─────────────────────────────────────────────────────────────

def monitor(state_path, out_path, stop=None):
    """Append one sample per period: which daemons and agent CLIs are alive.
    Runs until SIGTERM (or `stop()` returns true)."""
    done = {"v": False}

    def on_term(*_):
        done["v"] = True
    signal.signal(signal.SIGTERM, on_term)
    with open(out_path, "a") as fh:
        while not done["v"] and not (stop and stop()):
            st = load(state_path)
            procs = {n: (p["pid"] if pid_alive(p["pid"]) else None) for n, p in st["procs"].items()}
            fh.write(json.dumps({"ts": _now_iso(), "procs": procs, "clis": agent_cli_pids(st)}) + "\n")
            fh.flush()
            time.sleep(MONITOR_PERIOD_S)


# ── Reading what the topology wrote ─────────────────────────────────────

class Fleet:
    """A scenario's view of a running topology."""

    def __init__(self, state_path):
        self.path = state_path
        self.reload()

    def reload(self):
        self.st = load(self.path)
        return self

    @property
    def layout(self):
        return self.st["layout"]

    @property
    def controller(self):
        return self.st["procs"][self.st["controller"]]

    @property
    def port(self):
        return self.controller["port"]

    def token(self, proc=None):
        p = self.st["procs"][proc or self.st["controller"]]
        with open(os.path.join(p["home"], ".k2", "daemon.token")) as fh:
            return fh.read().strip()

    def hosts(self):
        return self.st["hosts"]

    def host(self, name):
        return next(h for h in self.st["hosts"] if h["name"] == name)

    def homes(self):
        return [h["home"] for h in self.st["hosts"]]

    def proc(self, name):
        return self.st["procs"][name]

    def workspace(self, name):
        """A fresh git repo all hosts can reach (the spawn's cwd goes to the
        host verbatim)."""
        path = os.path.join(self.st["work"], name)
        os.makedirs(path, exist_ok=True)
        subprocess.run(["git", "init", "-q", path], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return os.path.realpath(path)

    def fake_events(self, event=None):
        """Every fake-agent trace event of every host, in file order per host."""
        out = []
        for h in self.st["hosts"]:
            for e in read_jsonl(h["fake_trace"]):
                e.setdefault("host", h["name"])
                if event is None or e.get("event") == event:
                    out.append(e)
        return out

    def daemon_events(self, proc, event=None):
        return [e for e in read_jsonl(self.st["procs"][proc]["trace"]) if event is None or e.get("event") == event]

    def kill(self, name, sig=signal.SIGKILL, wait_s=None):
        return kill(self.st, name, sig, wait_s)

    def start(self, name, wait=True):
        pid = start(self.st, name, wait)
        return pid

    def alive(self, name):
        return pid_alive(self.st["procs"][name]["pid"])

    def cli_pids(self):
        return agent_cli_pids(self.st)

    def kill_cli(self, pid, sig=signal.SIGKILL):
        kill_cli(self.st, pid, sig)

    def control(self, wait=True):
        return control(self.st, wait)


def read_jsonl(path):
    out = []
    if path and os.path.isfile(path):
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    v = json.loads(line)
                except ValueError:
                    continue
                if isinstance(v, dict):
                    out.append(v)
    return out
