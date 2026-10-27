"""The lab's process manager for gate G2: start, kill, restart and watch the
k2-daemon processes of a topology, and describe them in a state file every
other lab piece reads (scenarios, chaos, invariant checkers).

Layouts:

  fleet   one K2_ROLE=controller daemon and two K2_ROLE=host daemons
          (host-1, host-2), each under its own throwaway HOME, on free
          loopback ports; agents are k2-fake-agent children of the hosts
  solo    today's daemon (in-process agent host), named "solo"; it is both
          the controller (the client's URL) and the one host, "local"

Backend `compose` runs the fleet layout as deploy/compose/compose.lab-fleet.yaml
(scripts/lab/lib/compose.sh builds the image and calls `up_compose`): a
daemon's "pid" is then its container id, kill is `docker compose kill`,
start is `docker compose start` (the same container, like a rebooted VM),
and agent CLIs are named "<host>:<pid>" (pids of different containers can
be equal). Homes and workspaces are bind mounts at the same absolute paths.

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
import fcntl
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


@contextlib.contextmanager
def _state_lock(st):
    with open(st["_path"] + ".lock", "a") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def refresh(st):
    """Take every process's current entry from the file: several lab
    processes (scenarios, chaos, the monitor) start and kill daemons."""
    if os.path.isfile(st["_path"]):
        st["procs"] = load(st["_path"])["procs"]
    return st


def update_proc(st, name, **fields):
    """Read-modify-write one process's entry under the state lock, so one
    lab process never writes back another's stale pids."""
    with _state_lock(st):
        refresh(st)
        st["procs"][name].update(fields)
        save(st)


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
        # The outbox's gone rule (a message for an agent whose host is gone
        # is failed after this grace); empty = the daemon's default (120 s).
        # prove.sh sets LAB_GONE_GRACE_S=30 under --chaos.
        "K2_DELIVERY_GONE_GRACE_SECS": os.environ.get("LAB_GONE_GRACE_S", ""),
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
                                      "K2_HOST_BOOTSTRAP_TOKEN_FILE": token_file,
                                      # P3 T2: a lost host's portable agents move after this (S10).
                                      "K2_HOST_MOVE_AFTER": host_move_after()})
    ctl_port = st["procs"]["controller"]["port"]
    st["controller"] = "controller"
    st["hosts"] = []
    for h in HOSTS:
        extra = _agent_env(st, h)
        extra.update({"K2_HOME": os.path.join(root, h, "home", ".k2"), "K2_ROLE": "host",
                      "K2_CONTROLLER_URL": "ws://127.0.0.1:%d" % ctl_port,
                      "K2_HOST_ENROLL_TOKEN_FILE": token_file, "K2_HOST_NAME": h})
        if os.environ.get("LAB_HOST_PTYD") == "1":
            # Agents under k2-ptyd (built by prove.sh), which is designed to
            # outlive its daemon; down() kills what is left of it by pid.
            extra.update({"K2_PTYD": "1", "K2_PTYD_BIN": os.environ["LAB_PTYD_BIN"]})
            st["host_ptyd"] = True
        if home_sync_on():
            # P3 home sync: a host saves and restores the homes of agents
            # whose cwd is under K2_WORK_ROOT (the fleet's work dir).
            extra["K2_WORK_ROOT"] = st["work"]
            st["home_sync"] = True
        proc(h, "host", extra)
        st["hosts"].append({"name": h, "proc": h, "home": st["procs"][h]["home"],
                            "fake_trace": extra["K2_FAKE_AGENT_TRACE"]})
    return st


def host_move_after():
    """The controller's K2_HOST_MOVE_AFTER: LAB_HOST_MOVE_AFTER, 20 s when
    S10 runs (LAB_S10=1), else empty (the daemon's own 300 s)."""
    return os.environ.get("LAB_HOST_MOVE_AFTER") or ("20" if os.environ.get("LAB_S10") == "1" else "")


def home_sync_on():
    """LAB_HOME_SYNC=1; it defaults to LAB_S8_RESUME_ELSEWHERE (S8's resume
    on the other host needs the agent's home there)."""
    return os.environ.get("LAB_HOME_SYNC", os.environ.get("LAB_S8_RESUME_ELSEWHERE", "0")) == "1"


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
        if not ours(st, p["pid"]):
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


def _compose(st, *args, check=False, timeout=180):
    c = st["compose"]
    env = dict(os.environ, **c["env"])
    return subprocess.run(["docker", "compose", "-p", c["project"], "-f", c["file"]] + list(args),
                          env=env, capture_output=True, text=True, check=check, timeout=timeout)


def _running(st, name):
    out = _compose(st, "ps", "--status", "running", "-q", name).stdout.strip()
    return out[:12] if out else None


def ours(st, pid):
    """True when `pid` is alive and runs this run's daemon binary: a stale
    pid that the OS reused for another process is never signalled.
    (compose: `pid` is a container id; true while it runs.)"""
    if st.get("backend") == "compose":
        if not pid:
            return False
        names = [n for n, p in st["procs"].items() if p.get("pid") == pid]
        return bool(names) and _running(st, names[0]) == pid
    if not pid_alive(pid):
        return False
    cmd = subprocess.run(["ps", "-o", "command=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    return cmd.split(" ")[0] == st["daemon_bin"]


def start(st, name, wait=True):
    refresh(st)
    p = st["procs"][name]
    if ours(st, p["pid"]):
        raise FleetError("%s is already running (pid %s)" % (name, p["pid"]))
    if st.get("backend") == "compose":
        r = _compose(st, "start", name)
        if r.returncode != 0:
            raise FleetError("docker compose start %s: %s" % (name, r.stderr.strip()[-300:]))
        pid = _running(st, name)
        update_proc(st, name, pid=pid, starts=p["starts"] + 1, started_at=_now_iso())
        if wait:
            wait_ready(st, name)
        return pid
    p["bin"] = st["daemon_bin"]
    pid = _spawn_detached(p)
    update_proc(st, name, bin=st["daemon_bin"], pid=pid, starts=p["starts"] + 1, started_at=_now_iso())
    p = st["procs"][name]
    if wait:
        wait_ready(st, name)
    return p["pid"]


def kill(st, name, sig=signal.SIGKILL, wait_s=None):
    """Signal a daemon and wait for it to be gone. SIGKILL waits briefly;
    SIGTERM waits `wait_s` (a drain may take a while), then gives up and
    returns False (still running)."""
    refresh(st)
    pid = st["procs"][name]["pid"]
    if not ours(st, pid):
        return True
    if st.get("backend") == "compose":
        _compose(st, "kill", "-s", signal.Signals(sig).name, name)
        end = time.monotonic() + (wait_s if wait_s is not None else (10 if sig == signal.SIGKILL else STOP_GRACE_S))
        while time.monotonic() < end:
            if not _running(st, name):
                return True
            time.sleep(0.2)
        return not _running(st, name)
    os.kill(pid, sig)
    end = time.monotonic() + (wait_s if wait_s is not None else (5 if sig == signal.SIGKILL else STOP_GRACE_S))
    while time.monotonic() < end:
        if not pid_alive(pid):
            return True
        time.sleep(0.05)
    return not pid_alive(pid)


def agent_cli_pids(st):
    """Live agent CLIs of this run: processes whose executable is in its shim
    dir. procs: pids; compose: "<host>:<pid>" from ps in each host container."""
    if st.get("backend") == "compose":
        keys = []
        for h in st["hosts"]:
            if not _running(st, h["proc"]):
                continue
            r = _compose(st, "exec", "-T", h["proc"], "ps", "-eo", "pid=,args=", timeout=30)
            for line in r.stdout.splitlines():
                parts = line.strip().split(None, 1)
                if len(parts) == 2 and parts[1].startswith(st["shim"] + "/"):
                    keys.append("%s:%s" % (h["name"], parts[0]))
        return sorted(keys)
    out = subprocess.run(["ps", "-axo", "pid=,command="], capture_output=True, text=True).stdout
    prefix = st["shim"] + "/"
    pids = []
    for line in out.splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) == 2 and parts[1].startswith(prefix):
            pids.append(int(parts[0]))
    return sorted(pids)


def ptyd_pids(st):
    """Live k2-ptyd processes of this run (procs): a daemon runs its copy
    from <home>/bin/ptyd/<version>/k2-ptyd, and every home is under the
    run's root, so a path under the root is ours and nothing else is."""
    if st.get("backend") == "compose":
        return []
    out = subprocess.run(["ps", "-axo", "pid=,command="], capture_output=True, text=True).stdout
    prefix = st["root"].rstrip("/") + "/"
    pids = []
    for line in out.splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) == 2:
            exe = parts[1].split(" ")[0]
            if exe.startswith(prefix) and os.path.basename(exe) == "k2-ptyd":
                pids.append(int(parts[0]))
    return sorted(pids)


def cli_key(st, host, pid):
    """How agent_cli_pids names a CLI."""
    return "%s:%s" % (host, pid) if st.get("backend") == "compose" else pid


def kill_cli(st, pid, sig=signal.SIGKILL, host=None):
    key = cli_key(st, host, pid)
    if key not in agent_cli_pids(st):
        raise FleetError("%s is not an agent CLI of this run" % key)
    if st.get("backend") == "compose":
        _compose(st, "exec", "-T", host, "kill", "-%d" % int(sig), str(pid), timeout=30)
        return
    os.kill(pid, sig)


def cli_alive(st, host, pid):
    if not pid:
        return False
    if st.get("backend") == "compose":
        return cli_key(st, host, pid) in agent_cli_pids(st)
    return pid_alive(pid)


def down(st):
    refresh(st)
    if st.get("backend") == "compose":
        for name in st["procs"]:
            r = _compose(st, "logs", "--no-color", "--timestamps", name)
            with open(st["procs"][name]["log"], "a") as fh:
                fh.write(r.stdout + r.stderr)
        _compose(st, "down", "--volumes", "--remove-orphans", "-t", "10")
        return
    for name, p in st["procs"].items():
        if ours(st, p["pid"]):
            with contextlib.suppress(ProcessLookupError):
                os.kill(p["pid"], signal.SIGTERM)
    end = time.monotonic() + STOP_GRACE_S
    while time.monotonic() < end and any(ours(st, p["pid"]) for p in st["procs"].values()):
        time.sleep(0.1)
    for p in st["procs"].values():
        if ours(st, p["pid"]):
            with contextlib.suppress(ProcessLookupError):
                os.kill(p["pid"], signal.SIGKILL)
    for pid in agent_cli_pids(st):
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)
    # k2-ptyd outlives its daemon by design: whatever is left after the
    # daemons stopped is recorded (evidence) and killed, pid by pid.
    left = ptyd_pids(st)
    if st.get("host_ptyd") or left:
        with open(os.path.join(st["run_dir"], "ptyd-leftovers.txt"), "a") as fh:
            fh.write("k2-ptyd still running after the daemons stopped: %s\n" % (left or "none"))
    for pid in left:
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


def up_compose(run_dir, state_path, root, image, project, compose_file):
    """The fleet layout on Docker Compose (scripts/lab/lib/compose.sh calls
    this after building `image`)."""
    run_dir = os.path.abspath(run_dir)
    os.makedirs(os.path.join(run_dir, "traces"), exist_ok=True)
    st = plan("fleet", root, run_dir, "/usr/bin/k2-daemon", "/usr/bin/k2-fake-agent")
    st.update(backend="compose", shim="/opt/k2-shim", _path=state_path)
    for name, p in st["procs"].items():
        p["env"] = {}  # in the compose file
        p["service"] = name
        os.makedirs(p["home"], exist_ok=True)
    st["compose"] = {"project": project, "file": compose_file, "image": image, "env": {
        "LAB_COMPOSE_PROJECT": project, "LAB_FLEET_IMAGE": image, "LAB_ROOT": root,
        "LAB_TRACES": os.path.join(run_dir, "traces"),
        "LAB_CTL_PORT": str(st["procs"]["controller"]["port"]),
        "LAB_H1_PORT": str(st["procs"]["host-1"]["port"]), "LAB_H2_PORT": str(st["procs"]["host-2"]["port"]),
        "LAB_TURN_MS": os.environ.get("LAB_TURN_MS", "300"), "LAB_SEED": os.environ.get("LAB_SEED", "0")}}
    st["compose"]["env"]["LAB_HOST_MOVE_AFTER"] = host_move_after()
    if home_sync_on():
        st["compose"]["env"]["LAB_WORK_ROOT"] = st["work"]
        st["home_sync"] = True
    os.makedirs(st["work"], exist_ok=True)
    with open(st["host_token_file"], "w") as fh:
        fh.write(os.urandom(24).hex())
    for h in st["hosts"]:
        open(h["fake_trace"], "a").close()
    save(st)
    r = _compose(st, "up", "-d", "--no-build", "--pull", "never", timeout=300)
    if r.returncode != 0:
        raise FleetError("docker compose up: %s" % r.stderr.strip()[-500:])
    for name in ("controller", "host-1", "host-2"):
        update_proc(st, name, pid=_running(st, name), starts=1, started_at=_now_iso())
    for name in ("controller", "host-1", "host-2"):
        wait_ready(st, name)
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
            procs = {n: (p["pid"] if ours(st, p["pid"]) else None) for n, p in st["procs"].items()}
            fh.write(json.dumps({"ts": _now_iso(), "procs": procs, "clis": agent_cli_pids(st)}) + "\n")
            fh.flush()
            time.sleep(MONITOR_PERIOD_S)


# ── Reading what the topology wrote ─────────────────────────────────────

def host_files(st, host, root, needle):
    """The files under `root`, as `host` sees its own disk, whose text has
    `needle`. None here: procs and compose hosts share this machine's view
    of the workspaces, so the caller reads them itself. An outside backend
    whose hosts have their own disks (kind with own work volumes) replaces
    it and reads through the host."""
    return None


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
        refresh(self.st)
        return ours(self.st, self.st["procs"][name]["pid"])

    def cli_pids(self):
        return agent_cli_pids(self.st)

    def kill_cli(self, pid, sig=signal.SIGKILL, host=None):
        kill_cli(self.st, pid, sig, host)

    def cli_alive(self, host, pid):
        return cli_alive(self.st, host, pid)

    def host_files(self, host, root, needle):
        return host_files(self.st, host, root, needle)

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
