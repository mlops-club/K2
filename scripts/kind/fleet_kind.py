"""The lab's kind backend (TEST ONLY): the fleet layout of tests/lab/labkit/
fleet.py on k2-kind-dist-fake, one controller and N agent hosts, driven
from the runner pod that scripts/kind/fleet-g2.sh creates there.

The lab reads agent homes, traces and workspaces from disk, so it runs
inside the cluster, as the compose backend runs beside its containers:

  /lab/homes/<pod>     each pod's HOME volume (data-<pod>), mounted read-write
  /var/lib/k2/work     the shared workspaces (k2-work), the hosts' path too
  /lab/repo            tests/lab, cli/ and scripts/kind of this tree (copied per run)
  /lab/run             the run directory (copied back to the Mac afterwards)

Every daemon runs under deploy/kind/dist-fake/supervise.sh. A proc's
"pid" is "<pod>/<pid>@<start time>" of its k2-daemon process (opaque to
the lab; a new process is a new pid). The pod is part of it: each pod has
its own pid namespace, and two pods started on the same clock tick have
the same "<pid>@<start time>". The backend:

  kill    writes $HOME/.k2-lab/hold, then `kubectl exec <pod> -- kill -<SIG>
          <pid>` (--kill=process, the default) or `kubectl delete pod`
          (--kill=pod; the StatefulSet makes a new pod, held until start);
          the image has no kill binary, so it is sh's builtin
          waits for the process to be gone
  start   removes the hold file; waits for a new k2-daemon process
  CLIs    "<host>:<pid>" of processes run from the fake-agent shim dir
          (listed with kubectl exec over /proc; the image has no ps)

Every daemon's API is on 127.0.0.1:<port> in the runner (the state file's
ports), forwarded to its pod: `forward` runs that, closing a port while its
daemon does not answer, so a client sees "connection refused" there, as on
a machine whose daemon is down.

    fleet_kind.py plan --state F --run-dir D --namespace NS --hosts k2-host-0,k2-host-1 [--kill process|pod]
                       [--pool MIN:MAX]
    fleet_kind.py forward --state F                  (until SIGTERM)
    fleet_kind.py lab <lab.py args...>               lab.py with this backend
    fleet_kind.py hold|release --state F <proc>      keep a daemon down / let it run

Pool mode (--pool, the controller runs K2_PROVISIONER=kubernetes): --hosts
names every host the pool may make; one with no pod yet is down, and only
the pool starts it (`start` refuses at once instead of waiting).

`lab` loads tests/lab/lab.py. When its fleet.py has no hook for an outside
backend (`backend_module` in the state file), it installs these functions
on the fleet module first. Standard library only; run with `python3 -I`.
"""

import json
import os
import select
import signal
import socket
import subprocess
import sys
import threading
import time

REPO = os.environ.get("LAB_KIND_REPO", "/lab/repo")
SHIM = "/opt/k2-fake/shim"
CTL_POD = "k2-controller-0"
BASE_PORT = 17420
API_PORT = 7420
SNAPSHOT_TTL_S = 0.3
KILL_WAIT_S = 10
START_WAIT_S = 90

# One /proc pass in the pod: the supervised daemon ("D <pid>@<start>") and
# every process run from the shim dir ("C <pid>").
SNAPSHOT_SH = r'''
d=$(cat "$HOME/.k2-lab/daemon.pid" 2>/dev/null)
if [ -n "$d" ] && grep -q '^Name:[[:space:]]*k2-daemon$' "/proc/$d/status" 2>/dev/null; then
    echo "D $d@$(cut -d' ' -f22 "/proc/$d/stat")"
fi
for p in /proc/[0-9]*; do
    c=$(tr '\000' ' ' < "$p/cmdline" 2>/dev/null) || continue
    case "$c" in "$1"/*) echo "C ${p#/proc/}" ;; esac
done
'''


class KindError(Exception):
    pass


def _fleetlib():
    sys.path.insert(0, os.path.join(REPO, "tests", "lab"))
    from labkit import fleet
    return fleet


# ── kubectl ─────────────────────────────────────────────────────────────

def kubectl(st, *args, timeout=30):
    return subprocess.run(["kubectl", "-n", st["kind"]["namespace"]] + list(args),
                          capture_output=True, text=True, timeout=timeout)


def _exec(st, name, *cmd, timeout=30):
    k = st["kind"]
    return kubectl(st, "exec", k["pods"][name], "-c", k["containers"][name], "--", *cmd, timeout=timeout)


_snap = {}


def snapshot(st, name, fresh=False):
    """(daemon pid or None, [cli pids]) of a pod; None when the pod can't be
    reached (gone, starting). Cached SNAPSHOT_TTL_S per process."""
    hit = _snap.get(name)
    if hit and not fresh and time.monotonic() - hit[0] < SNAPSHOT_TTL_S:
        return hit[1]
    try:
        r = _exec(st, name, "sh", "-c", SNAPSHOT_SH, "snapshot", SHIM, timeout=20)
    except subprocess.TimeoutExpired:
        r = None
    if r is None or r.returncode != 0:
        val = None
    else:
        daemon, clis = None, []
        for line in r.stdout.splitlines():
            kind, _, v = line.partition(" ")
            if kind == "D":
                daemon = "%s/%s" % (st["kind"]["pods"][name], v.strip())
            elif kind == "C" and v.strip().isdigit():
                clis.append(int(v))
        val = (daemon, sorted(clis))
    _snap[name] = (time.monotonic(), val)
    return val


def _hold_file(st, name):
    return os.path.join(st["procs"][name]["home"], ".k2-lab", "hold")


def hold(st, name):
    path = _hold_file(st, name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    open(path, "w").close()


def release(st, name):
    try:
        os.remove(_hold_file(st, name))
    except FileNotFoundError:
        pass


# ── the backend (what labkit/fleet.py delegates to) ─────────────────────

def ours(st, pid):
    if not pid:
        return False
    names = [n for n, p in st["procs"].items() if p.get("pid") == pid]
    if not names:
        return False
    snap = snapshot(st, names[0])
    return bool(snap) and snap[0] == pid


def _pod_exists(st, name):
    return kubectl(st, "get", "pod", st["kind"]["pods"][name], "-o", "name").returncode == 0


def start(st, name):
    """Let the daemon run again; returns its new pid (fleet.py then waits
    for it to be ready)."""
    release(st, name)
    if st["kind"].get("pool") and name != "controller" and not _pod_exists(st, name):
        raise KindError("%s: no pod; the host pool (%s) makes it when it needs a host" % (name, st["kind"]["pool"]))
    end = time.monotonic() + START_WAIT_S
    while time.monotonic() < end:
        snap = snapshot(st, name, fresh=True)
        if snap and snap[0]:
            return snap[0]
        time.sleep(0.2)
    raise KindError("%s: no k2-daemon in pod %s %d s after release" % (name, st["kind"]["pods"][name], START_WAIT_S))


def kill(st, name, sig=signal.SIGKILL, wait_s=None):
    """Hold the daemon down and signal it; True once its process is gone."""
    pid = st["procs"][name]["pid"]
    hold(st, name)
    wait = wait_s if wait_s is not None else KILL_WAIT_S
    pod = st["kind"]["pods"][name]
    if st["kind"].get("kill") == "pod":
        grace = "1" if sig == signal.SIGKILL else str(int(max(1, wait)))
        kubectl(st, "delete", "pod", pod, "--wait=false", "--grace-period=" + grace, timeout=30)
    else:
        num = pid.rsplit("/", 1)[-1].split("@")[0] if pid else ""
        if num:
            _exec(st, name, "sh", "-c", 'kill -"$1" "$2"', "kill", str(int(sig)), num, timeout=30)
    end = time.monotonic() + wait
    while True:
        snap = snapshot(st, name, fresh=True)
        if not snap or snap[0] != pid:
            return True
        if time.monotonic() >= end:
            return False
        time.sleep(0.2)


def agent_cli_pids(st):
    keys = []
    for h in st["hosts"]:
        snap = snapshot(st, h["proc"])
        if snap:
            keys += ["%s:%s" % (h["name"], p) for p in snap[1]]
    return sorted(keys)


def kill_cli(st, host, pid, sig=signal.SIGKILL):
    proc = next(h["proc"] for h in st["hosts"] if h["name"] == host)
    r = _exec(st, proc, "sh", "-c", 'kill -"$1" "$2"', "kill", str(int(sig)), str(pid))
    _snap.pop(proc, None)
    if r.returncode != 0:
        raise ProcessLookupError("kill %s on %s: %s" % (pid, host, r.stderr.strip()[-200:]))


def cli_alive(st, host, pid):
    return bool(pid) and "%s:%s" % (host, pid) in agent_cli_pids(st)


def down(st):
    """Leave every daemon running (the deployment stays; fleet-g2.sh and
    down.sh own it)."""
    for name in st["procs"]:
        release(st, name)


# ── fallback: install the backend on a fleet.py without the hook ────────

def install(fleet):
    """Route fleet.py's process control here, for a fleet.py that predates
    the outside-backend hook. The Fleet class and chaos call these through
    the module, so replacing them is enough."""
    import inspect
    if "backend_module" in inspect.getsource(fleet):
        return False
    orig_cli_key = fleet.cli_key

    def k_start(st, name, wait=True):
        fleet.refresh(st)
        p = st["procs"][name]
        if ours(st, p["pid"]):
            raise fleet.FleetError("%s is already running (pid %s)" % (name, p["pid"]))
        try:
            pid = start(st, name)
        except KindError as e:
            raise fleet.FleetError(str(e))
        fleet.update_proc(st, name, pid=pid, starts=p["starts"] + 1, started_at=fleet._now_iso())
        if wait:
            fleet.wait_ready(st, name)
        return pid

    def k_kill(st, name, sig=signal.SIGKILL, wait_s=None):
        fleet.refresh(st)
        if not ours(st, st["procs"][name]["pid"]):
            return True
        return kill(st, name, sig, wait_s)

    def k_kill_cli(st, pid, sig=signal.SIGKILL, host=None):
        if "%s:%s" % (host, pid) not in agent_cli_pids(st):
            raise fleet.FleetError("%s:%s is not an agent CLI of this run" % (host, pid))
        kill_cli(st, host, pid, sig)

    def k_cli_key(st, host, pid):
        return "%s:%s" % (host, pid) if st.get("backend") == "kind" else orig_cli_key(st, host, pid)

    fleet.ours, fleet.start, fleet.kill = ours, k_start, k_kill
    fleet.agent_cli_pids, fleet.kill_cli, fleet.cli_alive = agent_cli_pids, k_kill_cli, cli_alive
    fleet.down, fleet.cli_key = down, k_cli_key
    return True


# ── plan ────────────────────────────────────────────────────────────────

def plan(state_path, run_dir, namespace, hosts, kill_mode, pool=None):
    run_dir = os.path.abspath(run_dir)
    os.makedirs(os.path.join(run_dir, "traces"), exist_ok=True)
    st = {"layout": "fleet", "backend": "kind", "backend_module": os.path.abspath(__file__),
          "root": "/lab", "run_dir": run_dir, "daemon_bin": "k2-daemon", "fake_bin": None, "shim": SHIM,
          "work": "/var/lib/k2/work/lab", "lock": os.path.join(run_dir, "fleet.lock"),
          "controller": "controller", "hosts": [], "procs": {},
          "kind": {"namespace": namespace, "kill": kill_mode, "pods": {}, "containers": {}, "upstream": {},
                   "pod_home": {}, "pool": pool}}
    k = st["kind"]

    def proc(name, pod, role, container, upstream, pod_home, port):
        home = "/lab/homes/%s" % pod
        k["pods"][name], k["containers"][name], k["upstream"][name], k["pod_home"][name] = (
            pod, container, upstream, pod_home)
        st["procs"][name] = {"role": role, "port": port, "home": home,
                             "env": {"HOME": pod_home, "TMPDIR": "/tmp", "K2_DAEMON_PORT": str(API_PORT)},
                             "log": os.path.join(run_dir, "%s.log" % name),
                             "trace": os.path.join(home, ".k2-lab", "traces", "daemon.jsonl"),
                             "pid": None, "starts": 1}

    proc("controller", CTL_POD, "controller", "k2-daemon", "k2-lab-controller.%s.svc:%d" % (namespace, API_PORT),
         "/var/lib/k2", BASE_PORT)
    for i, pod in enumerate(hosts):
        proc(pod, pod, "host", "k2-host", "%s.k2-host.%s.svc:%d" % (pod, namespace, API_PORT), "/var/lib/k2-host",
             BASE_PORT + 1 + i)
        st["hosts"].append({"name": pod, "proc": pod, "home": st["procs"][pod]["home"],
                            "fake_trace": os.path.join(st["procs"][pod]["home"], ".k2-lab", "traces", "fake.jsonl")})
    for name, p in st["procs"].items():
        if not os.path.isdir(p["home"]):
            raise KindError("%s: %s is not mounted" % (name, p["home"]))
        # A pool host's fresh volume has no trace directory yet.
        os.makedirs(os.path.dirname(p["trace"]), exist_ok=True)
        release(st, name)
        snap = snapshot(st, name, fresh=True)
        p["pid"] = snap[0] if snap else None
    os.makedirs(st["work"], exist_ok=True)
    with open(state_path, "w") as fh:
        json.dump(st, fh, indent=2)
    return st


# ── forward ─────────────────────────────────────────────────────────────

def _pump(a, b):
    try:
        while True:
            data = a.recv(65536)
            if not data:
                break
            b.sendall(data)
    except OSError:
        pass
    finally:
        for s, how in ((b, socket.SHUT_WR), (a, socket.SHUT_RD)):
            try:
                s.shutdown(how)
            except OSError:
                pass


def _connect(upstream, timeout):
    host, port = upstream.rsplit(":", 1)
    s = socket.create_connection((host, int(port)), timeout=timeout)
    s.settimeout(None)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return s


def _reset(s):
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b"\x01\x00\x00\x00\x00\x00\x00\x00")
    except OSError:
        pass
    s.close()


class Forward:
    """127.0.0.1:<port> → upstream, listening only while upstream answers."""

    def __init__(self, port, upstream):
        self.port, self.upstream, self.lsock, self.up = port, upstream, None, False

    def check(self):
        try:
            _connect(self.upstream, 0.5).close()
            self.up = True
        except OSError:
            self.up = False
        if self.up and self.lsock is None:
            s = socket.socket()
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(("127.0.0.1", self.port))
                s.listen(128)
                s.setblocking(False)
                self.lsock = s
            except OSError:
                s.close()
        elif not self.up and self.lsock is not None:
            self.lsock.close()
            self.lsock = None

    def accept(self):
        try:
            c, _ = self.lsock.accept()
        except OSError:
            return
        c.setblocking(True)

        def serve():
            try:
                u = _connect(self.upstream, 2)
            except OSError:
                _reset(c)
                return
            c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            threading.Thread(target=_pump, args=(u, c), daemon=True).start()
            _pump(c, u)
        threading.Thread(target=serve, daemon=True).start()


def forward(state_path):
    st = json.load(open(state_path))
    fwds = [Forward(p["port"], st["kind"]["upstream"][n]) for n, p in st["procs"].items()]
    stop = {"v": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.update(v=True))
    checked = 0.0
    while not stop["v"]:
        if time.monotonic() - checked >= 0.25:
            checked = time.monotonic()
            for f in fwds:
                f.check()
        socks = {f.lsock: f for f in fwds if f.lsock is not None}
        if not socks:
            time.sleep(0.1)
            continue
        try:
            ready, _, _ = select.select(list(socks), [], [], 0.1)
        except (OSError, ValueError):
            continue
        for s in ready:
            socks[s].accept()


# ── main ────────────────────────────────────────────────────────────────

def main(argv):
    if not argv:
        print(__doc__)
        return 2
    cmd, rest = argv[0], argv[1:]
    if cmd == "lab":
        fleet = _fleetlib()
        install(fleet)
        sys.argv = ["lab.py"] + rest
        import runpy
        runpy.run_path(os.path.join(REPO, "tests", "lab", "lab.py"), run_name="__main__")
        return 0
    import argparse
    p = argparse.ArgumentParser(prog="fleet_kind.py")
    p.add_argument("cmd", choices=["plan", "forward", "hold", "release"])
    p.add_argument("proc", nargs="?")
    p.add_argument("--state", required=True)
    p.add_argument("--run-dir")
    p.add_argument("--namespace")
    p.add_argument("--hosts")
    p.add_argument("--kill", choices=["process", "pod"], default="process")
    p.add_argument("--pool")
    a = p.parse_args(argv)
    if a.cmd == "plan":
        st = plan(a.state, a.run_dir, a.namespace, [h for h in a.hosts.split(",") if h], a.kill, a.pool)
        for name, pr in st["procs"].items():
            print("fleet: %s (%s, pod %s) pid %s port %s" % (name, pr["role"], st["kind"]["pods"][name], pr["pid"],
                                                              pr["port"]))
        return 0
    if a.cmd == "forward":
        forward(a.state)
        return 0
    st = json.load(open(a.state))
    (hold if a.cmd == "hold" else release)(st, a.proc)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
