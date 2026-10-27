"""S13 on kind, the host-pool tier (P4 T5, design section 4 of
docs/prd/progress/p4-plan.md). TEST ONLY: namespace k2-kind-p4
(scripts/kind/p4-up.sh --pool=1:2 --max-awake=50), the lab's fake agent as
every CLI, k2lab through an explicit kubeconfig.

    python3 -I scripts/kind/s13.py [--registered 500] [--awake 100] [--resume 20] [--out DIR]

1. register N agents: N workspaces on the controller, each added and
   agent-enabled (`/cli/projects/add-from-path`, `/cli/projects/update`);
2. with N registered and none awake: processes per sleeping agent on the
   hosts (NFR2: zero);
3. wake A of them (`/cli/workspace/msg`, `wake=true`), one after another;
   wake-to-ready per agent, fresh: from the message (the controller pod's
   clock) to the agent CLI's start (`cli.exec`) and to its first answered
   turn (`cli.turn.end`), on the hosts' clocks (one kernel: the Docker VM);
   the pool's scale-ups;
4. at A awake: pods and volumes; per pod, the RSS of its containerd-shim
   and pause process; kubelet CPU per node over 60 s; the controller's CPU
   over 60 s and RSS; the hosts' processes (fake CLIs, k2-ptyd, daemon);
5. resumed: R of the awake agents are closed (asleep, home saved), then
   woken again: wake-to-ready with `--resume`;
6. after a pod delete: the host pod holding the most awake agents is
   deleted (SIGTERM, its drain releases them); the StatefulSet brings it
   back and its agents resume there: from the delete to each agent's CLI.

Writes s13.json and s13.md (the numbers beside the PRD's lab numbers) to
--out (default lab-runs/kind-p4/s13-<UTC>/). The Sandbox tier's column
waits for P4 T2. Prints no token: the controller is called inside its pod.
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import time

NS = "k2-kind-p4"
CLUSTER = "k2lab"
CTL = "k2-controller-0"
WORK = "/var/lib/k2/work/s13"
REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def kubeconfig():
    state = os.path.join(os.environ.get("TMPDIR", "/tmp"), "k2-kind", "p4")
    os.makedirs(state, mode=0o700, exist_ok=True)
    path = os.path.join(state, "kubeconfig")
    with open(path, "w") as fh:
        fh.write(subprocess.run(["kind", "get", "kubeconfig", "--name", CLUSTER], capture_output=True, text=True,
                                check=True).stdout)
    os.chmod(path, 0o600)
    return path


KC = None


def k(*args, timeout=120, check=True, stdin=None):
    r = subprocess.run(["kubectl", "--kubeconfig", KC, "-n", NS] + list(args), capture_output=True, text=True,
                       timeout=timeout, input=stdin)
    if check and r.returncode != 0:
        raise RuntimeError("kubectl %s: %s" % (" ".join(args[:4]), r.stderr.strip()[-300:]))
    return r.stdout


def ctl_sh(script, *args, timeout=600, stdin=None):
    """A shell script in the controller pod; $TOKEN_Q is "?token=…" (read there)."""
    pre = 'TOKEN_Q="?token=$(cat /var/lib/k2/.k2/daemon.token)"\n'
    return k("exec", "-i" if stdin else "-t=false", CTL, "-c", "k2-daemon", "--", "sh", "-c", pre + script, "_",
             *args, timeout=timeout, stdin=stdin)


def node_sh(node, script, timeout=120):
    return subprocess.run(["docker", "exec", node, "sh", "-c", script], capture_output=True, text=True,
                          timeout=timeout).stdout


def hosts():
    out = k("get", "pods", "-l", "app.kubernetes.io/name=k2-host", "-o",
            "jsonpath={range .items[*]}{.metadata.name} {.spec.nodeName} {.status.phase}{\"\\n\"}{end}")
    return [line.split() for line in out.splitlines() if line.strip()]


def host_sh(pod, script, timeout=120):
    return k("exec", pod, "-c", "k2-host", "--", "sh", "-c", script, timeout=timeout, check=False)


def trace(pod, name):
    out = host_sh(pod, 'cat "$HOME/.k2-lab/traces/%s" 2>/dev/null' % name, timeout=180)
    evs = []
    for line in out.splitlines():
        try:
            evs.append(json.loads(line))
        except ValueError:
            pass
    return evs


def ts_ms(iso):
    from datetime import datetime, timezone
    return int(datetime.strptime(iso.rstrip("Z"), "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc).timestamp() * 1000)


def ours(e):
    a = e.get("attrs") or {}
    return (a.get("cwd") or "").startswith(WORK + "/") and a.get("as", "claude") == "claude"


def agent_of(e):
    return os.path.basename((e.get("attrs") or {}).get("cwd") or "")


def pct(values, p):
    if not values:
        return None
    v = sorted(values)
    return v[min(len(v) - 1, int(round(p / 100.0 * (len(v) - 1))))]


def summary(ms):
    return {"n": len(ms), "p50_ms": pct(ms, 50), "p95_ms": pct(ms, 95), "max_ms": max(ms) if ms else None}


# ── measurements ────────────────────────────────────────────────────────

PROC_SH = r'''
for p in /proc/[0-9]*; do
    [ "${p#/proc/}" = $$ ] && continue
    c=$(tr "\0" " " < $p/cmdline 2>/dev/null) || continue
    [ -n "$c" ] || continue
    r=$(awk '/^VmRSS/{print $2}' $p/status 2>/dev/null)
    echo "${p#/proc/} ${r:-0} $c"
done
'''


def host_procs(pod):
    """{"cli": n, "cli_rss_kb": [..], "ptyd_rss_kb": n, "daemon_rss_kb": n, "other": n}"""
    out = host_sh(pod, PROC_SH)
    res = {"cli": 0, "cli_rss_kb": [], "ptyd_rss_kb": 0, "daemon_rss_kb": 0, "processes": 0}
    for line in out.splitlines():
        parts = line.split(" ", 2)
        if len(parts) < 3:
            continue
        rss, cmd = int(parts[1] or 0), parts[2]
        res["processes"] += 1
        if cmd.startswith("/opt/k2-fake/shim/") or "/k2-fake-agent " in cmd:
            res["cli"] += 1
            res["cli_rss_kb"].append(rss)
        elif "k2-ptyd" in cmd:
            res["ptyd_rss_kb"] += rss
        elif cmd.startswith("k2-daemon"):
            res["daemon_rss_kb"] += rss
    return res


def controller_cpu_rss(seconds):
    pid = "$(cat /var/lib/k2/.k2-lab/daemon.pid)"
    script = ("p=%s; a=$(awk '{print $14+$15}' /proc/$p/stat); sleep %d; b=$(awk '{print $14+$15}' /proc/$p/stat); "
              "echo $a $b $(awk '/^VmRSS/{print $2}' /proc/$p/status)" % (pid, seconds))
    a, b, rss = k("exec", CTL, "-c", "k2-daemon", "--", "sh", "-c", script, timeout=seconds + 60).split()
    return {"cpu_core": round((int(b) - int(a)) / 100.0 / seconds, 4), "rss_mb": round(int(rss) / 1024.0, 1)}


def kubelet_cpu(node, seconds):
    script = ("p=$(pgrep -x kubelet); a=$(awk '{print $14+$15}' /proc/$p/stat); sleep %d; "
              "b=$(awk '{print $14+$15}' /proc/$p/stat); echo $a $b $(getconf CLK_TCK)" % seconds)
    a, b, hz = node_sh(node, script, timeout=seconds + 60).split()
    return round((int(b) - int(a)) / float(hz) / seconds, 4)


def pod_overhead(node):
    """Per pod of this namespace on the node: RSS (kB) of its containerd-shim and its pause process."""
    script = r'''
for id in $(crictl pods --namespace %s -q 2>/dev/null); do
    name=$(crictl inspectp -o go-template --template '{{.status.metadata.name}}' $id 2>/dev/null)
    shim=""; pause=""
    for p in /proc/[0-9]*; do
        c=$(tr "\0" " " < $p/cmdline 2>/dev/null) || continue
        case "$c" in *containerd-shim*"-id $id"*) shim=${p#/proc/} ;; esac
    done
    [ -n "$shim" ] && for c in /proc/$shim/task/*/children; do for ch in $(cat $c 2>/dev/null); do
        case "$(tr "\0" " " < /proc/$ch/cmdline 2>/dev/null)" in /pause*) pause=$ch ;; esac; done; done
    rs=$( [ -n "$shim" ] && awk '/^VmRSS/{print $2}' /proc/$shim/status); rp=$( [ -n "$pause" ] && awk '/^VmRSS/{print $2}' /proc/$pause/status)
    echo "$name ${rs:-0} ${rp:-0}"
done''' % NS
    out = {}
    for line in node_sh(node, script).splitlines():
        parts = line.split()
        if len(parts) == 3:
            out[parts[0]] = {"shim_kb": int(parts[1]), "pause_kb": int(parts[2])}
    return out


def snapshot(label, cpu_seconds=0):
    hs = hosts()
    snap = {"label": label, "at": time.strftime("%H:%M:%SZ", time.gmtime()), "hosts": {}, "nodes": {}}
    for name, node, phase in hs:
        snap["hosts"][name] = {"node": node, "phase": phase}
        if phase == "Running":
            snap["hosts"][name]["procs"] = host_procs(name)
    nodes = sorted({node for _, node, _ in hs} | {k("get", "pod", CTL, "-o", "jsonpath={.spec.nodeName}")})
    for node in nodes:
        snap["nodes"][node] = {"pods": pod_overhead(node)}
    snap["pvcs"] = len(k("get", "pvc", "-o", "name").split())
    snap["pods"] = len(k("get", "pods", "-o", "name").split())
    if cpu_seconds:
        snap["controller"] = controller_cpu_rss(cpu_seconds)
        for node in nodes:
            snap["nodes"][node]["kubelet_cpu_core"] = kubelet_cpu(node, cpu_seconds)
    return snap


# ── the steps ───────────────────────────────────────────────────────────

def register(n):
    script = r'''
mkdir -p "$1"
i=0
while [ $i -lt $2 ]; do
    name=$(printf 'a-%03d' $i); d="$1/$name"
    mkdir -p "$d" && [ -d "$d/.git" ] || git init -q "$d"   # add-from-path wants a git repo (needsGitInit)
    r=$(curl -sS -X POST -H 'Content-Type: application/json' \
        -d "{\"path\":\"$d\",\"seed_wiki\":false,\"seed_agents_md\":false}" "http://127.0.0.1:7420/cli/projects/add-from-path$TOKEN_Q")
    id=$(printf '%s' "$r" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p' | head -1)
    curl -sS -o /dev/null -X POST -H 'Content-Type: application/json' -d "{\"id\":\"$id\",\"agentEnabled\":1}" \
        "http://127.0.0.1:7420/cli/projects/update$TOKEN_Q"
    echo "$name $id"
    i=$((i + 1))
done'''
    t = time.monotonic()
    out = ctl_sh(script, WORK, str(n), timeout=3600)
    ids = dict(line.split() for line in out.splitlines() if len(line.split()) == 2)
    return ids, round(time.monotonic() - t, 1)


def wake(names, text):
    """Messages with wake=true, one after another, from the controller pod; {name: send ms}."""
    script = r'''
for name in $NAMES; do
    echo "$name $(date +%s%3N)"
    curl -sS -o /dev/null --max-time 120 -X POST "http://127.0.0.1:7420/cli/workspace/msg$TOKEN_Q" \
        --data-urlencode "workspace=$1/$name" --data-urlencode "text=$2" --data-urlencode "from=s13" --data-urlencode "wake=true"
done'''
    out = ctl_sh("NAMES='%s'\n" % " ".join(names) + script, WORK, text, timeout=7200)
    return {p[0]: int(p[1]) for p in (line.split() for line in out.splitlines()) if len(p) == 2}


def exec_times(names, since_ms, resumed=None):
    """{name: (exec ms, first turn end ms)} for CLIs of these agents started after since_ms."""
    want, res = set(names), {}
    for pod, _, phase in hosts():
        if phase != "Running":
            continue
        evs = trace(pod, "fake.jsonl")
        starts = {}
        for e in evs:
            if e.get("event") == "cli.exec" and ours(e) and agent_of(e) in want and ts_ms(e["ts"]) >= since_ms:
                if resumed is not None and bool((e.get("attrs") or {}).get("resumed")) != resumed:
                    continue
                starts.setdefault(e.get("pid"), (agent_of(e), ts_ms(e["ts"]), pod))
        for e in evs:
            if e.get("event") == "cli.turn.end" and e.get("pid") in starts:
                name, t0, pod_ = starts[e["pid"]]
                if name not in res or res[name][0] > t0:
                    res[name] = (t0, ts_ms(e["ts"]), pod_)
        for name, t0, pod_ in starts.values():
            res.setdefault(name, (t0, None, pod_))
    return res


def wait_execs(names, since_ms, deadline_s, resumed=None, answers=True):
    """Until every agent's CLI started (and, with answers, answered a turn)."""
    end = time.monotonic() + deadline_s
    got = {}
    while time.monotonic() < end:
        got = exec_times(names, since_ms, resumed)
        if len(got) >= len(names) and (not answers or all(v[1] for v in got.values())):
            break
        time.sleep(10)
    return got


def latencies(sent, got):
    to_exec = [got[n][0] - sent[n] for n in sent if n in got]
    to_turn = [got[n][1] - sent[n] for n in sent if n in got and got[n][1]]
    return {"to_cli": summary(to_exec), "to_answer": summary(to_turn), "missing": sorted(set(sent) - set(got))}


def close(ids, names):
    script = r'''
for id in $IDS; do
    curl -sS -o /dev/null -X POST -H 'Content-Type: application/json' -d "{\"agent_name\":\"$id\",\"force\":true}" \
        "http://127.0.0.1:7420/cli/sessions/v2/close$TOKEN_Q"
done'''
    ctl_sh("IDS='%s'\n" % " ".join(ids[n] for n in names) + script, timeout=1800)


def now_ms_in_pod():
    return int(k("exec", CTL, "-c", "k2-daemon", "--", "date", "+%s%3N").strip())


def pool_events():
    out = k("exec", CTL, "-c", "k2-daemon", "--", "sh", "-c", "cat /var/lib/k2/.k2-lab/traces/daemon.jsonl", timeout=180)
    return [json.loads(line) for line in out.splitlines() if '"ctl.pool_scale"' in line]


def main():
    global KC
    ap = argparse.ArgumentParser()
    ap.add_argument("--registered", type=int, default=500)
    ap.add_argument("--awake", type=int, default=100)
    ap.add_argument("--resume", type=int, default=20)
    ap.add_argument("--cpu-seconds", type=int, default=60)
    ap.add_argument("--out")
    a = ap.parse_args()
    KC = kubeconfig()
    out = a.out or os.path.join(REPO, "lab-runs", "kind-p4", "s13-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()))
    os.makedirs(out, exist_ok=True)
    env = {e.split("=", 1)[0]: e.split("=", 1)[1] for e in k(
        "get", "statefulset", "k2-controller", "-o",
        "jsonpath={range .spec.template.spec.containers[0].env[*]}{.name}={.value}{\"\\n\"}{end}").splitlines() if "=" in e}
    henv = {e.split("=", 1)[0]: e.split("=", 1)[1] for e in k(
        "get", "statefulset", "k2-host", "-o",
        "jsonpath={range .spec.template.spec.containers[0].env[*]}{.name}={.value}{\"\\n\"}{end}").splitlines() if "=" in e}
    rep = {"namespace": NS, "pool": {"min": env.get("K2_PROVISIONER_MIN_HOSTS"), "max": env.get("K2_PROVISIONER_MAX_HOSTS"),
                                     "max_awake": henv.get("K2_HOST_MAX_AWAKE")},
           "registered": a.registered, "awake": a.awake}
    log = lambda m: print(time.strftime("%H:%M:%S ") + m, flush=True)

    log("baseline")
    rep["baseline"] = snapshot("baseline")
    log("registering %d agents" % a.registered)
    ids, secs = register(a.registered)
    rep["register"] = {"agents": len(ids), "seconds": secs}
    if len(ids) < a.registered:
        print("s13: registered %d of %d (the harness can't go on)" % (len(ids), a.registered), file=sys.stderr)
        return 1
    names = sorted(ids)
    log("registered %d in %ss; sleeping-agent processes" % (len(ids), secs))
    rep["registered_asleep"] = snapshot("%d registered, none awake" % len(ids))

    wake_names = names[:a.awake]
    log("waking %d" % len(wake_names))
    t0 = now_ms_in_pod()
    sent = wake(wake_names, "s13 [[remember: awake]]")
    got = wait_execs(wake_names, t0, 1200)
    rep["wake_fresh"] = latencies(sent, got)
    rep["wake_fresh"]["hosts"] = {h: sum(1 for v in got.values() if v[2] == h) for h, _, _ in hosts()}
    rep["pool_scale"] = [{"ts": e["ts"], "attrs": e.get("attrs")} for e in pool_events()]
    log("fresh: %s" % json.dumps(rep["wake_fresh"])[:300])

    log("at %d awake: overhead, kubelet and controller CPU over %ds" % (len(got), a.cpu_seconds))
    rep["awake_snapshot"] = snapshot("%d awake of %d" % (len(got), len(ids)), a.cpu_seconds)

    sample = wake_names[:a.resume]
    log("closing %d, then waking them again (resumed)" % len(sample))
    close(ids, sample)
    time.sleep(15)
    t1 = now_ms_in_pod()
    sent_r = wake(sample, "s13 again [[remember: resumed]]")
    got_r = wait_execs(sample, t1, 900, resumed=True)
    rep["wake_resumed"] = latencies(sent_r, got_r)
    log("resumed: %s" % json.dumps(rep["wake_resumed"])[:300])

    per_host = {}
    for n, v in got.items():
        per_host.setdefault(v[2], []).append(n)
    victim = max(per_host, key=lambda h: len(per_host[h])) if per_host else None
    if victim:
        on_victim = [n for n in per_host[victim] if n not in sample]
        log("deleting pod %s (%d awake agents)" % (victim, len(on_victim)))
        t2 = now_ms_in_pod()
        k("delete", "pod", victim, "--wait=false")
        # Resumed after the delete, they run no turn until a message: only their CLIs.
        got_d = wait_execs(on_victim, t2, 1200, answers=False)
        lat = [got_d[n][0] - t2 for n in on_victim if n in got_d]
        rep["after_pod_delete"] = {"pod": victim, "agents": len(on_victim), "to_cli": summary(lat),
                                   "missing": sorted(set(on_victim) - set(got_d))}
        log("after pod delete: %s" % json.dumps(rep["after_pod_delete"])[:300])

    rep["end"] = snapshot("end")
    with open(os.path.join(out, "s13.json"), "w") as fh:
        json.dump(rep, fh, indent=1)
    write_md(rep, os.path.join(out, "s13.md"))
    print("evidence: %s" % out)
    short = [what for what, got_n, want in (
        ("fresh wakes", rep["wake_fresh"]["to_cli"]["n"], len(wake_names)),
        ("resumed wakes", rep["wake_resumed"]["to_cli"]["n"], len(sample)),
        ("agents back after the pod delete", rep.get("after_pod_delete", {}).get("to_cli", {}).get("n", 0),
         rep.get("after_pod_delete", {}).get("agents", 0))) if got_n < want]
    if short:
        print("s13: INCOMPLETE: %s (see s13.json \"missing\")" % ", ".join(short))
        return 1
    print("s13: complete")
    return 0


def write_md(rep, path):
    aw = rep.get("awake_snapshot", {})
    ra = rep.get("registered_asleep", {})
    pods = {name: v for node in aw.get("nodes", {}).values() for name, v in node.get("pods", {}).items()}
    shim = [v["shim_kb"] for v in pods.values() if v["shim_kb"]]
    pause = [v["pause_kb"] for v in pods.values() if v["pause_kb"]]
    clis = [kb for h in aw.get("hosts", {}).values() for kb in (h.get("procs") or {}).get("cli_rss_kb", [])]
    asleep_clis = sum((h.get("procs") or {}).get("cli", 0) for h in ra.get("hosts", {}).values())
    kub = {n: v.get("kubelet_cpu_core") for n, v in aw.get("nodes", {}).items()}
    f = lambda x: "-" if x is None else x
    fresh, res, dele = rep.get("wake_fresh", {}), rep.get("wake_resumed", {}), rep.get("after_pod_delete", {})
    rows = [
        ("Pods for %s awake agents" % rep["awake"], "%d host pods (pool %s..%s, max_awake %s); %d pods in the namespace" % (
            len(aw.get("hosts", {})), rep["pool"]["min"], rep["pool"]["max"], rep["pool"]["max_awake"], aw.get("pods", 0)),
         "pending T2 (one per awake agent)", "-"),
        ("Per-pod overhead: containerd-shim RSS", "%s kB median of %d pods" % (f(pct(shim, 50)), len(shim)), "pending T2",
         "about 12 MiB per idle pod (all of it)"),
        ("Per-pod overhead: pause RSS", "%s kB median" % f(pct(pause, 50)), "pending T2", "(in the above)"),
        ("Per extra awake agent in a host pod", "fake CLI RSS %s kB median (a fake; real CLIs are owner-run)" % f(pct(clis, 50)),
         "-", "about 0.3 MiB per extra process; 330-490 MB per live claude CLI"),
        ("Kubelet CPU, busiest node", "%s core (per node: %s)" % (f(max(kub.values()) if kub else None), kub), "pending T2",
         "13-23 % of a core for 100 idle pods vs 2-4 %"),
        ("Volumes", "%s PVCs" % aw.get("pvcs"), "pending T2", "-"),
        ("Wake to CLI, fresh: p50 / p95", "%s / %s ms" % (f(fresh.get("to_cli", {}).get("p50_ms")), f(fresh.get("to_cli", {}).get("p95_ms"))),
         "pending T2", "Sandbox ready 6 s fresh"),
        ("Wake to answer, fresh: p50 / p95", "%s / %s ms" % (f(fresh.get("to_answer", {}).get("p50_ms")), f(fresh.get("to_answer", {}).get("p95_ms"))), "pending T2", "-"),
        ("Wake to CLI, resumed: p50 / p95", "%s / %s ms" % (f(res.get("to_cli", {}).get("p50_ms")), f(res.get("to_cli", {}).get("p95_ms"))),
         "pending T2", "-"),
        ("Back after a pod delete: p50 / p95", "%s / %s ms (%s agents on %s)" % (
            f(dele.get("to_cli", {}).get("p50_ms")), f(dele.get("to_cli", {}).get("p95_ms")), dele.get("agents"), dele.get("pod")),
         "pending T2", "Sandbox 34 s after a pod delete"),
        ("Controller CPU / RSS at %d awake" % rep["awake"], "%s core / %s MB" % (
            f(aw.get("controller", {}).get("cpu_core")), f(aw.get("controller", {}).get("rss_mb"))), "pending T2", "-"),
        ("Processes per sleeping agent (NFR2)", "%d agent CLIs with %d registered, none awake" % (asleep_clis, rep["register"]["agents"]),
         "pending T2", "zero"),
    ]
    with open(path, "w") as fh:
        fh.write("# S13 on kind: host pool (fake agents)\n\n")
        fh.write("| Measure | Host pool (this run) | Sandbox tier | PRD lab numbers |\n|---|---|---|---|\n")
        for r in rows:
            fh.write("| %s |\n" % " | ".join(str(x) for x in r))
        fh.write("\nRegistered %(agents)s agents in %(seconds)s s. Pool scale events: %(scale)s. Missing: fresh %(mf)s, resumed %(mr)s, after delete %(md)s.\n" % {
            "agents": rep["register"]["agents"], "seconds": rep["register"]["seconds"],
            "scale": len(rep.get("pool_scale", [])), "mf": fresh.get("missing"), "mr": res.get("missing"),
            "md": dele.get("missing")})


if __name__ == "__main__":
    sys.exit(main())
