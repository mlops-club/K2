#!/usr/bin/env bash
# Topology "fleet" (PRD section 3): one K2_ROLE=controller k2-daemon and two
# K2_ROLE=host k2-daemons (host-1, host-2), each under its own throwaway
# HOME. Contract: README.md here. The processes are managed by
# tests/lab/labkit/fleet.py, which writes the state file every other lab
# piece reads ($LAB_FLEET_STATE: ports, pids, homes, trace files) and can
# kill and restart any of them (scenarios S6, S8, S9; --chaos).
#
# LAB_FLEET_BACKEND: procs (default; processes on this machine) or compose
# (deploy/compose/compose.lab-fleet.yaml, Docker; scripts/lab/lib/compose.sh).
# LAB_FLEET_LAYOUT: fleet (default) or solo. prove.sh G2 --topology solo
# uses this adapter with layout solo: today's daemon (in-process agent
# host), under the same manager, so S6, S7 and chaos can restart it.
#
# Needs: LAB_REPO, LAB_DAEMON_BIN, LAB_FAKE_AGENT_BIN (set by prove.sh).

FLEET_STATE=""

topo_up() {
    local run_dir=$1 layout=${LAB_FLEET_LAYOUT:-fleet} backend=${LAB_FLEET_BACKEND:-procs}
    # Daemon logs and traces/ go at the top of the bundle (prove.sh exports
    # RUN); the state file sits in <run-dir> (topology/).
    local evidence=${RUN:-$run_dir}
    FLEET_STATE="$run_dir/fleet.json"
    LAB_FLEET_STATE=$FLEET_STATE
    export LAB_FLEET_STATE
    case "$backend" in
        procs)
            python3 -I "$LAB_REPO/tests/lab/lab.py" fleet up --layout "$layout" --run-dir "$evidence" \
                --state "$FLEET_STATE" --daemon-bin "$LAB_DAEMON_BIN" --fake-bin "$LAB_FAKE_AGENT_BIN" || return 1
            ;;
        compose)
            [ "$layout" = fleet ] || { echo "fleet: the compose backend has no solo layout" >&2; return 1; }
            # shellcheck source=../lib/compose.sh
            source "$LAB_REPO/scripts/lab/lib/compose.sh"
            compose_fleet_up "$evidence" "$FLEET_STATE" || return 1
            ;;
        *) echo "fleet: LAB_FLEET_BACKEND is procs or compose (got '$backend')" >&2; return 1 ;;
    esac
    eval "$(python3 -I - "$FLEET_STATE" <<'PY'
import json, shlex, sys
st = json.load(open(sys.argv[1]))
ctl = st["procs"][st["controller"]]
first = st["hosts"][0]
tok = open(ctl["home"] + "/.k2/daemon.token").read().strip()
for k, v in {"LAB_PORT": ctl["port"], "LAB_TOKEN": tok, "LAB_HOME": first["home"],
             "LAB_WORKSPACE": st["work"] + "/lab-agent", "LAB_FAKE_TRACE": first["fake_trace"]}.items():
    print("%s=%s" % (k, shlex.quote(str(v))))
PY
)" || return 1
    mkdir -p "$LAB_WORKSPACE" && git -C "$LAB_WORKSPACE" init -q
    export LAB_PORT LAB_TOKEN LAB_HOME LAB_WORKSPACE LAB_FAKE_TRACE
}

topo_describe() {
    local dir=$1
    mkdir -p "$dir"
    {
        echo "topology: ${LAB_FLEET_LAYOUT:-fleet} (backend ${LAB_FLEET_BACKEND:-procs}, $(uname -sm))"
        echo "daemon_bin: $LAB_DAEMON_BIN"
        echo "daemon_version: $("$LAB_DAEMON_BIN" --version 2>&1 | head -1)"
        echo "daemon_sha256: $(shasum -a 256 "$LAB_DAEMON_BIN" | awk '{print $1}')"
        echo "fake_agent: $("$LAB_FAKE_AGENT_BIN" --version 2>&1)"
        echo "state: ${FLEET_STATE#"$LAB_REPO"/}"
    } > "$dir/versions.txt"
    python3 -I - "$FLEET_STATE" "$dir" <<'PY'
import json, re, sys, urllib.request
st, out = json.load(open(sys.argv[1])), sys.argv[2]
secret = re.compile(r"(TOKEN|SECRET|KEY|PASS|AUTH)")
with open(out + "/env.txt", "w") as fh:
    for name, p in st["procs"].items():
        fh.write("# %s (%s) pid %s port %s\n" % (name, p["role"], p["pid"], p["port"]))
        for k, v in sorted(p["env"].items()):
            fh.write("%s=%s\n" % (k, "<redacted>" if secret.search(k) else v))
for name, p in st["procs"].items():
    for path, f in (("/boot-status", "boot-status"), ("/cli/hosts/status", "hosts-status")):
        try:
            body = urllib.request.urlopen("http://127.0.0.1:%d%s" % (p["port"], path), timeout=3).read().decode()
        except OSError as e:
            body = json.dumps({"error": str(e)})
        open("%s/%s-%s.json" % (out, name, f), "w").write(body)
PY
}

topo_metrics() {
    python3 -I - "$FLEET_STATE" <<'PY'
import json, subprocess, sys
st = json.load(open(sys.argv[1]))
for name, p in st["procs"].items():
    rss = subprocess.run(["ps", "-o", "rss=", "-p", str(p["pid"])], capture_output=True, text=True).stdout.strip() if p["pid"] else ""
    metric = "daemon_rss" if name == st["controller"] else "%s_rss" % name
    print("%s,%s,KiB,ps -o rss of %s (%s) pid %s" % (metric, rss or "NA", name, p["role"], p["pid"]))
PY
}

topo_down() {
    [ -n "$FLEET_STATE" ] && [ -f "$FLEET_STATE" ] || return 0
    if [ "${LAB_FLEET_BACKEND:-procs}" = compose ]; then
        source "$LAB_REPO/scripts/lab/lib/compose.sh"
        compose_fleet_down "$FLEET_STATE"
    else
        python3 -I "$LAB_REPO/tests/lab/lab.py" fleet down --state "$FLEET_STATE"
        local root
        root=$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["root"])' "$FLEET_STATE")
        case "$root" in */k2lab-fleet-*|*/k2lab-solo-*) chmod -R u+w "$root" 2>/dev/null; rm -rf "$root" ;; esac
    fi
    FLEET_STATE=""
}
