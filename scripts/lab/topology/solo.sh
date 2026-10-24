#!/usr/bin/env bash
# Topology "solo": today's k2-daemon on this machine (controller and agent
# host in one process), under a throwaway HOME. Contract: README.md here.
#
# Needs: LAB_REPO, LAB_DAEMON_BIN, LAB_FAKE_AGENT_BIN (set by prove.sh).
# Like tests/cli/_sandbox_daemon.sh: own HOME, TMPDIR and runtime dir, so the
# real ~/.k2, ~/.claude and keychain are never reached (a temp HOME keeps the
# vault key in a file). Agent CLIs resolve only inside the shim dir
# (K2_TEST_AGENT_SHIM_DIR, the daemon's spawn guard), and the shim dir is
# also first on PATH.

SOLO_ROOT=""
SOLO_PID=""
SOLO_ENV=()

topo_up() {
    local run_dir=$1
    SOLO_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/k2lab-solo-XXXXXX") || return 1
    mkdir -p "$SOLO_ROOT/home" "$SOLO_ROOT/tmp" "$SOLO_ROOT/run" "$SOLO_ROOT/shim"
    "$LAB_FAKE_AGENT_BIN" --install-shims "$SOLO_ROOT/shim" || return 1
    LAB_HOME="$SOLO_ROOT/home"
    LAB_WORKSPACE="$LAB_HOME/work/lab-agent"
    LAB_FAKE_TRACE="$run_dir/fake-agent.trace.jsonl"
    mkdir -p "$LAB_WORKSPACE"
    git -C "$LAB_WORKSPACE" init -q
    : > "$LAB_FAKE_TRACE"
    export LAB_HOME LAB_WORKSPACE LAB_FAKE_TRACE

    SOLO_ENV=(
        PATH="$SOLO_ROOT/shim:/usr/bin:/bin:/usr/sbin:/sbin"
        HOME="$LAB_HOME" TMPDIR="$SOLO_ROOT/tmp" XDG_RUNTIME_DIR="$SOLO_ROOT/run" K2_RUNTIME_DIR=
        USER="$(id -un)" LANG=en_US.UTF-8
        K2_TEST_AGENT_SHIM_DIR="$SOLO_ROOT/shim"
        K2_FAKE_AGENT_TRACE="$LAB_FAKE_TRACE"
        K2_FAKE_AGENT_TURN_MS="${LAB_TURN_MS:-300}" K2_FAKE_AGENT_SEED="${LAB_SEED:-0}"
    )
    env -i "${SOLO_ENV[@]}" "$LAB_DAEMON_BIN" >"$run_dir/daemon.log" 2>&1 &
    SOLO_PID=$!
    echo "$SOLO_PID" > "$SOLO_ROOT/daemon.pid"

    local i phase="" port=""
    for i in $(seq 1 300); do
        if ! kill -0 "$SOLO_PID" 2>/dev/null; then
            echo "solo: k2-daemon exited during boot (see daemon.log)" >&2
            return 1
        fi
        if [ -s "$LAB_HOME/.k2/daemon.port" ]; then
            port=$(cat "$LAB_HOME/.k2/daemon.port")
            phase=$(curl -s --connect-timeout 1 "http://127.0.0.1:$port/boot-status" 2>/dev/null \
                | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("phase",""))' 2>/dev/null || true)
            [ "$phase" = ready ] && break
        fi
        sleep 0.2
    done
    if [ "$phase" != ready ]; then
        echo "solo: k2-daemon never reached boot phase ready in 60 s (last: '$phase')" >&2
        return 1
    fi
    LAB_PORT=$port
    LAB_TOKEN=$(cat "$LAB_HOME/.k2/daemon.token")
    export LAB_PORT LAB_TOKEN
}

topo_describe() {
    local dir=$1
    mkdir -p "$dir"
    {
        echo "topology: solo (k2-daemon on $(uname -sm), in-process agent host)"
        echo "daemon_bin: $LAB_DAEMON_BIN"
        echo "daemon_version: $("$LAB_DAEMON_BIN" --version 2>&1 | head -1)"
        echo "daemon_sha256: $(shasum -a 256 "$LAB_DAEMON_BIN" | awk '{print $1}')"
        echo "fake_agent: $("$LAB_FAKE_AGENT_BIN" --version 2>&1)"
        echo "os: $(sw_vers -productName 2>/dev/null || uname -s) $(sw_vers -productVersion 2>/dev/null || uname -r)"
        echo "rustc: $(rustc --version 2>&1)"
        echo "python: $(python3 --version 2>&1)"
        echo "home: $LAB_HOME (removed by topo_down)"
    } > "$dir/versions.txt"
    curl -s --connect-timeout 2 "http://127.0.0.1:$LAB_PORT/boot-status" > "$dir/boot-status.json" || true
    # The daemon's whole environment (it starts under env -i), secrets
    # redacted: any name with TOKEN, SECRET, KEY, PASS or AUTH.
    printf '%s\n' "${SOLO_ENV[@]}" \
        | sed -E 's/^([A-Z0-9_]*(TOKEN|SECRET|KEY|PASS|AUTH)[A-Z0-9_]*)=.*/\1=<redacted>/' > "$dir/env.txt"
    ls -la "$LAB_HOME/.k2" > "$dir/k2-home-listing.txt" 2>&1 || true
}

topo_metrics() {
    local rss
    rss=$(ps -o rss= -p "$SOLO_PID" 2>/dev/null | tr -d ' ')
    echo "daemon_rss,${rss:-NA},KiB,ps -o rss of k2-daemon pid $SOLO_PID"
}

topo_down() {
    if [ -n "$SOLO_PID" ]; then
        kill "$SOLO_PID" 2>/dev/null || true
        local i
        for i in $(seq 1 25); do kill -0 "$SOLO_PID" 2>/dev/null || break; sleep 0.2; done
        kill -9 "$SOLO_PID" 2>/dev/null || true
        wait "$SOLO_PID" 2>/dev/null || true
        SOLO_PID=""
    fi
    if [ -n "$SOLO_ROOT" ] && [ -d "$SOLO_ROOT" ]; then
        # Agents, or an frpc, the daemon left behind under this sandbox.
        pkill -9 -f "$SOLO_ROOT/" 2>/dev/null || true
        rm -rf "$SOLO_ROOT"
        SOLO_ROOT=""
    fi
}
