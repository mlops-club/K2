#!/usr/bin/env bash
# Topology "two-process" (dist-link, PRD P2 first cut): a K2_ROLE=controller
# k2-daemon and one K2_ROLE=host k2-daemon on this machine, each under its own
# throwaway HOME, on fixed ports. The client (lab S1) talks to the controller
# only; agents (k2-fake-agent as `claude`) run as children of the host.
#
# Sourced: the adapter contract in README.md (topo_up, topo_describe,
# topo_metrics, topo_down). Needs LAB_DAEMON_BIN and LAB_FAKE_AGENT_BIN.
#
# Run directly: builds the two binaries (CARGO_BUILD_JOBS=3), then proves
# acceptance items 2 and 3 of docs/prd/progress/dist-link.md and writes the
# evidence under lab-runs/dist-link/<UTC timestamp>/:
#
#   scripts/lab/topology/two-process.sh [--no-build]
#
#   1. controller alone: spawn answers 503 "no agent host is connected"
#   2. host links (GET /cli/hosts/status)
#   3. lab S1 against the controller's port: PASS
#   4. a spawned agent's parent pid is the host daemon, not the controller
#   5. open_url posted to the HOST's /cli/browser/open-url arrives on the
#      CONTROLLER's /cli/sessions/events
#   6. hook events reached the controller (its status counts them)
#   7. the desktop app's way in: an Owner-role Connect user logs in
#      (POST /cli/auth/login) and, with that session token only, spawns
#      through the controller, attaches the grid (starts as a capable viewer,
#      set_mode claimer as the app does), types and sees the reply, receives
#      open_url on /cli/sessions/events, and closes
#
# Ports: DIST_CTL_PORT (default 17420) and DIST_HOST_PORT (17421).

TP_ROOT=""
TP_CTL_PID=""
TP_HOST_PID=""
TP_CTL_ENV=()
TP_HOST_ENV=()
TP_CTL_PORT=${DIST_CTL_PORT:-17420}
TP_HOST_PORT=${DIST_HOST_PORT:-17421}

tp_wait_ready() {
    local name=$1 pid=$2 port=$3 log=$4 i phase=""
    for i in $(seq 1 300); do
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "two-process: $name exited during boot: $(tail -3 "$log" | tr '\n' ' ')" >&2
            return 1
        fi
        phase=$(curl -s --connect-timeout 1 "http://127.0.0.1:$port/boot-status" 2>/dev/null \
            | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("phase",""))' 2>/dev/null || true)
        [ "$phase" = ready ] && return 0
        sleep 0.2
    done
    echo "two-process: $name never reached boot phase ready in 60 s (last: '$phase')" >&2
    return 1
}

# The controller alone (no host yet). Exports LAB_PORT and LAB_TOKEN.
tp_controller_up() {
    local run_dir=$1
    TP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/k2lab-2p-XXXXXX") || return 1
    mkdir -p "$TP_ROOT"/{ctl,host}/{home,tmp,run} "$TP_ROOT/shim" "$run_dir"
    head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$TP_ROOT/host-token"
    TP_CTL_ENV=(
        PATH="/usr/bin:/bin:/usr/sbin:/sbin"
        HOME="$TP_ROOT/ctl/home" K2_HOME="$TP_ROOT/ctl/home/.k2"
        TMPDIR="$TP_ROOT/ctl/tmp" XDG_RUNTIME_DIR="$TP_ROOT/ctl/run" K2_RUNTIME_DIR=
        USER="$(id -un)" LANG=en_US.UTF-8
        K2_ROLE=controller K2_HOSTS=1 K2_HOST_BOOTSTRAP_TOKEN_FILE="$TP_ROOT/host-token"
        K2_DAEMON_PORT="$TP_CTL_PORT"
        K2_TRACE_FILE="$run_dir/controller.trace.jsonl"
    )
    env -i "${TP_CTL_ENV[@]}" "$LAB_DAEMON_BIN" >"$run_dir/controller.log" 2>&1 &
    TP_CTL_PID=$!
    tp_wait_ready controller "$TP_CTL_PID" "$TP_CTL_PORT" "$run_dir/controller.log" || return 1
    LAB_PORT=$TP_CTL_PORT
    LAB_TOKEN=$(cat "$TP_ROOT/ctl/home/.k2/daemon.token")
    export LAB_PORT LAB_TOKEN
}

# The host, linked to the controller. Exports LAB_HOME, LAB_WORKSPACE,
# LAB_FAKE_TRACE (the agent home is the host's).
tp_host_up() {
    local run_dir=$1 i linked=""
    "$LAB_FAKE_AGENT_BIN" --install-shims "$TP_ROOT/shim" || return 1
    LAB_HOME="$TP_ROOT/host/home"
    LAB_WORKSPACE="$LAB_HOME/work/lab-agent"
    LAB_FAKE_TRACE="$run_dir/fake-agent.trace.jsonl"
    mkdir -p "$LAB_WORKSPACE"
    git -C "$LAB_WORKSPACE" init -q
    : > "$LAB_FAKE_TRACE"
    export LAB_HOME LAB_WORKSPACE LAB_FAKE_TRACE
    TP_HOST_ENV=(
        PATH="$TP_ROOT/shim:/usr/bin:/bin:/usr/sbin:/sbin"
        HOME="$LAB_HOME" K2_HOME="$LAB_HOME/.k2"
        TMPDIR="$TP_ROOT/host/tmp" XDG_RUNTIME_DIR="$TP_ROOT/host/run" K2_RUNTIME_DIR=
        USER="$(id -un)" LANG=en_US.UTF-8
        K2_ROLE=host K2_CONTROLLER_URL="ws://127.0.0.1:$TP_CTL_PORT"
        K2_HOST_ENROLL_TOKEN_FILE="$TP_ROOT/host-token" K2_HOST_NAME=host-1
        K2_DAEMON_PORT="$TP_HOST_PORT"
        K2_TRACE_FILE="$run_dir/host.trace.jsonl"
        K2_TEST_AGENT_SHIM_DIR="$TP_ROOT/shim"
        K2_FAKE_AGENT_TRACE="$LAB_FAKE_TRACE"
        K2_FAKE_AGENT_TURN_MS="${LAB_TURN_MS:-300}" K2_FAKE_AGENT_SEED="${LAB_SEED:-0}"
    )
    env -i "${TP_HOST_ENV[@]}" "$LAB_DAEMON_BIN" >"$run_dir/host.log" 2>&1 &
    TP_HOST_PID=$!
    tp_wait_ready host "$TP_HOST_PID" "$TP_HOST_PORT" "$run_dir/host.log" || return 1
    for i in $(seq 1 150); do
        linked=$(curl -s --connect-timeout 1 "http://127.0.0.1:$TP_HOST_PORT/cli/hosts/status" 2>/dev/null \
            | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("linked"))' 2>/dev/null || true)
        [ "$linked" = True ] && return 0
        sleep 0.2
    done
    echo "two-process: host never linked to the controller (status linked=$linked)" >&2
    return 1
}

topo_up() {
    tp_controller_up "$1" && tp_host_up "$1"
}

topo_describe() {
    local dir=$1
    mkdir -p "$dir"
    {
        echo "topology: two-process (K2_ROLE=controller :$TP_CTL_PORT + K2_ROLE=host :$TP_HOST_PORT on $(uname -sm))"
        echo "daemon_bin: $LAB_DAEMON_BIN"
        echo "daemon_version: $("$LAB_DAEMON_BIN" --version 2>&1 | head -1)"
        echo "fake_agent: $("$LAB_FAKE_AGENT_BIN" --version 2>&1)"
        echo "controller_pid: $TP_CTL_PID  host_pid: $TP_HOST_PID"
        echo "homes: $TP_ROOT/{ctl,host}/home (removed by topo_down)"
    } > "$dir/versions.txt"
    curl -s "http://127.0.0.1:$TP_CTL_PORT/boot-status" > "$dir/controller-boot-status.json" || true
    curl -s "http://127.0.0.1:$TP_HOST_PORT/boot-status" > "$dir/host-boot-status.json" || true
    curl -s "http://127.0.0.1:$TP_CTL_PORT/cli/hosts/status" > "$dir/controller-hosts-status.json" || true
    curl -s "http://127.0.0.1:$TP_HOST_PORT/cli/hosts/status" > "$dir/host-hosts-status.json" || true
    { echo "# controller"; printf '%s\n' "${TP_CTL_ENV[@]}"; echo "# host"; printf '%s\n' "${TP_HOST_ENV[@]}"; } \
        | sed -E 's/^([A-Z0-9_]*(TOKEN|SECRET|KEY|PASS|AUTH)[A-Z0-9_]*)=.*/\1=<redacted>/' > "$dir/env.txt"
}

topo_metrics() {
    local c h
    c=$(ps -o rss= -p "$TP_CTL_PID" 2>/dev/null | tr -d ' ')
    h=$(ps -o rss= -p "$TP_HOST_PID" 2>/dev/null | tr -d ' ')
    echo "daemon_rss,${c:-NA},KiB,ps -o rss of the controller pid $TP_CTL_PID"
    echo "host_rss,${h:-NA},KiB,ps -o rss of the host pid $TP_HOST_PID"
}

topo_down() {
    local pid i
    for pid in "$TP_HOST_PID" "$TP_CTL_PID"; do
        [ -n "$pid" ] || continue
        kill "$pid" 2>/dev/null || true
        for i in $(seq 1 25); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
        kill -9 "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    TP_HOST_PID="" TP_CTL_PID=""
    if [ -n "$TP_ROOT" ] && [ -d "$TP_ROOT" ]; then
        pkill -9 -f "$TP_ROOT/" 2>/dev/null || true
        rm -rf "$TP_ROOT"
        TP_ROOT=""
    fi
}

# ── Run directly ─────────────────────────────────────────────────────────
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    set -uo pipefail
    REPO=$(cd "$(dirname "$0")/../../.." && pwd)
    cd "$REPO" || exit 1
    BUILD=1
    [ "${1:-}" = --no-build ] && BUILD=0
    export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-3}
    if ! command -v cargo >/dev/null 2>&1 && [ -x "$HOME/.cache/k2-dist-rust/cargo/bin/cargo" ]; then
        export RUSTUP_HOME=$HOME/.cache/k2-dist-rust/rustup CARGO_HOME=$HOME/.cache/k2-dist-rust/cargo
        export PATH=$CARGO_HOME/bin:$PATH
    fi
    if [ "$BUILD" = 1 ]; then
        # The shared Mac's build-slot queue, when this machine has one.
        slot=()
        [ -x "$HOME/.cache/k2-dist-cargo-slot.sh" ] && slot=("$HOME/.cache/k2-dist-cargo-slot.sh")
        # Two commands: `--bin k2-daemon` would also filter the fake agent out.
        ${slot[@]+"${slot[@]}"} cargo build -p k2-daemon --bin k2-daemon || exit 1
        ${slot[@]+"${slot[@]}"} cargo build -p k2-fake-agent || exit 1
    fi
    target=$(cargo metadata --format-version 1 --no-deps 2>/dev/null \
        | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
    RUN="$REPO/lab-runs/dist-link/$(date -u +%Y%m%dT%H%M%SZ)"
    mkdir -p "$RUN/bin" "$RUN/scenarios"
    cp "$target/debug/k2-daemon" "$target/debug/k2-fake-agent" "$RUN/bin/" || exit 1
    LAB_DAEMON_BIN="$RUN/bin/k2-daemon" LAB_FAKE_AGENT_BIN="$RUN/bin/k2-fake-agent"
    export LAB_DAEMON_BIN LAB_FAKE_AGENT_BIN
    trap topo_down EXIT
    FAILS=0
    check() { # name ok detail
        local line="PASS $1 $3"
        if [ "$2" != 1 ]; then line="FAIL $1 $3"; FAILS=$((FAILS + 1)); fi
        echo "$line" | tee -a "$RUN/checks.txt"
    }
    post() { # port path token body
        curl -s -o "$RUN/post.out" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
            "http://127.0.0.1:$1$2?token=$3" -d "$4"
    }

    echo "evidence: ${RUN#$REPO/}"
    tp_controller_up "$RUN" || { echo "controller failed to start"; exit 1; }
    ctl_token=$LAB_TOKEN

    # 1. zero hosts → a clear error, nothing runs locally
    code=$(post "$TP_CTL_PORT" /cli/sessions/v2/spawn "$ctl_token" \
        '{"agent_name":"no-host-probe","cwd":"/tmp","command":"claude"}')
    cp "$RUN/post.out" "$RUN/zero-hosts-spawn.json"
    check "zero hosts: spawn is refused" "$([ "$code" = 503 ] && grep -q 'no agent host is connected' "$RUN/post.out" && echo 1)" \
        "HTTP $code $(cat "$RUN/post.out")"

    # 2. the host links
    tp_host_up "$RUN" || { echo "host failed to start"; exit 1; }
    host_token=$(cat "$LAB_HOME/.k2/daemon.token")
    topo_describe "$RUN/topology"
    check "host linked" "$(grep -q '"host_id":"host-1"' "$RUN/topology/controller-hosts-status.json" && echo 1)" \
        "$(cat "$RUN/topology/controller-hosts-status.json")"

    # 3. lab S1 against the controller's port
    printf '%s' "$ctl_token" > "$TP_ROOT/ctl-token"
    python3 -I tests/lab/lab.py s1 --port "$TP_CTL_PORT" --token-file "$TP_ROOT/ctl-token" --home "$LAB_HOME" \
        --cwd "$LAB_WORKSPACE" --out "$RUN/scenarios" --fake-trace "$LAB_FAKE_TRACE" | tee "$RUN/s1.out"
    s1=$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["status"])' "$RUN/scenarios/S1.json" 2>/dev/null)
    check "S1 through the controller" "$([ "$s1" = pass ] && echo 1)" "status=$s1"

    # 4. the agent is a child of the host, not the controller
    agent="parent-probe-$$"
    code=$(post "$TP_CTL_PORT" /cli/sessions/v2/spawn "$ctl_token" \
        "{\"agent_name\":\"$agent\",\"cwd\":\"$LAB_WORKSPACE\",\"command\":\"claude\",\"args\":[\"--session-id\",\"$(uuidgen | tr A-Z a-z)\"]}")
    sleep 1
    ps -axo pid=,ppid=,command= > "$RUN/ps.txt"
    agent_ppids=$(awk -v root="$TP_ROOT/shim/" 'index($0, root) {print $2}' "$RUN/ps.txt" | sort -u | tr '\n' ' ')
    {
        echo "controller pid $TP_CTL_PID, host pid $TP_HOST_PID"
        echo "agent processes (pid ppid command):"
        awk -v root="$TP_ROOT/shim/" 'index($0, root)' "$RUN/ps.txt"
        echo "controller children:"; pgrep -lP "$TP_CTL_PID" || echo "(none)"
        echo "host children:"; pgrep -lP "$TP_HOST_PID" || echo "(none)"
    } > "$RUN/process-tree.txt"
    ok=0
    # Only this run's agents: processes whose command is under this run's
    # throwaway shim dir. Exactly one parent, the host, never the controller.
    agent_ppids=$(echo $agent_ppids)
    if [ "$code" = 200 ] && [ -n "$agent_ppids" ] && [ "$agent_ppids" = "$TP_HOST_PID" ] \
        && [ "$agent_ppids" != "$TP_CTL_PID" ] && ! pgrep -P "$TP_CTL_PID" >/dev/null; then ok=1; fi
    check "agent is a child of the host" "$ok" "spawn HTTP $code; agent ppid(s): $agent_ppids; host $TP_HOST_PID; controller $TP_CTL_PID"
    post "$TP_CTL_PORT" /cli/sessions/v2/close "$ctl_token" "{\"agent_name\":\"$agent\",\"force\":true,\"clear_index\":true}" >/dev/null

    # 5. /login: open_url on the host reaches the controller's events socket
    url="https://claude.ai/oauth/authorize?dist-link=$$"
    python3 -I - "$REPO/tests/lab" "$TP_CTL_PORT" "$ctl_token" "$TP_HOST_PORT" "$host_token" "$url" \
        > "$RUN/open-url.txt" 2>&1 <<'PY'
import json, sys, time, urllib.parse, urllib.request
sys.path.insert(0, sys.argv[1])
from labkit import ws as wslib
cport, ctok, hport, htok, url = int(sys.argv[2]), sys.argv[3], int(sys.argv[4]), sys.argv[5], sys.argv[6]
sock = wslib.WebSocket("127.0.0.1", cport, "/cli/sessions/events?" + urllib.parse.urlencode({"token": ctok}), timeout=10)
time.sleep(0.3)
req = urllib.request.Request("http://127.0.0.1:%d/cli/browser/open-url?token=%s" % (hport, htok),
                             data=json.dumps({"url": url}).encode(), method="POST",
                             headers={"Content-Type": "application/json"})
print("host POST:", urllib.request.urlopen(req, timeout=10).read().decode())
deadline = time.time() + 15
while time.time() < deadline:
    sock.settimeout(max(0.1, deadline - time.time()))
    kind, data = sock.recv()
    if kind == "text":
        msg = json.loads(data)
        if msg.get("kind") == "open_url":
            print("controller event:", data)
            sys.exit(0 if msg.get("url") == url else 1)
print("no open_url on the controller")
sys.exit(1)
PY
    rc=$?
    check "open_url from the host reaches the controller's /cli/sessions/events" "$([ $rc = 0 ] && echo 1)" \
        "$(tr '\n' ' ' < "$RUN/open-url.txt")"

    # 6. hooks reached the controller
    curl -s "http://127.0.0.1:$TP_CTL_PORT/cli/hosts/status" > "$RUN/controller-hosts-status-after.json"
    grep -E '\[hosts\] (placed agent|hook )' "$RUN/controller.log" > "$RUN/controller-hosts.log" || true
    hooks=$(grep -c '\[hosts\] hook ' "$RUN/controller.log")
    check "hook events reached the controller" "$([ "${hooks:-0}" -gt 0 ] && echo 1)" "$hooks hook line(s) in controller.log"

    # 7. the desktop app's session token, end to end through the controller
    python3 -I - "$REPO/tests/lab" "$TP_CTL_PORT" "$ctl_token" "$TP_HOST_PORT" "$host_token" "$LAB_WORKSPACE" \
        > "$RUN/app-session.txt" 2>&1 <<'PY'
import json, sys, time, urllib.error, urllib.parse, urllib.request, uuid
sys.path.insert(0, sys.argv[1])
from labkit import ws as wslib
cport, owner, hport, htok, cwd = int(sys.argv[2]), sys.argv[3], int(sys.argv[4]), sys.argv[5], sys.argv[6]

def post(port, path, token, body):
    q = "?" + urllib.parse.urlencode({"token": token}) if token else ""
    req = urllib.request.Request("http://127.0.0.1:%d%s%s" % (port, path, q), data=json.dumps(body).encode(),
                                 method="POST", headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")

def fail(msg):
    print("FAIL:", msg)
    sys.exit(1)

def recv_until(sock, pick, what, deadline_s=30):
    seen = []
    end = time.time() + deadline_s
    while time.time() < end:
        sock.settimeout(max(0.1, end - time.time()))
        kind, data = sock.recv()
        if kind == "close":
            fail("socket closed waiting for %s; seen %s" % (what, seen[-5:]))
        if kind != "text":
            continue
        msg = json.loads(data)
        if pick(msg):
            return msg, seen
        seen.append(msg)
    fail("deadline waiting for %s" % what)

# The owner's account and its login, the way the app signs in.
user, pw = "applab" + uuid.uuid4().hex[:6], "correct horse battery staple 42"
print("users/add", post(cport, "/cli/users/add", owner, {"username": user, "password": pw}))
print("users/set-role", post(cport, "/cli/users/set-role", owner, {"username": user, "role": "owner"}))
st, login = post(cport, "/cli/auth/login", None, {"username": user, "password": pw})
if st != 200 or "token" not in login:
    fail("login: %s %s" % (st, login))
tok = login["token"]
print("logged in as", user, "(session token, not the owner token)")

events = wslib.WebSocket("127.0.0.1", cport, "/cli/sessions/events?" + urllib.parse.urlencode({"token": tok}), timeout=10)
agent, claude_session = "app-" + uuid.uuid4().hex[:8], str(uuid.uuid4())
st, sp = post(cport, "/cli/sessions/v2/spawn", tok, {"agent_name": agent, "cwd": cwd, "command": "claude",
                                                    "args": ["--session-id", claude_session], "cols": 100, "rows": 30})
if st != 200:
    fail("spawn with the session token: %s %s" % (st, sp))
print("spawn ->", st, "sessionId", sp["sessionId"], "reused", sp["reused"])
grid = wslib.WebSocket("127.0.0.1", cport, "/cli/sessions/grid?" + urllib.parse.urlencode(
    {"session": sp["sessionId"], "token": tok}), timeout=10)
mode, seen = recv_until(grid, lambda m: m.get("event") == "mode", "the first mode event")
print("first mode:", json.dumps(mode["payload"]))
if mode["payload"] != {"mode": "viewer", "capable": True}:
    fail("a Connect user starts as a capable viewer")
grid.send_text(json.dumps({"action": "set_mode", "mode": "claimer"}))
mode, _ = recv_until(grid, lambda m: m.get("event") == "mode", "the claimer ack")
print("after set_mode claimer:", json.dumps(mode["payload"]))
text = "hello from the app session " + uuid.uuid4().hex[:6]
recv_until(grid, lambda m: "k2-fake-agent" in json.dumps(m) and ">" in json.dumps(m), "the agent prompt")
grid.send_text(json.dumps({"action": "input", "text": text}))
time.sleep(0.15)
grid.send_text(json.dumps({"action": "input", "text": "\r"}))
recv_until(grid, lambda m: ("reply #1: " + text) in json.dumps(m), "the reply")
print("reply seen on the grid:", "reply #1: " + text)
grid.close()

url = "https://claude.ai/oauth/authorize?app-session=" + uuid.uuid4().hex[:6]
req = urllib.request.Request("http://127.0.0.1:%d/cli/browser/open-url?token=%s" % (hport, htok),
                             data=json.dumps({"url": url}).encode(), method="POST",
                             headers={"Content-Type": "application/json"})
urllib.request.urlopen(req, timeout=10).read()
ev, _ = recv_until(events, lambda m: m.get("kind") == "open_url" and m.get("url") == url, "open_url on the session's events")
print("open_url on /cli/sessions/events (session token):", json.dumps(ev))
st, cl = post(cport, "/cli/sessions/v2/close", tok, {"agent_name": agent, "force": True, "clear_index": True})
print("close ->", st, cl)
if st != 200 or not cl.get("closed"):
    fail("close with the session token")
print("OK")
PY
    rc=$?
    check "app session token: login, spawn, attach, type, open_url, close via the controller" "$([ $rc = 0 ] && echo 1)" \
        "$(tail -1 "$RUN/app-session.txt")"

    topo_metrics > "$RUN/metrics.csv"
    echo "evidence: ${RUN#$REPO/}"
    if [ "$FAILS" = 0 ]; then echo "two-process: PASS"; exit 0; fi
    echo "two-process: $FAILS check(s) failed"
    exit 1
fi
