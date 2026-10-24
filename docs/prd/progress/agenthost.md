# k2-dist · agenthost: the AgentHost interface, in-process only (PRD P1)

Task `agenthost`, branch `k2-dist/agenthost`. Stage **P1**, gate **G1**:
"A refactor with zero behavior change." Every way the daemon drives an agent
session goes through one `AgentHost` interface. The only implementation is
the in-process host, a thin wrapper over today's `DaemonPtySession` and its
placement table (`v2_session_map`). P2 adds a remote host over the signed
WebSocket; P4 adds the Kubernetes ones. Neither is in scope.

## What changes

```mermaid
graph TD
    %% ── Colour legend ──────────────────────────────────────────────
    %% Blue = exists today, unchanged · Violet = changed by this task
    %% Green = new in this task · Slate = in-memory state
    %% Shapes: rect = code (module or type), stadium = state

    %% ── CONTROLLER SIDE · crates/k2-daemon ─────────────────────────
    subgraph ctl [Controller side · crates/k2-daemon]
        SP[spawn routes<br/><sub>v2_spawn.rs · spawn.rs</sub>]:::changed
        AR[agents and wake<br/><sub>agents_routes.rs · wake_headless.rs</sub>]:::changed
        WS[grid WebSocket<br/><sub>sessions_grid_ws.rs · attach, input, resize</sub>]:::changed
        TR[terminal routes<br/><sub>terminal_routes.rs · read, write, resize, pin</sub>]:::changed
        MSG[k2 msg injection<br/><sub>workspace_msg.rs · inject_framed_locked</sub>]:::changed
        WD[watchdog and reaper<br/><sub>watchdog.rs · active_reaper.rs</sub>]:::changed
        SL[lookups<br/><sub>session_lookup.rs · LiveSession = AgentSession</sub>]:::changed
        OT[about 20 other users<br/><sub>still on v2_session_map, listed below</sub>]:::existing
    end

    %% ── THE INTERFACE · crates/k2-daemon/src/agent_host ────────────
    subgraph api [AgentHost interface · agent_host/mod.rs]
        T[trait AgentHost<br/><sub>spawn · place · attach · stream · write · inject · resize · pin · kill · hook_events · home</sub>]:::new
        AS[AgentSession<br/><sub>handle: session id, placement epoch, metadata</sub>]:::new
        AT[Attachment<br/><sub>viewer registration + events, labels, pins</sub>]:::new
    end

    %% ── IN-PROCESS HOST · agent_host/in_process.rs ─────────────────
    subgraph inp [In-process host · agent_host/in_process.rs · epoch 1]
        IP[InProcessHost<br/><sub>agent_host::host · the only impl in P1</sub>]:::new
        GE[grid emitter<br/><sub>grid_emitter.rs · host-side frame stream</sub>]:::existing
        MAP([placement table<br/><sub>v2_session_map.rs · agent id → session</sub>]):::state
        HB([hook plane<br/><sub>hook_ingest::subscribe</sub>]):::state
    end

    %% ── K2-CORE · unchanged ────────────────────────────────────────
    subgraph core [k2-core · terminal · not touched]
        PTY[DaemonPtySession<br/><sub>terminal/daemon_pty.rs · PTY + alacritty Term</sub>]:::existing
    end

    SP -- spawn, place, kill --> T
    AR -- through spawn.rs --> SP
    WS -- attach, stream, write, resize --> T
    TR -- read_screen, write, resize, pin --> T
    MSG -- inject steps --> T
    WD -- write Ctrl-C, kill, unplace --> T
    SL -- find_agent, find_session, placements --> T
    T --- AS
    T --- AT
    T -. implemented by .-> IP
    IP -- registers in --> MAP
    IP -- stream --> GE
    IP -- subscribes --> HB
    IP -- wraps --> PTY
    GE -- reads Term --> PTY
    OT -. direct, unchanged .-> MAP

    classDef existing fill:#dbeafe,stroke:#2563eb,color:#1e3a5f
    classDef changed  fill:#ede9fe,stroke:#7c3aed,color:#3b0764
    classDef new      fill:#d1fae5,stroke:#059669,color:#064e3b
    classDef external fill:#ffedd5,stroke:#ea580c,color:#7c2d12
    classDef state    fill:#f1f5f9,stroke:#64748b,color:#1e293b
```

## Main flow: spawn, attach, type (scenario S1) through `AgentHost`

```mermaid
sequenceDiagram
    participant C as Client<br/>(app or lab S1)
    participant R as v2_spawn.rs<br/>(controller route)
    participant H as InProcessHost<br/>(AgentHost, epoch 1)
    participant P as DaemonPtySession<br/>(k2-core, unchanged)
    participant M as v2_session_map<br/>(placement table)
    participant W as sessions_grid_ws.rs<br/>(controller route)

    Note over C,M: Phase 1: spawn and place
    C->>R: POST /cli/sessions/v2/spawn
    R->>H: find_agent(agent id)
    H->>M: lookup_by_agent_name
    M-->>R: none (fresh spawn)
    R->>H: spawn(DaemonPtyConfig)
    H->>P: DaemonPtySession::spawn
    P-->>R: AgentSession (session id, epoch 1)
    R->>H: place(agent id, session)
    H->>M: register (activity row, SessionAdded, DB stamp)
    R->>H: events(session) for the child-exit observer
    R-->>C: {sessionId}

    Note over C,W: Phase 2: attach and stream
    C->>W: WS /cli/sessions/grid?session=id
    W->>H: find_session(id), attach(session)
    H-->>W: Attachment (viewer registered, events, labels, pins)
    W->>H: stream(session, pane, proto)
    H-->>W: shared grid frames (grid_emitter)
    W-->>C: initial snapshot + live frames

    Note over C,P: Phase 3: type, resize, kill
    C->>W: {"action":"input"} / resize
    W->>H: write(session, bytes) / resize(session, cols, rows)
    H->>P: write / request_resize
    R->>H: unplace(agent id) on close or child exit
    H->>M: unregister (cleanup, then kill)
```

## How to try it

```sh
# The gate the owner runs (same result as the base commit):
scripts/lab/prove.sh G0 --topology solo && open "$(ls -dt lab-runs/G0/*/ | head -1)report.html"

# The in-process host's own tests (shim agent, attach, stream, inject,
# resize, kill, home):
for v in $(env | grep -oE '^K2(SO)?_[A-Z0-9_]+'); do unset $v; done
CARGO_BUILD_JOBS=4 cargo test -p k2-daemon --lib -- agent_host:: --test-threads=4
```

## Status

- **Milestone:** diagrams written. Design settled; no code yet.
- **Next:** the trait and the in-process host with their tests, then move
  the call sites in priority order (spawn, attach and stream, input and
  kill, lookups).
- **Open questions:** none yet.
