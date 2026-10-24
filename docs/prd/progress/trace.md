# k2-dist · trace: trace events and flows as tests (PRD section 3, rule 4)

Task `trace`, branch `k2-dist/trace`. Stage **P1**, gate **G1** on topology
**solo**: "Trace diff passes for spawn, hook event and CLI-crash flows on
solo." The daemon writes trace events in the lab's format
(`docs/prd/progress/lab.md`, "Trace format"); the lab's checker
(`tests/lab/labkit/flows.py`) matches them against the flows.

## What changes

```mermaid
graph TD
    %% ── Colour legend ──────────────────────────────────────────────
    %% Blue = exists today, unchanged · Violet = changed (trace lines only)
    %% Green = new in this task · Orange = outside K2 · Slate = files, env
    %% Shapes: rect = process or module, stadium = file,
    %%         hexagon = generated, flag = env var

    %% ── DAEMON · crates/k2-daemon (solo: controller and in-process host) ──
    subgraph daemon [k2-daemon · solo, controller and in-process host]
        SP[HTTP spawn entry<br/><sub>v2_spawn.rs · spawn_session, 1 line</sub>]:::changed
        AT[HTTP attach entry<br/><sub>sessions_grid_ws.rs · 1 line</sub>]:::changed
        HI[hook receipt<br/><sub>hook_ingest.rs · ingest_with</sub>]:::changed
        WM[message delivery<br/><sub>workspace_msg.rs · pending_live.rs</sub>]:::changed
    end

    %% ── CORE · crates/k2-core ──────────────────────────────────────
    subgraph core [k2-core · library]
        PTY[DaemonPtySession<br/><sub>terminal/daemon_pty.rs · spawn, write, kill</sub>]:::changed
        IB[inbox write<br/><sub>inbox.rs · compose</sub>]:::changed
        TR[trace sink<br/><sub>trace/mod.rs · emit, off unless K2_TRACE_FILE</sub>]:::new
        TC[correlation<br/><sub>trace/corr.rs · pane to agent, spawn scope</sub>]:::new
        TW[PTY watcher<br/><sub>trace/pty.rs · first output, child exit</sub>]:::new
        WT[writer thread<br/><sub>bounded queue, never blocks the caller</sub>]:::new
    end

    ENV>K2_TRACE_FILE<br/><sub>absolute path · new setting</sub>]:::state
    FT([daemon trace<br/><sub>run/daemon.trace.jsonl</sub>]):::state
    FA[k2-fake-agent as claude<br/><sub>tests/fakes · K2_FAKE_AGENT_TRACE</sub>]:::existing
    FF([fake-agent trace<br/><sub>run/fake-agent.trace.jsonl</sub>]):::state

    %% ── LAB · tests/lab ────────────────────────────────────────────
    subgraph lab [Lab kit · tests/lab · python3 stdlib]
        G1[G1 scenarios<br/><sub>scenarios/g1_solo.py · S1 + S7 crash</sub>]:::new
        CK[flow checker<br/><sub>labkit/flows.py</sub>]:::existing
        FJ([flows<br/><sub>docs/prd/flows/spawn, hook · crash.json new</sub>]):::new
        V{{verdict per flow<br/><sub>flows.json · solo steps vs not in solo</sub>}}:::new
    end

    SP -- spawn scope: agent_id --> TC
    PTY -- host.spawn, ptyd.open_session, pty.* --> TR
    PTY -- subscribe events --> TW
    TW -- first output, child exit --> TR
    AT -- api.attach --> TR
    HI -- cell.*, host.agent_event --> TR
    WM -- host.inject, pending_live.* --> TR
    IB -- inbox.write --> TR
    TC -. pane to agent .-> TR
    ENV -. turns on .-> TR
    TR -- try_send --> WT
    WT -- appends lines --> FT
    FA -- cli.* --> FF
    G1 -- spawn, attach, type, kill -9, resume --> daemon
    G1 -- merges --> FT
    G1 -- merges --> FF
    G1 --> CK
    CK -- reads --> FJ
    CK --> V

    classDef existing fill:#dbeafe,stroke:#2563eb,color:#1e3a5f
    classDef changed  fill:#ede9fe,stroke:#7c3aed,color:#3b0764
    classDef new      fill:#d1fae5,stroke:#059669,color:#064e3b
    classDef external fill:#ffedd5,stroke:#ea580c,color:#7c2d12
    classDef state    fill:#f1f5f9,stroke:#64748b,color:#1e293b
```

## Main flow: the CLI-crash flow (S7) as traced on solo

```mermaid
sequenceDiagram
    participant L as Lab<br/>(g1_solo.py)
    participant D as k2-daemon<br/>(controller + in-process host)
    participant P as PTY<br/>(daemon_pty, in-process)
    participant F as k2-fake-agent<br/>(as claude)
    participant T as Trace<br/>(K2_TRACE_FILE)

    Note over L,T: Phase 1: spawn (flow spawn, steps 1, 3 to 8)
    L->>D: POST /cli/sessions/v2/spawn {agent_name, claude --session-id X}
    D->>T: api.spawn_request (agent_id)
    D->>P: DaemonPtySession::spawn
    P->>T: host.spawn, ptyd.open_session, pty.spawned
    P->>F: exec claude --session-id X
    F->>T: cli.exec, cli.hook SessionStart
    F->>D: POST /hook/event (notify.sh)
    D->>T: cell.owner_walk, cell.owner_verdict, cell.event, host.agent_event
    L->>D: WS /cli/sessions/grid (api.attach)
    P-->>T: client.stream (first output with a client attached)

    Note over L,T: Phase 2: crash mid-turn (flow crash, steps 4 to 6)
    L->>D: type "a long task"
    F->>T: cli.turn.start
    L->>F: kill -9 (pid from cli.exec)
    P->>T: ptyd.child_exit {signal: 9, clean: false}
    D->>T: host.agent_exit (session marked dead)

    Note over L,T: Phase 3: the next need resumes (flow crash, steps 7 to 11)
    L->>D: POST /cli/sessions/v2/spawn {agent_name, no command}
    D->>T: api.spawn_request
    D->>P: spawn claude --resume X (tab-row recovery, today's code)
    P->>T: host.spawn {resume: true, session_id: X}
    F->>T: cli.exec {resumed: true}, cli.hook SessionStart
    D->>T: host.agent_event
    L->>L: checker: spawn, hook, crash pass (solo steps); I3 same session id
```

## Trace events the daemon emits

`src` is always `k2-daemon` (in solo the in-process host is the daemon, so
`host.*` and `ptyd.*` come from it too). `host` is `local`, `epoch` is null
(no placements before P2). `agent_id` is the spawn's `agent_name`; the hook
plane resolves it from the pane (terminal session id) the spawn registered.
`attrs.pane` is the terminal session id; `session_id` is the provider's.

| Event | Where (file, emission point) | Flow step |
|---|---|---|
| `api.spawn_request` | `k2-daemon/src/v2_spawn.rs`, first line of `spawn_session` | spawn 1, crash 1 and 7 |
| `api.attach` | `k2-daemon/src/sessions_grid_ws.rs`, first line of `serve_session_grid_connection` | (context) |
| `host.spawn` | `k2-core/src/terminal/daemon_pty.rs`, top of `DaemonPtySession::spawn` | spawn 3, crash 2 and 8 |
| `ptyd.open_session` | `daemon_pty.rs`, just before the PTY opens (in-process or k2-ptyd) | spawn 4 |
| `pty.spawned` | `daemon_pty.rs`, once the child pid is known (the child exec) | (context) |
| `pty.first_output` | `trace/pty.rs` watcher, first output event | (context) |
| `client.stream` | `trace/pty.rs` watcher, first output after a client attached | spawn 8 |
| `pty.input` | `daemon_pty.rs`, `write` (byte count only, never content) | (context) |
| `ptyd.child_exit` | `trace/pty.rs` watcher, `ChildExit` (code, signal, clean) | crash 5 |
| `host.agent_exit` | `daemon_pty.rs`, `mark_child_exited` (session marked dead) | crash 6 |
| `pty.kill` | `daemon_pty.rs`, `kill` | (context) |
| `cell.hook_received` | `k2-daemon/src/hook_ingest.rs`, `ingest_with` | (context) |
| `cell.owner_walk` | `hook_ingest.rs`, before `owner::check` | hook 2 |
| `cell.owner_verdict` | `hook_ingest.rs`, after `owner::check` | hook 3 |
| `cell.event` | `hook_ingest.rs`, an accepted envelope | hook 4 |
| `host.agent_event` | `hook_ingest.rs`, published to the activity store, with agent_id | spawn 7, hook 5, crash 11 |
| `host.inject` | `k2-daemon/src/workspace_msg.rs`, `inject_framed_locked` | msg 7 (not G1) |
| `pending_live.enqueue` | `k2-daemon/src/pending_live.rs` | (context, not G1) |
| `inbox.write` | `k2-core/src/inbox.rs`, `compose` | (context, not G1) |

### Flow steps not in solo

| Flow | Step | Why |
|---|---|---|
| spawn | 2 `placement.choose` | No placement exists before P2 (one in-process host). The AgentHost interface (task `agenthost`) is where it lands. |

All hook steps exist in solo (the owner check runs in the daemon, on the same
kernel as the CLI). All crash steps exist in solo.

## How to try it

```sh
# One command: builds the daemon and fake agent, boots solo with
# K2_TRACE_FILE, runs S1 and the CLI crash, prints the verdict per flow.
tests/lab/scenarios/g1_solo.sh

# The sink's unit tests
cargo test -p k2-core --lib -- trace::
```

## Status

- **Milestone:** diagrams written; sink, emission lines and the G1 runner
  written, first build in progress.
- **Next:** unit tests green, first solo run, latency check (NFR1).
- **Open questions (asked, waiting):**
  1. Where `crash.json` lives. In `docs/prd/flows/` it fails the lab's
     `test_one_file_per_prd_flow_and_in_sync` (exactly the 11 PRD flows).
     Recommended: `docs/prd/flows/extra/` now, moved when G1 is wired.
  2. `cli.hook` timing. The fake agent writes `cli.hook` after the hook
     command returns; the daemon handles the hook inside that command, so
     every daemon hook event precedes it and hook, spawn and crash fail on
     order. Recommended: the lab worker writes `cli.hook` before running
     the hook (and the exit code after, as its own event).

### New interface

- `K2_TRACE_FILE`: absolute path; the daemon appends one JSONL event per
  line. Unset or empty means off: no trace work at all. Set but not
  absolute, or not openable for append, is an error, never a silent off
  (coordinator, 2026-10-24): `k2_core::trace::boot_check()` returns it so
  the daemon can refuse to start, and the first use logs
  `[trace] ERROR: K2_TRACE_FILE="…" …` to stderr. **Integration:**
  `daemon_main.rs` (not mine) should call `k2_core::trace::boot_check()`
  at boot and exit non-zero on `Err`, as it does for a relative `K2_HOME`.
  For `deploy/README.md` at integration.

### Decisions inside owned files

- **Writer.** A bounded queue (16384) and one writer thread; emission
  points `try_send` and never wait. A full queue drops the event and the
  next line written is `trace.dropped {count}`. One `write` per line on an
  `O_APPEND` file, so lines never interleave (unit-tested with two sinks on
  one file).
- **Correlation.** `agent_id` comes from the HTTP spawn: `spawn_request`
  opens a thread-local scope that `DaemonPtySession::spawn` (same thread)
  reads, then pane → agent is kept for the hook plane. Spawns that do not
  come through the HTTP route (heartbeat wakes, canonical chat) carry
  `agent_id: null` until the AgentHost interface names the agent.
- **Exit order.** `ptyd.child_exit` (trace watcher) and `host.agent_exit`
  (`mark_child_exited`, from the daemon's exit observer) hear the same
  broadcast on different threads. The trace module writes
  `host.agent_exit` only after the pane's `ptyd.child_exit` (the session
  is marked dead because the child exited), so the crash flow is never
  flaky on that pair.
- **`client.stream`** is the first PTY output after a client attached
  (`api.attach`), so it means output reached an attached client, not just
  that the socket opened.
- **`host.inject`, `pending_live.enqueue`, `inbox.write`** are emitted for
  the msg flow later (not G1). `workspace_msg.rs` has no inbox write today
  (`k2 msg` is live-or-fail), so `inbox.write` is in
  `k2-core/src/inbox.rs::compose`; `pending_live.enqueue` carries the
  signal id as `msg_id`.
- **`lib.rs`** gains one line, `pub mod trace;`.
- **The crash scenario** uses a `tab-*` agent in a registered workspace,
  because that is where today's daemon resumes a dead CLI on the next need
  (`recovered_launch_ex`: harness command + provider session id in
  `workspace_tab_sessions`). The turn is 30 s (`--turn-ms`), so the
  `kill -9` lands mid-turn.

### Rebase notes (lines in files other workers own)

- `crates/k2-daemon/src/v2_spawn.rs`: one line, first line of `spawn_session`.
- `crates/k2-daemon/src/sessions_grid_ws.rs`: one line, first line of
  `serve_session_grid_connection`.
- `crates/k2-core/src/terminal/daemon_pty.rs`: trace lines only, plus
  `write` converts `bytes` once (to count them) and passes the `Cow` on.
