# outbox: durable message delivery across hosts (W2: PRD S3, S6, I2)

Task `outbox`, branch `k2-dist/outbox`, based on `k2-dist/dist-link`
@faefee0a8 (the protocol 2 contract, W0).

In a distributed K2 the controller owns messages (ADR-0002). An agent on a
host that runs `k2 msg <agent> "text"` (or composes into an inbox) hands the
message to its own host daemon, which forwards it up the host link as a
`deliver` frame. The controller writes it to a durable outbox in its
database and drains it to the host that runs the recipient, which injects it
into the agent and acks. Every hop is at least once, with the receiver
deduping on `msg_id`, so a message ends `delivered` or `failed` with a
reason (invariant I2); none vanish.

## The change

```mermaid
graph LR
    %% ── Colour legend ──────────────────────────────
    %% Blue = exists today · Violet = changed · Green = new
    %% Orange = outside K2 · Slate = state and settings
    %% Shapes: rect = process or module, cylinder = database table

    %% ── HOST A (sender) ────────────────────────────
    subgraph ha [Agent host A · k2-daemon K2_ROLE=host]
        CA[agent CLI<br/><sub>k2 msg bob · k2 msg --inbox-*</sub>]:::external
        WM[k2 msg / inbox compose<br/><sub>workspace_msg.rs · inbox_routes.rs · role=host → forward</sub>]:::changed
        HUP[host outbox, up<br/><sub>delivery_drain.rs · resend until acked</sub>]:::new
        HDBA[(delivery_outbox<br/><sub>host A k2.db · hop=up</sub>)]:::state
        LA[host link<br/><sub>hosts/link.rs · deliver hooks</sub>]:::changed
        CA -- POST /cli/workspace/msg --> WM
        WM -- enqueue --> HUP
        HUP --> HDBA
        HUP -- deliver --> LA
    end

    %% ── CONTROLLER ─────────────────────────────────
    subgraph ctl [Controller · k2-daemon K2_ROLE=controller]
        HUB[host hub + RemoteHost<br/><sub>hosts/hub.rs · agent_host/remote.rs · deliver hooks</sub>]:::changed
        ACC[accept<br/><sub>delivery_drain.rs · seen + enqueue, then ack</sub>]:::new
        DR[controller drain<br/><sub>delivery_drain.rs · per-pair order, backoff, dead letter</sub>]:::new
        ODB[(delivery_outbox<br/><sub>controller k2.db · hop=down</sub>)]:::state
        SDB[(delivery_seen<br/><sub>controller k2.db</sub>)]:::state
        PT[placement table<br/><sub>agent → host, epoch</sub>]:::existing
        ST{{/cli/hosts/outbox<br/><sub>gate.rs · states for the lab</sub>}}:::new
        HUB -- deliver from host --> ACC
        ACC --> SDB
        ACC --> ODB
        DR -- reads, marks --> ODB
        DR -- where is bob --> PT
        DR -- deliver --> HUB
        HUB -- deliver.ack --> DR
        ST -.-> ODB
    end

    %% ── HOST B (recipient) ─────────────────────────
    subgraph hb [Agent host B · k2-daemon K2_ROLE=host]
        LB[host link<br/><sub>hosts/link.rs · deliver hooks</sub>]:::changed
        RX[receive<br/><sub>delivery_drain.rs · dedupe, inject, ack</sub>]:::new
        HDBB[(delivery_seen<br/><sub>host B k2.db</sub>)]:::state
        INJ[inject into the agent<br/><sub>workspace_msg.rs · send_message_to_session</sub>]:::existing
        INB([inbox<br/><sub>agent cwd/.k2/inbox</sub>]):::state
        CB[agent CLI bob<br/><sub>child of host B</sub>]:::external
        LB --> RX
        RX --> HDBB
        RX -- kind=msg --> INJ
        RX -- kind=inbox --> INB
        INJ -- PTY write --> CB
    end

    LA == deliver / deliver.ack ==> HUB
    HUB == deliver / deliver.ack ==> LB

    classDef existing fill:#dbeafe,stroke:#2563eb,color:#1e3a5f
    classDef changed  fill:#ede9fe,stroke:#7c3aed,color:#3b0764
    classDef new      fill:#d1fae5,stroke:#059669,color:#064e3b
    classDef external fill:#ffedd5,stroke:#ea580c,color:#7c2d12
    classDef state    fill:#f1f5f9,stroke:#64748b,color:#1e293b
```

## The S3 flow, with the link to B dropping before the ack

```mermaid
sequenceDiagram
    participant A as agent alice<br/>(child of host A)
    participant HA as k2-daemon<br/>(host A)
    participant K as k2-daemon<br/>(controller)
    participant HB as k2-daemon<br/>(host B)
    participant B as agent bob<br/>(child of host B)

    Note over A,HA: Phase 1: send (hop up)
    A->>HA: POST /cli/workspace/msg {workspace: bob, text}
    HA->>HA: outbox row msg_id, hop=up, queued
    HA->>K: deliver {msg_id, agent_id: bob, from: agent:alice, body}
    K->>K: seen? no → outbox row hop=down, queued (one transaction)
    K-->>HA: deliver.ack {msg_id, delivered}
    HA->>HA: row → delivered (accepted by the controller)
    HA-->>A: {success: true, msg_id, state: accepted}

    Note over K,B: Phase 2: drain (hop down), first attempt
    K->>K: bob placed on host B at epoch e; head of (alice, bob)
    K->>HB: deliver {msg_id, agent_id: bob, epoch: e, attempt: 1}
    K->>K: row → sent, next try at now + backoff
    HB->>HB: seen? no
    HB->>B: inject "[from alice] text"
    HB->>HB: record msg_id in delivery_seen
    HB--xK: deliver.ack {delivered} (link drops, ack lost)

    Note over K,B: Phase 3: link back, resend, dedupe
    HB->>K: hello … welcome (relink); bob placed again
    K->>HB: deliver {msg_id, attempt: 2}
    HB->>HB: seen? yes → not injected again
    HB-->>K: deliver.ack {duplicate}
    K->>K: row → delivered; next message of the pair may go
```

## Delivery state machine (each outbox row)

```mermaid
stateDiagram-v2
    [*] --> queued: enqueue (msg_id, pair, seq)
    queued --> sent: deliver sent on a link (attempt += 1)
    sent --> sent: no ack by next_at → resend (attempt += 1, backoff doubles)
    sent --> delivered: deliver.ack delivered or duplicate
    sent --> failed: deliver.ack rejected (reason)
    sent --> failed: attempts > MAX_ATTEMPTS (reason no_ack)
    queued --> failed: older than TTL (reason expired)
    sent --> failed: older than TTL (reason expired)
    delivered --> [*]
    failed --> [*]
```

Rules (details fill in as the code lands):

- `msg_id`: `m_<13-digit ms>_<16 hex>`, minted where the message enters K2
  (the sender's host, or the controller for a message sent there). The same
  id travels every hop and is the dedupe key end to end.
- Order per (sender, recipient): the drain only sends the oldest unfinished
  row of a pair; the next goes after the head is `delivered` or `failed`.
- Attempts only count when a `deliver` actually went out on a link. A
  recipient with no placement (host away, agent not running) waits, without
  burning attempts, until it is placed again or the TTL ends it.
- Failures are loud: a `failed` row keeps its reason, the daemon logs
  `[delivery] FAILED <msg_id> …`, and a trace event records it.

## How to try it

```sh
# toolchain (shared machine: 3 jobs, 3 test threads, cargo slot)
export RUSTUP_HOME=$HOME/.cache/k2-dist-rust/rustup CARGO_HOME=$HOME/.cache/k2-dist-rust/cargo
export PATH=$CARGO_HOME/bin:$PATH CARGO_BUILD_JOBS=3

# unit tests: store (dedupe, order, backoff, dead letter, restart)
~/.cache/k2-dist-cargo-slot.sh cargo test -p k2-core --lib delivery:: -- --test-threads=3
# fake-host loopback test: A → controller → B exactly once across a link drop
~/.cache/k2-dist-cargo-slot.sh cargo test -p k2-daemon --lib delivery_drain:: -- --test-threads=3

# three real processes: controller + host-1 + host-2 (evidence under lab-runs/outbox/<UTC>/)
scripts/lab/topology/three-process-msg.sh
```

## Status

- Milestone: diagrams written.
- Next: `k2_core::delivery` store and its unit tests; then the drain and
  the host handlers.
- Open question (asked of the coordinator): the delivery hooks need small
  additive edits in `agent_host/remote.rs` and `hosts/link.rs`, which this
  task was told not to touch.
