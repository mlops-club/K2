# K2 distributed runtime: architecture

How K2 runs as a distributed system on Kubernetes. One **controller** daemon
owns state and talks to the desktop app. **Host** daemons run the agents
(Claude Code and others) and dial the controller. The controller adds and
removes host pods as work comes and goes.

This describes `k2-dist/wave3` and release candidate 3 (tag `k2-dist-rc3`,
`b60a9fd80`), which is what the owner's cluster runs. The proven deployment is
**kind** (Kubernetes in Docker) on a Mac. The same manifests are meant for any
Kubernetes, such as k3s; see [What is proven](#what-is-proven-and-what-is-not).

Related docs: the PRD (`docs/prd/k2-distributed-runtime.html`), the per-feature
progress notes (`docs/prd/progress/*.md`), the kind runbook
(`docs/prd/progress/k3s-owner-steps.md`) and the roll history
(`docs/prd/progress/kind-enroll.md`).

## How to read the diagrams

Diagrams follow `docs/prd/diagram-style.md`, the same style as the homelab
cluster docs. In the flowcharts, **shape says what kind of thing a node is**:

| Shape | Kind | Example |
|---|---|---|
| `((circle))` | a person | the owner |
| `[/parallelogram/]` | a script you run | `scripts/kind/up.sh` |
| `[rectangle]` | a long-running process | `k2-daemon`, `claude` |
| `{{hexagon}}` | a Kubernetes object | StatefulSet, Service, Secret |
| `([stadium])` | files or a volume | a PVC, `~/.claude` |
| `[(cylinder)]` | a database | `k2.db` |

**Subgraphs are places**, nested the way they really are: machine → VM →
node container → namespace → pod → container. **Colour** says who owns it:
green is K2's distributed runtime, blue is existing K2, orange is outside
software (Docker, kind, Kubernetes, Claude, Anthropic), and slate is state.

## 1. Deployment: the Kubernetes cluster

```mermaid
graph TD
    %% ── Legend ─────────────────────────────────────────────────
    %% Shapes: circle = person · parallelogram = script · rect = process
    %%         hexagon = Kubernetes object · stadium = volume/files
    %% Colours: green = K2 distributed runtime · blue = existing K2
    %%          orange = outside software · slate = state

    owner((Owner)):::person

    %% ── MAC ────────────────────────────────────────────────────
    subgraph mac [Mac · macOS laptop]
        app[K2.app<br/><sub>desktop client · Tauri</sub>]:::existing
        upsh[/"scripts/kind/up.sh<br/><sub>build · load · apply · enroll</sub>"/]:::script
        roll[/"scripts/kind/owner-roll-rc1.sh<br/><sub>backup · roll · checks · rollback</sub>"/]:::script

        %% ── DOCKER DESKTOP VM ─────────────────────────────────
        subgraph vm [VM · Docker Desktop Linux · 14 GB RAM · ~94 GB disk]
            build[k2-kind-build<br/><sub>Docker container · cargo build → images</sub>]:::external
            pf[k2-kind-dist-proxy<br/><sub>Docker container · published on the Mac at 127.0.0.1:7443</sub>]:::external

            %% ── KIND CLUSTER ───────────────────────────────────
            subgraph kind [Kubernetes cluster · kind k2lab · 3 node containers]

                %% ── NAMESPACE ─────────────────────────────────
                subgraph ns [Namespace · k2-kind-dist]
                    svc{{Service k2-controller<br/><sub>NodePort → :7420</sub>}}:::k8s
                    ctlsts{{StatefulSet k2-controller<br/><sub>replicas 1</sub>}}:::k8s
                    hoststs{{StatefulSet k2-host<br/><sub>replicas MIN..MAX · scaled by the controller</sub>}}:::k8s
                    enroll{{Secret k2-host-enroll<br/><sub>one-time token per host</sub>}}:::k8s
                    rbac{{Role + ServiceAccount<br/><sub>may scale k2-host · patch the Secret</sub>}}:::k8s

                    %% ── CONTROLLER POD ────────────────────────
                    subgraph ctlpod [Pod · k2-controller-0]
                        ctl[k2-daemon<br/><sub>K2_ROLE=controller · image k2-daemon:rc3</sub>]:::new
                    end

                    %% ── HOST POD ──────────────────────────────
                    subgraph hostpod [Pod · k2-host-N · one per host]
                        host[k2-daemon<br/><sub>K2_ROLE=host · image k2-agents:rc3</sub>]:::new
                        ptyd[k2-ptyd<br/><sub>owns the agents' PTYs · K2_PTYD=1</sub>]:::new
                        cli[claude<br/><sub>Claude Code CLI · one per agent</sub>]:::external
                    end
                end
            end
        end
    end

    anthropic[Anthropic API<br/><sub>api.anthropic.com</sub>]:::external

    %% ── EDGES ──────────────────────────────────────────────────
    owner -- uses --> app
    owner -- runs --> upsh
    owner -- runs --> roll
    upsh -- builds images in --> build
    upsh -- applies --> ns
    roll -- rolls --> ctlsts
    app -- "HTTP + WebSocket" --> pf
    pf -- forwards --> svc
    svc --> ctl
    ctlsts -- runs --> ctlpod
    hoststs -- runs --> hostpod
    ctl -- scales via API --> hoststs
    ctl -- writes tokens --> enroll
    rbac -.-> ctl
    enroll -. mounted .-> host
    host -- "dials /cli/hosts/attach (protocol 2)" --> ctl
    host -- spawns via --> ptyd
    ptyd -- runs in a PTY --> cli
    cli -- prompts --> anthropic

    classDef person   fill:#fef3c7,stroke:#d97706,color:#78350f
    classDef script   fill:#fce7f3,stroke:#db2777,color:#831843
    classDef existing fill:#dbeafe,stroke:#2563eb,color:#1e3a5f
    classDef new      fill:#d1fae5,stroke:#059669,color:#064e3b
    classDef external fill:#ffedd5,stroke:#ea580c,color:#7c2d12
    classDef k8s      fill:#ede9fe,stroke:#7c3aed,color:#3b0764
    classDef state    fill:#f1f5f9,stroke:#64748b,color:#1e293b
```

Key points:

- **The app only ever talks to the controller.** It reaches it through a
  loopback port (`127.0.0.1:7443`) that a small proxy container forwards
  into the cluster. There is no TLS on that hop today, because it never
  leaves the Mac.
- **Hosts dial out to the controller**, never the reverse. A host proves who
  it is with a one-time enrollment token from the Secret, then a signed key.
- **The controller scales the host pool itself.** With
  `K2_PROVISIONER=kubernetes` it patches the `k2-host` StatefulSet's scale
  (bounded by `K2_PROVISIONER_MIN_HOSTS` / `_MAX_HOSTS`, idle hosts drained
  after `_IDLE_AFTER`), and mints the new host's token first.
- **k2-ptyd** owns each agent's terminal (PTY), so the daemon can restart
  without killing the agent. Adopting those sessions after a restart is the
  ptyd S3 work, on wave3 after rc3 and still under review.

## 2. Where state lives

```mermaid
graph LR
    %% ── Legend ─────────────────────────────────────────────────
    %% Shapes: rect = process · cylinder = database · stadium = files/volume
    %%         hexagon = Kubernetes object
    %% Colours: green = K2 runtime · slate = state · violet = Kubernetes
    %%          orange = outside software · blue = existing K2

    %% ── MAC ────────────────────────────────────────────────────
    subgraph mac [Mac · K2.app]
        app[K2.app]:::existing
        hosts(["~/.k2so/connect-hosts.json<br/><sub>saved servers · no tokens</sub>"]):::state
        kc([macOS Keychain<br/><sub>session tokens k2_secret_*</sub>]):::state
        app --- hosts
        app --- kc
    end

    %% ── CONTROLLER ─────────────────────────────────────────────
    subgraph ctlpod ["Pod · k2-controller-0 · PVC data at /var/lib/k2"]
        ctl[k2-daemon controller]:::new
        db[(k2.db<br/><sub>placements · epochs · enrollment keys<br/>message outbox · turn tracking<br/>heartbeats · sources · activity</sub>)]:::state
        acct([connect-users.json · connect-sessions.json<br/><sub>accounts as argon2 hashes · daemon.token</sub>]):::state
        homes(["homes/objects + homes/index.db<br/><sub>agent home snapshots · content-addressed</sub>"]):::state
        ctl --- db
        ctl --- acct
        ctl --- homes
    end

    %% ── HOST ───────────────────────────────────────────────────
    subgraph hostpod ["Pod · k2-host-N · PVC data at /var/lib/k2-host"]
        host[k2-daemon host]:::new
        ptyd[k2-ptyd]:::new
        hdb[(host k2.db<br/><sub>outbound outbox · dedupe · write-ahead</sub>)]:::state
        agenthome(["agent HOME<br/><sub>~/.claude login · MEMORY.md · transcripts</sub>"]):::state
        key([host-key.pem<br/><sub>enrolled identity</sub>]):::state
        ring([PTY output rings<br/><sub>in memory · lost if ptyd dies</sub>]):::state
        host --- hdb
        host --- key
        host --- agenthome
        ptyd --- ring
    end

    %% ── SHARED ─────────────────────────────────────────────────
    subgraph shared [Volumes · workspaces]
        work(["k2-work PVC at /var/lib/k2/work<br/><sub>project files · .k2so/inbox<br/>shared by controller and hosts today</sub>"]):::state
    end

    %% ── KUBERNETES ─────────────────────────────────────────────
    subgraph k8s [Kubernetes objects]
        sec{{Secret k2-host-enroll<br/><sub>one-time host tokens</sub>}}:::k8s
        sec2{{Secret k2-secrets<br/><sub>bootstrap host token</sub>}}:::k8s
    end

    app -- session token --> ctl
    host -- saves home at each turn end --> homes
    homes -- restored on spawn or move --> agenthome
    ctl --- work
    host --- work
    sec -. mounted .-> host
    sec2 -. mounted .-> host

    classDef existing fill:#dbeafe,stroke:#2563eb,color:#1e3a5f
    classDef new      fill:#d1fae5,stroke:#059669,color:#064e3b
    classDef state    fill:#f1f5f9,stroke:#64748b,color:#1e293b
    classDef k8s      fill:#ede9fe,stroke:#7c3aed,color:#3b0764
```

What survives what:

| Event | What is kept | What is lost |
|---|---|---|
| Controller pod restarts | Everything in `k2.db` (outbox, placements, turn tracking), accounts, home snapshots | In-flight WebSocket streams; the app reconnects within seconds |
| Host daemon restarts | Agent HOME on the host's PVC; with ptyd S3 (post-rc3) the running agents | In rc3, the running agents' sessions; their in-progress turn |
| Host pod is lost (node dies) | The agent's home as of its last turn end, from the controller's snapshot store | The turn in progress (it ends `outcome unknown`), home changes since the last snapshot |
| Agent moves to another host | Memory and session id, restored from the snapshot | Nothing else, if its last turn had ended |

## 3. User flows

### 3.1 Starting an agent

```mermaid
sequenceDiagram
    participant U as Owner
    participant A as K2.app<br/>(Mac)
    participant C as k2-daemon<br/>(controller)
    participant K as Kubernetes API
    participant H as k2-daemon<br/>(host)
    participant P as k2-ptyd<br/>(host)
    participant CL as claude<br/>(CLI in a PTY)

    Note over U,C: Phase 1: ask for an agent
    U->>A: new terminal or chat agent
    A->>C: POST /cli/sessions/v2/spawn (or ensure-pinned-chat)
    C->>C: choose a host: pinned host, else last host, else most room<br/>new epoch → host_placements

    Note over C,H: Phase 2: only if no host has room
    C->>K: mint one-time token → patch Secret k2-host-enroll
    C->>K: patch StatefulSet k2-host scale to N+1
    K-->>H: new pod k2-host-N starts
    H->>C: dial /cli/hosts/attach · signed hello
    C-->>H: welcome

    Note over C,CL: Phase 3: spawn on the host
    C->>H: spawn {agent_id, epoch, body}
    H->>H: restore newest home snapshot (memory, login)
    H->>P: open PTY session
    P->>CL: start claude (K2_HOOK_SOCK, K2_HOOK_TOKEN)
    H-->>C: reply {sessionId}
    C-->>A: sessionId

    Note over A,CL: Phase 4: watch and type
    A->>C: WebSocket /cli/sessions/grid (session id)
    C->>H: grid.open {session, epoch}
    H-->>C: grid.data (snapshot, then deltas)
    C-->>A: grid.data (relayed verbatim)
    U->>A: types
    A->>C: input frame (keystrokes)
    C->>H: grid.data (input)
    H->>P: write to PTY
    P->>CL: keystrokes
```

### 3.2 Giving an agent an instruction by message

A message (from the app, another agent's `k2 msg`, a heartbeat or a source
trigger) goes through a durable outbox. It is delivered once. If the agent's
turn is cut off, the message ends with a clear reason rather than vanishing.

```mermaid
sequenceDiagram
    participant S as Sender<br/>(app · k2 msg · trigger)
    participant C as k2-daemon<br/>(controller)
    participant DB as k2.db<br/>(controller)
    participant H as k2-daemon<br/>(host)
    participant CL as claude<br/>(CLI in a PTY)

    Note over S,DB: Phase 1: accept
    S->>C: POST /cli/workspace/msg {to, text}
    C->>DB: outbox row: queued (msg_id)
    C-->>S: {success, msg_id, state: accepted}

    Note over C,CL: Phase 2: deliver (drain loop, oldest first per sender/recipient)
    C->>DB: row: sent (attempt n)
    C->>H: deliver {msg_id, agent_id, epoch, attempt}
    H->>H: dedupe · write-ahead (keeps the typed text)
    H->>CL: bracketed paste + Enter
    H-->>C: deliver.ack {delivered}
    C->>DB: row: delivered

    Note over H,DB: Phase 3: the agent works
    CL->>H: hook UserPromptSubmit (prompt text)
    H->>H: match the prompt to the typed message → tie it to this turn
    H->>C: hook frame {turn counts, tied msg_ids}
    C->>DB: host_turns: turn running, messages tied
    CL->>H: hook Stop
    H->>C: hook frame {stops+1}
    C->>DB: host_turns: turn answered

    Note over C,DB: If the turn is cut instead
    C->>DB: crash or close → tied messages: failed "interrupted: … crashed mid-turn"
    C->>DB: host lost → tied messages: failed "outcome unknown: host lost mid-turn"
```

Notes:

- Resends back off from 2 s to 60 s. A row fails after 10 unacked sends
  (`no_ack`) or 7 days (`expired`). A recipient that isn't placed waits
  without using attempts.
- K2 records the **state** of a message, not the answer's text. The reply
  lives in the agent's terminal and transcript, or in a new `k2 msg` back.
- A message is only failed with evidence: the host must have matched the
  agent's actual prompt to the text it typed. Messages Claude folds into a
  running turn are never failed.

### 3.3 Waking a sleeping agent with a message

```mermaid
sequenceDiagram
    participant S as Sender
    participant C as k2-daemon<br/>(controller)
    participant H as k2-daemon<br/>(host)
    participant CL as claude

    S->>C: POST /cli/workspace/msg {to, text, wake: true}
    C->>C: recipient not placed → wake path
    alt no --wake
        C-->>S: dormant_no_wake
    else wake
        C->>C: choose or scale up a host (as in 3.1)
        C->>H: spawn.workspace {agent_id, epoch, launch prompt = message}
        H->>H: restore home snapshot (resumes the same Claude session)
        H->>CL: claude --resume … with the message
        C-->>S: {woke: true}
    end

    Note over C,CL: Later, when idle
    C->>H: release {agent_id, epoch, reason}
    H->>H: save home snapshot to the controller
    H-->>C: released
```

### 3.4 Events: schedules and data-source triggers

K2 has **no inbound webhook receiver** today: nothing accepts an external HTTP
POST and routes it to an agent. The event mechanisms that exist are:

- **Heartbeats**: scheduled wake-ups. Configured with `/cli/heartbeat/*`,
  stored in `agent_heartbeats` and `heartbeat_fires`; the prompt is the
  agent's `WAKEUP.md`. A 60-second tick in the daemon fires them.
- **Data-source triggers**: the daemon **polls** an external source (it is
  not pushed to) and wakes an agent when the source's state changes in a
  way the trigger cares about. An agent can only propose a trigger; a person
  approves it. Routes `/cli/sources/triggers/*`, tables `sources`,
  `source_triggers`, `source_trigger_fires`.
- **Mail** (Stalwart capability): agents long-poll `/cli/mail/wait`; mail
  arrival does not wake an agent by itself.
- **Outbound only**: notifications to Slack, ntfy and other webhook targets.

An external system that wants to prompt an agent, such as another app, can
call the message route directly (flow 3.2) with a K2 token.

```mermaid
sequenceDiagram
    participant U as Owner
    participant A as K2.app
    participant C as k2-daemon<br/>(controller)
    participant DB as k2.db
    participant X as External source<br/>(polled)
    participant H as k2-daemon<br/>(host)
    participant CL as claude

    Note over U,DB: Configure
    U->>A: add heartbeat or approve a source trigger
    A->>C: /cli/heartbeat/add or /cli/sources/triggers/approve
    C->>DB: agent_heartbeats / source_triggers

    Note over C,X: Watch
    loop every tick
        C->>X: poll the source (sources scheduler)
        X-->>C: current state
        C->>C: hard state change? schedule due?
    end

    Note over C,CL: Fire: a message with wake (flows 3.2 and 3.3)
    C->>DB: source_trigger_fires / heartbeat_fires
    C->>C: k2 msg from source NAME (or WAKEUP.md), wake true
    C->>H: spawn.workspace or deliver
    H->>CL: the agent gets the event as a prompt
```

## What is proven, and what is not

Proven on kind on a Mac, against rc3:

- The owner's K2 (`k2-kind-dist`) on rc3: a real Claude Code turn through
  `127.0.0.1:7443`, a turn-end memory save, sleep and wake into the same
  session, the app's login path, reconnect within seconds after both pods
  restart.
- The final Linux gate (9,413 tests passing), the G2 scenario suite including
  S10 under heavy chaos (10 of 10 rounds), and the G4 suite on kind.
- A fresh bring-up from the runbook alone.
- The fs/git relay (the app's file browser and git panel served from the
  agent's own host): 13 review rounds and K5 on kind, 45 of 45 checks.
  Shipped **off** by default; the migration to switch it on is scripted.

Not proven, or not built:

- **Kubernetes other than kind** (k3s, cloud). The scripts load images with
  `kind load` and reach the controller through a local proxy. A k3s variant
  needs a registry or `k3s ctr` import, images built for the cluster's CPU
  (Macs build ARM; most cloud VMs are x86), and a remote address.
- **Remote access over the internet**: no TLS ingress or Tailscale setup yet.
- **Phone and web clients** against the distributed controller: untested.
- **An external app holding its own K2 credential** to post messages: the
  route exists; the right token type has not been settled.
- **Inbound webhooks**: not built (see 3.4).
- **Agents surviving a host-daemon restart** (ptyd S3): on wave3 after rc3,
  with fixes on `k2-dist/ptyd-s3.1` awaiting review. Don't roll the wave3 tip
  until they are merged and gated.
