# k2-dist: kind-app (K2 on kind, driven from the desktop app)

The last mile of P0 on this Mac: today's daemon (`K2_ROLE=all`) in one pod on
the kind cluster `k2lab`, with the Claude Code CLI in the image, reached from
the K2 desktop app through `kubectl port-forward`. The owner signs Claude in
the way K2 intends, with `/login` in a claude session opened from the app. No
API key, no OAuth token and no credential Secret is involved.

## The change

```mermaid
graph TD
    %% ── Colour legend ──────────────────────────────────────────────
    %% Blue = exists today · Violet = changed · Green = new
    %% Orange = outside K2 · Slate = files, volumes, env
    %% Shapes: rect = process, stadium = files/volume, hexagon = derived,
    %%         flag = env var or setting

    %% ── MAC · the owner's machine ─────────────────────────────────
    subgraph mac [Mac · owner]
        UP[up.sh / down.sh<br/><sub>scripts/kind · one command each way</sub>]:::new
        DFA([Dockerfile.agents<br/><sub>deploy/kind · FROM k2-daemon:dev + claude 2.1.308</sub>]):::new
        DFF([Dockerfile.fake<br/><sub>deploy/kind · FROM k2-daemon:dev + k2-fake-agent</sub>]):::new
        OCI[build.sh<br/><sub>packaging/oci · image task</sub>]:::existing
        PF[kubectl port-forward<br/><sub>127.0.0.1:7440 → svc/k2-solo:7420</sub>]:::external
        APP[K2 desktop app<br/><sub>src/renderer · Connections, browser tab</sub>]:::existing
        BR[Claude sign-in page<br/><sub>platform.claude.com · in the app's browser tab</sub>]:::external
        UP -- builds k2-daemon:dev --> OCI
        UP -- builds k2-agents:dev --> DFA
        UP -- "--fake: k2-agents:fake" --> DFF
        UP -- starts --> PF
        APP -- "http://127.0.0.1:7440 + token" --> PF
        APP -- opens --> BR
    end

    %% ── KIND · cluster k2lab ──────────────────────────────────────
    subgraph kind [kind k2lab · namespace k2-kind]
        KZ{{agents overlay<br/><sub>deploy/kind/agents · kustomize over solo.yaml</sub>}}:::new
        STS[StatefulSet k2-solo<br/><sub>packaging/oci/kind/solo.yaml · image k2-agents:dev</sub>]:::existing
        D[k2-daemon<br/><sub>K2_ROLE=all · K2_LISTEN=lan · :7420</sub>]:::existing
        SHIM[k2-open / xdg-open shim<br/><sub>~/.k2/bin · staged by the daemon</sub>]:::existing
        CL[claude<br/><sub>/usr/local/bin/claude · DISABLE_AUTOUPDATER=1</sub>]:::new
        PVC([PVC data<br/><sub>/var/lib/k2 = HOME · .k2, .claude, .claude.json</sub>]):::state
        KZ -- applied as --> STS
        STS -- runs --> D
        D -- spawns in a PTY --> CL
        CL -- "/login: xdg-open URL" --> SHIM
        SHIM -- POST /cli/browser/open-url --> D
        CL -- credentials.json --> PVC
        D -- daemon.token, k2.db --> PVC
    end

    UP -- kind load + kubectl apply -k --> KZ
    PF --> D
    D -- "open_url event (/cli/sessions/events)" --> APP

    classDef existing fill:#dbeafe,stroke:#2563eb,color:#1e3a5f
    classDef changed  fill:#ede9fe,stroke:#7c3aed,color:#3b0764
    classDef new      fill:#d1fae5,stroke:#059669,color:#064e3b
    classDef external fill:#ffedd5,stroke:#ea580c,color:#7c2d12
    classDef state    fill:#f1f5f9,stroke:#64748b,color:#1e293b
```

## Up, connect, sign in, work

```mermaid
sequenceDiagram
    participant O as owner<br/>(Mac terminal)
    participant U as up.sh<br/>(new)
    participant K as kind k2lab<br/>(kubectl, explicit kubeconfig)
    participant D as k2-daemon<br/>(pod, K2_ROLE=all)
    participant A as K2 app<br/>(Mac, remote client)
    participant C as claude<br/>(pod PTY)

    Note over O,K: Phase 1: up
    O->>U: scripts/kind/up.sh
    U->>U: build k2-daemon:dev, then k2-agents:dev
    U->>K: kind load docker-image (both), kubectl apply -k deploy/kind/agents
    K->>D: start StatefulSet, PVC at /var/lib/k2
    U->>K: rollout status, then port-forward 127.0.0.1:7440
    U->>D: GET /boot-status
    D-->>U: phase=ready
    U->>K: exec cat /var/lib/k2/.k2/daemon.token
    U-->>O: URL http://127.0.0.1:7440 and the token

    Note over O,A: Phase 2: connect the app
    O->>A: add the host (URL + token), see deploy/kind/README.md
    A->>D: /cli/... ?token=…, WS /cli/sessions/events
    D-->>A: workspaces, sessions, app-level events

    Note over A,C: Phase 3: sign Claude in (once; survives restarts)
    A->>D: spawn claude in a workspace
    D->>C: exec claude (PATH: ~/.k2/bin first, then /usr/local/bin)
    O->>C: /login
    C->>D: xdg-open <authorize URL> (k2-open shim → /cli/browser/open-url)
    D-->>A: open_url event, opens as a browser tab in the app
    C-->>O: "Browser didn't open? Use the url below", "Paste code here if prompted"
    O->>A: open the printed URL, sign in, copy the code
    O->>C: paste the code
    C->>D: (writes /var/lib/k2/.claude/.credentials.json on the PVC)

    Note over A,C: Phase 4: work
    O->>C: type a task
    C-->>A: output on the grid (/cli/sessions/grid)
```

## How to try it

```sh
scripts/kind/up.sh            # builds, loads, applies, waits, port-forwards; prints URL and token
# then follow deploy/kind/README.md in the desktop app
scripts/kind/down.sh          # stops the port-forward, deletes namespace k2-kind (and its PVC)
```

## Status

- **Milestone:** diagrams written.
- **Next:** Dockerfiles, overlays, up/down scripts, runbook; then build and run
  S1 against the fake-agent pod through the port-forward.
- **Decisions (mine, inside owned files):**
  - Own namespaces (`k2-kind`, and `k2-kind-fake` for the S1 proof) so this
    never collides with `image`'s `k2-solo` namespace on the same cluster.
    The overlays rename the namespace with kustomize; `solo.yaml` is untouched.
  - Claude Code 2.1.308 (npm `stable` tag on 2026-10-24) as the native
    linux binary from `@anthropic-ai/claude-code-<linux-arch>`, copied to
    `/usr/local/bin/claude` (a directory the daemon's PATH always includes),
    with `DISABLE_AUTOUPDATER=1`: the image tag is the version.
  - Owner decision 2026-10-24: no credential Secret; login is `/login` in a
    claude session opened from the app.
- **Open questions:** none yet.
