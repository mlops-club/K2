# k2-dist: kind-dist (distributed K2 on kind)

K2 split in two on the kind cluster `k2lab`: one controller pod
(`K2_ROLE=controller`, no agents) and one agent-host pod (`K2_ROLE=host`,
image `k2-agents:dev`, its own volume holding `HOME`, so Claude logins
persist). The host dials the controller and enrolls with a bootstrap token
that `up.sh` generates into a Secret. The desktop app talks only to the
controller, through `kubectl port-forward`. The manifests are a kustomize
overlay over the end state, `deploy/kubernetes/core/{controller,hosts}.yaml`
(controller and hosts only: no Postgres, mail or Gateway;
`K2_PROVISIONER=static`).

The daemon side (`K2_ROLE=host`, `/cli/hosts/*`) is being built by
`dist-link`. Every env name below that it has not confirmed yet is marked
**provisional**.

## The change

```mermaid
graph TD
    %% ── Colour legend ──────────────────────────────────────────────
    %% Blue = exists today · Violet = changed · Green = new
    %% Orange = outside K2 · Slate = files, volumes, env, Secrets
    %% Shapes: rect = process, stadium = files/volume, hexagon = derived,
    %%         flag = env var or setting

    %% ── MAC · the owner's machine ─────────────────────────────────
    subgraph mac [Mac · owner]
        UP[up.sh --dist / down.sh --dist / s1.sh --dist<br/><sub>scripts/kind · --fake for the test variant</sub>]:::changed
        PF[kubectl port-forward<br/><sub>127.0.0.1:7443 (fake 7442) → svc/k2-controller:7420</sub>]:::external
        APP[K2 desktop app<br/><sub>Connected Servers · owner + password</sub>]:::existing
        UP -- starts --> PF
        APP --> PF
    end

    %% ── OVERLAY · deploy/kind/dist ────────────────────────────────
    subgraph ovl [Overlay · deploy/kind/dist]
        BASE([controller.yaml, hosts.yaml<br/><sub>deploy/kubernetes/core · end state, untouched</sub>]):::existing
        KZ{{dist overlay<br/><sub>static provisioner · no PG/mail/Gateway · kind images</sub>}}:::new
        NK{{no-k2dev component<br/><sub>deploy/kind/components · both ConfigMaps</sub>}}:::existing
        BASE --> KZ
        NK --> KZ
    end

    %% ── KIND · namespace k2-kind-dist (fake: k2-kind-dist-fake) ───
    subgraph kind [kind k2lab · namespace k2-kind-dist]
        SEC([Secret k2-secrets<br/><sub>host-enroll-token · made by up.sh, never printed</sub>]):::state
        CTL[k2-controller-0<br/><sub>k2-daemon:dev · K2_ROLE=controller · K2_HOSTS=1</sub>]:::changed
        HOST[k2-host-0<br/><sub>k2-agents:dev · K2_ROLE=host (provisional)</sub>]:::new
        CL[claude / k2-fake-agent<br/><sub>runs in k2-host-0 only</sub>]:::new
        CPVC([PVC data-k2-controller-0<br/><sub>/var/lib/k2 · k2.db, users, daemon.token</sub>]):::state
        HPVC([PVC data-k2-host-0<br/><sub>/var/lib/k2-host = HOME · .claude, .k2</sub>]):::state
        SEC -. bootstrap token file .-> CTL
        SEC -. enroll token file .-> HOST
        HOST -- "dials ws://k2-controller:7420 /cli/hosts/enroll, /attach" --> CTL
        HOST -- spawns --> CL
        CTL --> CPVC
        HOST --> HPVC
    end

    UP -- "kind load, kubectl apply" --> KZ
    KZ -- applied as --> CTL
    KZ -- applied as --> HOST
    UP -- "creates (once)" --> SEC
    PF --> CTL

    classDef existing fill:#dbeafe,stroke:#2563eb,color:#1e3a5f
    classDef changed  fill:#ede9fe,stroke:#7c3aed,color:#3b0764
    classDef new      fill:#d1fae5,stroke:#059669,color:#064e3b
    classDef external fill:#ffedd5,stroke:#ea580c,color:#7c2d12
    classDef state    fill:#f1f5f9,stroke:#64748b,color:#1e293b
```

## Up, enroll, spawn on the host

```mermaid
sequenceDiagram
    participant U as up.sh --dist<br/>(Mac)
    participant K as kind k2lab<br/>(explicit kubeconfig)
    participant C as k2-controller-0<br/>(K2_ROLE=controller)
    participant H as k2-host-0<br/>(K2_ROLE=host)
    participant S as s1.sh --dist<br/>(the app's routes)

    Note over U,K: Phase 1: up
    U->>U: build k2-agents:dev (or :fake), Docker lock held
    U->>K: kind load k2-daemon:dev and the host image
    U->>K: apply Namespace, then Secret k2-secrets (if missing, from stdin)
    U->>K: apply deploy/kind/dist (controller, hosts)
    K->>C: start (PVC /var/lib/k2)
    K->>H: start (PVC /var/lib/k2-host = HOME)

    Note over C,H: Phase 2: enroll (dist-link, provisional)
    H->>C: WS /cli/hosts/enroll + bootstrap token
    C-->>H: host identity (stored on the host's PVC)
    H->>C: WS /cli/hosts/attach (kept open, reconnects)

    Note over U,C: Phase 3: the app's door
    U->>K: port-forward 127.0.0.1:7443 → svc/k2-controller
    U->>C: /cli/users/add + set-role owner (owner token from the pod, memory only)
    U-->>U: print URL, user, password

    Note over S,H: Phase 4: proof (fake variant, port 7442)
    S->>C: POST /cli/sessions/v2/spawn {command: claude, cwd: /work/lab-agent}
    C->>H: run the agent (over the attach link)
    H->>H: k2-fake-agent as claude
    S->>C: WS /cli/sessions/grid, type
    C-->>S: banner, reply (relayed from the host)
    S->>K: /proc scan: agent in k2-host-0, not in k2-controller-0
    S->>K: delete pod k2-host-0, wait, spawn again
```

## How to try it

Not runnable until `dist-link` lands `K2_ROLE=host` and `/cli/hosts/*`. The
commands the finished task gives:

```sh
scripts/kind/up.sh --dist --fake     # k2-kind-dist-fake on 127.0.0.1:7442
scripts/kind/s1.sh --dist            # S1 through 7442, plus placement and host-restart checks
scripts/kind/down.sh --dist --fake

scripts/kind/up.sh --dist            # k2-kind-dist on 127.0.0.1:7443 (k2-agents:dev, real claude)
scripts/kind/down.sh --dist
```

## Status

- **Milestone:** manifests aligned with dist-link's final env contract
  (`k2-dist/dist-link` @862b3c762). Waiting for RESUME-BUILD to build `k2-daemon:dist`
  (capped, see below), then run.
- **First build attempt (18:35–19:05), killed by the coordinator:**
  - `packaging/oci/build.sh` compiles inside `docker build`, where
    BuildKit can't cap memory. After 29 min the Docker API had been wedged
    for 26 min (the VM at about 650% CPU, kind starved, the owner's 7440
    down).
  - Replacement: `scripts/kind/build-daemon.sh <tree> <tag>`. Its steps:
    - compiles in a plain `docker run` on `k2-kind-toolchain:1.99.1`
      (`deploy/kind/Dockerfile.toolchain`, the same packages as
      packaging/oci's toolchain stage), with `--memory 4g
      --memory-swap 4g --cpus 2` and `CARGO_BUILD_JOBS=2`;
    - mounts the source read-only, with cargo's registry and target in
      named volumes;
    - builds the image with `deploy/kind/Dockerfile.daemon-prebuilt` (the
      runtime stage of packaging/oci, unchanged), which copies the two
      binaries in, with no compile;
    - holds the Docker lock and a cargo slot for the whole run.
  - The agents images build FROM it, so the binary is compiled once.
  - `up.sh` calls it before taking its own lock. Not run yet: waiting for
    the coordinator's RESUME-BUILD.
- **Contract, as used here:**
  - controller: `K2_ROLE=controller`, `K2_HOSTS=1`,
    `K2_HOST_BOOTSTRAP_TOKEN_FILE`, `K2_DAEMON_PORT=7420`, `K2_LISTEN=lan`.
  - host: `K2_ROLE=host`, `K2_CONTROLLER_URL=ws://k2-controller:7420`,
    `K2_HOST_ENROLL_TOKEN_FILE` (same Secret key), `K2_HOST_NAME` (the pod
    name), `K2_DAEMON_PORT=7420` (loopback only).
  - Dropped `K2_HOST_TOKEN_FILE`, the relayed name: dist-link uses
    deploy/README's. `K2_PROVISIONER=static` stays (declared in
    deploy/README; dist-link doesn't read it yet).
  - Readiness: the controller's `/boot-status`. For the host, an exec probe
    curls its loopback `/cli/hosts/status` for `"linked":true`, because the
    host's API listens on loopback only, so the kubelet can't `httpGet`
    it.
- **Workspaces (coordinator decision):** one `k2-work` PVC (RWO,
  local-path) at `/var/lib/k2/work` in both pods, because the app browses
  the controller's files and dist-link sends that cwd to the host verbatim.
  The host has podAffinity to the controller (RWO is per node). Each pod
  keeps its own HOME volume. `up.sh --dist` seeds `/var/lib/k2/work/demo`
  from the host pod.
- **Host UID 10000** (the end state uses 10001): both pods write the shared
  workspaces, and git refuses a repo another UID owns.
- **Images:**
  - `k2-daemon:dist` is built from a dist-link checkout:
    `K2_KIND_DAEMON_SRC=<that worktree> scripts/kind/up.sh --dist [--fake]`.
    `up.sh` refuses a tree without `crates/k2-daemon/src/hosts`.
  - `k2-agents:dist` and `k2-agents:dist-fake` build `FROM k2-daemon:dist`
    (`--build-arg BASE_IMAGE`).
  - Rust-compiling builds take a cargo slot and use `CARGO_BUILD_JOBS=3`.
- **The Mac's door:** proxy container `k2-kind-dist-fake-proxy` on
  127.0.0.1:7442 to NodePort 30742 (`k2-controller-nodeport`); dist uses
  7443 and 30743.
- **Limits of this cut (from dist-link's notes), relevant to the app:**
  - The controller relays `/cli/sessions/grid` for the owner token only. A
    desktop-app login (Connect session) gets an error frame, so the owner's
    app can't yet attach to an agent's terminal on the dist variant. S1
    uses the owner token.
  - The controller 501s the LLM login routes. `/login` inside a claude
    session would go through the grid, so it is blocked for the app by the
    same limit.
  - Mac paths aren't mapped. The app must send pod paths, which the
    controller's folder picker gives it.
- **Acceptance (pending the run):** on `k2-kind-dist-fake`, `s1.sh --dist`
  must show S1 PASS through 7442, the fake agent in `k2-host-0` and not
  in `k2-controller-0`, then S1 PASS again after `k2-host-0` is deleted.
