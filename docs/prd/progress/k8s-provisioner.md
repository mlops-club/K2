# k8s-provisioner: the host pool on Kubernetes (P4 T1)

Task `k8s-provisioner`, branch `k2-dist/k8s-provisioner` from
`k2-dist/wave2-preview` @3912ab2ff. Plan: `docs/prd/progress/p4-plan.md`
(branch `k2-dist/p4-plan`), section 1 and task T1. Owner decisions taken
per the plan's recommendations: D1 a small typed REST client on the
daemon's HTTP stack, D2 the controller scales between a minimum (1) and a
maximum, D8 W5 enroll per ordinal, D10 `k2-daemon --host-health`.

## The change

```mermaid
graph LR
    %% ── Colour legend ──────────────────────────────
    %% Blue = exists today · Violet = changed · Green = new
    %% Orange = outside K2 · Slate = state and settings
    %% Shapes: rect = process or module, stadium = Secret or volume,
    %% hexagon = an object Kubernetes holds, cylinder = database, flag = setting

    %% ── CONTROLLER POD · k2-daemon K2_ROLE=controller ──
    subgraph ctl [Controller pod · k2-daemon K2_ROLE=controller]
        RH[RemoteHost<br/><sub>agent_host/remote.rs · links, placements</sub>]:::existing
        PV[Provisioner trait<br/><sub>provisioner/mod.rs · P3 T2</sub>]:::existing
        KP[Kubernetes provisioner<br/><sub>provisioner/kubernetes.rs · pool scaler</sub>]:::new
        KC[k8s client<br/><sub>k8s/ · reqwest + rustls · ServiceAccount</sub>]:::new
        EN[enroll Registry<br/><sub>hosts/enroll.rs · mint_token</sub>]:::existing
        DB[(k2.db<br/><sub>host_keys, enroll_tokens, placements</sub>)]:::state
        SET>K2_PROVISIONER=kubernetes<br/><sub>MIN_HOSTS 1 · MAX_HOSTS · IDLE_AFTER 15 min</sub>]:::state
        RH -- need a host --> PV
        PV -- kind = kubernetes --> KP
        KP -- mint for ordinal N --> EN
        KP -- scale, Secret, pods --> KC
        EN --- DB
        SET -.-> KP
    end

    %% ── NAMESPACE k2 · HOST POOL ───────────────────
    subgraph pool [Namespace k2 · host pool]
        STS{{StatefulSet k2-host<br/><sub>statefulsets/scale · replicas</sub>}}:::external
        ES([Secret k2-host-enroll<br/><sub>key k2-host-N · one-time token</sub>]):::state
        H[k2-host-N<br/><sub>K2_ROLE=host · enroll, signed hello</sub>]:::existing
        HH[k2-daemon --host-health<br/><sub>readinessProbe · exit 0 when linked</sub>]:::new
        STS --> H
        ES -.-> H
        HH -- GET /cli/hosts/status --> H
    end

    API[Kubernetes API<br/><sub>in-cluster · RBAC namespace k2</sub>]:::external
    KC -- PATCH scale, PATCH Secret, GET or watch pods --> API
    API --> STS & ES
    H -- enroll, then signed hello --> RH
    RH -- hello from k2-host-N --> KP

    classDef existing fill:#dbeafe,stroke:#2563eb,color:#1e3a5f
    classDef changed  fill:#ede9fe,stroke:#7c3aed,color:#3b0764
    classDef new      fill:#d1fae5,stroke:#059669,color:#064e3b
    classDef external fill:#ffedd5,stroke:#ea580c,color:#7c2d12
    classDef state    fill:#f1f5f9,stroke:#64748b,color:#1e293b
```

`Provisioner trait` is drawn blue because P3 T2 owns it; it isn't on
`wave2-preview` yet (see Status).

## The flows

### Scale up for a wake, scale down when idle

```mermaid
sequenceDiagram
    autonumber
    participant N as need<br/>(wake, k2 msg --wake, app)
    participant C as controller<br/>(Kubernetes provisioner)
    participant K as Kubernetes API
    participant H as k2-host-N<br/>(new pod)

    Note over N,H: Phase 1: scale up (ensure_host)
    N->>C: agent X needs a host
    C->>C: every linked host is at k2.max_awake, pool < MAX_HOSTS
    C->>C: mint a one-time token for k2-host-N (W5)
    C->>K: PATCH Secret k2-host-enroll data.k2-host-N
    C->>K: PATCH statefulsets/k2-host/scale replicas N+1
    C-->>N: Wait(scaling k2-host-N)
    K-->>H: pod scheduled, volume attached
    H->>C: enroll (token, new key), then signed hello
    Note over C: the trigger is the hello, never pod Ready
    C->>C: retry X's need: choose picks k2-host-N
    Note over N,H: Phase 2: at the maximum
    N->>C: another need, every host full, pool = MAX_HOSTS
    C-->>N: refused: K2_PROVISIONER_MAX_HOSTS=3 reached (3 hosts at k2.max_awake)
    Note over N,H: Phase 3: scale down (idle)
    C->>C: spare room for IDLE_AFTER, pool > MIN_HOSTS
    C->>C: mark the highest ordinal draining (never chosen)
    C->>H: release its agents (P3 sleep, home snapshot)
    H-->>C: released
    C->>K: PATCH scale replicas N (volume and key stay)
```

## How to try it

```sh
# toolchain (shared machine: 3 jobs, 3 test threads)
export RUSTUP_HOME=$HOME/.cache/k2-dist-rust/rustup CARGO_HOME=$HOME/.cache/k2-dist-rust/cargo
export PATH=$CARGO_HOME/bin:$PATH CARGO_BUILD_JOBS=3

# the client, the pool logic and --host-health against fakes
~/.cache/k2-dist-cargo-slot.sh cargo test -p k2-daemon --lib -- k8s:: provisioner::kubernetes host_health --test-threads=3

# kind (k2lab only, explicit kubeconfig, namespace k2-p4-pool): see "kind" below once it lands
kind get kubeconfig --name k2lab > "$TMPDIR/k2lab.kubeconfig"
kubectl --kubeconfig "$TMPDIR/k2lab.kubeconfig" -n k2-p4-pool get statefulset,pods,pdb
```

## Status

- Milestones: diagrams written.
- Dependencies not on `wave2-preview` @3912ab2ff:
  - P3 T2 phase A (`provisioner/` trait, `agent_placement`): no branch yet;
  - S8 SIGTERM drain: in progress on `k2-dist/g2-fixes` (outbox worker).
- Building now (no dependency on either): the `k8s/` client (D1), `--host-health`
  (D10), and the pool scaler in `provisioner/kubernetes.rs` against P3's
  trait sketch (`Need`, `HostView`, `Ensure`), with a one-file adapter when
  P3 T2 lands.
- Waits for P3 and S8: the kind acceptance (six wakes → 3 pods; back to 1
  after the idle window with homes snapshotted).
