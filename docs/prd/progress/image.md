# k2-dist: image (P0, K2 in a container)

Today's K2 (`K2_ROLE=all`: controller plus the in-process agent host) in one
OCI image, run the same way under Docker Compose on the Mac and as one pod on
the kind cluster `k2lab`. This is the PRD's "Container" topology for gate G0.

## The change

```mermaid
graph TD
    %% ── Colour legend ──────────────────────────────────────────────
    %% Blue = exists today · Violet = changed · Green = new
    %% Orange = outside K2 · Slate = files, volumes, env
    %% Shapes: rect = process, stadium = files/volume, hexagon = derived,
    %%         flag = env var or setting

    %% ── BUILD · this Mac ──────────────────────────────────────────
    subgraph build [Build · Docker 29.6 on this Mac]
        DF([Dockerfile<br/><sub>packaging/oci/Dockerfile · multi-stage</sub>]):::new
        BS[build.sh<br/><sub>packaging/oci/build.sh · tag k2-daemon:dev</sub>]:::new
        IMG{{k2-daemon:dev<br/><sub>/usr/bin/k2-daemon, /usr/bin/k2-ptyd · UID 10000</sub>}}:::new
        BS -- docker build --> DF
        DF -- produces --> IMG
    end

    %% ── CONTAINER · k2-daemon image ───────────────────────────────
    subgraph ctr [Container · k2-daemon:dev]
        ENV>K2_ROLE=all · K2_LISTEN=lan<br/><sub>K2_DAEMON_PORT=7420 · K2_CAP_*</sub>]:::state
        D[k2-daemon<br/><sub>crates/k2-daemon · daemon_main.rs</sub>]:::changed
        RR[runtime_role<br/><sub>crates/k2-daemon · runtime_role.rs</sub>]:::new
        DP[fixed port<br/><sub>crates/k2-daemon · daemon_port.rs</sub>]:::new
        CAP[capabilities<br/><sub>crates/k2-daemon · capabilities.rs</sub>]:::new
        IK[install kind<br/><sub>classify_install_kind · /usr/ ⇒ package</sub>]:::changed
        BST{{GET /boot-status<br/><sub>phase=ready · installKind=package · capabilities</sub>}}:::changed
        VOL([state<br/><sub>/var/lib/k2 · HOME until K2_HOME lands</sub>]):::state
        ENV -.-> D
        D -- checks at boot --> RR
        D -- binds --> DP
        D -- detects once --> CAP
        D -- reports --> IK
        DP -- writes daemon.port --> VOL
        D -- k2.db, vault.db --> VOL
        CAP --> BST
        IK --> BST
    end

    %% ── OTHER BINARIES · same install-kind rule ───────────────────
    subgraph others [Other binaries · not in this image]
        RH[k2-root-helper<br/><sub>helper self-update</sub>]:::changed
        AE[k2-app-engine<br/><sub>installed via apps install-engine</sub>]:::changed
        ND[k2-node<br/><sub>update offer from its server</sub>]:::changed
    end
    IK -. same rule .-> RH
    IK -. same rule .-> AE
    IK -. same rule .-> ND

    %% ── RUNNERS ────────────────────────────────────────────────────
    subgraph compose [Docker Compose · compose.solo.yaml]
        CS[k2 service<br/><sub>127.0.0.1:7420 · volume k2-data</sub>]:::new
    end
    subgraph kind [kind k2lab · namespace k2-solo]
        STS[StatefulSet k2-solo<br/><sub>packaging/oci/kind/solo.yaml · PVC · restricted PSS</sub>]:::new
    end
    IMG --> CS
    IMG -- kind load --> STS
    CS -- runs --> D
    STS -- runs --> D
    CURL[curl / kubectl port-forward<br/><sub>owner on the Mac</sub>]:::external
    CURL --> BST

    classDef existing fill:#dbeafe,stroke:#2563eb,color:#1e3a5f
    classDef changed  fill:#ede9fe,stroke:#7c3aed,color:#3b0764
    classDef new      fill:#d1fae5,stroke:#059669,color:#064e3b
    classDef external fill:#ffedd5,stroke:#ea580c,color:#7c2d12
    classDef state    fill:#f1f5f9,stroke:#64748b,color:#1e293b
```

## Boot in a container

```mermaid
sequenceDiagram
    participant R as docker / kubelet<br/>(runner)
    participant D as k2-daemon<br/>(K2_ROLE=all)
    participant RR as runtime_role<br/>(new)
    participant P as daemon_port<br/>(new)
    participant C as capabilities<br/>(new)
    participant V as /var/lib/k2<br/>(volume)
    participant H as healthcheck<br/>(curl / readinessProbe)

    Note over R,D: Phase 1: start and settings
    R->>D: exec /usr/bin/k2-daemon (UID 10000 or any UID, GID 0)
    D->>RR: K2_ROLE
    alt K2_ROLE = controller
        RR-->>D: Err "K2_ROLE=controller needs agent hosts (PRD P2); not available yet"
        D-->>R: exit 2
    else K2_ROLE not all/controller
        RR-->>D: Err "allowed values: all, controller"
        D-->>R: exit 2
    else unset or all
        RR-->>D: all
    end

    Note over D,V: Phase 2: bind
    D->>P: K2_DAEMON_PORT=7420
    alt port taken
        P-->>D: Err "K2_DAEMON_PORT=7420 is in use"
        D-->>R: exit 2 (no random fallback)
    else free
        P-->>D: listener on 0.0.0.0:7420 (K2_LISTEN=lan)
        D->>V: write .k2/daemon.port = 7420
    end

    Note over D,C: Phase 3: capabilities (computed, logged, not acted on)
    D->>C: detect(env K2_CAP_*, root helper present?, keychain?)
    C-->>D: postgres/mail/sew/apps/front/vault = host-native | delegated | off
    D->>D: log one line "[daemon] capabilities: …"

    Note over D,H: Phase 4: ready
    D->>D: migrations, boot_status::set_ready()
    H->>D: GET /boot-status
    D-->>H: 200 phase=ready, installKind=package, capabilities{…}
```

## How to try it

Not runnable yet (diagrams milestone). The commands the finished task gives:

```sh
# Build (host arch; kind's nodes match it)
packaging/oci/build.sh                        # → k2-daemon:dev

# Compose on this Mac
docker compose -f deploy/compose/compose.solo.yaml up -d --wait \
  && curl -fsS http://127.0.0.1:7420/boot-status | jq .

# Random UID
docker run --rm -d --name k2-uid --user 54321:0 -e K2_DAEMON_PORT=7420 -e K2_LISTEN=lan \
  -p 127.0.0.1:7430:7420 k2-daemon:dev

# kind k2lab, explicit kubeconfig only
kind get kubeconfig --name k2lab > "$TMPDIR/k2lab.kubeconfig"
kind load docker-image k2-daemon:dev --name k2lab
kubectl --kubeconfig "$TMPDIR/k2lab.kubeconfig" apply -f packaging/oci/kind/solo.yaml
kubectl --kubeconfig "$TMPDIR/k2lab.kubeconfig" -n k2-solo rollout status sts/k2-solo
kubectl --kubeconfig "$TMPDIR/k2lab.kubeconfig" -n k2-solo port-forward svc/k2-solo 7440:7420 &
curl -fsS http://127.0.0.1:7440/boot-status | jq .
```

## Status

- **Milestone:** diagrams written.
- **Next:** env settings modules (`runtime_role.rs`, `daemon_port.rs`,
  `capabilities.rs`) with unit tests, then the image, Compose and kind.
- **Open questions (raised with the coordinator):**
  1. `/boot-status` shape is pinned: `tests/golden/permission_off/daemon/boot_status.txt`
     lists every top-level key of the full body, so an additive
     `capabilities` key fails `permission_off_golden`. Where do capabilities go?
  2. Sharing `classify_install_kind` with root helper, app engine and node:
     those crates can't depend on `k2-daemon`; the common crate they all
     depend on is `k2-root-proto`.

### Decisions inside owned files

- Capability values: the `K2_CAP_*` override values come from
  `deploy/README.md` (`host`, `external`, `engine`, `kubernetes`, `listen`,
  `off`). The report maps them onto the PRD's three states: `host` ⇒
  `host-native`; `external`, `engine`, `kubernetes`, `listen` ⇒ `delegated`;
  `off` ⇒ `off`. Anything else is a boot error naming the allowed values.
- Image contents: `k2-daemon` and `k2-ptyd` only. No root helper (no sudo in a
  container), so with no override every root-helper feature detects `off`.
