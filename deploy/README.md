# K2 deployment end state

These files declare what a distributed K2 looks like when the PRD
"K2 Distributed Runtime" is done (gate G4). They exist **before** the code so
the interfaces can be reviewed first. **None of it runs yet**: the
`ghcr.io/alakazam-211/k2-daemon`, `k2-host`, `k2-app-engine` and `sew` images
don't exist, and everything marked *new* below is work for stages P0–P4.

| Path | Mode |
|---|---|
| `compose/compose.yaml` | Docker Compose on one machine: controller, two agent hosts, Postgres, mail, Sew, Apps |
| `kubernetes/` | Kubernetes (kustomize): `core/` in namespace `k2`, `edge/` (Sew) in `k2-edge` |
| `kubernetes/examples/` | Objects the controller creates at runtime: a per-agent Sandbox and one App |

Single-machine mode (today's install scripts) needs no files here: it stays
one `k2-daemon` service with the agent host inside it.

## The two programs

Every mode runs the same two programs.

- **`k2-daemon`**, the controller. With `K2_ROLE=all` (the default, today's
  behaviour) it also runs agents in-process. With `K2_ROLE=controller` it runs
  none and waits for hosts to dial in.
- **`k2-host`**, an agent host, grown from `k2-node` and `k2-ptyd`. It dials out
  to one controller, runs agent PTYs and CLIs as child processes, checks hook
  ownership locally and drains on SIGTERM.

## Interfaces

### Controller (`k2-daemon`)

| Setting | Status | Meaning |
|---|---|---|
| `K2_HOME` | new (P0) | Root for every path under `~/.k2` today |
| `K2_ROLE` | new | `all` or `controller` |
| `K2_LISTEN=lan` | existing | Bind `0.0.0.0` instead of loopback |
| `K2_DAEMON_PORT` | new | Fixed port instead of the sticky one in `daemon.port` |
| `K2_TRACE_FILE` | new (P1) | Absolute path; the daemon appends one JSON line per trace event (the lab's format). Unset is off; a relative or unwritable path refuses to start (exit 2) |
| `K2_HOSTS=1` | new | Turns on host routes, as `K2_COMPUTE=1` does for compute nodes |
| `K2_HOST_BOOTSTRAP_TOKEN_FILE` | new | Hosts presenting this token enroll without the confirmation-code step |
| `K2_PROVISIONER` | new | `static` (hosts enroll themselves) or `kubernetes` (scale `StatefulSet/k2-host`, create Sandboxes) |
| `K2_KEK_FILE` | new | Vault key from a file or Secret, instead of the keychain or the root helper's `keep` |
| `K2_CAP_POSTGRES`, `K2_CAP_MAIL`, `K2_CAP_SEW`, `K2_CAP_APPS`, `K2_CAP_FRONT` | new | Per feature: `host` (root helper, today), `external`, `engine`, `kubernetes`, `listen` or `off` |
| `GET /boot-status` | existing | Health; ready when `phase` is `ready` |
| `GET /cli/hosts/enroll`, `GET /cli/hosts/attach` | new | WebSockets for hosts, mirroring `/cli/compute/enroll` and `/cli/compute/attach` |

### Agent host (`k2-host`)

| Setting | Status | Meaning |
|---|---|---|
| `K2_CONTROLLER_URL` | new | `ws://` inside a cluster or Compose network, `wss://` across the internet |
| `K2_HOST_NAME`, `K2_HOST_LABELS` | new | As `k2-node enroll --name/--label` today |
| `K2_HOST_ENROLL_TOKEN_FILE` | new | Bootstrap token; laptops use `k2-host enroll --controller … --enroll <code>` instead |
| `K2_HOST_MAX_AWAKE` | new | Awake agents this host accepts |
| `K2_HOST_LOCAL_ATTACH` | new | Local port the app attaches to directly when the controller is unreachable |
| `K2_HOST_SINGLE_AGENT` | new | Per-agent Sandbox tier: accept only this agent |
| `k2-host health` | new | Exit 0 when linked to the controller |
| SIGTERM | new behaviour | Drain: stop taking work, wait for each turn's Stop hook, save session ids, exit |

Agent CLIs keep today's environment: `K2_CELL`, `K2_SESSION_ID`,
`K2_HOOK_SOCK`, `K2_HOOK_TOKEN`.

## Decisions these files follow

- `docs/adr/adr-0001-agent-isolation-inside-host-pods.md`: agents share a host
  pod with process-level isolation by default; per-agent UIDs are an opt-in
  profile.
- `docs/adr/adr-0002-single-controller.md`: one controller replica, no HA.

## Checking the files

```sh
# Compose: syntax and references (needs the five files under compose/secrets/)
docker compose -f deploy/compose/compose.yaml config -q

# Kubernetes: every object against the real APIs. Needs a cluster with the
# Gateway API v1.6 CRDs, the Zalando postgres-operator v2 and agent-sandbox.
kubectl apply -f deploy/kubernetes/namespaces.yaml
kubectl apply -k deploy/kubernetes --dry-run=server
kubectl apply -f deploy/kubernetes/examples/ --dry-run=server
```

On 2026-10-24 both checks passed: Compose 2 on Docker 29.6, and a kind cluster
running Kubernetes 1.37 with Gateway API 1.6.3, Zalando postgres-operator
2.0.3 and agent-sandbox 1.0.8. The one warning was expected: Stalwart runs as
root, so namespace `k2` enforces Pod Security `baseline`, not `restricted`.

The Postgres contract was also run for real, since the controller depends on
it: `core/postgres.yaml` brought up PostgreSQL 17.9; patching in `app_notes`
(as in `examples/app.yaml`) produced the Secret
`app-notes.k2-pg.credentials.postgresql.acid.zalan.do` with `username` and
`password`, and that user could log in to database `app_notes`.
