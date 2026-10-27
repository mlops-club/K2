# kind-p4: the host pool on kind, the drain demo (P4 T4)

Branch `k2-dist/kind-p4` (from `k2-dist/kind-app`). Plan:
`docs/prd/progress/p4-plan.md` on `k2-dist/p4-plan`, task T4 and design
section 5. Daemon: wave3 rc1 (c59acf7e0, `k2-daemon:rc1` from dist-link),
which has the pool (T1), home sync (P3 T1) and turn-end snapshots (P3 T2).

## What's built

| Piece | What it does |
|---|---|
| `deploy/kind/p4/` | TEST ONLY overlay over `deploy/kind/dist-fake`, namespace `k2-kind-p4`, port 7444 (NodePort 30744), images `k2-daemon:p4` and `k2-agents:p4`. **No shared `k2-work`** (P3 D9): each host has its own `work` volume (claim template), the controller its own (`k2-work-controller`). Hosts on different worker nodes (required anti-affinity), not tied to the controller's node, and never on a node labelled `k2.dev/p4-drained`. The end state's PDB (`maxUnavailable: 1`), which `deploy/kind/dist` drops. `K2_HOST_DRAIN_SECS=90` under a 120 s grace, so a drain can wait out a turn |
| `scripts/kind/lib.sh`, `up.sh --p4` | the `p4` variant (implies `--dist --fake`) |
| `scripts/kind/p4-up.sh` | tags the given daemon image (default `k2-daemon:rc1`) as `k2-daemon:p4`, builds the fake agent from `--fake-src` (default `../rc1-src`, a detached rc1 checkout: its fake agent knows `[[turn-ms]]` and `[[remember:]]`), runs `up.sh --p4` with the pool (MIN = MAX = 2), enroll, `K2_PTYD=1` and `K2_WORK_ROOT=/var/lib/k2/work`, and checks two linked hosts on two nodes with their own volumes and the PDB |
| `scripts/kind/drain-demo.sh` | design section 5, scoped to `k2-kind-p4` (below) |
| `scripts/kind/fleet-g2.sh` pool mode | automatic when the controller runs `K2_PROVISIONER=kubernetes`: the fleet knows every host the pool may make (k2-host-0 .. MAX-1; one with no pod is down, only the pool starts it); their HOME volumes are made before the runner, which mounts them all (the StatefulSet takes an existing claim of its name); the fresh step deletes them all and mints only for running hosts |

## Decision: the drain is scoped to the namespace

`kubectl drain` and `kubectl cordon` act on a whole node. k2lab's workers
hold the owner's `k2-kind-dist` and `k2-kind` (worker2) and other workers'
namespaces (worker), so a real drain would evict or strand their pods (a
pod on local-path can only come back on its node). The demo does what a
drain does, for `k2-kind-p4` only:

- **cordon**: label the node `k2.dev/p4-drained=true`; only `k2-kind-p4`'s
  hosts refuse it (their required node affinity);
- **drain**: evict the host's pod through the Eviction API, the call
  `kubectl drain` makes: the PDB is honoured, the pod gets SIGTERM and its
  grace period;
- **uncordon**: remove the label.

On the owner's k3s cluster the plan's owner-run steps can use a real
`kubectl drain`; the script for that stays owner-run only.

## Findings from the design

- **A drained agent moves on its next need, not by itself.** rc1's drain
  (`hosts/drain.rs`): the host waits for the turn in progress, saves the
  home, sends `released`; the controller records the agent for resume on
  *that* host. A need (a message, a wake) places it the usual way, on any
  linked host. `ctl.move_decision` is only for hosts lost without a drain
  (S10). So the demo checks the release, then a message placing it on the
  other host at a higher epoch with the home restored and `--resume`.
- **Grace vs drain window.** The dist hosts' 60 s grace is shorter than
  rc1's default 600 s drain wait: a drain waiting on a long turn would be
  SIGKILLed before its release save. The p4 overlay sets 120 s / 90 s.

## First kind run (2026-10-26, rc1 c59acf7e0): the drain works, the move doesn't

`p4-up.sh`: PASS (two linked hosts on worker2 and worker, each with its
own data and work volumes, no shared `k2-work`, the PDB allows one).

`drain-demo.sh` (evidence `lab-runs/kind-p4/drain-20261026T124000Z/`):
checks 1–4 PASS, 5–7 FAIL, on one cause.

- PASS: demo placed on k2-host-0, its fact in MEMORY.md; a 30 s turn in
  progress; the PDB allowed the eviction; k2-host-0 Pending ("didn't
  match Pod's node affinity", and its PV's); the controller marked it
  draining. From the controller's log: the turn ended inside the drain
  (Stop 28 s after the SIGTERM), snapshot 2 was saved, and the agent was
  released: "resumed when the host is back or on the next need".
- FAIL: the next message (`/cli/workspace/msg`, `wake=true`) was only
  queued. demo was never placed on k2-host-1. It waited 4 minutes, until
  the "uncordon" let k2-host-0 back, and resumed there (same Claude
  conversation, epoch + 1).

**Cause, in rc1's daemon (not this branch):** a drain writes the agent's
resume row with the drained host's name (`agent_host/remote.rs:1866`);
`resume_due` resumes it only when *that* host links again; while it waits,
`delivery_drain.rs` holds its messages (`awaiting_resume`). Only
`move_lost` turns a row into `ANY_HOST`, and it looks only at placements a
lost host still holds, which a drained host released. So **a drained agent
whose host doesn't come back waits indefinitely, messages included**: a node
drained for maintenance, or, likely, a pool scale-down (its host's name
comes back only if the pool scales up again, and nothing asks it to).

Suggested fix (for the daemon's owner): a portable agent's drain resume
row whose host has been unlinked for `K2_HOST_MOVE_AFTER` becomes
`ANY_HOST`, like `move_lost`'s; or a message/wake for an agent awaiting
resume counts as a need and places it on any usable host. Then
`drain-demo.sh` checks 5–7 should pass as written.

## No shared `k2-work`: what moves, and what still needs the shared volume (2026-10-26)

Coordinator's question: with home sync and turn snapshots (rc1), do the
hosts still need the shared ReadWriteOnce `k2-work` and the pod affinity
to the controller? Tried in `k2-kind-p4` (`deploy/kind/p4`: each host only
its own `work` volume with `K2_WORK_ROOT=/var/lib/k2/work`, no affinity to
the controller, hosts on different nodes), rc1 images.

| Check | Result |
|---|---|
| Hosts land on different nodes | k2-host-0 on k2lab-worker2, k2-host-1 on k2lab-worker; claims only `data-*`, `work-k2-host-*`, `k2-work-controller` |
| `app-check.sh --p4` (the app's login, spawn, attach with a session token) | PASS: the CLI ran in k2-host-0 (worker2), nothing in the controller |
| `home-move-check.sh`: an agent's home moves between the two nodes | **PASS** (evidence `lab-runs/kind-p4/home-move-20261026T132616Z/`; the run before it, `…132015Z`, showed the same move and failed only on the check's choice of exec, since fixed): two turns on k2-host-0 (the second saved at its end with a file in the workspace); k2-host-0's daemon, k2-ptyd and CLI killed and held; after rc1's `K2_HOST_MOVE_AFTER` (300 s) the controller moved it (`ctl.move_decision`) to k2-host-1 at a higher epoch; k2-host-1 restored the home (objects fetched > 0) and resumed the same session with `--resume`; the workspace file and MEMORY.md (k2-host-0's bytes first) were there; a message after the move landed there; k2-host-0 relinked and the agent stayed on k2-host-1 with no CLI on k2-host-0 |

**What the hosts don't need:** an agent's own workspace and Claude home
move with it, by home sync and turn snapshots, between nodes, with no
shared volume.

**What still needs the shared volume: the controller's own disk.** The
controller serves these from its local filesystem in rc1, and nothing
relays them to the agent's host. Without `k2-work` they see the
controller's copy, not the agent's:

- **A workspace made through the app never reaches a host.**
  `home-move-check.sh` registered a git workspace on the controller
  (README.md and a commit, `add-from-path`). The agent's host only had the
  directory it creates for a new cwd (no commit, no README). Home sync
  carries a workspace host to host; nothing seeds it from the controller.
- **What the app shows is not what the agent did.** The agent's
  `notes.txt` (on k2-host-0, then k2-host-1) was never in the controller's
  copy, which is the one the file browser and git panel read.

The routes, in rc1 c59acf7e0 (`crates/k2-daemon/src/`), for the relay task
(dist-link, after rc1, coordinator's decision):

| Routes | Where |
|---|---|
| `GET /cli/fs/*` (read-dir, read-file, read-range, read-binary, info, stat, search-tree, zip-list, events, …) | table `misc_routes.rs:1334`–`1352` → `fs_routes::handle_*`; dispatched from `misc_routes::dispatch` (`misc_routes.rs:145`) |
| `POST /cli/fs/*` (create, delete, move, rename, copy, duplicate, compress, extract, …) | `routes/dispatcher.rs:7575` arm → `dispatch_unit6_post` (`routes/dispatcher.rs:13115`, the `/cli/fs/` rows `13118`–`13128`) |
| `POST /cli/fs/write-file` | `routes/dispatcher.rs:7466` (→ `fs_routes::handle_write_file`, row `13120`) |
| `POST /cli/fs/upload-binary`, `upload-chunk` | `routes/dispatcher.rs:5991` |
| `GET /cli/git/*` (changes, diff-file, diff-summary, diff-between, branches, worktrees, file-at-ref, merge-status, info, …) | `routes/dispatcher.rs:12959` arm → table `misc_routes.rs:1475`–`1501` → `git_routes::handle_git_*` |
| `POST /cli/git/*` (commit, stage, unstage, discard, init, create/remove/reopen-worktree, merge-branch, snapshot, notes, …) | `routes/dispatcher.rs:5902` arm → `git_routes::dispatch_unit4_git_post` (`git_routes.rs:792`) |
| `/cli/worktree` | `misc_routes.rs:255` |
| `POST /cli/projects/create` (a new workspace, git init) | `db_routes.rs:1407` → `handle_projects_create` (`db_routes.rs:877`) |
| `POST /cli/projects/add-from-path` (register an existing directory; seeds wiki/AGENTS.md) | `db_routes.rs:1417` → `handle_projects_add_from_path` (`db_routes.rs:1191`) → `pops::projects_add_from_path_ex` |
| Clone-to (`/cli/clone/unpack` lands a project on the local disk) | `clone_routes.rs:418` (`handle_clone_unpack`), `clone_routes.rs:590` |
| App deploy from a folder (`/cli/app/deploy {folder}`: the Seed is built from the controller's disk) | `apps/routes.rs:816` `seed_for_deploy` → `resolve_folder` (`:310`) → `build_seed` (`:482`) |
| `POST /cli/publish/*` (probably: it builds from a workspace; not traced here) | `routes/dispatcher.rs:8856` |

**Decision (coordinator, 2026-10-26):** the shared `k2-work` stays. On kind
it stays as it is (`deploy/kind/dist`, RWO, every pod on the controller's
node). In the k3s runbook it becomes ReadWriteMany (NFS or Longhorn RWX) so
hosts can spread across nodes, with the single-node RWO limit recorded for
clusters without RWX. Relaying fs/git to the agent's host is a new
dist-link task after rc1. `deploy/kind/p4` stays the test shape for moves.

**Script fixes from these runs:** the images have no `ps`/`pgrep`/`pkill`,
so `/proc` scans; a scan must skip its own `sh`, whose command line holds
the pattern (the first `home-move-check.sh` run killed its own shell before
the daemon, and every CLI count was one too high: `drain-demo.sh`'s
"no CLI on SRC" of the first run counted with a missing `pgrep` and is not
evidence); with `K2_PTYD=1` a lost host must take `k2-ptyd` down too (it
keeps the CLIs alive on purpose); an agent's exec is picked by its cwd
(a lost host's agents all move).

## Acceptance (plan T4)

| # | Check | Status |
|---|---|---|
| 1 | Drain demo on kind: same session id, `MEMORY.md` sha256 equal, I1–I4; a cast and an evidence bundle | **FAIL on rc1** (checks 1–4 pass): a drained agent doesn't move to the other host (above). Blocked on the daemon |
| 2 | The lab's S1–S9 and I1–I4 on the pool through `fleet-g2.sh` (pool mode) | **PASS, first half** (2026-10-26, evidence `lab-runs/kind-p4/pool-g2/`): `k2-kind-dist-fake` on rc1 c59acf7e0 (daemon, fake agent and lab), `K2_PROVISIONER=kubernetes` MIN 2 MAX 3, enroll, `K2_PTYD=1`, `K2_WORK_ROOT`. S1–S9 PASS; I1 (43 samples, 18 CLIs, never two live for one agent, 19 placements), I2 (4 messages final), I3 (4 resumes, session and MEMORY.md kept), I4 (15 home writes, never two hosts at once) PASS. The pool owned the replicas and tokens; k2-host-2 was known to the fleet (its volume mounted) but never needed: at `k2.max_awake=100` nothing filled two hosts. **Second half, after k2-dist/drain-move**: a small `K2_HOST_MAX_AWAKE` so the pool adds k2-host-2 mid-run, and its idle scale-down releases agents (the drain path drain-move fixes) |
| 3 | `prove.sh G4 --topology kind` | not started (with the lab worker) |
| 4 | k3s: the script and steps for the owner, never run by a worker | **written**: `docs/prd/progress/k3s-owner-steps.md` (runbook; its overlay renders offline with `kubectl kustomize`, nothing applied anywhere). A script for it is a gap listed there |
