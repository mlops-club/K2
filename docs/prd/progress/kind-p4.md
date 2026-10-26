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

## Acceptance (plan T4)

| # | Check | Status |
|---|---|---|
| 1 | Drain demo on kind: same session id, `MEMORY.md` sha256 equal, I1–I4; a cast and an evidence bundle | **FAIL on rc1** (checks 1–4 pass): a drained agent doesn't move to the other host (above). Blocked on the daemon |
| 2 | The lab's S1–S9 and I1–I4 on the pool through `fleet-g2.sh` (pool mode) | **PASS, first half** (2026-10-26, evidence `lab-runs/kind-p4/pool-g2/`): `k2-kind-dist-fake` on rc1 c59acf7e0 (daemon, fake agent and lab), `K2_PROVISIONER=kubernetes` MIN 2 MAX 3, enroll, `K2_PTYD=1`, `K2_WORK_ROOT`. S1–S9 PASS; I1 (43 samples, 18 CLIs, never two live for one agent, 19 placements), I2 (4 messages final), I3 (4 resumes, session and MEMORY.md kept), I4 (15 home writes, never two hosts at once) PASS. The pool owned the replicas and tokens; k2-host-2 was known to the fleet (its volume mounted) but never needed: at `k2.max_awake=100` nothing filled two hosts. **Second half, after k2-dist/drain-move**: a small `K2_HOST_MAX_AWAKE` so the pool adds k2-host-2 mid-run, and its idle scale-down releases agents (the drain path drain-move fixes) |
| 3 | `prove.sh G4 --topology kind` | not started (with the lab worker) |
| 4 | k3s: the script and steps for the owner, never run by a worker | not started |
