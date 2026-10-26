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

## Acceptance (plan T4)

| # | Check | Status |
|---|---|---|
| 1 | Drain demo on kind: same session id, `MEMORY.md` sha256 equal, I1–I4; a cast and an evidence bundle | written; kind run pending (needs the Docker lock for the fake agent image) |
| 2 | The lab's S1–S9 and I1–I4 on the pool through `fleet-g2.sh` (pool mode) | not started: `fleet_kind.py` pool mode |
| 3 | `prove.sh G4 --topology kind` | not started (with the lab worker) |
| 4 | k3s: the script and steps for the owner, never run by a worker | not started |
