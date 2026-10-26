# granularity: S13 on kind, the pod-granularity record (P4 T5)

Plan: `docs/prd/progress/p4-plan.md` (k2-dist/p4-plan), design section 4
and task T5. Branch `k2-dist/kind-p4` (the coordinator gave T5 to the
kind-app worker while rc1 and drain-move were pending). Decision record:
`docs/adr/adr-0003-pod-granularity.md` (draft; the owner decides at G4).

| Tier | Status |
|---|---|
| Host pool (default tier) | **S13 run on kind** (below) |
| Sandbox (one pod per agent, P4 T2) | **pending**: T2 isn't built yet (no `sandbox-tier` branch). The harness's Sandbox column stays empty until it is |
| Real CLIs (ten per tier) | owner-run on k3s (the lab never runs real agents) |

## The harness

`scripts/kind/s13.py`, on `k2-kind-p4` (`scripts/kind/p4-up.sh --pool=1:2
--max-awake=50`: rc1 c59acf7e0, the lab's fake agent, enroll, `K2_PTYD=1`,
home sync, no shared `k2-work`, hosts on different nodes):

1. register 500 agents (workspaces on the controller, `git init`,
   `add-from-path`, `agentEnabled`);
2. with 500 registered and none awake: processes per sleeping agent;
3. wake 100, one message each (`wake=true`): wake-to-CLI and
   wake-to-answer per agent, from the message (the controller pod's clock)
   to the fake agent's `cli.exec` and first `cli.turn.end` (the hosts'
   clocks; one kernel, the Docker VM's); the pool's scale-ups;
4. at 100 awake: pods, volumes, each pod's containerd-shim and pause RSS
   (on the kind node, by `crictl` sandbox id), kubelet CPU per node over 60
   s, the controller's CPU over 60 s and RSS, each host's processes;
5. resumed: 20 closed (asleep, home saved), then woken again (`--resume`);
6. after a pod delete: the host holding the most awake agents is deleted;
   its drain releases them and they resume on it when it is back.

It fails (exit 1, "INCOMPLETE") when an agent is missing at any step. The
first run registered nothing (`add-from-path` answers `needsGitInit` for a
directory that isn't a git repo, and the harness then went on with zero
agents); it now inits each workspace and stops if registration falls short.

## Results (2026-10-26, evidence `lab-runs/kind-p4/s13-20261026T141511Z/`)

| Measure | Host pool, this run | Sandbox tier | PRD lab numbers |
|---|---|---|---|
| Pods for 100 awake of 500 registered | 2 host pods (the pool went 1 → 2 at the 51st wake), plus the controller | pending T2 (about 100) | – |
| containerd-shim RSS per pod | 15.1 MB median (13.8–16.7 MB, 3 pods) | pending T2 | about 12 MiB per idle pod, all in |
| pause RSS per pod | 0.47 MB | pending T2 | (in the above) |
| Per extra awake agent in a host pod | one process: 1.9 MB median (the fake CLI) plus about 0.9 MB of the host daemon (83 → 126 MB with 50 awake) | – | about 0.3 MiB per extra process; 330–490 MB per live claude CLI |
| Processes per host pod at 50 awake | 54: 50 agent CLIs + tini, supervise, daemon, k2-ptyd | – | – |
| Kubelet CPU, 60 s, at 100 awake | 2.8 % of a core (k2lab-worker), 1.9 % (k2lab-worker2) | pending T2 | 13–23 % for 100 idle pods vs 2–4 % |
| Volumes | 6 PVCs (each host `data` + `work`, the controller's two) | pending T2 | – |
| Wake to CLI, fresh: p50 / p95 / max | 51 / 157 / 8864 ms (the max is the agent that waited for the pool's new host) | pending T2 | a Sandbox ready 6 s fresh |
| Wake to answer, fresh: p50 / p95 | 426 / 530 ms | pending T2 | – |
| Wake to CLI, resumed: p50 / p95 | 57 / 64 ms | pending T2 | – |
| Back after a pod delete: p50 / p95 | 5.3 / 5.4 s (30 agents; the drain released them, the pod came back on its local-path volume, they resumed there) | pending T2 | a Sandbox 34 s after a pod delete |
| Controller at 100 awake | 1.3 % of a core, 98 MB RSS | pending T2 | – |
| Processes per sleeping agent (NFR2) | **zero**: 500 registered, none awake, 3 processes per host pod, the same as before any agent existed | pending T2 | zero |

Registration of 500 took 34 s (sequential, in the controller pod).

**Read with care:**
- The fake agent is a few MB. The per-agent cost of a real Claude CLI
  (330–490 MB) dominates everything else here, and only the owner can
  measure it (ten real CLIs per tier, k3s).
- kind's nodes are containers in one Docker VM: the shim and pause RSS are
  real containerd processes, but kubelet CPU on kind is not a k3s node's.
- "After a pod delete" is the drain path that works today (the agents come
  back on the same host). Moving them to another host needs
  `k2-dist/drain-move`.

## Left for T5

- The Sandbox column, after T2 (and gVisor on `k2lab-sbx`, plan D5).
- The owner's real-CLI rows.
- The ADR's questions 2–4 depend on both.
