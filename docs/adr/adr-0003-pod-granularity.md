# ADR-0003: Agents run as processes in host pods by default; one pod per agent is an opt-in tier

**Date:** 2026-10-26 · **Status:** Draft (P4 T5; the owner decides at G4) · **PRD:** K2 Distributed Runtime (section 5, gate G4) · **Evidence:** `docs/prd/progress/granularity.md`

## Context

On Kubernetes an agent can run in one of two ways:

- **Host pool (default tier):** many agents as processes inside a host pod
  (`k2-host`, one claude/codex CLI per awake agent, isolated from each other
  by ADR-0001). The controller scales the pool between a minimum and a
  maximum (P4 T1).
- **Sandbox tier:** one pod per agent (agent-sandbox's `Sandbox`, P4 T2),
  optionally under a stronger runtime class (gVisor, Kata).

The PRD's working recommendation is the first by default and the second as
an opt-in, to be decided at G4 from S13 with real CLIs. The PRD's lab
numbers: about 12 MiB per idle pod against about 0.3 MiB per extra process;
13–23 % of a core of kubelet work for 100 idle pods against 2–4 %; 330–490
MB per live claude CLI; a Sandbox ready in 6 s fresh and 34 s after a pod
delete; gVisor about +47 MB with git about 3× slower; Kata +120–300 MB.

## What S13 on kind measured so far (host pool only)

100 awake of 500 registered, fake agents, `max_awake` 50 (details and
caveats in `granularity.md`):

- 2 host pods for 100 awake agents; one process per awake agent (about 1.9
  MB for the fake, plus about 0.9 MB of host daemon); **no process for a
  sleeping agent**;
- containerd-shim about 15 MB and pause about 0.5 MB per pod, which a
  Sandbox would pay per agent;
- kubelet 2–3 % of a core per node at 100 awake;
- wake to CLI 51 ms p50 and 157 ms p95 (one wake waited 8.9 s for the
  pool to add a host); resumed 57 ms p50; back on the same host 5.3 s
  after a pod delete;
- the controller at 1.3 % of a core and 98 MB.

**Not yet measured:** the Sandbox tier (T2 isn't built), gVisor on a
throwaway kind cluster (plan D5), and real CLIs on both tiers (owner-run on
k3s).

## Decision (proposed)

1. **The host pool is the default.** For the same 100 awake agents it uses
   2 pods instead of about 100, so about 2 × 15 MB of shims instead of about
   1.5 GB. It has no per-agent pod scheduling or volume, and wakes in tens
   of milliseconds against seconds. Agents still move between hosts by home
   sync (P3), with no shared volume needed for an agent's own home.
2. **A Sandbox is worth it** when the agents on one cluster must not share
   a kernel or a pod with other agents: untrusted or third-party agents,
   or a cluster shared between owners. Its cost is a pod, a volume and
   seconds of start per agent. *(To confirm with T2's numbers.)*
3. **Runtime class for Sandboxes**, on a cluster that has gVisor: gVisor
   by default, Kata only where KVM is available and the owner asks for it.
   *(Pending: gVisor's overhead on agent workloads, especially git, from
   D5.)*
4. **maxPods and pod CIDR for Sandbox agents:** each awake Sandbox agent is
   a pod, so a node's `maxPods` (110 by default on k3s) and its pod CIDR (a
   /24 per node by default) cap Sandbox agents per node at about 100, minus
   system pods. Size both before enabling the tier. *(Numbers to confirm
   with T2.)*

## Consequences

- The real-CLI rows (330–490 MB each) dominate memory on either tier.
  The owner's ten-per-tier run on k3s is what makes this record final.
- A pool host carries many agents. Draining it moves all of them, which
  depends on `k2-dist/drain-move` (a drained agent's move to another host)
  and on the controller's workspace routes (the shared `k2-work` volume,
  until fs/git are relayed to the agent's host).
- Sleeping agents cost nothing on hosts in either tier (NFR2). In the
  Sandbox tier, each one keeps its own volume.
