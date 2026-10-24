# ADR-0001: Agent isolation inside host pods

**Date:** 2026-10-24 · **Status:** Proposed · **PRD:** K2 Distributed Runtime (gates G2, G4)

## Context

On Kubernetes, the default host tier runs many agents as processes inside
one `k2-host` pod, the same way agents share one daemon on a VM today. That
keeps K2's density: in the lab, a pod costs about 12 MiB and a live `claude`
CLI 330–490 MB, but one pod per agent also brings per-agent volumes, slower
wake and duplicated logins (PRD section 5).

So agents in one host pod share a kernel namespace set. The question is how
far apart they are kept from each other, and from `k2-host` itself.

What K2 does today:

- **Default (every install):** agents run as the daemon's own user
  (`SandboxSpec::Passthrough`, `crates/k2-core/src/terminal/sandbox.rs`).
  An agent can read `~/.k2/daemon.token` and shares the daemon's cgroup.
- **Dedicated tier only:** `k2-vmm-worker`, started as root, gives each cell
  new mount and PID namespaces, a cgroup leaf, its own UID from the pool
  60000–60127 (`crates/k2-daemon/src/cell_uid_pool.rs`), `/dev/kvm` and a
  seccomp filter. Egress is a per-UID nft table allowing 443 and 53
  (`cell_egress.rs`).

Giving each agent its own UID inside a pod needs `k2-host` to start as root
with at least `CAP_SETUID` and `CAP_SETGID` (and usually `CAP_CHOWN` to hand
the agent its home). That fails Pod Security `restricted` and some clusters'
admission policies.

## Options

| | A. Process isolation (one UID) | B. Per-agent UIDs (narrow capabilities) | C. One pod per agent (Sandbox tier) |
|---|---|---|---|
| How | All agents run as the pod's UID. `k2-host` keeps its secrets out of agents' reach with file modes and by never exporting its link key. | `k2-host` starts as root with `SETUID`, `SETGID` and `CHOWN` only, then drops each agent to a UID from the pool. | agent-sandbox `Sandbox` per agent, optionally gVisor or Kata. |
| Agent vs agent | Same UID: one agent can read another's home and signal its processes. | Separate UIDs: homes are mode 0700, no cross-signalling. | Separate pods; with gVisor or Kata, separate kernels. |
| Agent vs host | Agent can read anything `k2-host` can, unless held outside the pod's filesystem. | `k2-host` secrets are root-only. | Host is per agent, so nothing else to protect. |
| Pod Security | Passes `restricted`. | Needs `baseline` plus allowed capabilities. | Passes `restricted` (the pod itself). |
| Network rules per agent | Pod-wide only (NetworkPolicy). | Pod-wide; per-UID rules would need nft and `NET_ADMIN`. | Per agent. |
| Memory per agent | CLI only. | CLI only. | CLI plus about 12 MiB (runc), about 47 MB (gVisor) or 120–300 MB (Kata). |
| Matches today | The default install. | The dedicated tier, minus microVMs. | Stronger than anything today. |

## Decision

- **Default: A.** Host pods add no capabilities. This matches what every K2
  install does today, so moving to Kubernetes makes nothing weaker.
- **Opt-in: B**, as a "per-agent users" host profile for owners who want
  agents kept apart without paying per-pod costs. It reuses the existing
  `cell_uid_pool`. Turning it on changes the host pod's security context and
  needs the namespace at `baseline`.
- **Strong isolation: C**, the Sandbox tier, for untrusted agents or shared
  clusters. How far this tier goes is decided at gate G4.

The host's link key needs care in A. Agents share `k2-host`'s UID, so they
can read any file it can. `k2-host` sets itself non-dumpable so agents can't
read its memory or open files through `/proc`, but the key file on the
volume stays readable. What limits the damage is the controller: it accepts
frames from a host only about agents placed on that host, so a stolen key
gives an agent no more than it already has, namely the agents in its own
pod. In B and C, file ownership keeps the key from agents.

## Consequences

- Same trust model as today by default; nothing regresses.
- A compromised agent in profile A can read its neighbours' homes in the same
  pod. Owners who mix trusted and untrusted agents must use B or C, and the
  UI should show which profile a host runs.
- Gate G4 tests all three: scenario S2 (hook attribution) and an isolation
  check that agent X cannot read agent Y's home under B and C.
- Per-agent egress rules (today's per-UID nft) exist only under C.
