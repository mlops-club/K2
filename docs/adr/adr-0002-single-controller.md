# ADR-0002: One controller, no high availability

**Date:** 2026-10-24 · **Status:** Proposed · **PRD:** K2 Distributed Runtime (gates G2, G5)

## Context

After the split, the controller (today's `k2-daemon` minus agent processes)
holds the database, scheduler, messaging outbox, vault, tunnel and front
door. Agent hosts dial out to it.

Today a daemon restart kills every agent PTY, including during the
zero-downtime handoff (`crates/k2-daemon/src/handoff/mod.rs`). After the
split, agents run on hosts, so a controller restart no longer stops them:
hosts buffer output in the PTY rings, queue hook events, reconnect and
replay (PRD flow "Controller restarts", scenario S9).

Running two or more controllers would need:

- the store moved from SQLite (`~/.k2/k2.db`, one process-wide mutex,
  `crates/k2-core/src/db/mod.rs`) to Postgres, across every module that uses
  `db::shared()`;
- leader election, so one scheduler fires heartbeats and one delivery loop
  drains the outbox;
- a single owner for the tunnel lease, the federation identity and the
  front door's certificates;
- every in-memory map (live sessions, injection locks, awareness budgets)
  rebuilt from shared state.

## Decision

Run exactly one controller in every mode:

- one `k2-daemon` service on a laptop or VM, as today;
- one `controller` service in Docker Compose;
- `StatefulSet/k2-controller` with `replicas: 1` and a volume for SQLite on Kubernetes.

Keep SQLite. Postgres stays a capability for K2 Apps and `/cli/db`, not the
controller's own store.

## Consequences

- **What an outage costs:** while the controller is down, awake agents keep
  working; new spawns, wakes, scheduled heartbeats and messages wait. Messages
  wait in the hosts' queues and the controller's outbox; heartbeats due during
  the outage fire once on return (today's catch-up behaviour). The local app
  can still attach directly to its own host's terminals (PRD FR11).
- **Recovery time** is a pod or service restart: seconds on Kubernetes, plus
  however long the node takes if the controller's node failed. On k3s with
  local-path storage the controller is pinned to its node, so a failed node
  means waiting for it; use a replicated storage class for the controller's
  volume if that matters.
- **Backups:** the controller volume (k2.db and vault.db) is the one thing to
  back up. The PRD should add a documented snapshot, since there is no replica.
- **Revisit when** the G5 soak test shows controller restarts or node loss
  hurting users, or when a deployment needs more than one region. The move
  then is: Postgres store first, leader lease second, active-passive before
  anything active-active.
