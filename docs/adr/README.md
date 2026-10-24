# Architecture decision records

One file per decision: context, the options with their tradeoffs, the
decision and its consequences. Status is Proposed until the owners accept it.

| ADR | Decision | Status |
|---|---|---|
| [0001](adr-0001-agent-isolation-inside-host-pods.md) | Agents share a host pod with process isolation by default; per-agent UIDs and per-agent pods are opt-in | Proposed |
| [0002](adr-0002-single-controller.md) | One controller replica in every mode; no high availability | Proposed |

Both come from the PRD "K2 Distributed Runtime" (2026-10-23). The deployment
files that follow them are in `deploy/`.
