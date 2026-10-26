# P3 T2: turn-end snapshots, and a lost host's agents move (S10)

Branch `k2-dist/p3-turn-snapshots`, from `k2-dist/wave3` a05fefbba (T1 home
sync merged as 21eb64ca9, then dist-link's chaos-fixes2). Plan:
`docs/prd/progress/p3-plan.md` on `k2-dist/p3-plan` (D3, section 2 "Move
(S10)", the S10 sequence diagram). T1 is `docs/prd/progress/home-sync.md`.

## The change

T1 saves an agent's home whenever its host lets it go: a sleep, a pool's
scale-down, a SIGTERM drain. A host that dies without letting go
(`kill -9`, a lost node, a power cut) saved nothing, and its agents stayed
placed on it, lost, until it came back (W3).

T2 adds two things.

- **On the host, a save at every turn's end.** The lead's `Stop` hook on the
  host's own hook stream saves that agent's home (`hosts::turn_save`).
  Saves are incremental: only the objects the store lacks are sent, and a
  stat-keyed hash cache means only the files that changed are read.
- **On the controller, a move.** A portable agent whose host has been lost for
  `K2_HOST_MOVE_AFTER` is unplaced and resumes on the next host that can
  take it (`RemoteHost::move_lost`, from the resume loop the S8 drain
  added). That host restores the newest snapshot (the last turn's end)
  before it resumes the conversation (T1, decision D). A pinned agent waits
  for its host.

What a crash loses is the turn in progress. The message that started that
turn is not lost: it was never acked, so the outbox delivers it again on the
new host.

```mermaid
sequenceDiagram
    autonumber
    participant L as lab<br/>(kill -9)
    participant C as controller
    participant S as home store<br/>(controller)
    participant H1 as host-1
    participant H2 as host-2

    Note over H1,S: each turn: UserPromptSubmit … Stop
    H1->>H1: Stop (lead) → turn_save: coalesced per agent
    H1->>S: have? → only the changed objects; commit {epoch e} (fence: live placement)
    S-->>H1: seq n
    L->>H1: SIGKILL host-1 and its CLI, mid-turn n+1
    C->>C: link drops → host-1 lost; placements kept (W3)
    C->>C: lost ≥ K2_HOST_MOVE_AFTER, agent portable → ctl.move_decision<br/>unplace, resume row (host "*")
    C->>H2: resume loop: spawn {agent, epoch e+1} (provisioner: host-1 is not linked)
    H2->>S: latest → seq n; fetch the objects it lacks
    H2->>H2: restore, then --session-id → --resume (the home holds it)
    C->>H2: deliver the unacked message of turn n+1 (I2)
    H1->>C: restarts: hello (nothing to resume), or, after a partition, hello.resume {agent, epoch e}
    C-->>H1: release (the agent moved) — never adopted again
```

## Decisions (T2)

D3 and section 2 of the plan decide most of this. These are the details
they leave open, decided here using their recommendations. One departs from
D3's number, for the reason given in T2-b.

| # | Decision | Why |
|---|---|---|
| T2-a | **A turn's end is the lead's `Stop` or `StopFailure`** on the host's own hook stream: the same stream the S8 drain follows. A subagent's `Stop` doesn't count. `SessionEnd` doesn't trigger a save either, because the CLI ending is a release, and the release already saves first | The hook stream is already on every host and needs no new frame. `StopFailure` (an API error) still leaves files changed by the turn |
| T2-b | **Coalesced per agent, no time floor by default.** One save runs at a time per agent. Turns that end while it runs make one more save after it, of the newest state (`Debounce`). `K2_HOME_TURN_SECS` (default **0**) sets the least time between two saves of one agent, and `off` turns turn-end saves off. D3 said 60 s | The coordinator's bound is "loses at most the current turn". A 60 s floor breaks it for every turn shorter than a minute, which is most of them: a crash in the next turn would also lose the one before it. T2-c makes a save cheap, so the floor isn't needed. It stays as a setting for very large homes |
| T2-c | **A hash cache keyed on stat** (size, mtime, ctime, inode, device; per host process). A file is read again when its stamp changes, or when it was written within 2 s of being hashed (git's "racy" files, `snapshot::RACY`) | T1 read and hashed every file on every save. In the lab, a turn's save read 8 of 37 files and sent 3 objects. A write always moves `ctime`, and `ctime` can't be set back, so an equal stamp means the same bytes. Bytes the store asks for are still re-read and checked against their hash before they are sent (T1) |
| T2-d | **A draining host makes no turn-end saves** | Its drain saves each agent before it releases it. A turn save at the same moment would only slow the drain |
| T2-e | **The fence also refuses a commit below the newest snapshot's epoch from a live placement** (T1-c refused it only for an unplaced agent) | Defence in depth for I4: no writer ever lands under a newer writer's snapshot, whatever the placement table says |
| T2-f | **The move**: in the controller's resume loop, every second (`move_lost`). A host is lost when its link is down with no drain; `K2_HOST_MOVE_AFTER` defaults to 300 s, the plan's number. Only **portable** agents move; a pinned one waits (D5, D11). A moved agent becomes a resume row with host `*`. The loop places it through the provisioner on any usable host, at a new epoch, and resumes the conversation (`ensure_pinned_chat` for a workspace's chat, else `--resume`). A host that was lost before the controller saw it (a controller restart) counts from the first tick | This is the part of T2 phase B that S10 needs, built on what S8 already has: the resume rows and the loop. Messages for the agent wait in the outbox meanwhile (`awaiting_resume`), as they do for a drained agent |
| T2-g | **A host back with a copy of an agent that moved is told to release it**, not adopted: either the agent waits to resume elsewhere (row `*`), or it last ran on another host | Adopting the copy would run a second CLI and write an old home over the new one. The fence (T2-e) would refuse the write anyway; the release also stops the CLI (I1) |
| T2-h | **What a crash loses**: the turn in progress (its workspace changes and its partial transcript and memory). Also lost, if the host dies within those seconds, is a turn that ended while the previous save was still running. The message that started the lost turn is delivered again on the new host, because the dead host never acked it | In the lab, turn three's work in progress (`turn3-wip.txt`) is gone; its message was delivered to host-2 (attempts 2 and 3) and ran there |
| T2-i | **Events**: `host.home_saved` and `host.home_save_failed` carry `trigger` (`turn` or `release`); `host.home_saved` also carries `hashed` (files read). The controller emits `ctl.move_decision {from, epoch, lost_ms}`. The failure event is **`host.home_save_failed`**, the code's name: the plan's T1-i said `host.home_push_failed`, now fixed on `k2-dist/p3-plan` | One name in code, plan and docs |

## Hooks in other workers' files

| File | Hook |
|---|---|
| `agent_host/remote.rs` | `move_lost` (new, ~75 lines); `resume_due` takes the `*` rows while any host is usable; `adopt` releases a moved agent's copy (one more match arm). This is the one hook that is not small: the move needs the controller's placement table, which only `RemoteHost` holds |
| `delivery_drain.rs` | `host_linked` starts `hosts::turn_save` next to `track_turns` (2 lines) |
| `hosts/mod.rs` | `pub mod turn_save;` |

My own files: `hosts/turn_save.rs` (new), `hosts/home.rs` (`save_as`,
`Trigger`, the cache, the fence), `hosts/drain.rs` (`K2_HOST_MOVE_AFTER`,
`ANY_HOST`, `CTL_MOVE`, the call in the loop), `k2_core::agent_home::snapshot`
(`HashCache`, `Pushed.hashed`), tests in `agent_home/tests.rs`,
`hosts/home_tests.rs`, `hosts/placement_tests.rs`.

## Settings

| Variable | Where | Default | Meaning |
|---|---|---|---|
| `K2_HOME_TURN_SECS` | host | 0 | least seconds between two turn-end saves of one agent; `off` = save only on release |
| `K2_HOST_MOVE_AFTER` | controller | 300 | seconds a host may be lost before its portable agents move |
| `K2_WORK_ROOT` | host | unset | as in T1: without it, home sync, and so turn saves, are off |

## Results

| Check | Result |
|---|---|
| `cargo test -p k2-core --lib -- agent_home::` | **14 passed**, T1's 13 plus `a_hash_cache_reads_only_the_files_that_changed`. A cold cache reads every file; with nothing changed, no file is read and nothing is sent, and the manifest is the same; a write of the same size (only `ctime` and `mtime` move) is read again and sent; a cache that thinks every file is racy reads all of them every time |
| `cargo test -p k2-daemon --lib -- hosts:: agent_host:: delivery_drain::` | **125 passed** (fake hosts on loopback). New: `turn_save` (every turn end saves; turns ending during a save make exactly one more; a window spaces saves but never drops the last turn; agents are independent; only the lead's Stop ends a turn; the env); `a_portable_agent_on_a_lost_host_moves_resumes_elsewhere_and_its_message_follows` (no move before `after`; moved at a new epoch with `--resume`; the message that waited is delivered on the new host; the old host back with its copy gets `release`); `a_pinned_agent_on_a_lost_host_waits_for_it`; `a_host_back_with_an_agent_that_moved_meanwhile_is_told_to_release_it`; the fence refuses a live placement under a newer snapshot; `K2_HOST_MOVE_AFTER` from the env |
| `scripts/lab/topology/turn-snapshots.sh` (controller, host-1 on `work-h1`, host-2 on `work-h2`, enrolled; k2-fake-agent; a registered workspace driven by `/cli/workspace/msg`; `K2_HOST_MOVE_AFTER=3`), runs `20261026T110609Z`, `20261026T110847Z` | **7/7 PASS, twice**. (1) Turn one ends: host-1 saves seq 1 (36 files, all read, 35 objects). (2) Turn two, after a workspace edit: seq 2 reads 8 of 37 files and sends 3 objects. (3) A 60 s third turn starts and writes `turn3-wip.txt`; host-1 and its CLI are SIGKILLed: no drain, no release save. (4) After 3.4 s the controller moves the agent; host-2 restores seq 2 (36 objects fetched) and runs `--resume <same session>` at epoch e+1. (5) host-2's workspace is byte for byte what turn two left (git HEAD, history, status, file hashes), `turn3-wip.txt` is absent, and MEMORY.md and the transcript are turn two's. (6) The next message lands on host-2, which remembers turns one, two, three (redelivered) and four. (7) host-1 restarts: the agent stays on host-2, one live CLI |
| control, `TS_TURN_SECS=off` (`20261026T111144Z`) | **5 FAIL, as expected**: no turn saves, so the moved agent finds no snapshot on host-2 and resumes with neither its workspace nor its memory (only turns three and four). The move itself and I1 still pass |

## Follow-ups

- **The placementNote after a move** still says "its home is on host-1 …
  where its conversation and memory are not". With home sync that is no
  longer true (T1 follow-up; T2 makes it common).
- **P4 pool**: a pod lost to a node failure is also a lost host. The pool's
  `idle_tick` should not scale a lost host's slot back up before
  `K2_HOST_MOVE_AFTER` has moved its agents (dist-link).
- **kind S10** (`kubectl drain` is a drain, not a crash): to test a crash,
  delete the pod with `--grace-period=0 --force` (T4, lab).
- **Long turns**: nothing is saved in the middle of a turn. A turn that runs
  for an hour loses an hour when its host dies.
- **The hash cache** lives only in memory: the first save after a host
  restarts reads every file.
- **lab-g2**: an S10 scenario in `prove.sh G3` (T4) can reuse this script's
  flow: register the workspace, send a message, `kill -9` the host tree, wait
  for `ctl.move_decision`.
