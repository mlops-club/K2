# g2-fixes: S7 placement affinity, S8 host drain, the gone rule (I2)

Branch `k2-dist/g2-fixes`, from `k2-dist/wave2-preview` @3912ab2ff (W5
merged). These fix the reds in lab G2 (`k2-dist/lab-g2`,
`docs/prd/progress/lab-g2.md`).

| Commit | What |
|---|---|
| 06b686e2b | S7: a re-placed agent goes back to the host that holds its home |
| 673b6767b | S8: a host drains on SIGTERM and the controller resumes its agents |
| 6dca72f32 | S8: the drain follows each turn from the host's own hooks |
| 749065ab2 | S8: the drain lets in-flight hooks land before it looks |
| 218233da1 | I2: a message for an agent whose session ended goes direct or fails |

## S7: placement affinity

Each host has its own home, so an agent's conversation and memory exist only
on the host that last ran it. When it was placed again (the app's
`ensure-pinned-chat` after its CLI died, or a wake), `RemoteHost::pick_host`
went by load. Under chaos it picked the other host, and `--resume` exited 1.

`RemoteHost::pick_host_for(agent, cwd)` now chooses, in order:

1. the host that last ran the agent, while it is linked (and not draining);
2. else the linked host that last ran the agent's cwd (that project's
   transcripts);
3. else by load. When the agent's own host is known but not linked, it
   says so in three places:
   - a log line;
   - the trace event `ctl.place_affinity {affinity: fallback, note}`;
   - `placementNote` in the v2 spawn answer.

The agent's host is remembered in memory and in a new controller table,
`host_affinity`. Unlike `host_placements`, that row outlives the session and
a controller restart. Every new placement emits
`ctl.place_affinity {affinity: home | load | fallback}`.

## S8: the host drain

```mermaid
sequenceDiagram
    autonumber
    participant L as lab<br/>(kill -TERM host)
    participant H as host<br/>(K2_ROLE=host)
    participant C as controller<br/>(RemoteHost + outbox)
    participant A as agent CLI

    L->>H: SIGTERM
    Note over H,C: Phase 1: stop taking work
    H->>C: refused {code: draining} (no new placements here)
    H->>H: spawns answered 503, deliveries left unacked
    H->>H: settle 2 s (hooks already on their way in land)
    Note over H,A: Phase 2: wait for each turn
    A-->>H: Stop hook (turn over); bounded by K2_HOST_DRAIN_SECS (600)
    H->>C: released {req: 0, agent} (before the stop, so it's recorded first)
    C->>C: drop the placement; host_resume += agent
    H->>A: stop; then the daemon's shutdown, exit
    Note over L,C: Phase 3: meanwhile, and after
    L->>C: k2 msg agent (host down)
    C->>C: awaiting resume → the outbox keeps it (not the direct path)
    L->>H: start the host again
    H->>C: hello
    C->>H: resume loop: spawn it again (--resume, same conversation)
    C->>H: deliver the message that waited
    H-->>C: deliver.ack delivered (once: I2)
```

- **Host** (`hosts/drain.rs`). `on_sigterm`, under `K2_ROLE=host` only, runs
  the drain before the daemon's usual shutdown. A turn runs from the agent's
  own `UserPromptSubmit` to its `Stop`, `StopFailure` or `SessionEnd`, or
  until the CLI exits. The host follows this from its hook stream from the
  moment it links (`track_turns`); a subagent's hooks don't count, and a
  `Working` activity row also counts as a turn. An agent still mid-turn at
  the deadline is released anyway and logged as cut.
- **Controller** (`agent_host/remote.rs`, `hosts/drain.rs`):
  - A draining host gets no new placements.
  - `released {req: 0}` from a host drops that placement and records the
    agent in `host_resume`, which survives restarts.
  - The resume loop (one tick a second) spawns the agent again once its
    host is linked. A workspace's chat goes through `ensure_pinned_chat`;
    any other agent is re-spawned with `--session-id` swapped for `--resume`.
  - If a need comes first (the app, a wake), the agent is placed the usual
    way, which also ends its resume.
  - `/cli/hosts/status` adds `draining` to each host and lists `resuming`.
- **Outbox** (`delivery_drain.rs`):
  - A recipient awaiting resume (directly or through its address aliases)
    isn't sent down the direct path. Its messages stay queued and are
    delivered once it is placed.
  - A draining host leaves deliveries unacked.

### Hook points in dist-link's files

| File | Edit |
|---|---|
| `hosts/link.rs` `handle()` | at the top: `if let Some(reply) = super::drain::refuse(&f) { let _ = tx.send(reply); return Ok(()); }` |
| `hosts/hub.rs` | none |
| `hosts/gate.rs` | `super::drain::ensure_controller_resumes();` beside `ensure_controller_drain()` on the attach route |
| `hosts/mod.rs` | `pub mod drain;`, `#[cfg(test)] mod placement_tests;` |
| `daemon_main.rs` (SIGTERM arm) | `crate::hosts::drain::on_sigterm().await;` (a no-op unless `K2_ROLE=host`) |
| `agent_host/remote.rs` | the S7 affinity and the S8 controller state (`draining`, `resume`, `released_by_drain`, `awaiting_resume`, `resume_due`, the `host_affinity` and `host_resume` tables). This is the placement code itself; nothing else in it changed |

Both new tables are `CREATE TABLE IF NOT EXISTS` in `PlacementStore`, beside
W3's, and only a controller creates them. Settings: `K2_HOST_DRAIN_SECS`
(host, default 600) and `K2_DELIVERY_GONE_GRACE_SECS` (controller, default
120, below).

## Results

| Check | Result |
|---|---|
| `cargo test -p k2-daemon --lib -- hosts:: agent_host:: delivery_drain::` | 75 passed: 3 S7 tests (back to the home host when another is less loaded, the fallback note, remembered across a restart), the drain on a fake host (released after each turn, one cut at the deadline, `refused` first), the turn tracker, the controller drain end to end on loopback (draining host skipped, release recorded, the message kept queued, resume with `--resume` on relink, the message delivered once) |
| lab G2 fleet, no chaos (`lab-runs/G2/2026-10-26T02-12-53Z`) | **S1–S9 PASS, I1–I4 pass**. S8 drain: host exited on its own after its turn's Stop, resumed with the same session, the message sent while it was down delivered exactly once |
| lab G2 fleet, chaos seed 4117 (`02-15-52Z`, 4 rounds) | S7 PASS in every round; S8 drain passed 3 of 4 rounds. Reds, each matched to `chaos.log`: chaos SIGKILLed the resumed S8 CLI 2.7 s after its drain (02:21:18); S3, chaos killed bob's host 0.4 s after delivery (02:17:53); S9 and W1 wake, chaos killed the agent's CLI (02:23:24, 02:16–02:17). I2: 3 messages to agents whose CLI chaos killed and nobody placed again, waiting in the outbox (below) |
| lab G2 fleet, chaos seed 2026 (`02-27-29Z`, 6 rounds) | S7 PASS; S8 passed 5 of 6 rounds. The red: chaos SIGKILLed host-1 0.4 s into S8's first turn (02:30:30), before the drain step. **I1–I4 pass** |
| `K2_ROLE=all` regression: `cargo test -p k2-daemon --no-fail-fast -- --test-threads=3`, throwaway `HOME` (749065ab2) | 162 test binaries: **5246 passed, 0 failed**, 6 ignored |
| `K2_ROLE=all` regression, same command, throwaway `HOME` (218233da1, gone rule) | 161 test binaries: **5248 passed, 0 failed**, 6 ignored |
| `cargo test -p k2-daemon --lib -- hosts:: agent_host:: delivery_drain::` (218233da1) | 77 passed, 3 runs out of 3 |
| earlier runs of the S7 fix alone (`01-17-05Z` seed 4117, `01-31-45Z` seed 2026) | S7 14/14 in both; no placement fell back |

## The gone rule (I2)

The outbox holds a message until its recipient is placed, or until the
7-day TTL fails it. That is W2's design, and S6 relies on it, because a host
that is away comes back. Under chaos an agent's CLI could die with nothing
needing that agent again, and then its messages stayed `queued` or `sent`
past the end of the run. The coordinator's rule:

- It applies only when the recipient's **session** ended and nothing places
  it again within `K2_DELIVERY_GONE_GRACE_SECS` (controller, default 120 s).
  "Ended" means one of:
  - the CLI exited or was killed (`session.gone`);
  - the session was closed or released;
  - the host relinked without the agent (adopt `Gone`);
  - the placement was dropped.
- After the grace, a `msg` row whose address resolves goes down the
  `K2_ROLE=all` direct path, which wakes it under today's wake rules or
  answers e.g. `dormant_no_wake`. It is traced as `ctl.msg_gone_direct`, and
  the row ends `delivered`, or `failed` with that reason.
- Anything else fails `recipient_gone: <agent>'s session ended and nothing
  placed it again within N s`. That failure is final, carries its reason and
  is traced as `delivery.failed`.
- A placement whose **host** is lost is not ended: it holds until the host
  links again or the TTL runs out (S6). The rule doesn't apply to an agent
  that a drain is resuming either (S8).

Code:

- `agent_host/remote.rs` records each end in `ended` and the table
  `host_ended`, so it survives a controller restart. A new placement or a
  drain release clears the end.
- `delivery_drain.rs` checks the end time against the recipient and its
  address aliases (`after_gone`) and runs the direct delivery off the drain
  loop (`deliver_gone`).

Tests (`hosts/placement_tests.rs`, loopback):

- `a_message_for_an_agent_whose_session_ended_goes_direct_or_fails_after_the_grace`:
  held during the grace, then the direct path for a resolvable address, and
  `recipient_gone` for a bare name.
- `a_message_for_an_agent_on_a_lost_host_keeps_waiting_and_lands_when_it_is_back`:
  with the grace set to zero, the row stays `queued` while the host is lost,
  then the host relinks still running the agent (`hello.resume`), and the
  message is delivered once.

## Final matrix (218233da1, lab `k2-dist/lab-g2` merged on a throwaway branch)

Every run used `scripts/lab/prove.sh G2 --topology fleet --parity none
--linux-gate hold`. The report verdict reads `fail` on every run, the
no-chaos one included, only because `--parity none` leaves the parity
suites `not_run`. The rows below are the scenarios and invariants.

| Run (bundle `lab-runs/G2/2026-10-26T…`) | S3 | S4 | S5 | S6 | S7 | S8 | S9 | I1 | I2 | I3 | I4 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| no chaos (`04-06-47Z`) | PASS | PASS | PASS | PASS | PASS | PASS | PASS | pass | pass (4 msgs) | pass | pass |
| seed 4117 (`04-14-15Z`), rounds passed | 3/4 | 2/4 | 4/4 | 4/4 | 4/4 | 4/4 | 3/4 | pass | pass (16) | pass | pass |
| seed 7 (`05-16-09Z`), rounds passed | 1/2 | 2/2 | 2/2 | 2/2 | 2/2 | 1/2 | 1/2 | pass | pass (7) | pass | pass |
| seed 2026 (`05-21-36Z`), rounds passed | 5/6 | 5/6 | 5/6 | 5/6 | 5/6 | 6/6 | 6/6 | pass | pass (22) | pass | pass |

S1, S2 and W1 pass in the no-chaos run. Every red in a chaos round has a
matching `chaos.log` event:

- **seed 4117**
  - W1/S4 at 05:10:06: chaos SIGKILLed the agent's CLI (pid 70798) 57 ms
    earlier, and host-1 hadn't relinked after the controller kill at
    05:09:58.
  - S3, plus W1/S4 at 05:12:08–09: chaos SIGKILLed the controller at
    05:12:02 (it restarted at 05:12:07).
  - S9: chaos SIGKILLed S9's own CLI (pid 9686) at 05:14:03.
- **seed 7**
  - S3 at 05:16:59.854: the controller restarted 1 ms earlier (chaos kill at
    05:16:57).
  - S8: chaos SIGKILLed host-2 at 05:17:22.4, 0.2 s after the drain's resume
    and before delivery. The lab's 120 s wait ended at 05:19:22; the gone
    rule fired 8 s later (`ctl.msg_gone_direct` → `dormant_no_wake`, final).
  - S9: chaos SIGKILLed S9's CLI (pid 55524) at 05:19:29.
- **seed 2026**
  - Round 1, S3–S7 at 05:22:26–27: the controller restarted at 05:22:26.1
    after a chaos kill, and no host had linked yet (`no agent host is
    connected`, then HTTP 400 on `ensure-pinned-chat`).

The gone rule in the lab:

- seed 4117: `s3-bob` went direct and was delivered (`ok: true`); `s9`
  failed `recipient_gone`.
- seed 7: `s8` went direct and failed `dormant_no_wake`; `s9` failed
  `recipient_gone`.

None of these rows was left non-final, which was the I2 red before this
rule. S6, where a host is lost and comes back, passed every round.
