# away: a laptop that comes and goes (P3 T3, first task: presence and held)

Branch `k2-dist/p3-laptop`, first from `k2-dist/wave3` 44ba6ade6, rebased
onto b60a9fd80 (rc3 candidate: K2-f through K2-f.4).

## What was there

T2 phase A (`placement.md`) already gave laptops:
- the `k2.kind=laptop` label;
- pinned on first placement (D5);
- "away" for a known host whose link is down;
- pinned agents that wait for their away host.

W3 keeps a lost host's placements, and the outbox keeps a message for an
unlinked host queued.

## The gap

**A laptop whose lid closes doesn't drop its link.** A suspended process
(`SIGSTOP`, sleep) leaves its TCP socket open, and the hub pinged it every
15 s without a read deadline (gap 4 in `p3-plan.md`). So the controller:
- showed the laptop linked;
- placed nothing elsewhere, but kept sending the laptop's messages into
  the socket;
- marked each send `sent`, waited for an ack, sent again and backed off.

After 10 sends (about 6 minutes) the message failed `no_ack`. A laptop
asleep for 6 minutes lost its messages, which breaks I2.

The sender also only saw `accepted`, never why its message waits (gap 6).

## The task, as decided per the plan

This is the first concrete laptop task: T3's presence and outbox hold.
- **D6:** away after 90 s.
- **Section 3 of the plan:** away, paused, held.
- **S11:** the acceptance scenario.

SAS enrollment (D7) and home sync on reconnect come next (see
Follow-ups).

| Plan item | As built |
|---|---|
| A read deadline on each link, every message a sign of life | `hosts::presence::Presence`: the time a link's host was last heard (an injected `Instant`). The hub (`hub::serve_attach`) refreshes it on every message: frames, pongs, pings. A link silent for `K2_HOST_AWAY_AFTER` (default 90 s) is closed. A write to a host that stopped reading (a full socket) is bounded by the same deadline, so it can't hold the loop. Pings go out every 15 s, or every third of a shorter deadline |
| `away`: silent over 90 s, or its link dropped | `RemoteHost::disconnect_away(host, conn, since, why)`. `why` is `silent for N s` or `its link closed`; `since` is when the host was last heard. `ctl.host_away {host, why, silent_ms, paused}`. Away counts from `since`, so `K2_HOST_MOVE_AFTER` (a portable agent's move) counts from the host's last sign of life, not from the deadline |
| Paused agents in `/cli/hosts/status` | `away` stays a list of ids (the lab and the app read it). New fields: `away_hosts: [{host_id, since_ms, why}]` and `paused: [{agent, host, epoch}]`, the placements kept on an away host |
| `held: {agent, host, away_since}` on the send | `remote::Held {agent, host, awaySinceMs, why}`, from `RemoteHost::held_on`: the agent is placed on an away host; or waits to resume on its own away host (a drained pinned agent); or is asleep and pinned to an away host |
| | **The controller's own `k2 msg`:** `Sent.held`, then `MsgResponse.held`, plus the hint "held: notes is paused on host laptop, which is away (silent for 92 s) since …; the message is delivered when it is back" |
| | **A host's `k2 msg` (the up hop):** the controller's `deliver.ack delivered` carries the `Held` JSON as its reason, which the host's waiting send reads (`Accepted::Held`). An older host ignores a reason on `delivered`, so the protocol doesn't change |
| | **The inbox compose route:** `held` |
| | **The status route:** `GET /cli/hosts/outbox` shows `held` on every queued or sent row that waits for an away host |
| | **Trace:** `ctl.msg_held` |
| The row stays queued until the host relinks (7-day TTL) | Unchanged: nothing is sent to an unlinked host, and no attempt is spent. The TTL is the outbox's |
| A `--wake` for a sleeping pinned agent on an away laptop is held, not cascaded | `Controller::is_direct` is false for a held recipient, so the message is queued instead of going to the cascade (which can't place it, and would fail it). `after_gone` waits while it is held. Once the laptop links, the direct path wakes the agent there and delivers. A pinned agent that slept before this controller saw its session end counts as ended, so its held messages don't wait for the TTL |

The host side doesn't change. A laptop that wakes finds its socket closed
(the controller closed it), reconnects in 1 s, and says hello with resume
(W3). The controller adopts its agents at the same epoch, and the outbox
delivers.

## Hooks in other workers' files

| File | Hook |
|---|---|
| `hosts/hub.rs` | the read deadline and the bounded writes in `serve_attach`; `PING_EVERY` is `pub(crate)` |
| `agent_host/remote.rs` | `Inner::away`, `Away`, `Held`, `away_after` (`with_away_after`, `K2_HOST_AWAY_AFTER` at boot), `disconnect_away`, `held_on`, `pinned_to`, `status_json`'s `away_hosts` and `paused` |
| `delivery_drain.rs` | `Controller::held`, `held_noted` (in `accept` and the controller's `send`), `is_direct` and `after_gone` hold, `row_json`, `Sent.held`, `Accepted::Held`, `ctl.msg_held` |
| `workspace_msg.rs` | `MsgResponse.held` and its hint; `from_outbox` is `pub(crate)` (a test) |
| `inbox_routes.rs` | `held` in the queued answer |

## Settings

| Setting | Where | Default | Meaning |
|---|---|---|---|
| `K2_HOST_AWAY_AFTER` | controller | 90 | seconds of silence (no frame, no pong) after which a linked host is away and its link closed |

## Results

| Check | Result |
|---|---|
| `cargo test -p k2-daemon --lib -- hosts:: agent_host:: delivery_drain:: active_reaper:: provisioner:: workspace_msg::` | **257 passed**, 0 failed. New: `hosts::presence::tests` (4: away only after the whole deadline of silence, with an injected clock; `K2_HOST_AWAY_AFTER`; pings at least three times per deadline; the reasons); `a_silent_laptop_is_away_its_agent_paused_and_its_message_held_until_it_is_back` (a fake laptop the test stops reading: away (silent), since it was last heard; notes paused and kept; a message accepted with `held`, queued with 0 attempts; nothing on host-1; relinked with resume: adopted, delivered on attempt 1, once); `a_wake_for_a_sleeping_agent_pinned_to_an_away_laptop_is_held_not_cascaded`; `delivery_drain::tests::a_held_ack_reaches_the_waiting_sender` (the up hop's ack, and `k2 msg`'s `held` and hint) |
| The changed suites (`hosts:: agent_host:: delivery_drain:: active_reaper:: provisioner::`), 20 times in a row | **20/20** (173 tests each). `workspace_msg::tests` was left out of the loop: when its binary runs directly (not through `cargo test`), five of its PTY tests fail 3 runs in 10 with "agent shim \"cat\" … not found in K2_TEST_AGENT_SHIM_DIR", a race in the test shim setup that is unrelated to this change (its only change here is the `held` field). Under `cargo test` they pass |
| `scripts/lab/topology/laptop-away.sh` (controller, laptop with `k2.kind=laptop`, host-1; enrolled; `K2_HOST_AWAY_AFTER=3`), runs `20261026T233940Z`, `20261026T234015Z` | **5/5 PASS, twice**. (1) notes, first placed on the laptop, is pinned to it. (2) The laptop, idle 9 s, is never away: its pongs count. (3) `SIGSTOP` the laptop: last heard about 0.6 s before the stop, away at 3 s after that (`silent for 3 s`); notes paused at its epoch. (4) `k2 msg` from host-1, with and without `--wake`: both answers say `held {agent notes, host laptop, awaySinceMs, why}` with the hint; the rows are queued with 0 attempts and show `held`; nothing is placed on host-1. (5) `SIGCONT`: the laptop links again, notes is adopted at the same epoch (one placement in the whole run), both messages are delivered on their first send, once each in the transcript |
| `three-process-placement.sh`, `drain-move.sh`, `three-process-msg.sh` on this branch (runs `20261026T234056Z`, `234111Z`, `234237Z`) | **6/6, 4/4, 4/4 PASS** |
| On wave3 b60a9fd80: `cargo test -p k2-daemon --lib -- hosts:: agent_host:: delivery_drain:: active_reaper:: workspace_msg:: hook_ingest:: provisioner::` | **384 passed** (the laptop tests and every K2-f test together) |
| On b60a9fd80: `laptop-away.sh`, runs `20261027T130413Z`, `20261027T130447Z` | **5/5 PASS, twice** (the same checks as above) |
| On b60a9fd80: `three-process-placement.sh` `20261027T130520Z`, `drain-move.sh` `130535Z`, `three-process-msg.sh` `130700Z`, `turn-snapshots.sh` `130719Z` | **all PASS** |
| On b60a9fd80: `cargo test -p k2-daemon --no-fail-fast -- --test-threads=3` (throwaway HOME) | **5477 passed, 0 failed**, 6 ignored, 162 binaries |

**Rebasing over K2-f.**
- The conflicts were only both sides adding tests at the end of
  `placement_tests.rs` and adding to one import in `delivery_drain.rs`.
- The laptop tests' frames carry K2-f's fields: `ended` in the hello, and
  no `prompt` in `deliver.ack`.
- A held message is K2-f's business only once its host is back and it is
  delivered and tied to a prompt.

## Follow-ups

- **Homes on reconnect.** A laptop with `K2_WORK_ROOT` that works offline
  (the controller unreachable) fails its turn-end saves
  (`host.home_save_failed`), and nothing retries them when it links again.
  The store keeps an old snapshot until the next turn ends. Next: on
  relink, the host saves each agent whose last save failed.
- **SAS enrollment** for interactive (laptop) enrollment (D7).
- **The app** shows `paused` agents and `held` messages (the app's own
  task).
