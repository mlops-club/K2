# K2-f: a turn cut by a crash fails its unanswered messages

Branch `k2-dist/k2-f`, from `k2-dist/wave3` ec518d8c1.

## The bug

lab's heavy-chaos S10 on wave3 ec518d8c1, every round:
1. Turn three was redelivered on the new host and acked.
2. Chaos killed it 0.05–9.4 s into the turn.
3. When the host came back, `ctl.adopt` answered `gone`, and the row stayed
   `delivered`, never answered and never reported.

The direct wake path did the same (`woke: true`, the CLI killed 0.4 s in).

K2-d already failed such messages as `interrupted`, but only when a close
(the reaper, a forced dismiss) cut the turn.

## The rule (coordinator's decision)

When the controller learns that a session ended mid-turn, the messages
delivered to it since its last turn ended fail with
`interrupted: <agent> crashed mid-turn before it answered`. They are not
resent: the cut turn may already have acted on them, so the sender decides
(I2, as K2-d).

"Mid-turn" is K2-d's turn, from the hooks the host forwards:
`UserPromptSubmit` until `Stop`, `StopFailure` or `SessionEnd`.

| How the controller learns it | Where |
|---|---|
| The CLI exits mid-turn (SIGKILL, a crash): the host's `session_gone` | `RemoteHost::on_frame`, `SessionGone` |
| The host comes back without it (`ctl.adopt` → `gone`) | `RemoteHost::adopt` |
| Its host is lost past `K2_HOST_MOVE_AFTER` and it moves (the turn there is gone with the host; the copy that resumes elsewhere starts a new one) | `RemoteHost::move_lost` |
| Closed mid-turn (K2-d, unchanged: `… was closed mid-turn …`) | `RemoteHost::unplace` |

A host that is only lost cuts nothing. It may come back still running the
turn (a partition), and then adopt keeps the turn and its messages.

The shared code:
- `take_cut` forgets the agent's turn and returns its unanswered messages
  if the turn was running (and was the same session).
- `report_cut` fails those messages once the placement lock is released.
- `delivery_drain::Cut` (`Closed`, `Crashed`) says which reason to give.

### The direct path

A message the controller delivers itself, not through the outbox's
`deliver`/`ack`, wasn't followed at all, and two of the three kinds had no
outbox row. Now each one is followed like an outbox delivery
(`note_delivered`), so a crash mid-turn fails it too:

| Direct delivery | Row |
|---|---|
| The gone rule's wake (`Controller::deliver_gone`) | its outbox row, `delivered` |
| A host's `k2 msg` the controller handles itself (`deliver_direct`: a sleeping recipient, `--wake`) | new: a `delivered` row under the host's `msg_id` (`record_direct`) |
| The controller's own `k2 msg` that the cascade delivered (`workspace_msg::deliver_live_with_receipt`) | new: a `delivered` row (`direct_delivered`); the answer carries its `msgId` |

So every message's end is in `GET /cli/hosts/outbox`.

## Hooks in other workers' files

| File | Hook |
|---|---|
| `agent_host/remote.rs` | `take_cut`, `report_cut`; called from `on_frame`'s `SessionGone`, `adopt`'s gone, `move_lost`, and `unplace` (K2-d's code, now through them) |
| `delivery_drain.rs` | `Cut`; `turn_cut` takes it; `delivered_to`, `record_direct`, `direct_delivered`; `deliver_gone` and `deliver_direct` note the delivery; `is_handing` (tests) |
| `workspace_msg.rs` | the cascade's delivery on a controller is recorded (`direct_delivered`) |
| `hosts/proto.rs` (K2-f.1) | `TurnCounts`; `ResumePlacement.turn`; `SessionGone.turn`; `Hook.subagent` (all serde-defaulted) |
| `hosts/link.rs` (K2-f.1) | `resume_list` reports turn counts and the agents that ended while unlinked; `session_gone` carries the counts; hooks flag subagents |
| `hosts/drain.rs` (K2-f.1) | `Turns` counts prompts and turn ends and keeps them after the CLI exits; `note_ingest` replaces `track_turns` |
| `hook_ingest.rs` (K2-f.1) | `publish` calls `hosts::drain::note_ingest` first |
| `active_reaper.rs` (K2-f.1) | the `track_turns` call is gone (turns are counted at ingest) |

## Results

| Check | Result |
|---|---|
| `cargo test -p k2-daemon --lib -- hosts:: agent_host:: delivery_drain:: active_reaper:: workspace_msg::` | **231 passed**, 0 failed. New: `a_cli_that_exits_mid_turn_fails_the_message_it_was_answering` (and one answered before its CLI went stays delivered; not resent); `a_host_back_without_an_agent_that_was_mid_turn_fails_its_message` (lost is not gone: nothing cut while lost; back without hal → failed; ivy, resumed mid-turn by the same host, keeps its message and finishes); `an_agent_moved_off_a_lost_host_mid_turn_fails_its_message`; `a_direct_delivery_cut_by_a_crash_fails_too` (the gone rule's wake, `deliver_direct`, `record_direct`). Changed: `a_sleeping_recipient_is_woken_by_the_controller_once_and_without_wake_is_refused` now expects the direct `--wake` delivery's `delivered` row (the refused one still has none) |
| Mutation: crash reports switched off (`report_cut` ignores `Crashed`) | the four new tests fail; K2-d's close test still passes |
| `hosts:: agent_host:: delivery_drain:: active_reaper::` (the built test binary) 20 times in a row | **20/20** (147 tests each) |
| `turn-snapshots.sh` (host-1 and its agent CLI SIGKILLed mid-turn three; the controller moves notes), run `20261027T010043Z` | **7/7 PASS**. The controller log: `notes's turn was cut (Crashed): 1 message(s) …` and `FAILED m_… : interrupted: <notes' id> crashed mid-turn before it answered`, `delivery.failed` in the trace. Before this, that message stayed `delivered` |
| `drain-move.sh` `20261027T005852Z`, `three-process-msg.sh` `20261027T010019Z` | **4/4, 4/4 PASS** |
| `cargo test -p k2-daemon --no-fail-fast -- --test-threads=3` (throwaway HOME) | **5350 passed, 0 failed**, 6 ignored, 162 binaries |

## K2-f.1: the review's fixes

An independent review of b47758098 found six issues. Lab's S10 heavy chaos
on 838b70847 confirmed #5:
- 8 of 10 rounds ended loud and right, with no false positives.
- Both reds were a controller restart between the delivery and the crash.

### #1: a host-daemon restart mid-turn

- **What the review assumed:** ptyd keeps the CLI alive across a host-daemon
  restart, so `Placed` should be rebuilt from the sessions ptyd kept, and a
  test should find the live turn's message not failed.
- **What this tree does instead:** ptyd has no adoption yet, which is ptyd
  slice S3.
  - The restarted daemon ends every session the previous one left in ptyd
    when it first connects to ptyd (`ptyd::client::end_leftover_sessions`:
    "no adoption before S3").
  - So no CLI outlives its host daemon. Rebuilding `Placed` would find no
    live session, and that test can't pass.
- **The part of #1 that is real:** after a restart the host knows nothing of
  the turn, so "crashed" claimed more than the controller knows.
- **Now:**
  - a host that comes back without the agent and without its turn fails
    the message with `outcome unknown: host lost mid-turn (<agent> had not
    answered when its host was lost)`;
  - so does a move off a lost host.

  Test: `a_host_daemon_restart_mid_turn_leaves_the_outcome_unknown`.
- **Once S3 adopts sessions:** the adopted agent comes back in `resume`
  with its turn counts (#2), and its message is not failed.

### #2: a `Stop` lost in a link outage

- **Turns on the host:** `hosts::drain::Turns` counts each pane's prompts
  and turn ends, and whether a turn runs. It is fed synchronously by the
  hook ingest (`note_ingest`, called from `hook_ingest::publish`). The
  counts are kept after the CLI exits until they're reported.
- **What the host sends:**
  - `hello`'s `resume` carries `turn: {prompts, stops, running}` for each
    live agent.
  - A CLI that ended while the link was down is reported once, with
    `child_alive: false` and its counts.
- **What the controller does:** it counts the hooks it was forwarded, then
  reconciles (`Turn::reconcile`):
  - a missed `Stop` means the turn's messages were answered;
  - a missed prompt moves the waiting ones into the turn that submitted
    them.
- **Reasons:**
  - an ended agent whose turn was running: `crashed`;
  - an agent the host doesn't mention: outcome unknown (#1).
- **The protocol:** the new fields are serde-defaulted, so a host and
  controller of different versions still talk.
- **Tests:** `a_stop_missed_in_a_link_outage_comes_back_in_the_hosts_resume`
  covers three agents:
  - kay, adopted, whose `Stop` was missed: a later close keeps its message;
  - lou, which ended after answering: delivered;
  - mo, which died mid-turn: `crashed`.

### #3: late notes

- **What the controller follows now:**
  - **pending:** delivered, not submitted yet;
  - **current:** submitted in the running turn.

  The next `UserPromptSubmit` moves pending into current. Only that turn's
  `Stop` clears current.
- **The outbox path:** the note is made when the message is sent (first
  attempt, prompt bodies only), before it can reach the agent. A rejection
  takes it back.
- **The direct paths** take a `TurnMark` (the pane's prompt count) before
  delivering and note with it afterwards. A prompt submitted since then
  lands in the running turn, or is already answered; never in the next
  turn.
- **No clocks:** counts are compared, not times from two hosts.
- **Tests:**
  - `a_late_note_lands_in_the_turn_that_had_its_prompt_not_the_next`: a
    direct delivery noted after its turn ended, one noted in the running
    turn, and an outbox message acked after its turn's `Stop`. None is
    failed by a crash in the next turn.
  - `a_late_note_for_the_running_turn_is_cut_with_it`.

### #4: hooks and `session_gone` on two channels

`session_gone` carries the pane's turn counts, taken from the host's own
synchronous count. The controller reconciles before it cuts, so a last
`Stop` that arrives later (or is dropped by a lagging receiver) still
counts. Subagent hooks are flagged (`subagent`) and never move the lead's
turn on either side. Test:
`session_gone_carries_the_turn_so_a_late_stop_hook_cant_fail_an_answer`.

### #5: the controller's turns survive its restart

Each agent's turn (pane, counts, pending and current message ids) is in
the controller's placement store (`host_turns`). It is written at every
prompt, turn end, note and reconcile, removed when the session ends, and
restored with the placements at start.

After a restart, the host's `resume` counts reconcile against the restored
turn, so a crash after the restart fails the message. Test:
`a_turn_survives_a_controller_restart_and_a_crash_after_it_fails_the_message`.

### #6: `record_direct`

`enqueue` and `mark_delivered` now run in one transaction, and the function
returns `Result`. `direct_delivered` answers `None` (no `msgId`) when the
row couldn't be written. Test:
`record_direct_is_one_transaction_and_reports_failure`.

### Written down, not done

- **Duplicate acks on relink:** on every relink a host re-acks every
  delivery of the last window (`report_delivered`, the K2-b fix for an ack
  lost with its link). Under chaos that is 14–41 acks per message.
  - **Why not now:** deduping needs to know which reports the controller
    got, which is not cheap.
  - **The cost meanwhile:** the controller treats them as late acks and
    nothing changes.
- **Routes with no row:** `deliver_live` and the `with_via` routes still
  deliver with no outbox row (review #5's second half).

### Results (K2-f.1)

| Check | Result |
|---|---|
| `cargo test -p k2-daemon --lib -- hosts:: agent_host:: delivery_drain:: active_reaper:: workspace_msg:: hook_ingest::` | **242 passed**, 0 failed. The new tests are named per item above, plus `hosts::drain::tests::turns_are_counted_and_kept_after_the_cli_exits_until_reported`. Two expected reasons changed from `crashed` to outcome unknown: hal, whose host came back without it and without its turn, and jo, moved off a lost host |
| Mutation: `Turn::reconcile` a no-op | the outage test (#2) and the `session_gone` test (#4) fail |
| Mutation: turns not restored at start | the controller-restart test (#5) fails |
| `hosts:: agent_host:: delivery_drain:: active_reaper:: hook_ingest::` (the built test binary) 20 times in a row | **20/20** (158 tests each) |
| `turn-snapshots.sh` `20261027T034339Z` | **7/7 PASS**. The cut on the move is now `HostLost`. In this run, turn three's message was still `sent` (its ack died with host-1), so nothing was failed. The outbox redelivered it to host-2 (attempt 2), which answered it: memory has turn three once |
| `drain-move.sh` `20261027T034617Z`, `three-process-msg.sh` `20261027T034747Z` | **4/4, 4/4 PASS** |
| `cargo test -p k2-daemon --no-fail-fast -- --test-threads=3` (throwaway HOME) | **5358 passed, 0 failed**, 6 ignored, 162 binaries |

## K2-f.2: the second review's fixes

The review of 73fe2eb01 found #6 fixed, #5 fixed for placements made after
the upgrade, and an old host with a new controller fine. It needed eight
more fixes. K2-f.2 is on the wave3 integration branch `k2-dist/k2-f1-wave3`
(wave3 9b4d7e0cf + K2-f.1, merge f76dce00d).

**How a turn is followed now**

- **Counts:** the controller takes each agent's prompts, turn ends and
  running state from the host's absolute counts. Every forwarded hook
  carries them, and so do `resume`, `ended` and `session_gone`. Older counts
  never overwrite newer ones; a host without counts is still counted hook by
  hook.
- **Notes:** each delivered message is a note with its prompt index, the
  `UserPromptSubmit` that submits it. A note is answered when a later
  prompt started, or when its own prompt's turn has ended.
- **Cuts:** when the session ends, every unanswered note is cut: one whose
  prompt was never submitted, or whose turn was running.

| # | Item | As built | Test |
|---|---|---|---|
| 1 (high) | The controller's counts drifted from the host's: a lagging hook forwarder dropped hooks, a turn was only created on a hook, an adopted agent had none | `Hook.turn` carries the host's counts after each hook, and the controller sets its own from them. `adopt` creates (or updates) the turn from `resume.turn`, in `adopt_unknown` too | `absolute_counts_survive_hooks_the_forwarder_dropped` (two hooks lost; the host lost: A answered, B unknown; a CLI's `session_gone` with counts: the same split); `an_adopted_agent_gets_its_turn_from_the_resume` |
| 2 | A nested CLI's hooks (`from_descendant`, a `claude -p` in a Bash tool) moved the lead's turn | `drain::is_subagent`: `agent_id` set or `from_descendant`, on the host's count and on the forwarded `Hook.subagent` | `a_nested_clis_hooks_are_not_the_leads` (the envelope rule, and a nested Stop that doesn't end the controller's turn) |
| 3 | Dead agents in `resume` with `child_alive: false` would be adopted by an older controller | A separate optional `Hello.ended: [{agent_id, session_id, epoch, turn}]`; `resume` lists live agents only, as before K2-f.1. An older controller ignores `ended` and finds the agent gone | the outage test uses `ended` (lou answered, mo `crashed`) |
| 4 | With no host counts and no turn running, a submitted message was left delivered | The cut fails every unanswered note, with or without a running turn: unknown outcome on a lost host, `crashed`/`closed` otherwise. An agent with no hooks at all (a provider without them) isn't cut on a crash or close, since nothing could ever mark its messages answered; on a lost host its outcome is unknown | `a_lost_host_leaves_a_submitted_message_unknown_not_delivered` |
| 5 | Pending messages: queued input merged into a turn, or several pending submitted as separate prompts | The host counts the pane's prompts before it types a message and acks with `prompt`: the one that submits it (`DeliverAck.prompt`). The note takes that index; the controller's estimate at send time stands for an older host. Each message is answered by its own prompt's turn | `each_message_is_answered_by_its_own_prompts_turn` (prompts 2 and 3; a crash in 3 fails only its message) |
| 6 | A resend was not noted (a redelivery to another host went unfollowed). Lab's S10 heavy chaos on f76dce00d went 1/10 green for this: turn three's attempt 1 went to host-1, killed before its ack, so attempt 2 on host-2 was the first real delivery | Every send notes the message (once per message and session; a move starts a new session). A resend's own note is dropped on a `duplicate` ack (typed before; its first note, or that turn's end, already counted). A note cut with a lost host fails nothing that was only `sent` | `a_resend_is_followed_and_a_duplicate_one_is_not`; `attempt_one_lost_unacked_attempt_two_delivered_then_a_crash_fails_it` (S10's shape: host-1 killed before the ack, moved, attempt 2 on host-2 acked with no prompt, the CLI killed mid-turn → `interrupted`) |
| 7 | The HTTP close route and a drain's release left the turn (and its row) behind | Both cut the turn like `unplace` (`… was closed mid-turn …`) and forget it | `the_close_route_and_a_drain_release_cut_a_running_turn` |
| 8 | Eviction of the host's pane counts was in HashMap order; a pane handed over before `OwnerReleased` lingered | `Turns` keeps 4096 panes, evicting the least recently heard, exited ones before live ones | `drain::tests::panes_are_evicted_oldest_first_exited_ones_before_live_ones` |

**The store:** `host_turns` gains `notes_json` (added to a table made by
K2-f.1; its pending/current columns are no longer read).

**What stays:**
- A message typed while a turn runs and merged by the CLI into that turn
  without its own `UserPromptSubmit` is still counted for the next prompt
  (the review's "unconfirmed" half of #5).
- The duplicate acks on relink.
- `deliver_live`/`with_via` with no row.

### Results (K2-f.2)

| Check | Result |
|---|---|
| `cargo test -p k2-daemon --lib -- hosts:: agent_host:: delivery_drain:: active_reaper:: workspace_msg:: hook_ingest::` | **286 passed**, 0 failed (with the wave3 merge's fs-relay tests) |
| Mutations | hook counts ignored → #1 fails; the ack's prompt ignored → #5 fails; `from_descendant` ignored → #2 fails; no cut with unknown counts and no turn running → #4 fails; only attempt 1 noted (K2-f.1's rule) → the S10 test fails |
| `hosts:: agent_host:: delivery_drain:: active_reaper:: hook_ingest::` (the built test binary) 30 times in a row | **30/30** (201 tests each). An earlier loop caught a race in the #6 test itself (fay placed again before host-a's link was seen down); it now waits for it |
| `turn-snapshots.sh` `20261027T061221Z`, `drain-move.sh` `20261027T061456Z`, `three-process-msg.sh` `20261027T061622Z` | **7/7, 4/4, 4/4 PASS** |
| `cargo test -p k2-daemon --no-fail-fast -- --test-threads=3` (throwaway HOME) | **5401 passed, 0 failed**, 6 ignored, 162 binaries (before the S10 test was added; that test alone changed afterwards) |
