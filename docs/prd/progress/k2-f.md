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

## K2-f.3: no false positives

Branch `k2-dist/k2-f3`, from `k2-dist/wave3` 6ffcbb9da.

The review of c504e6819 found items 2, 3, 7 and 8 correct and I2 holding,
but new false positives. The coordinator's principle: **a message fails
only with positive evidence that it was in the cut turn; when unsure, it
stays delivered.**

### The model

K2-f.2 estimated each message's turn on the controller (count + 1 at
typing, a note at every send). Those estimates were the false positives.
K2-f.3 drops them: only the host, which typed the message, ties it to a
turn.

- **The host registers what it types** (`hosts::drain::note_typed`): the
  message id and the digest of its text.
  - **Outbox delivery:** the write-ahead needle.
  - **Wake:** the launch prompt, when K2-c's `deliveryId` names the
    message.
  - **Inject from the controller:** the typed text, when the inject names
    the message (`Inject.msg_id`).
- **The hook carries a digest of the prompt.** `HookEnvelope.prompt_digest`
  (k2-core) is the SHA-256 of the normalized `UserPromptSubmit` prompt. The
  text is still dropped, as before.
- **A prompt whose digest equals a registered message takes it into its
  turn** (`TurnCounts.msgs`), and that turn's end releases it.
  - A message folded into a running turn (no prompt of its own) matches
    nothing.
  - So do two messages submitted as one prompt.
- **The controller takes `msgs` as the host reports them**, in every hook,
  `resume`, `ended` and `session_gone`. A cut fails exactly the running
  turn's `msgs`.
  - Closed, or the CLI crashed: `interrupted`.
  - A lost host (no counts at the end): its last known `msgs`, as
    `outcome unknown`. The evidence: the message was submitted and no
    `Stop` came.
- **Direct deliveries name their message to the host** (`DirectDelivery`):
  - the gone rule's wake;
  - a host's message the controller delivers itself;
  - the controller's own `k2 msg`, whose id now exists before its cascade
    runs.

| # | Item | As built | Test |
|---|---|---|---|
| 1 | Unhooked agents lost every delivered message (`hooked` forced true on load; `ended` needed counts) | No message is ever tied to a turn without its host matching a prompt, so an agent that never hooks has none to fail. `EndedPlacement.turn` is optional: an agent with no hooks is reported with none, and nothing is cut (not "outcome unknown"). `hooked` is gone | `an_agent_without_hooks_never_has_its_messages_failed` (a close, a controller restart, the CLI ending while unlinked: all stay delivered) |
| 2 | A message folded into a running turn was noted for the next prompt | Matched by prompt text on the host, so a folded message is in no turn | `a_message_folded_into_a_running_turn_is_never_failed` (typed mid-turn, folded, Stop, then close: not failed; and a crash in that turn: not failed); `hosts::drain::tests::a_message_is_in_the_turn_whose_prompt_is_its_text` |
| 3 | Two messages in a row got the same index | Each is matched to its own prompt | `two_messages_in_a_row_are_each_in_their_own_prompts_turn` |
| 4 | A duplicate ack dropped the only note | Nothing is noted at send, so an ack (duplicate or not) takes nothing away. The coordinator's suggested fix (the ack naming its session) isn't needed | the S10 test now sends a late duplicate ack mid-turn; the crash still fails the message |
| 5 | A `session_gone` lost with its link left the agent unreported | Each ended agent is kept (`Placed.ended`) and sent in every `hello.ended` until a welcomed hello carried it | `link::ended_tests::an_ended_agent_is_in_each_hello_until_one_is_welcomed` |
| 6 | K2-f.1 rows' pending/current lists weren't migrated | `host_turns` gains `msgs_json`. A K2-f.1 row's `current` and a K2-f.2 row's notes for its running prompt become the turn's `msgs` on load; pending messages and other prompts' notes are dropped (no evidence). A row is rewritten in the new form | `older_turn_rows_are_migrated` |

**Lab S10 on K2-f.2 (wave3 6ffcbb9da):** 9 of 10 rounds green, I1–I4
passing, no false positives (5 `interrupted`, 3 `outcome unknown`, all
real). The red was a cut before the delivery's ack:
1. Seed 7 round 3: attempt 2 was typed on host-2, its prompt started the
   turn, and host-2 was SIGKILLed before acking.
2. On relink, adopt said gone and the turn was cut, but the row was still
   `sent`. `mark_interrupted` only changed `delivered` rows.
3. Seven ms later host-2's relink report re-sent the ack, and the row read
   delivered and unanswered.

**The fix:** a cut fails a message its host tied to the cut turn whether
its row is `delivered` or still `sent` (`outbox::mark_interrupted`). The
late ack then finds the row finished and changes nothing.

This is the coordinator's decision (the cut's result stands when the ack
comes late) taken one step further. The row ends at the cut, not at the
ack, so a host that never comes back can't leave it `sent` to be resent to
the agent's next placement: a second delivery of a message the cut turn
already had.

It applies only to messages in the host's `msgs` for that turn (evidence
they were typed and submitted). A `sent` message no prompt carried isn't
touched, and its late ack ends it delivered.

Tests:
- `a_cut_before_the_ack_keeps_its_result_when_the_ack_comes_late` (that
  exact sequence, with the unmatched control);
- k2-core `an_interrupted_message_ends_failed_and_a_late_ack_changes_nothing`.

With the old rule, the first fails. The same pattern showed in this
branch's own turn-snapshots run `20261027T083234Z`: the cut on the move
had the host-matched turn-three message, still `sent`, and failed nothing.

**The other tests, rewritten for this model** (the fake hosts report
which messages their prompts matched):
- a CLI exiting mid-turn;
- a host back without the agent;
- a move off a lost host;
- the outage in hello;
- `session_gone` carrying the turn;
- a host-daemon restart;
- a controller restart;
- the direct paths (each names its message on its thread);
- absolute counts;
- an adopted agent;
- nested CLIs;
- S10 (attempt 1 lost unacked, attempt 2 on host-2);
- the close route and drain;
- K2-d's close test.

**Removed:**
- `DeliverAck.prompt`: the count-based index.
- The controller's note API: `note_delivered`, `set_note_prompt`,
  `unnote_*`, `TurnMark`.
- The two "late note" tests, which modeled the estimates.

**Known limits, all on the safe side** (a message stays delivered, never a
false failure):
- A prompt the CLI rewrites before `UserPromptSubmit` (a hook that edits
  it) won't match its message.
- Neither will a message typed by a path that doesn't name it: a `k2 msg`
  through `deliver_live` or `with_via`, or an older host.

### Results (K2-f.3)

| Check | Result |
|---|---|
| `cargo test -p k2-daemon --lib -- hosts:: agent_host:: delivery_drain:: active_reaper:: workspace_msg:: hook_ingest::` | **288 passed**, 0 failed |
| `cargo test -p k2-core --lib -- agent_hooks:: delivery::` | **77 passed** (the prompt digest; `mark_interrupted` on `sent`) |
| Mutations | host assigns every typed message on any prompt (no digest match) → the drain matching test fails; a welcome forgets every ended record → the #5 test fails; no migration → the #6 test fails; a cut only fails `delivered` rows (K2-f.2) → the seed-7 test fails. One mutation went unnoticed: the controller keeping a turn's `msgs` after it ends. The host already empties them at the turn's end, so the controller's own clearing is a second line no test reaches |
| `hosts:: agent_host:: delivery_drain:: active_reaper:: hook_ingest::` (the built test binary) 30 times in a row | **30/30** (204 tests each) |
| `turn-snapshots.sh` `20261027T100447Z` | **7/7 PASS**, with real processes and the fake agent's hooks: host-1 matched turn three's message to its prompt. When host-1 was SIGKILLed and the agent moved, the cut failed it `outcome unknown: host lost mid-turn` while it was still `sent` (its ack died with host-1), and it was not resent (host-2's memory has turns one, two and four). In the run before the seed-7 fix (`20261027T083234Z`) the same cut failed nothing |
| `drain-move.sh` `20261027T100623Z`, `three-process-msg.sh` `20261027T100750Z` | **4/4, 4/4 PASS** |
| `cargo test -p k2-daemon --no-fail-fast -- --test-threads=3` (throwaway HOME) | **5405 passed, 0 failed**, 6 ignored, 162 binaries |

## K2-f.4: one message per prompt; mixed versions

Branch `k2-dist/k2-f4`, from `k2-dist/wave3` 551942095. The review of
0ef283d36 found the design sound and I2 holding, with one false positive
and one mixed-version break.

| # | Item | As built | Test |
|---|---|---|---|
| 1 | A prompt took every typed entry with its digest, and an unmatched (folded) entry stayed for good: a folded, answered M1 and a later identical M2 as its own prompt were both tied, and a cut failed M1 | Each typed entry records the pane's prompt count when it was typed. At each `UserPromptSubmit`, entries two prompts old are dropped (folded, or never submitted). A prompt then ties only the one entry with its digest. When two or more match (a folded M1 and an M2 with the same text, both recent), it can't be told which was submitted, so none is tied and they are dropped | `hosts::drain::tests::a_folded_message_is_never_tied_to_a_later_identical_prompt`: M2 as the very next prompt → neither tied, so M1 can't fail; M2 prompts later → M1 dropped, M2 tied; A and B typed back to back → each its own prompt |
| 2 | A new host omitted `EndedPlacement.turn` for an agent without hooks, and a K2-f/f.1/f.2 controller requires it: the hello failed to decode and the host never linked | `turn` is always sent (`TurnCounts::default()`, not running). "No hooks" is a separate `hooked` flag, which a K2-f.2 host doesn't send and which defaults to true (it only reported agents it had counts for) | `proto::tests::a_new_hosts_hello_decodes_on_a_k2_f2_controller` (the new hello against copies of the K2-f.2 structs; a K2-f.2 ended entry reads as hooked) |
| 3 | The migration kept K2-f.1/K2-f.2 lists, which were the controller's estimates | A row's counts and running state are kept; its old lists are not read | `older_turn_rows_keep_their_counts_and_drop_their_lists` (both adopted mid-turn; a crash fails none of the old lists' messages) |
| – | An inject naming a message but with no `Write` step registered the digest of "" | Empty text is never registered | in the #1 test |

**One change from the rule as specified.** "Drop entries registered
before the previous prompt" would also drop the second of two messages
typed back to back:
1. B is typed before A's prompt.
2. A is submitted.
3. B is submitted next as its own prompt, but by then B was typed before
   the previous prompt (A's), so it would be dropped.

That would lose K2-f.3's #3. So an entry lasts until two prompts have
passed, and two matches tie none. The cost: a third message typed before
the first of three prompts isn't tied, which is on the safe side.

**A hard loss and the other messages (coordinator).** Since K2-f.3, a
message whose turn started on a host that is then lost hard ends
`outcome unknown` at the move and is not delivered again. That matches the
PRD ("node fails hard: the turn in progress is lost").

Every other message still reaches the agent's new placement exactly once:
one sent but never acked nor tied (it died in flight), and one queued
during the outage. One acked but never tied (folded) stays delivered and
isn't sent again.

The test `after_a_hard_loss_only_untyped_messages_reach_the_new_placement_once`
covers this. It passed on the code as it was; it pins the behavior.

**Written down for later**
- **#4 (unconfirmed):** a cut might fail a row that another host has
  already re-delivered. The fix would fail a delivered row only if the cut
  host delivered it (the delivering host id on the row).
- **#5 (latent):** after a host restart, its absolute counts start from
  zero again and are ignored until they pass the old totals. This matters
  once ptyd S3 adopts sessions; the fix is to reset the turn when a host
  restart is detected.

### Results (K2-f.4)

| Check | Result |
|---|---|
| `cargo test -p k2-daemon --lib -- hosts:: agent_host:: delivery_drain:: active_reaper:: workspace_msg:: hook_ingest::` | **354 passed** (with the hard-loss test) |
| Mutations | no window → the #1 test fails; a prompt takes every match (K2-f.3) → the #1 test fails; `turn` optional again → the decode test fails |
| `hosts:: agent_host:: delivery_drain:: active_reaper:: hook_ingest::` (the built test binary) 30 times in a row | **30/30** (269 tests each; before the hard-loss test was added) |
| `turn-snapshots.sh` `20261027T115743Z`, `drain-move.sh` `20261027T115919Z`, `three-process-msg.sh` `20261027T120046Z` | **7/7, 4/4, 4/4 PASS** (procs, done before the Mac's disk filled at 12:03Z) |
| `cargo test -p k2-daemon --no-fail-fast -- --test-threads=3` (throwaway HOME) | **5471 passed, 0 failed**, 6 ignored, 162 binaries. A first run overlapped the full disk (12:03–12:20Z) and was stopped and redone |
