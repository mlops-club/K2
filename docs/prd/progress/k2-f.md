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

## Results

| Check | Result |
|---|---|
| `cargo test -p k2-daemon --lib -- hosts:: agent_host:: delivery_drain:: active_reaper:: workspace_msg::` | **231 passed**, 0 failed. New: `a_cli_that_exits_mid_turn_fails_the_message_it_was_answering` (and one answered before its CLI went stays delivered; not resent); `a_host_back_without_an_agent_that_was_mid_turn_fails_its_message` (lost is not gone: nothing cut while lost; back without hal → failed; ivy, resumed mid-turn by the same host, keeps its message and finishes); `an_agent_moved_off_a_lost_host_mid_turn_fails_its_message`; `a_direct_delivery_cut_by_a_crash_fails_too` (the gone rule's wake, `deliver_direct`, `record_direct`). Changed: `a_sleeping_recipient_is_woken_by_the_controller_once_and_without_wake_is_refused` now expects the direct `--wake` delivery's `delivered` row (the refused one still has none) |
| Mutation: crash reports switched off (`report_cut` ignores `Crashed`) | the four new tests fail; K2-d's close test still passes |
| `hosts:: agent_host:: delivery_drain:: active_reaper::` (the built test binary) 20 times in a row | **20/20** (147 tests each) |
| `turn-snapshots.sh` (host-1 and its agent CLI SIGKILLed mid-turn three; the controller moves notes), run `20261027T010043Z` | **7/7 PASS**. The controller log: `notes's turn was cut (Crashed): 1 message(s) …` and `FAILED m_… : interrupted: <notes' id> crashed mid-turn before it answered`, `delivery.failed` in the trace. Before this, that message stayed `delivered` |
| `drain-move.sh` `20261027T005852Z`, `three-process-msg.sh` `20261027T010019Z` | **4/4, 4/4 PASS** |
| `cargo test -p k2-daemon --no-fail-fast -- --test-threads=3` (throwaway HOME) | **5350 passed, 0 failed**, 6 ignored, 162 binaries |

## Known limits

- **A turn that ended while its host was lost.** If the agent's turn ends
  while its host is lost and the host then comes back with the agent
  (adopted), the `Stop` hook sent during the outage is lost. The
  controller still thinks the turn is running until the next turn's
  `Stop`, so a close or crash before then would fail that message as
  interrupted although it was answered.
  - **Next:** the host could send each agent's turn state in its hello's
    resume.
- **Late acks.** A message whose ack (or direct delivery) is recorded only
  after its turn's `Stop` stays in that agent's unanswered list until the
  next `Stop`. K2-d has the same race.
