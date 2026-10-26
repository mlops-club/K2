# drain-move: a drained agent doesn't wait for its host forever; K2-d: a turn is never cut silently

Branch `k2-dist/drain-move`, from `k2-dist/wave3` c59acf7e0 (rc1).

## drain-move

The bug, found by kind-app's P4 drain demo on rc1:
- A SIGTERM drain (a pool's scale-down, node maintenance) records the
  agent's resume row under the drained host's name.
- `resume_due` resumes that row only when the same host links again, and the
  outbox holds the agent's messages as `awaiting_resume`.
- `move_lost` (T2) moves only the placements a lost host still holds.

So a host drained for good left its portable agents, and their messages,
waiting forever.

The coordinator decided to do both fixes:

1. **A need places the agent now.** A message the outbox holds for a drained
   agent is a need (`RemoteHost::resume_anywhere`):
   - a portable agent's resume row becomes "any host";
   - the resume loop places it at once on a usable host;
   - that host restores the newest home snapshot (T1) and resumes the
     session (`--resume`);
   - the message is then delivered to the new placement.

   A wake, or the app, already places a drained agent the usual way: that
   ends its resume.
2. **Backstop.** In the controller's resume loop, `move_lost` also promotes
   a portable agent's drain row once its host has been unlinked for
   `K2_HOST_MOVE_AFTER` (300 s), the same rule as for a lost host's
   placements. A host the controller never saw linked counts from the
   first tick.

**Pinned agents still wait for their host**, with their messages.

**Lock order.** The outbox's pass now collects its needs and acts on them
after releasing its DB lock. In production the outbox and the placement
policy share one DB, and `move_lost` takes the placement table's lock
before the DB's.

## K2-d

The bug, from lab run 12-17-31Z on lab-g2 ce7f30da8:
- The outbox delivered a message (turn three) to a moved agent that had no
  app attached.
- Its workspace had never been activated, so it was outside the Active set,
  and the idle reaper armed on its next tick and closed it mid-turn 30 s
  later.
- The message stayed `delivered` and was never answered.

The fix:
- **No reap mid-turn.** The reaper never arms or fires on an agent whose
  turn is running. An armed grace starts over once the turn ends, and the
  fire step checks again.
  - On a controller, the turn comes from the hooks hosts forward
    (`UserPromptSubmit` until `Stop`, `StopFailure` or `SessionEnd`):
    `RemoteHost::in_turn`.
  - Elsewhere, it comes from this process's hook stream or its activity row
    (`agent_host::in_turn`).
- **A delivery is activity.** The outbox sending a message, and its
  `Delivered` ack, bump the workspace's need clock
  (`active_reaper::note_agent_activity`), as an app attach or a local
  `deliver_live` does. So does a resume by the drain loop (a wake).
- **A cut turn is not answered.**
  - **What fails:** if a close still cuts a turn (a forced dismiss, a
    drain's deadline), the messages delivered since that agent's last turn
    end fail with `interrupted: <agent> was closed mid-turn before it
    answered` (`outbox::mark_interrupted`, delivered → failed).
  - **Why fail rather than resend:** the cut turn may already have acted on
    the message, so the sender decides (I2: final and loud).

## Hooks in other workers' files

| File | Hook |
|---|---|
| `agent_host/remote.rs` | `resume_anywhere` and `promote_resume`; `move_lost` covers drain rows; the turn map (`Inner::turns`), `in_turn`, `note_delivered`; `unplace` fails a cut turn's messages |
| `agent_host/mod.rs` | `in_turn` for every role |
| `delivery_drain.rs` | the pass: needs (after its lock) and activity; `on_ack` records deliveries; `turn_cut`; `set_activity` |
| `active_reaper.rs` | the turn gate (arm, fire), `note_agent_activity`, `track_turns` at start |
| `hosts/drain.rs` | `session_in_turn`; a resume is activity; `note_turn_hook` (tests) |
| `k2-core` `delivery/outbox.rs` | `mark_interrupted` |

## Results

| Check | Result |
|---|---|
| `cargo test -p k2-daemon --lib -- hosts:: agent_host:: delivery_drain:: active_reaper::` | **140 passed**. New tests: `a_message_for_a_drained_portable_agent_resumes_it_on_another_host_at_once` (the message makes host-b resume it at once with `--resume conv-1`, then deliver it, one attempt); `a_drained_pinned_agent_waits_for_its_host_even_with_a_message`; `a_drained_portable_agent_resumes_anywhere_once_its_host_is_gone_for_k2_host_move_after` (not while its host is linked, not before `after`, and only once); `the_controller_follows_turns_and_a_close_mid_turn_fails_the_unanswered_message` (a message-only agent with no app: the delivery is activity; its turn runs from `UserPromptSubmit` to `Stop`; closed after its Stop, the message stays delivered; closed mid-turn, it fails `interrupted`) |
| `cargo test -p k2-daemon --test active_reaper_integration` | **14 passed**. New: `a_message_only_agent_in_its_turn_is_not_reaped_until_the_turn_ends` (a workspace never activated, a real PTY: never armed during its turn; reaped a full grace after it ends) and `a_turn_starting_under_an_armed_grace_holds_the_reap` |
| `cargo test -p k2-core --no-fail-fast`, `cargo test -p k2-daemon --no-fail-fast` (throwaway HOME) | k2-core **3902 passed, 0 failed**; k2-daemon **5341 passed, 0 failed**, 6 ignored, 162 binaries |
| `scripts/lab/topology/drain-move.sh` (controller, host-1, host-2, enrolled, own work roots; `K2_HOST_MOVE_AFTER=600`; the reaper ticking every 1 s with a 1 s grace), runs `20261026T143747Z`, `20261026T143914Z` | **4/4 PASS, twice**. (1) notes wakes on host-1 from a message, with no app. (2) SIGTERM host-1: it saves notes' home (trigger `release`) and exits for good; the controller holds notes for resume. (3) A message for notes: within a second, notes resumes on host-2 (not after 600 s); host-2 restores the store's newest snapshot (34 objects fetched) and runs `--resume <same session>`. (4) host-2 has turn one's memory from host-1; the message's 8 s turn runs to its end under the 1 s reaper (no close); the message is delivered once (one `Delivered` ack, once as a prompt in the transcript). A first send that arrives before the resumed session is live is answered "not yet" and nothing is written |
| `scripts/lab/topology/turn-snapshots.sh` on this branch, `20261026T144058Z` | **7/7 PASS** |

**Two lab script fixes found on the way:**
- `saves_with` in turn-snapshots.sh passed one argument where it needed two,
  so it always printed 0. Its "no release save" check in the T2 runs was
  vacuous; the T2 runs' traces show no release save anyway (only `turn`
  saves, seq 1 and 2).
- A `grep -c … || echo 0` prints `0` twice when nothing matches; those
  counters now use `|| true`.

turn-snapshots.sh also gets 897c06c4a's `pkill` guard. drain-move.sh has
the guard from the start.

## Follow-ups

- **The first send after a resume** reaches the host before the session is
  live, and the host answers "not yet". The second send, 2 s later, lands.
  The resume loop could nudge the outbox once its spawn answers.
- **lab-g2's S8 resume-elsewhere and G3's S10** can now also cover "drained
  for good": drain a host, never restart it, then send a message.
