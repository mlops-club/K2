# K2-e: a message never reaches a host before its agent's spawn

Branch `k2-dist/k2-e`, from `k2-dist/resume-nudge` (`k2-dist/wave3`
d34b7292c plus the resume-nudge fix, 88e42d895).

## The bug

lab's Linux gate on wave3 cfcb1e387: `hosts::placement_tests::a_portable_agent_on_a_lost_host_moves_resumes_elsewhere_and_its_message_follows`
failed. host-b received bob's `Deliver` at epoch 2 before the resume
`Spawn`.

- `reserve_for` records the new placement (no session yet) before the
  `Spawn` goes out.
- The outbox's pass took any placement as deliverable, so a tick between
  the two sent the message to a host with no agent bob.
- A real host answers that with "no agent bob here", a final rejection, so
  I2 broke.

## The fix

It was already on `k2-dist/resume-nudge` (88e42d895), made there for "the
first send after a resume":
- The outbox delivers only to a placement whose spawn has answered
  (`session_id` set). A reservation still in flight is "not yet", and no
  attempt is spent (`delivery_drain.rs`, the pass).
- Each spawn that answers nudges the outbox (`delivery_drain::spawned`),
  so the message goes out at once.

Every path that places an agent reserves through `reserve_for`, so every
path gets the guard:
- a move (`move_lost` → the resume loop);
- drain-move's `resume_anywhere` (a message for a drained agent → the
  resume loop);
- a wake;
- the app.

A placement restored with a live session (a controller restart, a host's
hello) already has its session.

## The test, made deterministic

`resume_then_deliver` in `hosts/placement_tests.rs` runs a resume step by
step:
1. Receive the `Spawn` on the new host.
2. Assert the agent is reserved there with no session.
3. Run three outbox passes by hand, and assert the message is still
   `queued` with 0 attempts.
4. Answer the spawn.
5. The next frame must be the `Deliver`, on attempt 1, and the message
   ends `delivered` after one attempt.

No tick can fall between steps unobserved. The helper is used by:
- the lost-host move test;
- drain-move's `a_message_for_a_drained_portable_agent_resumes_it_on_another_host_at_once`
  (the `resume_anywhere` path);
- resume-nudge's `a_message_waits_for_the_spawn_to_answer_and_goes_out_on_its_first_attempt`.

## Results

| Check | Result |
|---|---|
| Guard removed (`if false && …`) | all three fail every time, at step 3: `(Sent, 1)` instead of `(Queued, 0)` |
| `cargo test -p k2-daemon --lib -- hosts:: agent_host:: delivery_drain:: active_reaper::` | **141 passed**, 0 failed |
| `hosts::placement_tests::` (the built test binary) 30 times in a row | **30/30** |
| `cargo test -p k2-daemon --no-fail-fast` | on resume-nudge, with the same production code: 5341 passed, 0 failed (this branch changes only tests) |
