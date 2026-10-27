# ptyd S3: a restarted host daemon adopts its agents

Branch `k2-dist/ptyd-s3-adopt`, from wave3 `b3e439241`.

## The problem

Today a host-daemon restart kills every agent CLI on that host. This
covers an upgrade, a crash, or a `kill -9`.

- **With ptyd off (the default):** the CLI is the daemon's own child.
- **With `K2_PTYD=1`:** the CLI outlives the daemon in ptyd. Then the
  restarted daemon ends it on its first connect to ptyd
  (`ptyd::client::end_leftover_sessions`, "no adoption before S3").
- **What the controller sees:** the host relinks with an empty `resume`.
  The controller decides the agent is `gone` and cuts its turn. The message
  in that turn fails with `outcome unknown: host lost mid-turn`
  (`a_host_daemon_restart_mid_turn_leaves_the_outcome_unknown`, K2-f.3 #1).

S3 keeps the CLI and gives it back to the restarted daemon. The controller
adopts it as live, its turn goes on, and its message is not failed.

## What survives, and where it is kept

ptyd holds the PTY master, the child, its output ring, and a JSON `meta`
per session (up to 64 KiB, P28). That meta is the state the restarted
daemon reads back. Nothing new goes to disk, so the state can't disagree
with which sessions really live.

| State | Today | S3 |
|---|---|---|
| The session (pid, size, label, pin, cwd, program, args) | ptyd `meta`, written at spawn and on label/pin changes | read back by `DaemonPtySession::adopt_ptyd` |
| Screen and scrollback | ptyd's ring | replayed into a new `Term` from the ring's start |
| `Placed`: agent → epoch, session → agent | host daemon memory | also in `meta.k2_host = {agent_id, epoch}`, set when the link's spawn returns |
| `drain::Turns`: prompts, stops, running, the running turn's msgs, typed msgs | host daemon memory | also in `meta.k2_turn`, rewritten on every lead hook and every typed message (`update_meta`, fire-and-forget) |
| The scoped hook passport | `~/.k2/hook-sessions.json` (persisted already, "for the future restart re-adoption") | unchanged. The adopted session's per-cell socket is bound again |
| Delivery seen table | the host's `k2.db` | unchanged. A delivery cut by the restart is already settled from the transcript (`settle_interrupted`) |

The turn meta is written before the hook is forwarded. `note_ingest` is
synchronous in the hook ingest, and `send_nowait` writes to ptyd's socket
before the `Hook` frame is queued. So after a restart, the counts the host
reports are never below what the controller was told.

## The restart, step by step (host role, ptyd on)

1. **Boot.** Before the link starts, the host registers an adopter for
   ptyd's leftovers in place of `end_leftover_sessions`. It then connects
   to ptyd eagerly. The adopter applies two rules:
   - a session that is `Running` and has `meta.k2_host` is adopted;
   - anything else is ended as today, and exited records are acked. This
     covers a session no controller placed (an old daemon's, a non-host
     role's) and one with no meta.
2. **Rebuild the session** (`k2-core`, `DaemonPtySession::adopt_ptyd(owner,
   info)`):
   - a `Term` at the session's ptyd size;
   - `attach(sid, ring_start)`, so K2's IO loop replays the ring into the
     `Term` before live output. Terminal-query answers stay muted during
     the replay (P75), so old queries in the ring aren't answered again;
   - label, label source, pin, cwd, program and durable args from the meta;
   - the pid from `list`;
   - the session's `exited` watch, registered before the attach, so an exit
     during the adoption isn't missed.
3. **Wire it into the daemon** (`k2-daemon`, `ptyd_adopt`), as a spawn
   does after the PTY opens:
   - `v2_session_map::register(agent_id, …)`, which also creates the
     activity row;
   - `spawn_child_exit_observer`, so an exit is `SessionRemoved`, then
     `session_gone`;
   - bind the per-cell hook socket again (`activate_cell_uds`). Hooks also
     fall back to loopback TCP.
4. **Rebuild `Placed` and `Turns`** from the meta: `epochs[agent] = epoch`,
   `sessions[sid] = agent`, and the pane's counts and typed messages. This
   is K2-f.3 #5, choice "rebuild": the counts go on from where they were,
   not from zero.
5. **Link.** `hello.resume` lists the adopted agents as today:
   `child_alive: true`, the epoch, the session id and the turn counts.
   - The controller still holds the placement at that epoch, so it decides
     `Adopted`: no cut, the turn and its notes are kept, and the outbox row
     stays `delivered`.
   - Hooks fired while the daemon was down are lost: the hook script fails
     and exits 0. A `Stop` lost that way leaves the turn running on both
     sides. The next prompt moves the counts on, and the earlier message is
     answered.
6. **K2-f.3 #5, the guard.** A resume entry gains `adopted: true` (optional;
   an older controller ignores it). On an adopted resume, the controller
   takes the host's counts as they are, even below its own, and replaces its
   `Turn` for the pane. It no longer ignores them until they pass the old
   totals. This covers a session whose meta never got its turn (spawned by
   an older daemon, or ptyd lost the write). A resume with no turn leaves
   the controller's turn as it is.

## What stays as it is

- **SIGTERM** (a host pod stopping) is still a drain (S8). The agents'
  turns end, their homes are saved, and they are released and stopped.
  S3 is for a restart without a drain: crash, `kill -9`, or an upgrade
  that restarts only the daemon.
- **Non-host roles** (the app's daemon, a controller) don't adopt; they end
  leftovers as before. The laptop topology is a later slice.
- **ptyd off** (the default) is unchanged: no ptyd, so nothing to adopt.

## Not covered here (for the coordinator)

- **A container restart in the host pod.** ptyd runs in the daemon's
  container, so a container restart kills ptyd and its children with it. To
  survive one, ptyd must run outside the daemon's container. That means a
  sidecar in the same pod with the K2 home and ptyd's socket dir on a shared
  volume, and the agent CLIs in the sidecar's image. Or the daemon is not
  PID 1 and is restarted by a supervisor in the container. Both are
  `deploy/**` changes, which this worker doesn't make. S3 is what either
  needs on the daemon's side.
- **`K2_PTYD=1` in the host image.** It isn't set there today. It is a
  deploy decision, together with the item above.

## Tests

- **`k2-core` `ptyd_backend`, `a_session_outlives_its_owner_and_is_adopted`.**
  A survive session prints a marker. A second owner takes over, as a
  restarted daemon does (the first is evicted). Then `adopt_ptyd`:
  - the same pid;
  - the screen holds the marker (replay);
  - input written after adoption reaches the child;
  - its exit is seen.
- **`k2-daemon` units.**
  - `a_pane_restored_from_its_meta_goes_on_where_it_was`: `Turns` round
    trip through the meta; the next prompt takes the message typed before
    the restart;
  - `only_placed_running_sessions_are_adopted`: running and `k2_host` set,
    versus exited, no meta or malformed;
  - `a_host_daemon_restart_that_adopts_the_cli_keeps_the_turn_and_its_message`:
    the controller side. An `adopted` resume at the same epoch keeps the
    message delivered, sent once, and the turn running; the agent's `Stop`
    then answers it;
  - `an_adopted_resume_resets_the_controllers_turn`: #5. Lower counts are
    taken when `adopted`, and ignored otherwise.
- **Lab `scripts/lab/topology/host-restart-adopt.sh`** (procs: a controller
  and host-1 with `K2_PTYD=1`, `k2-fake-agent` as `claude`):
  1. bob is placed on host-1, and his CLI runs under ptyd;
  2. a message to bob with `[[turn-ms=8000]]`, and his turn starts
     (`cli.turn.start`);
  3. host-1's daemon is killed with `kill -9` mid-turn, then started again
     with the same HOME;
  4. checks:
     - the CLI survives: same pid, never exited;
     - the controller adopted bob (`ctl.adopt decision=adopted`), with no
       `gone`;
     - the turn's message is `delivered` and was never failed;
     - the agent answers: `cli.turn.end` and the reply in the transcript,
       and the controller sees the `Stop` (bob not in turn);
     - a second message after the restart is delivered once and answered;
     - the grid shows the pre-restart output (replay).

## Results

| Check | Result |
|---|---|
| `scripts/lab/topology/host-restart-adopt.sh`, run `20261027T151414Z` | **7/7 PASS**. The host daemon was killed with `kill -9` mid-turn; the CLI kept its pid (77387, a ptyd child) and never exited or was exec'd again. host-1 logged `host.adopt` and the controller decided `adopted` at the same epoch, with no `gone`. The message stayed `delivered` and was sent once. Reply #2 is in the transcript and on the adopted screen, along with the output from before the restart. A third message was delivered once and answered |
| `cargo test -p k2-core --test ptyd_backend` | **9 passed** (with `a_session_outlives_its_owner_and_is_adopted`) |
| `cargo test -p k2-daemon --test ptyd_p76_integration` | **1 passed** |
| `cargo test -p k2-core --lib` | **3724 passed, 0 failed** |
| `cargo test -p k2-daemon --lib` (throwaway HOME) | **4678 passed, 0 failed** (with the four new tests) |

The old behaviour is pinned by
`a_host_daemon_restart_mid_turn_leaves_the_outcome_unknown`: a host that
comes back without the agent still leaves the message's outcome unknown.

## S3.1: the independent review of 311c97615

Branch `k2-dist/ptyd-s3.1`, from wave3 `e1769ca8e`. The review found the
I1 check at hello and the replay sound. It asked for these fixes before
the ptyd sidecar, which relies on adoption being right.

| # | Finding | As built | Test |
|---|---|---|---|
| 1 (high) | Adoption ran on the first successful ptyd connection, whenever it came. A boot without ptyd linked with an empty resume (the controller marked the agent gone), and a later spawn's connection adopted the old CLI over the new placement | Adoption happens only in the boot window. It opens at process start and closes when `install` returns, and the link starts only after that. `install` retries ptyd for `K2_PTYD_ADOPT_WAIT` (default 10 s). A first connection after the window ends every leftover and adopts none | `after_the_boot_window_leftovers_are_ended_not_adopted` |
| 2 | Two adoptable sessions for one agent: the last one listed won | `adopt::pick` groups by agent. The highest epoch is adopted, whatever the order, and the rest are ended | `one_session_per_agent_the_highest_epoch_wins` |
| 3 | A CLI of another controller, or of an agent this controller saw end, came back through `adopt_unknown` | `k2_host` also records `controller` (the attach URL), `host_name` and `host_id`. A mismatch with this host's link config is ended before the hello. A `host_id` other than the welcome's is ended after it, and the controller hears its `session_gone`. On the controller, an `adopted` resume of an agent whose session ended there (`host_ended`: closed, unplaced, or gone) is released, not adopted. Meta from an S3 daemon (without these fields) is taken as this host's | `a_session_placed_by_another_controller_or_for_another_host_is_ended`, `adopted_agents_are_confirmed_by_a_welcome_or_fenced`, `an_adopted_cli_whose_session_ended_here_is_released` |
| 4 | An adopted CLI lived on indefinitely if no hello was ever welcomed | The fence: `K2_HOST_MOVE_AFTER` after boot, adopted agents that no welcomed hello confirmed are ended. They stay placed, so the controller is told (`session_gone`, or the next hello's `ended`) | `adopted_agents_are_confirmed_by_a_welcome_or_fenced` |
| 5 | The controller reset the turn even when the host's meta was behind (an `update_meta` lost, for example over 64 KiB), dropping the running prompt's messages | An adopted resume's counts are taken only when they have at least as many prompts as the controller saw; otherwise the usual newer-only rule applies. A meta over `MAX_META` is logged (`meta_too_large`) and not sent, so ptyd keeps the last one that fit. Typed messages stay bounded (`KEEP_TYPED` 64) | `an_adopted_resume_resets_the_controllers_turn_but_never_to_fewer_prompts`, `an_oversize_meta_is_not_sent_and_a_later_one_is` |
| 6 | Boot race: the adopter was set late (after HTTP was up), and the sweep could end a session this process had just created | The host role's adopter is set at process start (`boot_settings_check`). The ptyd client keeps the sids this process created, and the sweep neither ends nor adopts them | `a_session_this_process_created_survives_a_later_sweep` |
| 7 (low) | An agent CLI has the same uid as ptyd, so it can read ptyd's key and rewrite its meta to name another agent or epoch | The cheap part: a session is adopted only if its hook passport is live in `hook-sessions.json` (minted by this host, not revoked). A forged or ended session is ended | `a_session_without_a_live_passport_is_ended` |
| 8 (low) | An adopted session lost its exit observer's inputs | `k2_exit` keeps `ephemeral_cwd`, `principal_key` and `quota_workspace` when a spawn has them, and the adopted session's observer gets them back | `the_exit_observers_inputs_come_back_from_the_meta` |

**Written down, not done:**
- **#7, the rest.** The live passport proves this host minted that session,
  not which agent it belongs to. A CLI could still swap two live sessions'
  `agent_id`. The full fix checks the passport's `HookPrincipal` against
  the agent, which needs the agent → workspace mapping on the host. A
  different uid for ptyd than for the agents would close it at the root
  (ADR-0001's isolated cells).
- **#8, the rest.**
  - A session's vault state isn't kept across a restart.
  - Sessions spawned locally on a host (not through the link) get no
    `k2_host` and are ended at a restart, as before S3.
  - A terminal query sent while no daemon was attached stays unanswered:
    the replay is muted (P75). A CLI waiting on one would hang. This is
    unconfirmed, and the fake agent doesn't send any.

### Results (S3.1)

| Check | Result |
|---|---|
| `scripts/lab/topology/host-restart-adopt.sh`, run `20261027T161131Z` | **7/7 PASS** |
| `cargo test -p k2-core --test ptyd_backend` | **11 passed** (with the #5 and #6 tests) |
| `cargo test -p k2-daemon --test ptyd_p76_integration` | **1 passed** |
| `cargo test -p k2-core --lib` | **3724 passed, 0 failed** |
| `cargo test -p k2-daemon --lib` (throwaway HOME) | **4686 passed, 0 failed** |
