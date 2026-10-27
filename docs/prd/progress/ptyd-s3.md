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
  - `meta_rebuilds_placed_and_turns`: the meta goes to `Placed` and
    `Turns`;
  - `only_placed_running_sessions_are_adopted`: running and `k2_host` set,
    versus exited or no meta;
  - `an_adopted_resume_resets_the_controllers_turn`: #5, lower counts are
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
