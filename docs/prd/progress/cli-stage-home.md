# cli-stage-home: with `K2_HOME` set, the `k2` CLI stays inside it, and agents still find it

Branch `k2-dist/cli-stage-home`, from `k2-dist/wave3` 0c6d3c9f7. Found by
lab's Linux gate (`docs/prd/progress/lab-g2.md`, wave3 88d1eddc2):
`k2_home_integration` `k2_home_holds_the_whole_k2_home_and_home_stays_empty`
failed as an unprivileged user, because `$HOME` got `.local/bin/k2`.

## The change

- **Staging** (`k2_core::cli_stage`, the pure `stage_plan`): with
  `K2_HOME` set, the CLI is staged at `$K2_HOME/bin/k2` only, on every
  Unix, beside the `k2-open` shim. Nothing is written to `/usr/local/bin`
  or `~/.local/bin`. Without `K2_HOME`, nothing changes: on Linux,
  `/usr/local/bin/k2`, then `~/.local/bin/k2`; on macOS, the app's symlink.
- **PATH** (coordinator: agents must keep a working `k2` in every
  topology, containers included): every agent session gets
  `<k2_home>/bin` first on its PATH whenever the daemon staged `k2` or
  `k2-open` there (`cli_stage::agent_bin_dir`).
  - In `terminal::daemon_pty`, the env built there serves both backends,
    in process and k2-ptyd, and every provider.
  - The legacy `alacritty_backend` terminal path does the same.
  - The `k2-open` shim already prepended that directory when it was staged;
    now the CLI alone is enough.
- **The scripts honor `K2_HOME`**: `cli/k2` and `scripts/k2-open` used
  `$HOME/.k2` even when `K2_HOME` was exported. Agent sessions inherit the
  daemon's `K2_HOME`, so the owner fallback (`heartbeat.{port,token}`) now
  finds the right home.

## Results

| Check | Result |
|---|---|
| `cargo test -p k2-core --lib -- cli_stage:: open_shim:: terminal::daemon_pty::` | 74 passed. New: `under_k2_home_the_cli_is_staged_only_inside_it` (Linux and not; an empty `K2_HOME` is unset), `a_staged_cli_under_a_k2_home_is_current_and_executable` |
| `cargo test -p k2-daemon --test k2_home_integration` | **4 passed**. `k2_home_holds_the_whole_k2_home_and_home_stays_empty` is kept and now also requires `bin/k2` in `K2_HOME`. New: `an_agent_sessions_path_resolves_k2_to_the_cli_staged_in_k2_home` and `an_agent_session_on_k2_ptyd_resolves_k2_to_the_cli_staged_in_k2_home`. In each, a real daemon under `K2_HOME` spawns an agent through `/cli/sessions/v2/spawn`; in its PTY the agent records `command -v k2` and its PATH. Both are `$K2_HOME/bin/k2`, with `$K2_HOME/bin` first. The ptyd test asserts the session ran on k2-ptyd, and `$HOME` stays empty |
| `scripts/lab/topology/two-process.sh` (host agents on k2-ptyd), `lab-runs/dist-link/20261026T122610Z` | **PASS**: every check, including W1 via the controller, wake by `k2 msg` (S4): 25 passed, 0 failed. The host log shows `k2 CLI staged: …/host/home/.k2/bin/k2` |

These ran on macOS. On Linux the path through `stage_plan` is the same
(`K2_HOME` wins before the platform is looked at), and the unit test covers
`linux = true`. Lab's Linux gate should rerun `k2_home_integration`.

## Notes

- **Version markers not bumped.** `cli/k2` and `scripts/k2-open` changed, but
  `release.sh` owns their markers (`K2_CLI_VERSION`,
  `K2_OPEN_SHIM_VERSION`), so a host that already holds a current-version
  copy keeps the old script until the next release. A fresh `K2_HOME` (a new
  volume, every lab run) gets the new one.
- **Root under `K2_HOME`.** A root daemon with `K2_HOME` set no longer writes
  `/usr/local/bin/k2`. A shell that isn't an agent session (`kubectl exec`
  into a pod) finds `k2` only if the image installs it or puts
  `$K2_HOME/bin` on PATH.
