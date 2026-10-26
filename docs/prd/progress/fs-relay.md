# fs-relay: the app's files and git through a controller, from the agent's host

Branch `k2-dist/fs-relay`, from `k2-dist/wave3` 897c06c4a. Task from the
coordinator (2026-10-26), after rc1: kind-app traced every route that acts
on the controller's own disk (`docs/prd/progress/kind-p4.md` on
`k2-dist/kind-p4`, "What still needs the shared volume"). This is the first
cut: the file browser and the git panel, reads and writes.

## The gap

Under `K2_ROLE=controller`, `/cli/fs/*` and `/cli/git/*` read and write the
controller's own disk. With the shared `k2-work` volume that is the same
disk the agents use; without it (a host on its own disk, `K2_WORK_ROOT`, home
sync) the app shows the controller's copy, not what the agent did, and its
edits and commits never reach the agent.

## The change

```mermaid
graph LR
    %% ── Colour legend ──────────────────────────────
    %% Blue = exists today · Violet = changed · Green = new · Slate = state · Orange = outside K2
    %% Shapes: rect = process, stadium = files, flag = setting

    %% ── APP ────────────────────────────────────────
    App[Desktop app<br/><sub>file browser · git panel</sub>]:::external

    %% ── CONTROLLER ─────────────────────────────────
    subgraph ctl [Controller · k2-daemon K2_ROLE=controller]
        G[gate::route<br/><sub>hosts/gate.rs · now takes the method</sub>]:::changed
        R[fs_relay::route<br/><sub>hosts/fs_relay.rs</sub>]:::new
        W[RemoteHost::workspace_route<br/><sub>agent_host/remote.rs</sub>]:::new
        L[fs_routes · git_routes<br/><sub>the controller's own disk</sub>]:::existing
        CR>K2_WORK_ROOT<br/><sub>the app's workspace paths</sub>]:::state
        CD([work-ctl<br/><sub>the controller's copy</sub>]):::state
    end

    %% ── HOST ───────────────────────────────────────
    subgraph host [Agent host · k2-daemon K2_ROLE=host]
        K[link · http.relay<br/><sub>hosts/link.rs</sub>]:::changed
        S[fs_relay::serve<br/><sub>its own daemon, its own token</sub>]:::new
        HL[fs_routes · git_routes<br/><sub>the host's own disk</sub>]:::existing
        HR>K2_WORK_ROOT<br/><sub>label k2.work_root</sub>]:::state
        HD([work-h1<br/><sub>the agent's workspace</sub>]):::state
    end

    App -- GET/POST /cli/fs, /cli/git --> G
    G -- controller only --> R
    R -- which host? --> W
    R -- no host holds the path --> L
    L --> CD
    R -- "http.relay (paths mapped)" --> K
    K --> S
    S -- loopback HTTP --> HL
    HL --> HD
    HR -. hello label .-> W
    CR -. maps paths .-> R

    classDef existing fill:#dbeafe,stroke:#2563eb,color:#1e3a5f
    classDef changed  fill:#ede9fe,stroke:#7c3aed,color:#3b0764
    classDef new      fill:#d1fae5,stroke:#059669,color:#064e3b
    classDef external fill:#ffedd5,stroke:#ea580c,color:#7c2d12
    classDef state    fill:#f1f5f9,stroke:#64748b,color:#1e293b
```

```mermaid
sequenceDiagram
    participant A as Desktop app
    participant C as k2-daemon<br/>(controller)
    participant H as k2-daemon<br/>(host-1, own disk)
    participant HD as host-1's daemon<br/>(loopback route)
    Note over C,H: Phase 0: host-1 links, hello labels k2.work_root=/var/lib/k2/work
    A->>C: GET /cli/git/changes?path=/work/notes&token=…
    Note over C: relayed route; a host on its own disk;<br/>notes runs on host-1 in /var/lib/k2/work/notes
    C->>C: token_ok (the controller authorises)
    C->>H: http.relay {GET, /cli/git/changes, path=/var/lib/k2/work/notes}
    H->>HD: GET /cli/git/changes?path=…&token=<host-1's own>
    HD-->>H: 200 [{"path":"lib.txt","status":"modified"}, …]
    H-->>C: http.reply {200, application/json, body}
    C-->>A: 200, paths mapped back to /work/notes
```

## Decisions

1. **Which requests.** Relayed: the file browser's reads (`info`,
   `read-dir`, `read-file`, `read-binary`, `read-range`, `stat`,
   `search-tree`, `zip-list`), its writes (`create`, `delete`, `move`,
   `rename`, `copy`, `duplicate`, `write-file`), every `/cli/git/*` route
   (status, diffs, branches, worktrees, stage, commit, notes, …) and
   `/cli/worktree`. Not relayed: what is about the controller's own machine
   (`open-finder`, `open-external`, `clipboard-paths`, `resolve-links`), the
   file events socket, the streaming uploads (`upload-binary`,
   `upload-chunk`), and the compress/extract jobs (their status polls carry
   a job id, no path, so they can't follow the start to its host). Those
   stay local; they are the next cut if the app needs them on a host.
2. **Which host.** The workspace of an agent placed on a host (its session's
   cwd), else the host that last ran an agent there (`last_host_for_cwd`,
   kept across restarts in `host_affinity`): "the workspace's home lives on
   that host". A placed agent wins; the longest matching workspace wins. A
   request naming workspaces on two hosts is refused (501 `cross_host`); a
   workspace whose host is not linked is a 503 `host_away`, never a silent
   read of the controller's stale copy (I2's spirit: loud, not wrong).
3. **Only hosts on their own disk.** A host with `K2_WORK_ROOT` adds the
   label `k2.work_root=<root>` to its hello (labels already carry
   `k2.max_awake`, `k2.kind`; no protocol change). Hosts without it share the
   controller's volume (`deploy/kind/dist`) and are never relayed to, so a
   shared-volume deployment, `K2_ROLE=all` and a controller with no such
   host take today's code path, byte for byte (the relay returns before it
   reads anything).
4. **Path namespaces (D2).** A workspace has the same path relative to each
   work root. The controller's root is its own `K2_WORK_ROOT`; the host's is
   its label. A request's paths (query values, and every string of a JSON or
   form body) under the workspace as either side names it are mapped to the
   host's name; the answer's paths are mapped back. In production the roots
   are the same path (`/var/lib/k2/work`), so the mapping is the identity;
   the lab runs them side by side on one Mac with different roots.
5. **Auth.** The controller authorises with the same check as the local
   routes (`http::token_ok`: the owner token or a Connect login; the role
   gate ran before). The host then runs the request as its own owner (its
   own token, loopback), and only for the relayed routes: a controller can't
   make a host run anything else.
6. **How the host serves it.** Through its own daemon's route over
   loopback (`http://127.0.0.1:<its port>`), so every relayed route behaves
   exactly as it does on that disk, with no per-route plumbing. The client
   keeps no idle connections (the daemon closes each one; see the enroll
   fix c453b247a).
7. **POST bodies.** The controller peeks the request (up to 8 MiB) without
   consuming it, so a request that turns out local reaches today's handlers
   untouched.
8. **What a host's own registry decides.** Routes that consult the host's
   project registry answer as the host does: `git/init` (and the git notes
   routes) need the workspace registered on the host, which hosts don't do
   today. That, and project create/add, clone-to unpack and app deploy from
   a folder, are the next phase (they create workspaces, not read them).

## Security review of 25c5c8c78: what changed

An independent review found 6 high, 5 medium and 3 low findings (the merge
was reverted from wave3). Every high and medium is fixed, each with a test
named for it in `crates/k2-daemon/src/hosts/fs_relay_tests.rs`:

| Finding | Fix | Test |
|---|---|---|
| H1 paths not confined (`..`, an unmapped second path, symlinks, `/tmp` vs `/private/tmp`) | Controller: a `..` in any path is a 400 `bad_path`; every path a request names must be in the workspace it is relayed to (400 `outside_workspace`); the controller's root is matched as given and canonical. Host: every absolute path in the query and the body (any key but the text keys), resolved through symlinks (the nearest existing ancestor for a new file), must be strictly under `K2_WORK_ROOT` (403 `outside_work_root`); names inside a path (`file_path`, `new_name`, `filename`) never hold `..` | `h1_traversal_and_an_unmapped_second_path_are_refused_on_the_controller`, `h1_the_host_confines_every_path_traversal_a_second_path_and_a_symlink_escape`, `a_path_goes_to_the_host_its_agents_workspace_is_on` (both root names) |
| H2 the allowlist bypassed by `..` / `%2e` | Exact route lists (no prefixes) on both sides; the host refuses a path with `%`, `?`, `#`, `\`, empty, `.` or `..` segments, builds the URL with `Url::set_path` and checks it unchanged after | `h2_dot_segments_and_percent_escapes_never_reach_another_host_route`, `the_relay_carries_exact_file_and_git_routes_…` |
| H3 the host's token in 502 bodies | The token goes as `Authorization: Bearer` (the daemon folds it into its auth), never in the URL; errors use `without_url()`; no redirects | `h3_the_hosts_token_is_not_in_an_error_body` |
| H4 a host-reported cwd controls routing | A cwd routes only when it is strictly under its host's `k2.work_root` (or under the controller's root, as the controller asked for it); `/`, a home directory or the root itself route nothing; the longest workspace wins, a placed agent only breaks a tie | `h4_a_malicious_cwd_captures_nothing_and_the_longest_workspace_wins` |
| H5 every caller becomes the host's owner | The git notes (`/cli/git/notes*`, author-or-Admin checks in the controller's database) are no longer relayed; what is relayed has no per-caller check beyond the role gate and `token_ok`, which the controller runs first | `the_relay_carries_exact_…` |
| H6 a big POST silently written to the controller's copy | No peeking: the dispatcher's POST arms (git, write-file, the fs arm, both uploads) read the whole body, then ask the relay; a request for a host workspace is relayed or refused (413 over 8 MiB), never served from the controller's copy | `m3_h6_auth_first_and_no_silent_fallback_for_a_big_post` |
| M1 a big answer kills the link | The host refuses an answer over 8 MiB (413: read by ranges); request bodies are capped the same way | `m1_an_oversize_answer_is_refused_on_the_host_not_sent` |
| M2 header injection from a host | The status line is rebuilt from its numeric code; the content type comes from a fixed list | `m2_a_hosts_status_and_content_type_cant_inject_headers`, and through the relay in the end-to-end test |
| M3 work and information before auth | `token_ok` first, before any routing (no `host_away` / `cross_host` to an unauthenticated caller); no peek buffer | `m3_h6_…` |
| M4 file contents rewritten | Only known path keys are mapped (`path`, `paths`, `dir`, `root`, `sources`, `destination`, …), by string prefix, the rest kept byte for byte; contents, messages and search text never | `m4_only_path_keys_are_mapped_…`, and the commit message in the end-to-end test |
| M5 notes split across two databases; `open-changes` on the host's bus | Notes and `open-changes` stay on the controller (as before fs-relay) | `the_relay_carries_exact_…` |

The lows (L1 retries and idempotency, L2 transport details, L3 worktree
paths outside the workspace) are open, for phase 2. Decision 1 above now
reads: the notes, `open-changes` and `/cli/worktree` (a database setting)
are not relayed; uploads are. Decision 7 is replaced by H6's fix.

## Re-review of cb6ee1009: what changed

The re-review found H1, H4 and H6 partial, M1 partial, and three lows.
Each is fixed, with a test:

| Finding | Fix | Test |
|---|---|---|
| H4 a host's label and cwd steer routing | **Coordinator's decision:** the controller decides each host's work root: `K2_HOST_WORK_ROOT` (default `/var/lib/k2/work`), `K2_HOST_WORK_ROOTS=host=/root,…` per host (the lab). A host's `k2.work_root` label only says it has its own disk. Routing comes only from the controller's own bindings (`relay_space`, kept across restarts): the cwd it asked for at a spawn, strictly under its own work root (`K2_WORK_ROOT`), never a host's report; only paths under the controller's work root route; the longest workspace wins; a binding expires after `K2_RELAY_SPACE_TTL_SECS` (7 days) without a spawn | `h4_a_hostile_work_root_label_and_cwd_capture_nothing`, `a_path_goes_to_the_host_the_controller_spawned_its_workspace_on`, `the_controllers_root_matches_as_given_and_canonical` |
| H1 a dangling symlink | The host resolves every path one component at a time (`lstat`): each link is followed to where it points, a dangling one too, `..` resolved lexically, 40 links at most; the result must be strictly under the work root. A relative name is resolved against its request's `path` and confined the same way | `h1_a_write_through_a_dangling_symlink_is_refused` |
| H6 fallbacks | `search-tree` is a POST (moved); `resolve-links` is relayed (the controller resolves an address or id to its registered path first); `compress` and `extract` for a workspace on a host are a 501 `not_relayed`; an App pass (`k2skn_`) to a relayed route on a controller with own-disk hosts is a 501 `not_relayed`. Every POST arm that serves these routes asks the relay (the create/copy/move/rename arm, the App-pass resolve-links arm, compress and extract were missing) | `m3_h6_auth_first_and_no_silent_fallback`, the end-to-end test (POST search-tree, an App pass GET) |
| M1 escaping past the frame cap | Both sides refuse a frame whose encoded size is over 15 MiB (the link's cap is 16 MiB), on top of the 8 MiB body cap | `m1_a_frame_of_max_size_with_escapes_is_refused_on_the_host` |
| Low: 8 MB upload chunks | The controller relays an upload chunk in 4 MiB pieces at their own offsets (only the last is the chunk's last) | the end-to-end test (an 8 MB chunk, byte for byte) |
| Low: a renamed file's diff | Only absolute paths route; a repo-relative `old_path` goes to the host, which confines it against `path` | the end-to-end test (`diff-file … old_path … sides=1`), `h1_traversal_…` |

Not changed: the remaining lows from the first review (retries and
idempotency, the blocking pool, worktree paths outside the workspace).

## Round 3 review of 16253180b: what changed

| Finding | Fix | Test |
|---|---|---|
| N1 `..` after a symlink (the H1 bypass) | `resolve` walks the components one at a time as the kernel does: a link's target is spliced in where it stands (an absolute one from `/`), `..` pops what is resolved so far (the parent of where a link really points), a dangling link is followed too; no lexical normalisation; a loop is refused after 40 links | `n1_dot_dot_after_a_symlink_is_the_links_parent_read_and_write_refused` (the exact `s -> …/.ssh/inner`, `d -> s/..` layout; a read and a write; a loop) |
| N3 bindings expiring under a live agent | A binding remembers its agent and never expires while the agent is placed on that host; it is touched whenever the controller keeps a session for the agent (a spawn, a resume) and when its host adopts it again | `n3_a_binding_lives_while_its_agent_is_placed` |
| N7 git past the root | Before a relayed git request (all but `init`), the host checks the workspace's own repository: a `.git` at the workspace root (nothing is looked for above it), its git dir (a gitfile's target, a common dir) and `core.worktree` under the work root. A host with a work root runs K2's own git commands with `-c core.hooksPath=/dev/null` (`k2_core::git::set_no_hooks`); libgit2 runs no hooks | `n7_git_never_reaches_past_the_work_root`, `n7_k2s_git_commands_run_no_hooks_on_a_host` |
| N6 batch failure; App passes | resolve-links: a link outside the work root (absolute, `~`, or through a link out of the workspace) is answered `linkable: false` in its place, the rest resolved. An App pass is validated first (a revoked one is the local route's 403) and refused (501) only when its workspace routes to a host | `n6_resolve_links_refuses_a_bad_link_not_the_batch`, `m3_h6_…` |

Accepted for now (documented, coordinator's decision):

- **N2, check then use.** The host checks a path, then its daemon's route
  opens it: a link swapped in between (by something on the host with write
  access to the workspace, i.e. the agent itself) is not caught. On Linux,
  `openat2(RESOLVE_BENEATH)` in the file routes would close it; a later
  hardening.
- **N4, workspaces outside the work root.** An agent spawned on an own-disk
  host with a cwd outside the controller's `K2_WORK_ROOT` is not routed:
  the controller logs a warning at that spawn, and the app sees the
  controller's copy of that folder.
- **N5, uploads restart.** A failed upload piece fails the chunk; the app
  restarts the transfer. An upload chunk must carry its numeric `offset`
  (a missing one is a 400, not taken as 0).

## Round 4 review of 0c491e05a: what changed

N1, N3 and N6 were confirmed fixed (N1 against the real filesystem: every
attack refused). N7 was partial:

| Finding | Fix | Test |
|---|---|---|
| N7 a nested repository; a one-line `[core] worktree = …` | No text parsing: for every git path a relayed request names (`path`, `projectPath`, `worktreePath`, …), the host opens the repository K2's git code would open (`Repository::discover`, as the git CLI from that folder) and requires its git dir, common dir and work tree to resolve under the work root (`k2_core::git::repo_dirs`). A nested repo, a gitfile, `core.worktree` in any syntax and a repository above the workspace all fail; one that doesn't exist yet opens its nearest existing folder | `n7_a_nested_repo_and_a_one_line_config_cant_reach_out`, `n7_git_never_reaches_past_the_work_root` (a repository above the work root, a gitfile, `core.worktree`) |
| N7 regression: the worktree routes got 403 | They name their repository `projectPath`: every git path key is checked, none required to be `path` | `n7_the_worktree_routes_pass_with_project_path` (create and prune through the host; `/etc` refused) |
| N6 edge: `docs/../README.md` failed the batch | `split_links` answers a candidate with `..` `linkable: false` in its place | `n6_resolve_links_refuses_a_bad_link_not_the_batch` |
| N6 nit: `resolve_pass` on an async worker | The App-pass check runs on the blocking pool at the gate too | |
| N3: `agent_id` on dev databases | `PlacementStore::open` adds the column to a `relay_space` made without it | `n3_an_older_relay_space_table_gets_its_agent_column` |

## Round 5 review of a07f81ada: what changed

| Finding | Fix | Test |
|---|---|---|
| R5-1 relative paths (high) | **Coordinator's decision.** The host refuses any relative value under a path key, except a route's repo-relative keys that its handler joins to an absolute base (`file_path`, `old_path`, `files` against `path` on the git routes; resolve-links' `paths` against `workspace`), which are confined as `base.join(v)`; a name (`new_name`, `filename`) is one plain component. The controller refuses to relay a request with any other relative path-key value (400 `relative_path`): only absolute paths go to a host. With no relative repository path left, `git_precheck` checks the exact repositories the handler opens | `r5_a_relative_path_beside_an_absolute_key_is_refused_read_and_write` (controller and host, read and write; a repo-relative stage passes), `r5_a_relative_nested_repo_is_refused` |
| R5-2 the repository's own commands in create-worktree | One hardened `git` on an agent host (`k2_core::git::git_command`, every K2 git CLI call): every inherited `GIT_*` removed, `GIT_CONFIG_NOSYSTEM=1`, `-c core.hooksPath=/dev/null -c core.fsmonitor=false`; `worktree add --no-checkout`, then a libgit2 checkout (libgit2 runs no external filter drivers); prune-worktrees through it too | `r5_create_worktree_runs_no_smudge_filter_or_fsmonitor` (with a control: unhardened, git does run the filter) |

## Checks

| Check | Result |
|---|---|
| Unit: route set, path mapping by whole components, query/JSON/form paths, the label | `hosts::fs_relay::tests` 2 tests, `hosts::config_tests` (the label) |
| Unit: which host (placed, the controller's or the host's name, siblings, no path, two hosts → split, host gone → away, shared-volume host → local) | `a_path_goes_to_the_host_its_agents_workspace_is_on` |
| In process: the controller's relay → a fake host link → a real host daemon: read-dir, read-file (paths mapped back), a bad token refused before relaying, a path no host holds stays local, write-file, git changes, stage-all, commit, diff-file | `the_file_browser_and_git_panel_through_the_controller_see_the_hosts_disk` |
| Two processes, separate work roots (`scripts/lab/topology/fs-relay.sh`) | **7/7 PASS** (`lab-runs/fs-relay/20261026T143509Z`) |

## How to try it

```sh
scripts/lab/topology/fs-relay.sh            # builds k2-daemon and the fake agent, then 7 checks
cargo test -p k2-daemon --lib fs_relay      # the unit and in-process tests
```

## Status

- First cut done: reads and writes of files and git through the controller
  from the agent's host.
- Next: project create/add on a host (and registering a workspace there, so
  `git init` works through the relay), clone-to unpack, app deploy from a
  folder, uploads and compress/extract jobs; then drop the shared `k2-work`
  on kind (`deploy/kind/p4`).
