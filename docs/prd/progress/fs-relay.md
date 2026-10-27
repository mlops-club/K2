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

## Round 6 review of 48fbab1ab: the structural fix

Round 6 found R6-1 (medium): create-worktree's `.worktrees/<branch>` and
`.gitignore` are paths the handler derives, and the host never confined
them. The coordinator decided on a structural fix instead of another
patch:

- **One schema per relayed route** (`hosts/fs_relay_schema.rs`, `SCHEMAS`).
  It lists every key the route's handler reads and what each key is: an
  absolute path, a path relative to a base key, a file name, resolve-links'
  candidates, or not a path. It also lists the paths the handler derives:
  the repository each git path opens, the worktree's folders and
  `.gitignore`, `<path>/.git`, an upload's collision-free file and a chunk's
  `.part`, and a copy's target plus every file under its sources (a link
  inside a copied folder would copy what it points at).
  - The host refuses any key the schema doesn't declare, and any key given
    twice (403 `undeclared_key`).
  - It confines every path and derived path to the work root (403
    `outside_work_root`).
  - It rebuilds the request its daemon gets from the confined values alone.
    A folder link is resolved, in the request's own spelling of the root,
    so the controller still maps answers back. The last name is kept as
    given, so a delete removes a link, not its target.
  - The controller refuses an undeclared key before relaying (400
    `undeclared_key`).
- **Repo-relative paths join the repository's work tree.** That is the base
  `git::stage_file` and `diff_file` join them to, not the request's `path`,
  which may be a sub-folder of the repository.
- **The handler scan.** A test reads each relayed handler's source: its
  `params.get`, `str_param`/`flag_param`, the struct it parses its body into
  (serde renames applied), and any helper it hands `params` to. It fails if
  a handler reads a key its schema doesn't declare, or uses `params` or
  `body` in a way the scan can't read. It also checks that each handler is
  the one the dispatch runs for its route.
- **Off by default.** fs-relay runs only when the controller has
  `K2_FS_RELAY=1`. Off, the gate and the POST arms hand every request back
  untouched, and no workspace is bound to a host: the controller behaves
  as it did without fs-relay.
  - A host keeps its git hardening and its `k2.work_root` label either way.
    The hardening only narrows what K2's own git does on a host, and the
    label is inert while the controller is off.
- **Named items:**
  - `.worktrees`, the branch's folder and `.gitignore` must not be symlinks
    on a host: the schema checks this, and so does `git::worktree_place`.
    `.gitignore` is opened with `O_NOFOLLOW` there.
  - The branch must be a valid branch name, and its folder one plain name.
  - Every git CLI call that takes a path puts `--` before it. That is
    `worktree add`; `worktree list`, `worktree prune` and `config` run in
    the folder and take no path.
  - Config-defined hooks: the host image's git is 2.39.5 (Debian bookworm),
    which has none. git 2.50 (macOS) doesn't run them either, by test.
    `core.hooksPath=/dev/null` covers the hooks directory. A newer git's
    `hook.<name>.command` runs whatever `hooksPath` says, and no `-c` can
    switch off hook names it doesn't know. So on a host, K2's git first
    reads the config with git's own parser (`git config --list
    --name-only`, includes and `includeIf` resolved as the command would
    see them), and refuses any `hook.*` key or a config it can't read
    (`git::git_command_in`).
- **Not closed:** confinement is a check before the handler runs. A link an
  agent swaps in between that check and the handler's open is still
  possible (a race), except at `.gitignore`. Later Linux hardening: open
  through `openat2` with `RESOLVE_BENEATH` (or `RESOLVE_IN_ROOT`) from the
  work root, so the kernel enforces the confinement at the open itself.
- **Test changes:**
  - `h1_…` now expects `undeclared_key` (still a 403) for a path under an
    unknown key.
  - `r5_…` stages with `filePath`, the key the stage handler reads. The
    old request's `file_path` was never read: the schema caught it.

| Item | Test |
|---|---|
| Off by default; off, nothing bound, gate and arms hand back | `r6_off_by_default_the_controller_serves_every_route_itself` |
| One schema per route; its paths are the mapped keys; relative keys join a path of the same route | `r6_every_relayed_route_has_one_schema` |
| A handler reads only declared keys (source scan, non-vacuous; a mutation removing `show_hidden` fails it) | `r6_a_relayed_handler_reads_only_its_schemas_keys` |
| Undeclared or repeated keys refused (host and controller) | `r6_an_undeclared_or_repeated_key_is_refused` |
| The daemon gets confined values | `r6_the_daemon_gets_the_confined_values` |
| R6-1: `.worktrees`, branch folder, `.gitignore` links refused; invalid branches | `r6_create_worktree_refuses_a_symlinked_worktrees_dir_branch_folder_or_gitignore` (with a control) |
| Config hooks | `r6_k2s_git_refuses_a_repository_whose_config_names_a_hook` (with a control) |
| `--` before paths | `git::tests::worktree_add_puts_dash_dash_before_its_paths`, `a_worktree_branch_is_a_valid_name_and_one_folder` |
| `file_path` joins the repository's work tree | `r6_a_repo_relative_path_joins_the_work_tree_the_handler_joins` |
| An upload's files, a copy's tree | `r6_an_uploads_files_and_a_copys_tree_are_confined` (with controls) |

## Round 7 review of e3b044365: F1

Round 7 found it safe to merge with the flag off, and not yet safe to
enable. F1 (medium) is fixed here; F2–F6 (low) are still open.

- **F1(a): a link inside the git dir.** The `Repo` check confined only the
  git dir, the common dir and the work tree themselves. A commit wrote its
  ref and reflog through symlinked `.git/logs` and `refs/heads`, and
  `worktree add` wrote through a symlinked `.git/worktrees`.
  - Now `git_precheck` walks the git dir and the common dir and refuses any
    symlink in them, for every git route, reads included.
  - A loose-object fan-out folder and `objects/pack` are checked
    themselves, not each object in them.
  - `objects/info/alternates` naming objects outside the work root is
    refused too.
- **F1(b): a forced checkout through a folder link.** abort-merge, a
  fast-forward merge, discard and resolve check out with force, and a
  tracked folder swapped for a symlink let them delete or write outside the
  root.
  - A new derived path, `CheckoutTree`, walks the whole work tree of the
    repository the route opens (not just the request's folder).
  - It refuses a link to a folder outside the work root, or a dangling one.
  - A link to a file is allowed, since the checkout replaces the link
    itself, and so is a link that stays inside the root.
- **Not closed:** this is still a check before use. A link swapped in
  after the check races it (N2). Later hardening: `openat2` with
  `RESOLVE_BENEATH`, or a separate uid for relayed serving.

| Item | Test |
|---|---|
| F1(a): `logs`, `refs/heads`, `worktrees` links; alternates out; reads too (with a control) | `r7_f1_a_link_in_the_git_dir_is_refused` |
| F1(b): abort-merge, merge-branch, discard, resolve; from a sub-folder; a dangling link (with controls: a folder link inside, a file link out) | `r7_f1_a_forced_checkout_through_a_folder_link_is_refused` |

Both tests fail with the two checks taken out (checked by mutation).

## Round 8 review of 02ea61653: F1(b) completed

F1(a) (links inside the git dir) was confirmed fixed. F1(b) was partial.

- **B1 (medium): every `.git` was skipped.** The work-tree walk skipped
  any folder named `.git`, at any depth. An untracked `hide/.git/deep`
  linking out, reached through a tracked `sub -> hide/.git`, let
  abort-merge delete outside the root. Now only the repository's own git
  dir is skipped, compared by its resolved path.
- **B2 (medium): an in-root folder link leaving the work tree.** Such a
  link was allowed but never walked, so a link out beneath its target was
  never seen. Now any link in the walk to a folder (or a dangling one)
  whose target is outside the repository's work tree is refused with 403
  `link_outside_worktree`. The walk never follows a link. A link to a file
  is still the checkout's to replace.
- **C (low-medium): unbounded walk cost.** 2000 links with long `a/../`
  chains took about 20 s for one check, on the link's task.
  - The host now runs all its checks (`split_links`, `confine`) in
    `spawn_blocking`.
  - The checks share one budget per request: 500,000 steps (each step is
    one link component resolved or one folder entry walked) and 5 s. Past
    either, the request is refused with 403 `walk_budget`.
- **Later (lows from the review, not now):**
  - alternates are read one level deep;
  - links among the object fan-out files;
  - the 200k entry cap;
  - self-loops;
  - submodules.
- **Test change:** r7's forced-checkout test also accepts
  `outside_work_root`. Discard's `sub/f` is now refused as a repo-relative
  path outside the work root before the walk runs.

| Item | Test |
|---|---|
| B1 | `r8_b1_only_the_repositorys_own_git_dir_is_skipped` |
| B2: abort-merge and a fast-forward merge-branch (with a control) | `r8_b2_a_folder_link_leaving_the_work_tree_is_refused` |
| C: a small budget refuses; the default budget passes; the next request starts afresh | `r8_c_the_checks_stop_at_their_walk_budget` |

Each of the three tests fails with its fix reverted (checked by mutation).

## Round 9 review of 57002f441: X1, X2, X3

B2 is closed. B1 reopened as X1.

- **X1 (high): a checkout through the object store.** The work-tree walk
  skipped the repository's git dir, and the git-dir check never listed
  `objects/xx` or `objects/pack`. So `.git/objects/ab/deep -> outside`,
  reached through a tracked `sub -> .git/objects/ab`, let abort-merge delete
  outside the root. Fixed both ways:
  - any link in the work tree whose target is inside the git dir or common
    dir is refused (403 `link_into_git_dir`);
  - the git-dir check lists every fan-out folder and `pack`, and refuses a
    link or a folder in one.
- **X2: a folder a check can't read was skipped.** Any read error other
  than NotFound now fails the check, closed (`read_dir_all`), in all three
  walks: the checkout's work tree, the git dir, and a copy's source.
- **X3 (medium): resolve-links' candidates ran outside the budget.** 2000
  candidates took 17 s.
  - One budget now covers the whole request (`check_request`), including
    `split_links`.
  - Past 500 candidates, the rest are answered `linkable: false` without
    being checked.
- **Lows done now:**
  - the git-dir walk counts against the budget;
  - the budget is an RAII guard, reset on drop (a panic too). A nested
    guard shares the request's budget.

| Item | Test |
|---|---|
| X1 (the repro) | `r9_x1_a_checkout_through_the_object_store_is_refused` |
| X1, each half on its own (with controls) | `r9_x1_links_into_the_git_dir_and_into_its_object_folders` |
| X2: all three walks (mode 0300), with a control | `r9_x2_a_folder_the_check_cant_list_fails_it` |
| X3: a small budget refuses; the default budget checks all; the cap | `r9_x3_resolve_links_candidates_are_in_the_budget_and_capped` |

Each test fails with its fix reverted (checked by mutation; the repro
fails only with both halves of X1 reverted).

## Round 7's lows: F2–F6

One commit each, on top of round 9.

- **F2: a repo-relative path is literal.** `diff_file` and
  `resolve_conflict` match the path as a literal pathspec
  (`pathspec_literal`, as discard and folder staging already did). So
  `filePath="*"` resolves or diffs no other file. Test:
  `git::tests::a_file_path_is_literal_not_a_pathspec`, with a control.
- **F3: the Trash stays inside the work root.** A host with a work root
  sends what goes to the Trash to `<root>/.k2-trash`
  (`safe_delete::set_host_trash`, set at host start). That covers delete
  without `permanent`, discard and remove-worktree: renamed there, never
  copied or unlinked. Test: `f3_the_trash_stays_inside_the_work_root`.
- **F4: no leaks about outside the work root.**
  - A host's listing gives a link's own size and time (`lstat`). A local
    listing still follows the link, as before.
  - resolve-links' answer from a host keeps only the workspace's root in
    its prefixes.
  - `/cli/fs/info` names no path, so it was never relayed; it is now out
    of the schema.
  - Test: `f4_a_host_tells_nothing_about_outside_its_work_root`, with a
    control.
- **F5: a request stays in its routed workspace.** The controller adds
  `k2_workspace` (the routed workspace's host path) to every relayed
  request. The app can't send it, because it is undeclared on every route.
  The host checks it is a workspace under the root, takes it off the query,
  and confines every path and resolve-links candidate to it.
  - If it is absent, the host confines to the work root, as before; a
    controller always sends it.
  - Git's repository checks stay at the work root, so a workspace inside a
    larger checkout keeps working.
  - Test: `f5_a_request_stays_in_its_routed_workspace`, with a control.
- **F6: the scan's blind spots.**
  - Now caught: a parse the scan can't name the struct of (a turbofish, a
    type inferred without `match`) and a `#[serde(flatten)]` both fail the
    scan; serde `alias` keys are read.
  - Tested on synthetic handlers:
    `f6_the_handler_scan_reads_what_it_can_and_fails_on_the_rest`.
  - **Still not checked (documented):** reads in the dispatcher arms,
    reads from the database or the environment, and a key's kind (path or
    plain). The scan detects drift; the host's run-time refusal of
    undeclared keys is the real control.

## Round 10 review of 28111e3dd: D1, D2, D3

X1, X2, X3, F2, F3 and F6 are confirmed fixed. The reviewer's 8 tests
are added as they were written (`rv10_*`); 4 of them failed before this
commit.

- **D1 (medium): a gitfile into a sibling workspace.** `notes/.git`
  holding `gitdir: <root>/other/.git` let stage-all and commit, routed to
  `notes`, move `other`'s branch.
  - Now the repository a git path opens must have its git dir, common dir
    and work tree inside the routed workspace.
  - The one exception is a workspace inside a larger checkout: a work tree
    that holds the workspace, with that work tree's own `.git` folder as
    its git dir.
  - Tests: `rv10_f5_gitfile_into_sibling_workspace`, plus
    `r10_d1_a_workspace_inside_a_larger_checkout_passes` (the exception).
- **D2 (low): case variants.** The link-into-git-dir check compares
  folders by device and inode, so `.GIT` and `.Git` on a case-insensitive
  disk are `.git`. Test: `rv10_item1_case_variant_link_into_git_dir`.
- **D3 (low): outside paths in refusals.** A refusal never names a path
  outside the work root: not a git dir, not an alternates line. Test:
  `rv10_f4_refusal_names_a_path_outside_the_root`.
- **Nits:**
  - A non-string resolve-links candidate is refused, not dropped. Test:
    `rv10_item3_check_request_leaves_nothing_behind`.
  - A host's `.k2-trash` that is itself a symlink is refused. Test:
    `r10_the_trash_folder_is_never_a_link`.
- **Not done (the review's unconfirmed note):** whether a link outside
  points at a file or a folder can still be told apart from the refusal.

## Round 11 review of bab357829: N1, N2

D1 (the reported attack), D2, D3 and both nits are confirmed fixed; the
ancestor exception can't reach a sibling's repository. The reviewer's
tests are added as given (`rv11_*`); 2 of them failed before this
commit.

- **N1 (medium): alternates were checked against the work root.**
  `notes/.git/objects/info/alternates` naming `other/.git/objects` let
  file-at-ref, routed to `notes`, read `other`'s content. Alternates must
  now resolve inside what the request may touch: the workspace, the
  enclosing checkout (the exception), or the project a K2 worktree belongs
  to. Test: `rv11_alternates_into_sibling_reads_its_objects`.
- **N2 (medium, a regression from D1): K2's own linked worktrees were
  refused.** A workspace at `<project>/.worktrees/<branch>` has its git dir
  at `<project>/.git/worktrees/<id>`, which D1 refused. It is accepted when
  all of these hold:
  - its work tree is the workspace;
  - its git dir is `<common>/worktrees/<id>`;
  - that git dir's `gitdir` back-link resolves to `<work tree>/.git`;
  - its common dir is in the workspace, or is an enclosing checkout's
    `.git`.

  A gitfile into another workspace's linked worktree is still refused
  (`rv11_d1_probes_all_refused`). Test:
  `rv11_d1_linked_worktree_workspace`.
- **Fail closed.** A git dir, common dir or work tree that can't be
  resolved fails the check. Before this, the unresolved path was used.
- **Accepted limit: the enclosing-checkout exception.** A workspace inside
  a larger checkout (`mono/team/notes`, with the repository at `mono`) gets
  git on that whole checkout. stage-all routed to `notes` stages
  `mono/team/other`'s changes too, and abort-merge's checkout acts on
  everything under `mono`. That is what git does on a sub-folder of a
  repository; the relay can't narrow it. A sibling workspace's own
  repository, or one outside the enclosing checkout, is never reached.
  Test: `rv11_exception_reaches_sibling_in_same_checkout`; it records the
  reach and asserts nothing.

## Round 12 review of f2950fe82: the strict git policy

N2 (linked worktrees) and fail-closed resolution are confirmed fixed. The
review found three more ways to escape through git discovery (nested,
linked or non-UTF-8 alternates; a non-UTF-8 `commondir`; a repository
libgit2 can't open, whose check was skipped while the git CLI followed it).
The coordinator decided to stop chasing git features one at a time and
apply a strict policy instead. On a host, the repository a relayed git
request opens must pass all of these:

- **(a) No alternates at all.** An `objects/info/alternates` or
  `http-alternates` in the git dir or common dir is refused, even one
  pointing inside the workspace; a K2 workspace never borrows objects.
  This replaces round 11's alternates walk.
- **(b) Clean pointer files.** `commondir`, the `gitdir` back-link and a
  `.git` gitfile are read as bytes (`k2_core::git::path_line`). Each must
  be one line of UTF-8 with no NUL or control character. `repo_dirs`
  refuses a `commondir` that isn't, instead of taking it as absent.
- **(c) Fail closed.** A `.git` at or above the path that libgit2 can't
  open is refused, not skipped.
- **(d) The CLI agrees.** `git rev-parse --absolute-git-dir
  --git-common-dir --show-toplevel`, run as the routes run git, must name
  the same three folders libgit2 opened.
- **(e) A known format.** `repositoryformatversion` must be 0 or 1. The
  only extensions allowed are `noop`, `worktreeConfig`,
  `relativeWorktrees` and `objectFormat=sha1`
  (`k2_core::git::known_repo_format`).

K2's linked worktrees keep working, including those of a project inside a
larger checkout.

**What the tests show (mutation, one item taken out at a time):**
- Without (a) or (c), the review's tests fail.
- Without (b), (d) or (e), nothing in the relay suite fails: the bypasses
  the review found are already caught by (c), because libgit2 can't open
  those repositories. (b), (d) and (e) are layers behind it, each with its
  own test:
  - `git::tests::a_pointer_file_is_one_clean_path_line`
  - `r12_d_the_git_cli_must_open_what_libgit2_opened`
  - `git::tests::a_repository_format_k2_knows`

**The reviewer's tests (`rv12_*`) are added; three are changed:**
- `rv12_n1_direct_alternates_refused` now expects alternates inside the
  workspace to be refused, as policy (a) says.
- `rv12_n2_relative_backlink` expects a refusal when libgit2 can't open
  the worktree. libgit2 1.8 can't open git 2.48's `--relative-paths`
  worktrees, so K2's handlers couldn't serve one either. K2 makes
  worktrees with absolute paths.
- `rv12_libgit2_unopenable_skips_check_cli_follows`: its cleanup
  `git config --unset`, run in `notes`, follows `notes`' commondir into
  `other`'s config, which the test had just restored. Its status is no
  longer asserted. The check refuses all three extensions the test tries.

## Round 13 review of 26daa2318: ready to enable, three lows closed

The review found the strict policy complete: every earlier bypass is
refused, and the routes run no repository-config commands. The three
lows it raised are closed here.

- **L1: a config can include another's.** `include.path` and `includeIf`
  could make git read another workspace's config. Before libgit2 opens
  anything, the host now runs `git rev-parse` and `git config
  --show-origin --list -z`, as the routes run git. Every value must come
  from one of:
  - the repository's own `config` or `config.worktree`;
  - the host's global config (`~/.gitconfig`, `$XDG_CONFIG_HOME/git/config`);
  - K2's own `-c` flags.

  Anything else is refused. This runs first because libgit2 follows
  includes too, with no time limit.
- **L2: a submodule's git dir.** The `.git` of each gitlink in the index
  and each submodule in `.gitmodules` must resolve inside the workspace,
  whether it is a gitfile or a folder.
- **L3: no time limit on git.** Every git command K2 runs on a host
  (`config --list`, `rev-parse`, `worktree list/add/prune`) runs within
  10 s (`k2_core::git::GIT_TIMEOUT`, `output_within`); past it the command
  is killed and the request refused. A config `include` of a FIFO is
  refused this way instead of hanging.
- **(c), optional:** a bare-layout folder (`HEAD` plus `objects/`) at or
  above the path counts as a repository too, for the fail-closed check.
- **A limit to know about:** a host whose own global config includes
  another file has all git refused. The K2 host image has none.

| Item | Test |
|---|---|
| L1: `include.path`, `includeIf` (with a control) | `r13_l1_a_config_include_from_elsewhere_is_refused` |
| L2: a submodule gitfile into a sibling; one inside passes | `r13_l2_a_submodule_git_dir_outside_the_workspace_is_refused` |
| L3: a slow command killed; a FIFO include refused | `r13_l3_git_runs_within_a_time_limit` |

The reviewer's round 13 tests (`rv13_*`) are added as given. Three of
them only record what happens (include, submodule, bare layout), and all
three now show a refusal.

## K5 on kind: two fixes

kind-app's K5 passed 49 of 50 checks. Two things are fixed here.

- **create-worktree failed with "FOREIGN KEY constraint failed".** Adding a
  workspace registered its project twice, once on the host and once on the
  controller, under different ids. A relayed create-worktree carries the
  controller's id, which the host's database doesn't have (git had already
  made the worktree). Now the ids agree end to end:
  - The controller registers a workspace made or added on a host under the
    host's project id (`projects_create_as`).
  - On a host, a project id it doesn't know is its own project for that
    path, or, with none, is registered under that id. This also covers
    workspaces registered before this fix.
  - A worktree made through the relay is recorded in the controller's
    registry under the host's workspace id, which is where the app lists
    workspaces from, and it goes when it is removed.
  - Test: `k5_a_relayed_create_worktree_with_the_controllers_project_id`
    reproduces the error with the fix taken out. Also
    `k5_a_project_registered_under_the_hosts_id` and
    `k5_the_controller_records_a_relayed_worktree`.
  - **Prune and remove (follow-up).** prune-worktrees now updates the
    controller's registry too. After a relayed prune, the controller asks
    the host for the project's worktrees (a relayed `git/worktrees`) and
    removes its own worktree rows the host no longer lists.
    - When the host's list can't be had, or names a path outside the
      project's, nothing is removed.
    - remove-worktree also removes a row by its `worktreePath` when the
      request names no `workspaceId`.
    - Test: `k5_a_relayed_prune_or_remove_updates_the_controllers_registry`,
      which fails with the fix taken out.
- **Commit failed with "Failed to get signature".** A fresh host had no git
  identity, and K2 set one only when it ran `git init` itself. Now a host
  sets one at start, as K2 does locally before its first commit
  (`git_identity::ensure_git_identity`):
  - only the missing `user.name` and `user.email`, from its OS account, in
    its own global config, on its own volume;
  - an identity already set is kept, and a repository's own `user.*` still
    wins at commit time, as in git.
  - The manual identity step in `scripts/kind/k5-check.sh` is no longer
    needed. Test: `k5_a_fresh_host_gets_a_git_identity`.

## K5: turning fs-relay on, on kind (runbook)

For kind-app, once the review of fs-relay is clean. Everything here is on
the kind cluster `k2lab`, namespace `k2-kind-p4` (the P4 overlay,
`deploy/kind/p4`), through an explicit kubeconfig. Never the current
context, and never the other `k2-kind*` namespaces.

```sh
kind get kubeconfig --name k2lab > "$TMPDIR/k2lab.kubeconfig"
kn() { kubectl --kubeconfig "$TMPDIR/k2lab.kubeconfig" -n k2-kind-p4 "$@"; }
```

### What it needs

- **An image with fs-relay.** Build `k2-daemon` from `k2-dist/fs-relay2`
  (or wave3 once fs-relay is merged), under the Docker lock, with
  `K2_KIND_BUILD_MEMORY=6g CARGO_BUILD_JOBS=1 scripts/kind/build-daemon.sh`
  (at the default 4g the compile is OOM-killed). Then bring P4 up from it
  with the switch on: `scripts/kind/p4-up.sh --daemon-image=<that image>
  --fake-src=<its tree> --fs-relay`. The `--fs-relay` switch is in
  `k2-dist/wave3` from 454463cd9. The controller and the hosts run the
  same image; never mix in a host built before fs-relay.
- **A git identity on each host: nothing to do** (wave3 85cf7e2f6). A
  host fills in a missing `user.name` and `user.email` at start, in its
  `HOME` on its own data volume, so the git panel's commit works on a
  fresh host. Before that fix it failed with `Failed to get signature`.
- **Volumes: nothing new.** The P4 overlay already has what K5 is about:
  - no shared `k2-work`;
  - the controller has its own workspaces volume (`k2-work-controller` at
    `/var/lib/k2/work`);
  - each host has its own (claim template `work` at `/var/lib/k2/work`).
- **RBAC: nothing new.** The relay goes over the host link the hosts
  already hold (`http.relay`/`http.reply` frames). Hosts never call the
  API server, and the controller's pool rights
  (`deploy/kind/dist/pool-rbac.yaml`) are unchanged.
- **The hosts' env: what P4 already sets.** `K2_WORK_ROOT=/var/lib/k2/work`
  is set by `p4-up.sh` for home sync. With it, each host:
  - says `k2.work_root=…` in its labels;
  - runs K2's git hardened (no hooks, filters or fsmonitor; it refuses a
    config with `hook.*`);
  - sends what goes to the Trash to `/var/lib/k2/work/.k2-trash`;
  - lists a symlink's own size.

  Nothing to add.
- **The controller's env: two variables, both set by `--fs-relay`.**
  - `K2_FS_RELAY=1`: the switch. Off by default, and off is the
    controller as it was.
  - `K2_WORK_ROOT=/var/lib/k2/work`: the controller's own work root. Only
    paths under it are relayed, mapped to the same relative path under the
    host's root.
  - Optional:
    - `K2_HOST_WORK_ROOT` (default `/var/lib/k2/work`, which the P4 hosts
      use);
    - `K2_HOST_WORK_ROOTS=host=path,…` when hosts differ;
    - `K2_RELAY_SPACE_TTL_SECS` (default 7 days): how long a workspace
      stays bound to its host after its agent is gone.

  `p4-up.sh --fs-relay` (through `up.sh --fs-relay`) writes both into the
  controller's env in the manifest it renders and applies. Nothing is set
  by hand afterwards. To see what it would apply without applying it:

  ```sh
  scripts/kind/p4-up.sh --fs-relay --render-only="$TMPDIR/p4.yaml"
  grep -A1 K2_FS_RELAY "$TMPDIR/p4.yaml"   # in the k2-controller StatefulSet
  ```

  `up.sh --fs-relay` refuses to run unless the hosts have their own work
  volumes (`--p4` or `--host-work=own`) and `--work-root`.

  To turn it off again, run `p4-up.sh` without `--fs-relay`. The
  controller's env then loses both variables, and it rolls.

### What the app then shows

A workspace is bound to a host when the controller spawns an agent in it
(its cwd under `K2_WORK_ROOT`), or when it is created, added or cloned
through the controller (phase 2). For a bound workspace:

- **The file browser** (read-dir, read-file, stat, search, uploads,
  create, rename, move, copy, delete, write) shows and changes the host's
  disk, at the controller's paths. The controller's own copy of that
  folder is never read or written.
- **The git panel** (snapshot, diff, stage, commit, discard, branches,
  worktrees, merge) acts on the host's repository.
- **Thread file links** (resolve-links) resolve against the host's
  workspace.
- **Phase 2:**
  - New Project and Add Project make or seed the folder on a host;
  - Clone-to unpacks on a host;
  - App deploy from a folder builds from a copy of the host's folder (no
    links).

What it refuses, and how the app sees it:

| Case | Answer |
|---|---|
| A workspace whose host is away | `503 host_away`; the controller's copy is never served instead |
| An App pass (`k2skn_`) on a host workspace | `501 not_relayed` |
| compress / extract on a host workspace | `501 not_relayed` |
| A path with `..`, a relative path, or two workspaces in one request | `400 bad_path` / `relative_path` / `cross_host` |
| A key the route doesn't take | `400 undeclared_key` |
| On the host: anything outside the routed workspace, after every link | `403 outside_work_root`, `link_outside_worktree`, `link_into_git_dir`, `walk_budget` |

A workspace that isn't bound (no agent has run there, nothing was created
through the controller) is the controller's own, as without fs-relay.

### End-to-end check

1. **Both sides are set.**
   - After `p4-up.sh --fs-relay`,
     `kn exec k2-controller-0 -c k2-daemon -- env | grep -E 'K2_FS_RELAY|K2_WORK_ROOT'`
     shows `1` and `/var/lib/k2/work`.
   - `GET /cli/hosts/status` on the controller (port 7444, owner token)
     lists each host as linked, with `k2.work_root=/var/lib/k2/work` in its
     labels.
2. **An agent on a host.**
   - `scripts/kind/app-check.sh --p4 --password-file=…` spawns in
     `/var/lib/k2/work/demo` and prints which host the agent ran on (call
     it `k2-host-N`).
   - Then write a file there from inside that host's pod:
     `kn exec k2-host-N -- sh -c 'echo on-host > /var/lib/k2/work/demo/k5.md'`.
3. **The file browser reads the host.**
   - With the app's session token,
     `GET /cli/fs/read-dir?path=/var/lib/k2/work/demo` through the
     controller lists `k5.md`, and `read-file` returns `on-host`.
   - `kn exec k2-controller-0 -c k2-daemon -- ls /var/lib/k2/work/demo` does
     not have it.
   - The controller's log has `ctl.fs_relay`; the host's has
     `host.fs_served`.
4. **Writes land on the host.**
   - `POST /cli/fs/write-file {path: /var/lib/k2/work/demo/k5b.md}` through
     the controller.
   - `kn exec k2-host-N -- cat /var/lib/k2/work/demo/k5b.md` shows it, and
     the controller's volume still lacks it.
5. **The git panel works on the host.**
   - Add the workspace through the controller, as the app's Add Workspace
     does: `POST /cli/projects/add-from-path {path: /var/lib/k2/work/demo}`
     (its `id` is the `projectId` a worktree needs).
   - `demo` is already a repository (`up.sh` made it), so no
     `POST /cli/git/init`. Then `info`, `changes`, `stage {filePath}`,
     `stage-all` and `commit {message: "k5"}` on `/var/lib/k2/work/demo`.
   - `kn exec k2-host-N -- git -C /var/lib/k2/work/demo log --oneline`
     shows `k5`.
   - `GET /cli/git/snapshot?path=…` through the controller shows the same
     HEAD.
   - A linked worktree: `POST /cli/git/create-worktree {projectPath,
     branch, projectId}` makes `.worktrees/<branch>` on the host, and the
     same steps work in it. (On 454463cd9 the route answered `400 FOREIGN
     KEY constraint failed`: the project had two ids, the controller's and
     the host's. Fixed in wave3 85cf7e2f6: one id end to end.)
6. **Refusals.**
   - `read-file?path=/var/lib/k2/work/demo/../../../etc/passwd` → `400 bad_path`.
   - An extra key (`…&mode=x`) → `400 undeclared_key`.
   - In the host pod, `ln -s /etc /var/lib/k2/work/demo/etc-link`, then
     `read-file?path=/var/lib/k2/work/demo/etc-link/passwd` → `403
     outside_work_root`.
7. **The host is away.**
   - `kn delete pod k2-host-N`, then, before it relinks,
     `read-dir?path=/var/lib/k2/work/demo` → `503 host_away`, never the
     controller's empty copy.
   - Once it is back (its `work` volume keeps the files), the same request
     shows `k5.md` again.
8. **Off is the old behaviour.**
   - Run `p4-up.sh` again without `--fs-relay` and wait for the rollout.
   - `read-dir?path=/var/lib/k2/work/demo` now answers from the
     controller's own volume: its copy (no `k5.md`), or `400 No such file
     or directory` when the controller never had that folder (the P4
     controller's `k2-work-controller` starts empty).

`scripts/kind/k5-check.sh [--host-away] [--off-after]` runs steps 1–8 in
this order, with step 5 in the workspace and in a linked worktree, and the
refusals of step 6 plus a git path outside the repository (`filePath
../../../etc/passwd` → `403 outside_work_root`). G4 runs it as K5 with
`LAB_KIND_FS_RELAY=1` (`scripts/lab/topology/kind.sh`).

The same checks run without kind, on two processes, in
`scripts/lab/topology/fs-relay.sh` (7 checks; it sets `K2_FS_RELAY=1`).

## The owner's K2 at rc3: fs-relay ON or OFF? (plan)

Recommendation: **OFF for rc3.** Turning it on for the owner is a storage
migration, not a switch, and it should be done as its own planned change
after rc3.

### The live shape (read 2026-10-27)

Read on `k2lab`, namespace `k2-kind-dist`, `get` only, through an explicit
kubeconfig. No exec, so the contents of the volumes were not listed.

- **Pods:** `k2-controller-0` (`k2-daemon:dist`) and `k2-host-0`
  (`k2-agents:dist`), both on `k2lab-worker2`. The host has `podAffinity` to
  the controller's node.
- **Controller env:**
  - `K2_ROLE=controller`, `K2_HOST_AUTH=enroll`;
  - `K2_PROVISIONER=kubernetes` with MIN 1 and MAX 2;
  - no `K2_WORK_ROOT` and no `K2_FS_RELAY`.
- **Host env:** `K2_WORK_ROOT=/var/lib/k2/work` (home sync, from the rc1
  roll), `K2_PTYD=1`, `K2_HOST_MAX_AWAKE=100`.
- **Volumes:**
  - `k2-work` (5Gi, ReadWriteOnce) is mounted by **both** the controller
    and the host at `/var/lib/k2/work`. This is the shared shape: there is
    one copy of every workspace, and both see it.
  - Each host has its own HOME (`data-k2-host-0`, and `data-k2-host-1` from
    the pool's second host).
  - The controller has `data-k2-controller-0`, plus three in-cluster
    backups (`k2-backup-controller-*`).
- **Where the workspaces live:** on `k2-work`, at `/var/lib/k2/work/<name>`.
  It is one volume, so the controller and the host already see the same
  files.

### What turning the relay on would change

- **As the owner's K2 stands, the relay can't usefully be on, and
  `up.sh` refuses it.** `up.sh --fs-relay` needs hosts on their own work
  volumes, and the owner's share the controller's. Forced on by hand, it
  would serve the same bytes through the host, with only costs:
  - a workspace whose host is away answers `503 host_away`, where today
    the controller reads the shared file;
  - App passes and compress/extract on such a workspace answer `501`;
  - repositories that use alternates or config includes are refused;
  - every request takes an extra hop.
- **Really on means the P4 shape:** `--host-work=own`. Each host gets its
  own `work` claim, and the controller keeps `k2-work` as its own copy.
  The render-only diff of the roll, without and with (`up.sh --dist
  --hosts=1 --ptyd=1 --work-root=/var/lib/k2/work`, then `+ --host-work=own
  --fs-relay`; rendered with token auth and the static provisioner, so
  `up.sh` reads none of the owner's Secrets; the delta is the same with
  enroll and the pool):

  ```diff
  # k2-controller: the switch and its work root
  -      - envFrom:
  +      - env:
  +        - name: K2_FS_RELAY
  +          value: "1"
  +        - name: K2_WORK_ROOT
  +          value: /var/lib/k2/work
  +        envFrom:
  # k2-host: no longer mounts k2-work...
  -      - name: work
  -        persistentVolumeClaim:
  -          claimName: k2-work
  # ...but its own claim, per host
  +  - metadata:
  +      name: work
  +    spec:
  +      accessModes:
  +      - ReadWriteOnce
  +      resources:
  +        requests:
  +          storage: 2Gi
  ```

- **The data would have to move.** Every workspace on `k2-work` would need
  copying onto the host that runs its agents (`work-k2-host-0`). Otherwise
  the host's agents would wake in empty folders, and the app (which now
  reads the host) would show them empty. Afterwards the controller's
  `k2-work` copy is stale and must not be written. With the pool's second
  host, a workspace lives on whichever host its agent was spawned on;
  moving between hosts relies on home sync (P3), which the drain demo
  tested on P4 but not on the owner's K2.
- **What the migration would take, outside rc3:**
  - **Backups:**
    - the controller's volume, with `scripts/kind/backup-controller.sh`;
    - `k2-work` copied in the cluster into `k2-backup-work-<UTC>` (a
      Job on the controller's node mounting both).
  - **Roll:**
    - the claim template change needs StatefulSet `k2-host` deleted first,
      as up.sh says; the host PVCs `data-k2-host-N` stay;
    - roll with `--host-work=own --fs-relay`;
    - a Job on the same node copies `k2-work` into `work-k2-host-0`, with a
      checksum check;
    - then K5's checks (`scripts/kind/k5-check.sh`) against the owner's
      port.
  - **Rollback:** roll again without `--host-work=own --fs-relay`, so the
    host mounts `k2-work` again. Before that, copy `work-k2-host-0` back
    over `k2-work`, so changes made while ON aren't lost. The backups are
    the last resort.
  - **kind-app's roll scripts need,** for ON:
    - a size for the host's work claim (`up.sh` fixes it at 2Gi, but
      `k2-work` is 5Gi);
    - the copy Job, forward and back;
    - a rehearsal of the whole thing on a test namespace first.

### rc3, with the relay OFF

- **The roll's flags stay as kind-app's owner roll has them:**
  `--dist --host-auth=enroll --ptyd=1 --provisioner=kubernetes
  --work-root=/var/lib/k2/work` and the pool, with no `--fs-relay` and no
  `--host-work=own`. The rc3 image carries fs-relay inert: the controller
  has no `K2_FS_RELAY`.
- **What does change with rc3, relay off, on any host with
  `K2_WORK_ROOT`** (the owner's has it):
  - K2's own git on the host is hardened: no hooks or fsmonitor, the
    10 s time limit, refused on a config naming `hook.*`;
  - `worktree add` checks out with libgit2, so git-lfs files stay
    pointers in worktrees K2 creates;
  - a fresh host gets a git identity at start (K5);
  - its Trash goes to `/var/lib/k2/work/.k2-trash`;
  - its listings give a link's own size.

  The app's file and git routes are still served by the controller (relay
  off), so these touch only what the host's own daemon does. None of them
  move data.

### Why OFF for rc3

1. **On the owner's shared volume the relay gives nothing.** It adds a hop,
   new failure modes (host away, App passes, compress/extract) and
   refusals, for files the controller already reads.
2. **ON for real is a storage migration.** A StatefulSet recreate, a data
   copy, a size the roll scripts can't set yet, and a rollback that has to
   copy back: it deserves its own rehearsal and window, not a release roll.
3. **The relay is new.** It was hardened across 13 review rounds and has
   passed K5 on a test namespace, not yet under the owner's daily use.
   rc3 ships it inert, so turning it on later is a flag and a migration,
   not a new build.

## Checks

| Check | Result |
|---|---|
| Unit: route set, path mapping by whole components, query/JSON/form paths, the label | `hosts::fs_relay::tests` 2 tests, `hosts::config_tests` (the label) |
| Unit: which host (placed, the controller's or the host's name, siblings, no path, two hosts → split, host gone → away, shared-volume host → local) | `a_path_goes_to_the_host_its_agents_workspace_is_on` |
| In process: the controller's relay → a fake host link → a real host daemon: read-dir, read-file (paths mapped back), a bad token refused before relaying, a path no host holds stays local, write-file, git changes, stage-all, commit, diff-file | `the_file_browser_and_git_panel_through_the_controller_see_the_hosts_disk` |
| Two processes, separate work roots (`scripts/lab/topology/fs-relay.sh`) | **7/7 PASS** (`lab-runs/fs-relay/20261026T143509Z`); round 6, with `K2_FS_RELAY=1`: **PASS** (`lab-runs/fs-relay/20261026T233729Z`) |

## How to try it

```sh
scripts/lab/topology/fs-relay.sh            # builds k2-daemon and the fake agent, then 7 checks (sets K2_FS_RELAY=1)
cargo test -p k2-daemon --lib fs_relay      # the unit and in-process tests
```

## Status

- First cut done: reads and writes of files and git through the controller
  from the agent's host.
- Next: project create/add on a host (and registering a workspace there, so
  `git init` works through the relay), clone-to unpack, app deploy from a
  folder, uploads and compress/extract jobs; then drop the shared `k2-work`
  on kind (`deploy/kind/p4`).
