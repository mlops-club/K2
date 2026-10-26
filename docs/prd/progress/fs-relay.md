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
