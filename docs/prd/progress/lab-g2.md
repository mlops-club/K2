# k2-dist · lab-g2: gate G2 on solo and fleet (PRD section 3)

Task `lab-g2` (W4), branch `k2-dist/lab-g2`, from `k2-dist/wave2-preview`
(wave 1 + dist-link W0/W1 + outbox W2). It grows the G0 lab
(`docs/prd/progress/lab.md`) into the G2 gate:

```sh
scripts/lab/prove.sh G2 --topology fleet [--chaos seed=N]   # controller + 2 hosts
scripts/lab/prove.sh G2 --topology solo  [--chaos seed=N]   # today's daemon
```

New: the `fleet` topology (backends `procs`, the default, and `compose`),
scenario clients S2 to S9, `--chaos seed=N`, and checkers for invariants I1
to I4. S8 and S9 need dist-link's W3 (host drain, controller re-adoption);
they are written now and stay red until W3 lands.

## What changes

```mermaid
graph TD
    %% ── Colour legend ──────────────────────────────────────────────
    %% Blue = exists today, unchanged · Violet = changed in this task
    %% Green = new in this task · Orange = outside K2 · Slate = files, state
    %% Shapes: rect = process, stadium = files, hexagon = generated report

    %% ── RUNNER · scripts/lab ───────────────────────────────────────
    subgraph runner [Runner · scripts/lab]
        P[prove.sh<br/><sub>G0 solo · G2 solo, fleet</sub>]:::changed
        FS[fleet adapter<br/><sub>topology/fleet.sh · LAB_FLEET_BACKEND</sub>]:::new
        SO[solo adapter<br/><sub>topology/solo.sh · + fleet state file</sub>]:::changed
    end

    %% ── LAB KIT · tests/lab/labkit ─────────────────────────────────
    subgraph kit [Lab kit · tests/lab/labkit · python3 stdlib]
        FL[fleet manager<br/><sub>fleet.py · up, down, kill, restart, monitor</sub>]:::new
        SC[scenarios S2 to S9<br/><sub>g2.py · one client, the app's routes</sub>]:::new
        W1[S4 wake, S5 heartbeat<br/><sub>w1.py · several agent homes</sub>]:::changed
        CH[chaos<br/><sub>chaos.py · seeded kills, chaos.log</sub>]:::new
        INV[invariant checkers<br/><sub>invariants.py · I1 to I4</sub>]:::new
        RW[report writer<br/><sub>report.py · G2 matrix</sub>]:::changed
    end

    %% ── FLEET · procs backend · this Mac ───────────────────────────
    subgraph fleet [Fleet · procs · each process its own HOME=mktemp -d]
        C[k2-daemon K2_ROLE=controller<br/><sub>the one URL clients use</sub>]:::existing
        H1[k2-daemon K2_ROLE=host<br/><sub>host-1 · dials the controller</sub>]:::existing
        H2[k2-daemon K2_ROLE=host<br/><sub>host-2 · dials the controller</sub>]:::existing
        FA[k2-fake-agent as claude<br/><sub>children of the hosts</sub>]:::changed
        TR([traces<br/><sub>daemon K2_TRACE_FILE + fake agent per host</sub>]):::state
    end

    %% ── FLEET · compose backend · Docker, 10g cap ──────────────────
    subgraph comp [Fleet · compose · deploy/compose/compose.lab-fleet.yaml]
        CC[controller · host-1 · host-2<br/><sub>k2-lab-fleet image · Linux build</sub>]:::new
    end

    B{{lab-runs/G2/ts/<br/><sub>report.html · invariants.txt · chaos.log · scenarios/</sub>}}:::new

    P -- up / down --> FS
    P -- up / down --> SO
    FS -- procs --> FL
    FS -. compose, final evidence .-> CC
    FL -- starts, kills, restarts --> C
    FL -- starts, kills, restarts --> H1
    FL -- starts, kills, restarts --> H2
    H1 -- link --> C
    H2 -- link --> C
    H1 -- spawns --> FA
    H2 -- spawns --> FA
    C -. writes .-> TR
    FA -. writes .-> TR
    SC -- HTTP + WS, controller only --> C
    W1 -- HTTP + WS --> C
    CH -- kill CLI, host, controller --> FL
    INV -. reads .-> TR
    P --> SC
    P --> INV
    P --> RW
    RW --> B

    classDef existing fill:#dbeafe,stroke:#2563eb,color:#1e3a5f
    classDef changed  fill:#ede9fe,stroke:#7c3aed,color:#3b0764
    classDef new      fill:#d1fae5,stroke:#059669,color:#064e3b
    classDef external fill:#ffedd5,stroke:#ea580c,color:#7c2d12
    classDef state    fill:#f1f5f9,stroke:#64748b,color:#1e293b
```

## One G2 run

```mermaid
sequenceDiagram
    participant P as prove.sh<br/>(runner)
    participant F as fleet.py<br/>(fleet manager)
    participant S as g2.py / w1.py<br/>(scenario clients)
    participant X as chaos.py<br/>(seeded killer)
    participant K as controller + hosts<br/>(k2-daemon)
    participant I as invariants.py<br/>(checkers)

    Note over P,K: Phase 1: up
    P->>F: up (backend procs: 1 controller, 2 hosts)
    F->>K: start each with K2_ROLE, own HOME, K2_TRACE_FILE
    F->>K: wait boot-status ready, hosts linked
    F-->>P: fleet.json (ports, pids, homes, traces)
    P->>F: monitor (samples live agent CLIs every 0.5 s)

    Note over P,K: Phase 2: scenarios
    P->>S: S1, S2 (no chaos)
    P->>X: chaos seed=N (only with --chaos)
    P->>S: S3 to S9
    X->>F: kill a CLI / a host / the controller at seeded times
    F->>K: SIGKILL, then restart after a seeded pause
    X-->>P: chaos.log
    P->>X: stop (it restores anything still down)

    Note over P,I: Phase 3: settle and judge
    P->>K: wait until every outbox row is final
    P->>I: traces, monitor samples, outbox listings, message ledger
    I-->>P: I1 to I4 verdicts
    P->>F: down
    P->>P: parity suites, I5, flows, report
```

## Scenarios

The client only ever talks to the controller's URL (on solo: the daemon).
Each scenario writes `scenarios/<S>.json` (checks with details) and
`scenarios/<S>.log`. Agents are `k2-fake-agent` as `claude`.

| | Scenario | What the client does | Pass when |
|---|---|---|---|
| S1 | Spawn, attach, type, see output | `labkit/s1.py` (G0), against the controller | 8/8 checks, as at G0 |
| S2 | Hook events land on the right agent | Two agents (fleet: one per host), a prompt typed into each | Every hook each CLI fired (SessionStart, UserPromptSubmit, Stop) reached the controller for that agent's session, and none for the other |
| S3 | `k2 msg` between agents on different hosts | alice on host-1 runs the real `cli/k2 msg bob …` in host-1's environment | The message is `delivered` and is in bob's transcript exactly once |
| S4 | Message wakes a sleeping agent | `w1.py wake`: an enabled agent with no process gets `k2 msg --wake` | Woken on a host, answers; asleep again, a second message resumes the same conversation |
| S5 | Heartbeat fires on a sleeping agent | `w1.py heartbeat`: add, then fire | Spawned on a host, the wakeup prompt answered |
| S6 | Inbox delivery survives restarts | fleet: host-2 (bob's) down, inbox item composed on host-1, the controller restarted, host-2 back, bob's next need | The item is in bob's inbox exactly once, its outbox rows `delivered`. solo: the item survives a daemon restart byte for byte |
| S7 | CLI killed mid-turn, next need resumes | An agent woken by a message with a long turn; its CLI SIGKILLed mid-turn; a second message | The second message resumes it with the same provider session id and the same MEMORY.md bytes, and is answered |
| S8 | Host killed mid-turn, nothing lost silently (fleet) | bob mid-turn on host-2; SIGTERM to host-2; a message to bob while it is down; host-2 restarted | host-2 drains (the turn's Stop before exit), bob is resumed by the replacement with no re-spawn by anyone, and the message is delivered once |
| S9 | Controller restart, agents keep running (fleet) | A client attached to bob; a prompt with a long turn; the controller restarted mid-turn | bob's CLI pid never changes, the turn finishes while the controller is down, the controller re-adopts bob, the reattached client sees the reply, typing and `k2 msg` reach the same pid |

Fake-agent directives (anywhere in a prompt, so they survive `k2 msg`'s
`[from …]` prefix): `[[turn-ms=N]]` sets this turn's length (S7, S8, S9
need a turn long enough to interrupt), `[[remember: text]]` appends to
MEMORY.md like a `remember ` prompt.

## Invariants

Judged after the scenarios from evidence, not from the scenarios' own
checks: daemon traces (`K2_TRACE_FILE`, one per process), fake-agent traces
(one per host, so every CLI event has its host), the fleet monitor's
samples, the outbox listings, and the lab's ledger of every message it sent.

| | Invariant | Checker |
|---|---|---|
| I1 | At most one live placement per agent; epochs | No monitor sample shows two live CLIs for one agent (keyed by its workspace) on any hosts; every `ctl.place` epoch is greater than every earlier one, across controller restarts |
| I2 | Every message reaches a final state | Every message in the ledger, the outboxes or the traces ends `delivered` or `failed` with a reason (fleet: controller hop down, each host's hop up); solo: `k2 msg` answered success, or failure with a reason |
| I3 | A resumed agent keeps its session id and memory byte for byte | For every resumed `cli.exec`: same provider session id as the agent's previous CLI, and the MEMORY.md sha256 it starts with equals the one the previous CLI last recorded |
| I4 | An agent's home is written by one host at a time | No agent has home writes (turns, memory, inbox items) from two hosts while the CLI on one of them is alive |
| I5 | Solo output matches the pre-change baseline | As at G0, against `tests/lab/baselines/G0-solo.json` |

## Chaos

`--chaos seed=N` starts `chaos.py` before S3 and stops it after S9. From
the seed it draws the schedule: every 4 to 12 s a target, a CLI (50 %), a
host (30 %) or the controller (20 %; solo: the daemon, 30 %), SIGKILLed and,
for a daemon, restarted after 1 to 5 s. Which CLI is drawn from the live
ones sorted by workspace. A scenario that is restarting processes itself
(S6, S8, S9) holds the fleet lock; chaos then logs a skip. `chaos.log`
records every step (time, target, pid, what happened); the seed is in the
report, so a failure replays with the same seed.

## How to try it

```sh
scripts/lab/prove.sh G2 --topology fleet --parity none          # scenarios + invariants only
scripts/lab/prove.sh G2 --topology fleet --chaos seed=4117 --parity none
scripts/lab/prove.sh G2 --topology solo --parity none
LAB_FLEET_BACKEND=compose scripts/lab/prove.sh G2 --topology fleet   # Docker: lock + slot, 10g cap
tests/lab/run_tests.sh                                            # the lab's own tests
```

## Status

- Latest, wave3 d34b7292c (drain-move, K2-d): G2 is green with and without
  app activation, under chaos seed 4117, and on the new S8 variant where the
  drained host never returns.
- Linux gate on cfcb1e387: 1 class (b) failure (k2_home), 1 class (a)
  failure (fixed in the gate), and 1 class (c) failure, a T2 race where a
  delivery can beat the resume spawn.

- Milestones: diagrams written; first working slice (fleet procs: S1, S2,
  S3, S5 green; S4 red on a daemon gap); chaos runs classified; G2 on solo;
  G2 on wave2-preview 13e5a5939 (procs, chaos, compose).
- Linux gate on wave2-preview 3912ab2ff: k2-core 3867/0, k2-daemon 5250/1
  (the one failure and five ignored tests are class (b), pre-existing; see
  below). Both wave-1 gate failures are fixed.
- G2 on wave3 (below): no chaos green on procs and compose; chaos on
  636391108 green but for one new K2 red (K2-c, a duplicate after an
  interrupted delivery); hosts on `K2_PTYD=1`: unplace stops the agent, a need
  while its host is drained places it fresh on the other host (K2 gap, not
  ptyd-specific).

### G2 on k2-dist/wave3

Throwaway run branches (never wave3 itself): `k2-dist/lab-g2-wave3-run`
(853157fee + lab-g2) and `k2-dist/lab-g2-wave3b-run` (636391108, the K2-a and
K2-b fixes, + lab-g2). Under `--chaos`, every daemon gets
`K2_DELIVERY_GONE_GRACE_SECS=30` and S8 waits grace + 30 s (ecc9d1edd).

| Run | Base | Result |
|---|---|---|
| procs, no chaos `07-27-32Z` | 853157fee | S1-S9 PASS, I1-I4 pass |
| compose, no chaos `07-46-33Z` | 853157fee | S1-S9 PASS, I1-I4 pass |
| procs, seeds 4117 / 7 / 2026 `07-33-19Z` `07-37-38Z` `07-40-55Z` | 853157fee | I1-I4 pass; reds: K2-a, K2-b, allowed, lab (fixed) |
| procs, seeds 4117 / 7 / 2026 `08-24-52Z` `08-30-21Z` `08-34-48Z` | 853157fee + lab fixes | 4117 green; 7 and 2026: K2-a only (+ one lab red, fixed) |
| procs, seeds 4117 / 7 / 2026 `08-59-21Z` `09-03-17Z` `09-07-34Z` | 636391108 + lab-g2 aefdb4bfa | 4117 and 7 green; 2026: S3 round 5 red (K2-c); I1-I4 pass |
| hosts on `K2_PTYD=1`, no chaos `08-01-41Z` and seed 4117 `08-03-30Z` | 853157fee | all PASS but S8's resume elsewhere (below) |
| `LAB_S8_RESUME_ELSEWHERE=1` without ptyd `08-08-09Z` (control) | 853157fee | the same S8 red: not ptyd |

K2 reds found on wave3 (for the owners):

| | What | Evidence | Status |
|---|---|---|---|
| K2-a | Right after a controller restart, before any host linked back, ensure-pinned-chat answered 400 `spawn failed: no agent host is connected` (spawn answers 503, which the app waits on); `k2 msg --wake` failed `spawn_failed`; heartbeat fire `error` | seed 7 and 2026 round 2 (`07-37-38Z`, `07-40-55Z`) | fixed in 636391108 (gone in every round since) |
| K2-b | A message injected just before chaos SIGKILLed its host was recorded `failed` (`dormant_no_wake`) though the agent had it | seed 4117 S8 round 5 (`07-33-19Z`) | fixed in 636391108 |
| K2-c | A message injected into a CLI that chaos killed mid-turn (host answers `retry`) is delivered again by the gone rule's direct path, which spawns `--resume <session> "<msg>"`: the same prompt twice in one conversation (the host's `duplicate` check is bypassed) | seed 2026 S3 round 5 (`09-07-34Z`): `ctl.deliver` 09:10:11.899, turn 1 09:10:12.055, chaos kill 09:10:12.325, `host.deliver retry` 09:10:12.433, `ctl.msg_gone_direct ok` 09:10:42.371 | open |
| resume elsewhere | A need (the app) for an agent its drained host released for resume, while that host is down, places it on the other host with `--session-id <same id>` (`resumed: false`): a fresh conversation under the old id; the history stays on the drained host | S8 `08-01-41Z`, `08-03-30Z` (3 rounds), `08-08-09Z` (in-process) | open (design: wait for the host, or carry the conversation) |

`K2_PTYD=1` (all 18 / 49 agent sessions `backend: ptyd`): the drain's turn ends
before exit, unplacing stops the agent's CLI though both hosts' k2-ptyd keep
running (as designed; the lab records them in `ptyd-leftovers.txt` and kills
them by pid), the queued message is delivered once, and the drained host's
return never places the agent twice.

Allowed reds (PRD failure table) the lab now tells apart, from `chaos.log`
only: chaos killed the scenario's own agent CLI or host (S8 drain, S9: the
attempt is redone, at most 3, dropped checks kept in `info.redone_after_chaos`),
killed the controller under a spawn in flight (S9), or killed the resumed CLI
before or during the queued message (S8: failing it with a reason is right).
Lab fixes: reattach when a controller restart closes the grid stream (S7, S9),
S3 sends from the other host when chaos left both agents on one, idempotent
routes wait on 502 `is not connected` (b0d0b81ae, fca655129, 2feba07f0,
aefdb4bfa).

### G2 matrix on wave2-preview 13e5a5939 (merged as 319e17dbb)

wave2-preview = wave 1 + W1 + W2 outbox + S4 wake fix + W3 survive +
the outbox recipient-by-workspace-name fix + the fake-agent line fix.
`prove.sh G2 --topology fleet --parity none` (procs), then `--chaos`:

| | no chaos `00-23-13Z` | seed 4117 `00-32-23Z` | seed 7 `00-38-58Z` | seed 2026 `00-45-33Z` |
|---|---|---|---|---|
| S1 spawn, attach, type | PASS | PASS | PASS | PASS |
| S2 hooks on the right agent | PASS | PASS | PASS | PASS |
| S3 k2 msg across hosts | PASS | PASS | PASS | PASS |
| S4 message wakes a sleeping agent | PASS | PASS | PASS | PASS |
| S5 heartbeat on a sleeping agent | PASS | PASS | PASS | PASS |
| S6 inbox survives restarts | PASS | PASS | PASS | PASS |
| S7 CLI killed mid-turn, resumes | PASS | FAIL (K2 placement) | PASS | FAIL (K2 placement) |
| S8 host killed mid-turn | FAIL (drain) | FAIL (drain) | FAIL (drain; crash: lab, fixed) | FAIL (drain) |
| S9 controller restart | PASS | PASS | PASS | PASS |
| S12 | NOT RUN (no client) | | | |
| I1 one live placement, epochs | pass | pass | pass | pass |
| I2 every message final | FAIL: S8's message queued | same | same | same |
| I3 resume keeps session, memory | pass | pass | pass | pass |
| I4 one writer host per home | pass | pass | pass | pass |

Reds, classified:

- **S8 drain (K2, spec)**: S8 has two halves (`g2.py` `s8`). The crash half
  is dist-link W3's definition (SIGKILL: host reported lost, the agent not
  placed elsewhere (I1), the grid ends, on return it spawns again resuming
  its conversation, the interrupted prompt in the transcript): it passes.
  The drain half is the PRD failure table's row for S8 ("SIGTERM starts a
  drain: … wait for each turn's Stop hook … The replacement resumes agents
  that were awake"): the host exits at once on SIGTERM, the turn never ends,
  nothing resumes the agent, and the message sent meanwhile stays queued
  (the I2 red). Not built by W3; for the owner to decide whether S8 needs it.
- **S7 under chaos (K2, placement)**: when the app reopens an agent whose CLI
  died, the controller places the resume by load only. In seeds 4117 and
  2026 it chose the other host, where the conversation does not exist (each
  host has its own home): the CLI started with `--resume <id>` and exited 1
  at once, while `ensure-pinned-chat` answered 200; the client's attach got
  `session … not found in v2 session map`. Without chaos it happens to pick
  the same host. The transcript probe (W1) knows which host ran a project;
  placement does not use it. Portable homes (P3) would also cover it.
- **S8 crash in seed 7 (lab, fixed e3bc70e98)**: S8 killed the host 56 ms
  after the turn started, before the agent recorded the prompt (the fake
  agent writes it after its UserPromptSubmit hook, slower while chaos
  restarts the controller). S8 now crashes only once the prompt is recorded.

Lab changes for this tree: S3 and I2 accept the controller's traced direct
delivery (`ctl.msg_accept` delivered, no outbox row, since the S4 fix) as
final (e74d18e1f); I1 treats the same placement traced again (a spawn
refused while its host is lost) as one placement (aaec7c10b); a spawn
refused because no host is linked is asked again once one is (b6f1177b8).

### Compose run (final evidence, LAB_FLEET_BACKEND=compose)

`lab-runs/G2/2026-10-26T01-11-23Z` (sha 0d825db09; `fleet-compose.txt` in
the bundle): Linux k2-daemon and k2-fake-agent built from `git archive` in
`k2-lab-gate:3` capped at 10g, 3 CPUs, 2 jobs (slot, then the Docker lock,
held for the whole run); image `k2-lab-fleet:<sha12>`; three containers
from `deploy/compose/compose.lab-fleet.yaml`, 3g and 2 CPUs each.

| S1 | S2 | S3 | S4 | S5 | S6 | S7 | S8 | S9 | I1 | I2 | I3 | I4 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| PASS | PASS | PASS | PASS | PASS | PASS | PASS | FAIL (drain) | PASS | pass | fail (S8's message) | pass | pass |

The same as procs without chaos: the only red is S8's drain half and the
message it leaves queued. The first compose try (`00-53-09Z`) found one lab
bug: S3 read the sending daemon's TMPDIR from the state file, which compose
keeps in the compose file (0d825db09).

### Daemon gaps the lab found (for the owners)

| Gap | Scenario | Evidence |
|---|---|---|
| On a host or controller, `k2 msg` hands every message to the outbox and ignores `wake` (`workspace_msg.rs:951`, `delivery_drain::send`); the outbox only delivers to a placed agent, so a sleeping agent is never woken and the message waits for the 7-day TTL | S4 on fleet; I2 | being fixed by the outbox worker |
| The controller's outbox matches the recipient only against placement ids (`delivery_drain.rs:282`): a workspace agent (placed under its project id) addressed by its workspace name, as agents address each other today (`k2 connections list`), never matches, so the message waits for the TTL. On solo the same `k2 msg <workspace>` and inbox compose deliver. The outbox's own test used bare v2 agent names, which solo's `k2 msg` does not accept (`workspace_not_found`) | S3, S6 on fleet; I2 | `lab-runs/dev/r4` (solo: PASS), `r5` (fleet: queued) |
| A host exits at once on SIGTERM: no drain, the turn in progress never ends, nothing resumes the agent | S8 | W3 |
| A restarted controller does not re-adopt running agents (no `ctl.adopt`, placements 0) | S9 | W3 |

### Chaos runs and how each red was classified

`--chaos seed=N`: under chaos the scenario set repeats in rounds for at
least 180 s (`LAB_CHAOS_MIN_S`), so chaos acts even on solo, where S3 to S9
take ~10 s; a scenario passes only if it passed in every round. Every red
was matched to `chaos.log` by time. Classes: **K2** (a daemon gap, table
above), **allowed** (a loss the PRD failure table allows: the turn in
progress when the CLI, or on solo the daemon, dies), **lab** (the lab
client or manager; fixed, commit given).

| Run | Result | Reds and class |
|---|---|---|
| fleet 4117 (`22-04-38Z`, 29 steps: 10 host, 6 controller, 2 CLI kills) | S1-S3, S5-S7 PASS | S4, S8, S9, I2: K2 |
| fleet 7 (`22-12-42Z`) | as 4117, plus S7 | S7: lab: chaos killed S7's CLI 0.1 s after its banner and S7 gave up; it now reopens the agent (the next need) and counts it (3beb44399) |
| fleet 2026 (`22-21-26Z`) | as 4117, plus S6 | S6: lab: its last read of the sender host's outbox raced a chaos restart of that host; it now waits for the host (3beb44399) |
| fleet 4117, first try (`21-55-47Z`) | — | lab: two lab processes wrote back each other's stale pids (chaos "killed" a gone pid, then started a second host-1), and chaos counted host helper CLIs as agents. Fixed: per-process state updates under a file lock, no signal unless the pid runs this run's daemon, agents only (3beb44399) |
| solo, first tries (`22-42-41Z`, `22-46-34Z`) | most rounds red | lab: the client gave up on "connection refused" and 503 `migrating` while chaos restarted the daemon, kept the old token after a restart (a daemon issues a new one each boot, `daemon_main.rs:811`), and retried nothing a dying daemon dropped. Fixed: wait for the daemon as the app does, rebuild each retry with the current token, resend only idempotent requests, never `k2 msg` or inbox compose (cb8ec65e7, 08bfdc7a9) |
| solo 7 (`23-03-46Z`, 2 rounds) | all PASS; I1-I4 PASS | none |
| solo 4117 (`23-07-16Z`, 8 rounds) | S4 red in round 8 | allowed: the solo daemon was killed 7 times in that round; the second wake's CLI died mid-turn. The client now does every w1 turn again when its CLI died first, counted (3a97361e3, 97145c43d) |
| solo 2026 (`23-13-22Z`, 6 rounds) | S5 red in rounds 5, 6; S7 in round 4 | S5: allowed (daemon or CLI killed mid-turn), same fix. S7: lab: chaos killed S7's CLI 0.16 s before S7 did; S7 now accepts a CLI already gone (97145c43d) |
| fleet 7, after those fixes (`23-17-20Z`) | S1, S2, S7 PASS; I1, I3, I4 PASS | S3, S4, S8, S9, I2: K2. S5: allowed (the heartbeat's CLI killed at 23:22:17; this run loaded w1 before the redo fix). S6: lab: chaos killed host-2 0.5 s before placement, so alice and bob both went to host-1 (correct placement); S6 now sends from any other host (6c9e62056) |

I1 (never two live CLIs for one agent; epochs strictly increasing across 6
to 7 controller kills per run), I3 and I4 passed in every chaos run.

### G2 on solo

`prove.sh G2 --topology solo --parity none` (`lab-runs/G2/2026-10-25T22-41-34Z`):
S1 to S7 PASS, S8 and S9 n/a (not on solo in the PRD matrix), I1 to I4
PASS. S12 is NOT RUN (no client yet), so the gate stays red.

### Linux gate fixes (wave-1 gate: k2-daemon 5179 passed, 2 failed)

Both failures were the gate container, not the code (20dfc130d):

- `root_helper::cli_refresh::tests::inspect_names_a_linked_or_writable_directory`
  expects a temp dir root does not own; the gate ran every test as root.
  `test-gate-linux.sh` takes `K2_GATE_TEST_USER=<user>` (root only): the
  workdir is handed to that user and the k2-ptyd build and package tests run
  as it (`runuser`); the root-only allowlisted `cell_egress` run stays root.
  Unset, the script runs as before. `linux_gate.sh` creates `k2gate` (uid
  10001) in the container and sets it.
- `sidecar_cli_integration` `resume_after_restart_cursor_agent`: its fake CLI
  calls `/usr/bin/uuidgen`; `gate.Dockerfile` adds `uuid-runtime`
  (`k2-lab-gate:3`).
- Proven: both pass in the gate run below.

### S10 under heavier chaos on wave3 ec518d8c1 (`LAB_S10_CHAOS=1`)

Before this, S10 held the fleet for most of each round, so chaos was light
in S10 runs (7 kills, 15 steps skipped). With `LAB_S10_CHAOS=1` under
`--chaos`, chaos acts during S10's hold:

- S10 takes the control lock only to SIGKILL its host and to start it
  again.
- Between those two moments it guards that host (`fleet.set_guard`). Chaos
  neither kills nor starts a guarded host and makes its pick among the
  rest, with the same seeded draws.
- Chaos inside the open hold excuses no red: the attempt isn't redone.
- The checks stay the same: one move, never before `K2_HOST_MOVE_AFTER`; a
  resume in the same session after the snapshot is restored, with turn
  two's memory; turn three's message delivered once and answered, or failed
  with a reason after a chaos hit; never doubled; one live CLI; I1–I4.

Runs: procs, `LAB_G2_ONLY=S10 LAB_G2_ROUNDS=5`, with `LAB_S10=1` and
`LAB_S8_RESUME_ELSEWHERE=1`, on throwaway branch `k2-dist/lab-g2-wave3g-run`
(ec518d8c1 + lab-g2, deleted after).

| Seed | `lab-runs/G2/` | Chaos SIGKILLs (host / controller / CLI) | Of those, inside S10's holds | S10 | I1–I4 |
|---|---|---|---|---|---|
| 4117 | `2026-10-27T00-18-03Z` | 45 / 29 / 4 (67 CLI steps skipped: no live CLI) | 25 / 18 / 4 = 47 | 0/5 | pass |
| 7 | `2026-10-27T00-42-54Z` | 48 / 45 / 6 (88 skipped) | 24 / 18 / 4 = 46 | 1/5 | I1–I3 pass; I4's 2 violations were the lab's (below); pass after the fix |

What held in all 10 rounds:

- The agent moved once, never early, and resumed in the same session
  after the snapshot was restored, with turn two's memory and turns one
  and two in its transcript.
- Never two live CLIs, and no message was doubled.
- When n4 found the agent's CLI killed, it failed loudly
  (`dormant_no_wake`).

Every red, classified:

- (c) K2-d follow-up, for outbox: a turn cut by a crash leaves its message
  `delivered` and unanswered. This was 9 of 10 rounds.
  - Turn three's message is redelivered on the new host and acked
    (`ctl.deliver_ack delivered`).
  - Chaos then SIGKILLs that CLI or its host 0.5–12.5 s into the 30 s
    turn.
  - On relink, `ctl.adopt` says `gone`. The row stays `delivered`, and the
    turn is never rerun or answered, though the prompt is in the
    transcript.
  - The only green round is the one where the first kill came at +30.2 s,
    after the turn ended.
  - K2-d fails a cut turn's messages ("interrupted") only when the reaper
    closes a chat. A crash-cut turn (the CLI exits without `Stop`, or
    adopt says `gone`) needs the same treatment.
  - Same gap on the direct wake path (first run, seed 4117 round 4): the
    wake answered `woke: true`, and chaos killed that CLI 0.4 s into its
    turn.
- (a) the lab, all fixed:
  - I2 "accepted without a msg id": my first version sent n4 woken under
    chaos. A woken send answers without an outbox id. n4 is `wake=false`
    again (b0333187c).
  - A synchronous loud failure (`spawn_failed` with a reason) wasn't
    accepted (b0333187c).
  - I4 matched workspaces by string prefix, so a redone attempt's
    `s10-x-2` read as `s10-x`. It now compares whole path components,
    with a regression test (d8c157794).

### G2 on wave3 d34b7292c (drain-move, K2-d)

Throwaway branch `k2-dist/lab-g2-wave3e-run` (d34b7292c + lab-g2 ba2c0240a,
deleted after). All runs are on procs, with `LAB_S10=1` and
`LAB_S8_RESUME_ELSEWHERE=1`.

| Run | `lab-runs/G2/` | Result |
|---|---|---|
| No chaos | `2026-10-26T21-54-44Z` | S1–S10 PASS; I1–I4 pass |
| No chaos, `LAB_NO_ACTIVATE=1` (K2-d: no app activation; no `projects/activate` in the run) | `2026-10-26T22-03-26Z` | S1–S10 PASS; I1–I4 pass |
| Chaos seed 4117 | `2026-10-26T22-05-48Z` | S1–S10 PASS, S3–S10 over 2 rounds; I1 (425 samples, 38 CLIs, 40 placements), I2 (12 messages), I3 (10 resumes), I4 pass |
| S8 only, `LAB_S8_NO_RETURN=1 LAB_NO_ACTIVATE=1`, 3 rounds | `2026-10-26T22-10-07Z` | S8 PASS in 3/3 rounds (`s8_mode: message`); I1–I4 pass |

The new S8 variant (outbox's suggestion, 1d3b21ff7) checks the following:

1. The drained host stays down, and no app need comes.
2. The message sent while the host is down resumes the agent on the other
   host by itself, in the same session with `--resume` (drain-move: a waiting
   message is a need).
3. That message is delivered once.
4. Only then does the host come back, and one CLI stays live for the agent.

Seed matrix on wave3 1816d189f (lab-g2 c68a368c2 merged), on the throwaway
branch `k2-dist/lab-g2-wave3f-run` (deleted after). All runs used `LAB_S10=1`
and `LAB_S8_RESUME_ELSEWHERE=1`.

| Run | `lab-runs/G2/` | Result |
|---|---|---|
| Procs, chaos seed 7 | `2026-10-26T22-17-04Z` | S1–S10 PASS, with S3–S10 over 2 rounds; I1 (627 samples, 38 CLIs, 40 placements), I2 (13 messages), I3 (10 resumes), I4 pass |
| Procs, chaos seed 2026 | `2026-10-26T22-23-36Z` | S1–S10 PASS, with S3–S10 over 1 round; I1–I4 pass |
| Compose (`k2-lab-fleet:1816d189fca4`), `LAB_NO_ACTIVATE=1`, no chaos | `2026-10-26T22-27-15Z` | S1–S10 PASS; I1–I4 pass; no `projects/activate` calls |

With K2-d in, S10 without app activation is green. Before drain-move, the
reaper closed those chats mid-turn.

### Linux gate on wave3 cfcb1e387 (k2-ptyd step, home sync, T2, chaos fixes)

`lab-runs/gate-debug/2026-10-26T15-18-14Z`:

- It ran from an emptied target volume with `K2_GATE_MIN_FREE_GB=12` (as
  approved) and `K2_GATE_LOW_MEMORY=1`.
- The VM had 17G free at the start and 14G at the end.
- The Docker lock was held throughout.
- The Mac hibernated mid-run, and the run survived it.

| Package | Test binaries | Passed | Failed | Ignored |
|---|---|---|---|---|
| k2-core | 17 | 3882 | 0 | 1 |
| k2-daemon | 161 | 5346 | 2 | 9 |
| k2-daemon root-only `cell_egress` | 1 | 1 | 0 | 0 |
| k2-ptyd (first time in the gate) | 5 | 42 | 0 | 0 (+1 target that didn't run, below) |

GATE RED. All 10 ignored tests are on the allowlist now, including the five
added with reasons. Each failure, classified:

- (b) pre-existing: `k2_home_integration`
  `k2_home_holds_the_whole_k2_home_and_home_stays_empty`. It's the same as on
  88d1eddc2 and sits with outbox.
- (a) the gate: `-p k2-ptyd --bin k2-ptyd-test-agent` (`unknown mode
  "--test-threads=3"`).
  - The batched step passed `--bins`, which also runs a bin marked
    `test = false`. That bin is the fake agent, and it refuses harness flags.
  - Fixed in ba2c0240a: the gate now passes `--bin` for each bin that has
    tests, from `cargo metadata`, and leaves out bins with required-features,
    as `--bins` did. Checked in the gate image: k2-core `contract-gen`,
    k2-daemon `k2-mail-helper k2-power-helper`, k2-ptyd `k2-ptyd`.
- (c) new, from P3 T2, for the hosts/outbox owner:
  `hosts::placement_tests::a_portable_agent_on_a_lost_host_moves_resumes_elsewhere_and_its_message_follows`
  (`placement_tests.rs:518`) failed with "expected bob's spawn, got Deliver
  { … epoch: Some(2) … }". host-b got the waiting message before bob's resume
  spawn.
  - `RemoteHost::spawn` reserves the new placement (`remote.rs:1191–1196`,
    `reserve_for`) before it sends the `Spawn` frame (`remote.rs:1218–1220`).
  - The outbox drain (`delivery_drain.rs:658`) delivers to any placement,
    including a reserved one whose spawn hasn't been answered.
  - A real host answers such a `Deliver` with "no agent … here", and a
    rejection fails the message for good (an I2 risk).
  - Suggested fix: the drain should wait for a placement's spawn to be
    answered before delivering to it.
  - The test is unchanged on d34b7292c. There it passed 30/30 on the Mac,
    so the race shows only on the slower Linux gate. No G2 run has shown it
    yet.

### G2 + S10 on wave3 0c6d3c9f7 (P3 T2 turn-end snapshots)

S10 (`LAB_S10=1`, fleet only, opt-in) is the first independent check of
outbox's T2: a host SIGKILLed with its CLI mid-turn, no drain. The agent
must not move before `K2_HOST_MOVE_AFTER` (the lab sets 20 s on the
controller when S10 runs); then `ctl.move_decision`, a resume on the other
host (same session, `--resume`, new epoch) after it restores the last
turn's snapshot (`host.home_restored`); the moved CLI starts with turn
two's MEMORY.md (sha256); turns one and two are in its transcript there;
turn three's message, never acked, is delivered again there once and
answered; I1 when the dead host returns (one live CLI over 15 s, on the
new host); a later message lands there. The fleet's hosts share one work
dir, so the workspace half is outbox's `turn-snapshots.sh`; S10 checks
what each host keeps apart (its HOME).

Throwaway branches `k2-dist/lab-g2-wave3d-run` (ff933ef19) and
`k2-dist/lab-g2-wave3e-run` (0c6d3c9f7 + lab-g2), deleted. Procs,
`LAB_S8_RESUME_ELSEWHERE=1`.

| Run | Tree | Chaos | Result |
|---|---|---|---|
| `2026-10-26T12-02-37Z` | ff933ef19 | 4117, 4 rounds | S1–S9 PASS, I1–I4 pass |
| `2026-10-26T12-07-13Z` | ff933ef19 | 7, 2 rounds | S4 r2, S9 r2 FAIL: chaos exhausted the lab's retry budget (fixed below) |
| `2026-10-26T12-17-31Z` | 0c6d3c9f7 | none | S1–S9 PASS; S10 FAIL (active reaper, below) |
| `2026-10-26T12-30-20Z` | 0c6d3c9f7 | 4117, 2 rounds | S1–S9 PASS; S10 FAIL (same) |
| `2026-10-26T12-40-08Z` | 0c6d3c9f7 | 7 | cut off: the session exited (SIGTERM) mid-run |
| `2026-10-26T12-50-20Z` | 0c6d3c9f7 | none | **S1–S10 PASS, I1–I4 pass** (after 93e599ef2) |
| `2026-10-26T12-53-39Z`, `13-07-59Z` | 0c6d3c9f7 | 7, 4117 | S1–S9 PASS; S10 FAIL (lab timing, below) |
| `2026-10-26T13-14-36Z` | 0c6d3c9f7 | 7, 2 rounds | **S1–S10 PASS, I1–I4 pass**, S10 never redone |
| `2026-10-26T13-19-19Z` | 0c6d3c9f7 | 4117, 2 rounds | **S1–S10 PASS, I1–I4 pass**, S10 never redone |

On the final tree, wave3 897c06c4a + lab-g2 (`k2-dist/lab-g2-wave3f-run`,
deleted), `LAB_S10=1`, `LAB_S8_RESUME_ELSEWHERE=1`:

| Run | Backend | Chaos | Result |
|---|---|---|---|
| `2026-10-26T13-24-38Z` | compose | none | S1–S7, S9, S10 PASS, I1–I4 pass; S8 FAIL once (below) |
| `2026-10-26T13-46-26Z` | procs | 2026, 1 round (7 kills, 15 skipped) | **S1–S10 PASS, I1–I4 pass** |
| `2026-10-26T13-50-31Z` | compose | none | **S1–S10 PASS, I1–I4 pass** |

The compose S8 red, resolved: a lab bug (class c), not the relay. S8
alone on wave3 8da963442 (`LAB_G2_ONLY=S8`, `LAB_G2_ROUNDS`, 9f4ec1ccd):
procs 0/20 red (`2026-10-26T14-17-34Z`), compose 6/10 (`14-27-15Z`). With
`grid_wait` now logging what it applied, every red had the frames (2
snapshots, 3-4 deltas) and the banner, wrapped:
`[from lab-g2 · m_…] s8-while-down-…k2-fake-agent (as claude) · ses` /
`sion <id> · resumed`. On compose the queued message reaches the resumed
CLI's PTY before its banner, which starts mid-row and wraps at 80 columns
over "session <id>"; the lab matched on rows joined by newlines. The agent
answered every time. Fixed by `Grid.has` (a needle split by a soft wrap
counts; c8add7d98): compose 10/10 green (`14-51-31Z`). The relay path,
checked on the way: `agent_host/remote.rs:1799` `open_grid` →
`hosts/link.rs:396` `GridOpen` → `local_grid` (`link.rs:660`, every local
grid message forwarded in order as `GridData`) → `remote.rs:946`
`on_frame` → `hosts/hub.rs:174` `serve_grid`.

The first sighting (seen once, before the repro): after the
drain, the need resumed the agent on host-1 (`--resume`, same session; one
live CLI when host-2 returned; the waiting message delivered once), but the
lab, attached through the controller at 13:42:13.47, got the stream's
`label_initial` and `mode` events and then no grid frame for 90 s. host-1
had accepted the controller's grid stream for the session ("grid stream 33
… for the owner") and sent its first snapshot within 0.09 ms, so the
controller didn't relay it. The rerun on the same tree passed.

`LAB_NO_ACTIVATE=1` (db793378c) skips the app's activate in `open_chat`:
message-only agents, the K2-d variant, for after drain-move lands.

Caveat: S10 holds the fleet for most of a round, so in the last two runs
chaos mostly skipped (seed 7: 4 kills, 26 skipped; 4117: 5 kills, 23
skipped). S1–S9 under fuller chaos on 0c6d3c9f7 is `12-30-20Z` (4117).

Lab fixes on the way (all class c):
- 7bbf819fe: chaos budgets: S4's wake retries up to 3 times (chaos killed
  the retry's CLI 200 ms after it took the prompt); `redo_on_chaos` up to 5
  attempts (S9: chaos hit all three). A redo still needs chaos.log to show
  the scenario's own agent or host was hit.
- 93e599ef2: `open_chat` activates the workspace first (`POST
  /cli/projects/activate`), as the app does. Without it the controller's
  active reaper counts a lab chat as aged out and closes it 30-60 s on,
  even mid-turn (its doc: "the need clock wins even if the agent is
  Working"); S10's long turn was the first to cross that. Also: S10's last
  message is queued (a msg id for I2), and `pkill -f "$ROOT/"` is skipped
  when the root var is empty (`/` would match every process).
- 426411217: S10 under chaos: turn three lasts 30 s and the reply wait
  outlasts its redelivered copy (the chaos gone wait is 60 s); the I1
  sampling and the last message stay under the fleet hold.

For outbox and dist-link (open, K2-d candidate): a queued outbox delivery
doesn't seem to count as a need on the controller. In `12-17-31Z`, turn
three was delivered to the moved agent at 12:27:21 and 12:27:25, and the
reaper armed at 12:27:27 and closed it mid-turn at 12:27:57 (host-1:
release save, CLI reaped, no turn end); the message stays delivered,
unanswered. An agent driven only by messages, with no app attached, can
be closed mid-turn this way.

### G2 on wave3 587c665c8 with home sync (resume elsewhere)

Base: wave3 587c665c8 (21eb64ca9's P3 home sync + dist-link's chaos-fixes2
with K2-b review fixes and K2-c + enroll-reread), merged with lab-g2 on a
throwaway branch `k2-dist/lab-g2-wave3c-run` (deleted after). Procs,
`LAB_S8_RESUME_ELSEWHERE=1`, so hosts get `K2_WORK_ROOT=<fleet work dir>`
(`LAB_HOME_SYNC`, ef81edbdd; outbox's two asks, with `exec_of` = the newest
`cli.exec` by ts).

| Run | Chaos | Rounds | Result |
|---|---|---|---|
| `2026-10-26T11-31-39Z` | none | 1 | S1–S9 PASS, I1–I4 pass |
| `2026-10-26T11-38-19Z` | seed 2026 | 2 | S6 r2 FAIL (lab bug, below); rest PASS, I1–I4 pass |
| `2026-10-26T11-47-16Z` | none | 1 | S1–S9 PASS, I1–I4 pass (after 49298f286) |
| `2026-10-26T11-48-57Z` | seed 2026 | 4 | S6 r4 FAIL (lab, below); rest PASS, I1–I4 pass |
| `2026-10-26T11-53-45Z` | seed 2026 | 2 | S1–S9 PASS, I1–I4 pass (after d50543ed9) |

- S8 resume elsewhere passes in every run: while the drained host is down,
  the need resumes the agent on the other host with the same session
  (home restored there), and once the host is back there is one live CLI
  (sampled 15 s); the message sent while it was down lands exactly once.
- K2-c did not come back: S3 is green in all 8 chaos rounds (it was S3
  round 5 on 636391108; no run here reached a round 5).
- Both S6 reds were the lab (class c), fixed:
  - 49298f286: `traced_final` took the controller's `ctl.msg_accept`
    (status delivered) as a final delivery. Every message a host hands up
    is accepted that way; it is final only for a direct delivery (the
    controller answers with no outbox row, and never sent it down with
    `ctl.deliver`). I2 had the same rule and gets the same fix; I2
    rechecked on five earlier runs, still pass. The real end: --chaos
    SIGKILLed bob's new CLI right after the host answered `retry` (no
    live session yet); nothing placed bob again within 30 s, so the
    controller failed it `recipient_gone`, as the PRD failure table says.
    S6 now accepts that (failed with a reason after --chaos killed bob's
    CLI, and not in his inbox).
  - d50543ed9: the same end when --chaos killed bob's host (with his CLI)
    1.6 s after he reopened there; S6 counts that kill too.

Gate follow-ups (coordinator's decisions, 705987b52): the gate runs
`cargo test -p k2-ptyd` (ptyd's own tests; ptyd changed in W3b) and checks
its ignored tests against the allowlist; the five ignored tests above are
allowlisted, each with its reason (root networking or useradd, a throwaway
systemd box or real Postgres majors, and the `apps::admission` doctest,
which is a `text` formula block, not code). Replayed on the 88d1eddc2
logs, every ignored test is on the list. Not yet run on Linux with
k2-ptyd. The `k2_home` failure is outbox's (k2-core `cli_stage`).

### Linux gate on wave3 88d1eddc2 (k8s, provisioner, home code)

`lab-runs/gate-debug/2026-10-26T09-26-12Z`, same setup as on 3912ab2ff
(below), from an emptied target volume at the default 20G free floor
(22G at start, 20G at the end), the Docker lock held throughout.

| Package | Test binaries | Passed | Failed | Ignored |
|---|---|---|---|---|
| k2-core | 17 | 3868 | 0 | 1 (allowlisted) |
| k2-daemon | 162 | 5311 | 1 | 9 |
| k2-daemon root-only `cell_egress` | 1 | 1 | 0 | 0 |

GATE RED, with nothing new: every failure is class (b), the same as on
3912ab2ff.

- Failed: `k2_home_integration`
  `k2_home_holds_the_whole_k2_home_and_home_stays_empty` (`$HOME` gets
  `.local/bin/k2`, the `cli_stage.rs` fallback; see below).
- Ignored but not allowlisted: the same five tests listed below.
- The 61 new k2-daemon tests and the 1 new k2-core test pass, and there are
  no compile errors.
- `k2-ptyd` changed (3 files), but the gate only builds its binary for
  `ptyd_backend`; the gate doesn't run k2-ptyd's own tests.

### Linux gate on wave2-preview 3912ab2ff (W5 included)

`lab-runs/gate-debug/2026-10-26T05-09-20Z`, through `scripts/lab/lib/linux_gate.sh`: slot,
then the Docker lock; `k2-lab-gate:3` at `--memory=10g`, 3 CPUs;
`K2_GATE_LOW_MEMORY=1` (1 build job, 256 codegen units); `K2_GATE_BATCH=20`;
tests as `k2gate`, the root-only allowlisted run as root.

| Package | Test binaries | Passed | Failed | Ignored |
|---|---|---|---|---|
| k2-core | 17 | 3867 | 0 | 1 (allowlisted) |
| k2-daemon | 162 | 5250 | 1 | 9 |
| k2-daemon root-only `cell_egress` | 1 | 1 | 0 | 0 |

- `root_helper::cli_refresh::...inspect_names_a_linked_or_writable_directory`
  and `sidecar_cli_integration` `resume_after_restart_cursor_agent` (the
  wave-1 gate's two failures) pass.
- Failed, class (b) pre-existing: `k2_home_integration`
  `k2_home_holds_the_whole_k2_home_and_home_stays_empty`: `$HOME` gets
  `.local/bin/k2`. On Linux the daemon stages the `k2` CLI at
  `/usr/local/bin/k2` and falls back to `$HOME/.local/bin/k2` when that is
  not writable (`k2-core/src/cli_stage.rs`, `stage_cli`, since 33b8f82ec),
  even under `K2_HOME`. As root (every earlier gate) the canonical path is
  writable, so the fallback never ran; as an unprivileged user it does. The
  code and the test are unchanged since the wave base 2987a7576. For the
  owner: under `K2_HOME`, stage the fallback inside `K2_HOME` or skip it.
- Ignored but not allowlisted, class (b): the five tests listed below.

Three gate bugs found on the way (all class (a), fixed in this branch):

| Try | What happened | Fix |
|---|---|---|
| 1 | the k2-daemon lib-test rustc was SIGKILLed at 10g, compiled in parallel with the integration tests | `LAB_GATE_LOW_MEMORY=1` (fewer jobs; the cap is unchanged) |
| 2 | `ld: final link failed: No space left on device`: `cargo test -p k2-daemon` links all 157 integration binaries before it runs one (target volume 34G, Docker VM disk 94G shared) | `K2_GATE_BATCH=N` runs the lib and bins, then N integration targets per step, then the doctests, and removes the linked executables after each step; same tests, same flags (33efc28d2). The end-of-run trim (ab4996747) keeps the volume at ~12G between runs |
| 3 | 115 test binaries and every spawned test daemon aborted on a stack overflow: low-memory mode built at opt-level 0, where k2-daemon's async stacks outgrow the thread stack (the profile says 1) | low-memory mode keeps the profile's opt-level and cuts only jobs and codegen units (b21e9fde6) |

Class (b), pre-existing, for the owner: ignored tests that are not on the
gate's allowlist (each is ignored on base; the allowlist is unchanged since
base). The gate fails on them until each is allowlisted or run:

- `compute_tenant_guard::tests::a_headless_daemons_cell_socket_is_closed_to_tenants`
- `compute_tenant_guard::tests::a_tenant_users_loopback_client_is_found`
- `crates/k2-daemon/src/apps/mod.rs - apps::admission (line 42)` (doctest)
- `sql::pgmove::real_move::real_move_16_to_17_rollback_forget_and_refusals`
- `sql::tests::real_pg_majors::real_pg_majors_side_by_side`

### First working slice: `prove.sh G2 --topology fleet --parity none`

Run `lab-runs/G2/2026-10-25T21-30-01Z` (sha 8971f54d2, procs backend):

| Scenario | Result | Why |
|---|---|---|
| S1 | PASS | as at G0, through the controller |
| S2 | PASS | each agent's SessionStart, UserPromptSubmit and Stop reached the controller (`ctl.hook`, `attrs.pane`) for its own session; agents on host-1 and host-2 |
| S3 | PASS | real `cli/k2 msg` on host-1 → controller outbox `delivered` → bob's transcript on host-2 once |
| S4 | **FAIL** | daemon gap: on a host or controller, `k2 msg` hands every message to the outbox and ignores `wake` (`workspace_msg.rs` `deliver_live_with_receipt`, `delivery_drain::send`); the outbox only delivers to a placed agent, so a sleeping agent is never woken and the message waits for the 7-day TTL. dist-link's W1 had S4 green before the outbox merged on top |
| S5 | PASS | heartbeat fire spawns on a host, wakeup answered |
| S6 | PASS | inbox item composed on host-1 while host-2 was down, controller restarted, host-2 back, bob's next spawn: item in bob's inbox once, outbox rows delivered |
| S7 | PASS | CLI SIGKILLed mid-turn; the app's next ensure-pinned-chat resumes the same session id (`--resume`), MEMORY.md sha256 and bytes unchanged |
| S8 | FAIL (W3) | the host exits at once on SIGTERM (no drain: the turn never ends), nothing resumes the agent, the message waits queued |
| S9 | FAIL (W3) | the agent keeps running and finishes its turn while the controller is down (both PASS); the restarted controller does not re-adopt it, so the grid, typing and `k2 msg` can't reach it |
| I2 | FAIL | the S4 messages and S8's message stay `queued` |

I1, I3, I4 were NOT RUN in that run (the traces landed under `topology/`;
fixed). In the scenario-level runs before it (`lab-runs/dev/r2`, `r3`): I1
pass (502 samples, 4 placements, epochs strictly increasing), I3 pass (S7's
resume kept the session id and MEMORY.md sha256), I4 pass.

Lab fixes found by the first runs: the fleet manager's pid pipe stayed open
for the daemon's lifetime (fd 3 now closed for the daemon); helper CLIs a
host starts itself (`codex app-server`, `cursor agent stdio`) inherited the
daemon's cwd and looked like one agent on two hosts (daemons now get their
own cwd, and the checkers count only CLIs with a pane).
