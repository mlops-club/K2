# Progress: app-reconnect

Branch `k2-dist/app-reconnect` (from `k2-dist/wave2-preview` 0206a21da), worktree `../app-reconnect`. The fix is f56bb7abe, and the long-outage test was tightened in a392739e6. The coordinator verified the branch for merge.

## The bug

After `k2-kind-dist` was rolled, the desktop app showed "Reconnecting to kind dist… retrying automatically" for more than 100 s and never recovered. Both pods had restarted on the same volumes, so the session token was still valid. Switching to another server and back connected at once, with no sign-in.

## Cause (`src/renderer/components/ConnectionGate.tsx`)

The fault is in the soft-poll's wedge detector:

1. A failure run longer than 60 s keeps the failure clock running until two OK probes in a row.
2. Every failed poll stamped a "flap".
3. So the first OK `/boot-status` after the outage still counted as both a sustained fail and a flap.
4. The gate asked the out-of-webview arbiter, which said `ready`.
5. The gate took that as a poisoned webview connection pool. It called `window.location.reload()` and returned without scheduling another tick.
6. If the reload doesn't take, the poll loop is gone, and only a host switch, which starts a new effect, brings it back.

The server side was fine. `scripts/kind/reconnect-check.sh --dist --fake` showed the same session token reopening `/cli/sessions/events` 7 s after both pods were deleted. whoami returned 200 and the host linked again after 9 s.

## Fix

- A sustained fail consults the arbiter only while the current probe fails.
- Flaps are stamped only on a transition from `connected`.
- After the wedge auto-reload, a fallback tick is scheduled.
- Each tick runs under a 45 s watchdog with a catch, so a request that never settles (abort ignored) or an exception can't end the loop.
- While the window is hidden, polling continues unless recovery is `connected`.

## Tests

`ConnectionGate.reconnect.test.tsx` drives the real gate under fake timers:

| Case | Old gate (3 runs) | Fixed gate (3 runs) |
|---|---|---|
| Server refused for 63, 71, 79, 87 or 95 s, then back with a new `instanceId` | 63 s and 95 s fail; the rest pass | all pass |
| Requests hang, then are refused, then the server is back | passes | passes |
| A request that never settles | fails | passes |
| Window hidden through the whole outage | fails | passes |

About the long-outage case:

- The old gate's mistake happens only when the server's return lands on the poll where the arbiter's 30 s throttle has run out, which is one poll in four.
- My first version of the test left that to random jitter. So it passed on the old gate in some runs: the coordinator saw 4 of 4 passes, while I had seen 3 of 3 failures.
- It now fixes the jitter and returns the server at five outage lengths one poll apart, so one of them always lands on the arbiter's poll.
- Each case asserts that the arbiter never saw `ready`, that recovery reaches `connected`, and that the health poll keeps running afterwards.
- On the old gate, the failing cases are the reload path: `location.reload()` does nothing here, as when a reload doesn't take, and the loop ends.

The hang case passes on the old gate too; it stays as a regression guard.

`ConnectionGate.policy.test.ts` adds the pure `shouldConsultArbiter` case.

Full renderer suite: 7916 of 7917 pass. The one failure is a Zen widgets test that timed out under load; on its own it passes with and without the fix.

How the tests were run: bun isn't installed, so `npm install --ignore-scripts --no-package-lock --legacy-peer-deps`, then `@rolldown/binding-darwin-arm64` and `@testing-library/dom@10.4.1` added. Node 22.10 needs `NODE_OPTIONS=--experimental-require-module` for jsdom 29.

The installed K2.app was not rebuilt or touched; the owner gets the fix with their next app build.

## Kind side (`k2-dist/kind-app`, 562314d89)

- `scripts/kind/reconnect-check.sh [--dist] [--fake] --password-file=PATH`. It refuses the owner's variants unless `--restart-live` is given.
- `up.sh` seeds `hasCompletedOnboarding` and `theme: "dark"` into the agent pod's `$HOME/.claude.json`, only for keys that are absent.
  - It was tested in a throwaway `k2-agents:dist` container: absent file, an existing theme kept, idempotent, mode kept.
  - It was not run against the owner's pods, which the coordinator had already patched by hand.
