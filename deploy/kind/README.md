# K2 on kind, driven from the desktop app

Today's K2 daemon (`K2_ROLE=all`) runs as one pod on the local kind cluster
`k2lab`, with the Claude Code CLI in its image. The K2 desktop app on this Mac
connects to it through a small proxy container on `127.0.0.1:7440`. You sign Claude in from the
app with `/login`. There is no API key, OAuth token or credential Secret.
Lab only: `deploy/kubernetes/` is the end state.

| File | What it is |
|---|---|
| `Dockerfile.agents` | `k2-agents:dev`: `k2-daemon:dev` plus Claude Code 2.1.308 (the native linux binary) at `/usr/local/bin/claude`, with `DISABLE_AUTOUPDATER=1` |
| `agents/kustomization.yaml` | Overlay over `packaging/oci/kind/solo.yaml`: namespace `k2-kind`, image `k2-agents:dev` |
| `Dockerfile.fake`, `fake/` | TEST ONLY: `k2-agents:fake` with the lab's `k2-fake-agent` as every agent CLI, in namespace `k2-kind-fake`, for `scripts/kind/s1.sh` |
| `components/no-k2dev` | Shared by both overlays: `K2_TUNNEL_LEASE=off`, `K2_FEDERATION=off`, relay list and DNS API pointed at a closed local port. Nothing here talks to k2.dev |
| `agents-egress-locked/` | Opt-in: `agents` plus a NetworkPolicy allowing only DNS and TCP 443 out. Blocks frp and other non-443 ports; it can't block k2.dev, which is also 443 |
| `proxy/Dockerfile`, `*/nodeport.yaml` | The Mac's door: `k2-kind-proxy:dev` (alpine 3.20.10 + socat) relays `127.0.0.1:7440` to the K2 Service's NodePort (30740; fake 30741, dist-fake 30742, dist 30743) on Docker's `kind` network |
| `scripts/kind/up.sh`, `down.sh` | One command up, one command down |
| `scripts/kind/forward.sh` | Check (`--status`) or recreate just the door; `--port-forward` for the fallback |
| `scripts/kind/doors.sh` | Every variant's door: start stopped proxies, set `--restart always`, check each port (`--check` changes nothing) |

## 1. Bring it up

```sh
scripts/kind/up.sh
```

The script does the following:

- builds `k2-daemon:dev` if it is missing, or always with `--rebuild-daemon`;
- builds `k2-agents:dev`, runs `kind load` and applies `deploy/kind/agents`;
- waits for the pod and starts the door, the Docker container `k2-kind-proxy`: `127.0.0.1:7440` → node port 30740 → the pod. Docker restarts it (`--restart always`: quitting Docker Desktop stops every container, and `unless-stopped` counts that as a manual stop, so the doors stayed down after a restart on 2026-10-25), and each connection is its own socat child, so one broken stream never drops the others. `kubectl port-forward` dropped the whole forward on one bad stream (seen on 2026-10-24); it remains a fallback, `scripts/kind/up.sh --port-forward`, which runs in its own session, logs to `$TMPDIR/k2-kind/agents/port-forward.log` and reconnects by itself.
- creates a K2 login for the app, user `owner` with the Owner role, and a random password;
- creates a git folder `/var/lib/k2/work/demo` for your first workspace.

It ends with:

```text
  Server URL : http://127.0.0.1:7440
  Username   : owner
  Password   : <24 random characters, shown once>
```

The password is shown only on the run that creates it. Re-running `up.sh` is
safe and keeps the login. `scripts/kind/up.sh --reset-password` sets and
shows a new password.

If the app ever can't reach `http://127.0.0.1:7440`, check the door with
`scripts/kind/forward.sh --status`, and recreate just the door with
`scripts/kind/forward.sh` (or `--port-forward` for the fallback). After
Docker Desktop restarts, `scripts/kind/doors.sh` starts every proxy that
is stopped, without replacing it. None of them builds K2 or applies
anything.

Port 7440 must be free on the Mac. Use `K2_KIND_PORT=7450 scripts/kind/up.sh`
otherwise, and also if your own local K2 daemon happens to use 7440.

## 2. Add the host in the app

The app signs in with a K2 username and password. It never asks for
`daemon.token`: `POST /cli/auth/login` returns a session token, and the app
sends that as `?token=` on every request and WebSocket. `http://` on
`127.0.0.1` is accepted. Only `http` and `https` are allowed, and loopback is
not treated as your local daemon (`src/renderer/lib/connect-validate.ts`
`parseServerUrl`). The app's version check needs `/boot-status`
`phase=ready`. The daemon's version string doesn't have to match the app's.

**Always use `http://127.0.0.1:7440`** (or your `K2_KIND_PORT`). Never use
`https://` or a `*.k2.dev` address: this host has no tunnel, and the app
never routes `http://127.0.0.1` through k2.dev. Don't run `k2 tunnel` in the
pod. Push, publish URLs, custom domains and federation are unavailable here.
`K2_AIRGAP` stays unset on purpose, because it would block `/login`.

1. Open **Settings**. In the group **Network**, click **Connected Servers**.
   Shortcut: open the server menu in the top bar (⌘L) and click
   **Add a server…**, which opens the same page with the form focused.
2. Under **Servers**, click **+ Add a server**. A form titled **Add server** opens.
3. Fill in the fields:
   - **Label (e.g. My Mac Mini)**: `kind k2lab`
   - **K2 Server URL (e.g. http://192.168.1.50:60710)**: `http://127.0.0.1:7440`
   - **Username**: `owner`
   - **Password**: the password `up.sh` printed
   - **Remember password (OS keychain)**: your choice
4. Click **Save**. The button reads **Verifying…** while it signs in. If the
   sign-in fails, the entry is removed again.
5. Saving does not switch to the host. Open the server menu in the top bar
   (⌘L, **Search servers…**) and pick **kind k2lab**. You are already signed
   in, so the switch is silent. If the app ever needs to sign in again, it
   shows **Sign in to kind k2lab** with a username and a password field and
   a **Connect** button.

## 3. Add a workspace in the pod

1. In the sidebar, click **Add Workspace**. On a remote host this opens
   **Open Folder on Host**, which browses the pod's files and starts in its
   home, `/var/lib/k2`.
2. Open `work`, then `demo`, and click **Select this folder**.
3. In **Add Workspace to K2**, click **Add workspace**.

Everything under `/var/lib/k2` is on the pod's volume and survives restarts.

## 4. Sign Claude in (once)

1. In the workspace's tab bar, click **+** (tooltip **New tab**) and choose
   **Claude**. The built-in preset runs `claude --dangerously-skip-permissions`
   in the pod. The daemon finds `/usr/local/bin/claude` on its agent PATH,
   as it would on a Linux box.
2. Claude's first-run screens appear: theme, then the login method.
   *Unverified: the exact screens depend on Claude Code 2.1.308. No real
   claude was run for this runbook.* Choose your Claude account. If Claude
   doesn't offer a login on first run, type `/login`.
3. **What happens to the login URL.** This was checked by reading the code,
   not by clicking:
   - On Linux, Claude opens the URL with `xdg-open`. Inside a K2 session,
     `xdg-open` is the daemon's shim (`~/.k2/bin/xdg-open`, staged by
     `crates/k2-core/src/open_shim.rs` and first on the agent's PATH). The
     shim posts the URL to `/cli/browser/open-url`.
   - The daemon broadcasts an `open_url` event on `/cli/sessions/events`,
     and the app opens it in a **browser tab inside the app** on your Mac.
     This works for a remote host as well as the local one
     (`initOpenUrlBrowserTabs` in `src/renderer/stores/tabs.ts`).
   - That automatic URL sends you back to `http://localhost:<port>/callback`.
     That port is in the pod, not on your Mac, so the last redirect in that
     tab fails. You can close the tab.
4. **Use the fallback.** Claude also prints **Browser didn't open? Use the
   url below to sign in** and a second URL, whose callback is
   `platform.claude.com/oauth/code/callback`. Open that URL: click it in the
   terminal, or copy it into any browser on the Mac. *Unverified: where a
   clicked link opens.* Sign in, then copy the code the page shows.
5. Paste the code into the Claude terminal at **Paste code here if prompted >**
   and press Enter.

The login is stored in `/var/lib/k2/.claude/.credentials.json`, with
`/var/lib/k2/.claude.json` beside it. That is Claude's normal store on
Linux, under the pod's `HOME`, which is the volume. K2's LLM wallet
(`crates/k2-core/src/llm_accounts`) uses the same store for the active login
and keeps any other logins under `/var/lib/k2/.k2/llm-accounts/`, on the
same volume.

Check that it was stored without printing it:

```sh
kind get kubeconfig --name k2lab > "$TMPDIR/k2lab.kubeconfig"
kubectl --kubeconfig "$TMPDIR/k2lab.kubeconfig" -n k2-kind exec k2-solo-0 -- ls -l /var/lib/k2/.claude/.credentials.json
```

## 5. Give it a task

Type the task into the Claude terminal, for example `create a README.md
that says hello from kind`, and press Enter. Output streams to the tab over
`/cli/sessions/grid`. Open more agents with **+ → Claude**.

## 6. Check that the login survives a restart

```sh
kubectl --kubeconfig "$TMPDIR/k2lab.kubeconfig" -n k2-kind delete pod k2-solo-0
kubectl --kubeconfig "$TMPDIR/k2lab.kubeconfig" -n k2-kind rollout status statefulset/k2-solo
```

The proxy needs nothing: the Service's NodePort follows the new pod. The app reconnects. Open a new
**Claude** tab: it should start signed in, with no `/login`.

## 7. Take it down

```sh
scripts/kind/down.sh              # removes the proxy, deletes namespace k2-kind: pod, volume, Claude login
scripts/kind/down.sh --keep-data  # removes the proxy, scales to 0, keeps the volume
```

Then remove **kind k2lab** in **Settings → Connected Servers**.

## Optional: close every port but DNS and 443

```sh
kubectl kustomize --load-restrictor LoadRestrictionsNone deploy/kind/agents-egress-locked \
  | kubectl --kubeconfig "$TMPDIR/k2lab.kubeconfig" apply -f -
```

This adds a NetworkPolicy that denies the pod every egress except DNS and
TCP 443. The NetworkPolicy limits only the pod's outbound traffic, so the
app's inbound connection through the proxy is unaffected (and the fallback
port-forward goes through the kubelet). Verified on the fake variant: port 80 out times out, 443 out
answers, the daemon boots and S1 passes.

What it shows: K2 on kind runs with frp and every other non-443 port closed.
What it does not show: independence from k2.dev. k2.dev is also served on
443, and a plain NetworkPolicy matches IPs and ports, not hostnames.

Remove it with
`kubectl --kubeconfig "$TMPDIR/k2lab.kubeconfig" -n k2-kind delete networkpolicy k2-solo-egress`.

## Distributed variant (`--dist`), lab only

`scripts/kind/up.sh --dist` runs K2 split in two (`deploy/kind/dist`, an
overlay over `deploy/kubernetes/core/{controller,hosts}.yaml`):

- `k2-controller-0` runs `K2_ROLE=controller`, with no agents. The app
  talks only to it, through the proxy on `127.0.0.1:7443` (`--fake`: 7442).
- `k2-host-0` runs `K2_ROLE=host` on `k2-agents:dist`. It dials the
  controller and enrolls with a bootstrap token that `up.sh` generates into
  the Secret `k2-secrets`; the token is never printed and never in git.
  Agents run here, and its own volume holds `HOME`, so the Claude login
  lives on the host.
- `k2-daemon:dist` must be built from a checkout with `k2-dist/dist-link`:
  `scripts/kind/build-daemon.sh <that checkout> k2-daemon:dist`. The
  compile runs in a memory-capped container; `k2-daemon` alone needs about
  5 GB, so use `K2_KIND_BUILD_MEMORY=6g CARGO_BUILD_JOBS=1`.

**Workspaces are shared through a lab-only workaround.** The app browses
the controller's files and sends that path, as is, to the host. So both
pods mount one volume, `k2-work`, at `/var/lib/k2/work` (`work-pvc.yaml`).
On kind it is ReadWriteOnce (local-path), which works only for pods on the
same node. The host therefore has a required podAffinity to the controller
(`host-patch.yaml`), which pins every host to the controller's node. That
is fine for one host on a laptop. It does not scale, and it would break as
soon as a host had to run elsewhere. The real fix is the PRD's portable
agent home (P3), not a shared volume.

Using it from the app is the same as the one-pod setup (sections 2 to 5),
with three differences:

- **Server URL** is `http://127.0.0.1:7443`.
- **The password** is in a file, never printed: `scripts/kind/up.sh --dist
  --password-file=$HOME/.local/state/k2-kind/dist/owner-password` writes it
  there (mode 0600). Read it with `cat` when you add the server.
- **The workspace** is `/var/lib/k2/work/demo` (in the folder picker:
  `work`, then `demo`). It is on the shared volume, so the controller's
  picker and the host's agents see the same path.

`/login` happens in a Claude tab, as in section 4, and the login lands on
the host's volume (`/var/lib/k2-host/.claude`).

Limits of this dist-link cut (k2-dist/dist-link 2c95fc4c2):

- Agent terminals work with the app's own login: the controller relays
  `/cli/sessions/grid` for app session tokens (dist-link f3f2ed2dd), as
  checked by `scripts/kind/app-check.sh --dist`.
- The controller still answers 501 to the LLM login wallet routes
  (Settings → LLMs sign-in) and to `/cli/terminal/*`. Use `/login` inside a
  Claude tab.

Check the app's path without any agent CLI:
`scripts/kind/app-check.sh --dist --password-file=<that file>`. It logs in,
spawns a plain `sh` with the session token, attaches, and shows that the
shell runs in `k2-host-0`.

S1, which uses the owner token, passes:
`scripts/kind/up.sh --dist --fake && scripts/kind/s1.sh --dist`.

## Proof without a real agent (S1)

```sh
scripts/kind/up.sh --fake         # k2-agents:fake in k2-kind-fake on 127.0.0.1:7441
scripts/kind/s1.sh                # S1 through the proxy on 7441 → lab-runs/kind/<time>/
scripts/kind/down.sh --fake
```

## Unverified (needs a click in the app or a real claude)

- Claude Code 2.1.308's first-run and `/login` screens, and that it prints
  the fallback URL next to opening the browser (read from strings in the
  binary, not run).
- That the automatic login tab ends on a failed `localhost` redirect. It
  follows from Claude's redirect URI. If it succeeds instead, skip steps 4
  and 5.
- Whether **Add a server…** works while the app is still on its
  **Connecting…** screen.
