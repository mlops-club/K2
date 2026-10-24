# K2 on kind, driven from the desktop app

Today's K2 daemon (`K2_ROLE=all`) runs as one pod on the local kind cluster
`k2lab`, with the Claude Code CLI in its image. The K2 desktop app on this Mac
connects to it through `kubectl port-forward`. You sign Claude in from the
app with `/login`. There is no API key, OAuth token or credential Secret.
Lab only: `deploy/kubernetes/` is the end state.

| File | What it is |
|---|---|
| `Dockerfile.agents` | `k2-agents:dev`: `k2-daemon:dev` plus Claude Code 2.1.308 (the native linux binary) at `/usr/local/bin/claude`, with `DISABLE_AUTOUPDATER=1` |
| `agents/kustomization.yaml` | Overlay over `packaging/oci/kind/solo.yaml`: namespace `k2-kind`, image `k2-agents:dev` |
| `Dockerfile.fake`, `fake/` | TEST ONLY: `k2-agents:fake` with the lab's `k2-fake-agent` as every agent CLI, in namespace `k2-kind-fake`, for `scripts/kind/s1.sh` |
| `scripts/kind/up.sh`, `down.sh` | One command up, one command down |

## 1. Bring it up

```sh
scripts/kind/up.sh
```

The script does the following:

- builds `k2-daemon:dev` if it is missing, or always with `--rebuild-daemon`;
- builds `k2-agents:dev`, runs `kind load` and applies `deploy/kind/agents`;
- waits for the pod and keeps a port-forward to `127.0.0.1:7440` running in the background. It reconnects after a pod restart.
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

The port-forward reconnects on its own. The app reconnects. Open a new
**Claude** tab: it should start signed in, with no `/login`.

## 7. Take it down

```sh
scripts/kind/down.sh              # deletes namespace k2-kind: pod, volume, Claude login
scripts/kind/down.sh --keep-data  # stops the port-forward, scales to 0, keeps the volume
```

Then remove **kind k2lab** in **Settings → Connected Servers**.

## Proof without a real agent (S1)

```sh
scripts/kind/up.sh --fake         # k2-agents:fake in k2-kind-fake on 127.0.0.1:7441
scripts/kind/s1.sh                # S1 through the port-forward → lab-runs/kind/<time>/
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
