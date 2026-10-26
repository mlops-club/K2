# Distributed K2 on your own k3s cluster: the owner's runbook (P4 T4)

**Owner-run only.** No worker has run any of this against a k3s cluster or
the kube context `default`. Every step is translated from what ran on the
kind cluster `k2lab`, and each section says what was proven there and what
was not. Untested steps are marked **(untested)**. The gaps are listed at
the end.

What ran on kind, and where the evidence is:

| Proven on kind | Evidence |
|---|---|
| Controller plus hosts, signed enrollment, the pool, `K2_PTYD=1`: the owner's own `k2-kind-dist`, rolled onto wave3 with backup, rehearsal and owner-proof | `docs/prd/progress/kind-enroll.md` (k2-dist/kind-app), `scripts/kind/owner-roll-wave3.sh` |
| The rc1 roll (c59acf7e0): home sync, the login copy to a second host, `POOL_MAX` | `scripts/kind/owner-roll-rc1.sh` (dry run OK; `--go` waits for the coordinator) |
| The lab's S1–S9 and I1–I4 on the pool (rc1, MIN 2 MAX 3) | `lab-runs/kind-p4/pool-g2/`, this branch's `kind-p4.md` |
| Hosts on different nodes with their own volumes; a drain: turn finished, home saved, released | `scripts/kind/drain-demo.sh`, `kind-p4.md` (the move to the other host fails on rc1: see "Hold") |
| Postgres through the Zalando operator, Apps as Deployments, mail (Stalwart) set up by the controller, a Gateway with TLS passthrough | `docs/prd/progress/k8s-capabilities.md` (k2-dist/k8s-capabilities, **not in rc1**) |

## Hold: one host until drain-move lands

On rc1, an agent that is drained off its host (pod deletion, node drain,
the pool's own scale-down) is resumed only when that same host comes back.
Until then its messages wait in the outbox. The fix is on
`k2-dist/drain-move`. Until it is merged, run the pool with
`K2_PROVISIONER_MIN_HOSTS=1` and `K2_PROVISIONER_MAX_HOSTS=1`, and don't
drain the node your host runs on. This is the coordinator's decision for
the kind roll as well.

## Two levels

- **Level A: the rc1 shape.** A controller and a pool of agent hosts, with
  signed enrollment, `K2_PTYD=1`, and home sync on the hosts. Without
  Postgres, mail, Sew, Apps or the front door: those stay off, as on
  `k2-kind-dist`. Everything in this runbook is level A unless it says
  otherwise.
- **Level B: the capabilities (T3).** `K2_CAP_POSTGRES`, `K2_CAP_APPS` and
  `K2_CAP_MAIL` delegated to the cluster, and the front door behind a
  Gateway. This needs a daemon built with `k2-dist/k8s-capabilities` merged,
  which rc1 doesn't have. Sew isn't possible yet: Sew 0.3.0 has no image and
  no TCP management API (see `k8s-capabilities.md`, "Sew on Kubernetes").

## 0. Before you start

Work through one explicit kubeconfig file for your cluster, never the
current context:

```sh
kubectl config view --minify --flatten --context <your-k3s-context> > ~/.kube/k2-k3s.kubeconfig
chmod 600 ~/.kube/k2-k3s.kubeconfig
export K=~/.kube/k2-k3s.kubeconfig
alias kk='kubectl --kubeconfig "$K" -n k2'
```

Check these on the cluster:

| What | How | Why |
|---|---|---|
| Node architecture | `kubectl --kubeconfig "$K" get nodes -o jsonpath='{range .items[*]}{.metadata.name} {.status.nodeInfo.architecture}{"\n"}{end}'` | The images must match it (section 1). A Mac builds arm64 |
| Storage class | `kubectl --kubeconfig "$K" get storageclass` | k3s ships `local-path`, which pins a volume to one node. The kind runs used `local-path` too |
| Pod CIDR | `kubectl --kubeconfig "$K" get nodes -o jsonpath='{.items[*].spec.podCIDR}'` (k3s default cluster CIDR `10.42.0.0/16`) | Level B mail: `K2_STALWART_ALLOWED_IPS` (section 4) |
| Free memory per node | `kubectl --kubeconfig "$K" describe nodes` | About 400 MB per awake Claude CLI (measured). kind ran hosts at 512Mi requested and 3Gi limit |
| Traefik, Gateway API CRDs | `kubectl --kubeconfig "$K" get gatewayclass` | Only for section 6 option (c) and level B |

## 1. Images

No K2 image is published. `ghcr.io/alakazam-211/k2-daemon` and `k2-host`
in `deploy/kubernetes` don't exist, so you build two images and get them
onto the nodes.

**Build** (what kind used; the source is a checkout of the release you run,
e.g. `k2-dist/wave3` at c59acf7e0):

```sh
# The daemon: a memory-capped compile, then a runtime-only image.
scripts/kind/build-daemon.sh <source checkout> k2-daemon:0.47.0-rc1
# The host: the daemon plus the Claude Code CLI (pinned in the Dockerfile; no credential inside).
DOCKER_BUILDKIT=1 docker build -f deploy/kind/Dockerfile.agents \
    --build-arg BASE_IMAGE=k2-daemon:0.47.0-rc1 -t k2-agents:0.47.0-rc1 deploy/kind
```

- `build-daemon.sh` builds the dev profile. For a long-lived install,
  `K2_BUILD_PROFILE=ship packaging/oci/build.sh k2-daemon:<version>` builds
  the shipping profile **(untested on k3s)**.
- Both build **for the Mac's architecture (arm64)**. If your nodes are
  amd64, build on an amd64 machine, or cross-build with
  `docker buildx build --platform linux/amd64` **(untested; a gap)**.
- Tag with a version, never a moving tag like `:dist`. A rollback is then
  just the previous tag.

**Get them onto the nodes.** Either:

- **Import** (no registry): on every node that can run K2 pods,
  `docker save k2-daemon:0.47.0-rc1 k2-agents:0.47.0-rc1 | ssh <node> sudo k3s ctr images import -`.
  Keep `imagePullPolicy: Never` or `IfNotPresent`. This is the k3s
  equivalent of kind's `kind load docker-image`, which is what ran.
- **A registry** you control, named in the k3s `registries.yaml` on each
  node, with `imagePullPolicy: IfNotPresent` and an image pull Secret if it
  is private **(untested)**.

## 2. The manifests

There is no `deploy/k3s` overlay yet (a gap). The shape that ran on kind is
`deploy/kind/dist` plus the switches `scripts/kind/up.sh` adds in a
generated overlay. On k3s you write that overlay yourself. Below is the one
`up.sh --dist --host-auth=enroll --ptyd=1 --provisioner=kubernetes
--max-hosts=1 --work-root=/var/lib/k2/work --login-from=k2-host-0`
generates, adapted for k3s: namespace `k2`, your image tags, no NodePort,
and pull policy `IfNotPresent`.

Create `~/k2-k3s/kustomization.yaml`. `<checkout>` is your checkout's path
*relative to `~/k2-k3s`* (e.g. `../src/k2`): kustomize refuses absolute
directories. Render and apply with `--load-restrictor LoadRestrictionsNone`,
since the overlay reaches outside its own directory:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: k2
resources:
  - <checkout>/deploy/kind/dist                 # controller, host StatefulSet, ConfigMaps, k2-work, NodePort
  - <checkout>/deploy/kind/dist/pool-rbac.yaml  # the pool: scale k2-host, patch only Secret k2-host-enroll
images:
  - {name: k2-daemon, newTag: 0.47.0-rc1}
  - {name: k2-agents, newTag: 0.47.0-rc1}
replicas:
  - {name: k2-host, count: 0}                   # 0 until the first host has its token (step 3)
patches:
  - target: {kind: Service, name: k2-controller-nodeport}
    patch: |-
      $patch: delete
      apiVersion: v1
      kind: Service
      metadata: {name: k2-controller-nodeport}
  - target: {kind: StatefulSet}
    patch: |-
      - {op: replace, path: /spec/template/spec/containers/0/imagePullPolicy, value: IfNotPresent}
  - target: {kind: StatefulSet, name: k2-controller}
    patch: |-
      apiVersion: apps/v1
      kind: StatefulSet
      metadata: {name: k2-controller}
      spec:
        template:
          spec:
            serviceAccountName: k2-controller
            automountServiceAccountToken: true
            containers:
              - name: k2-daemon
                env:
                  - {name: K2_HOST_AUTH, value: enroll}
                  - {name: K2_PROVISIONER, value: kubernetes}
                  - {name: K2_PROVISIONER_HOST_STATEFULSET, value: k2-host}
                  - {name: K2_PROVISIONER_MIN_HOSTS, value: "1"}
                  - {name: K2_PROVISIONER_MAX_HOSTS, value: "1"}   # the hold: 2 or more after drain-move
  - target: {kind: StatefulSet, name: k2-host}
    patch: |-
      apiVersion: apps/v1
      kind: StatefulSet
      metadata: {name: k2-host}
      spec:
        template:
          spec:
            containers:
              - name: k2-host
                env:
                  - {name: K2_PTYD, value: "1"}
                  - {name: K2_WORK_ROOT, value: /var/lib/k2/work}
                  - {name: K2_HOST_AUTH, value: enroll}
                  - name: K2_HOST_NAME
                    valueFrom: {fieldRef: {fieldPath: metadata.name}}
                  - {name: K2_HOST_ENROLL_TOKEN_FILE, value: "/run/secrets/k2-enroll/$(K2_HOST_NAME)"}
                volumeMounts:
                  - {name: k2-host-enroll, mountPath: /run/secrets/k2-enroll, readOnly: true}
            volumes:
              - name: k2-host-enroll
                secret: {secretName: k2-host-enroll, optional: true, defaultMode: 0440}
```

What this shape is, from `deploy/kind/dist`:

- **One controller** (StatefulSet `k2-controller`, its state on
  `data-k2-controller-0`, the app's logins included).
- **Hosts** (StatefulSet `k2-host`; each has `data-k2-host-N` at
  `/var/lib/k2-host`, which holds the Claude login and the agents' homes).
- **The workspaces volume `k2-work`**, shared by the controller and every
  host. It is ReadWriteOnce, so **every K2 pod runs on the controller's
  node** (podAffinity). That fits a single-node k3s, or a node you set
  aside for K2.
- **Across nodes**: the shape with no shared `k2-work`, each host its own
  work volume and hosts on different nodes, is `deploy/kind/p4`. It ran the
  drain demo on kind, but a drained agent's move needs drain-move. Use it
  after the hold ends.
- `components/no-k2dev` turns off the tunnel lease, federation and relays,
  which talk to k2.dev. Leave it in unless you mean to use those.
- Resources are kind-sized: the controller requests 512Mi and is limited to
  2Gi; hosts request 512Mi and are limited to 3Gi. Raise the host limit
  for more awake agents (about 400 MB each). Raise the volume sizes too:
  5Gi for the controller and 10Gi per host on kind.

## 3. Deploy, enroll the first host, the app login

These are `up.sh --dist`'s steps, in its order, without the kind parts.
Secrets come from files or stdin and never go on a command line.

```sh
kubectl --kubeconfig "$K" create namespace k2
# k2-secrets: the controller's bootstrap token file (the dist overlay mounts it; hosts don't use it in enroll mode).
head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' \
  | kk create secret generic k2-secrets --from-file=host-enroll-token=/dev/stdin
kubectl kustomize --load-restrictor LoadRestrictionsNone ~/k2-k3s | kubectl --kubeconfig "$K" apply -f -
kk rollout status statefulset/k2-controller --timeout=600s
kk port-forward svc/k2-controller 7443:7420 &      # the API, for the next steps (section 6)
# One-time token for k2-host-0, into Secret k2-host-enroll. The owner token goes on a pipe.
kk exec k2-controller-0 -c k2-daemon -- cat /var/lib/k2/.k2/daemon.token \
  | python3 -I scripts/kind/enroll.py mint --kubeconfig "$K" --namespace k2 \
      --url http://127.0.0.1:7443 --hosts k2-host-0 --deploy-label k3s
kk scale statefulset k2-host --replicas=1     # from here the pool owns the replica count
kk rollout status statefulset/k2-host --timeout=600s
```

Check that the host is enrolled and linked, and that its key is on its own
volume:

```sh
kk exec k2-host-0 -c k2-host -- sh -c 'curl -fsS http://127.0.0.1:7420/cli/hosts/status'   # "linked":true
kk exec k2-host-0 -c k2-host -- stat -c %a /var/lib/k2-host/.k2/host-key.pem                # 600
```

The pool mints the tokens for any later host itself, before it raises the
replicas. A host reads its token file only at boot.

**The app's login** (an Owner, as `up.sh` makes it). The password goes to a
0600 file and reaches the controller through the environment and stdin,
never an argv:

```sh
(umask 077; python3 -I -c 'import secrets,string; a=string.ascii_letters+string.digits; print("".join(secrets.choice(a) for _ in range(24)))' > ~/.k2-k3s-owner-password)
python3 -I -c 'import json,sys; print(json.dumps({"username":"owner","password":open(sys.argv[1]).read().strip()}))' ~/.k2-k3s-owner-password \
  | kk exec -i k2-controller-0 -c k2-daemon -- sh -c 'curl -sS -X POST -H "Content-Type: application/json" --data-binary @- "http://127.0.0.1:7420/cli/users/add?token=$(cat /var/lib/k2/.k2/daemon.token)"'
printf '{"username":"owner","role":"owner"}' \
  | kk exec -i k2-controller-0 -c k2-daemon -- sh -c 'curl -sS -X POST -H "Content-Type: application/json" --data-binary @- "http://127.0.0.1:7420/cli/users/set-role?token=$(cat /var/lib/k2/.k2/daemon.token)"'
```

## 4. Pod CIDR: `K2_STALWART_ALLOWED_IPS` (level B, mail)

With mail delegated (`K2_CAP_MAIL=external`, Stalwart from
`deploy/kubernetes/core/mail.yaml`, k2-dist/k8s-capabilities), Stalwart
accepts the controller's API key only from `K2_STALWART_ALLOWED_IPS`. Set it
to your cluster's pod CIDR, from section 0. The k3s default
`10.42.0.0/16` is the manifest's default; kind is `10.244.0.0/16`. Empty
means any address: don't. A wrong value fails closed: the controller's
mail setup is refused with 401 and nothing is opened. This ran on kind
(`mail-enable-check.sh`), not on k3s.

## 5. Claude's login on the host volume

The login lives in `$HOME/.claude` on the host's own volume
(`data-k2-host-0`, `HOME=/var/lib/k2-host`). It is never in an image, a
Secret, git or the Mac. On kind, the owner signed in once, and the roll
kept that volume, so the login stayed.

- **First host**: in the desktop app, open an agent on the server and run
  `/login` in its Claude session. Or, from a terminal:
  `kubectl --kubeconfig "$K" -n k2 exec -it k2-host-0 -c k2-host -- claude`,
  then `/login`, then exit. Check it in place without printing it:
  `kk exec k2-host-0 -c k2-host -- stat -c '%s %a' /var/lib/k2-host/.claude/.credentials.json`.
- **More hosts, after the hold**: either `/login` on each host, or the
  `login-seed` init container (`up.sh --login-from=k2-host-0`, rendered in
  `owner-roll-rc1.sh`). A host with no login copies k2-host-0's
  (`.credentials.json`, 0600, and `.claude.json`) from `data-k2-host-0`,
  mounted read-only on the init container only, inside the cluster.
  - It needs every host on k2-host-0's node, which is what this shape's
    podAffinity gives.
  - It was proven on kind with a dummy login.
  - Caveat: each host then refreshes its own copy. If Claude's refresh
    token rotates on use, which is unverified, the first host to refresh
    may leave the others unable to. A separate `/login` per host avoids
    this.
- Changing the deployment never touches `data-k2-host-0`: the StatefulSet
  keeps its name, `serviceName` and claim template `data`. Check this
  before any change (`owner-roll-rc1.sh` dry-run check (a)).

## 6. Exposing the controller to the desktop app

The app needs the controller's API (port 7420, HTTP and websockets). On
kind it was plain HTTP at `127.0.0.1:744x`, through a proxy container to a
NodePort. Across a real network, plain HTTP would carry the app's session
in clear. Options, safest first:

- **(a) `kubectl port-forward`** from the Mac:
  `kubectl --kubeconfig "$K" -n k2 port-forward svc/k2-controller 7443:7420`,
  then add `http://127.0.0.1:7443` in the app (Settings → Connected
  Servers). The traffic is encrypted to the API server. This is the
  fallback that ran on kind (`scripts/kind/port-forward.sh`), and the
  recommended start. It drops on a controller restart, so run it in a
  loop the way `port-forward.sh` does.
- **(b) An Ingress with TLS** (Traefik, k3s's default): a host name, a
  certificate (cert-manager with Let's Encrypt, or your own), and backend
  `k2-controller:7420`. Traefik passes websockets through by default
  **(untested: the app over an Ingress has never run)**. Allow only your
  addresses (a Traefik IP allow-list middleware) if the name is public.
- **(c) The Gateway with TLS passthrough to the controller's own front
  door** (`deploy/kubernetes/core/gateway.yaml`, `K2_CAP_FRONT=listen`,
  :443). This is level B. On kind it ran only with a stand-in backend
  (`gateway-check.sh`). The certificate the controller serves there is the
  front door's, not the app's API.
- **NodePort over plain HTTP**: only on a network you fully trust (one
  machine, or a private LAN), never across the internet.

## 7. Backup and restore

`scripts/kind/backup-controller.sh` ran on kind (the owner roll, and a
restore drill on `k2-kind-dist-fake`). It refuses any namespace but kind's,
so on k3s you do its steps by hand **(a generalized script is a gap)**.
The backup stays inside your cluster, or in your own backup system, never
in git or a log.

Backup:

1. `kk scale statefulset k2-controller --replicas=0`, then wait for the pod
   to go. Its SQLite database is then closed, so the copy is consistent.
   Agents keep running on the hosts.
2. A Job on the same node as `data-k2-controller-0` (a nodeAffinity on
   `kubernetes.io/hostname`, since local-path pins the volume; set no
   `nodeName`, so the new PVC binds) mounts it read-only. It tars it into
   a new PVC `k2-backup-controller-<UTC>`, and writes the archive's
   sha256 and file count next to it. The script's Job is the template.
3. `kk scale statefulset k2-controller --replicas=1`.

Restore:

1. Put back the images the backup was made with (section 8).
2. Scale the controller to 0.
3. A Job checks the archive's sha256, empties `data-k2-controller-0`, and
   untars into a directory it owns, then moves the contents up. A volume
   root's owner and mode aren't the Job's to set: the first version of the
   script failed on that.
4. Scale the controller to 1.
5. **In enroll mode**: the restored database may not know the hosts' keys.
   Mint each host a fresh one-time token (`enroll.py mint --renew <host>`
   after deleting that host's `/var/lib/k2-host/.k2/host-key.pem`) and
   restart its pod. A host reads its token file only at boot.

Host volumes hold the Claude login and the agents' homes. With home sync,
the newest home of each agent is also in the controller's snapshot store,
which the controller backup covers. To back up the login itself, use the
same Job pattern on `data-k2-host-0`, kept in-cluster. Longhorn (plan D3
(b)) would add volume snapshots and let a host's pod move nodes; it is
optional **(untested)**.

## 8. Upgrades and rollback

The pattern `owner-roll-wave3.sh` and `owner-roll-rc1.sh` follow:

1. Dry run: `kubectl kustomize --load-restrictor LoadRestrictionsNone ~/k2-k3s > render.yaml`,
   then `kk diff -f render.yaml`. Check
   that `k2-host` keeps its claim template and `serviceName`, the
   controller keeps its claim, and the pool's MIN and MAX are what you mean.
2. Keep the current images (by version tag) as the rollback.
3. Back up the controller (section 7).
4. Change the image tags in the overlay, render it and `kubectl apply -f` it. The app
   reconnects once, when the controller rolls.
5. Check:
   - `/boot-status` is 200;
   - the hosts are linked by their keys (0600);
   - `data-*` PVC uids are unchanged;
   - the login file's size and mtime are unchanged;
   - a short prompt is answered (`scripts/kind/owner-proof.py --port 7443
     --password-file ~/.k2-k3s-owner-password --kubeconfig "$K" --namespace k2`
     takes any namespace and kubeconfig; it runs your real Claude once).
6. Rollback: the previous tags, a restore of the backup, apply.

## Gaps

| Gap | Effect | Owner of the fix |
|---|---|---|
| A drained agent moves only when its host returns (rc1) | Pool MAX stays 1; don't drain the host's node | `k2-dist/drain-move` (in progress) |
| No published images; no arm64/amd64 multi-arch build tested | Build and import by hand; amd64 nodes need an amd64 build | packaging |
| No `deploy/k3s` overlay; `deploy/kubernetes` isn't runnable as is | Write the overlay above. The end state lacks `K2_ROLE=host`, enroll and the pool's Secret RBAC; its readiness probe `k2-host health` doesn't exist in the image; its controller K2_HOME differs from the daemon's | kind-p4 (an overlay), deploy/kubernetes owner |
| `backup-controller.sh` is kind-only | Backup and restore by hand | kind-p4 |
| App access over Ingress/TLS never run | Use port-forward until it is tried | kind-p4 with the owner |
| Level B not in rc1; Sew has no image or TCP management API | Postgres/Apps/mail stay off; no Sew | k8s-capabilities merge; Sew (decision pending) |
| Claude login copy and token rotation unverified | Prefer `/login` per host | – |
| Resources sized for kind | Raise host limits and volume sizes | owner |
| Shared `k2-work` keeps all K2 pods on one node | Multi-node needs the `deploy/kind/p4` shape, after drain-move | kind-p4 |
