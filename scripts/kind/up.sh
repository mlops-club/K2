#!/usr/bin/env bash
# K2 on the kind cluster k2lab, ready for the desktop app on this Mac
# (deploy/kind/README.md has the app steps).
#
#   scripts/kind/up.sh [--dist [--hosts=N] [--ptyd=0|1] [--host-auth=token|enroll]
#                             [--provisioner=static|kubernetes [--max-hosts=N]]
#                             [--work-root=PATH] [--login-from=POD] [--max-awake=N]
#                             [--host-work=shared|own] [--move-after=S] [--idle-after=S] [--fs-relay]] [--fake] [--rebuild-daemon]
#                      [--reset-password] [--port-forward] [--password-file=PATH] [--render-only=PATH]
#
# 1. builds k2-daemon:dev (scripts/kind/build-daemon.sh: a memory-capped
#    compile, then a runtime-only image) when it is missing, or always with
#    --rebuild-daemon; then k2-agents:dev (deploy/kind/Dockerfile.agents:
#    the daemon plus the Claude Code CLI) and the proxy image;
# 2. loads it into k2lab and applies deploy/kind/agents (namespace k2-kind);
# 3. waits for the pod, then opens the Mac's door: a socat container
#    (k2-kind-proxy, deploy/kind/proxy) from 127.0.0.1:7440 to the
#    Service's NodePort (30740), restarted by Docker; --port-forward uses
#    the old kubectl port-forward loop instead (scripts/kind/port-forward.sh);
# 4. creates the K2 Connect login the app signs in with (an Owner), once,
#    and prints the URL, the username and, when it set one, the password.
#
# --fake deploys the TEST variant instead (k2-agents:fake, namespace
# k2-kind-fake, port 7441): the lab's k2-fake-agent under every agent CLI
# name, for scripts/kind/s1.sh. --reset-password sets a new password for
# the existing login. --password-file=PATH writes a new password to PATH
# (mode 0600) and prints only the path, never the password. K2_KIND_PORT and
# K2_KIND_USER override the port and the username (default "owner").
#
# --dist deploys distributed K2 instead (deploy/kind/dist: a controller and
# one agent host, namespace k2-kind-dist, port 7443; with --fake,
# k2-kind-dist-fake on 7442). It also creates the Secret k2-secrets with a
# fresh host bootstrap token, once, from stdin: never printed, never in git.
# --hosts=N (default 1) runs N agent hosts (k2-host-0 .. k2-host-N-1), each
# with its own HOME volume, so its own Claude login. They all mount the one
# shared workspaces volume (k2-work), which is ReadWriteOnce: every pod must
# be on the controller's node (podAffinity), so N hosts share one node's
# CPU and memory. Scaling down keeps the removed hosts' volumes.
# --ptyd=0|1 sets K2_PTYD on the hosts (default KIND_PTYD_DEFAULT in
# lib.sh, else K2_KIND_PTYD): agent PTYs in k2-ptyd. Changing it rolls them.
# --host-auth=enroll turns on signed host enrollment (K2_HOST_AUTH=enroll;
# the owner's k2-kind-dist too since the owner's decision of 2026-10-26,
# docs/prd/progress/kind-enroll.md): up.sh mints a one-time token per new
# host into the Secret k2-host-enroll (scripts/kind/enroll.py; never
# printed); each host keeps its key on its own volume and relinks by it.
# Without the flag a run goes back to the bootstrap token (token).
# --provisioner=kubernetes (needs --host-auth=enroll; a daemon with the P4
# host pool, wave3) lets the controller run StatefulSet k2-host itself
# (K2_PROVISIONER=kubernetes, the rights in deploy/kind/dist/pool-rbac.yaml).
# Its floor and ceiling are both --hosts (K2_PROVISIONER_MIN_HOSTS and
# _MAX_HOSTS): never fewer hosts than that, and never a host added that a
# later idle scale-down could take away with its agents' homes on it
# (home sync, P3 T1, is not in yet). static (the default) is as before.
# --max-hosts=N (with --provisioner=kubernetes; default --hosts) raises the
# ceiling only: the pool starts at --hosts and may add hosts up to N. Use it
# only with home sync (--work-root), so a scale-down saves agents' homes.
# --work-root=PATH sets K2_WORK_ROOT on the hosts (home sync, P3 T1: agents
# whose cwd is under it are saved on release and restored on spawn). The
# workspaces volume is mounted at /var/lib/k2/work.
# --host-work=own gives each host its own workspaces volume (a claim
# template "work" at /var/lib/k2/work) instead of the shared k2-work, which
# the controller (and the lab's runner) keep. The hosts stay on the
# controller's node (the runner mounts their HOME volumes). A claim
# template can't change on a live StatefulSet: switching needs k2-host
# deleted first (its PVCs stay). shared (the default) is as before.
# --move-after=S sets the controller's K2_HOST_MOVE_AFTER (seconds before a
# lost host's portable agents move; the daemon's default is 300).
# --idle-after=S sets the controller's K2_PROVISIONER_IDLE_AFTER (seconds a
# host above the pool's minimum stays idle before it is drained and scaled
# away; the daemon's default is 900).
# --fs-relay turns fs-relay on in the controller (K2_FS_RELAY=1, and its
# own K2_WORK_ROOT=/var/lib/k2/work, where its workspaces volume is): the
# app's file browser and git panel are served from the disk of the host
# an agent runs on (docs/prd/progress/fs-relay.md, K5). It needs hosts on
# their own work volumes (--p4 or --host-work=own) that say so
# (--work-root). Off (the default), the controller is as before.
# --max-awake=N sets K2_HOST_MAX_AWAKE on the hosts (the end state's 100):
# how many awake agents a host takes before the pool adds one.
# --login-from=POD (e.g. k2-host-0) gives every host an init container that
# mounts POD's HOME volume (data-POD) read-only and, when the host has no
# Claude login of its own yet, copies POD's (.claude/.credentials.json,
# 0600, and .claude.json) onto the host's own volume. Inside the cluster
# only: nothing is read out or printed. A host that has a login keeps it;
# POD itself is skipped. The volumes are ReadWriteOnce, which works because
# every host sits on the controller's node (above).
# --p4 deploys the P4 test variant (TEST ONLY; implies --dist --fake;
# deploy/kind/p4, namespace k2-kind-p4, port 7444): dist-fake's lab
# hosts, but each with its own workspaces volume (no shared k2-work, P3
# D9), on different worker nodes, never on a node labelled
# k2.dev/p4-drained (scripts/kind/drain-demo.sh). scripts/kind/p4-up.sh
# runs it with the pool and home sync.
# --render-only=PATH builds nothing and applies nothing: it writes the
# manifests a --dist run would apply, at its final host count, to PATH
# (for `kubectl diff`), and exits.
#
# Uses only k2lab, through an explicit kubeconfig; never the current kube
# context. No credential goes into git, an image or a log: the owner signs
# Claude in with /login from the app, and the daemon token is read into
# memory only, never printed or put on a command line.
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"

fake=0 dist=0 rebuild_daemon=0 reset_password=0 door=proxy password_file= hosts=1 ptyd= host_auth=token
provisioner=static render_to= max_hosts= work_root= login_from= p4=0 max_awake= host_work=shared move_after= idle_after= fs_relay=0
for a in "$@"; do
    case "$a" in
        --fake) fake=1 ;;
        --dist) dist=1 ;;
        --p4) p4=1 dist=1 fake=1 ;;
        --rebuild-daemon) rebuild_daemon=1 ;;
        --reset-password) reset_password=1 ;;
        --port-forward) door=port-forward ;;
        --password-file=*) password_file=${a#--password-file=} ;;
        --hosts=*) hosts=${a#--hosts=} ;;
        --ptyd=0|--ptyd=1) ptyd=${a#--ptyd=} ;;
        --host-auth=token|--host-auth=enroll) host_auth=${a#--host-auth=} ;;
        --provisioner=static|--provisioner=kubernetes) provisioner=${a#--provisioner=} ;;
        --render-only=*) render_to=${a#--render-only=} ;;
        --max-hosts=*) max_hosts=${a#--max-hosts=} ;;
        --work-root=*) work_root=${a#--work-root=} ;;
        --login-from=*) login_from=${a#--login-from=} ;;
        --max-awake=*) max_awake=${a#--max-awake=} ;;
        --host-work=shared|--host-work=own) host_work=${a#--host-work=} ;;
        --move-after=*) move_after=${a#--move-after=} ;;
        --idle-after=*) idle_after=${a#--idle-after=} ;;
        --fs-relay) fs_relay=1 ;;
        -h|--help) sed -n '2,91p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "up.sh: unknown argument $a (see --help)" >&2; exit 2 ;;
    esac
done
case "$hosts" in ''|*[!0-9]*|0) echo "up.sh: --hosts=N takes a number from 1" >&2; exit 2 ;; esac
if [ "$hosts" != 1 ] && [ "$dist" = 0 ]; then
    echo "up.sh: --hosts=N needs --dist (the solo pod is its own host)" >&2; exit 2
fi
if [ -n "$ptyd" ] && [ "$dist" = 0 ]; then
    echo "up.sh: --ptyd needs --dist" >&2; exit 2
fi
if [ "$host_auth" = enroll ] && [ "$dist" = 0 ]; then
    echo "up.sh: --host-auth=enroll needs --dist" >&2; exit 2
fi
if [ "$provisioner" = kubernetes ] && { [ "$dist" = 0 ] || [ "$host_auth" != enroll ]; }; then
    # The pool mints each new host's enroll token itself.
    echo "up.sh: --provisioner=kubernetes needs --dist --host-auth=enroll" >&2; exit 2
fi
if [ -n "$render_to" ] && [ "$dist" = 0 ]; then
    echo "up.sh: --render-only needs --dist" >&2; exit 2
fi
max_hosts=${max_hosts:-$hosts}
case "$max_hosts" in ''|*[!0-9]*) echo "up.sh: --max-hosts=N takes a number" >&2; exit 2 ;; esac
if [ "$max_hosts" != "$hosts" ] && [ "$provisioner" != kubernetes ]; then
    echo "up.sh: --max-hosts needs --provisioner=kubernetes" >&2; exit 2
fi
if [ "$max_hosts" -lt "$hosts" ]; then
    echo "up.sh: --max-hosts=$max_hosts is below --hosts=$hosts" >&2; exit 2
fi
case "$work_root" in ''|/*) ;; *) echo "up.sh: --work-root takes an absolute path" >&2; exit 2 ;; esac
case "$max_awake" in ''|[1-9]|[1-9][0-9]|[1-9][0-9][0-9]) ;; *) echo "up.sh: --max-awake takes a number from 1" >&2; exit 2 ;; esac
[ -z "$max_awake" ] || [ "$dist" = 1 ] || { echo "up.sh: --max-awake needs --dist" >&2; exit 2; }
case "$move_after" in ''|[1-9]|[1-9][0-9]|[1-9][0-9][0-9]) ;; *) echo "up.sh: --move-after takes seconds from 1" >&2; exit 2 ;; esac
case "$idle_after" in ''|[1-9]|[1-9][0-9]|[1-9][0-9][0-9]|[1-9][0-9][0-9][0-9]) ;; *) echo "up.sh: --idle-after takes seconds from 1" >&2; exit 2 ;; esac
[ -z "$idle_after" ] || [ "$provisioner" = kubernetes ] || { echo "up.sh: --idle-after needs --provisioner=kubernetes" >&2; exit 2; }
{ [ "$host_work" = shared ] && [ -z "$move_after" ]; } || [ "$dist" = 1 ] || { echo "up.sh: --host-work and --move-after need --dist" >&2; exit 2; }
[ "$host_work" = shared ] || [ "$p4" = 0 ] || { echo "up.sh: --p4 hosts already have their own work volumes" >&2; exit 2; }
if [ "$fs_relay" = 1 ]; then
    [ "$dist" = 1 ] || { echo "up.sh: --fs-relay needs --dist" >&2; exit 2; }
    { [ "$p4" = 1 ] || [ "$host_work" = own ]; } || { echo "up.sh: --fs-relay needs hosts on their own work volumes (--p4 or --host-work=own)" >&2; exit 2; }
    [ -n "$work_root" ] || { echo "up.sh: --fs-relay needs --work-root (the hosts say where their workspaces are)" >&2; exit 2; }
fi
case "$login_from" in ''|k2-host-[0-9]*) ;; *) echo "up.sh: --login-from takes a host pod (k2-host-N)" >&2; exit 2 ;; esac
if { [ -n "$work_root" ] || [ -n "$login_from" ]; } && [ "$dist" = 0 ]; then
    echo "up.sh: --work-root and --login-from need --dist" >&2; exit 2
fi
if [ "$p4" = 1 ]; then kind_variant p4; else kind_variant_from_flags "$fake" "$dist"; fi
ptyd=${ptyd:-${K2_KIND_PTYD:-$KIND_PTYD_DEFAULT}}
case "$ptyd" in 0|1) ;; *) echo "up.sh: K2_PTYD must be 0 or 1 (got $ptyd)" >&2; exit 2 ;; esac
user=${K2_KIND_USER:-owner}
for t in docker kind kubectl curl python3; do
    command -v "$t" >/dev/null || { echo "up.sh: $t is not installed" >&2; exit 1; }
done
kind_kubeconfig
cd "$KIND_REPO"

# ── 1. images ───────────────────────────────────────────────────────────
# The daemon image comes from this tree, or from K2_KIND_DAEMON_SRC (for
# --dist: a checkout of k2-dist/dist-link until it is merged here). It is
# compiled by scripts/kind/build-daemon.sh in a memory-capped container,
# which takes the Docker lock and a cargo slot itself.
daemon_src=${K2_KIND_DAEMON_SRC:-$KIND_REPO}
if [ -n "$render_to" ]; then
    say "render only: no image is built or loaded"
elif [ "$rebuild_daemon" = 1 ] || ! docker image inspect "$KIND_BASE_IMAGE" >/dev/null 2>&1; then
    if [ "$KIND_DIST" = 1 ] && ! [ -d "$daemon_src/crates/k2-daemon/src/hosts" ]; then
        echo "up.sh: $daemon_src has no dist-link (crates/k2-daemon/src/hosts); set K2_KIND_DAEMON_SRC to a checkout of k2-dist/dist-link" >&2
        exit 1
    fi
    "$KIND_REPO/scripts/kind/build-daemon.sh" "$daemon_src" "$KIND_BASE_IMAGE" >/dev/null
else
    say "$KIND_BASE_IMAGE exists (--rebuild-daemon to rebuild it)"
fi
if [ -z "$render_to" ]; then
    # The rest (no Rust compile except the fake agent's small cargo stage, which
    # also takes a cargo slot) holds the machine-wide Docker lock the k2-dist
    # workers share, so only one Docker-heavy job runs at a time.
    DOCKER_LOCK="$HOME/.cache/k2-dist-docker.lock"
    mkdir -p "$HOME/.cache"
    until mkdir "$DOCKER_LOCK" 2>/dev/null; do say "waiting for $DOCKER_LOCK"; sleep 20; done
    trap 'rmdir "$DOCKER_LOCK" 2>/dev/null' EXIT
    slot=()
    [ -x "$HOME/.cache/k2-dist-cargo-slot.sh" ] && slot=("$HOME/.cache/k2-dist-cargo-slot.sh")
    say "building $KIND_IMAGE ($KIND_DOCKERFILE)"
    # The revision label is the K2 inside (the daemon it is built FROM), so
    # it can be trusted for what is deployed; this tree's HEAD (the kind
    # scripts, the Dockerfile) is k2.dev/kind-revision.
    base_rev=$(docker image inspect -f '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$KIND_BASE_IMAGE")
    [ -n "$base_rev" ] || { echo "up.sh: $KIND_BASE_IMAGE has no revision label" >&2; exit 1; }
    build=(docker build -q -f "$KIND_DOCKERFILE" -t "$KIND_IMAGE" --build-arg "BASE_IMAGE=$KIND_BASE_IMAGE"
        --label "org.opencontainers.image.revision=$base_rev" --label "k2.dev/kind-revision=$(git rev-parse HEAD)" \
        --label "k2.dev/base-image-id=$(docker image inspect -f '{{.Id}}' "$KIND_BASE_IMAGE")" "$KIND_CONTEXT")
    case "$KIND_DOCKERFILE" in *fake*) build=(${slot[@]+"${slot[@]}"} "${build[@]}") ;; esac   # its cargo stage
    DOCKER_BUILDKIT=1 "${build[@]}" >/dev/null
    if [ "$door" = proxy ]; then
        say "building $KIND_PROXY_IMAGE (deploy/kind/proxy)"
        DOCKER_BUILDKIT=1 docker build -q -t "$KIND_PROXY_IMAGE" deploy/kind/proxy >/dev/null
    fi

    # ── 2. load and apply ───────────────────────────────────────────────────
    for img in $KIND_LOAD; do
        say "loading $img into kind cluster $KIND_CLUSTER"
        kind load docker-image "$img" --name "$KIND_CLUSTER" >&2
    done
    rmdir "$DOCKER_LOCK"; trap - EXIT
fi
rendered="$KIND_STATE/rendered.yaml"
# The overlay to apply: for --dist, a throwaway kustomization in the state
# dir over the overlay in git (which keeps 1 host, token auth, no K2_PTYD),
# with k2-host's replicas and the switches set. Each switch goes in the pod
# templates, not a ConfigMap, so changing it rolls the pods; at its default
# it adds nothing, so the render is what it was before the switch existed.
write_overlay() {  # write_overlay <k2-host replicas>
    overlay="$KIND_STATE/overlay"
    rm -rf "${KIND_STATE:?}"/hosts-*
    mkdir -p "$overlay"
    {
        cat <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
# Everything in the variant's namespace, with its deploy label: the
# overlay's own objects already are; pool-rbac.yaml has neither, and
# \`kubectl apply -f\` would put an object without a namespace in the
# kubeconfig's default one.
namespace: $KIND_NS
labels:
  - pairs: {k2.dev/deploy: kind-$KIND_VARIANT}
    includeSelectors: false
resources:
  - $(python3 -I -c 'import os,sys; r = os.path.realpath; print(os.path.relpath(r(sys.argv[1]), r(sys.argv[2])))' "$KIND_REPO/$KIND_OVERLAY" "$overlay")
$([ "$provisioner" = kubernetes ] && printf '  - %s\n' "$(python3 -I -c 'import os,sys; r = os.path.realpath; print(os.path.relpath(r(sys.argv[1]), r(sys.argv[2])))' "$KIND_REPO/deploy/kind/dist/pool-rbac.yaml" "$overlay")")
replicas:
  - {name: k2-host, count: $1}
EOF
        if [ "$ptyd" = 1 ] || [ "$host_auth" = enroll ] || [ -n "$work_root" ] || [ -n "$login_from" ] || [ -n "$max_awake" ] \
            || [ "$host_work" = own ] || [ -n "$move_after" ] || [ -n "$idle_after" ] || [ "$fs_relay" = 1 ]; then echo "patches:"; fi
        if [ "$host_work" = own ]; then
            # The host's own workspaces volume; data's template is repeated
            # because a list of claim templates is replaced, not merged.
            cat <<EOF
  - target: {kind: StatefulSet, name: k2-host}
    patch: |-
      apiVersion: apps/v1
      kind: StatefulSet
      metadata:
        name: k2-host
      spec:
        template:
          spec:
            volumes:
              - name: work
                \$patch: delete
        volumeClaimTemplates:
          - metadata: {name: data}
            spec:
              accessModes: [ReadWriteOnce]
              resources: {requests: {storage: 10Gi}}
          - metadata: {name: work}
            spec:
              accessModes: [ReadWriteOnce]
              resources: {requests: {storage: 2Gi}}
EOF
        fi
        if [ -n "$move_after" ] || [ -n "$idle_after" ] || [ "$fs_relay" = 1 ]; then
            cat <<EOF
  - target: {kind: StatefulSet, name: k2-controller}
    patch: |-
      apiVersion: apps/v1
      kind: StatefulSet
      metadata:
        name: k2-controller
      spec:
        template:
          spec:
            containers:
              - name: k2-daemon
                env:
EOF
            [ -n "$move_after" ] && echo "                  - {name: K2_HOST_MOVE_AFTER, value: \"$move_after\"}"
            [ -n "$idle_after" ] && echo "                  - {name: K2_PROVISIONER_IDLE_AFTER, value: \"$idle_after\"}"
            if [ "$fs_relay" = 1 ]; then
                echo '                  - {name: K2_FS_RELAY, value: "1"}'
                echo '                  - {name: K2_WORK_ROOT, value: "/var/lib/k2/work"}'
            fi
            true
        fi
        if [ "$ptyd" = 1 ] || [ -n "$work_root" ] || [ -n "$max_awake" ]; then
            cat <<EOF
  - target: {kind: StatefulSet, name: k2-host}
    patch: |-
      apiVersion: apps/v1
      kind: StatefulSet
      metadata:
        name: k2-host
      spec:
        template:
          spec:
            containers:
              - name: k2-host
                env:
EOF
            [ "$ptyd" = 1 ] && echo '                  - {name: K2_PTYD, value: "1"}'
            [ -n "$work_root" ] && echo "                  - {name: K2_WORK_ROOT, value: \"$work_root\"}"
            [ -n "$max_awake" ] && echo "                  - {name: K2_HOST_MAX_AWAKE, value: \"$max_awake\"}"
        fi
        if [ -n "$login_from" ]; then
            # The variant's agents image by its own name: the base's images:
            # mapping doesn't reach what this overlay's patch adds.
            cat <<EOF
  - target: {kind: StatefulSet, name: k2-host}
    patch: |-
      apiVersion: apps/v1
      kind: StatefulSet
      metadata:
        name: k2-host
      spec:
        template:
          spec:
            initContainers:
              - name: login-seed
                image: $KIND_IMAGE
                imagePullPolicy: Never
                command:
                  - sh
                  - -c
                  - |
                    set -eu
                    [ "\$(cat /etc/hostname)" != "$login_from" ] || { echo "login-seed: the source host itself"; exit 0; }
                    if [ -e /var/lib/k2-host/.claude/.credentials.json ]; then echo "login-seed: has its own login"; exit 0; fi
                    if [ ! -s /login-src/.claude/.credentials.json ]; then echo "login-seed: $login_from has no login"; exit 0; fi
                    umask 077
                    mkdir -p /var/lib/k2-host/.claude
                    cp /login-src/.claude/.credentials.json /var/lib/k2-host/.claude/.credentials.json.tmp
                    chmod 600 /var/lib/k2-host/.claude/.credentials.json.tmp
                    mv /var/lib/k2-host/.claude/.credentials.json.tmp /var/lib/k2-host/.claude/.credentials.json
                    if [ ! -e /var/lib/k2-host/.claude.json ] && [ -s /login-src/.claude.json ]; then
                      cp /login-src/.claude.json /var/lib/k2-host/.claude.json
                    fi
                    echo "login-seed: copied $login_from's login"
                securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
                volumeMounts:
                  - {name: data, mountPath: /var/lib/k2-host}
                  - {name: login-src, mountPath: /login-src, readOnly: true}
            # Read-only on the init container's mount only: on POD itself
            # this is its own HOME volume, and readOnly here (the pod's
            # volume) made the whole volume read-only, its HOME too.
            volumes:
              - name: login-src
                persistentVolumeClaim: {claimName: data-$login_from}
EOF
        fi
        if [ "$host_auth" = enroll ]; then
            # Signed enrollment (W5): each host reads its own one-time token
            # from the Secret k2-host-enroll (scripts/kind/enroll.py mints
            # it) and keeps its key on its own volume
            # ($HOME/.k2/host-key.pem, 0600, the daemon's default).
            cat <<EOF
  - target: {kind: StatefulSet, name: k2-controller}
    patch: |-
      apiVersion: apps/v1
      kind: StatefulSet
      metadata:
        name: k2-controller
      spec:
        template:
          spec:
            containers:
              - name: k2-daemon
                env:
                  - {name: K2_HOST_AUTH, value: enroll}
EOF
            if [ "$provisioner" = kubernetes ]; then
                # The pool talks to the API server: its own ServiceAccount
                # (pool-rbac.yaml) and token. The pod env wins over the
                # ConfigMap's K2_PROVISIONER=static.
                cat <<EOF
                  - {name: K2_PROVISIONER, value: kubernetes}
                  - {name: K2_PROVISIONER_HOST_STATEFULSET, value: k2-host}
                  - {name: K2_PROVISIONER_MIN_HOSTS, value: "$hosts"}
                  - {name: K2_PROVISIONER_MAX_HOSTS, value: "$max_hosts"}
            serviceAccountName: k2-controller
            automountServiceAccountToken: true
EOF
            fi
            cat <<EOF
  - target: {kind: StatefulSet, name: k2-host}
    patch: |-
      apiVersion: apps/v1
      kind: StatefulSet
      metadata:
        name: k2-host
      spec:
        template:
          spec:
            containers:
              - name: k2-host
                env:
                  - {name: K2_HOST_AUTH, value: enroll}
                  # Repeated here so it comes before the line that uses it:
                  # a patch's entries go first, and \$(VAR) expands only
                  # variables defined earlier in the list.
                  - name: K2_HOST_NAME
                    valueFrom: {fieldRef: {fieldPath: metadata.name}}
                  - {name: K2_HOST_ENROLL_TOKEN_FILE, value: "/run/secrets/k2-enroll/\$(K2_HOST_NAME)"}
                volumeMounts:
                  - {name: k2-host-enroll, mountPath: /run/secrets/k2-enroll, readOnly: true}
            volumes:
              - name: k2-host-enroll
                secret: {secretName: k2-host-enroll, optional: true, defaultMode: 0440}
EOF
        fi
    } > "$overlay/kustomization.yaml"
    kubectl kustomize --load-restrictor LoadRestrictionsNone "$overlay" > "$rendered"
}
host_names() { local i; for i in $(seq 0 $(( hosts - 1 ))); do printf 'k2-host-%s\n' "$i"; done; }
apply_rendered() {  # apply_rendered <what>
    say "applying $KIND_OVERLAY (namespace $KIND_NS)$( [ "$KIND_DIST" = 1 ] && echo ", $1, K2_PTYD=$ptyd, K2_HOST_AUTH=$host_auth")"
    kc apply -f "$rendered" >&2
    # Same tag, new image: each pod template carries its image's id, so a
    # rebuilt image rolls its pods and an unchanged one does not.
    local pair sts id
    for pair in $KIND_ROLLOUT; do
        sts=${pair%%=*} id=$(docker image inspect -f '{{.Id}}' "${pair#*=}")
        kc -n "$KIND_NS" patch statefulset "$sts" --type merge -p \
            "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"k2.dev/image-id\":\"$id\"}}}}}" >&2
    done
    for pair in $KIND_ROLLOUT; do
        kc -n "$KIND_NS" rollout status "statefulset/${pair%%=*}" --timeout=600s >&2
    done
}
# Enroll mode: hosts that have no token in k2-host-enroll yet wait (0
# replicas) until the controller is up to mint them (after the door, below).
first_hosts=$hosts
if [ "$host_auth" = enroll ]; then
    have=$(kc -n "$KIND_NS" get secret k2-host-enroll -o jsonpath='{.data}' 2>/dev/null || true)
    for h in $(host_names); do
        case "$have" in *"\"$h\""*) ;; *) first_hosts=0 ;; esac
    done
    # An entry there is no proof the controller knows the host: after a fresh
    # controller volume (fleet-g2's fresh step, an interrupted run) it holds
    # spent tokens, and the rollout below would wait forever for a host
    # that can't enroll. When the running controller answers, a host with
    # no live key on it also waits for a new token.
    if [ "$first_hosts" = "$hosts" ] && [ -z "$render_to" ]; then
        live=$(kc -n "$KIND_NS" exec "$KIND_POD" -c k2-daemon -- cat /var/lib/k2/.k2/daemon.token 2>/dev/null \
            | python3 -I "$KIND_REPO/scripts/kind/enroll.py" keys --url "http://127.0.0.1:$KIND_PORT" 2>/dev/null || true)
        if [ -n "$live" ]; then
            for h in $(host_names); do
                printf '%s\n' "$live" | grep "^$h " | grep -vq REVOKED || first_hosts=0
            done
            [ "$first_hosts" = 0 ] && say "a host in k2-host-enroll has no live key on the controller: hosts start after new tokens"
        fi
    fi
fi
if [ -n "$render_to" ]; then
    write_overlay "$hosts"
    cp "$rendered" "$render_to"
    say "rendered $KIND_OVERLAY (${hosts} host(s), K2_PTYD=$ptyd, K2_HOST_AUTH=$host_auth, K2_PROVISIONER=$provisioner, max $max_hosts${work_root:+, K2_WORK_ROOT=$work_root}${login_from:+, login from $login_from}$( [ "$fs_relay" = 1 ] && echo ", fs-relay on")) to $render_to"
    exit 0
fi
if [ "$KIND_DIST" = 1 ]; then
    write_overlay "$first_hosts"
else
    overlay="$KIND_REPO/$KIND_OVERLAY"
    kubectl kustomize --load-restrictor LoadRestrictionsNone "$overlay" > "$rendered"
fi
if [ "$KIND_DIST" = 1 ]; then
    # The namespace first, then the bootstrap Secret, so the pods find it.
    kc apply -f "$rendered" -l k2.dev/bootstrap=namespace >&2
    if ! kc -n "$KIND_NS" get secret k2-secrets >/dev/null 2>&1; then
        # Built in Python, piped to `kubectl create` (not apply, which would
        # copy the data into an annotation). Never on argv, never printed.
        python3 -I -c 'import base64,json,secrets,sys
tok = secrets.token_urlsafe(32).encode()
print(json.dumps({"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
    "metadata": {"name": "k2-secrets", "namespace": sys.argv[1], "labels": {"k2.dev/deploy": sys.argv[2]}},
    "data": {"host-enroll-token": base64.b64encode(tok).decode()}}))' "$KIND_NS" "kind-$KIND_VARIANT" \
            | kc create -f - >&2
        say "created Secret $KIND_NS/k2-secrets (host bootstrap token, not shown)"
    fi
fi
apply_rendered "$first_hosts host(s)"

# ── 3. the Mac's door ───────────────────────────────────────────────────
if [ "$door" = proxy ]; then
    kind_start_proxy || exit 1
    say "proxy $KIND_PROXY: 127.0.0.1:$KIND_PORT → $KIND_PROXY_TARGET:$KIND_NODEPORT"
else
    kind_start_forward || exit 1
fi
base="http://127.0.0.1:$KIND_PORT"
kind_wait_ready || exit 1
say "daemon ready at $base"
if [ "$host_auth" = enroll ]; then
    # One-time tokens for hosts with no key on the controller; then the
    # hosts. The owner token goes to enroll.py on a pipe, never on argv.
    kc -n "$KIND_NS" exec "$KIND_POD" -c k2-daemon -- cat /var/lib/k2/.k2/daemon.token \
        | python3 -I "$KIND_REPO/scripts/kind/enroll.py" mint --kubeconfig "$KIND_KUBECONFIG" --namespace "$KIND_NS" \
            --url "$base" --hosts "$(host_names | paste -sd, -)" --deploy-label "kind-$KIND_VARIANT" >&2 \
        || { echo "up.sh: minting enrollment tokens failed" >&2; exit 1; }
    if [ "$first_hosts" != "$hosts" ]; then
        write_overlay "$hosts"
        apply_rendered "$hosts host(s), enrolling"
    fi
fi

# ── 4. the app's login ──────────────────────────────────────────────────
# The owner token stays in this shell: curl gets the URL that carries it as
# a config on a pipe (-K), never on its argv.
owner_token=$(kc -n "$KIND_NS" exec "$KIND_POD" -c k2-daemon -- cat /var/lib/k2/.k2/daemon.token)
k2_call() {  # k2_call <path> [json body] → response body on stdout
    if [ $# -ge 2 ]; then
        printf '%s' "$2" | curl -sS --max-time 30 -K <(printf 'url = "%s%s?token=%s"\n' "$base" "$1" "$owner_token") \
            -H 'Content-Type: application/json' --data-binary @-
    else
        curl -sS --max-time 30 -K <(printf 'url = "%s%s?token=%s"\n' "$base" "$1" "$owner_token")
    fi
}
expect_success() {  # expect_success <what> <response>
    python3 -I -c 'import json,sys; d=json.loads(sys.argv[2] or "{}"); sys.exit(0 if d.get("success") else 1)' "$1" "$2" \
        || { echo "up.sh: $1 failed: ${2:0:300}" >&2; exit 1; }
}
users=$(k2_call /cli/users)
exists=$(python3 -I -c 'import json,sys; print(any(u.get("username")==sys.argv[2] for u in json.loads(sys.argv[1]).get("users",[])))' "$users" "$user")
# JSON bodies from Python, never from a shell string ({a,b} would brace-expand).
# Values come in through the environment (B_<key>), so the password is on
# no process's argv.
body() { python3 -I -c 'import json,os,sys; print(json.dumps({k: os.environ["B_" + k] for k in sys.argv[1:]}))' "$@"; }
password=""
if [ "$exists" != True ] || [ "$reset_password" = 1 ]; then
    password=$(python3 -I -c 'import secrets,string; a=string.ascii_letters+string.digits; print("".join(secrets.choice(a) for _ in range(24)))')
fi
if [ "$exists" != True ]; then
    expect_success "creating login $user" "$(k2_call /cli/users/add "$(B_username="$user" B_password="$password" body username password)")"
    expect_success "making $user an Owner" "$(k2_call /cli/users/set-role "$(B_username="$user" B_role=owner body username role)")"
    say "created K2 login '$user' (Owner)"
elif [ "$reset_password" = 1 ]; then
    expect_success "resetting the password of $user" \
        "$(k2_call /cli/users/set-password "$(B_username="$user" B_password="$password" body username password)" )"
    say "set a new password for K2 login '$user'"
fi
# --dist: the controller's view of its hosts (GET /cli/hosts/status, no
# token: role, host ids and names, placements). Informational, never fatal.
if [ "$KIND_DIST" = 1 ]; then
    say "controller /cli/hosts/status: $(curl -s --max-time 5 "$base/cli/hosts/status" | python3 -I -c 'import json,sys
try:
    d = json.load(sys.stdin)
except ValueError:
    print("no JSON"); sys.exit()
hs = d.get("hosts", [])
print("role=%s, %d host(s): %s" % (d.get("role"), len(hs), ", ".join("%s (%s placements)" % (h.get("host_name"), len(h.get("placements") or [])) for h in hs)))')"
fi
unset owner_token

# A git folder for the app's "Add Workspace" picker, made by the pod that
# runs agents. --dist: on the shared work volume, the same path in both pods
# (the app browses the controller's files; the host gets that cwd verbatim).
kc -n "$KIND_NS" exec "$KIND_AGENT_POD" -c "$KIND_AGENT_CONTAINER" -- env D="$KIND_WORK/demo" sh -c \
    '[ -d "$D/.git" ] || { mkdir -p "$D" && git -C "$D" init -q; }'

# Claude Code's first-run screens (theme picker, onboarding) on the pod that
# runs agents: seed hasCompletedOnboarding and theme=dark into its
# $HOME/.claude.json so a Claude tab from the app opens straight to the
# prompt. Only keys that are absent are set (a theme the owner picked
# stays), the file keeps its mode, and nothing is written when both are
# there. The real-agent images only: the fake agent has no such screens.
if [ "$KIND_DOCKERFILE" = deploy/kind/Dockerfile.agents ]; then
    seeded=$(kc -n "$KIND_NS" exec "$KIND_AGENT_POD" -c "$KIND_AGENT_CONTAINER" -- perl -MJSON::PP -e '
        my $f = "$ENV{HOME}/.claude.json";
        my ($d, $mode) = ({}, 0600);
        if (-e $f) {
            open(my $in, "<", $f) or die "read $f: $!\n";
            my $text = do { local $/; <$in> };
            close $in;
            $d = JSON::PP->new->utf8->decode($text) if $text =~ /\S/;
            $mode = (stat $f)[2] & 07777;
        }
        ref $d eq "HASH" or die "$f is not a JSON object\n";
        my %want = (hasCompletedOnboarding => JSON::PP::true, theme => "dark");
        # Newer Claude Code keeps the theme in ~/.claude/settings.json (and
        # drops it from .claude.json): one set there is the owner'"'"'s choice.
        if (open(my $sf, "<", "$ENV{HOME}/.claude/settings.json")) {
            my $st = do { local $/; <$sf> };
            my $sd = eval { JSON::PP->new->utf8->decode($st) };
            delete $want{theme} if ref $sd eq "HASH" && exists $sd->{theme};
        }
        my @set = grep { !exists $d->{$_} } sort keys %want;
        if (!@set) { print "unchanged"; exit 0 }
        $d->{$_} = $want{$_} for @set;
        my $tmp = "$f.k2-seed.$$";
        open(my $out, ">", $tmp) or die "write $tmp: $!\n";
        print $out JSON::PP->new->utf8->canonical->pretty->encode($d);
        close $out or die "write $tmp: $!\n";
        chmod $mode, $tmp;
        rename $tmp, $f or die "rename $tmp: $!\n";
        print join(" ", @set);') \
        || { echo "up.sh: seeding \$HOME/.claude.json in $KIND_AGENT_POD failed" >&2; exit 1; }
    say "Claude Code first-run settings in $KIND_AGENT_POD:\$HOME/.claude.json: $seeded"
fi

flag=$KIND_FLAGS
if [ -n "$password" ] && [ -n "$password_file" ]; then
    mkdir -p "$(dirname "$password_file")"
    (umask 077; printf '%s\n' "$password" > "$password_file")
    chmod 600 "$password_file"
    password="in $password_file (mode 0600; not shown)"
fi
[ -n "$password" ] || password="(unchanged since it was set; scripts/kind/up.sh$flag --reset-password sets a new one)"

cat <<EOF

K2 is up on kind $KIND_CLUSTER (namespace $KIND_NS, agents on $KIND_IMAGE in $KIND_AGENT_POD).

  Server URL : $base
  Username   : $user
  Password   : $password

In the K2 app: Settings → Connected Servers → + Add a server
(deploy/kind/README.md). The door is $( [ "$door" = proxy ] && echo "the Docker container $KIND_PROXY" || echo "a kubectl port-forward (log: $KIND_STATE/port-forward.log)" );
scripts/kind/forward.sh$flag --status checks it, and scripts/kind/forward.sh$flag
recreates it. Stop everything: scripts/kind/down.sh$flag
EOF
