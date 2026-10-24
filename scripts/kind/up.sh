#!/usr/bin/env bash
# K2 on the kind cluster k2lab, ready for the desktop app on this Mac
# (deploy/kind/README.md has the app steps).
#
#   scripts/kind/up.sh [--fake] [--rebuild-daemon] [--reset-password]
#
# 1. builds k2-daemon:dev (packaging/oci/build.sh) when it is missing, or
#    always with --rebuild-daemon; then k2-agents:dev (deploy/kind/
#    Dockerfile.agents: the daemon plus the Claude Code CLI);
# 2. loads it into k2lab and applies deploy/kind/agents (namespace k2-kind);
# 3. waits for the pod, starts a port-forward to 127.0.0.1:7440 (kept
#    running in the background, and restarted if the pod restarts);
# 4. creates the K2 Connect login the app signs in with (an Owner), once,
#    and prints the URL, the username and, when it set one, the password.
#
# --fake deploys the TEST variant instead (k2-agents:fake, namespace
# k2-kind-fake, port 7441): the lab's k2-fake-agent under every agent CLI
# name, for scripts/kind/s1.sh. --reset-password sets a new password for
# the existing login. K2_KIND_PORT and K2_KIND_USER override the port and
# the username (default "owner").
#
# Uses only k2lab, through an explicit kubeconfig; never the current kube
# context. No credential goes into git, an image or a log: the owner signs
# Claude in with /login from the app, and the daemon token is read into
# memory only, never printed or put on a command line.
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"

variant=agents rebuild_daemon=0 reset_password=0
for a in "$@"; do
    case "$a" in
        --fake) variant=fake ;;
        --rebuild-daemon) rebuild_daemon=1 ;;
        --reset-password) reset_password=1 ;;
        -h|--help) sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "up.sh: unknown argument $a (see --help)" >&2; exit 2 ;;
    esac
done
kind_variant "$variant"
user=${K2_KIND_USER:-owner}
for t in docker kind kubectl curl python3; do
    command -v "$t" >/dev/null || { echo "up.sh: $t is not installed" >&2; exit 1; }
done
kind_kubeconfig
cd "$KIND_REPO"

# ── 1. images ───────────────────────────────────────────────────────────
# Docker-heavy steps (builds, kind load) take the machine-wide lock the
# k2-dist workers share, so only one runs in the Docker VM at a time.
DOCKER_LOCK="$HOME/.cache/k2-dist-docker.lock"
mkdir -p "$HOME/.cache"
until mkdir "$DOCKER_LOCK" 2>/dev/null; do say "waiting for $DOCKER_LOCK"; sleep 20; done
trap 'rmdir "$DOCKER_LOCK" 2>/dev/null' EXIT
if [ "$rebuild_daemon" = 1 ] || ! docker image inspect k2-daemon:dev >/dev/null 2>&1; then
    say "building k2-daemon:dev (packaging/oci/build.sh; a cold build takes a while)"
    CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-4} packaging/oci/build.sh k2-daemon:dev >&2
else
    say "k2-daemon:dev exists (--rebuild-daemon to rebuild it)"
fi
say "building $KIND_IMAGE ($KIND_DOCKERFILE)"
DOCKER_BUILDKIT=1 docker build -q -f "$KIND_DOCKERFILE" -t "$KIND_IMAGE" \
    --label "org.opencontainers.image.revision=$(git rev-parse HEAD)" "$KIND_CONTEXT" >/dev/null
image_id=$(docker image inspect -f '{{.Id}}' "$KIND_IMAGE")

# ── 2. load and apply ───────────────────────────────────────────────────
say "loading $KIND_IMAGE into kind cluster $KIND_CLUSTER"
kind load docker-image "$KIND_IMAGE" --name "$KIND_CLUSTER" >&2
rmdir "$DOCKER_LOCK"; trap - EXIT
say "applying $KIND_OVERLAY (namespace $KIND_NS)"
kubectl kustomize --load-restrictor LoadRestrictionsNone "$KIND_OVERLAY" | kc apply -f - >&2
# Same tag, new image: the pod template carries the image id, so a rebuilt
# image rolls the pod and an unchanged one does not.
kc -n "$KIND_NS" patch statefulset k2-solo --type merge -p \
    "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"k2.dev/image-id\":\"$image_id\"}}}}}" >&2
kc -n "$KIND_NS" rollout status statefulset/k2-solo --timeout=600s >&2

# ── 3. port-forward ─────────────────────────────────────────────────────
kind_stop_forward
if lsof -nP -iTCP:"$KIND_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "up.sh: 127.0.0.1:$KIND_PORT is already in use; set K2_KIND_PORT to a free port" >&2
    exit 1
fi
nohup "$KIND_REPO/scripts/kind/port-forward.sh" "$KIND_VARIANT" >"$KIND_STATE/port-forward.log" 2>&1 &
echo $! > "$KIND_STATE/port-forward.pid"
echo "$KIND_PORT" > "$KIND_STATE/port"
base="http://127.0.0.1:$KIND_PORT"
phase=""
for _ in $(seq 1 120); do
    phase=$(curl -s --connect-timeout 1 "$base/boot-status" 2>/dev/null \
        | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("phase",""))' 2>/dev/null || true)
    [ "$phase" = ready ] && break
    sleep 0.5
done
if [ "$phase" != ready ]; then
    echo "up.sh: $base/boot-status never said phase=ready (last: '$phase'); see $KIND_STATE/port-forward.log" >&2
    exit 1
fi
say "daemon ready at $base"

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
unset owner_token

# A git folder on the volume for the app's "Add Workspace" picker.
kc -n "$KIND_NS" exec "$KIND_POD" -c k2-daemon -- sh -c \
    'd=/var/lib/k2/work/demo; [ -d "$d/.git" ] || { mkdir -p "$d" && git -C "$d" init -q; }'

flag=""; [ "$variant" = fake ] && flag=" --fake"
[ -n "$password" ] || password="(unchanged since it was set; scripts/kind/up.sh$flag --reset-password sets a new one)"

cat <<EOF

K2 is up on kind $KIND_CLUSTER (namespace $KIND_NS, image $KIND_IMAGE).

  Server URL : $base
  Username   : $user
  Password   : $password

In the K2 app: Settings → Connected Servers → + Add a server
(deploy/kind/README.md). The port-forward runs in the background
(log: $KIND_STATE/port-forward.log). Stop everything: scripts/kind/down.sh$flag
EOF
