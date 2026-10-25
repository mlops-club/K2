#!/usr/bin/env bash
# K2 on the kind cluster k2lab, ready for the desktop app on this Mac
# (deploy/kind/README.md has the app steps).
#
#   scripts/kind/up.sh [--dist [--hosts=N]] [--fake] [--rebuild-daemon] [--reset-password]
#                      [--port-forward] [--password-file=PATH]
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
#
# Uses only k2lab, through an explicit kubeconfig; never the current kube
# context. No credential goes into git, an image or a log: the owner signs
# Claude in with /login from the app, and the daemon token is read into
# memory only, never printed or put on a command line.
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"

fake=0 dist=0 rebuild_daemon=0 reset_password=0 door=proxy password_file= hosts=1
for a in "$@"; do
    case "$a" in
        --fake) fake=1 ;;
        --dist) dist=1 ;;
        --rebuild-daemon) rebuild_daemon=1 ;;
        --reset-password) reset_password=1 ;;
        --port-forward) door=port-forward ;;
        --password-file=*) password_file=${a#--password-file=} ;;
        --hosts=*) hosts=${a#--hosts=} ;;
        -h|--help) sed -n '2,39p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "up.sh: unknown argument $a (see --help)" >&2; exit 2 ;;
    esac
done
case "$hosts" in ''|*[!0-9]*|0) echo "up.sh: --hosts=N takes a number from 1" >&2; exit 2 ;; esac
if [ "$hosts" != 1 ] && [ "$dist" = 0 ]; then
    echo "up.sh: --hosts=N needs --dist (the solo pod is its own host)" >&2; exit 2
fi
kind_variant_from_flags "$fake" "$dist"
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
if [ "$rebuild_daemon" = 1 ] || ! docker image inspect "$KIND_BASE_IMAGE" >/dev/null 2>&1; then
    if [ "$KIND_DIST" = 1 ] && ! [ -d "$daemon_src/crates/k2-daemon/src/hosts" ]; then
        echo "up.sh: $daemon_src has no dist-link (crates/k2-daemon/src/hosts); set K2_KIND_DAEMON_SRC to a checkout of k2-dist/dist-link" >&2
        exit 1
    fi
    "$KIND_REPO/scripts/kind/build-daemon.sh" "$daemon_src" "$KIND_BASE_IMAGE" >/dev/null
else
    say "$KIND_BASE_IMAGE exists (--rebuild-daemon to rebuild it)"
fi
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
build=(docker build -q -f "$KIND_DOCKERFILE" -t "$KIND_IMAGE" --build-arg "BASE_IMAGE=$KIND_BASE_IMAGE"
    --label "org.opencontainers.image.revision=$(git rev-parse HEAD)" "$KIND_CONTEXT")
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
rendered="$KIND_STATE/rendered.yaml"
overlay="$KIND_REPO/$KIND_OVERLAY"
if [ "$KIND_DIST" = 1 ]; then
    # --hosts=N: the overlay with k2-host's replicas set, from a throwaway
    # kustomization in the state dir (the overlays in git keep 1).
    overlay="$KIND_STATE/hosts-$hosts"
    mkdir -p "$overlay"
    printf 'apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - %s\nreplicas:\n  - {name: k2-host, count: %s}\n' \
        "$(python3 -I -c 'import os,sys; r = os.path.realpath; print(os.path.relpath(r(sys.argv[1]), r(sys.argv[2])))' "$KIND_REPO/$KIND_OVERLAY" "$overlay")" \
        "$hosts" > "$overlay/kustomization.yaml"
fi
kubectl kustomize --load-restrictor LoadRestrictionsNone "$overlay" > "$rendered"
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
say "applying $KIND_OVERLAY (namespace $KIND_NS)$( [ "$KIND_DIST" = 1 ] && echo ", $hosts host(s)")"
kc apply -f "$rendered" >&2
# Same tag, new image: each pod template carries its image's id, so a
# rebuilt image rolls its pods and an unchanged one does not.
for pair in $KIND_ROLLOUT; do
    sts=${pair%%=*} id=$(docker image inspect -f '{{.Id}}' "${pair#*=}")
    kc -n "$KIND_NS" patch statefulset "$sts" --type merge -p \
        "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"k2.dev/image-id\":\"$id\"}}}}}" >&2
done
for pair in $KIND_ROLLOUT; do
    kc -n "$KIND_NS" rollout status "statefulset/${pair%%=*}" --timeout=600s >&2
done

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
