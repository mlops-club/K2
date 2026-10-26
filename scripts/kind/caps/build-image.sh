#!/usr/bin/env bash
# Build k2-daemon:caps (this worktree's daemon, kind's runtime image plus
# postgresql-client, as packaging/oci/Dockerfile now ships) and load it into
# k2lab.
#
#   scripts/kind/caps/build-image.sh [--kind-app=<kind-app worktree>]
#
# 1. kind-app's scripts/kind/build-daemon.sh → k2-daemon:caps-base. It takes
#    the k2-dist Docker lock and a cargo slot itself: never wrap it in the lock.
# 2. Under the Docker lock (~/.cache/k2-dist-docker.lock): the
#    Dockerfile.pg-client layer → k2-daemon:caps, then `kind load` into k2lab.
set -euo pipefail
# shellcheck source=scripts/kind/caps/lib.sh
source "$(dirname "$0")/lib.sh"
kind_app="$CAPS_REPO/../kind-app"
for a in "$@"; do
    case "$a" in
        --kind-app=*) kind_app=${a#--kind-app=} ;;
        -h|--help) sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "build-image.sh: unknown argument $a" >&2; exit 2 ;;
    esac
done
[ -x "$kind_app/scripts/kind/build-daemon.sh" ] || { echo "no kind-app worktree at $kind_app (--kind-app=)" >&2; exit 2; }

say "building k2-daemon:caps-base from $CAPS_REPO"
K2_KIND_BUILD_MEMORY=${K2_KIND_BUILD_MEMORY:-6g} CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-1} \
    "$kind_app/scripts/kind/build-daemon.sh" "$CAPS_REPO" k2-daemon:caps-base >&2

lock="$HOME/.cache/k2-dist-docker.lock"
until mkdir "$lock" 2>/dev/null; do sleep 15; done
echo "k8s-capabilities $$ $(date +%s)" > "$lock/owner"
trap 'rm -f "$lock/owner"; rmdir "$lock"' EXIT
say "adding postgresql-client → k2-daemon:caps"
DOCKER_BUILDKIT=1 docker build -q -t k2-daemon:caps --build-arg BASE=k2-daemon:caps-base \
    -f "$CAPS_REPO/scripts/kind/caps/Dockerfile.pg-client" "$CAPS_REPO/scripts/kind/caps" >/dev/null
say "loading k2-daemon:caps into $CAPS_CLUSTER"
kind load docker-image k2-daemon:caps --name "$CAPS_CLUSTER" >/dev/null
echo k2-daemon:caps
