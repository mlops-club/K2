#!/usr/bin/env bash
# The fleet topology on Docker Compose (LAB_FLEET_BACKEND=compose), sourced
# by scripts/lab/topology/fleet.sh:
#
#   compose_fleet_up <run-dir> <state-file>
#   compose_fleet_down <state-file>
#
# 1. In a machine-wide cargo slot, take the shared Docker lock
#    ($HOME/.cache/k2-dist-docker.lock; order: slot, then lock, never the
#    other way round) and build Linux k2-daemon and k2-fake-agent from
#    `git archive <sha>` in the gate's build image (k2-lab-gate:2), capped
#    at --memory 10g, 3 CPUs, CARGO_BUILD_JOBS=2, with the gate's warm target
#    volume. The slot is given back after the build; the lock is kept for
#    the whole compose run (one Docker job at a time) and released by
#    compose_fleet_down.
# 2. Build k2-lab-fleet:<sha12> (scripts/lab/docker/fleet.Dockerfile:
#    apt-get and a copy, no compile) and run deploy/compose/compose.lab-fleet.yaml
#    through tests/lab/labkit/fleet.py (up_compose). Containers: 3g, 2 CPUs.
#
# lab-runs/.docker-hold (a coordinator hold) refuses before any Docker
# command. Builds the committed sha: commit before a compose run.

COMPOSE_LOCK="$HOME/.cache/k2-dist-docker.lock"
COMPOSE_PROJECT="k2-lab-fleet-$$"
COMPOSE_HELD=""

compose_fleet_up() {
    local run_dir=$1 state=$2 sha out image root
    if [ -f "$LAB_REPO/lab-runs/.docker-hold" ]; then
        echo "fleet compose: refusing to run (held by the lab, before any Docker command): $(head -1 "$LAB_REPO/lab-runs/.docker-hold")" >&2
        return 1
    fi
    sha=$(git -C "$LAB_REPO" rev-parse HEAD)
    out="$run_dir/fleet-image"
    mkdir -p "$out/bin" "$HOME/.cache"
    image="k2-lab-fleet:${sha:0:12}"
    echo "fleet compose: waiting for a cargo slot, then the Docker lock ($COMPOSE_LOCK)" >&2
    # Inside the slot: take the lock (kept after the slot ends), then build.
    if ! lab_slot bash -c '
        lock=$1 repo=$2 common=$3 sha=$4 out=$5 name=$6
        until mkdir "$lock" 2>/dev/null; do sleep 20; done
        echo "$name" > "$lock/owner"
        echo "fleet compose: holding the Docker lock from $(date -u +%H:%M:%SZ)" >&2
        docker run --rm --name "$name" \
            --memory=10g --memory-swap=10g --cpus 3 \
            -e CARGO_BUILD_JOBS=2 -e CARGO_TARGET_DIR=/cache/target \
            -e GIT_DIR=/src/git -e GIT_WORK_TREE=/src/wt \
            -e GIT_CONFIG_COUNT=1 -e GIT_CONFIG_KEY_0=safe.directory -e GIT_CONFIG_VALUE_0="*" \
            -v "$repo:/src/wt:ro" -v "$common:/src/git:ro" \
            -v k2-lab-gate-target:/cache/target -v k2-lab-gate-cargo-registry:/usr/local/cargo/registry \
            -v "$out/bin:/out" \
            k2-lab-gate:2 \
            bash -c "set -e; mkdir -p /build && git archive \"\$0\" | tar -x -C /build && cd /build \
                     && cargo build --locked -p k2-daemon --bin k2-daemon \
                     && cargo build --locked -p k2-fake-agent --bins \
                     && cp /cache/target/debug/k2-daemon /cache/target/debug/k2-fake-agent /out/" "$sha"
    ' _ "$COMPOSE_LOCK" "$LAB_REPO" "$(git -C "$LAB_REPO" rev-parse --git-common-dir | (cd "$LAB_REPO" && xargs realpath))" \
        "$sha" "$out" "k2-lab-fleet-build-$$" >"$run_dir/fleet-build.log" 2>&1; then
        echo "fleet compose: the Linux build failed (fleet-build.log): $(tail -3 "$run_dir/fleet-build.log" | tr '\n' ' ')" >&2
        return 1
    fi
    COMPOSE_HELD=1
    docker build -q -t "$image" -f "$LAB_REPO/scripts/lab/docker/fleet.Dockerfile" "$out" >"$run_dir/fleet-image.log" 2>&1 \
        || { echo "fleet compose: building $image failed (fleet-image.log)" >&2; return 1; }
    {
        echo "image: $image ($(docker image inspect "$image" --format '{{.Id}}'))"
        echo "sha: $sha"
        echo "compose: deploy/compose/compose.lab-fleet.yaml, project $COMPOSE_PROJECT"
        echo "build: k2-lab-gate:2, --memory=10g --cpus 3, CARGO_BUILD_JOBS=2; containers 3g, 2 CPUs each"
        echo "docker: $(docker version --format '{{.Server.Version}}') ($(docker info --format '{{.NCPU}} CPU, {{.MemTotal}} bytes'))"
    } >"$run_dir/fleet-compose.txt"
    root=$(mktemp -d /tmp/k2lab-fleet-XXXXXX) && root=$(realpath "$root")
    python3 -I - "$run_dir" "$state" "$root" "$image" "$COMPOSE_PROJECT" "$LAB_REPO/deploy/compose/compose.lab-fleet.yaml" <<'PY' || return 1
import os, sys
sys.path.insert(0, os.path.join(os.environ["LAB_REPO"], "tests", "lab"))
from labkit import fleet
run_dir, state, root, image, project, compose_file = sys.argv[1:]
try:
    st = fleet.up_compose(run_dir, state, root, image, project, compose_file)
except fleet.FleetError as e:
    print("fleet compose: %s" % e, file=sys.stderr)
    sys.exit(1)
for name, p in st["procs"].items():
    print("fleet: %s (%s) container %s port %s" % (name, p["role"], p["pid"], p["port"]), file=sys.stderr)
PY
}

compose_fleet_down() {
    local state=$1 root
    if [ -f "$state" ]; then
        python3 -I "$LAB_REPO/tests/lab/lab.py" fleet down --state "$state"
        root=$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["root"])' "$state" 2>/dev/null)
        case "$root" in /tmp/k2lab-fleet-*|/private/tmp/k2lab-fleet-*) chmod -R u+w "$root" 2>/dev/null; rm -rf "$root" ;; esac
    fi
    # Release the Docker lock only when this run took it.
    if [ "$(cat "$COMPOSE_LOCK/owner" 2>/dev/null)" = "k2-lab-fleet-build-$$" ]; then
        rm -f "$COMPOSE_LOCK/owner"
        rmdir "$COMPOSE_LOCK" 2>/dev/null
    fi
    COMPOSE_HELD=""
}
