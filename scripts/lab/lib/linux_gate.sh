#!/usr/bin/env bash
# The Linux gate in Docker (owner decision, 2026-10-24): scripts/test-gate-linux.sh
# <sha>, unchanged, as root, in scripts/lab/docker/gate.Dockerfile.
#
#   linux_gate.sh <repo> <git-common-dir> <sha> <run-dir> <name>
#
# prove.sh runs this INSIDE a machine-wide cargo slot (lab_slot), so the
# order is always: cargo slot, then the shared Docker lock. Never the other
# way round (a worker holding the lock while waiting for a slot could
# deadlock with one holding a slot while waiting for the lock).
#
# Limits (coordinator, 2026-10-24; the 8 GB Docker VM is shared with the
# owner's kind cluster):
# - one Docker-heavy job at a time: $HOME/.cache/k2-dist-docker.lock, taken
#   with mkdir, released on exit, INT or TERM (never by a run still waiting);
# - the container: a Linux k2-daemon compile needs about 5 GB, so this one
#   runs with --memory=6g --memory-swap=6g and ONE cargo build job (a
#   mounted CARGO_HOME/config.toml, since the gate's clean_cargo starts
#   cargo under env -i and drops CARGO_BUILD_JOBS), 3 CPUs and the gate's own
#   K2_GATE_THREADS (3) for the tests. The Docker lock keeps anything else
#   out of Docker meanwhile. If it is OOM-killed, lower the parallelism,
#   never the cap.
# - the image build only runs apt-get (no compile); BuildKit has no
#   --memory flag, so the lock is its only limit.
#
# Mounts: Docker Desktop cannot mount at a target under /Users, so the repo
# is /src/wt and its git dir /src/git (read-only); GIT_DIR/GIT_WORK_TREE let
# the gate's `git rev-parse` and `git archive <sha>` work there. The target
# dir and cargo registry are named volumes (warm across runs); the gate's
# workdir is fresh and its `target` is a symlink into the volume (the gate
# removes the link, not the cache). Nothing from $HOME is mounted.
set -uo pipefail
repo=$1 common=$2 sha=$3 run=$4 name=$5
image=k2-lab-gate:1
target_vol=k2-lab-gate-target
registry_vol=k2-lab-gate-cargo-registry
lock="$HOME/.cache/k2-dist-docker.lock"

# A coordinator hold on Docker (e.g. a Docker Desktop restart): while
# lab-runs/.docker-hold exists, start nothing in Docker and give the slot
# back at once. The first line is matched by parity.sh as "did not run".
if [ -f "$repo/lab-runs/.docker-hold" ]; then
    echo "refusing to run (held by the lab, before any Docker command): $(head -1 "$repo/lab-runs/.docker-hold")"
    exit 2
fi

mkdir -p "$HOME/.cache"
until mkdir "$lock" 2>/dev/null; do sleep 20; done
trap 'rmdir "$lock"' EXIT
trap 'exit 143' INT TERM
echo "lab: holding the shared Docker lock ($lock) from $(date -u +%H:%M:%SZ)"

docker build -q -t "$image" -f "$repo/scripts/lab/docker/gate.Dockerfile" "$repo/scripts/lab/docker" \
    >"$run/parity/linux-gate.image.log" 2>&1 \
    || { echo "lab: building the gate image failed"; cat "$run/parity/linux-gate.image.log"; exit 3; }
{
    echo "image: $image ($(docker image inspect "$image" --format '{{.Id}}'))"
    echo "base: $(grep -m1 '^FROM' "$repo/scripts/lab/docker/gate.Dockerfile" | cut -d' ' -f2)"
    echo "sha: $sha"
    echo "docker: $(docker version --format '{{.Server.Version}}') ($(docker info --format '{{.NCPU}} CPU, {{.MemTotal}} bytes'))"
    echo "limits: --memory=6g --memory-swap=6g --cpus 3, cargo build jobs 1 (config.toml), K2_GATE_THREADS=${LAB_TEST_THREADS:-3}"
    echo "volumes: $target_vol (target), $registry_vol (cargo registry)"
} >"$run/topology/linux-gate.txt"

cargo_cfg="$run/topology/linux-gate.cargo-config.toml"
printf '[build]\njobs = 1\n' >"$cargo_cfg"

docker run --rm --name "$name" \
    --memory=6g --memory-swap=6g --cpus 3 \
    -v "$cargo_cfg:/usr/local/cargo/config.toml:ro" \
    --cap-add NET_ADMIN -e K2_GATE_THREADS="${LAB_TEST_THREADS:-3}" \
    -e GIT_DIR=/src/git -e GIT_WORK_TREE=/src/wt \
    -e GIT_CONFIG_COUNT=1 -e GIT_CONFIG_KEY_0=safe.directory -e GIT_CONFIG_VALUE_0='*' \
    -v "$repo:/src/wt:ro" -v "$common:/src/git:ro" \
    -v "$target_vol:/cache/target" -v "$registry_vol:/usr/local/cargo/registry" \
    "$image" \
    bash -c 'rm -rf /gate/work && mkdir -p /gate/work && ln -s /cache/target /gate/work/target \
             && exec bash /src/wt/scripts/test-gate-linux.sh "$0" /gate/work' "$sha"
