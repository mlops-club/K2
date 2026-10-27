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
#   ran with --memory=6g --memory-swap=6g and the gate's K2_GATE_LOW_MEMORY=1
#   (one cargo build job, 256 codegen units; debuginfo is already 0), 3 CPUs
#   and the gate's own K2_GATE_THREADS (3) for the tests. The Docker lock
#   keeps anything else out of Docker meanwhile. If it is OOM-killed, cut
#   rustc's memory or the parallelism, never raise the cap on your own.
#   Since 2026-10-25 the VM is 14 GB: LAB_GATE_MEMORY defaults to 10g and
#   LAB_GATE_LOW_MEMORY to 0 (1 sets K2_GATE_LOW_MEMORY=1); change either
#   only when the coordinator says so.
# - the tests run as the unprivileged user k2gate (K2_GATE_TEST_USER;
#   created at container start, owning cargo, rustup and the target
#   volume); the gate's root-only allowlisted run stays root.
# - disk: the VM's disk is shared and k2-daemon's ~160 integration test
#   binaries need ~25 GB linked at once (ENOSPC at link on 2026-10-25), so
#   the gate runs with K2_GATE_BATCH=${LAB_GATE_BATCH:-20} (steps of 20 test
#   targets, linked executables removed after each step; 0 = one
#   `cargo test -p` per package). LAB_GATE_MIN_FREE_GB passes the gate's
#   K2_GATE_MIN_FREE_GB (default 20); lower it only when the coordinator says.
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
image=k2-lab-gate:3
target_vol=k2-lab-gate-target
registry_vol=k2-lab-gate-cargo-registry
# LAB_GATE_TARGET_DIR=<dir on the Mac>: the cargo target as a bind mount
# from that dir instead of the named volume, so the build's disk is the
# Mac's, not the shared VM's (coordinator, 2026-10-27, VM at 7.8G free).
# The VM then holds only the image and the container layer; set
# LAB_GATE_MIN_FREE_GB to match. The caller removes the dir afterwards.
target_mount=$target_vol
if [ -n "${LAB_GATE_TARGET_DIR:-}" ]; then
    mkdir -p "$LAB_GATE_TARGET_DIR" || { echo "lab: can't create LAB_GATE_TARGET_DIR $LAB_GATE_TARGET_DIR"; exit 2; }
    target_mount=$(cd "$LAB_GATE_TARGET_DIR" && pwd -P)
fi
lock="$HOME/.cache/k2-dist-docker.lock"
# 2026-10-25: the Docker VM is 14 GB; the coordinator caps gate containers at
# 10g, without the low-memory build (the wave-1 gate compiled k2-daemon so).
mem=${LAB_GATE_MEMORY:-10g}
low=${LAB_GATE_LOW_MEMORY:-0}
case "$mem" in [1-9]g|1[0-9]g) ;; *) echo "lab: LAB_GATE_MEMORY is <N>g, 1g..19g (got '$mem')"; exit 2 ;; esac
case "$low" in 0|1) ;; *) echo "lab: LAB_GATE_LOW_MEMORY is 0 or 1 (got '$low')"; exit 2 ;; esac
low_env=()
batch=${LAB_GATE_BATCH:-20}
case "$batch" in ''|*[!0-9]*) echo "lab: LAB_GATE_BATCH is a number (got '$batch')"; exit 2 ;; esac
low_desc="normal build (K2_GATE_LOW_MEMORY unset)"
if [ "$low" = 1 ]; then
    low_env=(-e K2_GATE_LOW_MEMORY=1)
    low_desc="K2_GATE_LOW_MEMORY=1 (1 build job, 256 codegen units, the profile's opt-level, debuginfo 0)"
fi

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
    echo "limits: --memory=$mem --memory-swap=$mem --cpus 3, $low_desc, K2_GATE_THREADS=${LAB_TEST_THREADS:-3}"
    echo "user: tests as k2gate (uid 10001, K2_GATE_TEST_USER); the root-only allowlisted runs as root"
    echo "volumes: $target_mount (target), $registry_vol (cargo registry)"
    echo "batch: K2_GATE_BATCH=$batch (integration test targets per cargo step; 0 = one cargo test -p)"
    echo "min free: K2_GATE_MIN_FREE_GB=${LAB_GATE_MIN_FREE_GB:-20 (the default)}"
} >"$run/topology/linux-gate.txt"

# The gate writes its per-package logs to <workdir>/logs; a symlink into
# this bind mount keeps them in the bundle (the container is --rm).
mkdir -p "$run/parity/linux-gate-logs"

docker run --rm --name "$name" \
    --memory="$mem" --memory-swap="$mem" --cpus 3 \
    ${low_env[@]+"${low_env[@]}"} \
    --cap-add NET_ADMIN -e K2_GATE_THREADS="${LAB_TEST_THREADS:-3}" \
    -e K2_GATE_BATCH="$batch" \
    ${LAB_GATE_MIN_FREE_GB:+-e K2_GATE_MIN_FREE_GB="$LAB_GATE_MIN_FREE_GB"} \
    -e GIT_DIR=/src/git -e GIT_WORK_TREE=/src/wt \
    -e GIT_CONFIG_COUNT=1 -e GIT_CONFIG_KEY_0=safe.directory -e GIT_CONFIG_VALUE_0='*' \
    -v "$repo:/src/wt:ro" -v "$common:/src/git:ro" \
    -v "$target_mount:/cache/target" -v "$registry_vol:/usr/local/cargo/registry" \
    -v "$run/parity/linux-gate-logs:/out/logs" \
    -e K2_GATE_TEST_USER=k2gate \
    "$image" \
    bash -c 'rm -rf /gate/work && mkdir -p /gate/work && ln -s /cache/target /gate/work/target \
             && ln -s /out/logs /gate/work/logs \
             && { id -u k2gate >/dev/null 2>&1 || useradd --uid 10001 --create-home k2gate; } \
             && chown -R k2gate /usr/local/cargo /usr/local/rustup /cache/target \
             && exec bash /src/wt/scripts/test-gate-linux.sh "$0" /gate/work' "$sha"
