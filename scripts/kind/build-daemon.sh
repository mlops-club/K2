#!/usr/bin/env bash
# Build the k2-daemon image from a source tree, with the compile capped.
#
#   scripts/kind/build-daemon.sh <source tree> <tag>
#   scripts/kind/build-daemon.sh ../dist-link k2-daemon:dist
#
# packaging/oci/build.sh compiles inside `docker build`, where BuildKit
# can't cap memory; on 2026-10-24 a cold build wedged the shared Docker VM
# (kind starved, the owner's pod unreachable). Here the compile is a plain
# `docker run` with a cgroup cap, the Linux binaries land in a local dir,
# and a runtime-only image (deploy/kind/Dockerfile.daemon-prebuilt, the
# runtime stage of packaging/oci/Dockerfile) copies them in. The agents
# images build FROM that, so the binary is compiled once.
#
#   1. k2-kind-toolchain:1.99.1 (deploy/kind/Dockerfile.toolchain), if missing
#   2. docker run --memory 4g --memory-swap 4g --cpus 2, CARGO_BUILD_JOBS=2:
#      k2-ptyd, then k2-daemon embedding it (the dev profile of
#      packaging/oci/Dockerfile); the source mounted read-only, cargo's
#      registry and target dir in named volumes (incremental next time)
#   3. docker build -f deploy/kind/Dockerfile.daemon-prebuilt → <tag>
#
# Holds the k2-dist Docker lock and a cargo slot for the whole run.
# K2_KIND_BUILD_MEMORY, K2_KIND_BUILD_CPUS and CARGO_BUILD_JOBS override the
# caps (lower them if the compile is OOM-killed; never raise the memory).
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
case "${1:-}" in -h|--help|"") sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;; esac
src=$(cd "${1:?source tree}" && pwd)
tag=${2:?tag, e.g. k2-daemon:dist}
[ -f "$src/crates/k2-daemon/Cargo.toml" ] && [ -f "$src/packaging/oci/entrypoint.sh" ] \
    || { echo "build-daemon.sh: $src is not a K2 tree" >&2; exit 2; }
mem=${K2_KIND_BUILD_MEMORY:-4g} cpus=${K2_KIND_BUILD_CPUS:-2} jobs=${CARGO_BUILD_JOBS:-2}
toolchain=k2-kind-toolchain:1.99.1
out="${TMPDIR:-/tmp}/k2-kind/build/${tag//[:\/]/-}"

# The cargo slot first (it may wait), then the Docker lock.
if [ -x "$HOME/.cache/k2-dist-cargo-slot.sh" ] && [ -z "${K2_KIND_IN_SLOT:-}" ]; then
    K2_KIND_IN_SLOT=1 exec "$HOME/.cache/k2-dist-cargo-slot.sh" "$0" "$@"
fi
DOCKER_LOCK="$HOME/.cache/k2-dist-docker.lock"
mkdir -p "$HOME/.cache"
until mkdir "$DOCKER_LOCK" 2>/dev/null; do say "waiting for $DOCKER_LOCK"; sleep 20; done
trap 'rmdir "$DOCKER_LOCK" 2>/dev/null' EXIT

if ! docker image inspect "$toolchain" >/dev/null 2>&1; then
    say "building $toolchain (deploy/kind/Dockerfile.toolchain)"
    DOCKER_BUILDKIT=1 docker build -q -t "$toolchain" - < "$KIND_REPO/deploy/kind/Dockerfile.toolchain" >/dev/null
fi

rm -rf "$out" && mkdir -p "$out/usr/bin"
say "compiling k2-ptyd and k2-daemon from $src ($(git -C "$src" rev-parse --short HEAD)) in a capped container: --memory $mem --cpus $cpus, CARGO_BUILD_JOBS=$jobs"
docker run --rm --name "k2-kind-build-$$" \
    --memory "$mem" --memory-swap "$mem" --cpus "$cpus" \
    -e CARGO_BUILD_JOBS="$jobs" -e CARGO_TARGET_DIR=/target \
    -e CARGO_PROFILE_RELEASE_LTO=false -e CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 -e CARGO_PROFILE_RELEASE_DEBUG=false \
    -v "$src:/src:ro" -v "$out/usr/bin:/out" \
    -v k2-kind-cargo-registry:/usr/local/cargo/registry -v k2-kind-cargo-git:/usr/local/cargo/git \
    -v k2-kind-target-dev:/target \
    -w /src "$toolchain" sh -euxc '
        cargo build --release --locked -p k2-ptyd --bin k2-ptyd
        K2_PTYD_EMBED=/target/release/k2-ptyd K2_REQUIRE_PTYD_EMBED=1 \
            cargo build --release --locked -p k2-daemon --bin k2-daemon
        install -m 0755 /target/release/k2-daemon /target/release/k2-ptyd /out/'
cp "$src/packaging/oci/entrypoint.sh" "$out/k2-entrypoint"
chmod 0755 "$out/k2-entrypoint"

say "building $tag (deploy/kind/Dockerfile.daemon-prebuilt; no compile)"
DOCKER_BUILDKIT=1 docker build -q -f "$KIND_REPO/deploy/kind/Dockerfile.daemon-prebuilt" -t "$tag" \
    --label "org.opencontainers.image.revision=$(git -C "$src" rev-parse HEAD)" \
    --label "org.opencontainers.image.version=$(sed -n 's/^version = "\(.*\)"/\1/p' "$src/crates/k2-daemon/Cargo.toml" | head -1)" \
    "$out" >/dev/null
say "built $tag from $src"
echo "$tag"
