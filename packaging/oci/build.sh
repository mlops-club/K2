#!/usr/bin/env bash
# Build the K2 container image for this machine's architecture.
#
#   packaging/oci/build.sh [tag]        # default k2-daemon:dev
#
# Local only: never pushes. Cargo's registry and target dir are BuildKit
# cache mounts, so a rebuild after a small change is incremental.
set -euo pipefail
tag=${1:-k2-daemon:dev}
root=$(cd "$(dirname "$0")/../.." && pwd)
DOCKER_BUILDKIT=1 docker build \
    -f "$root/packaging/oci/Dockerfile" \
    -t "$tag" \
    --label "org.opencontainers.image.revision=$(git -C "$root" rev-parse HEAD)" \
    --label "org.opencontainers.image.version=$(sed -n 's/^version = "\(.*\)"/\1/p' "$root/crates/k2-daemon/Cargo.toml" | head -1)" \
    "$root"
echo "$tag"
