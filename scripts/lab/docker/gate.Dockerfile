# The Linux gate's build box: scripts/test-gate-linux.sh runs in here,
# unchanged, as root. Base pinned by digest (rust 1.99.1, Debian bookworm);
# packages are what .github/workflows/checks.yml installs for the C/C++
# build scripts, plus nftables for the gate's root-only cell_egress test and
# lsof, which k2-core's daemon_pty fd tests run to count /dev/ptmx fds, and
# uuid-runtime (/usr/bin/uuidgen), which a sidecar test's fake CLI calls.
FROM rust:1.99.1-bookworm@sha256:48b7f435b8fc6378ce43b44f15e3a47fd8919b2825f835c39bd4bd3da50af5b8
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential cmake clang pkg-config libssl-dev nftables lsof uuid-runtime \
 && rm -rf /var/lib/apt/lists/*
