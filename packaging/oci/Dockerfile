# syntax=docker/dockerfile:1.7
# K2 in a container (PRD "K2 Distributed Runtime", stage P0, "Container"
# topology): today's daemon, controller plus in-process agent host
# (K2_ROLE=all), as one image for Docker Compose and Kubernetes.
#
#   packaging/oci/build.sh            # → k2-daemon:dev, for this machine's arch
#
# Binaries live under /usr/bin, so boot_status classifies them as
# install_kind="package" and the daemon never swaps itself: a new image tag
# is the update. No root helper (there is no sudo in a container), so the
# capability report says off/delegated for its features.
#
# Never pushed anywhere from this repo; the end-state name is
# ghcr.io/alakazam-211/k2-daemon:<version> (deploy/README.md).

ARG RUST_IMAGE=rust:1.99.1-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim

# ── toolchain: what the daemon's C/C++ deps need (as daemon-binaries.yml) ──
FROM ${RUST_IMAGE} AS toolchain
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential cmake clang pkg-config libssl-dev \
 && rm -rf /var/lib/apt/lists/* \
 && rustup component add clippy

# ── build: k2-ptyd first, then k2-daemon embedding it (P84) ──────────────
FROM toolchain AS build
# Parallel rustc jobs; keep it modest on a shared laptop (build.sh passes it).
ARG CARGO_BUILD_JOBS=4
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}
# dev (default): Cargo's release profile without LTO, 16 codegen units and no
# debug info, so k2-daemon:dev builds in minutes. ship: Cargo.toml's release
# profile untouched (fat LTO, one codegen unit), as release binaries are built.
ARG K2_BUILD_PROFILE=dev
WORKDIR /src
COPY . .
RUN --mount=type=cache,id=k2-oci-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=k2-oci-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=k2-oci-target-${K2_BUILD_PROFILE},target=/src/target \
    set -eux; \
    case "$K2_BUILD_PROFILE" in \
      dev) export CARGO_PROFILE_RELEASE_LTO=false CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 CARGO_PROFILE_RELEASE_DEBUG=false ;; \
      ship) ;; \
      *) echo "K2_BUILD_PROFILE must be dev or ship, not $K2_BUILD_PROFILE" >&2; exit 2 ;; \
    esac; \
    cargo build --release --locked -p k2-ptyd --bin k2-ptyd; \
    K2_PTYD_EMBED=/src/target/release/k2-ptyd K2_REQUIRE_PTYD_EMBED=1 \
      cargo build --release --locked -p k2-daemon --bin k2-daemon; \
    install -D -m 0755 target/release/k2-daemon /out/usr/bin/k2-daemon; \
    install -D -m 0755 target/release/k2-ptyd /out/usr/bin/k2-ptyd

# ── runtime ───────────────────────────────────────────────────────────────
FROM ${RUNTIME_IMAGE}
# curl: the Compose healthcheck. git: workspaces. ca-certificates: TLS out.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl git libssl3 tini \
 && rm -rf /var/lib/apt/lists/*
# UID 10000 as deploy/kubernetes/core/controller.yaml. State is group 0 and
# group-writable, so any UID with GID 0 (OpenShift style, `--user 54321:0`)
# can boot too.
RUN useradd --uid 10000 --gid 0 --home-dir /var/lib/k2 --no-create-home --shell /usr/sbin/nologin k2 \
 && install -d -m 0770 -o 10000 -g 0 /var/lib/k2
COPY --from=build /out/ /
COPY packaging/oci/entrypoint.sh /usr/bin/k2-entrypoint
# HOME=/var/lib/k2 puts K2's state in /var/lib/k2/.k2 until K2_HOME lands
# (P0 `home` task). Defaults below are today's behaviour plus the fixed port.
ENV HOME=/var/lib/k2 \
    K2_ROLE=all \
    K2_LISTEN=lan \
    K2_DAEMON_PORT=7420
VOLUME ["/var/lib/k2"]
WORKDIR /var/lib/k2
USER 10000:0
EXPOSE 7420
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/bin/k2-entrypoint"]
CMD ["/usr/bin/k2-daemon"]
