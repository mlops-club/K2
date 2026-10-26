# The lab's fleet image (LAB_FLEET_BACKEND=compose): today's k2-daemon and
# k2-fake-agent, Linux builds made by scripts/lab/lib/compose.sh in the
# capped gate container, copied onto the same runtime base as
# packaging/oci/Dockerfile. TEST ONLY: the fake agent is every agent CLI
# (shims in /opt/k2-shim); never pushed.
#
# Build context: a directory holding bin/k2-daemon and bin/k2-fake-agent.
FROM debian:bookworm-slim@sha256:7ff6ba31527b9e336162fa061746c366c4b2032f4d2cff2078bea43f78efe3f8
# curl: health checks. git: workspaces. procps: the lab's monitor lists agent
# CLIs with ps. tini: PID 1, reaps agent CLIs.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl git libssl3 procps tini \
 && rm -rf /var/lib/apt/lists/*
COPY bin/k2-daemon bin/k2-fake-agent /usr/bin/
RUN /usr/bin/k2-fake-agent --install-shims /opt/k2-shim
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/usr/bin/k2-daemon"]
