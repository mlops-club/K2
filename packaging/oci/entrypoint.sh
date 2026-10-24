#!/bin/sh
# Container entrypoint for k2-daemon (packaging/oci/Dockerfile).
#
# A random UID (`--user 54321:0`, OpenShift) has no passwd entry and may not
# own $HOME. The state directory is group 0 and group-writable, so make sure
# it exists and is ours to write, then hand over to the daemon.
set -eu
: "${HOME:=/var/lib/k2}"
if ! [ -w "$HOME" ]; then
    echo "k2-entrypoint: HOME=$HOME is not writable by uid $(id -u) gid $(id -g)" >&2
    echo "  mount a volume there, or run with group 0 (--user <uid>:0)" >&2
    exit 1
fi
exec "$@"
