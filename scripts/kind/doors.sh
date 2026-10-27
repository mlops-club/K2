#!/usr/bin/env bash
# Check the Mac's doors (the proxy containers) of every K2 variant on kind,
# and bring back the ones that are down:
#
#   scripts/kind/doors.sh            # start stopped proxies, set --restart always, check each port
#   scripts/kind/doors.sh --check    # report only, change nothing
#   scripts/kind/doors.sh --variant=dist-fake ...   # only these (agents, fake, dist, dist-fake)
#
# A proxy is never replaced or removed here (forward.sh does that); a
# stopped one is started as it is. Variants with no proxy container are
# skipped. Exit 1 when a proxy that exists is still not running afterwards.
# Run it after Docker Desktop restarts.
set -euo pipefail
# shellcheck source=scripts/kind/lib.sh
source "$(dirname "$0")/lib.sh"
fix=1 variants=""
for a in "$@"; do
    case "$a" in
        --check) fix=0 ;;
        --variant=*) variants="$variants ${a#--variant=}" ;;
        -h|--help) sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "doors.sh: unknown argument $a (see --help)" >&2; exit 2 ;;
    esac
done
rc=0
for v in ${variants:-agents fake dist-fake dist}; do
    kind_variant "$v"
    docker inspect "$KIND_PROXY" >/dev/null 2>&1 || continue
    if [ "$fix" = 1 ]; then
        line=$(kind_ensure_proxy) || rc=1
    else
        line="$KIND_PROXY: $(docker inspect -f '{{.State.Status}}, restart {{.HostConfig.RestartPolicy.Name}}' "$KIND_PROXY")"
    fi
    phase=""
    for _ in $(seq 1 10); do   # a just-started socat needs a moment
        phase=$(curl -s --connect-timeout 2 --max-time 5 "http://127.0.0.1:$KIND_PORT/boot-status" \
            | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("phase","?"))' 2>/dev/null) && break
        [ "$fix" = 1 ] || break
        sleep 1
    done
    echo "$line; 127.0.0.1:$KIND_PORT ($KIND_NS): ${phase:-unreachable}"
    [ "$(docker inspect -f '{{.State.Running}}' "$KIND_PROXY")" = true ] || rc=1
done
exit $rc
