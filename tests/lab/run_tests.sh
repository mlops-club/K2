#!/usr/bin/env bash
# The lab's own tests: flows (in sync with the PRD, schema, checker), the
# report writer, S1's client model, and the fake agent (cargo test).
#
#   tests/lab/run_tests.sh
set -euo pipefail
REPO=$(cd "$(dirname "$0")/../.." && pwd)
cd "$REPO"
# Keep the machine-wide cargo slot's own knobs (K2_DIST_PRIORITY, K2_DIST_SLOTS):
# only ~/.cache/k2-dist-cargo-slot.sh reads them; suites and the daemon start
# under env -i anyway.
for v in $(env | grep -oE '^K2(SO)?_[A-Z0-9_]+' | grep -vxE 'K2_DIST_(PRIORITY|SLOTS)' || true); do unset "$v"; done

echo "== flows in sync with the PRD"
python3 -I tests/lab/extract_flows.py --check
echo "== python: flow checker, report writer, S1 client model"
python3 -I -m unittest discover -s tests/lab -p 'test_*.py' -t tests/lab
echo "== cargo test -p k2-fake-agent (throwaway HOME; 3 jobs, 3 test threads: the Mac is shared)"
source scripts/lab/lib/toolchain.sh
lab_toolchain "$REPO"
lab_cargo test -p k2-fake-agent -- --test-threads=3
