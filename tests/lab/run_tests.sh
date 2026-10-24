#!/usr/bin/env bash
# The lab's own tests: flows (in sync with the PRD, schema, checker), the
# report writer, S1's client model, and the fake agent (cargo test).
#
#   tests/lab/run_tests.sh
set -euo pipefail
REPO=$(cd "$(dirname "$0")/../.." && pwd)
cd "$REPO"
for v in $(env | grep -oE '^K2(SO)?_[A-Z0-9_]+' || true); do unset "$v"; done

echo "== flows in sync with the PRD"
python3 -I tests/lab/extract_flows.py --check
echo "== python: flow checker, report writer, S1 client model"
python3 -I -m unittest discover -s tests/lab -p 'test_*.py' -t tests/lab
echo "== cargo test -p k2-fake-agent"
source scripts/lab/lib/toolchain.sh
lab_toolchain "$REPO"
cargo test -p k2-fake-agent
