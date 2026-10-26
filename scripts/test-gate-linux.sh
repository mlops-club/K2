#!/bin/bash
# Linux test gate (quiet-gate PRD §5.9, 0.45.1).
#
#   scripts/test-gate-linux.sh <sha> [workdir]
#
# Exports <sha> with `git archive` into a FRESH dir (never a copied or mixed
# tree), builds with its own CARGO_TARGET_DIR, and runs
#   cargo build -p k2-ptyd --bin k2-ptyd   (k2-core's ptyd_backend execs it)
#   cargo test -p k2-core   --no-fail-fast
#   cargo test -p k2-daemon --no-fail-fast
# under a clean `env -i` (HOME and TMPDIR = fresh temp dirs, no SHELL, no
# K2_*/K2SO_* vars). As root it also runs the root-only ignored tests on the
# allowlist (live nft check).
#
# Exit 0 only when every binary reports 0 failed AND the ignored tests are
# exactly the allowlist below. Prints a summary table, the failure list and
# the ignored list. The workdir (default /root/k2-gate-<sha>-<epoch>) is
# removed afterwards unless K2_GATE_KEEP=1.
#
# Env knobs:
#   K2_GATE_MIN_FREE_GB  refuse to start below this much free disk (default 20)
#   K2_GATE_DEBUG        cargo debuginfo level for the test build (default 0;
#                        panics still carry file:line and messages)
#   K2_GATE_KEEP=1       keep the workdir (logs, target) after the run
#   K2_GATE_THREADS      --test-threads (default: libtest's choice)
#   K2_GATE_LOW_MEMORY=1 for a memory-capped box (the lab's 10 GB Docker
#                        container): one cargo build job and 256 codegen
#                        units. The profile's opt-level stays: at opt-level 0
#                        k2-daemon's async stacks overflow (115 test binaries
#                        and every spawned test daemon aborted on 3912ab2ff),
#                        so a cheaper build would not be the same tests.
#                        Debuginfo is already 0 (K2_GATE_DEBUG) and
#                        incremental off. Unset (the default), nothing changes.
#   K2_GATE_BATCH=N      for a box with little disk (the lab's Docker VM):
#                        run each package's tests in steps instead of one
#                        `cargo test -p`: lib + bins, then the
#                        integration tests N targets at a time, then the
#                        doctests, removing the linked test executables after
#                        each step. Same tests, same flags; k2-daemon's ~160
#                        test binaries otherwise need ~25 GB linked at once.
#                        Logs: <pkg>.log, <pkg>-tests-NN.log, <pkg>-doc.log.
#                        Unset or 0 (the default), one `cargo test -p`.
#   K2_GATE_REALPG_PORT / K2_GATE_REALPG_SOCK / K2_GATE_REALPG_BIN
#                        a throwaway Postgres 16 cluster (TCP port, socket dir,
#                        bin dir with psql): set all three and the gate RUNS
#                        the allowlisted real-Postgres smoke against it (a
#                        failure fails the gate); set only some and the gate
#                        refuses to start. Never point it at the box's own
#                        server: the smoke creates and drops roles.

set -uo pipefail

SHA=${1:?usage: test-gate-linux.sh <sha> [workdir]}
REPO=$(git -C "$(dirname "$0")/.." rev-parse --show-toplevel) || exit 2
FULL_SHA=$(git -C "$REPO" rev-parse --verify "$SHA^{commit}") || { echo "unknown sha $SHA"; exit 2; }
WORK=${2:-${HOME:-/root}/k2-gate-${FULL_SHA:0:8}-$(date +%s)}
MIN_FREE_GB=${K2_GATE_MIN_FREE_GB:-20}

# Ignored tests the gate accepts (anything else ignored fails the gate).
ALLOWLIST=(
  "tunnel::connector::tests::live_start_stop_roundtrip"            # real frpc vs live Connect; manual only
  "classify_routes::tests::checkbox_on_no_marker_reaches_model_path" # needs the bundled GGUF model
  "cell_egress::tests::refused_localhost_port_is_refused_while_a_cell_table_exists" # root + nft; run below as root
  "sql::tests::real_pg::real_pg_guest_doors_smoke"                  # needs a throwaway Postgres 16; run below when K2_GATE_REALPG_* is set
  "sql::tests::real_pg::real_pg_search_path_pin_holds"              # needs a throwaway Postgres 16; run below when K2_GATE_REALPG_* is set
)

# Root-only ignored tests the gate RUNS explicitly when it is root:
# "<package> <lib filter>"
ROOT_RUNS=(
  "k2-daemon cell_egress::tests::refused_localhost_port_is_refused_while_a_cell_table_exists"
)

avail_gb() { df --output=avail -BG "$1" | tail -1 | tr -dc '0-9'; }

# The real-Postgres smoke: all three knobs or none (a partial set is a
# misconfigured gate, never a silent skip).
REALPG_SET=0
for v in K2_GATE_REALPG_PORT K2_GATE_REALPG_SOCK K2_GATE_REALPG_BIN; do
  [ -n "${!v:-}" ] && REALPG_SET=$((REALPG_SET + 1))
done
if [ $REALPG_SET -ne 0 ] && [ $REALPG_SET -ne 3 ]; then
  echo "refusing to run: set all of K2_GATE_REALPG_PORT/SOCK/BIN or none"
  exit 2
fi

mkdir -p "$WORK" || exit 2
FREE=$(avail_gb "$WORK")
if [ "${FREE:-0}" -lt "$MIN_FREE_GB" ]; then
  echo "refusing to run: ${FREE}G free under $WORK, need ${MIN_FREE_GB}G (K2_GATE_MIN_FREE_GB)"
  exit 2
fi

SRC=$WORK/src
LOGS=$WORK/logs
mkdir -p "$SRC" "$LOGS" "$WORK/target"
git -C "$REPO" archive "$FULL_SHA" | tar -x -C "$SRC" || { echo "git archive failed"; exit 2; }
echo "gate: $FULL_SHA -> $SRC (free ${FREE}G)"

CARGO_BIN=$(command -v cargo) || { echo "cargo not on PATH"; exit 2; }
TOOL_PATH=$(dirname "$CARGO_BIN"):/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
CARGO_HOME_DIR=${CARGO_HOME:-$HOME/.cargo}
RUSTUP_HOME_DIR=${RUSTUP_HOME:-$HOME/.rustup}
THREAD_ARGS=()
[ -n "${K2_GATE_THREADS:-}" ] && THREAD_ARGS=(-- "--test-threads=$K2_GATE_THREADS")

# K2_GATE_TEST_USER=<user>: when the gate runs as root (a container), run
# the build and the package tests as this unprivileged user, so tests that
# check ownership see what they see on a developer's machine; the root-only
# allowlisted runs below stay root. The workdir is handed to that user.
RUN_AS=()
TEST_USER=${K2_GATE_TEST_USER:-}
if [ -n "$TEST_USER" ]; then
  [ "$(id -u)" -eq 0 ] || { echo "refusing to run: K2_GATE_TEST_USER needs the gate to run as root"; exit 2; }
  id -u "$TEST_USER" >/dev/null 2>&1 || { echo "refusing to run: no user $TEST_USER (K2_GATE_TEST_USER)"; exit 2; }
  command -v runuser >/dev/null || { echo "refusing to run: runuser not found (util-linux)"; exit 2; }
  # Logs are written by this (root) shell's redirects, so they stay root's.
  chown -R "$TEST_USER" "$SRC" "$WORK/target/" || { echo "chown of the workdir for $TEST_USER failed"; exit 2; }
  RUN_AS=(runuser -u "$TEST_USER" --)
  echo "gate: tests run as $TEST_USER (uid $(id -u "$TEST_USER")); root-only runs as root"
fi

LOW_MEM_ENV=()
if [ "${K2_GATE_LOW_MEMORY:-0}" = 1 ]; then
  LOW_MEM_ENV=(CARGO_BUILD_JOBS=1 CARGO_PROFILE_DEV_CODEGEN_UNITS=256 CARGO_PROFILE_TEST_CODEGEN_UNITS=256)
  echo "gate: K2_GATE_LOW_MEMORY=1: ${LOW_MEM_ENV[*]}"
fi

# Run cargo in a clean environment: only what the toolchain needs, a fresh
# HOME and TMPDIR per package, no SHELL, no K2_* / K2SO_* variables.
# Extra VAR=value pairs for one clean_cargo call: EXTRA_ENV=(...) clean_cargo ...
# AS=(...) clean_cargo ... runs it as someone else (default: RUN_AS).
EXTRA_ENV=()
clean_cargo() {
  local log=$1; shift
  local h t rc user
  local as=("${RUN_AS[@]+"${RUN_AS[@]}"}")
  [ "${AS_ROOT:-0}" = 1 ] && as=()
  user=$(id -un)
  [ ${#as[@]} -gt 0 ] && user=$TEST_USER
  h=$(mktemp -d "${TMPDIR:-/tmp}/k2-gate-home-XXXXXX")
  t=$(mktemp -d "${TMPDIR:-/tmp}/k2-gate-tmp-XXXXXX")
  [ ${#as[@]} -gt 0 ] && chown "$user" "$h" "$t"
  ( cd "$SRC" && ${as[@]+"${as[@]}"} env -i \
      HOME="$h" TMPDIR="$t" PATH="$TOOL_PATH" USER="$user" LANG=C.UTF-8 \
      CARGO_HOME="$CARGO_HOME_DIR" RUSTUP_HOME="$RUSTUP_HOME_DIR" \
      CARGO_TARGET_DIR="$WORK/target" CARGO_INCREMENTAL=0 \
      CARGO_PROFILE_DEV_DEBUG="${K2_GATE_DEBUG:-0}" CARGO_PROFILE_TEST_DEBUG="${K2_GATE_DEBUG:-0}" \
      ${LOW_MEM_ENV[@]+"${LOW_MEM_ENV[@]}"} \
      ${EXTRA_ENV[@]+"${EXTRA_ENV[@]}"} "$CARGO_BIN" "$@" ) > "$log" 2>&1
  rc=$?
  rm -rf "$h" "$t"
  return $rc
}

STATUS=0
# k2-core's ptyd_backend tests exec the real k2-ptyd from the target dir
# (target/debug/k2-ptyd, next to the test binary). `cargo test -p k2-core`
# builds k2-ptyd only as a library, so on a fresh build box the binary is
# missing and all of ptyd_backend fails. Build it first, into the same
# target dir, as CI does (.github/workflows/checks.yml).
echo "gate: cargo build -p k2-ptyd --bin k2-ptyd (ptyd_backend needs the binary)"
if ! clean_cargo "$LOGS/k2-ptyd-bin.log" build -p k2-ptyd --bin k2-ptyd; then
  STATUS=1
fi
# Linked test and bin executables in deps/ (the libraries stay, so a later
# step or run relinks instead of rebuilding; target/debug/<bin> copies stay).
drop_linked() {
  find "$WORK/target/debug/deps" -maxdepth 1 -type f -perm -u+x ! -name '*.so' -delete 2>/dev/null
}
BATCH=${K2_GATE_BATCH:-0}
for pkg in k2-core k2-daemon; do
  if [ "$BATCH" -gt 0 ] 2>/dev/null; then
    echo "gate: cargo test -p $pkg --no-fail-fast, in steps of $BATCH integration tests (K2_GATE_BATCH)"
    clean_cargo "$LOGS/$pkg.log" test -p "$pkg" --lib --bins --no-fail-fast "${THREAD_ARGS[@]}" || STATUS=1
    drop_linked
    # Integration test targets, as cargo discovers them: tests/*.rs and
    # tests/<dir>/main.rs.
    dir="$SRC/crates/$pkg/tests"
    TESTS=()
    if [ -d "$dir" ]; then
      while IFS= read -r t; do TESTS+=("$t"); done < <(
        { for f in "$dir"/*.rs; do [ -f "$f" ] && basename "$f" .rs; done
          for f in "$dir"/*/main.rs; do [ -f "$f" ] && basename "$(dirname "$f")"; done; } | sort)
    fi
    n=0
    for ((i = 0; i < ${#TESTS[@]}; i += BATCH)); do
      n=$((n + 1))
      args=()
      for t in "${TESTS[@]:i:BATCH}"; do args+=(--test "$t"); done
      clean_cargo "$LOGS/$pkg-tests-$(printf %02d $n).log" test -p "$pkg" "${args[@]}" --no-fail-fast "${THREAD_ARGS[@]}" || STATUS=1
      drop_linked
    done
    echo "gate: $pkg: ${#TESTS[@]} integration test targets in $n steps"
    clean_cargo "$LOGS/$pkg-doc.log" test -p "$pkg" --doc --no-fail-fast "${THREAD_ARGS[@]}" || STATUS=1
    drop_linked
  else
    echo "gate: cargo test -p $pkg --no-fail-fast"
    if ! clean_cargo "$LOGS/$pkg.log" test -p "$pkg" --no-fail-fast "${THREAD_ARGS[@]}"; then
      STATUS=1
    fi
  fi
  echo "gate: $pkg done (exit $([ $STATUS -eq 0 ] && echo 0 || echo non-zero so far), free $(avail_gb "$WORK")G)"
done

if [ "$(id -u)" -eq 0 ]; then
  for entry in "${ROOT_RUNS[@]}"; do
    pkg=${entry%% *}; filter=${entry#* }
    echo "gate: root-only: $pkg $filter"
    if ! AS_ROOT=1 clean_cargo "$LOGS/root-${filter//::/_}.log" test -p "$pkg" --lib -- --ignored --exact "$filter"; then
      STATUS=1
    fi
  done
else
  echo "gate: not root: root-only allowlisted tests not run"
fi

if [ $REALPG_SET -eq 3 ]; then
  echo "gate: real-Postgres smoke on 127.0.0.1:$K2_GATE_REALPG_PORT"
  EXTRA_ENV=(REALPG_PORT="$K2_GATE_REALPG_PORT" REALPG_SOCK="$K2_GATE_REALPG_SOCK" REALPG_BIN="$K2_GATE_REALPG_BIN")
  if ! clean_cargo "$LOGS/realpg.log" test -p k2-daemon --lib -- --ignored --exact sql::tests::real_pg::real_pg_guest_doors_smoke; then
    STATUS=1
  fi
  if ! clean_cargo "$LOGS/realpg-searchpath.log" test -p k2-daemon --lib -- --ignored --exact sql::tests::real_pg::real_pg_search_path_pin_holds; then
    STATUS=1
  fi
  EXTRA_ENV=()
else
  echo "gate: K2_GATE_REALPG_* not set: real-Postgres smoke not run (allowlisted)"
fi

echo
echo "== summary (per test binary) =="
for log in "$LOGS"/*.log; do
  awk -v f="$(basename "$log")" '
    /^ *Running |^ *Doc-tests / { bin=$0; sub(/^ */, "", bin) }
    /^test result:/ { printf "%-20s %-70s %s\n", f, substr(bin, 1, 70), $0 }
  ' "$log"
done

echo
echo "== failed =="
FAILED=$(grep -h -E '^test .* \.\.\. FAILED$' "$LOGS"/*.log | sed -E 's/^test (.*) \.\.\. FAILED$/\1/' | sort -u)
COMPILE_ERR=$(grep -h -E '^error(\[E[0-9]+\])?: ' "$LOGS"/*.log | sort -u)
[ -n "$FAILED" ] && { echo "$FAILED"; STATUS=1; }
[ -n "$COMPILE_ERR" ] && { echo "compile errors:"; echo "$COMPILE_ERR"; STATUS=1; }
[ -z "$FAILED$COMPILE_ERR" ] && echo "(none)"

echo
echo "== ignored =="
IGNORED=$(grep -h -E '^test .* \.\.\. ignored' "$LOGS"/k2-core*.log "$LOGS"/k2-daemon*.log \
  | sed -E 's/^test (.*) \.\.\. ignored.*$/\1/' | sort -u)
echo "${IGNORED:-(none)}"
UNEXPECTED=""
while IFS= read -r t; do
  [ -z "$t" ] && continue
  ok=0
  for a in "${ALLOWLIST[@]}"; do [ "$t" = "$a" ] && ok=1; done
  [ $ok -eq 0 ] && UNEXPECTED+="$t"$'\n'
done <<< "$IGNORED"
if [ -n "$UNEXPECTED" ]; then
  echo
  echo "== ignored but NOT on the allowlist =="
  printf '%s' "$UNEXPECTED"
  STATUS=1
fi

echo
if [ $STATUS -eq 0 ]; then echo "GATE GREEN ($FULL_SHA)"; else echo "GATE RED ($FULL_SHA)"; fi
# Keep a reused target dir bounded (the lab's Docker run links target/ to a
# volume that outlives the run, so the removal below only drops the link):
# drop incremental state and the linked test and bin executables; the
# compiled libraries stay, so the next run relinks instead of rebuilding.
if [ -d "$WORK/target/debug" ]; then
  before=$(du -sh "$WORK/target/" 2>/dev/null | cut -f1)
  rm -rf "$WORK/target/debug/incremental"
  drop_linked
  echo "(target trimmed: $before -> $(du -sh "$WORK/target/" 2>/dev/null | cut -f1): incremental and linked executables removed)"
fi
if [ "${K2_GATE_KEEP:-0}" != "1" ]; then
  rm -rf "$WORK/target" "$SRC"
  echo "(target + src removed; logs kept in $LOGS)"
fi
exit $STATUS
