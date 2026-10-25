#!/usr/bin/env bash
# Rust toolchain for the lab. Sourced by prove.sh and the lab's tests.
#
#   source scripts/lab/lib/toolchain.sh; lab_toolchain "$REPO"
#
# Uses `cargo` from PATH when there is one. Otherwise installs a pinned,
# lab-local toolchain under lab-runs/.toolchain/ (gitignored): rustup-init is
# downloaded from static.rust-lang.org, checked against its published sha256,
# and run with --no-modify-path, so nothing outside the repo changes (no
# ~/.cargo, no ~/.rustup, no shell profile). Exports CARGO_HOME, RUSTUP_HOME
# and PATH for the caller, and LAB_RUST_SOURCE ("path" or "lab-local").

LAB_RUST_VERSION=1.99.1

# Machine-wide cargo queue (coordinator, 2026-10-24: the shared Mac was
# swapping): at most K2_DIST_SLOTS (2) cargo commands at once across all
# workers. The slot script keeps its state under the REAL $HOME, so it is
# resolved here, before any throwaway HOME. Missing script = no queue.
LAB_CARGO_SLOT=${LAB_CARGO_SLOT:-$HOME/.cache/k2-dist-cargo-slot.sh}
LAB_REAL_HOME=${LAB_REAL_HOME:-$HOME}
export LAB_CARGO_SLOT LAB_REAL_HOME

# lab_slot <cmd...>: run <cmd> in a machine-wide cargo slot (waits for one).
lab_slot() {
    if [ -x "$LAB_CARGO_SLOT" ]; then
        HOME="$LAB_REAL_HOME" "$LAB_CARGO_SLOT" "$@"
    else
        "$@"
    fi
}

lab_toolchain() {
    local repo=$1
    lab_native_env
    # CPU budget (coordinator, 2026-10-24): the Mac is shared by several
    # workers and the owner's kind cluster; three build jobs per cargo
    # command unless the caller set one.
    export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-3}
    if command -v cargo >/dev/null 2>&1; then
        # Pin the toolchain dirs to the caller's HOME now, so lab_cargo can
        # run cargo under a throwaway HOME and keep the build cache.
        export CARGO_HOME=${CARGO_HOME:-$HOME/.cargo} RUSTUP_HOME=${RUSTUP_HOME:-$HOME/.rustup}
        LAB_RUST_SOURCE=path
        export LAB_RUST_SOURCE
        return 0
    fi
    local root="$repo/lab-runs/.toolchain"
    export RUSTUP_HOME="$root/rustup" CARGO_HOME="$root/cargo"
    export PATH="$CARGO_HOME/bin:$PATH"
    LAB_RUST_SOURCE=lab-local
    export LAB_RUST_SOURCE
    if [ -x "$CARGO_HOME/bin/cargo" ] && "$CARGO_HOME/bin/rustc" --version 2>/dev/null | grep -q " $LAB_RUST_VERSION "; then
        return 0
    fi
    local triple
    case "$(uname -s)-$(uname -m)" in
        Darwin-arm64) triple=aarch64-apple-darwin ;;
        Darwin-x86_64) triple=x86_64-apple-darwin ;;
        Linux-x86_64) triple=x86_64-unknown-linux-gnu ;;
        Linux-aarch64) triple=aarch64-unknown-linux-gnu ;;
        *) echo "lab: no rustup-init for $(uname -sm)" >&2; return 1 ;;
    esac
    mkdir -p "$root"
    local url="https://static.rust-lang.org/rustup/dist/$triple/rustup-init"
    local init="$root/rustup-init"
    echo "lab: installing Rust $LAB_RUST_VERSION into $root (lab-local; nothing outside the repo)" >&2
    curl -sSfL --retry 3 -o "$init" "$url" || return 1
    local want got
    want=$(curl -sSfL --retry 3 "$url.sha256" | awk '{print $1}') || return 1
    got=$(shasum -a 256 "$init" | awk '{print $1}')
    if [ -z "$want" ] || [ "$want" != "$got" ]; then
        echo "lab: rustup-init checksum mismatch (want '$want', got '$got')" >&2
        rm -f "$init"
        return 1
    fi
    chmod +x "$init"
    "$init" -y --no-modify-path --profile minimal --default-toolchain "$LAB_RUST_VERSION" >&2 || return 1
    rm -f "$init"
}

# llama-cpp-sys-2 vendors cpp-httplib, which does not compile against
# OpenSSL 4 headers (`const X509_NAME *`), and Homebrew's `openssl` is 4.x
# since 2026-10. Point CMake at Homebrew's openssl@3 when it is there and the
# caller set nothing. Environment only; the repo's build config is unchanged.
lab_native_env() {
    if [ "$(uname -s)" = Darwin ] && [ -z "${OPENSSL_ROOT_DIR:-}" ] \
        && [ -d /opt/homebrew/opt/openssl@3 ]; then
        export OPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3
    fi
}

# lab_cargo <args...>: cargo in a machine-wide slot (lab_slot), under a
# clean environment, as the Linux gate's
# clean_cargo does: env -i, a FRESH HOME and TMPDIR (mktemp -d, removed
# after), no K2_* / K2SO_* / SHELL, only the toolchain's variables. Some
# pre-existing tests write under $HOME (seen 2026-10-24: ~/.k2/auth-audit.jsonl,
# ~/.k2/wiki), so no lab cargo command ever runs with the real HOME.
# Call lab_toolchain first.
lab_cargo() {
    local h t rc
    # Short paths under /tmp, not macOS's long $TMPDIR: some tests put the
    # temp path into a length-limited line (macos_keychain_tests feeds the
    # test keychain's path to `security -i`, capped at INTERACTIVE_LINE_MAX).
    h=$(mktemp -d /tmp/k2c-h.XXXXXX) || return 1
    t=$(mktemp -d /tmp/k2c-t.XXXXXX) || return 1
    lab_slot env -i HOME="$h" TMPDIR="$t" PATH="$PATH" USER="$(id -un)" LANG=en_US.UTF-8 \
        CARGO_HOME="$CARGO_HOME" RUSTUP_HOME="$RUSTUP_HOME" CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-3}" \
        ${CARGO_TARGET_DIR:+CARGO_TARGET_DIR="$CARGO_TARGET_DIR"} \
        ${OPENSSL_ROOT_DIR:+OPENSSL_ROOT_DIR="$OPENSSL_ROOT_DIR"} \
        ${MACOSX_DEPLOYMENT_TARGET:+MACOSX_DEPLOYMENT_TARGET="$MACOSX_DEPLOYMENT_TARGET"} \
        cargo "$@"
    rc=$?
    # Tests leave read-only files and dirs behind (seed stores); make them
    # writable so the throwaway dirs really go.
    chmod -R u+w "$h" "$t" 2>/dev/null
    rm -rf "$h" "$t"
    return $rc
}

# lab_cargo_shim <dir>: write <dir>/cargo, which runs the real cargo through
# the slot queue with the caller's environment (HOME included) unchanged.
# Put <dir> first on a suite's PATH so the suites' own `cargo build`s queue
# too, without editing them.
lab_cargo_shim() {
    local dir=$1 real
    real=$(command -v cargo) || return 1
    mkdir -p "$dir"
    cat >"$dir/cargo" <<SH
#!/bin/bash
# lab: queue this cargo command in a machine-wide slot (scripts/lab/lib/toolchain.sh).
if [ -x '$LAB_CARGO_SLOT' ]; then
    exec env HOME='$LAB_REAL_HOME' '$LAB_CARGO_SLOT' env HOME="\$HOME" '$real' "\$@"
fi
exec '$real' "\$@"
SH
    chmod 755 "$dir/cargo"
}
