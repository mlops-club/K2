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

lab_toolchain() {
    local repo=$1
    lab_native_env
    if command -v cargo >/dev/null 2>&1; then
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
