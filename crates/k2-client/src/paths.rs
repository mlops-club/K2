//! Canonical K2 home-directory paths (PRD `prd-k2so-cleanup-v1.md` §3.1,
//! K2 Distributed Runtime P0).
//!
//! The K2 home is `$K2_HOME` when that is set (a container's volume, for
//! example `/var/lib/k2`), else `~/.k2`. `K2_HOME` names the K2 home
//! itself, not a replacement `$HOME`: `K2_HOME=/var/lib/k2` puts the DB at
//! `/var/lib/k2/k2so.db`. Provider homes (`~/.claude`, `~/.codex`, …) and
//! the runtime dir (`K2_RUNTIME_DIR`) are not under the K2 home and do not
//! move with it.
//!
//! The legacy `~/.k2so` is a compatibility SYMLINK maintained by
//! `migration_home` for external tools that memorized the old path (not
//! created when `K2_HOME` is set) — no K2 code may construct a `.k2so`
//! path (enforced by `scripts/k2so-gate.sh` in CI). Every subsystem that
//! needs "the K2 home" resolves it here so the answer can never drift
//! again; a ratchet test (`no_new_k2_home_paths_outside_paths_*`) fails on
//! a K2-home path built anywhere else.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

/// The env var that overrides the K2 home.
pub const K2_HOME_ENV: &str = "K2_HOME";

/// The K2 home directory: `$K2_HOME`, else `$HOME/.k2`.
///
/// An unset or empty `K2_HOME` means `$HOME/.k2`, which falls back to a
/// relative `.k2` when no home directory can be resolved — the same
/// degrade `db::db_dir` has always used, so a pathological environment
/// behaves identically across subsystems instead of panicking in some and
/// degrading in others.
///
/// # Panics
///
/// When `K2_HOME` is set to a relative path. A relative home would follow
/// the process's working directory, so two processes (the daemon and a
/// CLI) could silently use different homes. The daemon checks
/// [`k2_home_env_error`] at boot and exits with the message instead.
pub fn k2_home() -> PathBuf {
    k2_home_from(std::env::var_os(K2_HOME_ENV), dirs::home_dir).unwrap_or_else(|e| panic!("{e}"))
}

/// The K2 home of a process whose `$HOME` is `home`: `$K2_HOME` when set,
/// else `home/.k2`. For seams that take an explicit `$HOME` (tests pass a
/// temp dir; production passes the real home, so this equals
/// [`k2_home`]).
///
/// # Panics
///
/// As [`k2_home`], on a relative `K2_HOME`.
pub fn k2_home_for(home: &Path) -> PathBuf {
    let home = home.to_path_buf();
    k2_home_from(std::env::var_os(K2_HOME_ENV), || Some(home)).unwrap_or_else(|e| panic!("{e}"))
}

/// [`k2_home`], or `None` when `K2_HOME` is unset and no home directory
/// can be resolved: for callers that refuse (instead of degrading to a
/// relative `.k2`) in that case.
///
/// # Panics
///
/// As [`k2_home`], on a relative `K2_HOME`.
pub fn k2_home_opt() -> Option<PathBuf> {
    let mut no_home = false;
    let p = k2_home_from(std::env::var_os(K2_HOME_ENV), || {
        let h = dirs::home_dir();
        no_home = h.is_none();
        h
    })
    .unwrap_or_else(|e| panic!("{e}"));
    (!no_home).then_some(p)
}

/// Why the current `K2_HOME` is unusable, or `None` when it is fine
/// (absolute, empty or unset). For a boot-time check that fails loudly
/// before anything resolves a path.
pub fn k2_home_env_error() -> Option<String> {
    k2_home_from(std::env::var_os(K2_HOME_ENV), || None).err()
}

/// Is `K2_HOME` set (non-empty)? Callers that only make sense for the
/// default `~/.k2` layout (the `~/.k2so` compat symlink) skip themselves.
pub fn k2_home_overridden() -> bool {
    std::env::var_os(K2_HOME_ENV).is_some_and(|v| !v.is_empty())
}

/// The resolver behind [`k2_home`], with its inputs explicit.
pub fn k2_home_from(k2_home_env: Option<OsString>, home: impl FnOnce() -> Option<PathBuf>) -> Result<PathBuf, String> {
    match k2_home_env.filter(|v| !v.is_empty()) {
        Some(v) => {
            let p = PathBuf::from(v);
            if p.is_absolute() {
                Ok(p)
            } else {
                Err(format!("K2_HOME must be an absolute path, got {:?}", p))
            }
        }
        None => Ok(home().unwrap_or_default().join(".k2")),
    }
}

/// A path under the K2 home: `k2_file("daemon.port")`.
pub fn k2_file(rel: impl AsRef<Path>) -> PathBuf {
    k2_home().join(rel)
}

/// `~/.k2/bin` — staged sidecar binaries (frpc, k2-open, xdg-open).
pub fn k2_bin() -> PathBuf {
    k2_home().join("bin")
}

/// `~/.k2/hooks` — agent lifecycle hook scripts (notify.sh).
pub fn k2_hooks() -> PathBuf {
    k2_home().join("hooks")
}

/// `~/.k2/daemon.port` — the daemon's loopback port.
pub fn daemon_port_file() -> PathBuf {
    k2_file("daemon.port")
}

/// `~/.k2/daemon.token` — the daemon's owner token.
pub fn daemon_token_file() -> PathBuf {
    k2_file("daemon.token")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn home() -> Option<PathBuf> {
        Some(PathBuf::from("/Users/u"))
    }

    #[test]
    fn unset_or_empty_is_home_dot_k2_never_k2so() {
        assert_eq!(k2_home_from(None, home), Ok(PathBuf::from("/Users/u/.k2")));
        assert_eq!(k2_home_from(Some("".into()), home), Ok(PathBuf::from("/Users/u/.k2")));
        let p = k2_home_from(None, home).unwrap();
        assert!(!p.to_string_lossy().contains(".k2so"), "legacy name must never appear in the canonical path");
    }

    #[test]
    fn no_home_degrades_to_relative_dot_k2() {
        assert_eq!(k2_home_from(None, || None), Ok(PathBuf::from(".k2")));
    }

    #[test]
    fn absolute_k2_home_is_the_home_itself() {
        assert_eq!(k2_home_from(Some("/var/lib/k2".into()), home), Ok(PathBuf::from("/var/lib/k2")));
    }

    #[test]
    fn relative_k2_home_is_refused() {
        assert_eq!(
            k2_home_from(Some("k2".into()), home),
            Err("K2_HOME must be an absolute path, got \"k2\"".to_string())
        );
        assert!(k2_home_from(Some("./var/k2".into()), home).is_err());
    }

    #[test]
    fn helpers_derive_from_k2_home() {
        assert_eq!(k2_bin(), k2_home().join("bin"));
        assert_eq!(k2_hooks(), k2_home().join("hooks"));
        assert_eq!(daemon_port_file(), k2_home().join("daemon.port"));
        assert_eq!(daemon_token_file(), k2_home().join("daemon.token"));
        assert_eq!(k2_file("a/b"), k2_home().join("a/b"));
    }
}
