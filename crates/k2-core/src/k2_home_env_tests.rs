//! `K2_HOME` moves every K2-home path (K2 Distributed Runtime P0).
//!
//! The resolver itself is tested in `k2_client::paths`; these tests set
//! the real variable (through `test_env::EnvVar`, under a `TempHome`) and
//! check what the modules that write the K2 home resolve and write.

use std::path::{Path, PathBuf};

use crate::paths;
use crate::test_env::{unique_temp_path, EnvVar, TempHome};

/// Every entry under `dir`, relative and sorted.
fn tree(dir: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap_or_else(|e| panic!("read_dir {}: {e}", d.display())) {
            let p = e.expect("entry").path();
            out.push(p.strip_prefix(dir).expect("under dir").to_string_lossy().into_owned());
            if p.is_dir() && !p.symlink_metadata().expect("meta").file_type().is_symlink() {
                stack.push(p);
            }
        }
    }
    out.sort();
    out
}

/// A temp `$HOME` plus `K2_HOME` pointing at a separate temp dir. The
/// `TempHome` comes first: it checks prod isolation, which refuses an
/// inherited `K2_HOME`.
struct K2HomeSet {
    k2: PathBuf,
    _var: EnvVar,
    home: TempHome,
}

impl K2HomeSet {
    fn new() -> Self {
        let home = TempHome::new();
        let k2 = unique_temp_path("k2home-env");
        let var = EnvVar::set(paths::K2_HOME_ENV, &k2);
        Self { k2, _var: var, home }
    }
}

impl Drop for K2HomeSet {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.k2);
    }
}

#[test]
fn k2_home_set_moves_the_resolver_and_its_helpers() {
    let t = K2HomeSet::new();
    assert_eq!(paths::k2_home(), t.k2);
    assert_eq!(paths::k2_home_opt(), Some(t.k2.clone()));
    assert_eq!(paths::k2_home_for(t.home.path()), t.k2);
    assert!(paths::k2_home_overridden());
    assert_eq!(paths::k2_home_env_error(), None);
    assert_eq!(paths::daemon_port_file(), t.k2.join("daemon.port"));
    assert_eq!(paths::daemon_token_file(), t.k2.join("daemon.token"));
    assert_eq!(paths::k2_bin(), t.k2.join("bin"));
    assert_eq!(paths::k2_hooks(), t.k2.join("hooks"));
}

#[test]
fn k2_home_set_moves_module_paths() {
    let t = K2HomeSet::new();
    let k2 = &t.k2;
    assert_eq!(crate::tunnel::config::config_path(), k2.join("tunnel.json"));
    assert_eq!(crate::tunnel::client_ip_labels::path(), k2.join("tunnel-client-ip.json"));
    assert_eq!(crate::tunnel::subdomains::persisted_path(), k2.join("tunnel-routes.json"));
    assert_eq!(crate::tunnel::unpair::tombstone_path(), k2.join("unpaired.json"));
    assert_eq!(crate::tunnel::connector::frpc_log_path(), k2.join("frpc.log"));
    assert_eq!(crate::federation::identity::identity_dir(), k2.join("federation"));
    assert_eq!(crate::auth_audit::path(), k2.join("auth-audit.jsonl"));
    assert_eq!(crate::connect_users::store_path(), k2.join("connect-users.json"));
    assert_eq!(crate::edge_attest::override_path(), k2.join("edge-keys.json"));
    assert_eq!(crate::themes::themes_dir(), k2.join("themes"));
    assert_eq!(crate::whats_new::state_path(), k2.join("whats-new.state"));
    assert_eq!(crate::llm::download::models_dir(), Ok(k2.join("models")));
    assert_eq!(crate::open_shim::staged_shim_path(), None, "not staged yet");
}

#[test]
fn k2_home_set_boot_writers_land_in_k2_home_not_home() {
    let t = K2HomeSet::new();
    let before = tree(t.home.path());

    // The 0.40 migration: only the K2 home, no `~/.k2so` symlink.
    let out = crate::migration_home::migrate_home_dir().expect("migration");
    assert_eq!(out, crate::migration_home::HomeMigration::K2HomeSet(t.k2.clone()));
    assert!(t.k2.is_dir());

    // The browser-open shim and the owner-only sweep.
    let staged = crate::open_shim::stage().expect("stage shim");
    assert_eq!(staged.first(), Some(&t.k2.join("bin").join("k2-open")));
    assert_eq!(crate::open_shim::staged_shim_path(), Some(t.k2.join("bin").join("k2-open")));
    std::fs::write(t.k2.join("k2so.db"), b"x").expect("seed db");
    let _ = crate::private_home::tighten_home();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = |p: &Path| std::fs::metadata(p).expect("meta").permissions().mode() & 0o777;
        assert_eq!(mode(&t.k2), 0o700, "the K2 home is owner-only");
        assert_eq!(mode(&t.k2.join("k2so.db")), 0o600);
    }

    // Settings and the auth audit log.
    let mut s = crate::app_settings::load();
    s.listen_lan = true;
    crate::app_settings::save(&s).expect("save settings");
    assert!(t.k2.join("settings.json").is_file());

    assert_eq!(tree(t.home.path()), before, "nothing new under $HOME");
    assert!(!t.home.path().join(".k2so").exists());
}

#[test]
fn k2_home_set_hook_script_names_the_absolute_port_file() {
    let t = K2HomeSet::new();
    let script = crate::agent_hooks::install::generate_hook_script("0.0.0");
    let want = format!("PORT=\"$(cat '{}' 2>/dev/null)\"", t.k2.join("heartbeat.port").display());
    assert!(script.contains(&want), "{script}");
    assert!(!script.contains("$HOME/.k2"), "{script}");
}

#[test]
fn k2_home_unset_keeps_home_dot_k2_byte_for_byte() {
    let home = TempHome::new();
    let _unset = EnvVar::remove(paths::K2_HOME_ENV);
    assert_eq!(paths::k2_home(), home.path().join(".k2"));
    assert_eq!(paths::k2_home_for(Path::new("/elsewhere")), PathBuf::from("/elsewhere/.k2"));
    assert!(!paths::k2_home_overridden());
    assert_eq!(crate::tunnel::config::config_path(), home.path().join(".k2/tunnel.json"));
    let script = crate::agent_hooks::install::generate_hook_script("0.0.0");
    assert!(script.contains("PORT=\"$(cat \"$HOME/.k2/heartbeat.port\" 2>/dev/null)\""), "{script}");
}

#[test]
fn k2_home_empty_means_unset() {
    let home = TempHome::new();
    let _empty = EnvVar::set(paths::K2_HOME_ENV, "");
    assert_eq!(paths::k2_home(), home.path().join(".k2"));
    assert!(!paths::k2_home_overridden());
    assert_eq!(paths::k2_home_env_error(), None);
}

#[test]
fn k2_home_relative_is_refused_loudly() {
    let _home = TempHome::new();
    let _rel = EnvVar::set(paths::K2_HOME_ENV, "var/k2");
    assert_eq!(
        paths::k2_home_env_error(),
        Some("K2_HOME must be an absolute path, got \"var/k2\"".to_string())
    );
    let err = std::panic::catch_unwind(paths::k2_home).expect_err("a relative K2_HOME must panic");
    let msg = err.downcast_ref::<String>().cloned().unwrap_or_default();
    assert_eq!(msg, "K2_HOME must be an absolute path, got \"var/k2\"");
}
