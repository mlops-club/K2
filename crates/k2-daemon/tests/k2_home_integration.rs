//! K2 Distributed Runtime P0: a real `k2-daemon` started with `K2_HOME`
//! keeps its whole K2 home there (DB, ports, tokens, vault, lock) and
//! writes nothing under `$HOME`. A relative `K2_HOME` is refused at boot.
//!
//! The spawned daemon gets a cleared env, an empty temp `$HOME`, its own TMPDIR /
//! XDG_RUNTIME_DIR (`DaemonRuntime`) and the usual test-daemon knobs, so
//! it never reaches the production daemon, keychain or runtime dir.

#![cfg(unix)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

struct Daemon {
    child: std::process::Child,
    root: PathBuf,
    _rt: k2_core::test_env::DaemonRuntime,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn read_trimmed(p: &Path) -> Option<String> {
    let t = std::fs::read_to_string(p).ok()?.trim().to_string();
    (!t.is_empty()).then_some(t)
}

fn get(port: u16, path: &str) -> Option<(u16, String)> {
    let mut s = TcpStream::connect(("127.0.0.1", port)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(10))).ok()?;
    write!(s, "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n").ok()?;
    let mut raw = Vec::new();
    let _ = s.read_to_end(&mut raw);
    let text = String::from_utf8_lossy(&raw).to_string();
    let status = text.split_whitespace().nth(1)?.parse().ok()?;
    let body = text.split_once("\r\n\r\n").map(|(_, b)| b.to_string()).unwrap_or_default();
    Some((status, body))
}

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

/// `<root>/home` (empty `$HOME`), `<root>/tmp` + `<root>/run` (runtime),
/// and the daemon started with `K2_HOME=k2_home`.
fn spawn(root: &Path, k2_home: &std::ffi::OsStr) -> Daemon {
    let home = root.join("home");
    std::fs::create_dir_all(&home).expect("temp HOME");
    // `DaemonRuntime::new` creates `<root>/tmp` and `<root>/run`.
    let shim = root.join("agent-shim-empty");
    std::fs::create_dir_all(&shim).expect("shim dir");
    let rt = k2_core::test_env::DaemonRuntime::new(root);
    // A clean env, as the Linux gate's `env -i`: an inherited SHELL /
    // ZDOTDIR would make the daemon's login-shell PATH probe run the
    // developer's own rc files, which may write under this `$HOME`
    // (nvm's installer does), and that is not K2 writing.
    let child = Command::new(env!("CARGO_BIN_EXE_k2-daemon"))
        .env_clear()
        .env("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
        .env("LANG", "C.UTF-8")
        .env("HOME", &home)
        .env("K2_HOME", k2_home)
        .envs(rt.envs())
        .env("K2_TEST_AGENT_SHIM_DIR", &shim)
        .env("K2SO_WATCHDOG_DISABLED", "1")
        .env("K2_HEARTBEAT_NO_SELF_HEAL", "1")
        .env("K2_SUBSCRIPTION_PROBE", "deny")
        .env("K2_TUNNEL_LEASE", "off")
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(root.join("stderr.log")).expect("stderr log"))
        .spawn()
        .expect("spawn k2-daemon");
    Daemon { child, root: root.to_path_buf(), _rt: rt }
}

#[test]
fn k2_home_holds_the_whole_k2_home_and_home_stays_empty() {
    let root = k2_core::test_env::unique_temp_path("k2home-boot");
    let k2 = root.join("var/lib/k2");
    let mut d = spawn(&root, k2.as_os_str());

    let deadline = Instant::now() + Duration::from_secs(60);
    let port = loop {
        let port = read_trimmed(&k2.join("daemon.port")).and_then(|p| p.parse::<u16>().ok());
        if let (Some(port), Some(_)) = (port, read_trimmed(&k2.join("daemon.token"))) {
            if let Some((200, body)) = get(port, "/boot-status") {
                if body.contains("\"phase\":\"ready\"") {
                    break port;
                }
            }
        }
        if let Some(status) = d.child.try_wait().expect("try_wait") {
            let err = std::fs::read_to_string(root.join("stderr.log")).unwrap_or_default();
            panic!("daemon exited before ready ({status}):\n{err}");
        }
        assert!(Instant::now() < deadline, "daemon never reached ready under K2_HOME={}", k2.display());
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(port > 0);

    let files = tree(&k2);
    for want in [
        "daemon.lock",
        "daemon.port",
        "daemon.token",
        "heartbeat.port",
        "heartbeat.token",
        "k2so.db",
        "settings.json",
        "federation/identity.pem",
        "vault/vault.db",
        "bin/k2-open",
    ] {
        assert!(files.iter().any(|f| f == want), "{want} missing from K2_HOME: {files:?}");
    }
    assert!(!files.iter().any(|f| f == ".k2" || f.starts_with(".k2/")), "K2_HOME is the home itself, not its parent: {files:?}");
    assert_eq!(tree(&root.join("home")), Vec::<String>::new(), "nothing may land under $HOME");
}

#[test]
fn a_relative_k2_home_is_refused_at_boot() {
    let root = k2_core::test_env::unique_temp_path("k2home-rel");
    let mut d = spawn(&root, std::ffi::OsStr::new("var/k2"));
    let status = d.child.wait().expect("wait");
    let err = std::fs::read_to_string(root.join("stderr.log")).expect("stderr log");
    assert_eq!(status.code(), Some(2), "{err}");
    assert!(err.contains("[daemon] FATAL: K2_HOME must be an absolute path, got \"var/k2\""), "{err}");
    assert_eq!(tree(&root.join("home")), Vec::<String>::new(), "nothing may land under $HOME");
}
