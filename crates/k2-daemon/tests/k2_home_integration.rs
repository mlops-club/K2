//! K2 Distributed Runtime P0: a real `k2-daemon` started with `K2_HOME`
//! keeps its whole K2 home there (DB, ports, tokens, vault, lock) and
//! writes nothing under `$HOME`. A relative `K2_HOME` is refused at boot.
//! Its agents still have a working `k2`: the CLI is staged in `K2_HOME`
//! and every agent session finds it first on its PATH.
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

fn post(port: u16, path: &str, body: &str) -> Option<(u16, String)> {
    let mut s = TcpStream::connect(("127.0.0.1", port)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(30))).ok()?;
    write!(
        s,
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .ok()?;
    let mut raw = Vec::new();
    let _ = s.read_to_end(&mut raw);
    let text = String::from_utf8_lossy(&raw).to_string();
    let status = text.split_whitespace().nth(1)?.parse().ok()?;
    let body = text.split_once("\r\n\r\n").map(|(_, b)| b.to_string()).unwrap_or_default();
    Some((status, body))
}

/// `<root>/home` (empty `$HOME`), `<root>/tmp` + `<root>/run` (runtime),
/// and the daemon started with `K2_HOME=k2_home`.
fn spawn(root: &Path, k2_home: &std::ffi::OsStr) -> Daemon {
    let shim = root.join("agent-shim-empty");
    std::fs::create_dir_all(&shim).expect("shim dir");
    spawn_with(root, k2_home, &shim, &[])
}

/// [`spawn`] with agents resolved from `shim` and `extra` env.
fn spawn_with(root: &Path, k2_home: &std::ffi::OsStr, shim: &Path, extra: &[(&str, &Path)]) -> Daemon {
    let home = root.join("home");
    std::fs::create_dir_all(&home).expect("temp HOME");
    // `DaemonRuntime::new` creates `<root>/tmp` and `<root>/run`.
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
        .env("K2_TEST_AGENT_SHIM_DIR", shim)
        .envs(extra.iter().map(|(k, v)| (*k, *v)))
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

/// Wait for `d` to reach boot phase ready; its port.
fn ready(d: &mut Daemon, root: &Path, k2: &Path) -> u16 {
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
    port
}

#[test]
fn k2_home_holds_the_whole_k2_home_and_home_stays_empty() {
    let root = k2_core::test_env::unique_temp_path("k2home-boot");
    let k2 = root.join("var/lib/k2");
    let mut d = spawn(&root, k2.as_os_str());
    ready(&mut d, &root, &k2);

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
        "bin/k2",
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

/// The built `k2-ptyd` beside this test's target dir (or `K2_PTYD_BIN`).
fn ptyd_bin() -> PathBuf {
    if let Some(p) = std::env::var_os("K2_PTYD_BIN") {
        return PathBuf::from(p);
    }
    // target/<profile>/deps/k2_home_integration-<hash> → target/<profile>/k2-ptyd
    let exe = std::env::current_exe().expect("current_exe");
    exe.parent().and_then(|d| d.parent()).map(|d| d.join("k2-ptyd")).expect("target dir")
}

/// Kills what is still running from under `root` (k2-ptyd outlives its
/// daemon by design) when the test ends, pass or fail.
struct Reap(PathBuf);
impl Drop for Reap {
    fn drop(&mut self) {
        // Never a pattern that matches every process (an empty root, `/`).
        let root = self.0.to_string_lossy();
        if root.starts_with("/tmp/k2hp-") || root.contains("/k2home-path") {
            let _ = Command::new("pkill").arg("-9").arg("-f").arg(self.0.as_os_str()).status();
        }
    }
}

/// Agents keep a working `k2` (k2 msg, …) under `K2_HOME`, containers
/// included: the daemon staged the CLI at `$K2_HOME/bin/k2`, and an agent
/// session's PATH resolves `k2` there first. Nothing lands under `$HOME`.
#[test]
fn an_agent_sessions_path_resolves_k2_to_the_cli_staged_in_k2_home() {
    path_probe(false);
}

/// The same for a session hosted by k2-ptyd (`K2_PTYD=1`).
#[test]
fn an_agent_session_on_k2_ptyd_resolves_k2_to_the_cli_staged_in_k2_home() {
    path_probe(true);
}

fn path_probe(ptyd: bool) {
    // Short, under /tmp: k2-ptyd's socket must fit sun_path (104 bytes on macOS).
    let root = if ptyd {
        static N: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        PathBuf::from(format!("/tmp/k2hp-{}-{n}", std::process::id()))
    } else {
        k2_core::test_env::unique_temp_path("k2home-path")
    };
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("root");
    let _reap = Reap(root.clone());
    let k2 = root.join("var/lib/k2");
    let work = root.join("work");
    std::fs::create_dir_all(&work).expect("work dir");
    let probe = root.join("probe.txt");
    // The agent: in its session (a PTY, not a version probe) it writes
    // where `k2` resolves and its PATH, then waits to be stopped.
    let shim = root.join("agent-shim");
    std::fs::create_dir_all(&shim).expect("shim dir");
    let agent = shim.join("claude");
    std::fs::write(
        &agent,
        "#!/bin/sh\nif [ -t 0 ]; then\n  { command -v k2; printf '%s\\n' \"$PATH\"; } > \"$K2_TEST_PATH_PROBE.tmp\"\n  mv \"$K2_TEST_PATH_PROBE.tmp\" \"$K2_TEST_PATH_PROBE\"\n  sleep 60\nfi\n",
    )
    .expect("agent shim");
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&agent, std::fs::Permissions::from_mode(0o755)).expect("chmod agent shim");
    }
    let bin = ptyd_bin();
    let mut extra: Vec<(&str, &Path)> = vec![("K2_TEST_PATH_PROBE", probe.as_path())];
    if ptyd {
        assert!(bin.is_file(), "k2-ptyd not found at {}: run `cargo build -p k2-ptyd --bin k2-ptyd` or set K2_PTYD_BIN", bin.display());
        extra.push(("K2_PTYD", Path::new("1")));
        extra.push(("K2_PTYD_BIN", bin.as_path()));
    }
    let mut d = spawn_with(&root, k2.as_os_str(), &shim, &extra);
    let port = ready(&mut d, &root, &k2);
    let token = read_trimmed(&k2.join("daemon.token")).expect("daemon.token");
    let cli = k2.join("bin/k2");
    assert!(cli.is_file(), "the CLI is staged in K2_HOME: {:?}", tree(&k2));

    let body = format!(r#"{{"agent_name":"pathcheck","cwd":"{}","command":"claude"}}"#, work.display());
    let (status, answer) = post(port, &format!("/cli/sessions/v2/spawn?token={token}"), &body).expect("spawn answered");
    assert_eq!(status, 200, "{answer}");
    let deadline = Instant::now() + Duration::from_secs(60);
    let seen = loop {
        if let Some(t) = read_trimmed(&probe) {
            break t;
        }
        assert!(Instant::now() < deadline, "the agent never wrote its PATH probe; spawn answered {answer}");
        std::thread::sleep(Duration::from_millis(100));
    };
    let mut lines = seen.lines();
    let resolved = lines.next().unwrap_or_default();
    let path = lines.next().unwrap_or_default();
    assert_eq!(resolved, cli.to_string_lossy(), "`k2` in the agent's session; PATH {path}");
    assert_eq!(path.split(':').next(), Some(&*k2.join("bin").to_string_lossy()), "K2_HOME/bin is first on the PATH");
    let log = std::fs::read_to_string(root.join("stderr.log")).unwrap_or_default();
    assert_eq!(log.contains("[daemon_pty] ptyd session="), ptyd, "the session ran on k2-ptyd: {ptyd}");
    assert_eq!(tree(&root.join("home")), Vec::<String>::new(), "nothing may land under $HOME");
}
