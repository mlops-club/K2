//! T1 through the controller: the fence on real placements (fake hosts on
//! the loopback hub) and the routes over loopback HTTP, driven by the
//! host's own client. Deadlines only stop a hung test.

use std::fs;
use std::path::{Path, PathBuf};

use k2_core::agent_home::restore::{self, Outcome, Target};
use k2_core::agent_home::snapshot::{self, Options};
use k2_core::agent_home::Remote;
use k2_core::chat_history::claude_project_hash;
use serde_json::{json, Value};
use tokio::net::TcpListener;

use super::*;
use crate::hosts::placement_tests::{fake_host, gone, serve as serve_hub, sid, spawn_on, wait_hosts, within, TOKEN};

const OWNER: &str = "owner-token";

struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Self {
        static CTR: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = CTR.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
        let p = std::env::temp_dir().join(format!("k2-home-sync-{}-{nanos}-{n}", std::process::id()));
        fs::create_dir_all(&p).unwrap();
        TempDir(fs::canonicalize(&p).unwrap())
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// The home routes on loopback HTTP for `s`; returns the base URL.
async fn serve_home(s: &'static Server) -> String {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = l.accept().await {
            tokio::spawn(async move {
                let mut buf = vec![0u8; 64 * 1024];
                let n = stream.peek(&mut buf).await.unwrap_or(0);
                let head = String::from_utf8_lossy(&buf[..n]).into_owned();
                let target = head.split(' ').nth(1).unwrap_or("/").to_string();
                let (path, query) = target.split_once('?').unwrap_or((&target, ""));
                serve(&mut stream, &mut buf, s, path, query, OWNER).await;
            });
        }
    });
    base
}

fn write(p: &Path, s: &str) {
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    fs::write(p, s).unwrap();
}

/// Run blocking client calls off the test's async workers.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    within("a blocking call", tokio::task::spawn_blocking(f)).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_home_moves_between_hosts_over_http_and_only_its_writer_may_commit() {
    let root = TempDir::new();
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve_hub(remote).await;
    let s: &'static Server = Box::leak(Box::new(Server::new(&root.0.join("controller"), remote, TOKEN).unwrap()));
    let base = serve_home(s).await;

    let project = root.0.join("work").join("notes");
    let p = project.to_string_lossy().into_owned();
    let home_a = root.0.join("host-a");
    let slug = |h: &Path| h.join(".claude/projects").join(claude_project_hash(&p));
    write(&project.join("README.md"), "# notes\n");
    write(&project.join(".k2/AGENT.md"), "# agent\n");
    write(&slug(&home_a).join("memory/MEMORY.md"), "- remember x\n");
    write(&slug(&home_a).join(&format!("{}.jsonl", sid(1))), "{\"type\":\"user\"}\n");

    // notes runs on host-a at epoch e1.
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "notes", &p, &sid(1)).await;
    let e1 = remote.placement("notes").unwrap().epoch.0;

    // A wrong hello credential gets no transfer token.
    let b2 = base.clone();
    let err = blocking(move || HttpRemote::connect(&b2, "host-a", "not-the-token").err()).await.expect("refused");
    assert!(err.contains("HTTP 403"), "{err}");

    // host-a saves notes' home (as its close or drain would).
    let (b2, p2, ha) = (base.clone(), p.clone(), home_a.clone());
    let pushed = blocking(move || {
        let r = HttpRemote::connect(&b2, "host-a", TOKEN)?;
        snapshot::push(&r, "notes", e1, "host-a", &p2, None, &Options::new(&ha))
    })
    .await
    .unwrap();
    assert_eq!(pushed.seq, 1);

    // host-b links; it may neither write notes' home nor read it.
    let mut b = fake_host(&url, "host-b").await;
    wait_hosts(remote, 2).await;
    let (b2, p2, ha) = (base.clone(), p.clone(), home_a.clone());
    let (commit, read) = blocking(move || {
        let r = HttpRemote::connect(&b2, "host-b", TOKEN).unwrap();
        let c = snapshot::push(&r, "notes", e1, "host-b", &p2, None, &Options::new(&ha)).unwrap_err();
        (c, r.latest("notes").unwrap_err())
    })
    .await;
    assert!(commit.contains("HTTP 409") && commit.contains("placed on host-a"), "{commit}");
    assert!(read.contains("HTTP 403"), "{read}");

    // notes' CLI exits; host-a goes away; notes is placed again, on host-b.
    gone(&mut a, remote, "notes").await;
    drop(a);
    wait_hosts(remote, 1).await;
    // Once it isn't placed, the host that last ran it may still save it
    // (the sleep's own push: `unplace` drops the placement before `close`).
    let (b2, p2, ha) = (base.clone(), p.clone(), home_a.clone());
    let late = blocking(move || {
        let r = HttpRemote::connect(&b2, "host-a", TOKEN)?;
        snapshot::push(&r, "notes", e1, "host-a", &p2, Some(1), &Options::new(&ha))
    })
    .await
    .unwrap();
    assert_eq!(late.seq, 2);
    spawn_on(remote, &mut b, "notes", &p, &sid(1)).await;
    let e2 = remote.placement("notes").unwrap().epoch.0;
    assert!(e2 > e1);

    // host-b is another machine: no workspace, its own home.
    fs::remove_dir_all(&project).unwrap();
    let home_b = root.0.join("host-b");
    let t = Target { home: home_b.clone(), state: root.0.join("host-b-state"), work_root: Some(root.0.join("work")) };
    let b2 = base.clone();
    let out = blocking(move || {
        let r = HttpRemote::connect(&b2, "host-b", TOKEN)?;
        restore::restore(&r, "notes", &t)
    })
    .await
    .unwrap();
    assert!(matches!(out, Outcome::Restored { seq: 2, .. }), "{out:?}");
    assert_eq!(fs::read_to_string(project.join("README.md")).unwrap(), "# notes\n");
    assert_eq!(fs::read_to_string(slug(&home_b).join("memory/MEMORY.md")).unwrap(), "- remember x\n");
    assert!(slug(&home_b).join(format!("{}.jsonl", sid(1))).is_file());

    // host-a comes back with its old copy: its write at e1 is refused (I4).
    let _a = fake_host(&url, "host-a").await;
    let (b2, p2, ha) = (base.clone(), p.clone(), home_a.clone());
    write(&project.join("README.md"), "# notes, stale on host-a\n");
    let stale = blocking(move || {
        let r = HttpRemote::connect(&b2, "host-a", TOKEN).unwrap();
        snapshot::push(&r, "notes", e1, "host-a", &p2, Some(2), &Options::new(&ha)).unwrap_err()
    })
    .await;
    assert!(stale.contains("HTTP 409") && stale.contains(&format!("placed on host-b at epoch {e2}")), "{stale}");
    assert_eq!(s.store().latest("notes").unwrap().map(|l| (l.seq, l.host)), Some((2, "host-a".to_string())));

    // The owner's status, and the pool's check.
    let b2 = base.clone();
    let status = blocking(move || reqwest::blocking::get(format!("{b2}{STATUS_PATH}?token={OWNER}")).unwrap().text().unwrap()).await;
    let status: Value = serde_json::from_str(&status).unwrap();
    assert_eq!(status["snapshots"][0]["agent"], "notes");
    assert_eq!(status["snapshots"][0]["seq"], 2);
    let b2 = base.clone();
    let code = blocking(move || reqwest::blocking::get(format!("{b2}{STATUS_PATH}?token=nope")).unwrap().status().as_u16()).await;
    assert_eq!(code, 403);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_fence_follows_the_placement_then_the_last_host() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve_hub(remote).await;
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "dave", "/w/dave", &sid(1)).await;
    let e = remote.placement("dave").unwrap().epoch.0;
    let f = |host: &'static str, epoch: u64, newest: Option<&k2_core::agent_home::Latest>| {
        fence(remote, &Fence { agent: "dave", host, epoch, newest })
    };
    assert_eq!(f("host-a", e, None), Ok(()));
    assert!(f("host-a", e - 1, None).unwrap_err().contains("may not write"));
    assert!(f("host-b", e, None).unwrap_err().contains("placed on host-a"));
    let newest = k2_core::agent_home::Latest {
        seq: 3,
        epoch: e,
        host: "host-a".into(),
        manifest_sha: "0".repeat(64),
        created_ms: 0,
        files: 0,
        bytes: 0,
    };
    // T2: even its live placement never writes under a newer writer's
    // snapshot (a host adopted again after its agent moved and came back).
    let newer = k2_core::agent_home::Latest { seq: 4, epoch: e + 1, host: "host-b".into(), ..newest.clone() };
    assert_eq!(f("host-a", e, Some(&newest)), Ok(()), "a turn's save from the live placement");
    assert!(f("host-a", e, Some(&newer)).unwrap_err().contains("older than"));
    gone(&mut a, remote, "dave").await;
    assert_eq!(f("host-a", e, Some(&newest)), Ok(()), "the last host, after the placement ended");
    assert!(f("host-b", e + 1, Some(&newest)).unwrap_err().contains("not the host that last ran"));
    assert!(f("host-a", e - 1, Some(&newest)).unwrap_err().contains("older than"));
    assert!(f("host-a", e, None).is_ok());
    assert!(fence(remote, &Fence { agent: "nobody", host: "host-a", epoch: 1, newest: None }).unwrap_err().contains("none known"));
}

#[test]
fn a_close_reply_carries_what_happened_to_the_home() {
    assert_eq!(with_home(r#"{"closed":true}"#.into(), Value::Null), r#"{"closed":true}"#);
    let v: Value = serde_json::from_str(&with_home(r#"{"closed":true}"#.into(), json!({"seq": 4}))).unwrap();
    assert_eq!(v, json!({"closed": true, "home": {"seq": 4}}));
    assert_eq!(with_home("not json".into(), json!({"seq": 4})), "not json");
}

#[test]
fn only_workspaces_under_the_work_root_are_synced() {
    let root = TempDir::new();
    let get = |v: Option<&'static str>| move |k: &str| (k == WORK_ROOT_VAR).then(|| v.map(str::to_string)).flatten();
    assert_eq!(work_root(get(None)), None, "unset: home sync is off");
    assert_eq!(work_root(get(Some("  "))), None);
    let work = root.0.join("work");
    fs::create_dir_all(work.join("notes")).unwrap();
    let raw: &'static str = Box::leak(work.to_string_lossy().into_owned().into_boxed_str());
    let r = work_root(get(Some(raw))).unwrap();
    assert!(in_scope(&r, &work.join("notes")));
    assert!(!in_scope(&r, &work), "the root itself is not a workspace");
    assert!(!in_scope(&r, &root.0), "outside");
    assert!(!in_scope(&r, Path::new("/tmp")));
}

#[test]
fn one_cwd_per_agent_the_second_agent_in_a_placed_agents_cwd_is_refused() {
    let mut claims = std::collections::HashMap::new();
    let cwd = Path::new("/w/notes");
    let placed = |a: &str| a == "notes";
    assert_eq!(claim_in(&mut claims, cwd, "notes", &placed), Ok(()));
    let err = claim_in(&mut claims, cwd, "twin", &placed).unwrap_err();
    assert!(err.contains("agent notes's workspace") && err.contains("one cwd per agent"), "{err}");
    assert_eq!(claim_in(&mut claims, cwd, "notes", &placed), Ok(()), "the owner itself, again");
    // Once the owner isn't placed here any more, the cwd is free.
    assert_eq!(claim_in(&mut claims, cwd, "twin", &|_| false), Ok(()));
    assert_eq!(claims.get(cwd).map(String::as_str), Some("twin"));
}

#[test]
fn a_spawn_runs_the_agent_where_its_workspace_is_on_this_host() {
    let here = || Some((PathBuf::from("/disk-b/work/team/notes"), Some("team/notes".to_string())));
    // Named under host A's root: the same place under this host's.
    assert_eq!(map_cwd(Path::new("/disk-a/work/team/notes"), here()), Some(PathBuf::from("/disk-b/work/team/notes")));
    assert_eq!(map_cwd(Path::new("/disk-b/work/team/notes"), here()), None, "already here");
    assert_eq!(map_cwd(Path::new("/disk-a/work/other"), here()), None, "another workspace");
    assert_eq!(map_cwd(Path::new("/disk-a/work/team/notes"), None), None, "no home here yet");
    assert_eq!(map_cwd(Path::new("/x/team/notes"), Some((PathBuf::from("/b/team/notes"), None))), None);
}

#[test]
fn a_new_agents_cwd_is_created_under_the_work_root_only() {
    let root = TempDir::new();
    let work = root.0.join("work");
    fs::create_dir_all(&work).unwrap();
    let fresh = work.join("team").join("fresh");
    assert_eq!(prepare_cwd(&work, &fresh), Ok(true));
    assert!(fresh.is_dir());
    assert_eq!(prepare_cwd(&work, &fresh), Ok(false), "already there");
    let outside = root.0.join("elsewhere").join("x");
    assert_eq!(prepare_cwd(&work, &outside), Ok(false));
    assert!(!outside.exists(), "never created outside the work root");
    assert_eq!(prepare_cwd(&work, &work), Ok(false));
}

#[test]
fn a_spawn_resumes_a_conversation_the_restored_home_holds() {
    let mut args = vec![json!("--dangerously-skip-permissions"), json!("--session-id"), json!("abc")];
    assert_eq!(resume_held(&mut args, &|id| id == "abc"), Some("abc".into()));
    assert_eq!(args, [json!("--dangerously-skip-permissions"), json!("--resume"), json!("abc")]);
    let mut fresh = vec![json!("--session-id"), json!("new")];
    assert_eq!(resume_held(&mut fresh, &|_| false), None, "not held: a new conversation");
    assert_eq!(fresh, [json!("--session-id"), json!("new")]);
    let mut resumed = vec![json!("--resume"), json!("abc")];
    assert_eq!(resume_held(&mut resumed, &|_| true), None, "already a resume");
    assert_eq!(resume_held(&mut [json!("--session-id")], &|_| true), None, "no id");
}
