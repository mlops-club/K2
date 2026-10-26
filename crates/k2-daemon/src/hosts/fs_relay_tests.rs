//! fs-relay: the mapping, the routing decision, the host's checks, and a
//! request through the controller's relay to a host's own daemon (a real
//! one, in process). One test per finding of the security review of
//! 25c5c8c78 (H1–H6, M1–M5), named for it.

use std::path::{Path, PathBuf};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use super::*;
use crate::hosts::hub;
use crate::hosts::proto::{ATTACH_PATH, PROTOCOL};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

const DEADLINE: Duration = Duration::from_secs(30);
const TOKEN: &str = "fs-relay-bootstrap";

async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(DEADLINE, f).await.unwrap_or_else(|_| panic!("timed out waiting for {what}"))
}

fn pb(s: &str) -> PathBuf {
    PathBuf::from(s)
}

fn strs(p: &[&str]) -> Vec<String> {
    p.iter().map(|s| s.to_string()).collect()
}

#[test]
fn the_relay_carries_exact_file_and_git_routes_not_notes_or_the_controllers_own() {
    for (m, p) in [
        ("GET", "/cli/fs/read-dir"),
        ("GET", "/cli/fs/read-file"),
        ("GET", "/cli/fs/stat"),
        ("POST", "/cli/fs/search-tree"),
        ("POST", "/cli/fs/resolve-links"),
        ("POST", "/cli/fs/write-file"),
        ("POST", "/cli/fs/rename"),
        ("POST", "/cli/fs/upload-chunk"),
        ("GET", "/cli/git/changes"),
        ("GET", "/cli/git/diff-file"),
        ("POST", "/cli/git/commit"),
    ] {
        assert!(relayed(m, p), "{m} {p}");
    }
    for (m, p) in [
        // M5/H5: notes are in the controller's database, checked by author.
        ("GET", "/cli/git/notes"),
        ("POST", "/cli/git/notes/add"),
        ("POST", "/cli/git/notes/delete"),
        ("POST", "/cli/git/open-changes"),
        ("POST", "/cli/worktree"),
        // The controller's own machine.
        ("GET", "/cli/fs/events"),
        ("POST", "/cli/fs/open-finder"),
        ("GET", "/cli/fs/clipboard-paths"),
        ("GET", "/cli/fs/search-tree"),
        ("POST", "/cli/fs/compress"),
        // H2: exact routes, not prefixes.
        ("GET", "/cli/git/../users/list"),
        ("POST", "/cli/git/commit/../../users/add"),
        ("GET", "/cli/git/changes/"),
        ("GET", "/cli/fs/write-file"),
        ("PUT", "/cli/fs/write-file"),
    ] {
        assert!(!relayed(m, p), "{m} {p}");
    }
}

/// M4: only known path keys are mapped, by string prefix: file contents,
/// commit messages and search text never change, nor do the bytes after
/// the prefix.
#[test]
fn m4_only_path_keys_are_mapped_and_the_rest_of_a_path_is_kept_byte_for_byte() {
    let maps = vec![(pb("/ctl/work/notes"), pb("/h1/work/notes"))];
    assert_eq!(map_any("/ctl/work/notes", &maps), "/h1/work/notes");
    assert_eq!(map_any("/ctl/work/notes/a//b/", &maps), "/h1/work/notes/a//b/", "no normalisation");
    assert_eq!(map_any("/ctl/work/notes-old/a", &maps), "/ctl/work/notes-old/a", "a component prefix only");
    let q = map_query("path=%2Fctl%2Fwork%2Fnotes%2Fa%20b.md&token=secret&query=%2Fctl%2Fwork%2Fnotes", &maps);
    assert_eq!(q, "path=%2Fh1%2Fwork%2Fnotes%2Fa+b.md&query=%2Fctl%2Fwork%2Fnotes", "the token dropped, the search text kept");
    let mut v = json!({
        "path": "/ctl/work/notes/a.md",
        "content": "/ctl/work/notes is where I keep things",
        "message": "move /ctl/work/notes/x",
        "sources": ["/ctl/work/notes/x", "/ctl/work/notes/y"],
        "entries": [{"path": "/ctl/work/notes/z", "name": "/ctl/work/notes"}],
    });
    map_json(&mut v, &maps);
    assert_eq!(
        v,
        json!({
            "path": "/h1/work/notes/a.md",
            "content": "/ctl/work/notes is where I keep things",
            "message": "move /ctl/work/notes/x",
            "sources": ["/h1/work/notes/x", "/h1/work/notes/y"],
            "entries": [{"path": "/h1/work/notes/z", "name": "/ctl/work/notes"}],
        })
    );
    let mut named = named_paths("path=%2Fa%2Fb&token=%2Fnot&x=%2Fc", br#"{"sources": ["/c/d"], "destination": "/e", "content": "/f"}"#);
    named.sort();
    assert_eq!(named, ["/a/b", "/c/d", "/e"]);
    assert!(named_paths("path=%2Fa&old_path=renamed.md", b"").iter().all(|p| p.starts_with('/')), "a relative name doesn't route");
    assert_eq!(work_root_label(&["pool=a".into(), "k2.work_root=/h1/work".into()]), Some(pb("/h1/work")));
}

/// M2: a host's status and content type can't carry headers.
#[test]
fn m2_a_hosts_status_and_content_type_cant_inject_headers() {
    assert_eq!(safe_status("200 OK\r\nSet-Cookie: k2_session=x"), "200 OK");
    assert_eq!(safe_status("404 Not Found"), "404 Not Found");
    assert_eq!(safe_status("nonsense"), "502 Bad Gateway");
    assert_eq!(safe_status("99 Too Low"), "502 Bad Gateway");
    assert_eq!(safe_content_type("text/html\r\nX-Evil: 1"), "application/json");
    assert_eq!(safe_content_type("text/plain; charset=utf-8"), "text/plain; charset=utf-8");
    assert_eq!(safe_content_type("application/json"), "application/json");
}

// ── a controller, fake hosts ───────────────────────────────────────────

/// A controller with work root `ctl` and its hosts' roots as configured
/// here (`per_host`, else the default), never from the process's env.
async fn controller_at(ctl: &[&str], per_host: &[(&str, &Path)]) -> (&'static RemoteHost, String) {
    let roots = crate::agent_host::remote::HostRoots {
        default: pb(crate::agent_host::remote::DEFAULT_HOST_WORK_ROOT),
        per_host: per_host.iter().map(|(h, r)| (h.to_string(), r.to_path_buf())).collect(),
    };
    let remote = RemoteHost::new().with_relay_roots(ctl.iter().map(|c| pb(c)).collect(), roots);
    serve_controller(Box::leak(Box::new(remote))).await
}

async fn serve_controller(remote: &'static RemoteHost) -> (&'static RemoteHost, String) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}{ATTACH_PATH}", l.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((s, _)) = l.accept().await {
            tokio::spawn(hub::serve_attach(s, remote, TOKEN));
        }
    });
    (remote, url)
}

fn h(root: &str) -> PathBuf {
    pb(root)
}

async fn send(ws: &mut Ws, f: &Frame) {
    ws.send(Message::Text(f.encode())).await.expect("send frame");
}

async fn recv(ws: &mut Ws) -> Frame {
    within("a frame", async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(t))) => return Frame::decode(&t).expect("frame"),
                Some(Ok(Message::Close(_))) | None => panic!("link closed"),
                Some(Ok(_)) => continue,
                Some(Err(e)) => panic!("link error: {e}"),
            }
        }
    })
    .await
}

async fn host(url: &str, name: &str, labels: &[&str]) -> Ws {
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.expect("dial");
    let labels = labels.iter().map(|l| l.to_string()).collect();
    send(&mut ws, &Frame::Hello { protocol: PROTOCOL, host_name: name.into(), token: TOKEN.into(), labels, resume: vec![] }).await;
    assert!(matches!(recv(&mut ws).await, Frame::Welcome { .. }));
    ws
}

/// Place `agent` (cwd `cwd`) on the fake `host`.
async fn place(remote: &'static RemoteHost, host: &mut Ws, agent: &str, cwd: &str) {
    let body = json!({"agent_name": agent, "cwd": cwd, "command": "claude"}).to_string();
    let spawn = tokio::spawn(async move { remote.spawn(body.as_bytes()).await });
    match recv(host).await {
        Frame::Spawn { req, .. } => {
            let reply = json!({"sessionId": uuid::Uuid::new_v4().to_string(), "reused": false}).to_string();
            send(host, &Frame::Reply { req, status: "200 OK".into(), body: reply }).await;
        }
        other => panic!("expected spawn, got {other:?}"),
    }
    let (st, body) = within("the spawn", spawn).await.unwrap();
    assert_eq!(st, "200 OK", "{body}");
}

async fn wait_unlinked(remote: &RemoteHost, host: &str) {
    within("the host to go", async {
        while remote.status_json()["lost"].as_array().is_none_or(|l| !l.iter().any(|h| h == host)) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_path_goes_to_the_host_the_controller_spawned_its_workspace_on() {
    let (remote, url) = controller_at(&["/ctl/work"], &[("h1", &h("/h1/work")), ("h2", &h("/h2/work"))]).await;
    // A host on the shared volume (no label): nothing is bound, everything local.
    let mut shared = host(&url, "shared", &[]).await;
    place(remote, &mut shared, "old", "/ctl/work/old").await;
    assert!(!remote.any_host_on_own_disk());
    assert_eq!(remote.workspace_route(&strs(&["/ctl/work/old/a"])), WorkspaceRoute::Local);
    let mut h1 = host(&url, "h1", &["k2.work_root=/h1/work"]).await;
    let mut h2 = host(&url, "h2", &["k2.work_root=/h2/work"]).await;
    place(remote, &mut h1, "notes", "/ctl/work/notes").await;
    place(remote, &mut h2, "tasks", "/ctl/work/tasks").await;
    assert_eq!(
        remote.workspace_route(&strs(&["/ctl/work/notes/src/a.rs"])),
        WorkspaceRoute::Relay { host: "h1".into(), maps: vec![(pb("/ctl/work/notes"), pb("/h1/work/notes"))] }
    );
    assert_eq!(
        remote.workspace_route(&strs(&["/ctl/work/tasks"])),
        WorkspaceRoute::Relay { host: "h2".into(), maps: vec![(pb("/ctl/work/tasks"), pb("/h2/work/tasks"))] }
    );
    // Only the controller's names route (a host's own path, a sibling, elsewhere: local).
    for p in ["/h2/work/tasks/x", "/ctl/work/other", "/ctl/work/notes-old", "/etc/passwd"] {
        assert_eq!(remote.workspace_route(&strs(&[p])), WorkspaceRoute::Local, "{p}");
    }
    assert_eq!(remote.workspace_route(&strs(&["/ctl/work/notes/a", "/ctl/work/tasks/b"])), WorkspaceRoute::Split(vec!["h1".into(), "h2".into()]));
    drop(h1);
    wait_unlinked(remote, "h1").await;
    assert_eq!(
        remote.workspace_route(&strs(&["/ctl/work/notes/a"])),
        WorkspaceRoute::Away { host: "h1".into(), path: "/ctl/work/notes/a".into() }
    );
    drop((shared, h2));
}

/// The controller's root as given and canonical (/tmp, /private/tmp): a
/// request may name it either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_controllers_root_matches_as_given_and_canonical() {
    let (remote, url) = controller_at(&["/tmp/work", "/private/tmp/work"], &[("h1", &h("/h1/work"))]).await;
    let mut h1 = host(&url, "h1", &["k2.work_root=/h1/work"]).await;
    place(remote, &mut h1, "notes", "/tmp/work/notes").await;
    assert_eq!(
        remote.workspace_route(&strs(&["/private/tmp/work/notes/x"])),
        WorkspaceRoute::Relay { host: "h1".into(), maps: vec![(pb("/private/tmp/work/notes"), pb("/h1/work/notes"))] }
    );
    drop(h1);
}

/// H4 (re-review): the controller decides a host's work root; a hostile
/// label (`/`) and cwds outside the controller's root capture nothing; the
/// longest workspace wins.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h4_a_hostile_work_root_label_and_cwd_capture_nothing() {
    let (remote, url) = controller_at(&["/ctl/work"], &[("h1", &h("/h1/work")), ("h2", &h("/h2/work"))]).await;
    let mut evil = host(&url, "evil", &["k2.work_root=/"]).await;
    place(remote, &mut evil, "root", "/").await;
    place(remote, &mut evil, "home", "/Users/eric").await;
    for p in ["/etc/passwd", "/Users/eric/.ssh/id_ed25519", "/ctl/work/notes/a"] {
        assert_eq!(remote.workspace_route(&strs(&[p])), WorkspaceRoute::Local, "{p}");
    }
    // Its workspace maps to the root the controller configures (the default), never its label's.
    place(remote, &mut evil, "proj", "/ctl/work/proj").await;
    assert_eq!(
        remote.workspace_route(&strs(&["/ctl/work/proj/x"])),
        WorkspaceRoute::Relay { host: "evil".into(), maps: vec![(pb("/ctl/work/proj"), pb("/var/lib/k2/work/proj"))] }
    );
    let mut h1 = host(&url, "h1", &["k2.work_root=/h1/work"]).await;
    let mut h2 = host(&url, "h2", &["k2.work_root=/h2/work"]).await;
    place(remote, &mut h1, "outer", "/ctl/work/outer").await;
    place(remote, &mut h2, "inner", "/ctl/work/outer/inner").await;
    assert!(matches!(remote.workspace_route(&strs(&["/ctl/work/outer/inner/x"])), WorkspaceRoute::Relay { host, .. } if host == "h2"));
    assert!(matches!(remote.workspace_route(&strs(&["/ctl/work/outer/y"])), WorkspaceRoute::Relay { host, .. } if host == "h1"));
    drop((evil, h1, h2));
    // Expiry (and N3: never while placed): n3_a_binding_lives_while_its_agent_is_placed.
}

/// H1 (controller): `..` is refused, and so is a second path outside the
/// workspace the request goes to (a copy's destination).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h1_traversal_and_an_unmapped_second_path_are_refused_on_the_controller() {
    let (remote, url) = controller_at(&["/ctl/work"], &[("h1", &h("/h1/work"))]).await;
    let mut h1 = host(&url, "h1", &["k2.work_root=/h1/work"]).await;
    place(remote, &mut h1, "notes", "/ctl/work/notes").await;
    let refused = |d: Decision| match d {
        Decision::Refuse(a) => (a.status, serde_json::from_str::<Value>(&a.body).unwrap()["code"].clone()),
        other => panic!("expected a refusal, got {other:?}"),
    };
    let q = format!("path={}", encode("/ctl/work/notes/../../../home/k2/.ssh/id_ed25519"));
    assert_eq!(refused(decide(remote, &q, b"")), ("400 Bad Request".into(), json!("bad_path")));
    let copy = json!({"sources": ["/ctl/work/notes/x"], "destination": "/home/k2/.bashrc"}).to_string();
    assert_eq!(refused(decide(remote, "", copy.as_bytes())), ("400 Bad Request".into(), json!("outside_workspace")));
    let inside = json!({"sources": ["/ctl/work/notes/x"], "destination": "/ctl/work/notes/y"}).to_string();
    assert!(matches!(decide(remote, "", inside.as_bytes()), Decision::Relay { .. }));
    // A repo-relative name (a renamed file's old path) doesn't route.
    assert!(matches!(decide(remote, "path=%2Fctl%2Fwork%2Fnotes&old_path=old.md", b""), Decision::Relay { .. }));
    drop(h1);
}

/// M3 and H6: nothing about the hosts before the caller's token; a POST to
/// a host workspace too big to relay is a 413; compress and extract are
/// refused there; an App pass is refused (never the controller's copy).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m3_h6_auth_first_and_no_silent_fallback() {
    let (remote, url) = controller_at(&["/ctl/work"], &[("h1", &h("/h1/work"))]).await;
    let mut h1 = host(&url, "h1", &["k2.work_root=/h1/work"]).await;
    place(remote, &mut h1, "notes", "/ctl/work/notes").await;
    let big = json!({"path": "/ctl/work/notes/big.bin", "content": "x".repeat(MAX_RELAY_BODY + 1)}).to_string();
    assert_eq!(relay_post_on(remote, "/cli/fs/write-file", "token=wrong", "owner", big.as_bytes()).await, None, "the local route's 403");
    let a = relay_post_on(remote, "/cli/fs/write-file", "token=owner", "owner", big.as_bytes()).await.expect("answered");
    assert_eq!(a.status, "413 Payload Too Large");
    let zip = json!({"paths": ["/ctl/work/notes/a"], "destination": "/ctl/work/notes/a.zip"}).to_string();
    let a = relay_post_on(remote, "/cli/fs/compress", "token=owner", "owner", zip.as_bytes()).await.expect("answered");
    assert_eq!(a.status, "501 Not Implemented", "{}", a.body);
    let elsewhere = json!({"paths": ["/ctl/other/a"], "destination": "/ctl/other/a.zip"}).to_string();
    assert_eq!(relay_post_on(remote, "/cli/fs/compress", "token=owner", "owner", elsewhere.as_bytes()).await, None, "not a host's: local");
    // N6: an App pass that isn't valid is the local route's (its own 403), not a 501.
    let w = json!({"workspace": "notes", "path": "a.md", "content": "x"}).to_string();
    assert_eq!(relay_post_on(remote, "/cli/fs/write-file", "token=k2skn_abc", "owner", w.as_bytes()).await, None);
    // N5: an upload chunk without its offset is refused, not taken as 0.
    let chunk = json!({"upload_id": "u", "dir": "/ctl/work/notes", "filename": "f", "base64": "eA==", "is_last": true}).to_string();
    let a = relay_post_on(remote, "/cli/fs/upload-chunk", "token=owner", "owner", chunk.as_bytes()).await.expect("answered");
    assert_eq!(a.status, "400 Bad Request", "{}", a.body);
    drop(h1);
    wait_unlinked(remote, "h1").await;
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut s, _) = l.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        route_on(remote, &mut s, &mut buf, "GET", "/cli/fs/read-dir", "path=%2Fctl%2Fwork%2Fnotes&token=wrong", "owner").await
    });
    let _c = TcpStream::connect(addr).await.unwrap();
    assert!(!within("the gate", server).await.unwrap(), "a bad token: no host_away, the local route answers");
}

// ── the host's side ────────────────────────────────────────────────────

struct HostDisk {
    _root_var: k2_core::test_env::EnvVar,
    _home: crate::test_support::TempHome,
    root: PathBuf,
    ws: PathBuf,
}

/// A host's disk: `K2_WORK_ROOT` (canonical) with a workspace `notes`.
fn host_disk() -> HostDisk {
    let home = crate::test_support::TempHome::new();
    let root = home.path().join("h1-work");
    std::fs::create_dir_all(root.join("notes")).unwrap();
    let root = std::fs::canonicalize(&root).unwrap();
    let var = k2_core::test_env::EnvVar::set(crate::hosts::home::WORK_ROOT_VAR, &root);
    HostDisk { ws: root.join("notes"), root, _home: home, _root_var: var }
}

fn b64(v: &Value) -> String {
    base64::engine::general_purpose::STANDARD.encode(v.to_string())
}

async fn served(method: &str, path: &str, query: &str, body: Option<Value>) -> (String, Value) {
    match serve(1, method, path, query, &body.as_ref().map(b64).unwrap_or_default()).await {
        Frame::HttpReply { status, body, .. } => (status, serde_json::from_str(&body).unwrap_or(Value::String(body))),
        other => panic!("{other:?}"),
    }
}

/// H2: the host takes only the exact routes, spelled plainly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2_dot_segments_and_percent_escapes_never_reach_another_host_route() {
    let d = host_disk();
    let q = format!("path={}", encode(&d.ws.to_string_lossy()));
    for p in [
        "/cli/git/../users/list",
        "/cli/git/%2e%2e/users/list",
        "/cli/git/changes/%2e%2e/%2e%2e/users",
        "/cli/fs/./read-dir",
        "/cli/fs//read-dir",
        "/cli/fs/read-dir?x=1",
        "/cli/fs/read-dir#x",
        "/cli/fs/read-dir\\..\\users",
        "/cli/users/list",
    ] {
        let (st, _) = served("GET", p, &q, None).await;
        assert_eq!(st, "403 Forbidden", "{p}");
    }
}

/// H1 (host): every path is confined to a workspace under the work root,
/// after symlinks; a name never climbs out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h1_the_host_confines_every_path_traversal_a_second_path_and_a_symlink_escape() {
    let d = host_disk();
    let outside = d.root.parent().unwrap().join("secret");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("id_ed25519"), "KEY").unwrap();
    std::os::unix::fs::symlink(&outside, d.ws.join("link")).unwrap();
    let e = |p: &Path| format!("path={}", encode(&p.to_string_lossy()));
    let refused = |r: (String, Value)| r.0 == "403 Forbidden" && r.1["code"] == "outside_work_root";
    // Traversal, written out.
    let trav = format!("path={}", encode(&format!("{}/../../secret/id_ed25519", d.ws.display())));
    assert!(refused(served("GET", "/cli/fs/read-file", &trav, None).await));
    // A path elsewhere; the work root itself.
    assert!(refused(served("GET", "/cli/fs/read-file", &e(&outside.join("id_ed25519")), None).await));
    assert!(refused(served("GET", "/cli/fs/read-dir", &e(&d.root), None).await));
    // Through a symlink inside the workspace.
    assert!(refused(served("GET", "/cli/fs/read-file", &e(&d.ws.join("link/id_ed25519")), None).await));
    // A second path (a copy's destination), and a path under a key the relay doesn't know.
    let copy = json!({"sources": [d.ws.join("a")], "destination": outside.join("x")});
    assert!(refused(served("POST", "/cli/fs/copy", "", Some(copy)).await));
    let odd_key = json!({"path": d.ws.join("a"), "target_dir": outside});
    assert!(refused(served("POST", "/cli/fs/create", "", Some(odd_key)).await));
    // A name inside a path.
    let q = format!("{}&file_path={}", e(&d.ws), encode("../../secret/id_ed25519"));
    assert!(refused(served("GET", "/cli/git/diff-file", &q, None).await));
    // Text keys are not paths: confinement lets it through (to a daemon that isn't there).
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    set_local_daemon(closed, "unused");
    let write = json!({"path": d.ws.join("a.md"), "content": "/etc/passwd is a path in my notes"});
    let (st, body) = served("POST", "/cli/fs/write-file", "", Some(write)).await;
    assert_eq!(st, "502 Bad Gateway", "content is not confined: {body}");
}

/// H3: the host's own token never appears in what it answers, even when
/// its daemon can't be reached (reqwest's error carries the URL).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h3_the_hosts_token_is_not_in_an_error_body() {
    let d = host_disk();
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    set_local_daemon(closed, "HOST-OWNER-SECRET-7f3a");
    let (st, body) = served("GET", "/cli/fs/read-dir", &format!("path={}", encode(&d.ws.to_string_lossy())), None).await;
    assert_eq!(st, "502 Bad Gateway");
    assert!(!body.to_string().contains("HOST-OWNER-SECRET-7f3a"), "{body}");
    assert!(!body.to_string().contains("token"), "{body}");
}

/// M1: an answer too big for one link frame is a 413 on the host.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m1_an_oversize_answer_is_refused_on_the_host_not_sent() {
    let d = host_disk();
    let daemon = crate::test_harness::start("host-owner").await;
    set_local_daemon(daemon.port, "host-owner");
    std::fs::write(d.ws.join("big.txt"), "y".repeat(MAX_RELAY_BODY + 1024)).unwrap();
    let q = format!("path={}", encode(&d.ws.join("big.txt").to_string_lossy()));
    let (st, body) = served("GET", "/cli/fs/read-file", &q, None).await;
    assert_eq!(st, "413 Payload Too Large", "{body}");
}

/// H1 (re-review): a write through a dangling symlink (its target doesn't
/// exist yet) is refused when the target is outside the work root, and so
/// is a relative name through a symlinked folder.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h1_a_write_through_a_dangling_symlink_is_refused() {
    let d = host_disk();
    let outside = d.root.parent().unwrap().join("ssh");
    std::fs::create_dir_all(&outside).unwrap();
    std::os::unix::fs::symlink(outside.join("authorized_keys"), d.ws.join("dangle")).unwrap();
    std::os::unix::fs::symlink(&outside, d.ws.join("dir-link")).unwrap();
    let refused = |r: (String, Value)| r.0 == "403 Forbidden" && r.1["code"] == "outside_work_root";
    let w = json!({"path": d.ws.join("dangle"), "content": "ssh-ed25519 AAAA attacker"});
    assert!(refused(served("POST", "/cli/fs/write-file", "", Some(w)).await));
    assert!(!outside.join("authorized_keys").exists(), "nothing written there");
    let q = format!("path={}&file_path={}", encode(&d.ws.to_string_lossy()), encode("dir-link/authorized_keys"));
    assert!(refused(served("GET", "/cli/git/diff-file", &q, None).await));
    // A link that stays inside the workspace is fine (the daemon isn't reachable: 502, not 403).
    std::os::unix::fs::symlink(d.ws.join("real.md"), d.ws.join("inner")).unwrap();
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    set_local_daemon(closed, "unused");
    let (st, _) = served("POST", "/cli/fs/write-file", "", Some(json!({"path": d.ws.join("inner"), "content": "x"}))).await;
    assert_eq!(st, "502 Bad Gateway");
}

/// M1 (re-review): an answer under the body cap whose frame, escaped, is
/// over what the link carries is a 413 on the host.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m1_a_frame_of_max_size_with_escapes_is_refused_on_the_host() {
    let d = host_disk();
    let daemon = crate::test_harness::start("host-owner").await;
    set_local_daemon(daemon.port, "host-owner");
    // 4,000,000 quotes: the JSON answer is about 8,000,000 bytes (under the
    // 8 MiB body cap); the frame escapes each `\"` again, about 16 MB.
    std::fs::write(d.ws.join("quotes.txt"), "\"".repeat(4_000_000)).unwrap();
    let q = format!("path={}", encode(&d.ws.join("quotes.txt").to_string_lossy()));
    let (st, body) = served("GET", "/cli/fs/read-file", &q, None).await;
    assert_eq!((st.as_str(), &body["code"]), ("413 Payload Too Large", &json!("too_large")), "{body}");
}

/// The app's file browser and git panel through the controller: a
/// workspace on a host's own disk (another work root) is read, searched,
/// written, uploaded to (an 8 MB chunk) and committed there, with every
/// path in the controller's namespace; a renamed file's diff; an App pass
/// refused; a host's status line can't inject a header.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_file_browser_and_git_panel_through_the_controller_see_the_hosts_disk() {
    let d = host_disk();
    let ctl_root = d.root.parent().unwrap().join("ctl-work");
    let (host_root, ws) = (d.root.clone(), d.ws.clone());
    std::fs::create_dir_all(ctl_root.join("notes")).unwrap(); // the controller's own (empty) copy
    std::fs::write(ws.join("on-the-host.md"), "written by the agent\n").unwrap();
    let host_daemon = crate::test_harness::start("host-owner").await;
    set_local_daemon(host_daemon.port, "host-owner");
    let ctl_s = ctl_root.to_string_lossy().into_owned();
    let (remote, url) = controller_at(&[&ctl_s], &[("h1", &host_root)]).await;
    // The label doesn't decide the root: the controller's configuration does.
    let mut h1 = host(&url, "h1", &["k2.work_root=/somewhere/else"]).await;
    let ctl_ws = ctl_root.join("notes");
    place(remote, &mut h1, "notes", &ctl_ws.to_string_lossy()).await;
    tokio::spawn(async move {
        while let Some(Ok(m)) = h1.next().await {
            if let Message::Text(t) = m {
                if let Ok(Frame::HttpRelay { req, method, path, query, body_b64 }) = Frame::decode(&t) {
                    let mut r = serve(req, &method, &path, &query, &body_b64).await;
                    // A hostile host's status line.
                    if let Frame::HttpReply { status, content_type, .. } = &mut r {
                        if path == "/cli/fs/stat" {
                            *status = "200 OK\r\nSet-Cookie: k2_session=stolen".into();
                            *content_type = "text/html\r\nX-Evil: 1".into();
                        }
                    }
                    let _ = h1.send(Message::Text(r.encode())).await;
                }
            }
        }
    });
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = l.accept().await {
            tokio::spawn(async move {
                let mut head = [0u8; 4096];
                let n = s.peek(&mut head).await.unwrap();
                let line = String::from_utf8_lossy(&head[..n]).lines().next().unwrap_or("").to_string();
                let mut parts = line.split(' ');
                let method = parts.next().unwrap_or("").to_string();
                let target = parts.next().unwrap_or("").to_string();
                let (path, query) = target.split_once('?').unwrap_or((&target, ""));
                let mut buf = vec![0u8; 64 * 1024];
                // As the dispatcher: GETs at the gate, POSTs in their arm once the body is read.
                if method == "POST" {
                    let body = http::read_post_body_capped(&mut s, &mut buf, 32 << 20).await.unwrap_or_default();
                    match relay_post_on(remote, path, query, "ctl-owner", &body).await {
                        Some(a) => http::send_response(&mut s, &a.status, &a.content_type, &a.body).await,
                        None => http::send_response(&mut s, "404 Not Found", "application/json", r#"{"error":"local"}"#).await,
                    }
                } else if !route_on(remote, &mut s, &mut buf, &method, path, query, "ctl-owner").await {
                    let _ = http::read_post_body_capped(&mut s, &mut buf, 1 << 20).await;
                    http::send_response(&mut s, "404 Not Found", "application/json", r#"{"error":"local"}"#).await;
                }
            });
        }
    });
    let client = reqwest::Client::builder().pool_max_idle_per_host(0).build().unwrap();
    let base = format!("http://127.0.0.1:{port}");
    let q = |p: &Path| url::form_urlencoded::byte_serialize(p.to_string_lossy().as_bytes()).collect::<String>();
    let get = |path: String| {
        let c = client.clone();
        async move {
            let r = c.get(path).send().await.unwrap();
            let st = r.status().as_u16();
            let injected = r.headers().get("set-cookie").is_some() || r.headers().get("x-evil").is_some();
            let ct = r.headers().get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
            (st, serde_json::from_str::<Value>(&r.text().await.unwrap_or_default()).unwrap_or(Value::Null), injected, ct)
        }
    };
    let post = |path: String, body: Value| {
        let c = client.clone();
        async move {
            let r = c.post(path).body(body.to_string()).send().await.unwrap();
            let st = r.status().as_u16();
            (st, serde_json::from_str::<Value>(&r.text().await.unwrap_or_default()).unwrap_or(Value::Null))
        }
    };

    let (st, dir, ..) = get(format!("{base}/cli/fs/read-dir?path={}&token=ctl-owner", q(&ctl_ws))).await;
    assert_eq!(st, 200, "{dir}");
    let names: Vec<&str> = dir.as_array().unwrap().iter().filter_map(|e| e["name"].as_str()).collect();
    assert_eq!(names, ["on-the-host.md"], "{dir}");
    assert_eq!(dir[0]["path"], json!(ctl_ws.join("on-the-host.md").to_string_lossy()), "paths come back in the controller's namespace");
    let (st, file, ..) = get(format!("{base}/cli/fs/read-file?path={}&token=ctl-owner", q(&ctl_ws.join("on-the-host.md")))).await;
    assert_eq!(st, 200, "{file}");
    assert!(file.to_string().contains("written by the agent"), "{file}");
    // Search (a POST): the host's tree, paths mapped back.
    let (st, found) = post(format!("{base}/cli/fs/search-tree?token=ctl-owner"), json!({"root": ctl_ws, "query": "on-the"})).await;
    assert_eq!(st, 200, "{found}");
    assert!(found.to_string().contains(&*ctl_ws.join("on-the-host.md").to_string_lossy()), "{found}");
    // M2, through the relay: the host's status and type are rebuilt.
    let (st, _, injected, ct) = get(format!("{base}/cli/fs/stat?path={}&token=ctl-owner", q(&ctl_ws))).await;
    assert_eq!((st, injected, ct.as_str()), (200, false, "application/json"));
    let (st, ..) = get(format!("{base}/cli/fs/read-dir?path={}&token=wrong", q(&ctl_ws))).await;
    assert_eq!(st, 404, "a bad token is the local route's to answer");
    let (st, ..) = get(format!("{base}/cli/fs/read-dir?path={}&token=k2skn_pass", q(&ctl_ws))).await;
    assert_eq!(st, 404, "a pass that isn't valid: the local route's to answer");
    let (st, local, ..) = get(format!("{base}/cli/fs/read-dir?path={}&token=ctl-owner", q(&ctl_root))).await;
    assert_eq!((st, local), (404, json!({"error": "local"})));

    let (st, w) = post(
        format!("{base}/cli/fs/write-file?token=ctl-owner"),
        json!({"path": ctl_ws.join("from-the-app.md").to_string_lossy(), "content": "edited in the app\n"}),
    )
    .await;
    assert_eq!(st, 200, "{w}");
    assert_eq!(std::fs::read_to_string(ws.join("from-the-app.md")).unwrap(), "edited in the app\n");
    assert!(!ctl_ws.join("from-the-app.md").exists());

    // An 8 MB upload chunk: relayed in pieces, whole on the host.
    let data: Vec<u8> = (0..8_000_000u32).map(|i| (i % 251) as u8).collect();
    let chunk = json!({
        "upload_id": "relay-8mb", "dir": ctl_ws, "filename": "big.bin", "offset": 0,
        "base64": base64::engine::general_purpose::STANDARD.encode(&data), "is_last": true, "total_bytes": data.len(),
    });
    let (st, up) = post(format!("{base}/cli/fs/upload-chunk?token=ctl-owner"), chunk).await;
    assert_eq!(st, 200, "{up}");
    assert_eq!(std::fs::read(ws.join("big.bin")).unwrap(), data, "the bytes, in order");
    std::fs::remove_file(ws.join("big.bin")).unwrap();

    k2_core::git::init_repo(&ws.to_string_lossy()).unwrap();
    {
        use std::io::Write;
        let mut cfg = std::fs::OpenOptions::new().append(true).open(ws.join(".git/config")).unwrap();
        writeln!(cfg, "[user]\n\tname = lab\n\temail = lab@example.invalid").unwrap();
    }
    let (st, changes, ..) = get(format!("{base}/cli/git/changes?path={}&token=ctl-owner", q(&ctl_ws))).await;
    assert_eq!(st, 200, "{changes}");
    assert!(changes.to_string().contains("on-the-host.md") && changes.to_string().contains("from-the-app.md"), "{changes}");
    let (st, r) = post(format!("{base}/cli/git/stage-all?token=ctl-owner"), json!({"path": ctl_ws.to_string_lossy()})).await;
    assert_eq!(st, 200, "stage: {r}");
    let (st, r) = post(
        format!("{base}/cli/git/commit?token=ctl-owner"),
        json!({"path": ctl_ws.to_string_lossy(), "message": "first, through /tmp/the/controller"}),
    )
    .await;
    assert_eq!(st, 200, "commit: {r}");
    assert_eq!(r["message"], "first, through /tmp/the/controller", "a message is never mapped");
    std::fs::write(ws.join("on-the-host.md"), "written by the agent\nand changed\n").unwrap();
    let (st, diff, ..) = get(format!("{base}/cli/git/diff-file?path={}&file_path=on-the-host.md&token=ctl-owner", q(&ctl_ws))).await;
    assert_eq!(st, 200, "{diff}");
    assert!(diff.to_string().contains("and changed"), "{diff}");
    // A renamed file's diff: its old path is repo-relative.
    std::fs::rename(ws.join("from-the-app.md"), ws.join("renamed.md")).unwrap();
    let (st, diff, ..) = get(format!(
        "{base}/cli/git/diff-file?path={}&file_path=renamed.md&old_path=from-the-app.md&sides=1&token=ctl-owner",
        q(&ctl_ws)
    ))
    .await;
    assert_eq!(st, 200, "a renamed file's diff: {diff}");
}

// ── round 3 ────────────────────────────────────────────────────────────

/// N1: `..` after a symlink is the parent of where the link points, as the
/// kernel walks it: `ws/s -> <outside>/.ssh/inner`, `ws/d -> s/..`, so
/// `ws/d/id_ed25519` is `<outside>/.ssh/id_ed25519`, refused for a read
/// and for a write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn n1_dot_dot_after_a_symlink_is_the_links_parent_read_and_write_refused() {
    let d = host_disk();
    let ssh = d.root.parent().unwrap().join("home-k2/.ssh");
    std::fs::create_dir_all(ssh.join("inner")).unwrap();
    std::fs::write(ssh.join("id_ed25519"), "PRIVATE KEY").unwrap();
    std::os::unix::fs::symlink(ssh.join("inner"), d.ws.join("s")).unwrap();
    std::os::unix::fs::symlink("s/..", d.ws.join("d")).unwrap();
    assert_eq!(resolve(&d.ws.join("d/id_ed25519")), Some(ssh.join("id_ed25519")), "where the kernel opens it");
    let refused = |r: (String, Value)| r.0 == "403 Forbidden" && r.1["code"] == "outside_work_root";
    let q = format!("path={}", encode(&d.ws.join("d/id_ed25519").to_string_lossy()));
    assert!(refused(served("GET", "/cli/fs/read-file", &q, None).await), "read");
    let w = json!({"path": d.ws.join("d/authorized_keys"), "content": "ssh-ed25519 AAAA attacker"});
    assert!(refused(served("POST", "/cli/fs/write-file", "", Some(w)).await), "write");
    assert!(!ssh.join("authorized_keys").exists());
    // A loop is refused, not followed for ever.
    std::os::unix::fs::symlink("loop-b", d.ws.join("loop-a")).unwrap();
    std::os::unix::fs::symlink("loop-a", d.ws.join("loop-b")).unwrap();
    assert_eq!(resolve(&d.ws.join("loop-a/x")), None);
}

/// N3: a binding doesn't expire while its agent is placed on its host.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn n3_a_binding_lives_while_its_agent_is_placed() {
    let roots = crate::agent_host::remote::HostRoots { default: pb("/h/work"), per_host: Default::default() };
    let remote = RemoteHost::new().with_relay_roots(vec![pb("/ctl/work")], roots).with_relay_ttl(Duration::ZERO);
    let (remote, url) = serve_controller(Box::leak(Box::new(remote))).await;
    let mut h = host(&url, "h", &["k2.work_root=/h/work"]).await;
    place(remote, &mut h, "notes", "/ctl/work/notes").await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(matches!(remote.workspace_route(&strs(&["/ctl/work/notes/a"])), WorkspaceRoute::Relay { .. }), "placed: kept past the TTL");
    send(&mut h, &Frame::SessionGone { agent_id: "notes".into(), epoch: None, exit_code: Some(0) }).await;
    within("the placement to go", async {
        while remote.placement("notes").is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(remote.workspace_route(&strs(&["/ctl/work/notes/a"])), WorkspaceRoute::Local, "gone and past the TTL");
}

/// N7: a relayed git request works on its workspace's own repository: not
/// one above it, not a git dir or `core.worktree` outside the work root.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn n7_git_never_reaches_past_the_work_root() {
    let d = host_disk();
    let refused = |r: (String, Value)| r.0 == "403 Forbidden" && r.1["code"] == "outside_work_root";
    let q = |p: &Path| format!("path={}", encode(&p.to_string_lossy()));
    // No .git in the workspace, a repository above the work root: the one
    // git would open is outside, refused.
    let above = d.root.parent().unwrap().to_path_buf();
    k2_core::git::init_repo(&above.to_string_lossy()).unwrap();
    assert!(refused(served("GET", "/cli/git/changes", &q(&d.ws), None).await));
    std::fs::remove_dir_all(above.join(".git")).unwrap();
    // A gitfile pointing out of the work root.
    let elsewhere = d.root.parent().unwrap().join("elsewhere-repo");
    std::fs::create_dir_all(&elsewhere).unwrap();
    k2_core::git::init_repo(&elsewhere.to_string_lossy()).unwrap();
    std::fs::write(d.ws.join(".git"), format!("gitdir: {}\n", elsewhere.join(".git").display())).unwrap();
    assert!(refused(served("GET", "/cli/git/changes", &q(&d.ws), None).await));
    // core.worktree out of the work root.
    std::fs::remove_file(d.ws.join(".git")).unwrap();
    k2_core::git::init_repo(&d.ws.to_string_lossy()).unwrap();
    {
        use std::io::Write;
        let mut cfg = std::fs::OpenOptions::new().append(true).open(d.ws.join(".git/config")).unwrap();
        writeln!(cfg, "[core]\n\tworktree = {}", elsewhere.display()).unwrap();
    }
    assert!(refused(served("GET", "/cli/git/changes", &q(&d.ws), None).await));
}

/// N7: with hooks off (an agent host), K2's own git commands run none.
#[test]
fn n7_k2s_git_commands_run_no_hooks_on_a_host() {
    let dir = k2_core::test_env::unique_temp_path("relay-hooks");
    std::fs::create_dir_all(&dir).unwrap();
    let repo = dir.to_string_lossy().into_owned();
    k2_core::git::init_repo(&repo).unwrap();
    let git = |args: &[&str]| {
        std::process::Command::new("git")
            .args(["-c", "user.name=lab", "-c", "user.email=lab@example.invalid"])
            .args(args)
            .current_dir(&dir)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .unwrap()
    };
    std::fs::write(dir.join("a.md"), "a").unwrap();
    git(&["add", "-A"]);
    git(&["commit", "-qm", "first"]);
    let marker = dir.join("hook-ran");
    k2_core::test_env::write_executable(&dir.join(".git/hooks/post-checkout"), &format!("#!/bin/sh\ntouch {}\n", marker.display()));
    k2_core::git::set_no_hooks(true);
    let made = k2_core::git::create_worktree(&repo, "relay-branch");
    k2_core::git::set_no_hooks(false);
    assert!(made.is_ok(), "{made:?}");
    assert!(!marker.exists(), "the post-checkout hook didn't run");
    let _ = std::fs::remove_dir_all(&dir);
}

/// N6: resolve-links answers per link: one outside the work root is
/// `linkable: false` in its place, the others are resolved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn n6_resolve_links_refuses_a_bad_link_not_the_batch() {
    let d = host_disk();
    let daemon = crate::test_harness::start("host-owner").await;
    set_local_daemon(daemon.port, "host-owner");
    std::fs::write(d.ws.join("a.md"), "a").unwrap();
    let outside = d.root.parent().unwrap().join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    std::os::unix::fs::symlink(&outside, d.ws.join("out-link")).unwrap();
    k2_core::projects_ops::projects_create("notes", &d.ws.to_string_lossy(), None).unwrap();
    let body = json!({"workspace": d.ws, "paths": ["a.md", "/etc/passwd", "~/x", "out-link/y", "docs/../a.md"]});
    let (st, v) = served("POST", "/cli/fs/resolve-links", "", Some(body)).await;
    assert_eq!(st, "200 OK", "{v}");
    let links = v["links"].as_array().unwrap();
    assert_eq!(links.len(), 5, "{v}");
    assert_eq!(links[0]["input"], "a.md");
    assert_eq!(links[0]["linkable"], true, "{v}");
    for (i, input) in [(1, "/etc/passwd"), (2, "~/x"), (3, "out-link/y"), (4, "docs/../a.md")] {
        assert_eq!((links[i]["input"].as_str(), links[i]["linkable"].as_bool()), (Some(input), Some(false)), "{v}");
    }
}

// ── round 4 ────────────────────────────────────────────────────────────

fn host_git(dir: &Path, args: &[&str]) {
    let out = std::process::Command::new("git")
        .args(["-c", "user.name=lab", "-c", "user.email=lab@example.invalid"])
        .args(args)
        .current_dir(dir)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .unwrap();
    assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
}

/// N7 (round 4): a nested repository whose `core.worktree` is outside the
/// work root, and a one-line `[core] worktree = …` config: the repository
/// git would open is checked, not a text parse of one file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn n7_a_nested_repo_and_a_one_line_config_cant_reach_out() {
    let d = host_disk();
    let refused = |r: (String, Value)| r.0 == "403 Forbidden" && r.1["code"] == "outside_work_root";
    let q = |p: &Path| format!("path={}&file_path=id_ed25519&sides=1", encode(&p.to_string_lossy()));
    let outside = d.root.parent().unwrap().join("home-k2");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("id_ed25519"), "KEY").unwrap();
    k2_core::git::init_repo(&d.ws.to_string_lossy()).unwrap();
    // Nested: ws/sub/.git, its work tree outside.
    let sub = d.ws.join("sub");
    std::fs::create_dir_all(&sub).unwrap();
    k2_core::git::init_repo(&sub.to_string_lossy()).unwrap();
    host_git(&sub, &["config", "core.worktree", &outside.to_string_lossy()]);
    assert!(refused(served("GET", "/cli/git/diff-file", &q(&sub), None).await), "the nested repository");
    std::fs::remove_dir_all(&sub).unwrap();
    // One line: `[core] worktree = <outside>`.
    {
        use std::io::Write;
        let mut cfg = std::fs::OpenOptions::new().append(true).open(d.ws.join(".git/config")).unwrap();
        writeln!(cfg, "[core] worktree = {}", outside.display()).unwrap();
    }
    assert!(refused(served("GET", "/cli/git/diff-file", &q(&d.ws), None).await), "the one-line config");
}

/// N7 (round 4): the worktree routes name their repository `projectPath`;
/// they pass the check (a 403 before), and run on the host.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn n7_the_worktree_routes_pass_with_project_path() {
    let d = host_disk();
    let daemon = crate::test_harness::start("host-owner").await;
    set_local_daemon(daemon.port, "host-owner");
    k2_core::git::init_repo(&d.ws.to_string_lossy()).unwrap();
    std::fs::write(d.ws.join("a.md"), "a").unwrap();
    host_git(&d.ws, &["add", "-A"]);
    host_git(&d.ws, &["commit", "-qm", "first"]);
    let project = k2_core::projects_ops::projects_create("notes", &d.ws.to_string_lossy(), None).unwrap();
    let body = json!({"projectPath": d.ws, "branch": "relay-wt", "projectId": project.id});
    let (st, v) = served("POST", "/cli/git/create-worktree", "", Some(body)).await;
    assert_eq!(st, "200 OK", "{v}");
    let (st, v) = served("POST", "/cli/git/prune-worktrees", "", Some(json!({"projectPath": d.ws, "projectId": project.id}))).await;
    assert_eq!(st, "200 OK", "{v}");
    // A projectPath outside the work root is refused before anything runs.
    let (st, _) = served("POST", "/cli/git/prune-worktrees", "", Some(json!({"projectPath": "/etc", "projectId": project.id}))).await;
    assert_eq!(st, "403 Forbidden");
}

/// N3 (round 4): a database made before `relay_space.agent_id` gets it.
#[test]
fn n3_an_older_relay_space_table_gets_its_agent_column() {
    let path = k2_core::test_env::unique_temp_path("relay-space-old.db");
    {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.execute_batch("CREATE TABLE relay_space (rel TEXT PRIMARY KEY, host_id TEXT NOT NULL, touched_ms INTEGER NOT NULL); INSERT INTO relay_space VALUES ('notes', 'h1', 1);").unwrap();
    }
    let db = std::sync::Arc::new(parking_lot::ReentrantMutex::new(rusqlite::Connection::open(&path).unwrap()));
    let store = crate::agent_host::remote::PlacementStore::open(db).unwrap();
    store.relay_space("tasks", "h2", 2, "agent-t").unwrap();
    let rows = store.load_relay_spaces().unwrap();
    assert!(rows.contains(&("notes".into(), "h1".into(), 1, String::new())), "{rows:?}");
    assert!(rows.contains(&("tasks".into(), "h2".into(), 2, "agent-t".into())), "{rows:?}");
    let _ = std::fs::remove_file(&path);
}
