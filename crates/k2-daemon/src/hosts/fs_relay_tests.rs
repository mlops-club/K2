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
        ("POST", "/cli/fs/resolve-links"),
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

async fn controller() -> (&'static RemoteHost, String) {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}{ATTACH_PATH}", l.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((s, _)) = l.accept().await {
            tokio::spawn(hub::serve_attach(s, remote, TOKEN));
        }
    });
    (remote, url)
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
async fn a_path_goes_to_the_host_its_agents_workspace_is_on() {
    let (remote, url) = controller().await;
    let ctl = [pb("/ctl/work")];
    // A host on the shared volume (no label): everything stays local.
    let mut shared = host(&url, "shared", &[]).await;
    place(remote, &mut shared, "old", "/ctl/work/old").await;
    assert!(!remote.any_host_on_own_disk());
    assert_eq!(remote.workspace_route(&strs(&["/ctl/work/old/a"]), &ctl), WorkspaceRoute::Local);
    let mut h1 = host(&url, "h1", &["k2.work_root=/h1/work"]).await;
    let mut h2 = host(&url, "h2", &["k2.work_root=/h2/work"]).await;
    // notes as the controller asked for it; tasks as host 2 names it.
    place(remote, &mut h1, "notes", "/ctl/work/notes").await;
    place(remote, &mut h2, "tasks", "/h2/work/tasks").await;
    assert_eq!(
        remote.workspace_route(&strs(&["/ctl/work/notes/src/a.rs"]), &ctl),
        WorkspaceRoute::Relay { host: "h1".into(), maps: vec![(pb("/ctl/work/notes"), pb("/h1/work/notes"))] }
    );
    assert_eq!(
        remote.workspace_route(&strs(&["/ctl/work/tasks"]), &ctl),
        WorkspaceRoute::Relay { host: "h2".into(), maps: vec![(pb("/ctl/work/tasks"), pb("/h2/work/tasks"))] }
    );
    // The controller's root as given and canonical (/tmp, /private/tmp): either name.
    let two = [pb("/tmp/work"), pb("/private/tmp/work")];
    assert_eq!(
        remote.workspace_route(&strs(&["/private/tmp/work/tasks/x"]), &two),
        WorkspaceRoute::Relay { host: "h2".into(), maps: vec![(pb("/private/tmp/work/tasks"), pb("/h2/work/tasks"))] }
    );
    assert_eq!(remote.workspace_route(&strs(&["/ctl/work/other"]), &ctl), WorkspaceRoute::Local);
    assert_eq!(remote.workspace_route(&strs(&["/ctl/work/notes-old"]), &ctl), WorkspaceRoute::Local);
    assert_eq!(
        remote.workspace_route(&strs(&["/ctl/work/notes/a", "/ctl/work/tasks/b"]), &ctl),
        WorkspaceRoute::Split(vec!["h1".into(), "h2".into()])
    );
    drop(h1);
    wait_unlinked(remote, "h1").await;
    assert_eq!(
        remote.workspace_route(&strs(&["/ctl/work/notes/a"]), &ctl),
        WorkspaceRoute::Away { host: "h1".into(), path: "/ctl/work/notes/a".into() }
    );
    drop((shared, h2));
}

/// H4: a host that reports a cwd outside its work root (`/`, a home dir,
/// the root itself) routes nothing; the longest workspace wins.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h4_a_malicious_cwd_captures_nothing_and_the_longest_workspace_wins() {
    let (remote, url) = controller().await;
    let ctl = [pb("/ctl/work")];
    let mut evil = host(&url, "evil", &["k2.work_root=/evil/work"]).await;
    place(remote, &mut evil, "root", "/").await;
    place(remote, &mut evil, "home", "/home/k2").await;
    place(remote, &mut evil, "itsroot", "/evil/work").await;
    for p in ["/etc/passwd", "/home/k2/.ssh/id_ed25519", "/ctl/work/notes/a", "/evil/work/x"] {
        assert_eq!(remote.workspace_route(&strs(&[p]), &ctl), WorkspaceRoute::Local, "{p}");
    }
    // Two workspaces, one inside the other, on two hosts: the deeper one wins.
    let mut h1 = host(&url, "h1", &["k2.work_root=/h1/work"]).await;
    let mut h2 = host(&url, "h2", &["k2.work_root=/h2/work"]).await;
    place(remote, &mut h1, "outer", "/ctl/work/outer").await;
    place(remote, &mut h2, "inner", "/ctl/work/outer/inner").await;
    assert!(matches!(remote.workspace_route(&strs(&["/ctl/work/outer/inner/x"]), &ctl), WorkspaceRoute::Relay { host, .. } if host == "h2"));
    assert!(matches!(remote.workspace_route(&strs(&["/ctl/work/outer/y"]), &ctl), WorkspaceRoute::Relay { host, .. } if host == "h1"));
    drop((evil, h1, h2));
}

/// H1 (controller): `..` is refused, and so is a second path outside the
/// workspace the request goes to (a copy's destination).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h1_traversal_and_an_unmapped_second_path_are_refused_on_the_controller() {
    let (remote, url) = controller().await;
    let ctl = [pb("/ctl/work")];
    let mut h1 = host(&url, "h1", &["k2.work_root=/h1/work"]).await;
    place(remote, &mut h1, "notes", "/ctl/work/notes").await;
    let refused = |d: Decision| match d {
        Decision::Refuse(a) => (a.status, serde_json::from_str::<Value>(&a.body).unwrap()["code"].clone()),
        other => panic!("expected a refusal, got {other:?}"),
    };
    let q = format!("path={}", encode("/ctl/work/notes/../../../home/k2/.ssh/id_ed25519"));
    assert_eq!(refused(decide(remote, &ctl, &q, b"")), ("400 Bad Request".into(), json!("bad_path")));
    let copy = json!({"sources": ["/ctl/work/notes/x"], "destination": "/home/k2/.bashrc"}).to_string();
    assert_eq!(refused(decide(remote, &ctl, "", copy.as_bytes())), ("400 Bad Request".into(), json!("outside_workspace")));
    let inside = json!({"sources": ["/ctl/work/notes/x"], "destination": "/ctl/work/notes/y"}).to_string();
    assert!(matches!(decide(remote, &ctl, "", inside.as_bytes()), Decision::Relay { .. }));
    drop(h1);
}

/// M3 and H6: nothing about the hosts before the caller's token; a POST to
/// a host workspace too big to relay is a 413, never the controller's copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m3_h6_auth_first_and_no_silent_fallback_for_a_big_post() {
    let (remote, url) = controller().await;
    let ctl = [pb("/ctl/work")];
    let mut h1 = host(&url, "h1", &["k2.work_root=/h1/work"]).await;
    place(remote, &mut h1, "notes", "/ctl/work/notes").await;
    let big = json!({"path": "/ctl/work/notes/big.bin", "content": "x".repeat(MAX_RELAY_BODY + 1)}).to_string();
    // A bad token: left to the local route (its own 403), nothing decided.
    assert_eq!(relay_post_on(remote, &ctl, "/cli/fs/write-file", "token=wrong", "owner", big.as_bytes()).await, None);
    // The owner, too big: 413, not the local copy.
    let a = relay_post_on(remote, &ctl, "/cli/fs/write-file", "token=owner", "owner", big.as_bytes()).await.expect("answered");
    assert_eq!(a.status, "413 Payload Too Large");
    // A GET with a bad token to an away host's workspace: no host_away (no map leaked).
    drop(h1);
    wait_unlinked(remote, "h1").await;
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut s, _) = l.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        route_on(remote, &ctl, &mut s, &mut buf, "GET", "/cli/fs/read-dir", "path=%2Fctl%2Fwork%2Fnotes&token=wrong", "owner").await
    });
    let _c = TcpStream::connect(addr).await.unwrap();
    assert!(!within("the gate", server).await.unwrap(), "not answered by the relay");
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

/// The app's file browser and git panel through the controller: a
/// workspace on a host's own disk (another work root) is read, written and
/// committed there, with every path in the controller's namespace; a
/// host's status line can't inject a header.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_file_browser_and_git_panel_through_the_controller_see_the_hosts_disk() {
    let d = host_disk();
    let ctl_root = d.root.parent().unwrap().join("ctl-work");
    let (host_root, ws) = (d.root.clone(), d.ws.clone());
    std::fs::create_dir_all(ctl_root.join("notes")).unwrap(); // the controller's own (empty) copy
    std::fs::write(ws.join("on-the-host.md"), "written by the agent\n").unwrap();
    let host_daemon = crate::test_harness::start("host-owner").await;
    set_local_daemon(host_daemon.port, "host-owner");
    let (remote, url) = controller().await;
    let mut h1 = host(&url, "h1", &[&format!("k2.work_root={}", host_root.display())]).await;
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
    let roots = vec![ctl_root.clone()];
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = l.accept().await {
            let roots = roots.clone();
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
                    match relay_post_on(remote, &roots, path, query, "ctl-owner", &body).await {
                        Some(a) => http::send_response(&mut s, &a.status, &a.content_type, &a.body).await,
                        None => http::send_response(&mut s, "404 Not Found", "application/json", r#"{"error":"local"}"#).await,
                    }
                } else if !route_on(remote, &roots, &mut s, &mut buf, &method, path, query, "ctl-owner").await {
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
    // M2, through the relay: the host's status and type are rebuilt.
    let (st, _, injected, ct) = get(format!("{base}/cli/fs/stat?path={}&token=ctl-owner", q(&ctl_ws))).await;
    assert_eq!((st, injected, ct.as_str()), (200, false, "application/json"));
    let (st, ..) = get(format!("{base}/cli/fs/read-dir?path={}&token=wrong", q(&ctl_ws))).await;
    assert_eq!(st, 404, "a bad token is the local route's to answer");
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
}
