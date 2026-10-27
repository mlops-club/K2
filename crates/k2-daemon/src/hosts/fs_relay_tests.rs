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
    let remote = RemoteHost::new().with_fs_relay(true).with_relay_roots(ctl.iter().map(|c| pb(c)).collect(), roots);
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
    assert_eq!(refused(decide(remote, "/cli/fs/read-file", &q, b"")), ("400 Bad Request".into(), json!("bad_path")));
    let copy = json!({"sources": ["/ctl/work/notes/x"], "destination": "/home/k2/.bashrc"}).to_string();
    assert_eq!(refused(decide(remote, "/cli/fs/copy", "", copy.as_bytes())), ("400 Bad Request".into(), json!("outside_workspace")));
    let inside = json!({"sources": ["/ctl/work/notes/x"], "destination": "/ctl/work/notes/y"}).to_string();
    assert!(matches!(decide(remote, "/cli/fs/copy", "", inside.as_bytes()), Decision::Relay { .. }));
    // A repo-relative name (a renamed file's old path) doesn't route.
    assert!(matches!(decide(remote, "/cli/git/diff-file", "path=%2Fctl%2Fwork%2Fnotes&old_path=old.md", b""), Decision::Relay { .. }));
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
    // R6: a key the route's schema doesn't declare is refused as such.
    let odd_key = json!({"path": d.ws.join("a"), "target_dir": outside});
    let (st, v) = served("POST", "/cli/fs/create", "", Some(odd_key)).await;
    assert_eq!((st.as_str(), &v["code"]), ("403 Forbidden", &json!("undeclared_key")));
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
    let remote = RemoteHost::new().with_fs_relay(true).with_relay_roots(vec![pb("/ctl/work")], roots).with_relay_ttl(Duration::ZERO);
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
    let _hardened = Hardened::on();
    let made = k2_core::git::create_worktree(&repo, "relay-branch");
    drop(_hardened);
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

// ── round 5 ────────────────────────────────────────────────────────────

/// K2's git hardening is process-wide: one test at a time turns it on.
struct Hardened(#[allow(dead_code)] std::sync::MutexGuard<'static, ()>);

impl Hardened {
    fn on() -> Hardened {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let g = LOCK.lock().unwrap_or_else(|p| p.into_inner());
        k2_core::git::set_no_hooks(true);
        Hardened(g)
    }
}

impl Drop for Hardened {
    fn drop(&mut self) {
        k2_core::git::set_no_hooks(false);
    }
}

/// R5-1: a relative value under a path key, with an absolute key beside it
/// to route the request: refused on the controller and on the host (a
/// handler would take it against the daemon's working directory), for a
/// read and for a write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r5_a_relative_path_beside_an_absolute_key_is_refused_read_and_write() {
    let (remote, url) = controller_at(&["/ctl/work"], &[("h1", &h("/h1/work"))]).await;
    let mut h1 = host(&url, "h1", &["k2.work_root=/h1/work"]).await;
    place(remote, &mut h1, "notes", "/ctl/work/notes").await;
    let code = |d: Decision| match d {
        Decision::Refuse(a) => serde_json::from_str::<Value>(&a.body).unwrap()["code"].clone(),
        other => panic!("expected a refusal, got {other:?}"),
    };
    assert_eq!(code(decide(remote, "/cli/fs/read-file", "path=.k2%2Fdaemon.token&dir=%2Fctl%2Fwork%2Fnotes", b"")), json!("relative_path"));
    let w = json!({"path": ".k2/evil", "dir": "/ctl/work/notes", "content": "x"}).to_string();
    assert_eq!(code(decide(remote, "/cli/fs/write-file", "", w.as_bytes())), json!("relative_path"));
    drop(h1);
    // The host refuses the same, on its own.
    let d = host_disk();
    let refused = |r: (String, Value)| r.0 == "403 Forbidden" && r.1["code"] == "outside_work_root";
    let q = format!("path=.k2%2Fdaemon.token&dir={}", encode(&d.ws.to_string_lossy()));
    assert!(refused(served("GET", "/cli/fs/read-file", &q, None).await), "read");
    let w = json!({"path": ".k2/evil", "dir": d.ws, "content": "x"});
    assert!(refused(served("POST", "/cli/fs/write-file", "", Some(w)).await), "write");
    // A repo-relative name on a git route is fine, against its absolute path
    // (R6: stage's key is `filePath`, as its handler reads it).
    let ok = json!({"path": d.ws, "filePath": "a.md"});
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    set_local_daemon(closed, "unused");
    k2_core::git::init_repo(&d.ws.to_string_lossy()).unwrap();
    let (st, _) = served("POST", "/cli/git/stage", "", Some(ok)).await;
    assert_eq!(st, "502 Bad Gateway", "passes the checks (the daemon isn't there)");
}

/// R5-1: a relative repository path (a nested repo with its work tree
/// outside), beside an absolute `projectPath`: refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r5_a_relative_nested_repo_is_refused() {
    let d = host_disk();
    let outside = d.root.parent().unwrap().join("home-k2");
    std::fs::create_dir_all(&outside).unwrap();
    k2_core::git::init_repo(&d.ws.to_string_lossy()).unwrap();
    let sub = d.ws.join("sub");
    std::fs::create_dir_all(&sub).unwrap();
    k2_core::git::init_repo(&sub.to_string_lossy()).unwrap();
    host_git(&sub, &["config", "core.worktree", &outside.to_string_lossy()]);
    let rel = sub.strip_prefix("/").unwrap().to_string_lossy().into_owned();
    let q = format!("path={}&file_path=id_ed25519&sides=1&projectPath={}", encode(&rel), encode(&d.ws.to_string_lossy()));
    let (st, v) = served("GET", "/cli/git/diff-file", &q, None).await;
    assert_eq!((st.as_str(), &v["code"]), ("403 Forbidden", &json!("outside_work_root")), "{v}");
}

/// R5-2: creating a worktree runs none of the repository's own commands:
/// not a smudge filter (`.gitattributes` + `filter.<x>.smudge`), not
/// `core.fsmonitor`; the worktree still has its files (libgit2 checkout).
#[test]
fn r5_create_worktree_runs_no_smudge_filter_or_fsmonitor() {
    let dir = k2_core::test_env::unique_temp_path("relay-filters");
    std::fs::create_dir_all(&dir).unwrap();
    let repo = dir.to_string_lossy().into_owned();
    k2_core::git::init_repo(&repo).unwrap();
    std::fs::write(dir.join("a.md"), "content a\n").unwrap();
    std::fs::write(dir.join(".gitattributes"), "*.md filter=evil\n").unwrap();
    host_git(&dir, &["add", "-A"]);
    host_git(&dir, &["commit", "-qm", "first"]);
    let smudged = dir.join("smudge-ran");
    let monitored = dir.join("fsmonitor-ran");
    host_git(&dir, &["config", "filter.evil.smudge", &format!("touch {}; cat", smudged.display())]);
    host_git(&dir, &["config", "core.fsmonitor", &format!("touch {}; true", monitored.display())]);
    let made = {
        let _hardened = Hardened::on();
        k2_core::git::create_worktree(&repo, "relay-filters")
    };
    let made = made.expect("created");
    assert!(!smudged.exists(), "the smudge filter didn't run");
    assert!(!monitored.exists(), "the fsmonitor didn't run");
    let wt = std::path::PathBuf::from(made.path);
    assert_eq!(std::fs::read_to_string(wt.join("a.md")).unwrap(), "content a\n", "checked out by libgit2");
    // The control: K2's git unhardened (a local daemon) does run the filter,
    // so the test above would have caught it.
    let _serial = Hardened::on();
    k2_core::git::set_no_hooks(false);
    k2_core::git::create_worktree(&repo, "control").expect("created");
    assert!(smudged.exists(), "unhardened, git runs the repository's filter");
    drop(_serial);
    let _ = std::fs::remove_dir_all(&dir);
}

// ── round 6 ────────────────────────────────────────────────────────────

/// R6: fs-relay is off unless the controller has `K2_FS_RELAY=1`. Off, a
/// host on its own disk binds nothing and every route is the controller's
/// own: the gate and the POST arms hand the request back untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r6_off_by_default_the_controller_serves_every_route_itself() {
    {
        let _unset = k2_core::test_env::EnvVar::remove(FS_RELAY_VAR);
        assert!(!RemoteHost::new().fs_relay_on(), "unset: off");
    }
    for (v, on) in [("1", true), ("0", false), ("true", false), ("", false)] {
        let _set = k2_core::test_env::EnvVar::set(FS_RELAY_VAR, v);
        assert_eq!(RemoteHost::new().fs_relay_on(), on, "K2_FS_RELAY={v}");
    }
    let roots = crate::agent_host::remote::HostRoots { default: pb("/h1/work"), per_host: Default::default() };
    let remote = RemoteHost::new().with_fs_relay(false).with_relay_roots(vec![pb("/ctl/work")], roots);
    let (remote, url) = serve_controller(Box::leak(Box::new(remote))).await;
    let mut h1 = host(&url, "h1", &["k2.work_root=/h1/work"]).await;
    place(remote, &mut h1, "notes", "/ctl/work/notes").await;
    assert_eq!(remote.workspace_route(&strs(&["/ctl/work/notes/a.md"])), WorkspaceRoute::Local, "nothing bound");
    let body = json!({"path": "/ctl/work/notes/a.md", "content": "x"}).to_string();
    assert_eq!(relay_post_on(remote, "/cli/fs/write-file", "token=ctl-owner", "ctl-owner", body.as_bytes()).await, None);
    let refused = json!({"path": "/ctl/work/notes/../../x", "content": "x"}).to_string();
    assert_eq!(relay_post_on(remote, "/cli/fs/write-file", "token=ctl-owner", "ctl-owner", refused.as_bytes()).await, None, "not even refused");
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let (mut s, _client) = tokio::join!(async { l.accept().await.unwrap().0 }, TcpStream::connect(addr));
    let mut buf = vec![0u8; 1024];
    let q = format!("token=ctl-owner&path={}", encode("/ctl/work/notes/a.md"));
    assert!(!route_on(remote, &mut s, &mut buf, "GET", "/cli/fs/read-file", &q, "ctl-owner").await, "the gate hands it back");
    drop(h1);
}

/// R6: every relayed route has exactly one schema; its path keys are the
/// ones the controller maps and routes, and every relative key joins a path
/// key of the same route.
#[test]
fn r6_every_relayed_route_has_one_schema() {
    use schema::Kind;
    for (i, s) in schema::SCHEMAS.iter().enumerate() {
        assert!(
            !schema::SCHEMAS[..i].iter().any(|t| t.route == s.route),
            "{} has one schema (and one method)",
            s.route
        );
        assert!(relayed(s.method, s.route));
        for (k, kind) in s.keys {
            match kind {
                Kind::Abs | Kind::Candidates => assert!(PATH_KEYS.contains(k), "{} {k}: a path key the controller maps", s.route),
                Kind::RepoRel(base) | Kind::NameIn(base) | Kind::NameBeside(base) => {
                    assert_eq!(s.kind(base), Some(Kind::Abs), "{} {k} joins {base}, one of its paths", s.route)
                }
                Kind::Plain => assert!(!PATH_KEYS.contains(k) || *k == "base", "{} {k}: not a path key", s.route),
            }
        }
    }
}

/// The body of `fn name(` in `src`: from its first `{` to the matching `}`.
fn fn_body<'a>(src: &'a str, name: &str) -> &'a str {
    let at = [format!("pub fn {name}("), format!("fn {name}(")]
        .iter()
        .find_map(|sig| src.find(sig.as_str()))
        .unwrap_or_else(|| panic!("no fn {name}"));
    let open = at + src[at..].find('{').unwrap();
    let mut depth = 0;
    for (i, c) in src[open..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return &src[at..open + i + 1];
                }
            }
            _ => {}
        }
    }
    panic!("unbalanced fn {name}")
}

/// Every `"…"` right after `pat` in `body`.
fn quoted_after(body: &str, pat: &str) -> Vec<String> {
    body.match_indices(pat).filter_map(|(i, _)| body[i + pat.len()..].split('"').next().map(str::to_string)).collect()
}

fn camel(s: &str) -> String {
    let mut out = String::new();
    let mut up = false;
    for c in s.chars() {
        if c == '_' {
            up = true;
        } else if up {
            out.extend(c.to_uppercase());
            up = false;
        } else {
            out.push(c);
        }
    }
    out
}

/// The JSON keys `struct name` deserializes (its serde renames applied).
fn struct_keys(src: &str, name: &str) -> Vec<String> {
    let at = src.find(&format!("struct {name} {{")).unwrap_or_else(|| panic!("no struct {name}"));
    let attrs = &src[src[..at].rfind("\n\n").unwrap_or(0)..at];
    let camel_case = attrs.contains("rename_all = \"camelCase\"");
    let body = &src[at..at + src[at..].find("\n}").unwrap()];
    let mut keys = Vec::new();
    let mut rename = None;
    for line in body.lines().skip(1) {
        let l = line.trim();
        if l.starts_with("#[serde(") && l.contains("rename = \"") {
            rename = quoted_after(l, "rename = \"").into_iter().next();
        } else if let Some((f, _)) = l.strip_prefix("pub ").unwrap_or(l).split_once(':').filter(|_| !l.starts_with("//") && !l.starts_with('#')) {
            let f = f.trim();
            keys.push(rename.take().unwrap_or_else(|| if camel_case { camel(f) } else { f.to_string() }));
        }
    }
    keys
}

/// The request keys `handler` (in `src`) reads: through `params.get`,
/// `str_param`/`flag_param`, the struct it parses its body into, and any
/// fn of the same file it hands `params` to. A use of `params` or `body`
/// the scan can't read fails it.
fn handler_keys(src: &str, handler: &str) -> Vec<String> {
    let body = fn_body(src, handler);
    let inner = &body[body.find('{').unwrap()..];
    let mut keys = Vec::new();
    for pat in ["params.get(\"", "str_param(params, \"", "flag_param(params, \""] {
        keys.extend(quoted_after(inner, pat));
    }
    let mut understood = 0;
    for pat in ["params.get(\"", "str_param(params, \"", "flag_param(params, \""] {
        understood += inner.matches(pat).count();
    }
    // `params` handed whole to another fn of this file.
    for (i, _) in inner.match_indices("params)") {
        let call = &inner[..i];
        let name_end = call.rfind('(').unwrap();
        let name = call[..name_end].rsplit(|c: char| !(c.is_alphanumeric() || c == '_')).next().unwrap();
        if name.starts_with("handle_") {
            keys.extend(handler_keys(src, name));
            understood += 1;
        }
    }
    let used = inner.match_indices("params").filter(|(i, _)| !inner[i + 6..].starts_with(':')).count();
    assert_eq!(used, understood, "{handler}: every use of params is one the scan reads");
    // The body: parsed into a struct, never read any other way.
    for (i, _) in inner.match_indices(" = match ") {
        let line_start = inner[..i].rfind('\n').unwrap_or(0);
        let decl = &inner[line_start..i];
        if let Some((_, ty)) = decl.split_once(": ") {
            let rest = &inner[i..i + 60.min(inner.len() - i)];
            if rest.contains("from_slice(body)") || rest.contains("parse_body(body)") {
                keys.extend(struct_keys(src, ty.trim()));
            }
        }
    }
    let body_uses = inner
        .match_indices("body")
        .filter(|(i, _)| {
            let before = inner[..*i].chars().last().unwrap_or(' ');
            let after = &inner[i + 4..];
            !(before.is_alphanumeric() || before == '_' || after.starts_with(|c: char| c.is_alphanumeric() || c == '_') || after.starts_with(':'))
        })
        .count();
    let parsed = inner.matches("from_slice(body)").count() + inner.matches("parse_body(body)").count();
    assert_eq!(body_uses, parsed, "{handler}: the body is only parsed into its struct");
    keys
}

/// The match arm the text at `at` is in: its line and the lines under it
/// (indented deeper, or a `||`/`&&` of its pattern).
fn match_arm(src: &str, at: usize) -> String {
    let start = src[..at].rfind('\n').map_or(0, |i| i + 1);
    let indent = |l: &str| l.len() - l.trim_start().len();
    let mut lines = src[start..].lines();
    let first = lines.next().unwrap_or("");
    let base = indent(first);
    let mut arm = vec![first];
    for l in lines {
        let t = l.trim_start();
        if !t.is_empty() && indent(l) <= base && !t.starts_with("||") && !t.starts_with("&&") {
            break;
        }
        arm.push(l);
    }
    arm.join("\n")
}

/// R6: the handler each relayed route runs on a host reads only the keys
/// its schema declares (a source scan: a new key a handler reads fails
/// here until the schema declares and confines it). Each handler is the
/// one the daemon's dispatch runs for its route.
#[test]
fn r6_a_relayed_handler_reads_only_its_schemas_keys() {
    let fs = include_str!("../fs_routes.rs");
    let git = include_str!("../git_routes.rs");
    let dispatch = [include_str!("../routes/dispatcher.rs"), include_str!("../misc_routes.rs"), git];
    let handlers: &[(&str, &str, &str)] = &[
        ("/cli/fs/info", "fs", "handle_info"),
        ("/cli/fs/read-dir", "fs", "handle_read_dir"),
        ("/cli/fs/read-file", "fs", "handle_read_file"),
        ("/cli/fs/read-binary", "fs", "handle_read_binary"),
        ("/cli/fs/read-range", "fs", "handle_read_range"),
        ("/cli/fs/stat", "fs", "handle_stat"),
        ("/cli/fs/zip-list", "fs", "handle_zip_list"),
        ("/cli/fs/create", "fs", "handle_create"),
        ("/cli/fs/delete", "fs", "handle_delete"),
        ("/cli/fs/move", "fs", "handle_move"),
        ("/cli/fs/rename", "fs", "handle_rename"),
        ("/cli/fs/copy", "fs", "handle_copy"),
        ("/cli/fs/duplicate", "fs", "handle_duplicate"),
        ("/cli/fs/write-file", "fs", "handle_write_file"),
        ("/cli/fs/upload-binary", "fs", "handle_upload_binary"),
        ("/cli/fs/upload-chunk", "fs", "handle_upload_chunk"),
        ("/cli/fs/search-tree", "fs", "handle_search_tree"),
        ("/cli/fs/resolve-links", "fs", "handle_resolve_links"),
        ("/cli/git/branches", "git", "handle_git_branches"),
        ("/cli/git/changes", "git", "handle_git_changes"),
        ("/cli/git/diff-between", "git", "handle_git_diff_between_branches"),
        ("/cli/git/diff-file", "git", "handle_git_diff_file"),
        ("/cli/git/diff-summary", "git", "handle_git_diff_summary"),
        ("/cli/git/file-at-ref", "git", "handle_git_file_at_ref"),
        ("/cli/git/info", "git", "handle_git_info"),
        ("/cli/git/merge-status", "git", "handle_git_merge_status"),
        ("/cli/git/snapshot", "git", "handle_git_snapshot"),
        ("/cli/git/worktrees", "git", "handle_git_worktrees"),
        ("/cli/git/abort-merge", "git", "handle_git_abort_merge"),
        ("/cli/git/commit", "git", "handle_git_commit"),
        ("/cli/git/create-worktree", "git", "handle_git_create_worktree"),
        ("/cli/git/delete-branch", "git", "handle_git_delete_branch"),
        ("/cli/git/discard", "git", "handle_git_discard"),
        ("/cli/git/init", "git", "handle_git_init"),
        ("/cli/git/merge-branch", "git", "handle_git_merge_branch"),
        ("/cli/git/prune-worktrees", "git", "handle_git_prune_worktrees"),
        ("/cli/git/remove-worktree", "git", "handle_git_remove_worktree"),
        ("/cli/git/reopen-worktree", "git", "handle_git_reopen_worktree"),
        ("/cli/git/resolve", "git", "handle_git_resolve_conflict"),
        ("/cli/git/stage", "git", "handle_git_stage"),
        ("/cli/git/stage-all", "git", "handle_git_stage_all"),
        ("/cli/git/unstage", "git", "handle_git_unstage"),
    ];
    for s in schema::SCHEMAS {
        assert!(handlers.iter().any(|(r, _, _)| *r == s.route), "{}: its handler is scanned", s.route);
    }
    // The scan reads what the handlers read (it isn't vacuous): query keys,
    // a helper's, a struct's (camelCase), a defaulted field.
    let has = |src: &str, h: &str, keys: &[&str]| {
        let got = handler_keys(src, h);
        assert!(keys.iter().all(|k| got.iter().any(|g| g == k)), "{h}: {got:?} has {keys:?}");
    };
    has(fs, "handle_read_range", &["path", "offset", "len"]);
    has(git, "handle_git_diff_file", &["path", "file_path", "sides", "mode", "staged", "base", "old_path"]);
    has(git, "handle_git_create_worktree", &["projectPath", "branch", "projectId", "existingBranch"]);
    has(git, "handle_git_discard", &["path", "files", "includeStaged"]);
    has(fs, "handle_upload_chunk", &["upload_id", "dir", "filename", "offset", "base64", "is_last", "total_bytes"]);
    for (route, file, handler) in handlers {
        let s = schema::SCHEMAS.iter().find(|s| s.route == *route).unwrap_or_else(|| panic!("{route} has a schema"));
        let src = if *file == "fs" { fs } else { git };
        // The daemon dispatches the route to this handler (or to its
        // `_gated` form, whose owner branch is this handler).
        let gated = format!("{handler}_gated(");
        let dispatched = dispatch.iter().any(|d| {
            d.match_indices(&format!("\"{route}\"")).any(|(i, _)| {
                let arm = match_arm(d, i);
                arm.contains(&format!("{handler}(")) || arm.contains(&gated)
            })
        });
        assert!(dispatched, "{route} is dispatched to {handler}");
        if dispatch.iter().any(|d| d.contains(&gated)) {
            assert!(fn_body(src, &format!("{handler}_gated")).contains(&format!("None => {handler}(")), "{handler}_gated runs {handler} for the owner");
        }
        for k in handler_keys(src, handler) {
            assert!(s.kind(&k).is_some(), "{route}: {handler} reads {k}, which its schema doesn't declare");
        }
    }
}

/// R6: the host refuses a key the route's schema doesn't declare, or one
/// given twice; so does the controller, before relaying.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r6_an_undeclared_or_repeated_key_is_refused() {
    let d = host_disk();
    std::fs::write(d.ws.join("a.md"), "a").unwrap();
    let undeclared = |r: (String, Value)| r.0 == "403 Forbidden" && r.1["code"] == "undeclared_key";
    let p = encode(&d.ws.join("a.md").to_string_lossy());
    assert!(undeclared(served("GET", "/cli/fs/read-file", &format!("path={p}&dir={}", encode(&d.ws.to_string_lossy())), None).await));
    assert!(undeclared(served("GET", "/cli/fs/read-file", &format!("path={p}&path={p}"), None).await), "twice");
    let w = json!({"path": d.ws.join("a.md"), "content": "x", "mode": "0777"});
    assert!(undeclared(served("POST", "/cli/fs/write-file", "", Some(w)).await));
    assert!(undeclared(served("POST", "/cli/git/stage", "", Some(json!(["not", "an", "object"]))).await));
    let (remote, url) = controller_at(&["/ctl/work"], &[("h1", &h("/h1/work"))]).await;
    let mut h1 = host(&url, "h1", &["k2.work_root=/h1/work"]).await;
    place(remote, &mut h1, "notes", "/ctl/work/notes").await;
    let code = |d: Decision| match d {
        Decision::Refuse(a) => serde_json::from_str::<Value>(&a.body).unwrap()["code"].clone(),
        other => panic!("expected a refusal, got {other:?}"),
    };
    assert_eq!(code(decide(remote, "/cli/fs/read-file", "path=%2Fctl%2Fwork%2Fnotes%2Fa.md&encoding=latin1", b"")), json!("undeclared_key"));
    let w = json!({"path": "/ctl/work/notes/a.md", "content": "x", "mode": "0777"}).to_string();
    assert_eq!(code(decide(remote, "/cli/fs/write-file", "", w.as_bytes())), json!("undeclared_key"));
    let ok = json!({"path": "/ctl/work/notes/a.md", "content": "x"}).to_string();
    assert!(matches!(decide(remote, "/cli/fs/write-file", "", ok.as_bytes()), Decision::Relay { .. }));
    drop(h1);
}

fn link(target: &Path, at: &Path) {
    std::os::unix::fs::symlink(target, at).unwrap();
}

fn route_schema(method: &str, route: &str) -> &'static schema::Schema {
    schema::schema(method, route).unwrap()
}

/// R6: the daemon gets the confined values: a folder link resolved (in the
/// request's own spelling of the work root), the last name as given (a
/// delete removes the link, not its target), nothing undeclared.
#[test]
fn r6_the_daemon_gets_the_confined_values() {
    let d = host_disk();
    std::fs::create_dir_all(d.ws.join("real")).unwrap();
    std::fs::write(d.ws.join("real/a.md"), "a").unwrap();
    link(&d.ws.join("real"), &d.ws.join("alias"));
    link(&d.ws.join("real/a.md"), &d.ws.join("a-link"));
    let spelled = d.root.parent().unwrap().join("root-alias");
    link(&d.root, &spelled);
    let q = |v: &Path| vec![("path".to_string(), v.to_string_lossy().into_owned())];
    let read = route_schema("GET", "/cli/fs/read-file");
    let c = schema::confine(&d.root, read, &q(&d.ws.join("alias/a.md")), &Value::Null).unwrap();
    assert_eq!(c.query, q(&d.ws.join("real/a.md")));
    let c = schema::confine(&d.root, read, &q(&spelled.join("notes/alias/a.md")), &Value::Null).unwrap();
    assert_eq!(c.query, q(&spelled.join("notes/real/a.md")), "the request's spelling of the root");
    let del = route_schema("POST", "/cli/fs/delete");
    let body = json!({"paths": [d.ws.join("a-link"), d.ws.join("alias/a.md")], "permanent": false});
    let c = schema::confine(&d.root, del, &[], &body).unwrap();
    assert_eq!(c.body, json!({"paths": [d.ws.join("a-link"), d.ws.join("real/a.md")], "permanent": false}));
    assert!(c.query.is_empty());
}

/// R6 (R6-1): create-worktree's derived paths: a symlinked `.worktrees`,
/// branch folder or `.gitignore` is refused on the host (the schema) and by
/// K2's git on a host; nothing is written through it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r6_create_worktree_refuses_a_symlinked_worktrees_dir_branch_folder_or_gitignore() {
    let d = host_disk();
    k2_core::git::init_repo(&d.ws.to_string_lossy()).unwrap();
    std::fs::write(d.ws.join("a.md"), "a").unwrap();
    host_git(&d.ws, &["add", "-A"]);
    host_git(&d.ws, &["commit", "-qm", "first"]);
    let outside = d.root.parent().unwrap().join("outside-wt");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("bashrc"), "mine\n").unwrap();
    let wt = route_schema("POST", "/cli/git/create-worktree");
    let body = |b: &str| json!({"projectPath": d.ws, "branch": b, "projectId": "p"});
    let refused = |b: &str| matches!(schema::confine(&d.root, wt, &[], &body(b)), Err(schema::Refusal::Outside(_)));
    let made = |b: &str| {
        let _hardened = Hardened::on();
        k2_core::git::create_worktree(&d.ws.to_string_lossy(), b)
    };
    // `.worktrees` a link out.
    link(&outside, &d.ws.join(".worktrees"));
    assert!(refused("wt-a"));
    assert!(made("wt-a").is_err());
    assert!(!outside.join("wt-a").exists(), "no worktree out there");
    std::fs::remove_file(d.ws.join(".worktrees")).unwrap();
    // The branch's folder a link out (to an empty folder git would fill).
    std::fs::create_dir_all(d.ws.join(".worktrees")).unwrap();
    let empty = outside.join("empty");
    std::fs::create_dir_all(&empty).unwrap();
    link(&empty, &d.ws.join(".worktrees/wt-b"));
    assert!(refused("wt-b"));
    assert!(made("wt-b").is_err());
    assert_eq!(std::fs::read_dir(&empty).unwrap().count(), 0, "nothing checked out there");
    // `.gitignore` a link out.
    link(&outside.join("bashrc"), &d.ws.join(".gitignore"));
    assert!(refused("wt-c"));
    assert!(made("wt-c").is_err());
    assert_eq!(std::fs::read_to_string(outside.join("bashrc")).unwrap(), "mine\n", "not appended to");
    std::fs::remove_file(d.ws.join(".gitignore")).unwrap();
    // A branch whose folder isn't one name.
    for b in ["..", "-x", "a/../.."] {
        assert!(schema::confine(&d.root, wt, &[], &body(b)).is_err(), "{b}");
    }
    // The control: plain, it goes through and creates the worktree.
    assert!(schema::confine(&d.root, wt, &[], &body("wt-d")).is_ok());
    assert!(made("wt-d").is_ok());
}

/// R6: K2's git on a host refuses a repository whose config names a hook
/// (`hook.<name>.*`, config-defined hooks a newer git runs whatever
/// `core.hooksPath` says); unhardened (a local daemon) it carries on.
#[test]
fn r6_k2s_git_refuses_a_repository_whose_config_names_a_hook() {
    let dir = k2_core::test_env::unique_temp_path("relay-cfg-hook");
    std::fs::create_dir_all(&dir).unwrap();
    let repo = dir.to_string_lossy().into_owned();
    k2_core::git::init_repo(&repo).unwrap();
    std::fs::write(dir.join("a.md"), "a").unwrap();
    host_git(&dir, &["add", "-A"]);
    host_git(&dir, &["commit", "-qm", "first"]);
    host_git(&dir, &["config", "hook.evil.command", "touch /nonexistent/never"]);
    host_git(&dir, &["config", "hook.evil.event", "post-checkout"]);
    {
        let _hardened = Hardened::on();
        let made = k2_core::git::create_worktree(&repo, "cfg-hook");
        assert!(made.as_ref().is_err_and(|e| e.contains("hook.evil")), "{made:?}");
        assert!(k2_core::git::list_worktrees(&repo).is_empty());
        assert!(k2_core::git::git_command_in(&dir).is_err());
    }
    let _serial = Hardened::on();
    k2_core::git::set_no_hooks(false);
    assert!(k2_core::git::create_worktree(&repo, "cfg-hook").is_ok(), "unhardened, as before");
    drop(_serial);
    let _ = std::fs::remove_dir_all(&dir);
}

/// R6: a repo-relative `file_path` joins the repository's work tree, as
/// `git::stage_file` and `diff_file` join it, not the request's `path` (a
/// sub-folder of the repository).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r6_a_repo_relative_path_joins_the_work_tree_the_handler_joins() {
    let d = host_disk();
    k2_core::git::init_repo(&d.ws.to_string_lossy()).unwrap();
    std::fs::create_dir_all(d.ws.join("sub")).unwrap();
    let outside = d.root.parent().unwrap().join("outside-rel");
    std::fs::create_dir_all(&outside).unwrap();
    link(&outside, &d.ws.join("escape"));
    let refused = |r: (String, Value)| r.0 == "403 Forbidden" && r.1["code"] == "outside_work_root";
    let sub = d.ws.join("sub");
    let stage = json!({"path": sub, "filePath": "escape/id_ed25519"});
    assert!(refused(served("POST", "/cli/git/stage", "", Some(stage)).await), "ws/escape, not ws/sub/escape");
    let q = format!("path={}&file_path=escape%2Fid_ed25519&sides=1", encode(&sub.to_string_lossy()));
    assert!(refused(served("GET", "/cli/git/diff-file", &q, None).await));
    // The same name under the folder that is in the repository passes.
    let ok = json!({"path": sub, "filePath": "sub/a.md"});
    assert!(schema::confine(&d.root, route_schema("POST", "/cli/git/stage"), &[], &ok).is_ok());
}

/// R6: an upload's derived files (its collision-free name, a chunk's
/// `.part`) and a copy's tree (a link inside a copied folder) are confined.
#[test]
fn r6_an_uploads_files_and_a_copys_tree_are_confined() {
    let d = host_disk();
    let outside = d.root.parent().unwrap().join("outside-up");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("secret"), "s").unwrap();
    let outside_file = |r: Result<schema::Confined, schema::Refusal>| matches!(r, Err(schema::Refusal::Outside(_)));
    // `a.txt` taken, `a (1).txt` a dangling link out: the upload would land there.
    std::fs::write(d.ws.join("a.txt"), "a").unwrap();
    link(&outside.join("new"), &d.ws.join("a (1).txt"));
    let up = json!({"dir": d.ws, "filename": "a.txt", "base64": ""});
    assert!(outside_file(schema::confine(&d.root, route_schema("POST", "/cli/fs/upload-binary"), &[], &up)));
    let fresh = json!({"dir": d.ws, "filename": "b.txt", "base64": ""});
    assert!(schema::confine(&d.root, route_schema("POST", "/cli/fs/upload-binary"), &[], &fresh).is_ok());
    // A chunk's `.part` a link out.
    link(&outside.join("part"), &d.ws.join(".k2-upload-x1.part"));
    let chunk = json!({"upload_id": "x1", "dir": d.ws, "filename": "c.bin", "offset": 0, "base64": "", "is_last": false});
    assert!(outside_file(schema::confine(&d.root, route_schema("POST", "/cli/fs/upload-chunk"), &[], &chunk)));
    // A filename that isn't one name.
    let bad = json!({"dir": d.ws, "filename": "../x", "base64": ""});
    assert!(schema::confine(&d.root, route_schema("POST", "/cli/fs/upload-binary"), &[], &bad).is_err());
    // A folder holding a link out: copying or duplicating it copies the secret.
    std::fs::create_dir_all(d.ws.join("folder/deep")).unwrap();
    link(&outside.join("secret"), &d.ws.join("folder/deep/s"));
    std::fs::create_dir_all(d.ws.join("dest")).unwrap();
    let copy = json!({"sources": [d.ws.join("folder")], "destination": d.ws.join("dest")});
    assert!(outside_file(schema::confine(&d.root, route_schema("POST", "/cli/fs/copy"), &[], &copy)));
    assert!(outside_file(schema::confine(&d.root, route_schema("POST", "/cli/fs/duplicate"), &[], &json!({"path": d.ws.join("folder")}))));
    std::fs::remove_file(d.ws.join("folder/deep/s")).unwrap();
    assert!(schema::confine(&d.root, route_schema("POST", "/cli/fs/copy"), &[], &copy).is_ok(), "the control");
}

// ── round 7 ────────────────────────────────────────────────────────────

/// A host's workspace `ws` as a repository with one commit (`sub/f`).
fn committed_repo(ws: &Path) {
    k2_core::git::init_repo(&ws.to_string_lossy()).unwrap();
    std::fs::create_dir_all(ws.join("sub")).unwrap();
    std::fs::write(ws.join("sub/f"), "tracked\n").unwrap();
    host_git(ws, &["add", "-A"]);
    host_git(ws, &["commit", "-qm", "first"]);
}

/// R7 F1(a): a symlink anywhere in the git dir (logs, refs, worktrees) or
/// alternates outside the work root: every git route on that repository is
/// refused before libgit2 or git writes through it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r7_f1_a_link_in_the_git_dir_is_refused() {
    let d = host_disk();
    committed_repo(&d.ws);
    let outside = d.root.parent().unwrap().join("outside-git");
    std::fs::create_dir_all(&outside).unwrap();
    let refused = |r: (String, Value)| r.0 == "403 Forbidden" && r.1["code"] == "outside_work_root";
    let git = d.ws.join(".git");
    let swap = |name: &str| {
        let real = git.join(name);
        let kept = git.join(format!("{name}.kept"));
        std::fs::rename(&real, &kept).unwrap();
        link(&outside, &real);
        move || {
            std::fs::remove_file(&real).unwrap();
            std::fs::rename(&kept, &real).unwrap();
        }
    };
    for name in ["logs", "refs/heads"] {
        let back = swap(name);
        let commit = json!({"path": d.ws, "message": "secret message"});
        assert!(refused(served("POST", "/cli/git/commit", "", Some(commit)).await), "{name}");
        assert!(refused(served("GET", "/cli/git/snapshot", &format!("path={}", encode(&d.ws.to_string_lossy())), None).await), "{name}: reads too");
        back();
    }
    std::fs::create_dir_all(git.join("worktrees")).unwrap();
    let back = swap("worktrees");
    let wt = json!({"projectPath": d.ws, "branch": "evil", "projectId": "p"});
    assert!(refused(served("POST", "/cli/git/create-worktree", "", Some(wt)).await));
    back();
    assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 0, "nothing written outside");
    // Objects borrowed from outside the work root.
    std::fs::create_dir_all(git.join("objects/info")).unwrap();
    std::fs::write(git.join("objects/info/alternates"), format!("{}\n", outside.display())).unwrap();
    let commit = json!({"path": d.ws, "message": "m"});
    assert!(refused(served("POST", "/cli/git/commit", "", Some(commit.clone())).await), "alternates");
    std::fs::remove_file(git.join("objects/info/alternates")).unwrap();
    // The control: the same repository, no link, passes the host's checks.
    assert!(schema::confine(&d.root, route_schema("POST", "/cli/git/commit"), &[], &commit).is_ok());
}

/// R7 F1(b): a forced checkout (abort-merge, a fast-forward merge,
/// discard, resolve) through a tracked folder swapped for a link to a
/// folder outside the work root is refused; nothing outside is deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r7_f1_a_forced_checkout_through_a_folder_link_is_refused() {
    let d = host_disk();
    committed_repo(&d.ws);
    let outside = d.root.parent().unwrap().join("outside-co");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("f"), "theirs\n").unwrap();
    std::fs::remove_dir_all(d.ws.join("sub")).unwrap();
    link(&outside, &d.ws.join("sub"));
    // Round 8: refused as a link leaving the work tree (discard's `sub/f`
    // first, as a repo-relative path outside the work root).
    let refused = |r: (String, Value)| r.0 == "403 Forbidden" && (r.1["code"] == "link_outside_worktree" || r.1["code"] == "outside_work_root");
    let p = json!(d.ws);
    for (route, body) in [
        ("/cli/git/abort-merge", json!({"path": p})),
        ("/cli/git/merge-branch", json!({"path": p, "branch": "main"})),
        ("/cli/git/discard", json!({"path": p, "files": ["sub/f"]})),
        ("/cli/git/resolve", json!({"path": p, "filePath": "a.md", "resolution": "ours"})),
    ] {
        assert!(refused(served("POST", route, "", Some(body)).await), "{route}");
    }
    // From a sub-folder of the repository too: the whole work tree counts.
    std::fs::create_dir_all(d.ws.join("deeper")).unwrap();
    assert!(refused(served("POST", "/cli/git/abort-merge", "", Some(json!({"path": d.ws.join("deeper")}))).await));
    assert_eq!(std::fs::read_to_string(outside.join("f")).unwrap(), "theirs\n", "not deleted");
    // A dangling folder link out: refused too.
    std::fs::remove_file(d.ws.join("sub")).unwrap();
    link(&outside.join("gone"), &d.ws.join("sub"));
    assert!(refused(served("POST", "/cli/git/abort-merge", "", Some(json!({"path": p}))).await), "dangling");
    // The controls: a link to a folder inside the work root, and one to a
    // file outside (the checkout replaces the link), pass.
    std::fs::remove_file(d.ws.join("sub")).unwrap();
    std::fs::create_dir_all(d.ws.join("real-sub")).unwrap();
    link(&d.ws.join("real-sub"), &d.ws.join("sub"));
    link(&outside.join("f"), &d.ws.join("file-link"));
    assert!(schema::confine(&d.root, route_schema("POST", "/cli/git/abort-merge"), &[], &json!({"path": p})).is_ok());
}

// ── round 8 ────────────────────────────────────────────────────────────

/// R8 B1: a folder named `.git` that isn't the repository's own git dir is
/// walked: an untracked `hide/.git/deep` linking out, reached through a
/// tracked `sub` linked to `hide/.git` (inside the root), is refused, and
/// nothing outside is deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r8_b1_only_the_repositorys_own_git_dir_is_skipped() {
    let d = host_disk();
    k2_core::git::init_repo(&d.ws.to_string_lossy()).unwrap();
    std::fs::create_dir_all(d.ws.join("sub/deep")).unwrap();
    std::fs::write(d.ws.join("sub/deep/f"), "tracked\n").unwrap();
    host_git(&d.ws, &["add", "-A"]);
    host_git(&d.ws, &["commit", "-qm", "first"]);
    let outside = d.root.parent().unwrap().join("outside-b1");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("f"), "theirs\n").unwrap();
    std::fs::create_dir_all(d.ws.join("hide/.git")).unwrap();
    link(&outside, &d.ws.join("hide/.git/deep"));
    std::fs::remove_dir_all(d.ws.join("sub")).unwrap();
    link(&d.ws.join("hide/.git"), &d.ws.join("sub"));
    let (st, v) = served("POST", "/cli/git/abort-merge", "", Some(json!({"path": d.ws}))).await;
    assert_eq!((st.as_str(), &v["code"]), ("403 Forbidden", &json!("link_outside_worktree")), "{v}");
    assert_eq!(std::fs::read_to_string(outside.join("f")).unwrap(), "theirs\n", "not deleted");
}

/// R8 B2: a folder link inside the work root whose target is outside the
/// work tree is refused, not followed: `sub -> ws/elsewhere`, with
/// `elsewhere/deep` linking out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r8_b2_a_folder_link_leaving_the_work_tree_is_refused() {
    let d = host_disk();
    let repo = d.ws.join("repo");
    std::fs::create_dir_all(repo.join("sub/deep")).unwrap();
    k2_core::git::init_repo(&repo.to_string_lossy()).unwrap();
    std::fs::write(repo.join("sub/deep/f"), "tracked\n").unwrap();
    host_git(&repo, &["add", "-A"]);
    host_git(&repo, &["commit", "-qm", "first"]);
    let outside = d.root.parent().unwrap().join("outside-b2");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("f"), "theirs\n").unwrap();
    std::fs::create_dir_all(d.ws.join("elsewhere")).unwrap();
    link(&outside, &d.ws.join("elsewhere/deep"));
    std::fs::remove_dir_all(repo.join("sub")).unwrap();
    link(&d.ws.join("elsewhere"), &repo.join("sub"));
    for (route, body) in [
        ("/cli/git/abort-merge", json!({"path": repo})),
        ("/cli/git/merge-branch", json!({"path": repo, "branch": "main"})),
    ] {
        let (st, v) = served("POST", route, "", Some(body)).await;
        assert_eq!((st.as_str(), &v["code"]), ("403 Forbidden", &json!("link_outside_worktree")), "{route}: {v}");
    }
    assert_eq!(std::fs::read_to_string(outside.join("f")).unwrap(), "theirs\n", "not deleted");
    // The control: a folder link that stays in the work tree passes.
    std::fs::remove_file(repo.join("sub")).unwrap();
    std::fs::create_dir_all(repo.join("real-sub")).unwrap();
    link(&repo.join("real-sub"), &repo.join("sub"));
    assert!(schema::confine(&d.root, route_schema("POST", "/cli/git/abort-merge"), &[], &json!({"path": repo})).is_ok());
}

/// R8 C: the checks run within a walk budget: many links, each a long
/// `a/../` chain, are refused with `walk_budget` past it (a small budget
/// here); within the default budget the same tree passes.
#[test]
fn r8_c_the_checks_stop_at_their_walk_budget() {
    let d = host_disk();
    let repo = d.ws.join("repo");
    std::fs::create_dir_all(repo.join("a")).unwrap();
    k2_core::git::init_repo(&repo.to_string_lossy()).unwrap();
    let chain = "a/../".repeat(40);
    for i in 0..50 {
        link(Path::new(&format!("{chain}a")), &repo.join(format!("l{i}")));
    }
    let abort = route_schema("POST", "/cli/git/abort-merge");
    let body = json!({"path": repo});
    let r = schema::confine_within(&d.root, abort, &[], &body, 1_000, Duration::from_secs(60));
    assert!(matches!(r, Err(schema::Refusal::Coded("walk_budget", _))), "{r:?}");
    assert!(schema::confine(&d.root, abort, &[], &body).is_ok(), "within the default budget");
    // The budget is per request: the next check starts afresh.
    assert!(schema::confine(&d.root, route_schema("GET", "/cli/fs/read-file"), &[("path".into(), repo.join("a").to_string_lossy().into())], &Value::Null).is_ok());
}

// ── round 9 ────────────────────────────────────────────────────────────

fn mode(p: &Path, m: u32) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(p, std::fs::Permissions::from_mode(m)).unwrap();
}

/// R9 X1 (the repro): `.git/objects/ab/deep` linking out, reached through
/// a tracked `sub` linked to `.git/objects/ab`: refused, nothing outside
/// deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r9_x1_a_checkout_through_the_object_store_is_refused() {
    let d = host_disk();
    k2_core::git::init_repo(&d.ws.to_string_lossy()).unwrap();
    std::fs::create_dir_all(d.ws.join("sub/deep")).unwrap();
    std::fs::write(d.ws.join("sub/deep/f"), "tracked\n").unwrap();
    host_git(&d.ws, &["add", "-A"]);
    host_git(&d.ws, &["commit", "-qm", "first"]);
    let outside = d.root.parent().unwrap().join("outside-x1");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("f"), "theirs\n").unwrap();
    std::fs::create_dir_all(d.ws.join(".git/objects/ab")).unwrap();
    link(&outside, &d.ws.join(".git/objects/ab/deep"));
    std::fs::remove_dir_all(d.ws.join("sub")).unwrap();
    link(&d.ws.join(".git/objects/ab"), &d.ws.join("sub"));
    let (st, v) = served("POST", "/cli/git/abort-merge", "", Some(json!({"path": d.ws}))).await;
    assert_eq!(st, "403 Forbidden", "{v}");
    assert_eq!(std::fs::read_to_string(outside.join("f")).unwrap(), "theirs\n", "not deleted");
}

/// R9 X1, each half on its own: (a) a work-tree link into the git dir is
/// refused even when the git dir itself is clean; (b) the git dir's
/// fan-out folders and `pack` are listed: a link or a folder in one is
/// refused, for every git route.
#[test]
fn r9_x1_links_into_the_git_dir_and_into_its_object_folders() {
    let d = host_disk();
    committed_repo(&d.ws);
    let abort = route_schema("POST", "/cli/git/abort-merge");
    let body = json!({"path": d.ws});
    link(&d.ws.join(".git/refs"), &d.ws.join("refs-link"));
    let r = schema::confine(&d.root, abort, &[], &body);
    assert!(matches!(r, Err(schema::Refusal::Coded("link_into_git_dir", _))), "{r:?}");
    std::fs::remove_file(d.ws.join("refs-link")).unwrap();
    assert!(schema::confine(&d.root, abort, &[], &body).is_ok(), "the control");
    let outside = d.root.parent().unwrap().join("outside-x1b");
    std::fs::create_dir_all(&outside).unwrap();
    let snap = route_schema("GET", "/cli/git/snapshot");
    let q = vec![("path".to_string(), d.ws.to_string_lossy().into_owned())];
    for (at, what) in [("objects/cd", "a link"), ("objects/pack", "a link")] {
        std::fs::create_dir_all(d.ws.join(".git").join(at)).unwrap();
        let l = d.ws.join(".git").join(at).join("x");
        link(&outside, &l);
        assert!(matches!(schema::confine(&d.root, snap, &q, &Value::Null), Err(schema::Refusal::Outside(_))), "{at}: {what}");
        std::fs::remove_file(&l).unwrap();
        std::fs::create_dir_all(&l).unwrap();
        assert!(matches!(schema::confine(&d.root, snap, &q, &Value::Null), Err(schema::Refusal::Outside(_))), "{at}: a folder");
        std::fs::remove_dir(&l).unwrap();
    }
    assert!(schema::confine(&d.root, snap, &q, &Value::Null).is_ok(), "the control");
}

/// R9 X2: a folder a check can't list (mode 0300) fails it, closed, in
/// all three walks: the checkout's work tree, the git dir, a copy's
/// source.
#[test]
fn r9_x2_a_folder_the_check_cant_list_fails_it() {
    let d = host_disk();
    committed_repo(&d.ws);
    let hidden = d.ws.join("hidden");
    std::fs::create_dir_all(&hidden).unwrap();
    mode(&hidden, 0o300);
    let abort = route_schema("POST", "/cli/git/abort-merge");
    let r = schema::confine(&d.root, abort, &[], &json!({"path": d.ws}));
    mode(&hidden, 0o755);
    assert!(r.is_err(), "the work tree: {r:?}");
    let refs = d.ws.join(".git/refs/tags");
    std::fs::create_dir_all(&refs).unwrap();
    mode(&refs, 0o300);
    let q = vec![("path".to_string(), d.ws.to_string_lossy().into_owned())];
    let r = schema::confine(&d.root, route_schema("GET", "/cli/git/snapshot"), &q, &Value::Null);
    mode(&refs, 0o755);
    assert!(r.is_err(), "the git dir: {r:?}");
    let src = d.ws.join("src-folder");
    std::fs::create_dir_all(src.join("inner")).unwrap();
    mode(&src.join("inner"), 0o300);
    let copy = json!({"sources": [src], "destination": d.ws.join("hidden")});
    let r = schema::confine(&d.root, route_schema("POST", "/cli/fs/copy"), &[], &copy);
    mode(&src.join("inner"), 0o755);
    assert!(r.is_err(), "a copy's source: {r:?}");
    assert!(schema::confine(&d.root, route_schema("POST", "/cli/fs/copy"), &[], &copy).is_ok(), "the control");
}

/// R9 X3: resolve-links' candidates are checked within the request's one
/// budget (a small one refuses), and past a cap they are answered
/// unchecked.
#[test]
fn r9_x3_resolve_links_candidates_are_in_the_budget_and_capped() {
    let d = host_disk();
    std::fs::create_dir_all(d.ws.join("a")).unwrap();
    // Each candidate a link whose target is a long `a/../` chain.
    link(Path::new(&format!("{}a", "a/../".repeat(40))), &d.ws.join("lk"));
    let links = route_schema("POST", "/cli/fs/resolve-links");
    let body = |n: usize| json!({"workspace": d.ws, "paths": vec!["lk"; n]});
    let r = check_request(&d.root, links, &[], body(100), 2_000, Duration::from_secs(60));
    assert!(matches!(r, Err(schema::Refusal::Coded("walk_budget", _))), "a small budget: {:?}", r.err());
    let (_, refused) = check_request(&d.root, links, &[], body(100), schema::WALK_STEPS, schema::WALK_TIME).unwrap();
    assert!(refused.is_empty(), "within the default budget, all checked");
    let (c, refused) = check_request(&d.root, links, &[], body(2_000), schema::WALK_STEPS, schema::WALK_TIME).unwrap();
    assert_eq!(refused.len(), 1_500, "past the cap: answered unchecked");
    assert_eq!(c.body["paths"].as_array().unwrap().len(), 500);
}

// ── F2–F6 (round 7's lows) ─────────────────────────────────────────────

/// The host's Trash folder, for one test at a time (it is process-wide).
struct HostTrash(#[allow(dead_code)] std::sync::MutexGuard<'static, ()>);

impl HostTrash {
    fn at(dir: &Path) -> HostTrash {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let g = LOCK.lock().unwrap_or_else(|p| p.into_inner());
        k2_core::safe_delete::set_host_trash(Some(dir.to_path_buf()));
        HostTrash(g)
    }
}

impl Drop for HostTrash {
    fn drop(&mut self) {
        k2_core::safe_delete::set_host_trash(None);
    }
}

/// F3: on a host, what a delete (not `permanent`) or a worktree removal
/// sends to the Trash lands in the work root's `.k2-trash`, never the
/// host's own Trash outside the root.
#[test]
fn f3_the_trash_stays_inside_the_work_root() {
    let d = host_disk();
    let trash = d.root.join(HOST_TRASH);
    std::fs::write(d.ws.join("gone.md"), "x").unwrap();
    std::fs::create_dir_all(d.ws.join("wt")).unwrap();
    {
        let _t = HostTrash::at(&trash);
        k2_core::fs_commands::delete(&[d.ws.join("gone.md").to_string_lossy().into_owned()], false).unwrap();
        k2_core::safe_delete::trash(d.ws.join("wt")).unwrap();
    }
    assert!(!d.ws.join("gone.md").exists() && !d.ws.join("wt").exists());
    let names: Vec<String> = std::fs::read_dir(&trash).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).collect();
    assert!(names.iter().any(|n| n.starts_with("gone.md-")) && names.iter().any(|n| n.starts_with("wt-")), "{names:?}");
}
