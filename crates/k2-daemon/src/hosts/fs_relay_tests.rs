//! fs-relay: the mapping, the routing decision, and a request through the
//! controller's relay to a host's own daemon (a real one, in process).

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

#[test]
fn the_relay_carries_file_and_git_reads_and_writes_not_the_controllers_own_machine() {
    for (m, p) in [
        ("GET", "/cli/fs/read-dir"),
        ("GET", "/cli/fs/read-file"),
        ("GET", "/cli/fs/stat"),
        ("POST", "/cli/fs/write-file"),
        ("POST", "/cli/fs/rename"),
        ("GET", "/cli/git/changes"),
        ("GET", "/cli/git/diff-file"),
        ("POST", "/cli/git/commit"),
        ("POST", "/cli/git/notes/add"),
        ("GET", "/cli/worktree"),
    ] {
        assert!(relayed(m, p), "{m} {p}");
    }
    for (m, p) in [
        ("GET", "/cli/fs/events"),
        ("POST", "/cli/fs/open-finder"),
        ("POST", "/cli/fs/open-external"),
        ("GET", "/cli/fs/clipboard-paths"),
        ("POST", "/cli/fs/resolve-links"),
        ("POST", "/cli/fs/upload-binary"),
        ("POST", "/cli/fs/upload-chunk"),
        ("POST", "/cli/fs/compress"),
        ("GET", "/cli/fs/compress-status"),
        ("GET", "/cli/fs/write-file"),
        ("GET", "/cli/projects"),
    ] {
        assert!(!relayed(m, p), "{m} {p}");
    }
}

#[test]
fn paths_map_between_work_roots_by_whole_components() {
    let maps = vec![(pb("/ctl/work/notes"), pb("/h1/work/notes"))];
    assert_eq!(map_any("/ctl/work/notes", &maps), "/h1/work/notes");
    assert_eq!(map_any("/ctl/work/notes/src/a.rs", &maps), "/h1/work/notes/src/a.rs");
    assert_eq!(map_any("/ctl/work/notes-old/a", &maps), "/ctl/work/notes-old/a", "not a component prefix");
    assert_eq!(map_any("src/a.rs", &maps), "src/a.rs", "relative stays");
    // The query: values mapped (and re-encoded), the client's token dropped.
    let q = map_query("path=%2Fctl%2Fwork%2Fnotes%2Fa%20b.md&token=secret&show_hidden=true", &maps);
    assert_eq!(q, "path=%2Fh1%2Fwork%2Fnotes%2Fa+b.md&show_hidden=true");
    // JSON: every path string, at any depth.
    let mut v = json!({"path": "/h1/work/notes", "entries": [{"path": "/h1/work/notes/a"}], "n": 2});
    map_json(&mut v, &[(pb("/h1/work/notes"), pb("/ctl/work/notes"))]);
    assert_eq!(v, json!({"path": "/ctl/work/notes", "entries": [{"path": "/ctl/work/notes/a"}], "n": 2}));
    // The paths a request names: query values and body strings.
    let named = named_paths("path=%2Fa%2Fb&token=%2Fnot&x=1", br#"{"from": "/c/d", "to": ["/e"], "msg": "hi"}"#);
    assert_eq!(named, ["/a/b", "/c/d", "/e"]);
    assert_eq!(named_paths("", b"path=%2Fform%2Fp&x=1"), ["/form/p"]);
    assert_eq!(work_root_label(&["pool=a".into(), "k2.work_root=/h1/work".into()]), Some(pb("/h1/work")));
    assert_eq!(work_root_label(&["k2.max_awake=2".into()]), None);
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

/// Place `agent` (cwd `cwd`, as the controller asks) on the fake `host`.
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
    let ctl = Some(Path::new("/ctl/work"));
    let paths = |p: &[&str]| p.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    // A host on the shared volume (no label): everything stays local.
    let mut shared = host(&url, "shared", &[]).await;
    place(remote, &mut shared, "old", "/ctl/work/old").await;
    assert!(!remote.any_host_on_own_disk());
    assert_eq!(remote.workspace_route(&paths(&["/ctl/work/old/a"]), ctl), WorkspaceRoute::Local);
    // Hosts on their own disks.
    let mut h1 = host(&url, "h1", &["k2.work_root=/h1/work"]).await;
    let mut h2 = host(&url, "h2", &["k2.work_root=/h2/work"]).await;
    assert!(remote.any_host_on_own_disk());
    // notes asked for in the controller's namespace; tasks named as host 2 has it.
    place(remote, &mut h1, "notes", "/ctl/work/notes").await;
    place(remote, &mut h2, "tasks", "/h2/work/tasks").await;
    assert_eq!(
        remote.workspace_route(&paths(&["/ctl/work/notes/src/a.rs"]), ctl),
        WorkspaceRoute::Relay { host: "h1".into(), maps: vec![(pb("/ctl/work/notes"), pb("/h1/work/notes"))] }
    );
    assert_eq!(
        remote.workspace_route(&paths(&["/ctl/work/tasks"]), ctl),
        WorkspaceRoute::Relay { host: "h2".into(), maps: vec![(pb("/ctl/work/tasks"), pb("/h2/work/tasks"))] },
        "the controller's name of a host path"
    );
    assert_eq!(
        remote.workspace_route(&paths(&["/h2/work/tasks/x"]), ctl),
        WorkspaceRoute::Relay { host: "h2".into(), maps: vec![(pb("/h2/work/tasks"), pb("/h2/work/tasks"))] },
        "the host's own name"
    );
    // Without a controller work root, one name for both.
    assert_eq!(
        remote.workspace_route(&paths(&["/ctl/work/notes"]), None),
        WorkspaceRoute::Relay { host: "h1".into(), maps: vec![(pb("/ctl/work/notes"), pb("/ctl/work/notes"))] }
    );
    // A path no agent's workspace holds, or a sibling with a shared prefix: local.
    assert_eq!(remote.workspace_route(&paths(&["/ctl/work/other"]), ctl), WorkspaceRoute::Local);
    assert_eq!(remote.workspace_route(&paths(&["/ctl/work/notes-old"]), ctl), WorkspaceRoute::Local);
    assert_eq!(remote.workspace_route(&[], ctl), WorkspaceRoute::Local);
    // Two hosts in one request: refused.
    assert_eq!(
        remote.workspace_route(&paths(&["/ctl/work/notes/a", "/ctl/work/tasks/b"]), ctl),
        WorkspaceRoute::Split(vec!["h1".into(), "h2".into()])
    );
    // notes' host goes: its workspace is there, not here.
    drop(h1);
    wait_unlinked(remote, "h1").await;
    assert_eq!(
        remote.workspace_route(&paths(&["/ctl/work/notes/a"]), ctl),
        WorkspaceRoute::Away { host: "h1".into(), path: "/ctl/work/notes/a".into() }
    );
    drop((shared, h2));
}

/// The app's file browser and git panel through the controller: a
/// workspace on a host's own disk (another work root) is read, written and
/// committed there, with every path in the controller's namespace.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_file_browser_and_git_panel_through_the_controller_see_the_hosts_disk() {
    let home = crate::test_support::TempHome::new();
    let root = home.path().join("relay");
    std::fs::create_dir_all(&root).unwrap();
    // Canonical, as a host's `k2.work_root` label is (`home::work_root`).
    let root = std::fs::canonicalize(&root).unwrap();
    let (ctl_root, host_root) = (root.join("ctl-work"), root.join("h1-work"));
    let ws = host_root.join("notes");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::create_dir_all(ctl_root.join("notes")).unwrap(); // the controller's own (empty) copy
    std::fs::write(ws.join("on-the-host.md"), "written by the agent\n").unwrap();
    // The host's own daemon.
    let host_daemon = crate::test_harness::start("host-owner").await;
    set_local_daemon(host_daemon.port, "host-owner");
    // The controller, and the host on its own disk: it serves http.relay.
    let (remote, url) = controller().await;
    let mut h1 = host(&url, "h1", &[&format!("k2.work_root={}", host_root.display())]).await;
    let ctl_ws = ctl_root.join("notes");
    place(remote, &mut h1, "notes", &ctl_ws.to_string_lossy()).await;
    tokio::spawn(async move {
        while let Some(Ok(m)) = h1.next().await {
            if let Message::Text(t) = m {
                if let Ok(Frame::HttpRelay { req, method, path, query, body_b64 }) = Frame::decode(&t) {
                    let r = serve(req, &method, &path, &query, &body_b64).await;
                    let _ = h1.send(Message::Text(r.encode())).await;
                }
            }
        }
    });
    // The controller's routes: relayed, or "local" (a 404 here).
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let ctl_root2 = ctl_root.clone();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = l.accept().await {
            let ctl_root = ctl_root2.clone();
            tokio::spawn(async move {
                let mut head = [0u8; 4096];
                let n = s.peek(&mut head).await.unwrap();
                let line = String::from_utf8_lossy(&head[..n]).lines().next().unwrap_or("").to_string();
                let mut parts = line.split(' ');
                let method = parts.next().unwrap_or("").to_string();
                let target = parts.next().unwrap_or("").to_string();
                let (path, query) = target.split_once('?').unwrap_or((&target, ""));
                let mut buf = vec![0u8; 64 * 1024];
                if !route_on(remote, Some(ctl_root), &mut s, &mut buf, &method, path, query, "ctl-owner").await {
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
            (st, serde_json::from_str::<Value>(&r.text().await.unwrap_or_default()).unwrap_or(Value::Null))
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

    // The file browser: the host's file, its path in the controller's namespace.
    let (st, dir) = get(format!("{base}/cli/fs/read-dir?path={}&token=ctl-owner", q(&ctl_ws))).await;
    assert_eq!(st, 200, "{dir}");
    let names: Vec<&str> = dir.as_array().unwrap().iter().filter_map(|e| e["name"].as_str()).collect();
    assert_eq!(names, ["on-the-host.md"], "{dir}");
    assert_eq!(dir[0]["path"], json!(ctl_ws.join("on-the-host.md").to_string_lossy()), "paths come back in the controller's namespace");
    let (st, file) = get(format!("{base}/cli/fs/read-file?path={}&token=ctl-owner", q(&ctl_ws.join("on-the-host.md")))).await;
    assert_eq!(st, 200, "{file}");
    assert!(file.to_string().contains("written by the agent"), "{file}");
    // The controller authorises first: the host never sees a bad token.
    let (st, _) = get(format!("{base}/cli/fs/read-dir?path={}&token=wrong", q(&ctl_ws))).await;
    assert_eq!(st, 403);
    // A path no host holds stays on the controller.
    let (st, local) = get(format!("{base}/cli/fs/read-dir?path={}&token=ctl-owner", q(&ctl_root))).await;
    assert_eq!((st, local), (404, json!({"error": "local"})));

    // Writes land on the host's disk, never the controller's copy.
    let (st, w) = post(
        format!("{base}/cli/fs/write-file?token=ctl-owner"),
        json!({"path": ctl_ws.join("from-the-app.md").to_string_lossy(), "content": "edited in the app\n"}),
    )
    .await;
    assert_eq!(st, 200, "{w}");
    assert_eq!(std::fs::read_to_string(ws.join("from-the-app.md")).unwrap(), "edited in the app\n");
    assert!(!ctl_ws.join("from-the-app.md").exists());

    // The git panel. The repository is the agent's, on the host (`git
    // init` through the relay needs the workspace registered on the host:
    // the project create/add phase).
    k2_core::git::init_repo(&ws.to_string_lossy()).unwrap();
    {
        use std::io::Write;
        let mut cfg = std::fs::OpenOptions::new().append(true).open(ws.join(".git/config")).unwrap();
        writeln!(cfg, "[user]\n\tname = lab\n\temail = lab@example.invalid").unwrap();
    }
    let (st, changes) = get(format!("{base}/cli/git/changes?path={}&token=ctl-owner", q(&ctl_ws))).await;
    assert_eq!(st, 200, "{changes}");
    let listed = changes.to_string();
    assert!(listed.contains("on-the-host.md") && listed.contains("from-the-app.md"), "{changes}");
    let (st, r) = post(format!("{base}/cli/git/stage-all?token=ctl-owner"), json!({"path": ctl_ws.to_string_lossy()})).await;
    assert_eq!(st, 200, "stage: {r}");
    let (st, r) = post(
        format!("{base}/cli/git/commit?token=ctl-owner"),
        json!({"path": ctl_ws.to_string_lossy(), "message": "first, through the controller"}),
    )
    .await;
    assert_eq!(st, 200, "commit: {r}");
    std::fs::write(ws.join("on-the-host.md"), "written by the agent\nand changed\n").unwrap();
    let (st, diff) = get(format!(
        "{base}/cli/git/diff-file?path={}&file_path=on-the-host.md&token=ctl-owner",
        q(&ctl_ws)
    ))
    .await;
    assert_eq!(st, 200, "{diff}");
    assert!(diff.to_string().contains("and changed"), "{diff}");
}
