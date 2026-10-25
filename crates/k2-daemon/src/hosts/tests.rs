//! The host link on loopback sockets: the controller's hub and relay
//! (`hub`) with `RemoteHost`, against
//!
//! - a fake host written here, frame by frame (placement, epochs, hooks,
//!   `open_url`, refusals, link loss), and
//! - the real host link (`link::serve_once`) running real PTYs through
//!   today's `v2_spawn` with the `claude` shim (`exec cat`).
//!
//! Deadlines only stop a hung test; nothing asserts a duration.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k2_core::test_env::{self, AgentShim, TempHome};
use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use super::proto::{Frame, ATTACH_PATH, PROTOCOL};
use super::{hub, link, HostConfig};
use crate::agent_host::remote::{RemoteHost, NO_HOSTS};
use crate::agent_host::PlacementEpoch;

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

const DEADLINE: Duration = Duration::from_secs(30);
const TOKEN: &str = "test-bootstrap-token";

/// A controller on loopback: `/cli/hosts/attach` and `/cli/sessions/grid`
/// served by `hub` over one shared `RemoteHost`.
struct Controller {
    remote: &'static RemoteHost,
    attach_url: String,
    grid_addr: std::net::SocketAddr,
}

impl Controller {
    async fn start() -> Self {
        // Leaked: each test owns its RemoteHost for the test's life.
        let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
        let attach = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let attach_url = format!("ws://{}{ATTACH_PATH}", attach.local_addr().unwrap());
        tokio::spawn(async move {
            while let Ok((s, _)) = attach.accept().await {
                tokio::spawn(hub::serve_attach(s, remote, TOKEN));
            }
        });
        let grid = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let grid_addr = grid.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((s, _)) = grid.accept().await {
                tokio::spawn(async move {
                    // The relay reads `session` the way the gate does.
                    let mut peek = [0u8; 2048];
                    let n = s.peek(&mut peek).await.unwrap();
                    let head = String::from_utf8_lossy(&peek[..n]).to_string();
                    let session = head
                        .split("session=")
                        .nth(1)
                        .and_then(|r| r.split([' ', '&']).next())
                        .unwrap_or("")
                        .to_string();
                    hub::serve_grid(s, remote, &session).await;
                });
            }
        });
        Controller { remote, attach_url, grid_addr }
    }

    async fn attach_grid(&self, session: &str) -> Ws {
        let url = format!("ws://{}/cli/sessions/grid?session={session}", self.grid_addr);
        tokio_tungstenite::connect_async(url).await.expect("grid upgrade").0
    }

    async fn wait_hosts(&self, n: usize) {
        within("hosts to link", async {
            while self.remote.status_json()["hosts"].as_array().map(Vec::len) != Some(n) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
    }
}

async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(DEADLINE, f).await.unwrap_or_else(|_| panic!("timed out waiting for {what}"))
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

/// A fake host that completed the handshake as `name`.
async fn fake_host(c: &Controller, name: &str) -> Ws {
    let (mut ws, _) = tokio_tungstenite::connect_async(c.attach_url.as_str()).await.expect("dial");
    send(&mut ws, &Frame::Hello { protocol: PROTOCOL, host_name: name.into(), token: TOKEN.into(), labels: vec![] }).await;
    match recv(&mut ws).await {
        Frame::Welcome { host_id, protocol, .. } => {
            assert_eq!(host_id, name);
            assert_eq!(protocol, PROTOCOL);
        }
        other => panic!("expected welcome, got {other:?}"),
    }
    ws
}

/// Place `agent` on the fake host, answering its spawn with `session`.
/// Returns the epoch the controller assigned.
async fn place(c: &Controller, host: &mut Ws, agent: &str, session: &str) -> u64 {
    let body = json!({"agent_name": agent, "cwd": "/tmp", "command": "claude"}).to_string();
    let remote = c.remote;
    let spawn = tokio::spawn(async move { remote.spawn(body.as_bytes()).await });
    let (req, epoch) = match recv(host).await {
        Frame::Spawn { req, agent_id, epoch, body } => {
            assert_eq!(agent_id, agent);
            assert_eq!(body["command"], "claude", "the spawn body travels verbatim");
            (req, epoch)
        }
        other => panic!("expected spawn, got {other:?}"),
    };
    let reply = json!({"sessionId": session, "reused": false}).to_string();
    send(host, &Frame::Reply { req, status: "200 OK".into(), body: reply.clone() }).await;
    let (status, out) = within("the spawn answer", spawn).await.unwrap();
    assert_eq!((status.as_str(), out.as_str()), ("200 OK", reply.as_str()));
    epoch
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_with_no_host_connected_is_a_clear_error() {
    let c = Controller::start().await;
    let (status, body) = c.remote.spawn(br#"{"agent_name":"a-none","command":"claude"}"#).await;
    assert_eq!(status, "503 Service Unavailable");
    assert_eq!(serde_json::from_str::<Value>(&body).unwrap(), json!({"error": NO_HOSTS}));
    assert!(c.remote.placements().is_empty(), "nothing is placed");
    // And nothing to relay a grid for.
    let mut client = c.attach_grid("no-such-session").await;
    let first = within("the error frame", client.next()).await.unwrap().unwrap();
    let v: Value = serde_json::from_str(first.to_text().unwrap()).unwrap();
    assert_eq!(v["event"], "error");
    assert_eq!(v["payload"]["message"], "session no-such-session is not placed on any agent host");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fake_host_s1_spawn_attach_type_hook_open_url_close() {
    let c = Controller::start().await;
    let mut host = fake_host(&c, "fake-1").await;
    c.wait_hosts(1).await;

    // ── spawn: placed on the host, at a fresh epoch ─────────────────────
    let epoch = place(&c, &mut host, "agent-s1", "sess-s1").await;
    let p = c.remote.placement("agent-s1").expect("placed");
    assert_eq!(p.host_id, "fake-1");
    assert_eq!(p.session_id.as_deref(), Some("sess-s1"));
    assert_eq!(p.epoch, PlacementEpoch(epoch));

    // A second spawn of the same agent goes back to the same placement.
    let remote = c.remote;
    let again = tokio::spawn(async move { remote.spawn(br#"{"agent_name":"agent-s1","command":"claude"}"#).await });
    match recv(&mut host).await {
        Frame::Spawn { req, epoch: e, .. } => {
            assert_eq!(e, epoch, "same placement, same epoch");
            send(&mut host, &Frame::Reply { req, status: "200 OK".into(), body: r#"{"sessionId":"sess-s1","reused":true}"#.into() }).await;
        }
        other => panic!("expected spawn, got {other:?}"),
    }
    assert_eq!(within("reuse", again).await.unwrap().0, "200 OK");

    // ── attach: the host's frames reach the client, input reaches the host
    let mut client = c.attach_grid("sess-s1").await;
    let stream = match recv(&mut host).await {
        Frame::GridOpen { stream, session_id, epoch: e, proto } => {
            assert_eq!((session_id.as_str(), e, proto), ("sess-s1", epoch, None));
            stream
        }
        other => panic!("expected grid.open, got {other:?}"),
    };
    let snapshot = r#"{"event":"snapshot","payload":{"grid":[]}}"#;
    send(&mut host, &Frame::GridData { stream, text: snapshot.into() }).await;
    let got = within("the snapshot", client.next()).await.unwrap().unwrap();
    assert_eq!(got.to_text().unwrap(), snapshot);
    let input = r#"{"action":"input","text":"hi\r"}"#;
    client.send(Message::Text(input.into())).await.unwrap();
    assert_eq!(recv(&mut host).await, Frame::GridData { stream, text: input.into() });

    // ── hooks: accepted for the placed session at its epoch only ───────
    let mut hooks = c.remote.hook_events();
    let hook = |pane: &str, event: &str, epoch: Option<u64>| Frame::Hook {
        pane: pane.into(),
        event: event.into(),
        session_id: Some(pane.into()),
        conversation: None,
        agent_pid: Some(42),
        at_ms: 1,
        epoch,
    };
    send(&mut host, &hook("not-placed", "Stop", None)).await;
    send(&mut host, &hook("sess-s1", "Stop", Some(epoch + 1000))).await;
    send(&mut host, &hook("sess-s1", "UserPromptSubmit", Some(epoch))).await;
    let h = within("the hook", hooks.recv()).await.unwrap();
    assert_eq!((h.host_id.as_str(), h.agent_id.as_str(), h.session_id.as_str(), h.event.as_str()),
        ("fake-1", "agent-s1", "sess-s1", "UserPromptSubmit"), "the unplaced and stale-epoch hooks were dropped");
    assert_eq!(c.remote.placement("agent-s1").unwrap().hooks, 1);

    // ── /login: open_url lands on the controller's session events bus ──
    let mut events = crate::session_events::subscribe();
    let url = format!("https://claude.ai/oauth/authorize?state={}", uuid::Uuid::new_v4());
    send(&mut host, &Frame::OpenUrl { url: url.clone(), source: "shim".into() }).await;
    within("open_url on the bus", async {
        loop {
            if let Ok(crate::session_events::SessionEvent::OpenUrl { url: u, source }) = events.recv().await {
                if u == url {
                    assert_eq!(source, "shim");
                    return;
                }
            }
        }
    })
    .await;

    // ── client leaves: the host is told to close the stream ────────────
    client.close(None).await.unwrap();
    match recv(&mut host).await {
        Frame::GridClose { stream: s, .. } => assert_eq!(s, stream),
        other => panic!("expected grid.close, got {other:?}"),
    }

    // ── close: forwarded, and the placement goes ────────────────────────
    let close = tokio::spawn(async move { remote.close(br#"{"agent_name":"agent-s1","force":true}"#).await });
    match recv(&mut host).await {
        Frame::Close { req, agent_id, epoch: e, body } => {
            assert_eq!((agent_id.as_str(), e), ("agent-s1", epoch));
            assert_eq!(body["force"], true);
            send(&mut host, &Frame::Reply { req, status: "200 OK".into(), body: r#"{"closed":true,"indexCleared":false}"#.into() }).await;
        }
        other => panic!("expected close, got {other:?}"),
    }
    assert_eq!(within("close", close).await.unwrap().0, "200 OK");
    assert_eq!(c.remote.placement("agent-s1"), None);
    // Closing an agent placed nowhere answers as today's close does.
    let (s, b) = c.remote.close(br#"{"agent_name":"agent-s1","clear_index":true}"#).await;
    assert_eq!((s.as_str(), b.as_str()), ("200 OK", r#"{"closed":false,"indexCleared":true}"#));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn placement_spreads_and_session_gone_drops_it() {
    let c = Controller::start().await;
    let mut h1 = fake_host(&c, "h1").await;
    c.wait_hosts(1).await;
    let e1 = place(&c, &mut h1, "a1", "s1").await;
    let mut h2 = fake_host(&c, "h2").await;
    c.wait_hosts(2).await;
    // h2 has fewer placements, so the next agent goes there.
    let e2 = place(&c, &mut h2, "a2", "s2").await;
    assert!(e2 > e1, "epochs only grow");
    assert_eq!(c.remote.placement("a2").unwrap().host_id, "h2");

    // A stale session.gone (wrong epoch) changes nothing; the right one drops it.
    send(&mut h1, &Frame::SessionGone { agent_id: "a1".into(), epoch: Some(e1 + 1) }).await;
    send(&mut h1, &Frame::SessionGone { agent_id: "a2".into(), epoch: Some(e2) }).await; // not h1's
    send(&mut h1, &Frame::SessionGone { agent_id: "a1".into(), epoch: Some(e1) }).await;
    within("a1 to go", async {
        while c.remote.placement("a1").is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(c.remote.placement("a2").is_some(), "another host's session.gone is ignored");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bad_token_and_wrong_protocol_are_refused() {
    let c = Controller::start().await;
    for (hello, code) in [
        (Frame::Hello { protocol: PROTOCOL, host_name: "x".into(), token: "wrong".into(), labels: vec![] }, "bad_token"),
        (Frame::Hello { protocol: PROTOCOL + 1, host_name: "x".into(), token: TOKEN.into(), labels: vec![] }, "protocol"),
        (Frame::OpenUrl { url: "https://x".into(), source: "shim".into() }, "protocol"),
    ] {
        let (mut ws, _) = tokio_tungstenite::connect_async(c.attach_url.as_str()).await.unwrap();
        send(&mut ws, &hello).await;
        match recv(&mut ws).await {
            Frame::Refused { code: got, .. } => assert_eq!(got, code),
            other => panic!("expected refused, got {other:?}"),
        }
    }
    assert_eq!(c.remote.status_json()["hosts"], json!([]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn link_loss_fails_waiting_requests_and_forgets_placements() {
    let c = Controller::start().await;
    let mut host = fake_host(&c, "lossy").await;
    c.wait_hosts(1).await;
    place(&c, &mut host, "kept", "s-kept").await;
    let remote = c.remote;
    let pending = tokio::spawn(async move { remote.spawn(br#"{"agent_name":"never","command":"claude"}"#).await });
    assert!(matches!(recv(&mut host).await, Frame::Spawn { .. }));
    drop(host);
    let (status, body) = within("the failed spawn", pending).await.unwrap();
    assert_eq!(status, "502 Bad Gateway", "{body}");
    assert!(body.contains("disconnected before it answered"), "{body}");
    c.wait_hosts(0).await;
    assert!(c.remote.placements().is_empty());
    // A newer link with the same name replaces an older one.
    let mut old = fake_host(&c, "dup").await;
    let _new = fake_host(&c, "dup").await;
    match recv(&mut old).await {
        Frame::Refused { code, .. } => assert_eq!(code, "duplicate"),
        other => panic!("expected refused, got {other:?}"),
    }
    c.wait_hosts(1).await;
}

/// Wait until the relayed grid's text (every snapshot/delta row seen)
/// contains `needle`.
async fn wait_grid_text(client: &mut Ws, needle: &str) -> String {
    let mut seen = String::new();
    within(needle, async {
        loop {
            match client.next().await {
                Some(Ok(Message::Text(t))) => {
                    seen.push_str(&t);
                    if seen.contains(needle) {
                        return;
                    }
                }
                Some(Ok(_)) => {}
                other => panic!("grid ended before {needle:?}: {other:?}; seen {seen}"),
            }
        }
    })
    .await;
    seen
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_host_link_runs_the_agent_and_relays_its_pty() {
    let home = TempHome::new();
    let _shim = AgentShim::install();
    let _ = k2_core::db::init_for_tests();
    let c = Controller::start().await;
    let token_file = test_env::unique_temp_path("host-token");
    std::fs::write(&token_file, TOKEN).unwrap();
    let cfg = HostConfig {
        attach_url: c.attach_url.clone(),
        token: TOKEN.into(),
        host_name: "real-1".into(),
        labels: vec![],
    };
    let link_task = tokio::spawn(async move { link::serve_once(&cfg).await });
    c.wait_hosts(1).await;

    // spawn through the controller: the real host runs today's v2 spawn.
    let agent = format!("dist-link-real-{}", uuid::Uuid::new_v4());
    let cwd = home.path().join("work");
    std::fs::create_dir_all(&cwd).unwrap();
    let body = json!({"agent_name": agent, "cwd": cwd, "command": "claude", "cols": 80, "rows": 24}).to_string();
    let (status, out) = within("the spawn", c.remote.spawn(body.as_bytes())).await;
    assert_eq!(status, "200 OK", "{out}");
    let out: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(out["reused"], false);
    let session = out["sessionId"].as_str().expect("sessionId").to_string();
    let p = c.remote.placement(&agent).expect("placed");
    assert_eq!((p.host_id.as_str(), p.session_id.as_deref()), ("real-1", Some(session.as_str())));
    // The agent is a child of the host's daemon (here: this process), in
    // the host's own placement table.
    let live = crate::agent_host::host().find_agent(&agent).expect("on the host");
    let pid = live.child_pid().expect("child pid");
    assert!(live.is_child_alive());

    // attach and type: `cat` echoes, so the text comes back on the grid.
    let mut client = c.attach_grid(&session).await;
    let first = wait_grid_text(&mut client, r#""event":"snapshot""#).await;
    assert!(first.contains(r#""event":"snapshot""#));
    let text = format!("dist-link-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
    client.send(Message::Text(json!({"action": "input", "text": text}).to_string())).await.unwrap();
    client.send(Message::Text(json!({"action": "input", "text": "\r"}).to_string())).await.unwrap();
    wait_grid_text(&mut client, &text).await;
    client.close(None).await.unwrap();

    // close (kill): the host reaps the child and the placement goes.
    let (status, out) =
        within("the close", c.remote.close(json!({"agent_name": agent, "force": true}).to_string().as_bytes())).await;
    assert_eq!(status, "200 OK", "{out}");
    assert_eq!(serde_json::from_str::<Value>(&out).unwrap()["closed"], true);
    assert_eq!(c.remote.placement(&agent), None);
    assert!(crate::agent_host::host().find_agent(&agent).is_none());
    within("the child to exit", async {
        // SAFETY: signal 0 only checks that the pid exists.
        while unsafe { libc::kill(pid, 0) } == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;

    link_task.abort();
}
