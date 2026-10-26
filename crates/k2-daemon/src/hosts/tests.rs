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
/// The controller daemon's owner token in these tests.
const OWNER_TOKEN: &str = "test-controller-owner-token";

/// A controller on loopback: `/cli/hosts/attach`, `/cli/sessions/grid`
/// and `/cli/sessions/bytes` served by `hub` over one shared `RemoteHost`.
struct Controller {
    remote: &'static RemoteHost,
    attach_url: String,
    grid_addr: std::net::SocketAddr,
}

impl Controller {
    async fn start() -> Self {
        // Leaked: each test owns its RemoteHost for the test's life.
        let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
        Self::start_on(remote).await
    }

    async fn start_on(remote: &'static RemoteHost) -> Self {
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
                    // What the gate does: the query decides the client
                    // (`gate::grid_client`) and names the session.
                    let mut peek = [0u8; 4096];
                    let n = s.peek(&mut peek).await.unwrap();
                    let head = String::from_utf8_lossy(&peek[..n]).to_string();
                    let target = head.split_whitespace().nth(1).unwrap_or("").to_string();
                    let (path, query) = target.split_once('?').map(|(p, q)| (p.to_string(), q.to_string())).unwrap_or_default();
                    let params = crate::routes::http::parse_params(&path, &query);
                    let session = params.get("session").cloned().unwrap_or_default();
                    match super::gate::grid_client(&query, OWNER_TOKEN) {
                        Ok(client) if path == "/cli/sessions/bytes" => {
                            let from = params.get("from").and_then(|f| f.parse().ok()).unwrap_or(0);
                            hub::serve_bytes(s, remote, &session, from, client).await
                        }
                        Ok(client) => hub::serve_grid(s, remote, &session, client).await,
                        Err(why) => {
                            use tokio::io::AsyncWriteExt;
                            let mut s = s;
                            let body = format!("{why:?}");
                            let _ = s
                                .write_all(format!("HTTP/1.1 403 Forbidden\r\nContent-Length: {}\r\n\r\n{body}", body.len()).as_bytes())
                                .await;
                        }
                    }
                });
            }
        });
        Controller { remote, attach_url, grid_addr }
    }

    async fn attach_grid(&self, session: &str) -> Ws {
        self.attach_grid_with(session, OWNER_TOKEN).await.expect("grid upgrade")
    }

    async fn attach_grid_with(&self, session: &str, token: &str) -> Result<Ws, String> {
        let url = format!("ws://{}/cli/sessions/grid?session={session}&token={token}", self.grid_addr);
        tokio_tungstenite::connect_async(url).await.map(|r| r.0).map_err(|e| e.to_string())
    }

    async fn attach_bytes(&self, session: &str, from: u64) -> Result<Ws, String> {
        let url = format!("ws://{}/cli/sessions/bytes?session={session}&from={from}&token={OWNER_TOKEN}", self.grid_addr);
        tokio_tungstenite::connect_async(url).await.map(|r| r.0).map_err(|e| e.to_string())
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

/// One real host link (`link::serve_once`) at a time: a host's link state
/// is process-wide (`placed`, `resume_list`), so one test's relink would
/// drop another's spawn still in flight as dead.
fn real_link() -> std::sync::MutexGuard<'static, ()> {
    static REAL_LINK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    REAL_LINK.lock().unwrap_or_else(|p| p.into_inner())
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
    send(&mut ws, &Frame::Hello { protocol: PROTOCOL, host_name: name.into(), token: TOKEN.into(), labels: vec![], resume: vec![] }).await;
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
    // Each agent its own cwd: a shared one would be a placement preference (S7).
    let body = json!({"agent_name": agent, "cwd": format!("/w/{agent}"), "command": "claude"}).to_string();
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
    // The host's reply, verbatim; a placementNote (S7) may be added.
    let mut out: Value = serde_json::from_str(&out).unwrap();
    out.as_object_mut().unwrap().remove("placementNote");
    assert_eq!((status.as_str(), out), ("200 OK", serde_json::from_str::<Value>(&reply).unwrap()));
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
        Frame::GridOpen { stream, session_id, epoch: e, proto, as_user, ring_offset: None } => {
            assert_eq!((session_id.as_str(), e, proto, as_user), ("sess-s1", epoch, None, None));
            stream
        }
        other => panic!("expected grid.open, got {other:?}"),
    };
    let snapshot = r#"{"event":"snapshot","payload":{"grid":[]}}"#;
    send(&mut host, &Frame::GridData { stream, text: snapshot.into(), ring_offset: None }).await;
    let got = within("the snapshot", client.next()).await.unwrap().unwrap();
    assert_eq!(got.to_text().unwrap(), snapshot);
    let input = r#"{"action":"input","text":"hi\r"}"#;
    client.send(Message::Text(input.into())).await.unwrap();
    assert_eq!(recv(&mut host).await, Frame::GridData { stream, text: input.into(), ring_offset: None });

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
    send(&mut h1, &Frame::SessionGone { agent_id: "a1".into(), epoch: Some(e1 + 1), exit_code: None }).await;
    send(&mut h1, &Frame::SessionGone { agent_id: "a2".into(), epoch: Some(e2), exit_code: None }).await; // not h1's
    send(&mut h1, &Frame::SessionGone { agent_id: "a1".into(), epoch: Some(e1), exit_code: None }).await;
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
        (Frame::Hello { protocol: PROTOCOL, host_name: "x".into(), token: "wrong".into(), labels: vec![], resume: vec![] }, "bad_token"),
        (Frame::Hello { protocol: PROTOCOL + 1, host_name: "x".into(), token: TOKEN.into(), labels: vec![], resume: vec![] }, "protocol"),
        (Frame::Hello { protocol: 0, host_name: "x".into(), token: TOKEN.into(), labels: vec![], resume: vec![] }, "protocol"),
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
async fn link_loss_fails_waiting_requests_and_keeps_placements_as_lost() {
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
    // W3: a protocol-2 host's placement is kept, lost, not forgotten; the
    // spawn that never got an answer reserved nothing.
    assert_eq!(c.remote.placements().iter().map(|p| p.agent_id.as_str()).collect::<Vec<_>>(), vec!["kept"]);
    assert_eq!(c.remote.status_json()["lost"], json!(["lossy"]));
    // A protocol-1 host can't resume, so its placements are forgotten, as before W3.
    let (mut v1, _) = tokio_tungstenite::connect_async(c.attach_url.as_str()).await.unwrap();
    v1.send(Message::Text(r#"{"t":"hello","protocol":1,"host_name":"old","token":"test-bootstrap-token","labels":[]}"#.into()))
        .await
        .unwrap();
    assert!(matches!(recv(&mut v1).await, Frame::Welcome { protocol: 1, .. }));
    c.wait_hosts(1).await;
    place(&c, &mut v1, "on-v1", "s-v1").await;
    drop(v1);
    c.wait_hosts(0).await;
    assert_eq!(c.remote.placement("on-v1"), None, "a protocol-1 host's placements are forgotten");
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
    let _link = real_link();
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
    let link_task = tokio::spawn(async move { link::serve_once(&cfg, &link::Timings::PRODUCTION, || {}).await });
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

// ── reconnect (kind-dist finding: a read error mid-session) ─────────────

#[test]
fn backoff_restarts_after_a_link_and_doubles_to_the_cap_while_attempts_fail() {
    let t = link::Timings::PRODUCTION;
    let s = Duration::from_secs;
    // A link that was up: retry after 1 s, whatever came before.
    assert_eq!(link::next_backoff(s(30), true, &t), (s(1), s(2)));
    // Failed attempts: 1, 2, 4, 8, 16, 30, 30.
    let mut prev = t.backoff_min;
    let mut sleeps = vec![];
    for _ in 0..7 {
        let (sleep, next) = link::next_backoff(prev, false, &t);
        sleeps.push(sleep.as_secs());
        prev = next;
    }
    assert_eq!(sleeps, vec![1, 2, 4, 8, 16, 30, 30]);
}

/// Complete the controller's half of the handshake on `s`.
async fn welcome(s: TcpStream) -> WebSocketStream<TcpStream> {
    let mut ws = tokio_tungstenite::accept_async(s).await.expect("upgrade");
    match ws.next().await {
        Some(Ok(Message::Text(t))) => assert!(matches!(Frame::decode(&t), Ok(Frame::Hello { .. }))),
        other => panic!("expected hello, got {other:?}"),
    }
    let w = Frame::Welcome { protocol: PROTOCOL, host_id: "h".into(), controller: "fake".into() };
    ws.send(Message::Text(w.encode())).await.unwrap();
    ws
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_reconnects_after_a_reset_a_mute_peer_and_a_silent_controller() {
    use link::LinkEvent::*;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}{ATTACH_PATH}", listener.local_addr().unwrap());
    // Held sockets stay open until the test ends.
    let (hold_tx, mut hold_rx) = tokio::sync::mpsc::unbounded_channel::<Box<dyn std::any::Any + Send>>();
    tokio::spawn(async move {
        // 1: link, then reset the socket mid-session (no closing handshake).
        let (s, _) = listener.accept().await.unwrap();
        let ws = welcome(s).await;
        // A zero linger makes the drop a RST: the "connection reset without
        // closing handshake" kind-dist saw.
        #[allow(deprecated)]
        ws.get_ref().set_linger(Some(Duration::ZERO)).unwrap();
        drop(ws);
        // 2: accept TCP but never answer the upgrade.
        let (s, _) = listener.accept().await.unwrap();
        hold_tx.send(Box::new(s)).unwrap();
        // 3: link, then go silent (no frames, no pings) with the socket open.
        let (s, _) = listener.accept().await.unwrap();
        hold_tx.send(Box::new(welcome(s).await)).unwrap();
        // 4: link and stay.
        let (s, _) = listener.accept().await.unwrap();
        hold_tx.send(Box::new(welcome(s).await)).unwrap();
    });
    tokio::spawn(async move {
        let mut held = vec![];
        while let Some(s) = hold_rx.recv().await {
            held.push(s);
        }
    });

    let ms = Duration::from_millis;
    // The connect and idle deadlines are the ones under test (the mute peer
    // and the silent controller); the rest only need to never fire here,
    // even on a loaded machine.
    let t = link::Timings {
        connect: ms(2000),
        welcome: ms(5000),
        idle: ms(2000),
        close: ms(500),
        backoff_min: ms(20),
        backoff_max: ms(80),
    };
    let cfg = HostConfig { attach_url: url, token: TOKEN.into(), host_name: "reconnect-1".into(), labels: vec![] };
    let (otx, mut orx) = tokio::sync::mpsc::unbounded_channel();
    let task = tokio::spawn(link::run_with(cfg, t, Some(otx)));
    // The fourth link stays up, so exactly 13 events come.
    let mut events = vec![];
    within("four attempts and the fourth link", async {
        while events.len() < 13 {
            events.push(orx.recv().await.expect("link loop alive"));
        }
    })
    .await;
    task.abort();

    // Error text is the transport's; check its kind, then the decisions.
    let err = |i: usize| match &events[i] {
        Ended { error: Some(e), .. } => e.clone(),
        other => panic!("event {i}: expected an end with an error, got {other:?} in {events:?}"),
    };
    // The reset reaches whichever side moves next: the read, or the
    // keepalive ping's send.
    assert!(err(2).starts_with("read: ") || err(2).starts_with("send: "), "{events:?}");
    assert!(err(5).starts_with("connect: no answer within"), "{events:?}");
    assert!(err(9).starts_with("nothing from the controller for"), "{events:?}");
    let shape: Vec<link::LinkEvent> = events
        .iter()
        .map(|e| match e {
            Ended { was_linked, .. } => Ended { was_linked: *was_linked, error: None },
            e => e.clone(),
        })
        .collect();
    let ended = |was_linked| Ended { was_linked, error: None };
    assert_eq!(
        shape,
        vec![
            // reset mid-session → retry at the minimum
            Attempt(1), Linked, ended(true), Sleep(ms(20)),
            // a peer that never upgrades → connect deadline, then doubled
            Attempt(2), ended(false), Sleep(ms(40)),
            // a silent controller → idle deadline, back to the minimum
            Attempt(3), Linked, ended(true), Sleep(ms(20)),
            Attempt(4), Linked,
        ]
    );
}

// ── the desktop app's session token (kind-dist finding) ──────────────────

/// Read grid messages until one satisfies `pick`; returns it and every
/// message before it.
async fn grid_until(client: &mut Ws, what: &str, pick: impl Fn(&Value) -> bool) -> (Value, Vec<Value>) {
    let mut before = vec![];
    within(what, async {
        loop {
            match client.next().await {
                Some(Ok(Message::Text(t))) => {
                    let v: Value = serde_json::from_str(&t).expect("grid JSON");
                    if pick(&v) {
                        return (v, std::mem::take(&mut before));
                    }
                    before.push(v);
                }
                Some(Ok(_)) => {}
                other => panic!("grid ended before {what}: {other:?}; seen {before:?}"),
            }
        }
    })
    .await
}

fn grid_text(msgs: &[Value]) -> String {
    msgs.iter().map(Value::to_string).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn app_session_token_spawns_and_attaches_through_the_controller() {
    let _link = real_link();
    use k2_core::connect_users::{self, Role};
    let home = TempHome::new();
    let _shim = AgentShim::install();
    let _ = k2_core::db::init_for_tests();

    // The desktop app signs in (POST /cli/auth/login checks the password,
    // then issues a session): an Owner-role Connect user.
    let user = format!("applab{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
    let password = "correct horse battery staple 42";
    connect_users::add_user(&user, password).expect("add user");
    connect_users::set_role(&user, Role::Owner).expect("owner role");
    assert!(connect_users::verify(&user, password));
    let session_token = connect_users::create_session(&user);
    let query = format!("token={session_token}");

    // Spawn and close take the dispatcher's own check under every role.
    assert!(crate::routes::http::token_ok(&query, OWNER_TOKEN));
    // The grid is relayed for that session, as that user; junk is refused.
    assert_eq!(
        super::gate::grid_client(&query, OWNER_TOKEN),
        Ok(hub::GridClient::ConnectUser { username: user.clone(), token: session_token.clone(), owner_token: OWNER_TOKEN.into() })
    );
    assert_eq!(super::gate::grid_client("token=nope", OWNER_TOKEN), Err(super::gate::GridRefusal::BadToken));
    assert_eq!(super::gate::grid_client("", OWNER_TOKEN), Err(super::gate::GridRefusal::BadToken));

    let c = Controller::start().await;
    let cfg = HostConfig { attach_url: c.attach_url.clone(), token: TOKEN.into(), host_name: "app-1".into(), labels: vec![] };
    let link_task = tokio::spawn(async move { link::serve_once(&cfg, &link::Timings::PRODUCTION, || {}).await });
    c.wait_hosts(1).await;

    let agent = format!("dist-link-app-{}", uuid::Uuid::new_v4());
    let cwd = home.path().join("work");
    std::fs::create_dir_all(&cwd).unwrap();
    let body = json!({"agent_name": agent, "cwd": cwd, "command": "claude", "cols": 80, "rows": 24}).to_string();
    let (status, out) = within("the spawn", c.remote.spawn(body.as_bytes())).await;
    assert_eq!(status, "200 OK", "{out}");
    let session = serde_json::from_str::<Value>(&out).unwrap()["sessionId"].as_str().unwrap().to_string();
    assert!(crate::agent_host::host().find_agent(&agent).is_some(), "runs on the host");

    // Attach as the app does: the session token on the grid socket.
    let mut client = c.attach_grid_with(&session, &session_token).await.expect("grid upgrade with the session token");
    let (mode, before) = grid_until(&mut client, "the first mode event", |v| v["event"] == "mode").await;
    assert!(before.iter().any(|v| v["event"] == "snapshot"), "{before:?}");
    // A Connect user starts as a capable viewer, exactly as under K2_ROLE=all.
    assert_eq!(mode["payload"], json!({"mode": "viewer", "capable": true}));

    // As a viewer, typing is denied (once) and never reaches the PTY.
    client.send(Message::Text(json!({"action": "input", "text": "as-viewer"}).to_string())).await.unwrap();
    grid_until(&mut client, "input_denied", |v| v["event"] == "input_denied").await;
    // Opting in to claimer, as the app does, then typing reaches the agent.
    client.send(Message::Text(json!({"action": "set_mode", "mode": "claimer"}).to_string())).await.unwrap();
    let (mode, _) = grid_until(&mut client, "the claimer ack", |v| v["event"] == "mode").await;
    assert_eq!(mode["payload"], json!({"mode": "claimer", "capable": true}));
    let text = format!("app-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
    client.send(Message::Text(json!({"action": "input", "text": text}).to_string())).await.unwrap();
    client.send(Message::Text(json!({"action": "input", "text": "\r"}).to_string())).await.unwrap();
    let (_, seen) = grid_until(&mut client, "the echo", |v| v.to_string().contains(&text)).await;
    assert!(!grid_text(&seen).contains("as-viewer"), "viewer input never reached the PTY: {seen:?}");

    // A removed user stops at once: the next frame ends the stream.
    connect_users::remove_user(&user).expect("remove user");
    let _ = client.send(Message::Text(json!({"action": "input", "text": "after-removal"}).to_string())).await;
    within("the stream to end", async {
        loop {
            match client.next().await {
                Some(Ok(Message::Text(t))) => assert!(!t.contains("after-removal"), "removed user's input reached the PTY"),
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                Some(Ok(_)) => {}
            }
        }
    })
    .await;

    let (status, out) = within("the close", c.remote.close(json!({"agent_name": agent, "force": true}).to_string().as_bytes())).await;
    assert_eq!(status, "200 OK", "{out}");
    assert!(crate::agent_host::host().find_agent(&agent).is_none());
    link_task.abort();
}

// ── protocol 2 contract (W0) ─────────────────────────────────────────────

#[test]
fn placement_epochs_are_persisted_and_only_grow() {
    use crate::agent_host::remote::EpochCounter;
    let dir = test_env::unique_temp_path("epochs");
    let path = dir.join("hosts").join("placement-epoch");
    let before = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    // No file: starts above the clock (protocol-1 epochs were ms stamps).
    let c = EpochCounter::persisted(path.clone());
    let e1 = c.next().unwrap();
    let e2 = c.next().unwrap();
    assert!(e1 > before, "first epoch {e1} is above any protocol-1 epoch issued before {before}");
    assert_eq!(e2, e1 + 1, "then it counts");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), e2.to_string(), "persisted before it is used");
    // A restarted controller continues from the file, not from the clock.
    let c = EpochCounter::persisted(path.clone());
    assert_eq!(c.next().unwrap(), e2 + 1);
    // A resumed placement's epoch is never issued again.
    c.observe(e2 + 100);
    assert_eq!(c.next().unwrap(), e2 + 101);
    // In memory (tests): counts from what it was given.
    let m = EpochCounter::in_memory(0);
    assert_eq!((m.next().unwrap(), m.next().unwrap()), (1, 2));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protocol_1_hosts_are_still_accepted_and_resume_is_handed_to_adopt() {
    let c = Controller::start().await;
    // A protocol-1 host (the image deployed on kind before W0).
    let (mut v1, _) = tokio_tungstenite::connect_async(c.attach_url.as_str()).await.unwrap();
    v1.send(Message::Text(r#"{"t":"hello","protocol":1,"host_name":"old-1","token":"test-bootstrap-token","labels":[]}"#.into()))
        .await
        .unwrap();
    match recv(&mut v1).await {
        Frame::Welcome { protocol, host_id, .. } => assert_eq!((protocol, host_id.as_str()), (1, "old-1")),
        other => panic!("expected welcome, got {other:?}"),
    }
    c.wait_hosts(1).await;
    assert_eq!(c.remote.link_protocol("old-1"), Some(1));

    // A protocol-2 host that still runs a placement reports it in hello.
    let resume = vec![super::proto::ResumePlacement {
        agent_id: "kept".into(),
        session_id: "s-kept".into(),
        epoch: 7,
        ring: Some(super::proto::RingRange { start: 0, end: 512 }),
        child_alive: true,
        ..Default::default()
    }];
    let resume: Vec<super::proto::ResumePlacement> =
        resume.into_iter().map(|r| super::proto::ResumePlacement { session_id: uuid::Uuid::new_v4().to_string(), ..r }).collect();
    let (mut v2, _) = tokio_tungstenite::connect_async(c.attach_url.as_str()).await.unwrap();
    send(&mut v2, &Frame::Hello { protocol: 2, host_name: "new-1".into(), token: TOKEN.into(), labels: vec![], resume: resume.clone() }).await;
    match recv(&mut v2).await {
        Frame::Welcome { protocol, .. } => assert_eq!(protocol, 2),
        other => panic!("expected welcome, got {other:?}"),
    }
    c.wait_hosts(2).await;
    assert_eq!(c.remote.link_protocol("new-1"), Some(2));
    // W3: the hub reconciled the resume on link: an agent the controller
    // didn't hold is adopted, at the host's epoch.
    let p = c.remote.placement("kept").expect("adopted");
    assert_eq!((p.host_id.as_str(), p.epoch.0), ("new-1", 7));
}

// ── W1: RemoteHost is the controller's AgentHost ─────────────────────────

/// Poll a sync read (bounded; fails loudly).
async fn until_sync<T>(what: &str, mut read: impl FnMut() -> Option<T>) -> T {
    within(what, async {
        loop {
            if let Some(v) = read() {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_host_drives_a_workspace_spawn_through_the_agent_host_trait() {
    let _link = real_link();
    use crate::agent_host::{AgentHost, InjectResult};
    use k2_core::inject_flow::InjectAction;
    use k2_core::terminal::{AlacEvent, LabelSource};

    let home = TempHome::new();
    let _shim = AgentShim::install();
    let _ = k2_core::db::init_for_tests();
    let c = Controller::start().await;
    let cfg = HostConfig { attach_url: c.attach_url.clone(), token: TOKEN.into(), host_name: "w1-host".into(), labels: vec![] };
    let link_task = tokio::spawn(async move { link::serve_once(&cfg, &link::Timings::PRODUCTION, || {}).await });
    c.wait_hosts(1).await;
    let ctl: &'static dyn AgentHost = c.remote;
    assert_eq!(ctl.kind(), "remote");

    // The workspace spawn (chat, heartbeat, wake), forwarded whole.
    let key = format!("w1-{}", uuid::Uuid::new_v4());
    let cwd = home.path().join("work");
    std::fs::create_dir_all(&cwd).unwrap();
    let req = crate::spawn::SpawnWorkspaceSessionRequest {
        agent_name: key.clone(),
        cwd: cwd.to_string_lossy().into_owned(),
        command: Some("claude".into()),
        args: Some(vec![]),
        cols: 90,
        rows: 28,
        canonical_key: Some(key.clone()),
        label: Some("W1 agent".into()),
        ..Default::default()
    };
    let remote = c.remote;
    let (spawned, session) = {
        let req = req.clone();
        within("the workspace spawn", tokio::task::spawn_blocking(move || remote.spawn_workspace_blocking(&req)))
            .await
            .unwrap()
            .expect("spawned on the host")
    };
    assert!(!spawned.reused);
    assert!(session.is_remote());
    // On the host: an in-process session under the same key.
    let on_host = crate::agent_host::host().find_agent(&key).expect("in the host's table");
    assert_eq!(on_host.session_id(), session.session_id());
    assert!(!on_host.is_remote());

    // Lookups and metadata through the trait.
    let found = ctl.find_agent(&key).expect("find_agent");
    assert!(found.same_session(&session));
    assert!(ctl.find_session(&session.session_id()).is_some_and(|s| s.same_session(&session)));
    assert!(ctl.placements().iter().any(|(a, s)| a == &key && s.same_session(&session)));
    assert_eq!(found.program(), Some("claude"));
    assert_eq!(found.cwd_path(), Some(cwd.as_path()));
    assert_eq!((found.label(), found.label_source()), ("W1 agent".to_string(), LabelSource::Locked));
    assert!(found.is_child_alive());
    assert_eq!(found.child_pid(), None, "a host pid is meaningless on the controller");
    // A second spawn of the same key is the host's reuse.
    let again = {
        let req = req.clone();
        tokio::task::spawn_blocking(move || remote.spawn_workspace_blocking(&req)).await.unwrap().unwrap()
    };
    assert!(again.0.reused);
    assert!(again.1.same_session(&session), "the same handle");

    // Live state, asked of the host.
    assert_eq!(ctl.size(&found), (90, 28));
    assert_eq!(ctl.resize_now(&found, 100, 30), (100, 30));
    assert_eq!(ctl.size(&found), (100, 30));
    assert!(!found.bracketed_paste_active(), "cat never asks for bracketed paste");
    assert_eq!(found.subscriber_count(), 0);

    // write reaches the PTY; the screen read comes back from the host.
    let typed = format!("w1-write-{}", &uuid::Uuid::new_v4().simple().to_string()[..6]);
    ctl.write(&found, format!("{typed}\r").into_bytes()).unwrap();
    until_sync("the write on the screen", || ctl.read_screen(&found, 50).iter().any(|l| l.contains(&typed)).then_some(())).await;
    assert!(found.visible_text_rows().iter().any(|l| l.contains(&typed)));
    // inject runs as one unit on the host.
    let injected = format!("w1-inject-{}", &uuid::Uuid::new_v4().simple().to_string()[..6]);
    let r = tokio::task::block_in_place(|| {
        ctl.inject(
            &found,
            vec![InjectAction::Write(injected.clone().into_bytes()), InjectAction::Sleep(20), InjectAction::Write(b"\r".to_vec()), InjectAction::CheckAlive],
        )
    });
    assert_eq!(r, InjectResult::Delivered);
    until_sync("the inject on the screen", || ctl.read_screen(&found, 50).iter().any(|l| l.contains(&injected)).then_some(())).await;
    // Labels and pins.
    assert_eq!(ctl.set_label(&found, "renamed".into(), true), "renamed");
    assert_eq!(on_host.label(), "renamed");
    let mut pins = ctl.attach(&found).pins;
    ctl.pin(&found, Some((80, 24)), Some("test".into()));
    assert_eq!(pins.recv().await.unwrap(), Some((80, 24)));

    // kill ends it: ChildExit reaches the controller's subscribers and the
    // placement goes.
    let mut events = ctl.events(&found);
    tokio::task::block_in_place(|| ctl.kill(&found));
    within("ChildExit on the controller", async {
        loop {
            if let Ok(AlacEvent::ChildExit(_)) = events.recv().await {
                return;
            }
        }
    })
    .await;
    assert!(!found.is_child_alive());
    until_sync("the placement to go", || ctl.find_agent(&key).is_none().then_some(())).await;
    assert!(crate::agent_host::host().find_agent(&key).is_none());

    // unplace closes it on the host.
    let key2 = format!("{key}-2");
    let req2 = crate::spawn::SpawnWorkspaceSessionRequest { agent_name: key2.clone(), canonical_key: Some(key2.clone()), ..req };
    tokio::task::spawn_blocking(move || remote.spawn_workspace_blocking(&req2)).await.unwrap().unwrap();
    assert!(crate::agent_host::host().find_agent(&key2).is_some());
    let gone = tokio::task::block_in_place(|| ctl.unplace(&key2)).expect("was placed");
    assert!(!gone.is_child_alive());
    assert!(ctl.find_agent(&key2).is_none());
    assert!(crate::agent_host::host().find_agent(&key2).is_none(), "closed on the host");

    // The low-level spawn has no remote form.
    assert!(ctl.spawn(k2_core::terminal::DaemonPtyConfig::default()).is_err());
    link_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_spawn_with_no_host_is_the_clear_error() {
    let c = Controller::start().await;
    let remote = c.remote;
    let req = crate::spawn::SpawnWorkspaceSessionRequest { agent_name: "x".into(), canonical_key: Some("x".into()), ..Default::default() };
    let err = tokio::task::spawn_blocking(move || remote.spawn_workspace_blocking(&req).map(|_| ())).await.unwrap().unwrap_err();
    // The same message, marked retryable (K2-a: routes answer 503).
    assert_eq!(err, format!("{}{NO_HOSTS}", crate::agent_host::remote::HOST_UNAVAILABLE));
    assert!(c.remote.placements().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transcript_probe_asks_the_host_that_ran_the_project() {
    let _link = real_link();
    let home = TempHome::new();
    let _shim = AgentShim::install();
    let _ = k2_core::db::init_for_tests();
    let c = Controller::start().await;
    let remote = c.remote;
    // No host: no answer (the caller then checks this machine, as before).
    assert_eq!(tokio::task::block_in_place(|| remote.transcript_exists("claude", "any", "/w")), None);

    let cfg = HostConfig { attach_url: c.attach_url.clone(), token: TOKEN.into(), host_name: "tp-host".into(), labels: vec![] };
    let link_task = tokio::spawn(async move { link::serve_once(&cfg, &link::Timings::PRODUCTION, || {}).await });
    c.wait_hosts(1).await;
    let project = home.path().join("proj");
    std::fs::create_dir_all(&project).unwrap();
    let project_s = project.to_string_lossy().into_owned();
    // A Claude transcript on the host's disk (this test process is the host).
    let sid = uuid::Uuid::new_v4().to_string();
    let canonical = std::fs::canonicalize(&project).unwrap();
    let slug = k2_core::chat_history::claude_project_hash(&canonical.to_string_lossy());
    let dir = home.path().join(".claude").join("projects").join(slug);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(format!("{sid}.jsonl")), "{}\n").unwrap();
    assert!(k2_core::chat_history::claude_session_file_exists(&sid, &project_s), "the host's own check finds it");

    // No placement yet: every linked host is asked.
    assert_eq!(tokio::task::block_in_place(|| remote.transcript_exists("claude", &sid, &project_s)), Some(true));
    assert_eq!(tokio::task::block_in_place(|| remote.transcript_exists("claude", "not-there", &project_s)), Some(false));
    // After an agent ran in the project, its host is the one asked.
    let req = crate::spawn::SpawnWorkspaceSessionRequest {
        agent_name: format!("tp-{}", uuid::Uuid::new_v4()),
        cwd: project_s.clone(),
        command: Some("claude".into()),
        args: Some(vec![]),
        cols: 80,
        rows: 24,
        ..Default::default()
    };
    let key = crate::spawn::canonical_key_for_request(&req);
    tokio::task::spawn_blocking(move || remote.spawn_workspace_blocking(&req).map(|_| ())).await.unwrap().unwrap();
    assert_eq!(tokio::task::block_in_place(|| remote.transcript_exists("claude", &sid, &project_s)), Some(true));
    tokio::task::block_in_place(|| crate::agent_host::AgentHost::unplace(remote, &key));
    link_task.abort();
}

// ── W3: placements survive controller restarts and host loss ────────────

use crate::agent_host::remote::PlacementStore;

/// A controller whose placements live in its own DB file.
fn store_at(path: &std::path::Path) -> std::sync::Arc<PlacementStore> {
    std::sync::Arc::new(PlacementStore::open_file(path).expect("store"))
}

impl Controller {
    /// The same harness on a store-backed RemoteHost, restored from it.
    async fn start_with_store(db: &std::path::Path) -> Self {
        let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::with_store(store_at(db), None)));
        remote.restore();
        Self::start_on(remote).await
    }
}

/// Spawn `agent` on a fake host through the S1 route; returns (session, epoch).
async fn place_on(c: &Controller, host: &mut Ws, agent: &str) -> (String, u64) {
    let session = uuid::Uuid::new_v4().to_string();
    let epoch = place(c, host, agent, &session).await;
    (session, epoch)
}

async fn hello_with(c: &Controller, name: &str, resume: Vec<super::proto::ResumePlacement>) -> Ws {
    let (mut ws, _) = tokio_tungstenite::connect_async(c.attach_url.as_str()).await.expect("dial");
    send(&mut ws, &Frame::Hello { protocol: PROTOCOL, host_name: name.into(), token: TOKEN.into(), labels: vec![], resume }).await;
    assert!(matches!(recv(&mut ws).await, Frame::Welcome { .. }));
    ws
}

fn resumed(agent: &str, session: &str, epoch: u64) -> super::proto::ResumePlacement {
    super::proto::ResumePlacement {
        agent_id: agent.into(),
        session_id: session.into(),
        epoch,
        child_alive: true,
        cwd: Some("/w".into()),
        program: Some("claude".into()),
        ..Default::default()
    }
}

/// The store keeps its own connection: restore (a controller's first
/// `host()`), persist and forget all finish while another thread holds
/// `k2_core::db::shared()`, as the active reaper does around its `host()`
/// call. On the shared connection this hung a restarted controller's boot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_placement_store_never_waits_on_the_shared_db_lock() {
    let db = test_env::unique_temp_path("w3-store-lock").with_extension("db");
    let shared = k2_core::db::shared();
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let holder = std::thread::spawn(move || {
        let _held = shared.lock();
        locked_tx.send(()).unwrap();
        let _ = done_rx.recv();
    });
    locked_rx.recv().unwrap();
    let (session, epoch) = {
        let c = Controller::start_with_store(&db).await;
        let mut host = fake_host(&c, "h-lock").await;
        c.wait_hosts(1).await;
        place_on(&c, &mut host, "agent-lock").await // persist
    };
    let c = Controller::start_with_store(&db).await; // restore
    let p = c.remote.placement("agent-lock").expect("restored");
    assert_eq!((p.epoch.0, p.session_id.as_deref()), (epoch, Some(session.as_str())));
    // The host returns without it: gone, its row deleted (forget).
    let _host = hello_with(&c, "h-lock", vec![]).await;
    c.wait_hosts(1).await;
    assert!(c.remote.placement("agent-lock").is_none());
    assert!(store_at(&db).load().unwrap().is_empty());
    done_tx.send(()).unwrap();
    holder.join().unwrap();
    let _ = std::fs::remove_file(&db);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn placements_survive_a_controller_restart_and_are_adopted() {
    let db = test_env::unique_temp_path("w3-store").with_extension("db");
    // Controller #1 places an agent and dies.
    let (session, epoch) = {
        let c = Controller::start_with_store(&db).await;
        let mut host = fake_host(&c, "h-s9").await;
        c.wait_hosts(1).await;
        place_on(&c, &mut host, "agent-s9").await
    };
    // Controller #2, same store: the placement is back, lost until h-s9 links.
    let c = Controller::start_with_store(&db).await;
    let p = c.remote.placement("agent-s9").expect("restored");
    assert_eq!((p.host_id.as_str(), p.epoch.0, p.session_id.as_deref()), ("h-s9", epoch, Some(session.as_str())));
    assert_eq!(c.remote.status_json()["lost"], json!(["h-s9"]));
    let ctl: &'static dyn crate::agent_host::AgentHost = c.remote;
    assert!(ctl.find_agent("agent-s9").is_some(), "still findable while lost");
    // While lost: no grid, and a spawn of the agent is refused, not placed elsewhere (I1).
    let mut other = fake_host(&c, "h-other").await;
    c.wait_hosts(1).await;
    let mut client = c.attach_grid(&session).await;
    let v: Value = serde_json::from_str(within("error", client.next()).await.unwrap().unwrap().to_text().unwrap()).unwrap();
    assert!(v["payload"]["message"].as_str().unwrap().contains("is lost"), "{v}");
    let (status, body) = c.remote.spawn(br#"{"agent_name":"agent-s9","command":"claude"}"#).await;
    assert_eq!(status, "502 Bad Gateway", "{body}");
    assert!(body.contains("lost"), "{body}");
    // New epochs stay above the restored one.
    let (_, e2) = place_on(&c, &mut other, "agent-new").await;
    assert!(e2 > epoch, "{e2} > {epoch}");
    // h-s9 comes back still running it at that epoch: adopted, grid works.
    let mut host = hello_with(&c, "h-s9", vec![resumed("agent-s9", &session, epoch)]).await;
    c.wait_hosts(2).await;
    assert_eq!(c.remote.status_json()["lost"], json!([]));
    let mut client = c.attach_grid(&session).await;
    let stream = match recv(&mut host).await {
        Frame::GridOpen { stream, session_id, epoch: e, .. } => {
            assert_eq!((session_id.as_str(), e), (session.as_str(), epoch));
            stream
        }
        other => panic!("expected grid.open, got {other:?}"),
    };
    send(&mut host, &Frame::GridData { stream, text: r#"{"event":"snapshot","payload":{"version":9}}"#.into(), ring_offset: Some(9) }).await;
    assert!(within("snapshot", client.next()).await.unwrap().unwrap().to_text().unwrap().contains("snapshot"));
    let _ = std::fs::remove_file(&db);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_old_epoch_placement_is_released_and_a_missing_one_is_gone() {
    let db = test_env::unique_temp_path("w3-epochs").with_extension("db");
    let c = Controller::start_with_store(&db).await;
    let mut host = fake_host(&c, "h-i1").await;
    c.wait_hosts(1).await;
    let (s_old, e_old) = place_on(&c, &mut host, "agent-i1").await;
    let (_s_gone, _) = place_on(&c, &mut host, "agent-gone").await;
    // The agent is closed and placed again: a newer epoch on the same host.
    let remote = c.remote;
    let close = tokio::spawn(async move { remote.close(br#"{"agent_name":"agent-i1","force":true}"#).await });
    match recv(&mut host).await {
        Frame::Close { req, .. } => send(&mut host, &Frame::Reply { req, status: "200 OK".into(), body: r#"{"closed":true}"#.into() }).await,
        other => panic!("expected close, got {other:?}"),
    }
    within("close", close).await.unwrap();
    let (s_new, e_new) = place_on(&c, &mut host, "agent-i1").await;
    assert!(e_new > e_old);
    drop(host);
    within("lost", async {
        while c.remote.status_json()["lost"] != json!(["h-i1"]) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let ctl: &'static dyn crate::agent_host::AgentHost = c.remote;
    let mut gone_events = ctl.events(&ctl.find_agent("agent-gone").expect("kept while lost"));
    // h-i1 links again resuming the OLD epoch of agent-i1 and not agent-gone.
    let mut host = hello_with(&c, "h-i1", vec![resumed("agent-i1", &s_old, e_old)]).await;
    match recv(&mut host).await {
        Frame::Release { agent_id, epoch, reason, .. } => {
            assert_eq!((agent_id.as_str(), epoch), ("agent-i1", e_old));
            assert!(reason.contains("stale"), "{reason}");
        }
        other => panic!("expected release, got {other:?}"),
    }
    // The controller's placement is untouched (the newer epoch, still lost
    // until a resume names it); agent-gone is gone, with ChildExit.
    let p = c.remote.placement("agent-i1").expect("kept");
    assert_eq!((p.epoch.0, p.session_id.as_deref()), (e_new, Some(s_new.as_str())));
    assert_eq!(c.remote.placement("agent-gone"), None);
    assert!(matches!(within("ChildExit", gone_events.recv()).await, Ok(k2_core::terminal::AlacEvent::ChildExit(_))));
    // And the store agrees after a restart.
    let rows = store_at(&db).load().unwrap();
    assert_eq!(rows.iter().map(|r| (r.agent_id.as_str(), r.epoch)).collect::<Vec<_>>(), vec![("agent-i1", e_new)]);
    let _ = std::fs::remove_file(&db);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_real_host_resumes_its_agents_after_a_link_drop() {
    let _link = real_link();
    let home = TempHome::new();
    let _shim = AgentShim::install();
    let _ = k2_core::db::init_for_tests();
    let db = test_env::unique_temp_path("w3-real").with_extension("db");
    let c = Controller::start_with_store(&db).await;
    let cfg = HostConfig { attach_url: c.attach_url.clone(), token: TOKEN.into(), host_name: "w3-host".into(), labels: vec![] };
    let link_cfg = cfg.clone();
    let link1 = tokio::spawn(async move { link::serve_once(&link_cfg, &link::Timings::PRODUCTION, || {}).await });
    c.wait_hosts(1).await;
    let key = format!("w3-{}", uuid::Uuid::new_v4());
    let cwd = home.path().join("work");
    std::fs::create_dir_all(&cwd).unwrap();
    let req = crate::spawn::SpawnWorkspaceSessionRequest {
        agent_name: key.clone(),
        cwd: cwd.to_string_lossy().into_owned(),
        command: Some("claude".into()),
        args: Some(vec![]),
        cols: 80,
        rows: 24,
        canonical_key: Some(key.clone()),
        ..Default::default()
    };
    let remote = c.remote;
    let (_, session) = tokio::task::spawn_blocking(move || remote.spawn_workspace_blocking(&req)).await.unwrap().unwrap();
    let pid = crate::agent_host::host().find_agent(&key).unwrap().child_pid().unwrap();
    // The link drops (the controller side of it, as on a restart).
    link1.abort();
    let _ = link1.await;
    within("lost", async {
        while c.remote.status_json()["lost"] != json!(["w3-host"]) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    // The agent keeps running on the host.
    assert_eq!(crate::agent_host::host().find_agent(&key).unwrap().child_pid(), Some(pid));
    // The host links again; its hello resumes the agent; adopted.
    let mut adopted = c.remote.hook_events(); // keep a receiver alive; adoption is checked through the table
    let link2 = tokio::spawn(async move { link::serve_once(&cfg, &link::Timings::PRODUCTION, || {}).await });
    within("adopted", async {
        while c.remote.status_json()["lost"] != json!([]) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let _ = adopted.try_recv();
    let ctl: &'static dyn crate::agent_host::AgentHost = c.remote;
    let again = ctl.find_agent(&key).expect("still placed");
    assert!(again.same_session(&session), "the same handle, not a new placement");
    // Ops reach it again: the same process.
    let typed = format!("w3-{}", &uuid::Uuid::new_v4().simple().to_string()[..6]);
    ctl.write(&again, format!("{typed}\r").into_bytes()).unwrap();
    until_sync("the write after resume", || ctl.read_screen(&again, 50).iter().any(|l| l.contains(&typed)).then_some(())).await;
    assert_eq!(crate::agent_host::host().find_agent(&key).unwrap().child_pid(), Some(pid));
    tokio::task::block_in_place(|| ctl.unplace(&key));
    link2.abort();
    let _ = std::fs::remove_file(&db);
}

#[test]
fn grid_frames_carry_their_version_as_the_ring_offset() {
    assert_eq!(link::frame_version(r#"{"event":"delta","payload":{"rows":24,"version":4211,"damagedRows":[]}}"#), Some(4211));
    assert_eq!(link::frame_version(r#"{"event":"label_initial","payload":{"label":"x"}}"#), None);
}

// ── W3b: byte streams ────────────────────────────────────────────────────

/// The next message of a `/cli/sessions/bytes` client: `Ok(text)` for a
/// text event, `Err(bytes)` for a binary chunk, `None` once closed.
async fn bytes_next(ws: &mut Ws) -> Option<Result<Value, Vec<u8>>> {
    within("a bytes message", async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(t))) => return Some(Ok(serde_json::from_str(&t).expect("json event"))),
                Some(Ok(Message::Binary(b))) => return Some(Err(b)),
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return None,
                Some(Ok(_)) => continue,
            }
        }
    })
    .await
}

/// The end of a byte stream from the host's side: the `error` event that
/// names why, then the close. Returns the message.
async fn bytes_end(ws: &mut Ws) -> String {
    let why = match bytes_next(ws).await {
        Some(Ok(v)) if v["event"] == "error" => v["payload"]["message"].as_str().unwrap().to_string(),
        other => panic!("expected the error event that ends the stream, got {other:?}"),
    };
    assert!(bytes_next(ws).await.is_none(), "closed after: {why}");
    why
}

/// Read binary chunks until `want` bytes arrived.
async fn bytes_until(ws: &mut Ws, want: usize) -> Vec<u8> {
    let mut got = Vec::new();
    while got.len() < want {
        match bytes_next(ws).await {
            Some(Err(b)) => got.extend(b),
            other => panic!("expected bytes ({} of {want} so far), got {other:?}", got.len()),
        }
    }
    got
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_byte_stream_relays_the_hosts_ring_from_the_clients_offset() {
    use base64::Engine;
    let b64 = |b: &[u8]| base64::engine::general_purpose::STANDARD.encode(b);
    let c = Controller::start().await;
    let mut host = fake_host(&c, "h-bytes").await;
    c.wait_hosts(1).await;
    let (session, epoch) = place_on(&c, &mut host, "agent-bytes").await;

    // From offset 5: the host acks the ring range, the bytes follow in order.
    let mut client = c.attach_bytes(&session, 5).await.expect("bytes upgrade");
    let stream = match recv(&mut host).await {
        Frame::BytesOpen { stream, session_id, epoch: e, from } => {
            assert_eq!((session_id.as_str(), e, from), (session.as_str(), epoch, 5));
            stream
        }
        other => panic!("expected bytes.open, got {other:?}"),
    };
    send(&mut host, &Frame::BytesAck { stream, start: 5, end: 11, error: None }).await;
    let ack = bytes_next(&mut client).await.unwrap().unwrap();
    assert_eq!(
        ack,
        json!({"event": "session:ack", "payload": {"sessionId": session, "fromOffset": 5, "currentFrontOffset": 5, "currentBackOffset": 11}})
    );
    send(&mut host, &Frame::BytesData { stream, offset: 5, b64: b64(b"world!") }).await;
    send(&mut host, &Frame::BytesData { stream, offset: 11, b64: b64(b"\x1b[1mlive") }).await;
    assert_eq!(bytes_until(&mut client, 14).await, b"world!\x1b[1mlive");
    // A chunk out of order is never passed on: the stream ends, on the host too.
    send(&mut host, &Frame::BytesData { stream, offset: 100, b64: b64(b"skipped") }).await;
    let err = bytes_next(&mut client).await.unwrap().unwrap();
    assert!(err["payload"]["message"].as_str().unwrap().contains("reconnect with from=19"), "{err}");
    assert!(bytes_next(&mut client).await.is_none());
    assert!(matches!(recv(&mut host).await, Frame::BytesClose { stream: s, .. } if s == stream));

    // A host without a byte ring: an error event, then the close.
    let mut client = c.attach_bytes(&session, 0).await.expect("bytes upgrade");
    let stream = match recv(&mut host).await {
        Frame::BytesOpen { stream, .. } => stream,
        other => panic!("expected bytes.open, got {other:?}"),
    };
    send(&mut host, &Frame::BytesAck { stream, start: 0, end: 0, error: Some(super::byte_ring::NO_RING.into()) }).await;
    let err = bytes_next(&mut client).await.unwrap().unwrap();
    assert_eq!(err["payload"]["message"], super::byte_ring::NO_RING);
    assert!(bytes_next(&mut client).await.is_none());
    assert!(matches!(recv(&mut host).await, Frame::BytesClose { stream: s, .. } if s == stream));

    // The host's link drops: the open stream ends, and a new one is refused
    // before the upgrade (the session stays placed, the host is lost).
    let mut client = c.attach_bytes(&session, 0).await.expect("bytes upgrade");
    assert!(matches!(recv(&mut host).await, Frame::BytesOpen { .. }));
    drop(host);
    assert!(bytes_end(&mut client).await.contains("host link closed"));
    within("lost", async {
        while c.remote.status_json()["lost"] != json!(["h-bytes"]) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let refused = c.attach_bytes(&session, 0).await.expect_err("refused before the upgrade");
    assert!(refused.contains("400"), "{refused}");
}

/// The test ring: per session, the offset of its first byte and the bytes.
/// Process-wide (the source is), so it answers only its own sessions.
static TEST_RING: parking_lot::Mutex<Option<std::collections::HashMap<String, (u64, Vec<u8>)>>> = parking_lot::Mutex::new(None);

fn test_ring(session: &str, from: u64, max: usize) -> Result<super::byte_ring::RingRead, String> {
    let g = TEST_RING.lock();
    let Some((front, data)) = g.as_ref().and_then(|m| m.get(session)) else {
        return Err(super::byte_ring::NO_RING.to_string());
    };
    let end = front + data.len() as u64;
    let start = from.max(*front).min(end);
    let lo = (start - front) as usize;
    let hi = (lo + max).min(data.len());
    Ok(super::byte_ring::RingRead { start, end, bytes: data[lo..hi].to_vec() })
}

fn ring_append(session: &str, bytes: &[u8]) {
    TEST_RING.lock().get_or_insert_with(Default::default).entry(session.to_string()).or_default().1.extend_from_slice(bytes);
}

/// S9 for bytes: a client reads a session's output, the controller side of
/// the link goes (as on a controller restart) while the agent keeps
/// printing, the host links again, and the client, reconnecting with
/// `from` = its next byte, gets exactly the bytes it missed, then the live
/// tail: no gap, no duplicate. Real host link (`link::serve_once`); the
/// ring is the test's (the host's byte-ring source).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reconnecting_byte_client_gets_exactly_the_bytes_it_missed() {
    let _link = real_link();
    super::byte_ring::set(Some(test_ring));
    let session = uuid::Uuid::new_v4().to_string();
    ring_append(&session, b"\x1b[2Jbanner\r\n> ");
    let c = Controller::start().await;
    let host_name = format!("w3b-{}", &session[..8]);
    let cfg = HostConfig { attach_url: c.attach_url.clone(), token: TOKEN.into(), host_name: host_name.clone(), labels: vec![] };
    let link_cfg = cfg.clone();
    let link1 = tokio::spawn(async move { link::serve_once(&link_cfg, &link::Timings::PRODUCTION, || {}).await });
    c.wait_hosts(1).await;
    // Placed on that host (adopted, as after a controller restart).
    c.remote.adopt(&host_name, &[resumed("agent-w3b", &session, 7)]);

    let mut client = c.attach_bytes(&session, 0).await.expect("bytes upgrade");
    let ack = bytes_next(&mut client).await.unwrap().unwrap();
    assert_eq!((ack["payload"]["currentFrontOffset"].as_u64(), ack["payload"]["currentBackOffset"].as_u64()), (Some(0), Some(14)));
    let mut seen = bytes_until(&mut client, 14).await;
    ring_append(&session, b"w3b prompt\r\n");
    seen.extend(bytes_until(&mut client, 12).await);

    // The controller side goes; the agent keeps printing.
    link1.abort();
    let _ = link1.await;
    let why = bytes_end(&mut client).await;
    assert!(why.contains("byte stream ended at offset 26"), "the stream ends with the link: {why}");
    ring_append(&session, b"[k2-fake-agent] reply #1: w3b prompt\r\n> ");
    let link2 = tokio::spawn(async move { link::serve_once(&cfg, &link::Timings::PRODUCTION, || {}).await });
    within("relinked", async {
        while c.remote.status_json()["lost"] != json!([]) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    // The real host doesn't run this session, so its hello drops it (gone);
    // place it again as an adopting controller would.
    c.remote.adopt(&host_name, &[resumed("agent-w3b", &session, 7)]);

    let from = seen.len() as u64;
    let mut client = c.attach_bytes(&session, from).await.expect("bytes upgrade");
    let ack = bytes_next(&mut client).await.unwrap().unwrap();
    assert_eq!(ack["payload"]["fromOffset"].as_u64(), Some(from));
    assert_eq!(ack["payload"]["currentFrontOffset"].as_u64(), Some(from), "no gap: the ring still has every byte after {from}");
    let missed = b"[k2-fake-agent] reply #1: w3b prompt\r\n> ";
    seen.extend(bytes_until(&mut client, missed.len()).await);
    ring_append(&session, b"after");
    seen.extend(bytes_until(&mut client, 5).await);
    let all = TEST_RING.lock().as_ref().unwrap()[&session].1.clone();
    assert_eq!(seen, all, "exactly the session's bytes, once each, in order");
    drop(client);
    link2.abort();
}

/// The ring dropped bytes a stream hadn't sent: the stream ends with the
/// gap named, never skipping it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ring_overrun_ends_the_byte_stream_instead_of_skipping() {
    let _link = real_link();
    super::byte_ring::set(Some(test_ring));
    let session = uuid::Uuid::new_v4().to_string();
    ring_append(&session, b"0123456789");
    let c = Controller::start().await;
    let host_name = format!("w3b-{}", &session[..8]);
    let cfg = HostConfig { attach_url: c.attach_url.clone(), token: TOKEN.into(), host_name: host_name.clone(), labels: vec![] };
    let link = tokio::spawn(async move { link::serve_once(&cfg, &link::Timings::PRODUCTION, || {}).await });
    c.wait_hosts(1).await;
    c.remote.adopt(&host_name, &[resumed("agent-overrun", &session, 3)]);
    let mut client = c.attach_bytes(&session, 0).await.expect("bytes upgrade");
    assert!(matches!(bytes_next(&mut client).await, Some(Ok(_))));
    assert_eq!(bytes_until(&mut client, 10).await, b"0123456789");
    // The ring moves past what was sent: its front is now 20.
    {
        let mut g = TEST_RING.lock();
        let e = g.as_mut().unwrap().get_mut(&session).unwrap();
        *e = (20, b"KLMN".to_vec());
    }
    let why = bytes_end(&mut client).await;
    assert!(why.contains("ring overrun: bytes [10, 20) were dropped"), "{why}");
    // Reconnecting from 10 shows the gap in the ack: bytes resume at 20.
    let mut client = c.attach_bytes(&session, 10).await.expect("bytes upgrade");
    let ack = bytes_next(&mut client).await.unwrap().unwrap();
    assert_eq!((ack["payload"]["fromOffset"].as_u64(), ack["payload"]["currentFrontOffset"].as_u64()), (Some(10), Some(20)));
    assert_eq!(bytes_until(&mut client, 4).await, b"KLMN");
    link.abort();
}

// ── P3 T2 phase A: placement through the provisioner ─────────────────────

/// A fake host that said hello as `name` with `labels`.
async fn fake_host_labeled(c: &Controller, name: &str, labels: &[&str]) -> Ws {
    let (mut ws, _) = tokio_tungstenite::connect_async(c.attach_url.as_str()).await.expect("dial");
    let labels = labels.iter().map(|l| l.to_string()).collect();
    send(&mut ws, &Frame::Hello { protocol: PROTOCOL, host_name: name.into(), token: TOKEN.into(), labels, resume: vec![] }).await;
    assert!(matches!(recv(&mut ws).await, Frame::Welcome { .. }));
    ws
}

/// Close `agent` on its (fake) host, as a sleep does.
async fn close_on(c: &Controller, host: &mut Ws, agent: &str) {
    let remote = c.remote;
    let body = json!({"agent_name": agent, "force": true}).to_string();
    let close = tokio::spawn(async move { remote.close(body.as_bytes()).await });
    let req = match recv(host).await {
        Frame::Close { req, agent_id, .. } => {
            assert_eq!(agent_id, agent);
            req
        }
        other => panic!("expected close, got {other:?}"),
    };
    send(host, &Frame::Reply { req, status: "200 OK".into(), body: json!({"closed": true}).to_string() }).await;
    within("the close answer", close).await.unwrap();
    assert!(c.remote.placement(agent).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wake_goes_back_to_the_last_host_not_the_emptiest() {
    let c = Controller::start().await;
    let mut a = fake_host(&c, "h-a").await;
    let mut b = fake_host(&c, "h-b").await;
    c.wait_hosts(2).await;
    // bob's first placement: both empty, by name.
    place(&c, &mut a, "bob", &uuid::Uuid::new_v4().to_string()).await;
    let row = c.remote.policy().get("bob").unwrap();
    assert_eq!((row.mode, row.last_host.as_deref()), (crate::provisioner::Mode::Portable, Some("h-a")));
    close_on(&c, &mut a, "bob").await; // asleep
    // h-a now runs another agent; h-b is emptier.
    place(&c, &mut a, "other", &uuid::Uuid::new_v4().to_string()).await;
    // The wake goes back to h-a, where bob's home is.
    place(&c, &mut a, "bob", &uuid::Uuid::new_v4().to_string()).await;
    // A new agent still goes to the emptiest host.
    place(&c, &mut b, "carol", &uuid::Uuid::new_v4().to_string()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_laptop_agent_is_pinned_and_waits_for_its_away_laptop() {
    let c = Controller::start().await;
    let mut laptop = fake_host_labeled(&c, "eric-mbp", &["k2.kind=laptop"]).await;
    c.wait_hosts(1).await;
    place(&c, &mut laptop, "notes", &uuid::Uuid::new_v4().to_string()).await;
    let row = c.remote.policy().get("notes").unwrap();
    assert_eq!(row.mode, crate::provisioner::Mode::Pinned { host: "eric-mbp".into() }, "first placed on a laptop: pinned (D5)");
    close_on(&c, &mut laptop, "notes").await;
    // A server host links; the laptop's lid closes (its link drops).
    let mut server = fake_host(&c, "h-1").await;
    c.wait_hosts(2).await;
    drop(laptop);
    c.wait_hosts(1).await;
    let status = c.remote.status_json();
    assert_eq!(status["away"], json!(["eric-mbp"]), "{status}");
    assert_eq!(status["provisioner"], "static");
    // A wake of notes: refused with the reason, never placed on h-1.
    let (st, body) = c.remote.spawn(br#"{"agent_name":"notes","command":"claude"}"#).await;
    assert_eq!(st, "503 Service Unavailable", "{body}");
    assert!(body.contains("pinned to host eric-mbp") && body.contains("never placed elsewhere"), "{body}");
    assert!(c.remote.placement("notes").is_none());
    // The workspace spawn (the --wake / heartbeat / app path) is refused the same way.
    let remote = c.remote;
    let req = crate::spawn::SpawnWorkspaceSessionRequest {
        agent_name: "notes".into(),
        cwd: "/w/notes".into(),
        canonical_key: Some("notes".into()),
        ..Default::default()
    };
    let err = tokio::task::spawn_blocking(move || remote.spawn_workspace_blocking(&req).err()).await.unwrap().expect("refused");
    assert!(err.contains("pinned to host eric-mbp"), "{err}");
    // Another agent still goes to h-1.
    place(&c, &mut server, "bob", &uuid::Uuid::new_v4().to_string()).await;
    // The laptop opens: notes goes back to it.
    let mut laptop = fake_host_labeled(&c, "eric-mbp", &["k2.kind=laptop"]).await;
    c.wait_hosts(2).await;
    assert_eq!(c.remote.status_json()["away"], json!([]));
    place(&c, &mut laptop, "notes", &uuid::Uuid::new_v4().to_string()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_host_refuses_naming_k2_host_max_awake() {
    let c = Controller::start().await;
    let mut h = fake_host_labeled(&c, "h-small", &["k2.max_awake=1", "pool=default"]).await;
    c.wait_hosts(1).await;
    place(&c, &mut h, "first", &uuid::Uuid::new_v4().to_string()).await;
    let (st, body) = c.remote.spawn(br#"{"agent_name":"second","command":"claude"}"#).await;
    assert_eq!(st, "503 Service Unavailable", "{body}");
    assert!(body.contains("K2_HOST_MAX_AWAKE"), "{body}");
    let host = &c.remote.status_json()["hosts"][0];
    assert_eq!((host["max_awake"].as_u64(), host["kind"].as_str()), (Some(1), Some("server")), "{host}");
    assert_eq!(host["labels"], json!(["k2.max_awake=1", "pool=default"]));
    // Sleeping the first makes room.
    close_on(&c, &mut h, "first").await;
    place(&c, &mut h, "second", &uuid::Uuid::new_v4().to_string()).await;
}

// ── P3 phase A follow-ups: enrolled hosts, the away list, adoption ───────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enrolled_hosts_count_as_away_until_they_link_or_are_revoked() {
    let c = Controller::start().await;
    c.remote.set_enrolled(["vm-2".to_string()].into());
    assert_eq!(c.remote.status_json()["away"], json!(["vm-2"]), "enrolled, never linked");
    let _vm2 = fake_host(&c, "vm-2").await;
    c.wait_hosts(1).await;
    assert_eq!(c.remote.status_json()["away"], json!([]));
    // Revoked (the enroll route pushes the new set): no longer known.
    c.remote.set_enrolled(Default::default());
    assert_eq!(c.remote.status_json()["away"], json!([]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_seen_only_as_a_last_host_is_forgotten_a_pinned_one_is_not() {
    // Forget at once: only what still names a host keeps it away.
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new().with_away_forget(Duration::ZERO)));
    let c = Controller::start_on(remote).await;
    let mut server = fake_host(&c, "h-old").await;
    let mut laptop = fake_host_labeled(&c, "eric-mbp", &["k2.kind=laptop"]).await;
    c.wait_hosts(2).await;
    // Both empty: by name first (eric-mbp), then the emptier (h-old).
    place(&c, &mut laptop, "notes", &uuid::Uuid::new_v4().to_string()).await;
    place(&c, &mut server, "bob", &uuid::Uuid::new_v4().to_string()).await;
    close_on(&c, &mut server, "bob").await;
    close_on(&c, &mut laptop, "notes").await;
    drop(server);
    drop(laptop);
    c.wait_hosts(0).await;
    // bob's policy still names h-old as its last host; notes is pinned to eric-mbp.
    assert_eq!(c.remote.policy().get("bob").unwrap().last_host.as_deref(), Some("h-old"));
    assert_eq!(c.remote.status_json()["away"], json!(["eric-mbp"]), "h-old is forgotten; the laptop notes waits for is not");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adopted_agents_from_before_the_table_are_portable_on_servers_pinned_on_laptops() {
    let c = Controller::start().await;
    let labeled_hello = |name: &'static str, labels: Vec<String>, resume| {
        let url = c.attach_url.clone();
        async move {
            let (mut ws, _) = tokio_tungstenite::connect_async(url.as_str()).await.expect("dial");
            send(&mut ws, &Frame::Hello { protocol: PROTOCOL, host_name: name.into(), token: TOKEN.into(), labels, resume }).await;
            assert!(matches!(recv(&mut ws).await, Frame::Welcome { .. }));
            ws
        }
    };
    let _server = labeled_hello("h-1", vec![], vec![resumed("old-server", &uuid::Uuid::new_v4().to_string(), 5)]).await;
    let _laptop =
        labeled_hello("eric-mbp", vec!["k2.kind=laptop".into()], vec![resumed("old-laptop", &uuid::Uuid::new_v4().to_string(), 6)]).await;
    c.wait_hosts(2).await;
    let server = c.remote.policy().get("old-server").expect("adopted");
    assert_eq!((server.mode, server.last_host.as_deref()), (crate::provisioner::Mode::Portable, Some("h-1")));
    let laptop = c.remote.policy().get("old-laptop").expect("adopted");
    assert_eq!(laptop.mode, crate::provisioner::Mode::Pinned { host: "eric-mbp".into() });
}

// ── P4 T1: the Kubernetes provisioner through RemoteHost ─────────────────

/// Mints `k2e_test_<n>`; nothing is enrolled.
#[derive(Default)]
struct TestEnroll(parking_lot::Mutex<Vec<String>>);

impl crate::provisioner::kubernetes::Enroller for TestEnroll {
    fn enrolled(&self, _host_name: &str) -> bool {
        false
    }
    fn mint(&self, _ttl_ms: i64) -> Result<String, String> {
        let t = format!("k2e_test_{}", self.0.lock().len());
        self.0.lock().push(t.clone());
        Ok(t)
    }
}

/// A controller whose provisioner is `kubernetes` on the fake API server,
/// `replicas` pods in `k2-host`, at most `max`.
async fn k8s_controller(replicas: u32, max: u32, idle_after: Duration) -> (Controller, crate::k8s::fake::FakeApi) {
    use crate::provisioner::kubernetes::{K8sPool, Kubernetes, PoolSettings};
    let api = crate::k8s::fake::FakeApi::start();
    api.state.lock().replicas.insert("k2-host".into(), replicas);
    api.state.lock().secrets.insert("k2-host-enroll".into(), Default::default());
    let settings = PoolSettings { max_hosts: max, idle_after, ..PoolSettings::default() };
    let pool = K8sPool { client: api.client(), statefulset: "k2-host".into(), enroll_secret: "k2-host-enroll".into() };
    let k = Kubernetes::new(settings, Box::new(pool), std::sync::Arc::new(TestEnroll::default()));
    let remote: &'static RemoteHost = Box::leak(Box::new(
        RemoteHost::new()
            .with_placement(Box::new(k), std::sync::Arc::new(crate::provisioner::policy::PolicyStore::in_memory()))
            .with_host_wait(DEADLINE),
    ));
    (Controller::start_on(remote).await, api)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kubernetes_a_third_wake_scales_up_and_lands_on_the_new_hosts_hello() {
    let (c, api) = k8s_controller(1, 2, Duration::from_secs(900)).await;
    let mut h0 = fake_host_labeled(&c, "k2-host-0", &["k2.max_awake=2"]).await;
    c.wait_hosts(1).await;
    place(&c, &mut h0, "a1", &uuid::Uuid::new_v4().to_string()).await;
    place(&c, &mut h0, "a2", &uuid::Uuid::new_v4().to_string()).await;
    // The third wake: k2-host-0 is at k2.max_awake=2.
    let remote = c.remote;
    let third = tokio::spawn(async move { remote.spawn(br#"{"agent_name":"a3","command":"claude"}"#).await });
    within("the scale-up", async {
        while api.state.lock().replicas["k2-host"] != 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let calls = api.calls();
    let secret = calls.iter().position(|(m, p)| m == "PATCH" && p.ends_with("/secrets/k2-host-enroll")).expect("token put");
    let scale = calls.iter().position(|(m, p)| m == "PATCH" && p.ends_with("/statefulsets/k2-host/scale")).expect("scaled");
    assert!(secret < scale, "the token before the scale: {calls:?}");
    assert!(api.state.lock().secrets["k2-host-enroll"].contains_key("k2-host-1"));
    // The pod reads Ready: the need is not retried on that.
    api.state.lock().pods = vec![crate::k8s::fake::pod_json("k2-host-1", "Running", true, Some("node-b"))];
    assert!(c.remote.placement("a3").is_none());
    // k2-host-1's hello: the waiting need is placed there.
    let mut h1 = fake_host_labeled(&c, "k2-host-1", &["k2.max_awake=2"]).await;
    let req = match recv(&mut h1).await {
        Frame::Spawn { req, agent_id, .. } => {
            assert_eq!(agent_id, "a3");
            req
        }
        other => panic!("expected the waiting spawn on k2-host-1, got {other:?}"),
    };
    let session = uuid::Uuid::new_v4().to_string();
    send(&mut h1, &Frame::Reply { req, status: "200 OK".into(), body: json!({"sessionId": session, "reused": false}).to_string() }).await;
    let (st, body) = within("the third wake", third).await.unwrap();
    assert_eq!(st, "200 OK", "{body}");
    assert_eq!(c.remote.placement("a3").unwrap().host_id, "k2-host-1");
    // At the maximum (2 hosts, both full after a4): refused naming the limit.
    place(&c, &mut h1, "a4", &uuid::Uuid::new_v4().to_string()).await;
    let (st, body) = c.remote.spawn(br#"{"agent_name":"a5","command":"claude"}"#).await;
    assert_eq!(st, "503 Service Unavailable", "{body}");
    assert!(body.contains("K2_PROVISIONER_MAX_HOSTS=2"), "{body}");
    assert_eq!(api.state.lock().replicas["k2-host"], 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kubernetes_idle_drains_the_highest_ordinal_releases_its_agents_then_scales_down() {
    // An idle window of 0: the first tick with spare room drains.
    let (c, api) = k8s_controller(2, 3, Duration::ZERO).await;
    let mut h0 = fake_host_labeled(&c, "k2-host-0", &["k2.max_awake=2"]).await;
    let mut h1 = fake_host_labeled(&c, "k2-host-1", &["k2.max_awake=2"]).await;
    c.wait_hosts(2).await;
    place(&c, &mut h0, "a0", &uuid::Uuid::new_v4().to_string()).await;
    place(&c, &mut h1, "a1", &uuid::Uuid::new_v4().to_string()).await;
    // a1 fits on k2-host-0: drain k2-host-1, which releases a1 (a close).
    let remote = c.remote;
    let tick = tokio::task::spawn_blocking(move || remote.idle_tick(std::time::Instant::now()));
    let req = match recv(&mut h1).await {
        Frame::Close { req, agent_id, .. } => {
            assert_eq!(agent_id, "a1");
            req
        }
        other => panic!("expected a1's release, got {other:?}"),
    };
    send(&mut h1, &Frame::Reply { req, status: "200 OK".into(), body: json!({"closed": true}).to_string() }).await;
    assert_eq!(within("the drain", tick).await.unwrap(), crate::provisioner::IdleStep::Drain("k2-host-1".into()));
    assert!(c.remote.placement("a1").is_none());
    assert_eq!(api.state.lock().replicas["k2-host"], 2, "not before its agents are gone");
    // While draining it takes nothing new: a1's wake goes to k2-host-0.
    place(&c, &mut h0, "a1", &uuid::Uuid::new_v4().to_string()).await;
    let remote = c.remote;
    let step = tokio::task::spawn_blocking(move || remote.idle_tick(std::time::Instant::now())).await.unwrap();
    assert_eq!(step, crate::provisioner::IdleStep::ScaledDown { host: "k2-host-1".into(), to: 1 });
    assert_eq!(api.state.lock().replicas["k2-host"], 1);
}

// ── P3 follow-up 2: a host's S8 drain reaches placement ──────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_that_says_it_is_draining_gets_no_new_agent() {
    let c = Controller::start().await;
    let mut a = fake_host(&c, "h-a").await;
    let mut b = fake_host(&c, "h-b").await;
    c.wait_hosts(2).await;
    // h-a (as empty as h-b, and first by name) starts its SIGTERM drain.
    send(&mut a, &Frame::Refused { code: super::drain::DRAINING_CODE.into(), message: "host is draining (SIGTERM)".into() }).await;
    within("h-a draining", async {
        while c.remote.status_json()["hosts"].as_array().unwrap().iter().all(|h| !(h["host_id"] == "h-a" && h["draining"] == true)) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    // A new agent goes to h-b, never to the draining h-a.
    place(&c, &mut b, "fresh", &uuid::Uuid::new_v4().to_string()).await;
    assert_eq!(c.remote.placement("fresh").unwrap().host_id, "h-b");
}

// ── K2-a (G2 chaos): "no host linked yet" is transient ───────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_need_waits_for_a_known_host_to_link_instead_of_failing() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new().with_link_grace(DEADLINE)));
    let c = Controller::start_on(remote).await;
    let h = fake_host(&c, "h-1").await;
    c.wait_hosts(1).await;
    drop(h); // as across a controller restart: known, not linked yet
    c.wait_hosts(0).await;
    let spawn = tokio::spawn(async move { remote.spawn(br#"{"agent_name":"early","cwd":"/w/early","command":"claude"}"#).await });
    // It waits (not refused, not placed) before the host is back.
    waiting_for_a_host(remote).await;
    assert!(!spawn.is_finished(), "waiting, not answered");
    // The host links back: the waiting spawn lands on it.
    let mut h = fake_host(&c, "h-1").await;
    let req = match recv(&mut h).await {
        Frame::Spawn { req, agent_id, .. } => {
            assert_eq!(agent_id, "early");
            req
        }
        other => panic!("expected the waiting spawn, got {other:?}"),
    };
    let session = uuid::Uuid::new_v4().to_string();
    send(&mut h, &Frame::Reply { req, status: "200 OK".into(), body: json!({"sessionId": session, "reused": false}).to_string() }).await;
    let (st, body) = within("the waiting spawn", spawn).await.unwrap();
    assert_eq!(st, "200 OK", "{body}");
}

/// Until a need waits on `remote` for a host's hello.
async fn waiting_for_a_host(remote: &'static RemoteHost) {
    within("a need waiting for a host", async {
        while remote.hello_waiters() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_need_no_host_links_back_for_is_a_503_once_the_grace_is_over() {
    let grace = Duration::from_millis(300);
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new().with_link_grace(grace)));
    let c = Controller::start_on(remote).await;
    let h = fake_host(&c, "h-1").await;
    c.wait_hosts(1).await;
    drop(h);
    c.wait_hosts(0).await;
    let started = std::time::Instant::now();
    let spawn = tokio::spawn(async move { remote.spawn(br#"{"agent_name":"never","cwd":"/w/never","command":"claude"}"#).await });
    waiting_for_a_host(remote).await;
    let (st, body) = within("the grace running out", spawn).await.unwrap();
    assert_eq!(st, "503 Service Unavailable", "{body}");
    assert!(body.contains("no agent host"), "{body}");
    assert!(started.elapsed() >= grace, "it waited the grace out first");
    assert_eq!(remote.hello_waiters(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn past_the_link_grace_no_host_is_a_retryable_503_on_every_path() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new().with_link_grace(Duration::ZERO)));
    let c = Controller::start_on(remote).await;
    let h = fake_host(&c, "h-1").await;
    c.wait_hosts(1).await;
    drop(h);
    c.wait_hosts(0).await;
    let (st, body) = c.remote.spawn(br#"{"agent_name":"late","command":"claude"}"#).await;
    assert_eq!(st, "503 Service Unavailable", "{body}");
    assert!(body.contains("no agent host"), "{body}");
    // The workspace spawn (ensure-pinned-chat, wake, heartbeat) says the same, marked retryable.
    let req = crate::spawn::SpawnWorkspaceSessionRequest {
        agent_name: "late".into(),
        cwd: "/w/late".into(),
        canonical_key: Some("late".into()),
        ..Default::default()
    };
    let err = tokio::task::spawn_blocking(move || remote.spawn_workspace_blocking(&req).err()).await.unwrap().expect("refused");
    assert!(crate::agent_host::remote::is_host_unavailable(&err), "{err}");
    assert!(err.contains("no agent host"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_controller_that_never_knew_a_host_refuses_at_once() {
    // A long grace: if it waited, the test's deadline would trip.
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new().with_link_grace(Duration::from_secs(600))));
    let (st, body) = within("an immediate answer", remote.spawn(br#"{"agent_name":"x","command":"claude"}"#)).await;
    assert_eq!(st, "503 Service Unavailable", "{body}");
}
