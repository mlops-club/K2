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
                    // What the gate does: the query decides the client
                    // (`gate::grid_client`) and names the session.
                    let mut peek = [0u8; 4096];
                    let n = s.peek(&mut peek).await.unwrap();
                    let head = String::from_utf8_lossy(&peek[..n]).to_string();
                    let target = head.split_whitespace().nth(1).unwrap_or("").to_string();
                    let query = target.split_once('?').map(|(_, q)| q).unwrap_or("").to_string();
                    let params = crate::routes::http::parse_params("/cli/sessions/grid", &query);
                    let session = params.get("session").cloned().unwrap_or_default();
                    match super::gate::grid_client(&query, OWNER_TOKEN) {
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
        Frame::GridOpen { stream, session_id, epoch: e, proto, as_user } => {
            assert_eq!((session_id.as_str(), e, proto, as_user), ("sess-s1", epoch, None, None));
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
    assert!(err(2).starts_with("read: "), "{events:?}");
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
