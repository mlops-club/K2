//! G2 fixes on loopback sockets, against fake hosts written here frame by
//! frame: placement affinity (S7: a re-placed agent goes back to the host
//! that holds its home) and the host drain (S8).
//!
//! Deadlines only stop a hung test; nothing asserts a duration.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use super::hub;
use super::proto::{Frame, ATTACH_PATH, PROTOCOL};
use crate::agent_host::remote::{PlacementStore, RemoteHost};

pub(super) type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

const DEADLINE: Duration = Duration::from_secs(30);
pub(super) const TOKEN: &str = "test-bootstrap-token";

pub(super) async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(DEADLINE, f).await.unwrap_or_else(|_| panic!("timed out waiting for {what}"))
}

pub(super) async fn send(ws: &mut Ws, f: &Frame) {
    ws.send(Message::Text(f.encode())).await.expect("send frame");
}

pub(super) async fn recv(ws: &mut Ws) -> Frame {
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

/// The hub on loopback over `remote`; returns the attach URL.
pub(super) async fn serve(remote: &'static RemoteHost) -> String {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}{ATTACH_PATH}", l.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((s, _)) = l.accept().await {
            tokio::spawn(hub::serve_attach(s, remote, TOKEN));
        }
    });
    url
}

pub(super) async fn fake_host(url: &str, name: &str) -> Ws {
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.expect("dial");
    send(&mut ws, &Frame::Hello { protocol: PROTOCOL, host_name: name.into(), token: TOKEN.into(), labels: vec![], resume: vec![] }).await;
    assert!(matches!(recv(&mut ws).await, Frame::Welcome { .. }));
    ws
}

pub(super) async fn wait_hosts(remote: &RemoteHost, n: usize) {
    within("hosts to link", async {
        while remote.status_json()["hosts"].as_array().map(Vec::len) != Some(n) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

/// Spawn `agent` (cwd `cwd`) through the controller; `host` is the fake
/// host the test expects the spawn on, which answers with `session`.
/// Returns the controller's answer body.
pub(super) async fn spawn_on(remote: &'static RemoteHost, host: &mut Ws, agent: &str, cwd: &str, session: &str) -> Value {
    let body = json!({"agent_name": agent, "cwd": cwd, "command": "claude"}).to_string();
    let spawn = tokio::spawn(async move { remote.spawn(body.as_bytes()).await });
    match recv(host).await {
        Frame::Spawn { req, agent_id, .. } => {
            assert_eq!(agent_id, agent);
            let reply = json!({"sessionId": session, "reused": false}).to_string();
            send(host, &Frame::Reply { req, status: "200 OK".into(), body: reply }).await;
        }
        other => panic!("expected spawn, got {other:?}"),
    }
    let (status, body) = within("the spawn answer", spawn).await.unwrap();
    assert_eq!(status, "200 OK", "{body}");
    serde_json::from_str(&body).unwrap()
}

pub(super) fn sid(n: u8) -> String {
    format!("00000000-0000-4000-8000-0000000000{n:02}")
}

pub(super) async fn gone(host: &mut Ws, remote: &RemoteHost, agent: &str) {
    send(host, &Frame::SessionGone { agent_id: agent.into(), epoch: None, exit_code: Some(137) }).await;
    within("the placement to go", async {
        while remote.placement(agent).is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_re_placed_agent_goes_back_to_the_host_with_its_home_even_when_another_is_less_loaded() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "s7", "/w/s7", &sid(1)).await;
    spawn_on(remote, &mut a, "other", "/w/other", &sid(2)).await;
    let mut b = fake_host(&url, "host-b").await;
    wait_hosts(remote, 2).await;

    // The CLI dies (S7). host-b now has fewer placements, but s7's
    // conversation is on host-a.
    gone(&mut a, remote, "s7").await;
    let answer = spawn_on(remote, &mut a, "s7", "/w/s7", &sid(3)).await;
    assert_eq!(remote.placement("s7").unwrap().host_id, "host-a");
    assert!(answer.get("placementNote").is_none(), "{answer}");

    // A new agent with no home anywhere still goes by load.
    spawn_on(remote, &mut b, "fresh", "/w/fresh", &sid(4)).await;
    assert_eq!(remote.placement("fresh").unwrap().host_id, "host-b");

    // A new agent in a cwd host-a ran follows the cwd (its transcripts).
    gone(&mut a, remote, "other").await;
    spawn_on(remote, &mut a, "other-sidecar", "/w/other", &sid(5)).await;
    assert_eq!(remote.placement("other-sidecar").unwrap().host_id, "host-a");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn when_the_home_host_is_away_the_agent_is_placed_by_load_and_the_answer_says_so() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "s7", "/w/s7", &sid(1)).await;
    gone(&mut a, remote, "s7").await;
    drop(a);
    let mut b = fake_host(&url, "host-b").await;
    wait_hosts(remote, 1).await;

    let answer = spawn_on(remote, &mut b, "s7", "/w/s7", &sid(2)).await;
    assert_eq!(remote.placement("s7").unwrap().host_id, "host-b");
    let note = answer["placementNote"].as_str().unwrap_or_default();
    assert!(note.contains("home is on host host-a, which is not linked") && note.contains("placed on host-b"), "{answer}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_home_host_is_remembered_across_a_controller_restart() {
    let path = k2_core::test_env::unique_temp_path("affinity.db");
    let open = || {
        let store = Arc::new(PlacementStore::open_file(&path).unwrap());
        let r: &'static RemoteHost = Box::leak(Box::new(RemoteHost::with_store(store, None)));
        r
    };
    {
        let remote = open();
        let url = serve(remote).await;
        let mut a = fake_host(&url, "host-a").await;
        spawn_on(remote, &mut a, "s7", "/w/s7", &sid(1)).await;
        gone(&mut a, remote, "s7").await;
    }
    // A new controller on the same database: host-a holds two other agents,
    // host-b none, and s7 still goes to host-a.
    let remote = open();
    remote.restore();
    let url = serve(remote).await;
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "x1", "/w/x1", &sid(2)).await;
    let _b = fake_host(&url, "host-b").await;
    wait_hosts(remote, 2).await;
    spawn_on(remote, &mut a, "s7", "/w/s7", &sid(3)).await;
    assert_eq!(remote.placement("s7").unwrap().host_id, "host-a");
    std::fs::remove_file(&path).unwrap();
}

// ── S8: a draining host, its agents resumed, their messages kept ───────

fn mem_db() -> crate::delivery_drain::Db {
    Arc::new(parking_lot::ReentrantMutex::new(rusqlite::Connection::open_in_memory().unwrap()))
}

const FAST: k2_core::delivery::Policy = k2_core::delivery::Policy {
    backoff_base_ms: 100,
    backoff_max_ms: 400,
    max_attempts: 1_000,
    ttl_ms: 3_600_000,
    keep_finished_ms: 3_600_000,
    seen_cap: 1_000,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drained_agent_is_resumed_when_its_host_is_back_and_its_messages_wait_for_it() {
    use crate::delivery_drain::{start_controller, Direct};
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    // Every address resolves on this controller, as a workspace would: an
    // unplaced one would take the direct path, unless it awaits a resume.
    let outbox = start_controller(
        remote,
        mem_db(),
        FAST,
        Direct {
            resolves: Box::new(|_| true),
            aliases: Box::new(|_| Vec::new()),
            deliver: Box::new(|_, _, _| panic!("a drained agent's message never takes the direct path")),
        },
    );
    super::drain::ensure_controller_resumes_with(remote, Arc::new(super::drain::resume_v2));
    let mut a = fake_host(&url, "host-a").await;
    let body = json!({"agent_name": "bob", "cwd": "/w/bob", "command": "claude", "args": ["--session-id", "conv-1"]}).to_string();
    let spawn = tokio::spawn(async move { remote.spawn(body.as_bytes()).await });
    let Frame::Spawn { req, .. } = recv(&mut a).await else { panic!("expected spawn") };
    send(&mut a, &Frame::Reply { req, status: "200 OK".into(), body: json!({"sessionId": sid(1), "reused": false}).to_string() }).await;
    assert_eq!(within("spawn", spawn).await.unwrap().0, "200 OK");
    let mut b = fake_host(&url, "host-b").await;
    wait_hosts(remote, 2).await;

    // SIGTERM on host-a: it says it is draining. New agents go elsewhere.
    send(&mut a, &Frame::Refused { code: super::drain::DRAINING_CODE.into(), message: "draining".into() }).await;
    within("the controller to see the drain", async {
        while remote.status_json()["hosts"][0]["draining"] != json!(true) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    // carol's cwd was last run on host-a (bob's), which would win; host-a
    // is draining, so it lands on host-b.
    spawn_on(remote, &mut b, "carol", "/w/bob", &sid(2)).await;
    assert_eq!(remote.placement("carol").unwrap().host_id, "host-b");

    // bob's turn ended: host-a releases it, then stops it and exits.
    send(&mut a, &Frame::Released { req: 0, agent_id: "bob".into(), epoch: 0, ring_end: None }).await;
    within("bob's resume to be recorded", async {
        while !remote.awaiting_resume("bob") {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(remote.status_json()["resuming"], json!(["bob"]));
    assert!(remote.placement("bob").is_none());
    send(&mut a, &Frame::SessionGone { agent_id: "bob".into(), epoch: None, exit_code: None }).await;
    drop(a);
    wait_hosts(remote, 1).await;

    // A message for bob while host-a is gone: queued, not delivered directly.
    let msg_body = json!({"kind": "msg", "text": "while you were out"});
    assert!(!outbox.is_direct("bob", &msg_body), "awaiting a resume: the outbox keeps it");
    let msg_id = outbox.send("agent:alice", "bob", msg_body).unwrap();

    // host-a is back (a restart): bob is resumed there, same conversation.
    let mut a = fake_host(&url, "host-a").await;
    match recv(&mut a).await {
        Frame::Spawn { req, agent_id, body, .. } => {
            assert_eq!(agent_id, "bob");
            assert_eq!(body["args"], json!(["--resume", "conv-1"]));
            assert_eq!(body["cwd"], json!("/w/bob"));
            send(&mut a, &Frame::Reply { req, status: "200 OK".into(), body: json!({"sessionId": sid(3), "reused": false}).to_string() })
                .await;
        }
        other => panic!("expected bob's resume, got {other:?}"),
    }
    // Then the message that waited, once.
    match recv(&mut a).await {
        Frame::Deliver { msg_id: got, agent_id, .. } => {
            assert_eq!((got.as_str(), agent_id.as_str()), (msg_id.as_str(), "bob"));
            send(&mut a, &Frame::DeliverAck { msg_id: got, status: crate::hosts::proto::DeliverStatus::Delivered, reason: None }).await;
        }
        other => panic!("expected the waiting message, got {other:?}"),
    }
    within("the message to be delivered", async {
        while outbox.get(&msg_id).map(|r| r.state) != Some(k2_core::delivery::State::Delivered) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(!remote.awaiting_resume("bob"));
    assert_eq!(remote.placement("bob").unwrap().host_id, "host-a");
}

// ── the gone rule: a recipient whose session ended (I2 under chaos) ────

async fn wait_final(outbox: &crate::delivery_drain::Controller, msg_id: &str) -> k2_core::delivery::outbox::Row {
    within("the message's final state", async {
        loop {
            if let Some(r) = outbox.get(msg_id).filter(|r| {
                matches!(r.state, k2_core::delivery::State::Delivered | k2_core::delivery::State::Failed)
            }) {
                return r;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_for_an_agent_whose_session_ended_goes_direct_or_fails_after_the_grace() {
    use crate::delivery_drain::{start_controller, Direct};
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let direct_calls: Arc<parking_lot::Mutex<Vec<(String, bool)>>> = Arc::default();
    let calls = Arc::clone(&direct_calls);
    // `ws-*` resolve on the controller (a workspace); `bare-*` don't.
    let outbox = start_controller(
        remote,
        mem_db(),
        FAST,
        Direct {
            resolves: Box::new(|r| r.starts_with("ws-")),
            aliases: Box::new(|_| Vec::new()),
            deliver: Box::new(move |to: &str, _from: &str, body: &Value| {
                let wake = body["wake"].as_bool() == Some(true);
                calls.lock().push((to.to_string(), wake));
                if wake { Ok(()) } else { Err("dormant_no_wake".into()) }
            }),
        },
    );
    let mut a = fake_host(&url, "host-a").await;
    for (agent, n) in [("bare-1", 1), ("ws-1", 2), ("ws-2", 3)] {
        spawn_on(remote, &mut a, agent, "/w", &sid(n)).await;
        gone(&mut a, remote, agent).await; // the CLI was killed
        assert!(remote.session_ended_at(agent).is_some(), "{agent}");
    }
    let msg = |wake: bool| json!({"kind": "msg", "text": "hi", "wake": wake});
    let bare = outbox.send("agent:alice", "bare-1", msg(false)).unwrap();
    let ws_sleep = outbox.send("agent:alice", "ws-1", msg(false)).unwrap();
    let ws_wake = outbox.send("agent:alice", "ws-2", msg(true)).unwrap();

    // Within the grace (default 120 s): they wait.
    assert!(outbox.pass(k2_core::delivery::now_ms()).is_empty());
    for id in [&bare, &ws_sleep, &ws_wake] {
        assert_eq!(outbox.get(id).unwrap().state, State::Queued, "{id}");
    }

    // Past it: a bare name fails recipient_gone; a workspace takes today's
    // direct path, which wakes it with --wake and refuses without.
    outbox.set_gone_grace(Duration::ZERO);
    let r = wait_final(&outbox, &bare).await;
    assert_eq!(r.state, State::Failed);
    assert!(r.reason.as_deref().unwrap_or("").starts_with("recipient_gone: bare-1's session ended"), "{:?}", r.reason);
    let r = wait_final(&outbox, &ws_sleep).await;
    assert_eq!(r.state, State::Failed);
    assert!(r.reason.as_deref().unwrap_or("").starts_with("dormant_no_wake"), "{:?}", r.reason);
    assert_eq!(wait_final(&outbox, &ws_wake).await.state, State::Delivered);
    let mut seen = direct_calls.lock().clone();
    seen.sort();
    assert_eq!(seen, [("ws-1".to_string(), false), ("ws-2".to_string(), true)], "once each");

    // Placed again: no longer ended, so nothing about it is "gone".
    spawn_on(remote, &mut a, "bare-1", "/w", &sid(4)).await;
    assert_eq!(remote.session_ended_at("bare-1"), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_for_an_agent_on_a_lost_host_keeps_waiting_and_lands_when_it_is_back() {
    use crate::delivery_drain::{start_controller, Direct};
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = start_controller(
        remote,
        mem_db(),
        FAST,
        Direct {
            resolves: Box::new(|_| true),
            aliases: Box::new(|_| Vec::new()),
            deliver: Box::new(|_, _, _| panic!("a lost host's agent never takes the direct path")),
        },
    );
    // The rule would act at once if it applied.
    outbox.set_gone_grace(Duration::ZERO);
    let mut b = fake_host(&url, "host-b").await;
    spawn_on(remote, &mut b, "dave", "/w/dave", &sid(1)).await;
    drop(b); // the host is lost; dave may still run there
    within("host-b to be lost", async {
        while remote.status_json()["lost"] != json!(["host-b"]) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(remote.session_ended_at("dave"), None);
    let id = outbox.send("agent:alice", "dave", json!({"kind": "msg", "text": "hold on"})).unwrap();
    for _ in 0..3 {
        assert!(outbox.pass(k2_core::delivery::now_ms()).is_empty());
    }
    assert_eq!(outbox.get(&id).unwrap().state, State::Queued, "held while the host is lost (S6)");

    // host-b is back, still running dave (hello.resume, W3): adopted, and
    // the message lands. (Back *without* dave would mean his session ended:
    // the other test's case.)
    let epoch = remote.placement("dave").unwrap().epoch.0;
    let (mut b, _) = tokio_tungstenite::connect_async(url.as_str()).await.expect("dial");
    let resume = super::proto::ResumePlacement {
        agent_id: "dave".into(),
        session_id: sid(1),
        epoch,
        ring: None,
        child_alive: true,
        cwd: Some("/w/dave".into()),
        program: Some("claude".into()),
        args: vec![],
        label: None,
    };
    send(&mut b, &Frame::Hello { protocol: PROTOCOL, host_name: "host-b".into(), token: TOKEN.into(), labels: vec![], resume: vec![resume] })
        .await;
    assert!(matches!(recv(&mut b).await, Frame::Welcome { .. }));
    match recv(&mut b).await {
        Frame::Deliver { msg_id, agent_id, .. } => {
            assert_eq!((msg_id.as_str(), agent_id.as_str()), (id.as_str(), "dave"));
            send(&mut b, &Frame::DeliverAck { msg_id, status: crate::hosts::proto::DeliverStatus::Delivered, reason: None }).await;
        }
        other => panic!("expected the held message, got {other:?}"),
    }
    assert_eq!(wait_final(&outbox, &id).await.state, State::Delivered);
}

/// K2-b (G2 chaos, seed 4117 S8 r5): the controller sent the message, the
/// host was SIGKILLed before its ack, and came back without the agent (its
/// session ended). The host's report on relink marks the row delivered
/// before the gone rule can fail it (or resend it: a duplicate).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_whose_ack_died_with_the_host_is_reconciled_on_relink_not_failed() {
    use crate::delivery_drain::{start_controller, Direct};
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = start_controller(
        remote,
        mem_db(),
        FAST,
        Direct {
            resolves: Box::new(|_| true),
            aliases: Box::new(|_| Vec::new()),
            deliver: Box::new(|_, _, _| panic!("a delivered message never takes the direct path again")),
        },
    );
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "erin", "/w/erin", &sid(1)).await;
    let id = outbox.send("agent:alice", "erin", json!({"kind": "msg", "text": "hello", "wake": false})).unwrap();
    match recv(&mut a).await {
        Frame::Deliver { msg_id, .. } => assert_eq!(msg_id, id),
        other => panic!("expected the deliver, got {other:?}"),
    }
    drop(a); // SIGKILL: the agent got it, the ack never left
    // Back without erin (her CLI died with the host): her session ended.
    let mut a = fake_host(&url, "host-a").await;
    within("erin gone", async {
        while remote.session_ended_at("erin").is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    // What the relinked host reports once erin's transcript shows the prompt.
    send(
        &mut a,
        &Frame::DeliverAck {
            msg_id: id.clone(),
            status: crate::hosts::proto::DeliverStatus::Delivered,
            reason: Some("found in the agent's transcript after a host restart".into()),
        },
    )
    .await;
    // The gone rule would act at once now; the row is already final.
    outbox.set_gone_grace(Duration::ZERO);
    let r = wait_final(&outbox, &id).await;
    assert_eq!((r.state, r.reason), (State::Delivered, None));
}

/// K2-b's third branch: the host can't tell (the agent's home is gone).
/// The row ends failed with `delivery_uncertain`, visibly, not delivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_uncertain_delivery_fails_visibly_with_delivery_uncertain() {
    use crate::delivery_drain::{start_controller, Direct, UNCERTAIN_REASON};
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = start_controller(
        remote,
        mem_db(),
        FAST,
        Direct {
            resolves: Box::new(|_| true),
            aliases: Box::new(|_| Vec::new()),
            deliver: Box::new(|_, _, _| panic!("an uncertain delivery is never resent")),
        },
    );
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "fred", "/w/fred", &sid(1)).await;
    let id = outbox.send("agent:alice", "fred", json!({"kind": "msg", "text": "hello"})).unwrap();
    assert!(matches!(recv(&mut a).await, Frame::Deliver { .. }));
    drop(a);
    let mut a = fake_host(&url, "host-a").await;
    let why = format!("{UNCERTAIN_REASON}: interrupted by a host restart and the agent's transcript can't tell (home gone)");
    send(&mut a, &Frame::DeliverAck { msg_id: id.clone(), status: crate::hosts::proto::DeliverStatus::Rejected, reason: Some(why) }).await;
    let r = wait_final(&outbox, &id).await;
    assert_eq!(r.state, State::Failed);
    assert!(r.reason.as_deref().unwrap_or("").starts_with(UNCERTAIN_REASON), "{:?}", r.reason);
}

// ── T2 (S10): a lost host's agents move ────────────────────────────────

/// `host`'s next frame is `agent`'s spawn: answer it with `session`.
/// Returns the spawn's epoch and body.
async fn answer_spawn(host: &mut Ws, agent: &str, session: &str) -> (u64, Value) {
    match recv(host).await {
        Frame::Spawn { req, agent_id, epoch, body } => {
            assert_eq!(agent_id, agent);
            send(host, &Frame::Reply { req, status: "200 OK".into(), body: json!({"sessionId": session, "reused": false}).to_string() })
                .await;
            (epoch, body)
        }
        other => panic!("expected {agent}'s spawn, got {other:?}"),
    }
}

/// `host-a` links again, still running `agent` at `epoch` (hello.resume).
async fn back_with(url: &str, agent: &str, epoch: u64) -> Ws {
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.expect("dial");
    let resume = super::proto::ResumePlacement {
        agent_id: agent.into(),
        session_id: sid(1),
        epoch,
        ring: None,
        child_alive: true,
        cwd: Some(format!("/w/{agent}")),
        program: Some("claude".into()),
        args: vec![],
        label: None,
    };
    send(&mut ws, &Frame::Hello { protocol: PROTOCOL, host_name: "host-a".into(), token: TOKEN.into(), labels: vec![], resume: vec![resume] })
        .await;
    assert!(matches!(recv(&mut ws).await, Frame::Welcome { .. }));
    ws
}

async fn wait_lost(remote: &RemoteHost, host: &str) {
    within("the host to be lost", async {
        while remote.status_json()["lost"] != json!([host]) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_portable_agent_on_a_lost_host_moves_resumes_elsewhere_and_its_message_follows() {
    use crate::delivery_drain::{start_controller, Direct};
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = start_controller(
        remote,
        mem_db(),
        FAST,
        Direct {
            resolves: Box::new(|_| true),
            aliases: Box::new(|_| Vec::new()),
            deliver: Box::new(|_, _, _| panic!("a moving agent's message never takes the direct path")),
        },
    );
    outbox.set_gone_grace(Duration::ZERO);
    super::drain::ensure_controller_resumes_with(remote, Arc::new(super::drain::resume_v2));
    let mut a = fake_host(&url, "host-a").await;
    let body = json!({"agent_name": "bob", "cwd": "/w/bob", "command": "claude", "args": ["--session-id", "conv-1"]}).to_string();
    let spawn = tokio::spawn(async move { remote.spawn(body.as_bytes()).await });
    let (e1, _) = answer_spawn(&mut a, "bob", &sid(1)).await;
    assert_eq!(within("spawn", spawn).await.unwrap().0, "200 OK");
    let mut b = fake_host(&url, "host-b").await;
    wait_hosts(remote, 2).await;

    // kill -9 host-a: its link drops, no drain, no release.
    drop(a);
    wait_lost(remote, "host-a").await;
    let id = outbox.send("agent:alice", "bob", json!({"kind": "msg", "text": "after the crash"})).unwrap();
    let now = std::time::Instant::now();
    assert_eq!(remote.move_lost(now, Duration::from_secs(3600)), Vec::<String>::new(), "not lost long enough");
    assert_eq!(remote.placement("bob").unwrap().host_id, "host-a", "kept, lost (W3)");
    assert_eq!(outbox.get(&id).unwrap().state, State::Queued);

    // Lost past K2_HOST_MOVE_AFTER: bob moves, and resumes on host-b at a
    // new epoch, the same conversation.
    assert_eq!(remote.move_lost(now, Duration::ZERO), vec!["bob"]);
    let (e2, body) = answer_spawn(&mut b, "bob", &sid(2)).await;
    assert!(e2 > e1, "a new epoch: {e2} after {e1}");
    assert_eq!(body["args"], json!(["--resume", "conv-1"]));
    match recv(&mut b).await {
        Frame::Deliver { msg_id, agent_id, .. } => {
            assert_eq!((msg_id.as_str(), agent_id.as_str()), (id.as_str(), "bob"));
            send(&mut b, &Frame::DeliverAck { msg_id, status: crate::hosts::proto::DeliverStatus::Delivered, reason: None }).await;
        }
        other => panic!("expected the waiting message on host-b, got {other:?}"),
    }
    assert_eq!(wait_final(&outbox, &id).await.state, State::Delivered);
    assert_eq!(remote.placement("bob").unwrap().host_id, "host-b");
    assert!(!remote.awaiting_resume("bob"));

    // host-a comes back still running its copy (a partition, not a crash):
    // told to release it (I1); bob stays on host-b.
    let mut a = back_with(&url, "bob", e1).await;
    match recv(&mut a).await {
        Frame::Release { agent_id, epoch, .. } => assert_eq!((agent_id.as_str(), epoch), ("bob", e1)),
        other => panic!("expected a release of the stale copy, got {other:?}"),
    }
    assert_eq!(remote.placement("bob").unwrap().host_id, "host-b");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pinned_agent_on_a_lost_host_waits_for_it() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "lap", "/w/lap", &sid(1)).await;
    remote.policy().set_mode("lap", &crate::provisioner::Mode::Pinned { host: "host-a".into() }).unwrap();
    let _b = fake_host(&url, "host-b").await;
    wait_hosts(remote, 2).await;
    drop(a);
    wait_lost(remote, "host-a").await;
    assert_eq!(remote.move_lost(std::time::Instant::now(), Duration::ZERO), Vec::<String>::new());
    assert_eq!(remote.placement("lap").unwrap().host_id, "host-a", "pinned: never placed elsewhere");
    assert!(remote.resume_due().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_back_with_an_agent_that_moved_meanwhile_is_told_to_release_it() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "eve", "/w/eve", &sid(1)).await;
    let e1 = remote.placement("eve").unwrap().epoch.0;
    drop(a);
    wait_lost(remote, "host-a").await;
    // No other host: eve moves, and waits for any host.
    assert_eq!(remote.move_lost(std::time::Instant::now(), Duration::ZERO), vec!["eve"]);
    assert!(remote.placement("eve").is_none());
    assert!(remote.awaiting_resume("eve"));
    // host-a is back with its old copy: not adopted (it would write an old
    // home and a second CLI); released. eve then resumes from its snapshot.
    let mut a = back_with(&url, "eve", e1).await;
    match recv(&mut a).await {
        Frame::Release { agent_id, epoch, reason, .. } => {
            assert_eq!((agent_id.as_str(), epoch), ("eve", e1));
            assert!(reason.contains("moved"), "{reason}");
        }
        other => panic!("expected a release, got {other:?}"),
    }
    assert!(remote.placement("eve").is_none(), "the stale copy is not adopted");
    assert_eq!(remote.resume_due().iter().map(|r| r.agent_id.as_str()).collect::<Vec<_>>(), vec!["eve"]);
}

// ── drain-move: a drained agent doesn't wait for its host forever ──────

/// The outbox with every address resolving here and no direct path.
fn outbox_for(remote: &'static RemoteHost) -> Arc<crate::delivery_drain::Controller> {
    use crate::delivery_drain::{start_controller, Direct};
    start_controller(
        remote,
        mem_db(),
        FAST,
        Direct {
            resolves: Box::new(|_| true),
            aliases: Box::new(|_| Vec::new()),
            deliver: Box::new(|_, _, _| panic!("these agents' messages never take the direct path")),
        },
    )
}

/// `agent` (`--session-id conv-1`) on `host`, then `host` drains it and
/// goes for good (a scale-down). Returns once the controller holds the
/// resume row and the host is unlinked.
async fn drained_for_good(remote: &'static RemoteHost, host: &mut Ws, agent: &str) {
    let body = json!({"agent_name": agent, "cwd": format!("/w/{agent}"), "command": "claude", "args": ["--session-id", "conv-1"]}).to_string();
    let spawn = tokio::spawn(async move { remote.spawn(body.as_bytes()).await });
    answer_spawn(host, agent, &sid(1)).await;
    assert_eq!(within("spawn", spawn).await.unwrap().0, "200 OK");
    send(host, &Frame::Refused { code: super::drain::DRAINING_CODE.into(), message: "draining".into() }).await;
    send(host, &Frame::Released { req: 0, agent_id: agent.into(), epoch: 0, ring_end: None }).await;
    within("the resume to be recorded", async {
        while !remote.awaiting_resume(agent) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    send(host, &Frame::SessionGone { agent_id: agent.into(), epoch: None, exit_code: None }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_for_a_drained_portable_agent_resumes_it_on_another_host_at_once() {
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    super::drain::ensure_controller_resumes_with(remote, Arc::new(super::drain::resume_v2));
    let mut a = fake_host(&url, "host-a").await;
    let mut b = fake_host(&url, "host-b").await;
    wait_hosts(remote, 2).await;
    drained_for_good(remote, &mut a, "bob").await;
    drop(a);
    wait_hosts(remote, 1).await;
    assert!(remote.resume_due().is_empty(), "no need yet: bob waits for host-a (K2_HOST_MOVE_AFTER)");

    // A message is a need: bob resumes on host-b now, the same conversation
    // (host-b restores its home snapshot first: T1), then gets it, once.
    let id = outbox.send("agent:alice", "bob", json!({"kind": "msg", "text": "after the scale-down"})).unwrap();
    let (_, body) = answer_spawn(&mut b, "bob", &sid(2)).await;
    assert_eq!(body["args"], json!(["--resume", "conv-1"]));
    match recv(&mut b).await {
        Frame::Deliver { msg_id, agent_id, attempt, .. } => {
            assert_eq!((msg_id.as_str(), agent_id.as_str(), attempt), (id.as_str(), "bob", 1));
            send(&mut b, &Frame::DeliverAck { msg_id, status: crate::hosts::proto::DeliverStatus::Delivered, reason: None }).await;
        }
        other => panic!("expected the message on host-b, got {other:?}"),
    }
    let row = wait_final(&outbox, &id).await;
    assert_eq!((row.state, row.attempts), (State::Delivered, 1), "delivered once");
    assert_eq!(remote.placement("bob").unwrap().host_id, "host-b");
    assert!(!remote.awaiting_resume("bob"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drained_pinned_agent_waits_for_its_host_even_with_a_message() {
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    let _b = fake_host(&url, "host-b").await;
    wait_hosts(remote, 2).await;
    remote.policy().set_mode("lap", &crate::provisioner::Mode::Pinned { host: "host-a".into() }).unwrap();
    drained_for_good(remote, &mut a, "lap").await;
    drop(a);
    wait_hosts(remote, 1).await;
    let id = outbox.send("agent:alice", "lap", json!({"kind": "msg", "text": "hold on"})).unwrap();
    for _ in 0..3 {
        outbox.pass(k2_core::delivery::now_ms());
    }
    assert!(remote.resume_due().is_empty(), "pinned: never resumed elsewhere");
    assert_eq!(remote.move_lost(std::time::Instant::now(), Duration::ZERO), Vec::<String>::new());
    assert!(remote.resume_due().is_empty());
    assert!(remote.awaiting_resume("lap"));
    assert_eq!(outbox.get(&id).unwrap().state, State::Queued, "the message waits with it");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drained_portable_agent_resumes_anywhere_once_its_host_is_gone_for_k2_host_move_after() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let mut a = fake_host(&url, "host-a").await;
    let _b = fake_host(&url, "host-b").await;
    wait_hosts(remote, 2).await;
    drained_for_good(remote, &mut a, "cat").await;
    let now = std::time::Instant::now();
    assert_eq!(remote.move_lost(now, Duration::ZERO), Vec::<String>::new(), "host-a is still linked: it may resume cat");
    drop(a);
    wait_hosts(remote, 1).await;
    assert!(remote.resume_due().is_empty());
    let later = std::time::Instant::now();
    assert_eq!(remote.move_lost(later, Duration::from_secs(3600)), Vec::<String>::new(), "not gone long enough");
    assert_eq!(remote.move_lost(later, Duration::ZERO), vec!["cat"]);
    let due = remote.resume_due();
    assert_eq!(
        due.iter().map(|r| (r.agent_id.as_str(), r.host_id.as_str())).collect::<Vec<_>>(),
        vec![("cat", super::drain::ANY_HOST)],
        "any host now"
    );
    assert_eq!(remote.move_lost(later, Duration::ZERO), Vec::<String>::new(), "once");
}

// ── K2-d: a turn in progress is never cut silently ─────────────────────

async fn hook(host: &mut Ws, pane: &str, event: &str) {
    send(host, &Frame::Hook { pane: pane.into(), event: event.into(), session_id: Some(pane.into()), conversation: None, agent_pid: None, at_ms: 0, epoch: None })
        .await;
}

/// The outbox delivers `text` to `agent` on `host`, which acks it.
async fn delivered(outbox: &crate::delivery_drain::Controller, host: &mut Ws, agent: &str, text: &str) -> String {
    let id = outbox.send("agent:alice", agent, json!({"kind": "msg", "text": text})).unwrap();
    match recv(host).await {
        Frame::Deliver { msg_id, .. } => {
            assert_eq!(msg_id, id);
            send(host, &Frame::DeliverAck { msg_id, status: crate::hosts::proto::DeliverStatus::Delivered, reason: None }).await;
        }
        other => panic!("expected the delivery, got {other:?}"),
    }
    assert_eq!(wait_final(outbox, &id).await.state, k2_core::delivery::State::Delivered);
    id
}

/// `remote.unplace(agent)` with `host` answering its close.
async fn close_on(remote: &'static RemoteHost, host: &mut Ws, agent: &str) {
    let a = agent.to_string();
    let unplace = tokio::task::spawn_blocking(move || crate::agent_host::AgentHost::unplace(remote, &a).is_some());
    match recv(host).await {
        Frame::Close { req, agent_id, .. } => {
            assert_eq!(agent_id, agent);
            send(host, &Frame::Reply { req, status: "200 OK".into(), body: json!({"closed": true}).to_string() }).await;
        }
        other => panic!("expected the close, got {other:?}"),
    }
    assert!(within("unplace", unplace).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_controller_follows_turns_and_a_close_mid_turn_fails_the_unanswered_message() {
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let seen: Arc<parking_lot::Mutex<Vec<String>>> = Arc::default();
    let s = Arc::clone(&seen);
    outbox.set_activity(Arc::new(move |a: &str| s.lock().push(a.to_string())));
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "dan", "/w/dan", &sid(1)).await;
    spawn_on(remote, &mut a, "eve", "/w/eve", &sid(2)).await;

    // A message-only agent, no app: the delivery is activity (the reaper's
    // need clock), and its turn runs from UserPromptSubmit to Stop.
    let m1 = delivered(&outbox, &mut a, "dan", "first").await;
    assert!(seen.lock().iter().any(|x| x == "dan"), "the delivery counted as activity: {:?}", seen.lock());
    assert!(!remote.in_turn("dan"));
    hook(&mut a, &sid(1), "UserPromptSubmit").await;
    within("dan's turn to start", async {
        while !remote.in_turn("dan") {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    hook(&mut a, &sid(1), "Stop").await;
    within("dan's turn to end", async {
        while remote.in_turn("dan") {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    // Answered, then closed: it stays delivered.
    close_on(remote, &mut a, "dan").await;
    assert_eq!(outbox.get(&m1).unwrap().state, State::Delivered);

    // eve gets a message and its turn starts; something closes it anyway
    // (a forced dismiss, a deadline): the message fails, saying why.
    let m2 = delivered(&outbox, &mut a, "eve", "second").await;
    hook(&mut a, &sid(2), "UserPromptSubmit").await;
    within("eve's turn to start", async {
        while !remote.in_turn("eve") {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    close_on(remote, &mut a, "eve").await;
    let row = outbox.get(&m2).unwrap();
    assert_eq!(row.state, State::Failed, "never left as answered");
    assert!(row.reason.as_deref().unwrap_or_default().contains("interrupted: eve was closed mid-turn"), "{row:?}");
    assert!(!remote.in_turn("eve"), "no placement, no turn");
}

/// A message waiting for an agent that is being placed (a resume after a
/// drain) is not sent while the spawn runs, where the host would answer
/// "not yet" and the outbox back off: it goes out once the spawn answers,
/// on its first attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_waits_for_the_spawn_to_answer_and_goes_out_on_its_first_attempt() {
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    super::drain::ensure_controller_resumes_with(remote, Arc::new(super::drain::resume_v2));
    let mut a = fake_host(&url, "host-a").await;
    let mut b = fake_host(&url, "host-b").await;
    wait_hosts(remote, 2).await;
    drained_for_good(remote, &mut a, "ann").await;
    drop(a);
    wait_hosts(remote, 1).await;

    let id = outbox.send("agent:alice", "ann", json!({"kind": "msg", "text": "hello again"})).unwrap();
    let (req, epoch) = match recv(&mut b).await {
        Frame::Spawn { req, agent_id, epoch, .. } => {
            assert_eq!(agent_id, "ann");
            (req, epoch)
        }
        other => panic!("expected ann's resume, got {other:?}"),
    };
    // Reserved on host-b, its spawn not answered: nothing is sent.
    assert_eq!(remote.placement("ann").map(|p| (p.host_id, p.epoch.0, p.session_id)), Some(("host-b".into(), epoch, None)));
    for _ in 0..3 {
        outbox.pass(k2_core::delivery::now_ms());
    }
    let row = outbox.get(&id).unwrap();
    assert_eq!((row.state, row.attempts), (State::Queued, 0), "no attempt spent while the spawn runs");

    send(&mut b, &Frame::Reply { req, status: "200 OK".into(), body: json!({"sessionId": sid(2), "reused": false}).to_string() }).await;
    match recv(&mut b).await {
        Frame::Deliver { msg_id, attempt, .. } => {
            assert_eq!((msg_id.as_str(), attempt), (id.as_str(), 1), "its first attempt");
            send(&mut b, &Frame::DeliverAck { msg_id, status: crate::hosts::proto::DeliverStatus::Delivered, reason: None }).await;
        }
        other => panic!("expected the message after the spawn, got {other:?}"),
    }
    let row = wait_final(&outbox, &id).await;
    assert_eq!((row.state, row.attempts), (State::Delivered, 1));
}
