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
    send(&mut ws, &Frame::Hello { protocol: PROTOCOL, host_name: name.into(), token: TOKEN.into(), labels: vec![], resume: vec![], ended: vec![] }).await;
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
    send(host, &Frame::SessionGone { agent_id: agent.into(), epoch: None, exit_code: Some(137), turn: None }).await;
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

/// The away home host's agent placed by load on a host that syncs homes,
/// with a snapshot in the store: the note says its home comes with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_a_host_that_syncs_homes_the_note_says_the_home_is_restored() {
    let snap = k2_core::agent_home::Latest {
        seq: 5,
        epoch: 1,
        host: "host-a".into(),
        manifest_sha: "0".repeat(64),
        created_ms: 0,
        files: 1,
        bytes: 1,
    };
    let remote: &'static RemoteHost =
        Box::leak(Box::new(RemoteHost::new().with_home_latest(move |a| (a == "s7").then(|| snap.clone()))));
    let url = serve(remote).await;
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "s7", "/w/s7", &sid(1)).await;
    gone(&mut a, remote, "s7").await;
    drop(a);
    let (mut b, _) = tokio_tungstenite::connect_async(url.as_str()).await.expect("dial");
    let labels = vec![super::home::LABEL_HOME_SYNC.to_string()];
    send(&mut b, &Frame::Hello { protocol: PROTOCOL, host_name: "host-b".into(), token: TOKEN.into(), labels, resume: vec![], ended: vec![] }).await;
    assert!(matches!(recv(&mut b).await, Frame::Welcome { .. }));
    wait_hosts(remote, 1).await;

    let answer = spawn_on(remote, &mut b, "s7", "/w/s7", &sid(2)).await;
    let note = answer["placementNote"].as_str().unwrap_or_default();
    assert!(note.contains("last ran on host host-a, which is not linked; placed on host-b by load, which restores its home (snapshot 5"), "{answer}");
    assert!(!note.contains("are not"), "{note}");
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
    send(&mut a, &Frame::SessionGone { agent_id: "bob".into(), epoch: None, exit_code: None, turn: None }).await;
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
        turn: None,
        adopted: false,
    };
    send(&mut b, &Frame::Hello { protocol: PROTOCOL, host_name: "host-b".into(), token: TOKEN.into(), labels: vec![], resume: vec![resume], ended: vec![] })
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

/// K2-e: `agent` resumes on `host` while `msg_id` waits for it. Until its
/// spawn answers it is only reserved there, and a Deliver would meet "no
/// agent here" (final): the outbox's passes send nothing and spend no
/// attempt. Once the spawn answers, the message goes out on its first
/// attempt. Step by step, so no tick can slip in between. The spawn's
/// epoch and body.
async fn resume_then_deliver(
    remote: &RemoteHost,
    outbox: &crate::delivery_drain::Controller,
    host: &mut Ws,
    host_id: &str,
    agent: &str,
    session: &str,
    msg_id: &str,
) -> (u64, Value) {
    use k2_core::delivery::State;
    let (req, epoch, body) = match recv(host).await {
        Frame::Spawn { req, agent_id, epoch, body } => {
            assert_eq!(agent_id, agent);
            (req, epoch, body)
        }
        other => panic!("expected {agent}'s resume before anything for it, got {other:?}"),
    };
    assert_eq!(
        remote.placement(agent).map(|p| (p.host_id, p.epoch.0, p.session_id)),
        Some((host_id.to_string(), epoch, None)),
        "reserved, its spawn not answered"
    );
    for _ in 0..3 {
        outbox.pass(k2_core::delivery::now_ms());
    }
    let row = outbox.get(msg_id).unwrap();
    assert_eq!((row.state, row.attempts), (State::Queued, 0), "nothing sent while the spawn runs");
    send(host, &Frame::Reply { req, status: "200 OK".into(), body: json!({"sessionId": session, "reused": false}).to_string() }).await;
    match recv(host).await {
        Frame::Deliver { msg_id: m, agent_id, attempt, .. } => {
            assert_eq!((m.as_str(), agent_id.as_str(), attempt), (msg_id, agent, 1), "after the spawn, its first attempt");
            send(host, &Frame::DeliverAck { msg_id: m, status: crate::hosts::proto::DeliverStatus::Delivered, reason: None }).await;
        }
        other => panic!("expected the waiting message after the spawn, got {other:?}"),
    }
    let row = wait_final(outbox, msg_id).await;
    assert_eq!((row.state, row.attempts), (State::Delivered, 1), "delivered once");
    (epoch, body)
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
        turn: None,
        adopted: false,
    };
    send(&mut ws, &Frame::Hello { protocol: PROTOCOL, host_name: "host-a".into(), token: TOKEN.into(), labels: vec![], resume: vec![resume], ended: vec![] })
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
    // new epoch, the same conversation; the message follows the spawn's
    // answer, never before it (K2-e).
    assert_eq!(remote.move_lost(now, Duration::ZERO), vec!["bob"]);
    let (e2, body) = resume_then_deliver(remote, &outbox, &mut b, "host-b", "bob", &sid(2), &id).await;
    assert!(e2 > e1, "a new epoch: {e2} after {e1}");
    assert_eq!(body["args"], json!(["--resume", "conv-1"]));
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
    send(host, &Frame::SessionGone { agent_id: agent.into(), epoch: None, exit_code: None, turn: None }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_for_a_drained_portable_agent_resumes_it_on_another_host_at_once() {
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
    let (_, body) = resume_then_deliver(remote, &outbox, &mut b, "host-b", "bob", &sid(2), &id).await;
    assert_eq!(body["args"], json!(["--resume", "conv-1"]));
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
    send(host, &Frame::Hook { pane: pane.into(), event: event.into(), session_id: Some(pane.into()), conversation: None, agent_pid: None, at_ms: 0, epoch: None, subagent: false, turn: None })
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

    // eve gets a message and its turn starts, the host tying the message
    // to that prompt (K2-f.3); something closes it anyway (a forced
    // dismiss, a deadline): the message fails, saying why.
    let m2 = delivered(&outbox, &mut a, "eve", "second").await;
    prompt_with(remote, &mut a, "eve", &sid(2), running(1, 0, &[&m2])).await;
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
    resume_then_deliver(remote, &outbox, &mut b, "host-b", "ann", &sid(2), &id).await;
}

// ── K2-f: a turn cut by a crash or a lost host fails its messages ──────
//
// K2-f.3: the fake hosts here are K2-f.3 hosts: each hook carries the
// host's counts after it, and `msgs`, the messages the running turn's
// prompt submitted as the host matched them. A message fails only when its
// host said it was in the cut turn.

fn counts(prompts: u64, stops: u64, running: bool) -> super::proto::TurnCounts {
    super::proto::TurnCounts { prompts, stops, running, msgs: vec![] }
}

fn running(prompts: u64, stops: u64, msgs: &[&str]) -> super::proto::TurnCounts {
    super::proto::TurnCounts { prompts, stops, running: true, msgs: msgs.iter().map(|m| m.to_string()).collect() }
}

/// A hook carrying the host's counts after it.
async fn hook_at(host: &mut Ws, pane: &str, event: &str, turn: super::proto::TurnCounts) {
    let f = Frame::Hook {
        pane: pane.into(),
        event: event.into(),
        session_id: Some(pane.into()),
        conversation: None,
        agent_pid: None,
        at_ms: 0,
        epoch: None,
        subagent: false,
        turn: Some(turn),
    };
    send(host, &f).await;
}

async fn until(what: &str, f: impl Fn() -> bool) {
    within(what, async {
        while !f() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

/// A prompt starts a turn on `agent`; its host matched `msgs` to it.
async fn prompt_with(remote: &RemoteHost, host: &mut Ws, agent: &str, pane: &str, turn: super::proto::TurnCounts) {
    hook_at(host, pane, "UserPromptSubmit", turn).await;
    until("the turn to start", || remote.in_turn(agent)).await;
}

/// The turn ends (`counts` after the Stop).
async fn stop_with(remote: &RemoteHost, host: &mut Ws, agent: &str, pane: &str, turn: super::proto::TurnCounts) {
    hook_at(host, pane, "Stop", turn).await;
    until("the turn to end", || !remote.in_turn(agent)).await;
}

async fn interrupted(outbox: &crate::delivery_drain::Controller, msg_id: &str) -> String {
    let row = wait_final(outbox, msg_id).await;
    let row = within("the delivered row to fail", async {
        let mut row = row;
        while row.state != k2_core::delivery::State::Failed {
            tokio::time::sleep(Duration::from_millis(10)).await;
            row = outbox.get(msg_id).unwrap();
        }
        row
    })
    .await;
    row.reason.unwrap_or_default()
}

/// Nothing fails `msg_id`: a few passes, and it is still delivered.
async fn stays_delivered(outbox: &crate::delivery_drain::Controller, msg_id: &str) {
    for _ in 0..3 {
        outbox.pass(k2_core::delivery::now_ms());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(outbox.get(msg_id).unwrap().state, k2_core::delivery::State::Delivered, "{msg_id}");
}

fn resumed(agent: &str, session: &str, epoch: u64, turn: Option<super::proto::TurnCounts>) -> super::proto::ResumePlacement {
    super::proto::ResumePlacement {
        agent_id: agent.into(),
        session_id: session.into(),
        epoch,
        child_alive: true,
        cwd: Some(format!("/w/{agent}")),
        program: Some("claude".into()),
        turn,
        ..Default::default()
    }
}

fn ended(agent: &str, session: &str, epoch: u64, turn: Option<super::proto::TurnCounts>) -> super::proto::EndedPlacement {
    super::proto::EndedPlacement { agent_id: agent.into(), session_id: session.into(), epoch, hooked: turn.is_some(), turn: turn.unwrap_or_default() }
}

async fn back(url: &str, host: &str, resume: Vec<super::proto::ResumePlacement>, ended: Vec<super::proto::EndedPlacement>) -> Ws {
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.expect("dial");
    send(&mut ws, &Frame::Hello { protocol: PROTOCOL, host_name: host.into(), token: TOKEN.into(), labels: vec![], resume, ended }).await;
    assert!(matches!(recv(&mut ws).await, Frame::Welcome { .. }));
    ws
}

fn epoch_of(remote: &RemoteHost, agent: &str) -> u64 {
    remote.placement(agent).unwrap().epoch.0
}

/// The CLI is SIGKILLed mid-turn: the host reports it gone with its turn.
/// The message its host matched to that turn fails `interrupted … crashed`,
/// not resent. One answered before (its turn ended) stays delivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cli_that_exits_mid_turn_fails_the_message_it_was_answering() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "fan", "/w/fan", &sid(1)).await;
    let m0 = delivered(&outbox, &mut a, "fan", "turn two").await;
    prompt_with(remote, &mut a, "fan", &sid(1), running(1, 0, &[&m0])).await;
    stop_with(remote, &mut a, "fan", &sid(1), counts(1, 1, false)).await;
    let m1 = delivered(&outbox, &mut a, "fan", "turn three").await;
    prompt_with(remote, &mut a, "fan", &sid(1), running(2, 1, &[&m1])).await;
    send(&mut a, &Frame::SessionGone { agent_id: "fan".into(), epoch: None, exit_code: Some(137), turn: Some(running(2, 1, &[&m1])) }).await;
    assert_eq!(interrupted(&outbox, &m1).await, "interrupted: fan crashed mid-turn before it answered");
    stays_delivered(&outbox, &m0).await;
    assert_eq!(outbox.get(&m1).unwrap().attempts, 1, "not resent");
}

/// K2-f.3 #2: a message typed while the user's own turn runs is folded
/// into it (no prompt of its own: the host matched nothing). That turn
/// ends; a close afterwards fails nothing. Nor does a crash in a turn its
/// host never tied it to.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_folded_into_a_running_turn_is_never_failed() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "gil", "/w/gil", &sid(1)).await;
    spawn_on(remote, &mut a, "hal", "/w/hal", &sid(2)).await;
    // The user's own prompt (turn N); the message lands mid-turn.
    prompt_with(remote, &mut a, "gil", &sid(1), running(1, 0, &[])).await;
    let m = delivered(&outbox, &mut a, "gil", "typed mid-turn").await;
    stop_with(remote, &mut a, "gil", &sid(1), counts(1, 1, false)).await;
    close_on(remote, &mut a, "gil").await;
    stays_delivered(&outbox, &m).await;
    // hal: the same, and its CLI dies in the turn instead.
    prompt_with(remote, &mut a, "hal", &sid(2), running(1, 0, &[])).await;
    let mh = delivered(&outbox, &mut a, "hal", "typed mid-turn too").await;
    send(&mut a, &Frame::SessionGone { agent_id: "hal".into(), epoch: None, exit_code: Some(137), turn: Some(running(1, 0, &[])) }).await;
    until("hal to go", || remote.placement("hal").is_none()).await;
    stays_delivered(&outbox, &mh).await;
}

/// K2-f.3 #3: two messages typed in a row are submitted as their own
/// prompts; the host ties each to its own. The CLI dies in the second's
/// turn: only that one fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_messages_in_a_row_are_each_in_their_own_prompts_turn() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "ivy", "/w/ivy", &sid(1)).await;
    let ma = delivered(&outbox, &mut a, "ivy", "A").await;
    let mb = delivered(&outbox, &mut a, "ivy", "B").await;
    prompt_with(remote, &mut a, "ivy", &sid(1), running(1, 0, &[&ma])).await;
    stop_with(remote, &mut a, "ivy", &sid(1), counts(1, 1, false)).await;
    prompt_with(remote, &mut a, "ivy", &sid(1), running(2, 1, &[&mb])).await;
    gone(&mut a, remote, "ivy").await;
    assert_eq!(interrupted(&outbox, &mb).await, "interrupted: ivy crashed mid-turn before it answered");
    stays_delivered(&outbox, &ma).await;
}

/// The host is SIGKILLed mid-turn. While it is only lost nothing is cut
/// (a partition: it may come back still running the turn). Its daemon
/// restarts without jo and knows nothing of jo's turn: jo's matched
/// message fails with its outcome unknown. kim, resumed mid-turn by the
/// same host, keeps its message and finishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_back_without_an_agent_that_was_mid_turn_leaves_its_outcome_unknown() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "jo", "/w/jo", &sid(1)).await;
    spawn_on(remote, &mut a, "kim", "/w/kim", &sid(2)).await;
    let ek = epoch_of(remote, "kim");
    let mj = delivered(&outbox, &mut a, "jo", "for jo").await;
    let mk = delivered(&outbox, &mut a, "kim", "for kim").await;
    prompt_with(remote, &mut a, "jo", &sid(1), running(1, 0, &[&mj])).await;
    prompt_with(remote, &mut a, "kim", &sid(2), running(1, 0, &[&mk])).await;
    drop(a);
    wait_lost(remote, "host-a").await;
    stays_delivered(&outbox, &mj).await;
    let mut a = back(&url, "host-a", vec![resumed("kim", &sid(2), ek, Some(running(1, 0, &[&mk])))], vec![]).await;
    assert_eq!(interrupted(&outbox, &mj).await, "outcome unknown: host lost mid-turn (jo had not answered when its host was lost)");
    assert!(remote.in_turn("kim"), "adopted mid-turn");
    stop_with(remote, &mut a, "kim", &sid(2), counts(1, 1, false)).await;
    stays_delivered(&outbox, &mk).await;
}

/// A portable agent mid-turn on a host lost past K2_HOST_MOVE_AFTER moves:
/// its turn there is gone with the host, and the matched message's
/// outcome is unknown.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_agent_moved_off_a_lost_host_mid_turn_leaves_its_outcome_unknown() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "lou", "/w/lou", &sid(1)).await;
    let m = delivered(&outbox, &mut a, "lou", "before the crash").await;
    let other = delivered(&outbox, &mut a, "lou", "not submitted").await;
    prompt_with(remote, &mut a, "lou", &sid(1), running(1, 0, &[&m])).await;
    drop(a);
    wait_lost(remote, "host-a").await;
    assert_eq!(remote.move_lost(std::time::Instant::now(), Duration::ZERO), vec!["lou"]);
    assert_eq!(interrupted(&outbox, &m).await, "outcome unknown: host lost mid-turn (lou had not answered when its host was lost)");
    stays_delivered(&outbox, &other).await;
}

/// #2 (K2-f.1): the link drops mid-turn; while it is down mo's turn ends
/// (its Stop never forwarded), ned's CLI exits after answering, and pat's
/// CLI dies mid-turn. The host's hello says so: mo's and ned's messages
/// stay delivered (a later close of mo fails nothing), pat's fails
/// "crashed".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_missed_in_a_link_outage_comes_back_in_the_hosts_hello() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "mo", "/w/mo", &sid(1)).await;
    spawn_on(remote, &mut a, "ned", "/w/ned", &sid(2)).await;
    spawn_on(remote, &mut a, "pat", "/w/pat", &sid(3)).await;
    let (em, en, ep) = (epoch_of(remote, "mo"), epoch_of(remote, "ned"), epoch_of(remote, "pat"));
    let mm = delivered(&outbox, &mut a, "mo", "for mo").await;
    let mn = delivered(&outbox, &mut a, "ned", "for ned").await;
    let mp = delivered(&outbox, &mut a, "pat", "for pat").await;
    prompt_with(remote, &mut a, "mo", &sid(1), running(1, 0, &[&mm])).await;
    prompt_with(remote, &mut a, "ned", &sid(2), running(1, 0, &[&mn])).await;
    prompt_with(remote, &mut a, "pat", &sid(3), running(1, 0, &[&mp])).await;
    drop(a);
    wait_lost(remote, "host-a").await;
    let mut a = back(
        &url,
        "host-a",
        vec![resumed("mo", &sid(1), em, Some(counts(1, 1, false)))],
        vec![ended("ned", &sid(2), en, Some(counts(1, 1, false))), ended("pat", &sid(3), ep, Some(running(1, 0, &[&mp])))],
    )
    .await;
    assert_eq!(interrupted(&outbox, &mp).await, "interrupted: pat crashed mid-turn before it answered");
    assert!(!remote.in_turn("mo"), "its missed Stop applied");
    close_on(remote, &mut a, "mo").await;
    stays_delivered(&outbox, &mm).await;
    stays_delivered(&outbox, &mn).await;
}

/// #4 (K2-f.1): `session_gone` carries the turn, so a Stop hook that comes
/// after it (or never) can't fail an answered message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_gone_carries_the_turn_so_a_late_stop_hook_cant_fail_an_answer() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "quin", "/w/quin", &sid(1)).await;
    let m = delivered(&outbox, &mut a, "quin", "answered, then exited").await;
    prompt_with(remote, &mut a, "quin", &sid(1), running(1, 0, &[&m])).await;
    send(&mut a, &Frame::SessionGone { agent_id: "quin".into(), epoch: None, exit_code: Some(0), turn: Some(counts(1, 1, false)) }).await;
    until("quin to go", || remote.placement("quin").is_none()).await;
    hook_at(&mut a, &sid(1), "Stop", counts(1, 1, false)).await;
    stays_delivered(&outbox, &m).await;
}

/// #1 (K2-f.1): the host's daemon restarts while a turn runs. Without
/// ptyd adoption (S3) the CLI doesn't outlive its daemon, and the new
/// daemon knows nothing of the turn: its matched message's outcome is
/// unknown, not a crash, and nothing is resent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_daemon_restart_mid_turn_leaves_the_outcome_unknown() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "rex", "/w/rex", &sid(1)).await;
    let m = delivered(&outbox, &mut a, "rex", "mid-turn").await;
    prompt_with(remote, &mut a, "rex", &sid(1), running(1, 0, &[&m])).await;
    drop(a);
    wait_lost(remote, "host-a").await;
    let _a = back(&url, "host-a", vec![], vec![]).await;
    assert_eq!(interrupted(&outbox, &m).await, "outcome unknown: host lost mid-turn (rex had not answered when its host was lost)");
    assert_eq!(outbox.get(&m).unwrap().attempts, 1, "not resent");
}

/// ptyd S3: the host's daemon restarts mid-turn and adopts the CLI ptyd
/// kept: it resumes the agent at its epoch, alive, with its turn
/// (`adopted`). The controller adopts it: the turn's message is not failed
/// nor resent, the turn still runs, and the agent's Stop answers it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_daemon_restart_that_adopts_the_cli_keeps_the_turn_and_its_message() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "rex", "/w/rex", &sid(1)).await;
    let epoch = remote.placement("rex").unwrap().epoch.0;
    let m = delivered(&outbox, &mut a, "rex", "mid-turn").await;
    prompt_with(remote, &mut a, "rex", &sid(1), running(1, 0, &[&m])).await;
    drop(a);
    wait_lost(remote, "host-a").await;
    let adopted = super::proto::ResumePlacement { adopted: true, ..resumed("rex", &sid(1), epoch, Some(running(1, 0, &[&m]))) };
    let mut a = back(&url, "host-a", vec![adopted], vec![]).await;
    assert_eq!(remote.placement("rex").map(|p| p.epoch.0), Some(epoch), "adopted (the hub adopts before it welcomes)");
    stays_delivered(&outbox, &m).await;
    assert_eq!(outbox.get(&m).unwrap().attempts, 1, "not resent");
    assert!(remote.in_turn("rex"), "the turn the restart interrupted still runs");
    hook_at(&mut a, &sid(1), "Stop", super::proto::TurnCounts { prompts: 1, stops: 1, running: false, msgs: vec![] }).await;
    until("rex answered", || !remote.in_turn("rex")).await;
    stays_delivered(&outbox, &m).await;
}

/// K2-f.3 #5: a host daemon that restarted counts from what ptyd kept,
/// which can be behind the controller (hooks fired while no daemon
/// listened). An `adopted` resume's counts are taken even when lower; the
/// same counts from a host that did not restart are older news, ignored.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_adopted_resume_resets_the_controllers_turn() {
    for adopted in [true, false] {
        let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
        let url = serve(remote).await;
        let mut a = fake_host(&url, "host-a").await;
        spawn_on(remote, &mut a, "ivy", "/w/ivy", &sid(1)).await;
        let epoch = remote.placement("ivy").unwrap().epoch.0;
        prompt_with(remote, &mut a, "ivy", &sid(1), running(3, 2, &[])).await;
        assert!(remote.in_turn("ivy"));
        drop(a);
        wait_lost(remote, "host-a").await;
        // The host's own counts: its third prompt's Stop only, lower in sum.
        let behind = super::proto::TurnCounts { prompts: 2, stops: 2, running: false, msgs: vec![] };
        let r = super::proto::ResumePlacement { adopted, ..resumed("ivy", &sid(1), epoch, Some(behind)) };
        let _a = back(&url, "host-a", vec![r], vec![]).await;
        // The hub adopts before it welcomes.
        assert_eq!(remote.placement("ivy").map(|p| p.epoch.0), Some(epoch));
        assert_eq!(remote.in_turn("ivy"), !adopted, "adopted={adopted}");
    }
}

/// K2-f.3 #1: an agent that never sends hooks (a provider without them):
/// nothing ever ties its messages to a turn, so neither a close, nor a
/// controller restart then a close, nor its CLI ending while the link was
/// down (reported with no turn) fails them, or calls them unknown.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_agent_without_hooks_never_has_its_messages_failed() {
    let placements = k2_core::test_env::unique_temp_path("k2f3-unhooked.db");
    let open = |restore: bool| {
        let store = Arc::new(PlacementStore::open_file(&placements).unwrap());
        let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::with_store(store, None)));
        if restore {
            remote.restore();
        }
        remote
    };
    let remote = open(false);
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "sid", "/w/sid", &sid(1)).await;
    spawn_on(remote, &mut a, "tom", "/w/tom", &sid(2)).await;
    let et = epoch_of(remote, "tom");
    let m1 = delivered(&outbox, &mut a, "sid", "no hooks").await;
    let m2 = delivered(&outbox, &mut a, "tom", "no hooks either").await;
    close_on(remote, &mut a, "sid").await;
    stays_delivered(&outbox, &m1).await;
    drop(a);
    // A controller restart on the same table; tom's CLI ended meanwhile.
    let remote = open(true);
    let url = serve(remote).await;
    let outbox2 = outbox_for(remote);
    let _a = back(&url, "host-a", vec![], vec![ended("tom", &sid(2), et, None)]).await;
    until("tom to go", || remote.placement("tom").is_none()).await;
    stays_delivered(&outbox, &m2).await;
    assert!(outbox2.get(&m2).is_none(), "the new outbox never had it");
    std::fs::remove_file(&placements).unwrap();
}

/// K2-f.1 #6, K2-f.3 #6: a direct delivery's row is written whole or not
/// at all, and a failure says so.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn record_direct_is_one_transaction_and_reports_failure() {
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let outbox = outbox_for(remote);
    let body = json!({"kind": "msg", "text": "x"});
    let now = k2_core::delivery::now_ms();
    outbox.record_direct("m_0000000000008_one", "agent:alice", "uma", &body, now).unwrap();
    assert_eq!(outbox.get("m_0000000000008_one").unwrap().state, State::Delivered);
    let err = outbox.record_direct("m_0000000000008_one", "agent:alice", "uma", &body, now).unwrap_err();
    assert!(err.contains("already finished"), "{err}");
    assert!(outbox.record_direct("", "agent:alice", "uma", &body, now).is_err());
    assert_eq!(outbox.get("").map(|r| r.state), None, "nothing half written");
}

/// The direct path: the gone rule's wake, a host's message the controller
/// handles itself, and the controller's own `k2 msg`. Each runs under its
/// message's id (so the spawn or inject names it to the host), each has a
/// delivered row, and a crash in the turn the host tied it to fails it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_direct_delivery_its_host_tied_to_a_cut_turn_fails() {
    use crate::delivery_drain::{start_controller, Direct};
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let named: Arc<parking_lot::Mutex<Vec<Option<String>>>> = Arc::default();
    let seen = Arc::clone(&named);
    let outbox = start_controller(
        remote,
        mem_db(),
        FAST,
        Direct {
            resolves: Box::new(|_| true),
            aliases: Box::new(|_| Vec::new()),
            // The wake: the agent is placed by the time it answers, and the
            // delivery's id is on this thread for the spawn or inject.
            deliver: Box::new(move |to, _, _| {
                seen.lock().push(crate::delivery_drain::direct_delivery_id());
                for _ in 0..3000 {
                    if remote.placement(to).is_some_and(|p| p.session_id.is_some()) {
                        return Ok(());
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(format!("{to} never placed"))
            }),
        },
    );
    outbox.set_gone_grace(Duration::ZERO);
    let mut a = fake_host(&url, "host-a").await;

    // 1. The gone rule wakes vic (asleep) and delivers directly.
    remote.set_session_ended_for_test("vic", 0);
    let m1 = outbox.send("agent:alice", "vic", json!({"kind": "msg", "text": "wake up", "wake": true})).unwrap();
    until("the message to go to the direct path", || outbox.is_handing(&m1) || {
        for r in outbox.pass(k2_core::delivery::now_ms()) {
            let c = Arc::clone(&outbox);
            tokio::task::spawn_blocking(move || c.deliver_gone(&r));
        }
        outbox.is_handing(&m1)
    })
    .await;
    spawn_on(remote, &mut a, "vic", "/w/vic", &sid(1)).await;
    assert_eq!(wait_final(&outbox, &m1).await.state, State::Delivered);
    prompt_with(remote, &mut a, "vic", &sid(1), running(1, 0, &[&m1])).await;
    gone(&mut a, remote, "vic").await;
    assert_eq!(interrupted(&outbox, &m1).await, "interrupted: vic crashed mid-turn before it answered");

    // 2. A host's message the controller delivered itself, and 3. the
    // controller's own k2 msg: each now has a delivered row.
    spawn_on(remote, &mut a, "wes", "/w/wes", &sid(2)).await;
    let body = json!({"kind": "msg", "text": "direct"});
    let now = k2_core::delivery::now_ms();
    let (st, _) = tokio::task::block_in_place(|| outbox.deliver_direct("m_0000000000003_up", "agent:alice", "wes", &body, now));
    assert_eq!(st, crate::hosts::proto::DeliverStatus::Delivered);
    outbox.record_direct("m_0000000000004_own", "user:owner", "wes", &body, now).unwrap();
    prompt_with(remote, &mut a, "wes", &sid(2), running(1, 0, &["m_0000000000003_up", "m_0000000000004_own"])).await;
    gone(&mut a, remote, "wes").await;
    for id in ["m_0000000000003_up", "m_0000000000004_own"] {
        assert_eq!(interrupted(&outbox, id).await, "interrupted: wes crashed mid-turn before it answered", "{id}");
    }
    assert_eq!(named.lock().clone(), vec![Some(m1.clone()), Some("m_0000000000003_up".to_string())], "each direct delivery named its message");
}

/// #5 (K2-f.1): the controller restarts between a delivery and the crash
/// that cuts its turn. The turn and its matched message are in the
/// controller's store: restored, and the crash after the restart fails it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_turn_survives_a_controller_restart_and_a_crash_after_it_fails_the_message() {
    use crate::delivery_drain::{start_controller, Direct};
    let placements = k2_core::test_env::unique_temp_path("k2f-placements.db");
    let outbox_db = k2_core::test_env::unique_temp_path("k2f-outbox.db");
    let start = |restore: bool| {
        let store = Arc::new(PlacementStore::open_file(&placements).unwrap());
        let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::with_store(store, None)));
        if restore {
            remote.restore();
        }
        let db = Arc::new(parking_lot::ReentrantMutex::new(rusqlite::Connection::open(&outbox_db).unwrap()));
        let outbox = start_controller(
            remote,
            db,
            FAST,
            Direct {
                resolves: Box::new(|_| true),
                aliases: Box::new(|_| Vec::new()),
                deliver: Box::new(|_, _, _| panic!("no direct path here")),
            },
        );
        (remote, outbox)
    };
    let (remote, outbox) = start(false);
    let url = serve(remote).await;
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "sam", "/w/sam", &sid(1)).await;
    let epoch = epoch_of(remote, "sam");
    let m = delivered(&outbox, &mut a, "sam", "before the restart").await;
    prompt_with(remote, &mut a, "sam", &sid(1), running(1, 0, &[&m])).await;
    drop(a);

    // A new controller process on the same databases; the host's resume
    // says nothing of the turn (an older host): the stored turn stands.
    let (remote, outbox) = start(true);
    let url = serve(remote).await;
    let mut a = back(&url, "host-a", vec![resumed("sam", &sid(1), epoch, None)], vec![]).await;
    until("sam adopted mid-turn", || remote.in_turn("sam")).await;
    gone(&mut a, remote, "sam").await;
    assert_eq!(interrupted(&outbox, &m).await, "interrupted: sam crashed mid-turn before it answered");
    std::fs::remove_file(&placements).unwrap();
    std::fs::remove_file(&outbox_db).unwrap();
}

/// K2-f.4 #3: a turn row written by K2-f.1 (`current_json`) or K2-f.2
/// (`notes_json`) keeps its counts and running state; its lists were the
/// controller's estimates, not its host's evidence, and are dropped: a
/// crash after the upgrade fails none of those messages.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn older_turn_rows_keep_their_counts_and_drop_their_lists() {
    let placements = k2_core::test_env::unique_temp_path("k2f3-migrate.db");
    {
        let _store = PlacementStore::open_file(&placements).unwrap();
        let c = rusqlite::Connection::open(&placements).unwrap();
        for (agent, n) in [("ari", 1), ("bev", 2)] {
            c.execute(
                "INSERT INTO host_placements (agent_id, host_id, session_id, epoch) VALUES (?1, 'host-a', ?2, ?3)",
                rusqlite::params![agent, sid(n), n as i64 + 40],
            )
            .unwrap();
        }
        c.execute(
            "INSERT INTO host_turns (agent_id, pane, running, prompts, stops, pending_json, current_json) VALUES ('ari', ?1, 1, 2, 1, '[\"m_pending\"]', '[\"m_current\"]')",
            [sid(1)],
        )
        .unwrap();
        c.execute(
            "INSERT INTO host_turns (agent_id, pane, running, prompts, stops, notes_json) VALUES ('bev', ?1, 1, 3, 2, ?2)",
            rusqlite::params![sid(2), r#"[{"msg_id":"m_in_turn","prompt":3},{"msg_id":"m_next","prompt":4},{"msg_id":"m_old","prompt":2}]"#],
        )
        .unwrap();
    }
    let store = Arc::new(PlacementStore::open_file(&placements).unwrap());
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::with_store(store, None)));
    remote.restore();
    let outbox = outbox_for(remote);
    let now = k2_core::delivery::now_ms();
    for id in ["m_pending", "m_current", "m_in_turn", "m_next", "m_old"] {
        outbox.record_direct(id, "agent:alice", "x", &json!({"kind": "msg", "text": id}), now).unwrap();
    }
    let url = serve(remote).await;
    let mut a = back(&url, "host-a", vec![resumed("ari", &sid(1), 41, None), resumed("bev", &sid(2), 42, None)], vec![]).await;
    until("both adopted", || remote.in_turn("ari") && remote.in_turn("bev")).await;
    gone(&mut a, remote, "ari").await;
    gone(&mut a, remote, "bev").await;
    for id in ["m_pending", "m_current", "m_in_turn", "m_next", "m_old"] {
        stays_delivered(&outbox, id).await;
    }
    std::fs::remove_file(&placements).unwrap();
}

/// K2-f.2 #1: a controller that never followed an agent's turn (adopted
/// after an upgrade or a restart without its table) takes it from the
/// resume.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_adopted_agent_gets_its_turn_from_the_resume() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let _a = back(&url, "host-a", vec![resumed("bo", &sid(1), 7, Some(running(3, 2, &[])))], vec![]).await;
    until("bo adopted", || remote.placement("bo").is_some()).await;
    assert!(remote.in_turn("bo"), "in its third turn, as the host says");
}

/// K2-f.2 #2: a CLI nested in the lead's (a `claude -p` run by a Bash
/// tool) posts hooks for the same pane; its Stop doesn't end the lead's
/// turn, on the host or on the controller.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_nested_clis_hooks_are_not_the_leads() {
    use k2_core::agent_hooks::envelope::{parse, HookHeaders};
    let headers = HookHeaders::from_lookup(|k| (k.eq_ignore_ascii_case("X-K2-Pane")).then_some("p1"));
    let mut env = parse(&headers, br#"{"hook_event_name":"Stop"}"#).expect("envelope");
    assert!(!super::drain::is_subagent(&env));
    env.from_descendant = true;
    assert!(super::drain::is_subagent(&env), "a nested CLI is not the lead");

    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "cy", "/w/cy", &sid(1)).await;
    prompt_with(remote, &mut a, "cy", &sid(1), running(1, 0, &[])).await;
    let nested = Frame::Hook {
        pane: sid(1),
        event: "Stop".into(),
        session_id: Some(sid(1)),
        conversation: None,
        agent_pid: None,
        at_ms: 0,
        epoch: None,
        subagent: true,
        turn: None,
    };
    send(&mut a, &nested).await;
    hook(&mut a, &sid(1), "PreToolUse").await;
    until("the hooks", || remote.status_json()["hosts"][0]["hooks"].as_u64() >= Some(3)).await;
    assert!(remote.in_turn("cy"), "the lead's turn still runs");
}

/// K2-f.2 #1: hooks lost on the way (a lagging forwarder) are in the next
/// hook's absolute counts. A goes in prompt 1, B in prompt 2; A's Stop and
/// B's prompt are lost; a tool hook in B's turn carries them. The host is
/// then lost: A is known answered, B's outcome is unknown.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absolute_counts_survive_hooks_the_forwarder_dropped() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "ada", "/w/ada", &sid(1)).await;
    let ma = delivered(&outbox, &mut a, "ada", "A").await;
    let mb = delivered(&outbox, &mut a, "ada", "B").await;
    prompt_with(remote, &mut a, "ada", &sid(1), running(1, 0, &[&ma])).await;
    hook_at(&mut a, &sid(1), "PreToolUse", running(2, 1, &[&mb])).await;
    spawn_on(remote, &mut a, "bea", "/w/bea", &sid(2)).await; // the hook is in by now (one link, in order)
    drop(a);
    wait_lost(remote, "host-a").await;
    let moved = remote.move_lost(std::time::Instant::now(), Duration::ZERO);
    assert!(moved.contains(&"ada".to_string()), "{moved:?}");
    assert_eq!(interrupted(&outbox, &mb).await, "outcome unknown: host lost mid-turn (ada had not answered when its host was lost)");
    stays_delivered(&outbox, &ma).await;
}

/// K2-f.2 #6, lab's S10: attempt 1 goes to host-1, killed before it acks;
/// the agent moves to host-2, where attempt 2 is the first real delivery
/// and its prompt carries it. The CLI is killed mid-turn: it fails.
/// Attempt 1, never acked, fails nothing (the row was only `sent`). And a
/// resend the host calls a duplicate takes nothing back: notes come from
/// the host's prompts, not from sends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attempt_one_lost_unacked_attempt_two_delivered_then_a_crash_fails_it() {
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-1").await;
    spawn_on(remote, &mut a, "notes", "/w/notes", &sid(1)).await;
    let m = outbox.send("agent:alice", "notes", json!({"kind": "msg", "text": "turn three"})).unwrap();
    assert!(matches!(recv(&mut a).await, Frame::Deliver { attempt: 1, .. }));
    drop(a); // SIGKILL before the ack
    wait_lost(remote, "host-1").await;
    assert_eq!(remote.move_lost(std::time::Instant::now(), Duration::ZERO), vec!["notes"]);
    assert_eq!(outbox.get(&m).unwrap().state, State::Sent);

    let mut b = fake_host(&url, "host-2").await;
    wait_hosts(remote, 1).await;
    spawn_on(remote, &mut b, "notes", "/w/notes", &sid(2)).await;
    let Frame::Deliver { msg_id, attempt, .. } = recv(&mut b).await else { panic!("expected the resend") };
    assert_eq!((msg_id.as_str(), attempt > 1), (m.as_str(), true));
    send(&mut b, &Frame::DeliverAck { msg_id: msg_id.clone(), status: crate::hosts::proto::DeliverStatus::Delivered, reason: None }).await;
    assert_eq!(wait_final(&outbox, &m).await.state, State::Delivered);
    prompt_with(remote, &mut b, "notes", &sid(2), running(1, 0, &[&m])).await;
    // A late duplicate ack of the same message changes nothing.
    send(&mut b, &Frame::DeliverAck { msg_id, status: crate::hosts::proto::DeliverStatus::Duplicate, reason: None }).await;
    gone(&mut b, remote, "notes").await;
    assert_eq!(interrupted(&outbox, &m).await, "interrupted: notes crashed mid-turn before it answered");
}

/// K2-f.2 #7: the v2 close route (HTTP) and a drain's release at its
/// deadline cut a running turn like `unplace` does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_close_route_and_a_drain_release_cut_a_running_turn() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "hy", "/w/hy", &sid(1)).await;
    spawn_on(remote, &mut a, "io", "/w/io", &sid(2)).await;
    let mh = delivered(&outbox, &mut a, "hy", "closed").await;
    let mi = delivered(&outbox, &mut a, "io", "drained").await;
    prompt_with(remote, &mut a, "hy", &sid(1), running(1, 0, &[&mh])).await;
    prompt_with(remote, &mut a, "io", &sid(2), running(1, 0, &[&mi])).await;

    let close = tokio::spawn(async move { remote.close(br#"{"agent_name":"hy"}"#).await });
    match recv(&mut a).await {
        Frame::Close { req, .. } => send(&mut a, &Frame::Reply { req, status: "200 OK".into(), body: json!({"closed": true}).to_string() }).await,
        other => panic!("expected the close, got {other:?}"),
    }
    assert_eq!(within("the close", close).await.unwrap().0, "200 OK");
    assert_eq!(interrupted(&outbox, &mh).await, "interrupted: hy was closed mid-turn before it answered");

    send(&mut a, &Frame::Released { req: 0, agent_id: "io".into(), epoch: 0, ring_end: None }).await;
    assert_eq!(interrupted(&outbox, &mi).await, "interrupted: io was closed mid-turn before it answered");
    assert!(!remote.in_turn("hy") && !remote.in_turn("io"));
}

/// Lab S10, seed 7 round 3 (wave3 6ffcbb9da): a cut before the delivery's
/// ack. The message is typed on host-2 and its prompt starts the turn (the
/// host tied it), then host-2 is SIGKILLed before it acks: the row is still
/// `sent`. host-2 relinks without the agent: adopt says gone, the turn is
/// cut, and the row ends failed then, with the cut's reason. host-2's
/// relink report then re-sends its ack from its seen table: the row stays
/// failed (never delivered-and-unanswered), and it is not resent. A message
/// whose ack died the same way but that no prompt carried ends delivered
/// with its late ack, as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cut_before_the_ack_keeps_its_result_when_the_ack_comes_late() {
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut b = fake_host(&url, "host-2").await;
    spawn_on(remote, &mut b, "notes", "/w/notes", &sid(1)).await;
    spawn_on(remote, &mut b, "pal", "/w/pal", &sid(2)).await;
    let m = outbox.send("agent:alice", "notes", json!({"kind": "msg", "text": "turn three"})).unwrap();
    assert!(matches!(recv(&mut b).await, Frame::Deliver { msg_id, .. } if msg_id == m));
    // (Messages go out one at a time per recipient: the control is pal's.)
    let n = outbox.send("agent:alice", "pal", json!({"kind": "msg", "text": "typed, never submitted"})).unwrap();
    within("pal's delivery", async {
        loop {
            if let Frame::Deliver { msg_id, .. } = recv(&mut b).await {
                if msg_id == n {
                    break;
                }
            }
        }
    })
    .await;
    prompt_with(remote, &mut b, "notes", &sid(1), running(1, 0, &[&m])).await;
    assert_eq!((outbox.get(&m).unwrap().state, outbox.get(&n).unwrap().state), (State::Sent, State::Sent), "neither acked");
    drop(b); // SIGKILL before the acks
    wait_lost(remote, "host-2").await;

    // Its daemon restarts without notes or pal: gone, notes' turn cut.
    let mut b = back(&url, "host-2", vec![], vec![]).await;
    let why = "outcome unknown: host lost mid-turn (notes had not answered when its host was lost)";
    assert_eq!(interrupted(&outbox, &m).await, why);
    // The relink report: both acks, late.
    for id in [&m, &n] {
        send(&mut b, &Frame::DeliverAck { msg_id: id.clone(), status: crate::hosts::proto::DeliverStatus::Delivered, reason: Some("reported on relink".into()) }).await;
    }
    assert_eq!(wait_final(&outbox, &n).await.state, State::Delivered, "no evidence it was in the cut turn");
    for _ in 0..3 {
        outbox.pass(k2_core::delivery::now_ms());
    }
    let row = outbox.get(&m).unwrap();
    assert_eq!((row.state, row.reason.as_deref(), row.attempts), (State::Failed, Some(why), 1), "the cut's result stands; not resent");
}

/// A host lost hard mid-turn (PRD: the turn in progress is lost). The
/// message whose prompt that turn had (tied by the host) ends `outcome
/// unknown` at the move and isn't delivered again. Every other message
/// reaches the agent's new placement exactly once: one sent but never
/// acked nor tied (it died in flight), and one queued while the host was
/// away. One acked but never tied (folded) stays delivered: it is not sent
/// again either.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn after_a_hard_loss_only_untyped_messages_reach_the_new_placement_once() {
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-1").await;
    spawn_on(remote, &mut a, "nia", "/w/nia", &sid(1)).await;
    let m = delivered(&outbox, &mut a, "nia", "tied to the turn").await;
    let n = delivered(&outbox, &mut a, "nia", "acked, folded").await;
    prompt_with(remote, &mut a, "nia", &sid(1), running(1, 0, &[&m])).await;
    // u: sent, and host-1 dies before acking (nor did a prompt carry it).
    let u = outbox.send("agent:alice", "nia", json!({"kind": "msg", "text": "in flight"})).unwrap();
    assert!(matches!(recv(&mut a).await, Frame::Deliver { msg_id, .. } if msg_id == u));
    drop(a); // SIGKILL
    wait_lost(remote, "host-1").await;
    // q: sent while host-1 is gone.
    let q = outbox.send("agent:alice", "nia", json!({"kind": "msg", "text": "during the outage"})).unwrap();
    assert_eq!(remote.move_lost(std::time::Instant::now(), Duration::ZERO), vec!["nia"]);
    assert_eq!(interrupted(&outbox, &m).await, "outcome unknown: host lost mid-turn (nia had not answered when its host was lost)");

    let mut b = fake_host(&url, "host-2").await;
    wait_hosts(remote, 1).await;
    spawn_on(remote, &mut b, "nia", "/w/nia", &sid(2)).await;
    let mut got: Vec<String> = Vec::new();
    within("u and q on host-2", async {
        while !(got.contains(&u) && got.contains(&q)) {
            if let Frame::Deliver { msg_id, .. } = recv(&mut b).await {
                send(&mut b, &Frame::DeliverAck { msg_id: msg_id.clone(), status: crate::hosts::proto::DeliverStatus::Delivered, reason: None }).await;
                got.push(msg_id);
            }
        }
    })
    .await;
    for id in [&u, &q] {
        assert_eq!(wait_final(&outbox, id).await.state, State::Delivered, "{id}");
    }
    // Nothing more comes to host-2: no second copy, and neither m nor n.
    for _ in 0..3 {
        outbox.pass(k2_core::delivery::now_ms());
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    while let Ok(Some(Ok(Message::Text(t)))) = tokio::time::timeout(Duration::from_millis(50), b.next()).await {
        if let Ok(Frame::Deliver { msg_id, .. }) = Frame::decode(&t) {
            got.push(msg_id);
        }
    }
    got.sort();
    let mut want = vec![u.clone(), q.clone()];
    want.sort();
    assert_eq!(got, want, "u and q once each; m and n not again");
    assert_eq!(outbox.get(&n).unwrap().state, State::Delivered);
    assert_eq!(outbox.get(&m).unwrap().state, State::Failed);
}

// ── P3 T3: a laptop that comes and goes ────────────────────────────────

/// A fake laptop (`k2.kind=laptop`), linking with `resume`.
async fn laptop(url: &str, name: &str, resume: Vec<super::proto::ResumePlacement>) -> Ws {
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.expect("dial");
    let hello = Frame::Hello { protocol: PROTOCOL, host_name: name.into(), token: TOKEN.into(), labels: vec!["k2.kind=laptop".into()], resume, ended: vec![] };
    send(&mut ws, &hello).await;
    assert!(matches!(recv(&mut ws).await, Frame::Welcome { .. }));
    ws
}

/// A host that stays awake: a task reads its link (answering the hub's
/// pings) and passes its frames on.
fn awake(mut ws: Ws) -> tokio::sync::mpsc::UnboundedReceiver<Frame> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(Ok(m)) = ws.next().await {
            if let Message::Text(t) = m {
                let _ = tx.send(Frame::decode(&t).expect("frame"));
            }
        }
    });
    rx
}

async fn wait_away(remote: &RemoteHost, host: &str) -> Value {
    within("the host to be away", async {
        loop {
            let s = remote.status_json();
            if s["away"].as_array().is_some_and(|a| a.iter().any(|h| h == host)) {
                return s;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
}

/// S11: the laptop's lid closes with notes on it. Its link stays open but
/// nothing comes (no frames, no pongs): after `K2_HOST_AWAY_AFTER` the hub
/// closes it, the laptop is away (silent) and notes is paused, not placed
/// on host-1 although host-1 has room. A message for notes from a cloud
/// agent is accepted and held, and nothing is sent. The lid opens: the
/// laptop links again still running notes (W3), and the message is
/// delivered on its first attempt, once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_silent_laptop_is_away_its_agent_paused_and_its_message_held_until_it_is_back() {
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new().with_away_after(Duration::from_secs(2))));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut lap = laptop(&url, "eric-mbp", vec![]).await;
    let mut host1 = awake(fake_host(&url, "host-1").await);
    wait_hosts(remote, 2).await;
    spawn_on(remote, &mut lap, "notes", "/w/notes", &sid(1)).await;
    let e1 = remote.placement("notes").unwrap().epoch.0;
    assert_eq!(
        remote.policy().get("notes").unwrap().mode,
        crate::provisioner::Mode::Pinned { host: "eric-mbp".into() },
        "first placed on a laptop: pinned (D5)"
    );

    // The lid closes: the test stops reading the laptop's link.
    let before = k2_core::delivery::now_ms();
    let status = wait_away(remote, "eric-mbp").await;
    let away = &status["away_hosts"].as_array().unwrap().iter().find(|h| h["host_id"] == "eric-mbp").unwrap().clone();
    assert!(away["why"].as_str().unwrap().starts_with("silent for "), "{away}");
    let since = away["since_ms"].as_i64().unwrap();
    assert!(since <= before + 1, "away from when it was last heard: {since} vs {before}");
    assert_eq!(status["paused"], json!([{"agent": "notes", "host": "eric-mbp", "epoch": e1}]));
    assert_eq!(remote.placement("notes").unwrap().host_id, "eric-mbp", "kept (W3)");

    // A message from a cloud agent (the up hop): accepted, held.
    let id = "m_0000000000001_laptop".to_string();
    let body = json!({"kind": "msg", "text": "after the lid closed"});
    let (status, reason) = outbox.accept(&id, "agent:alice", "notes", &body, k2_core::delivery::now_ms());
    assert_eq!(status, crate::hosts::proto::DeliverStatus::Delivered);
    let held: crate::agent_host::remote::Held = serde_json::from_str(&reason.expect("held")).unwrap();
    assert_eq!((held.agent.as_str(), held.host.as_str(), held.away_since_ms), ("notes", "eric-mbp", Some(since)));
    assert!(held.why.unwrap().starts_with("silent for "));
    for _ in 0..3 {
        assert!(outbox.pass(k2_core::delivery::now_ms()).is_empty());
    }
    let row = outbox.get(&id).unwrap();
    assert_eq!((row.state, row.attempts), (State::Queued, 0), "held: nothing sent to an away host");
    // The app's spawn for notes goes to its placement, on the away laptop:
    // refused, not placed on host-1.
    let wake = json!({"agent_name": "notes", "cwd": "/w/notes", "command": "claude"}).to_string();
    let (code, why) = remote.spawn(wake.as_bytes()).await;
    assert!(!code.starts_with('2'), "{code} {why}");
    assert_eq!(remote.placement("notes").unwrap().host_id, "eric-mbp");
    assert!(host1.try_recv().is_err(), "nothing for notes on host-1");

    // The lid opens: the laptop links again, still running notes.
    drop(lap);
    let mut lap = {
        let resume = super::proto::ResumePlacement {
            agent_id: "notes".into(),
            session_id: sid(1),
            epoch: e1,
            ring: None,
            child_alive: true,
            cwd: Some("/w/notes".into()),
            program: Some("claude".into()),
            args: vec![],
            label: None,
            turn: None,
            adopted: false,
        };
        laptop(&url, "eric-mbp", vec![resume]).await
    };
    match recv(&mut lap).await {
        Frame::Deliver { msg_id, agent_id, attempt, epoch, .. } => {
            assert_eq!((msg_id.as_str(), agent_id.as_str(), attempt, epoch), (id.as_str(), "notes", 1, Some(e1)));
            send(&mut lap, &Frame::DeliverAck { msg_id, status: crate::hosts::proto::DeliverStatus::Delivered, reason: None }).await;
        }
        other => panic!("expected the held message, got {other:?}"),
    }
    let row = wait_final(&outbox, &id).await;
    assert_eq!((row.state, row.attempts), (State::Delivered, 1), "delivered once");
    assert!(outbox.held("notes").is_none());
    let status = remote.status_json();
    assert_eq!(status["paused"], json!([]));
    assert!(!status["away"].as_array().unwrap().iter().any(|h| h == "eric-mbp"));
    assert!(host1.try_recv().is_err(), "nothing for notes on host-1");
}

/// S11 with `--wake`: lap sleeps (no placement), pinned to a laptop that
/// is away. A wake message is not handed to the cascade (which can't place
/// it); the outbox holds it. When the laptop links, the direct path wakes
/// lap there and delivers it, once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wake_for_a_sleeping_agent_pinned_to_an_away_laptop_is_held_not_cascaded() {
    use crate::delivery_drain::{start_controller, Direct, Taken};
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let delivered: Arc<parking_lot::Mutex<Vec<String>>> = Arc::default();
    let seen = Arc::clone(&delivered);
    let outbox = start_controller(
        remote,
        mem_db(),
        FAST,
        Direct {
            resolves: Box::new(|_| true),
            aliases: Box::new(|_| Vec::new()),
            deliver: Box::new(move |to, _, _| {
                seen.lock().push(to.to_string());
                Ok(())
            }),
        },
    );
    outbox.set_gone_grace(Duration::ZERO);
    remote.policy().set_mode("lap", &crate::provisioner::Mode::Pinned { host: "eric-mbp".into() }).unwrap();
    let body = json!({"kind": "msg", "text": "wake up", "wake": true, "wake_timeout_ms": 1000});
    assert!(!outbox.is_direct("lap", &body), "held, not the cascade");
    let held = outbox.held("lap").expect("held");
    assert_eq!((held.agent.as_str(), held.host.as_str(), held.away_since_ms), ("lap", "eric-mbp", None), "never seen: no since");

    let id = "m_0000000000002_wake".to_string();
    match outbox.take(&id, "agent:alice", "lap", &body, k2_core::delivery::now_ms()) {
        Taken::Ack(crate::hosts::proto::DeliverStatus::Delivered, Some(reason)) => {
            assert_eq!(serde_json::from_str::<crate::agent_host::remote::Held>(&reason).unwrap(), held);
        }
        other => panic!("expected an accepted, held message, got {other:?}"),
    }
    for _ in 0..3 {
        assert!(outbox.pass(k2_core::delivery::now_ms()).is_empty(), "not handed to the direct path while away");
    }
    assert_eq!(outbox.get(&id).unwrap().state, State::Queued);
    assert!(delivered.lock().is_empty());

    // The laptop links: the direct path wakes lap and delivers. The
    // controller's own drain loop may hand the row over first; either way
    // it goes once.
    let _lap = laptop(&url, "eric-mbp", vec![]).await;
    wait_hosts(remote, 1).await;
    assert!(outbox.held("lap").is_none());
    within("the held wake to be delivered", async {
        while outbox.get(&id).unwrap().state != State::Delivered {
            for r in outbox.pass(k2_core::delivery::now_ms()) {
                outbox.deliver_gone(&r);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(outbox.pass(k2_core::delivery::now_ms()).is_empty());
    assert_eq!(*delivered.lock(), vec!["lap".to_string()], "once");
}
