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
            send(&mut a, &Frame::DeliverAck { msg_id: got, status: crate::hosts::proto::DeliverStatus::Delivered, reason: None, prompt: None }).await;
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
    };
    send(&mut b, &Frame::Hello { protocol: PROTOCOL, host_name: "host-b".into(), token: TOKEN.into(), labels: vec![], resume: vec![resume], ended: vec![] })
        .await;
    assert!(matches!(recv(&mut b).await, Frame::Welcome { .. }));
    match recv(&mut b).await {
        Frame::Deliver { msg_id, agent_id, .. } => {
            assert_eq!((msg_id.as_str(), agent_id.as_str()), (id.as_str(), "dave"));
            send(&mut b, &Frame::DeliverAck { msg_id, status: crate::hosts::proto::DeliverStatus::Delivered, reason: None, prompt: None }).await;
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
            prompt: None,
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
    send(&mut a, &Frame::DeliverAck { msg_id: id.clone(), status: crate::hosts::proto::DeliverStatus::Rejected, reason: Some(why), prompt: None }).await;
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
            send(host, &Frame::DeliverAck { msg_id: m, status: crate::hosts::proto::DeliverStatus::Delivered, reason: None, prompt: None }).await;
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
            send(host, &Frame::DeliverAck { msg_id, status: crate::hosts::proto::DeliverStatus::Delivered, reason: None, prompt: None }).await;
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

// ── K2-f: a turn cut by a crash fails its unanswered messages ──────────

async fn turn_started(remote: &RemoteHost, host: &mut Ws, agent: &str, pane: &str) {
    hook(host, pane, "UserPromptSubmit").await;
    within("the turn to start", async {
        while !remote.in_turn(agent) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
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

/// The CLI is SIGKILLed mid-turn: the host reports its session gone. The
/// message it was answering fails `interrupted … crashed`, not resent. A
/// CLI that exits after its turn ended leaves its message delivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cli_that_exits_mid_turn_fails_the_message_it_was_answering() {
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "fan", "/w/fan", &sid(1)).await;
    spawn_on(remote, &mut a, "gus", "/w/gus", &sid(2)).await;

    let m1 = delivered(&outbox, &mut a, "fan", "turn three").await;
    turn_started(remote, &mut a, "fan", &sid(1)).await;
    gone(&mut a, remote, "fan").await;
    let why = interrupted(&outbox, &m1).await;
    assert_eq!(why, "interrupted: fan crashed mid-turn before it answered");
    assert!(!remote.in_turn("fan"));

    let m2 = delivered(&outbox, &mut a, "gus", "answered").await;
    turn_started(remote, &mut a, "gus", &sid(2)).await;
    hook(&mut a, &sid(2), "Stop").await;
    within("gus's turn to end", async {
        while remote.in_turn("gus") {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    gone(&mut a, remote, "gus").await;
    for _ in 0..3 {
        outbox.pass(k2_core::delivery::now_ms());
    }
    assert_eq!(outbox.get(&m2).unwrap().state, State::Delivered, "answered before its CLI went");
    assert_eq!(outbox.get(&m1).unwrap().attempts, 1, "not resent");
}

/// The host is SIGKILLed mid-turn. While it is only lost nothing is cut
/// (it may come back still running the turn: a partition). Its daemon
/// restarts without hal and knows nothing of hal's turn: adopt answers
/// gone, and hal's message fails with its outcome unknown (not "crashed":
/// nothing says the turn didn't end before the host went). ivy, resumed by
/// the same host mid-turn, keeps its message and finishes its turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_back_without_an_agent_that_was_mid_turn_fails_its_message() {
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "hal", "/w/hal", &sid(1)).await;
    spawn_on(remote, &mut a, "ivy", "/w/ivy", &sid(2)).await;
    let ivy_epoch = remote.placement("ivy").unwrap().epoch.0;
    let m1 = delivered(&outbox, &mut a, "hal", "for hal").await;
    let m2 = delivered(&outbox, &mut a, "ivy", "for ivy").await;
    turn_started(remote, &mut a, "hal", &sid(1)).await;
    turn_started(remote, &mut a, "ivy", &sid(2)).await;

    drop(a);
    wait_lost(remote, "host-a").await;
    for _ in 0..3 {
        outbox.pass(k2_core::delivery::now_ms());
    }
    assert_eq!((outbox.get(&m1).unwrap().state, outbox.get(&m2).unwrap().state), (State::Delivered, State::Delivered), "lost is not gone");

    let (mut a, _) = tokio_tungstenite::connect_async(url.as_str()).await.expect("dial");
    let ivy = super::proto::ResumePlacement {
        agent_id: "ivy".into(),
        session_id: sid(2),
        epoch: ivy_epoch,
        ring: None,
        child_alive: true,
        cwd: Some("/w/ivy".into()),
        program: Some("claude".into()),
        args: vec![],
        label: None,
        turn: None,
    };
    send(&mut a, &Frame::Hello { protocol: PROTOCOL, host_name: "host-a".into(), token: TOKEN.into(), labels: vec![], resume: vec![ivy], ended: vec![] }).await;
    assert!(matches!(recv(&mut a).await, Frame::Welcome { .. }));
    assert_eq!(interrupted(&outbox, &m1).await, "outcome unknown: host lost mid-turn (hal had not answered when its host was lost)");
    assert!(remote.placement("hal").is_none());
    assert!(remote.in_turn("ivy"), "adopted mid-turn");
    hook(&mut a, &sid(2), "Stop").await;
    within("ivy's turn to end", async {
        while remote.in_turn("ivy") {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(outbox.get(&m2).unwrap().state, State::Delivered);
}

/// A portable agent mid-turn on a host lost past K2_HOST_MOVE_AFTER moves:
/// the turn it had there is gone with the host, and its message fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_agent_moved_off_a_lost_host_mid_turn_fails_its_message() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "jo", "/w/jo", &sid(1)).await;
    let m = delivered(&outbox, &mut a, "jo", "before the crash").await;
    turn_started(remote, &mut a, "jo", &sid(1)).await;
    drop(a);
    wait_lost(remote, "host-a").await;
    assert_eq!(remote.move_lost(std::time::Instant::now(), Duration::ZERO), vec!["jo"]);
    assert_eq!(interrupted(&outbox, &m).await, "outcome unknown: host lost mid-turn (jo had not answered when its host was lost)");
}

/// The direct path delivers without the outbox's deliver/ack: a wake of a
/// sleeping agent (the gone rule), a host's message the controller handles
/// itself, the controller's own `k2 msg`. Each is followed like an outbox
/// delivery, so a crash mid-turn fails it too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_direct_delivery_cut_by_a_crash_fails_too() {
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
            // The wake: the agent is placed by the time it answers.
            deliver: Box::new(move |to, _, _| {
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

    // 1. The gone rule wakes lee (asleep) and delivers directly.
    remote.set_session_ended_for_test("lee", 0);
    let m1 = outbox.send("agent:alice", "lee", json!({"kind": "msg", "text": "wake up", "wake": true})).unwrap();
    // Handed to the direct path (by this pass or the drain's own) before
    // lee is placed, so it never goes the outbox's way.
    within("the message to go to the direct path", async {
        while !outbox.is_handing(&m1) {
            for r in outbox.pass(k2_core::delivery::now_ms()) {
                let c = Arc::clone(&outbox);
                tokio::task::spawn_blocking(move || c.deliver_gone(&r));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    spawn_on(remote, &mut a, "lee", "/w/lee", &sid(1)).await;
    assert_eq!(wait_final(&outbox, &m1).await.state, State::Delivered);
    turn_started(remote, &mut a, "lee", &sid(1)).await;
    gone(&mut a, remote, "lee").await;
    assert_eq!(interrupted(&outbox, &m1).await, "interrupted: lee crashed mid-turn before it answered");

    // 2. A host's message the controller delivered itself, and 3. the
    // controller's own k2 msg: each now has a delivered row.
    spawn_on(remote, &mut a, "max", "/w/max", &sid(2)).await;
    let body = json!({"kind": "msg", "text": "direct"});
    let now = k2_core::delivery::now_ms();
    let (st, _) = tokio::task::block_in_place(|| outbox.deliver_direct("m_0000000000003_up", "agent:alice", "max", &body, now));
    assert_eq!(st, crate::hosts::proto::DeliverStatus::Delivered);
    outbox.record_direct("m_0000000000004_own", "user:owner", "max", &body, &outbox.turn_mark("max"), now).unwrap();
    for id in ["m_0000000000003_up", "m_0000000000004_own"] {
        assert_eq!(outbox.get(id).unwrap().state, State::Delivered, "{id}");
    }
    turn_started(remote, &mut a, "max", &sid(2)).await;
    gone(&mut a, remote, "max").await;
    for id in ["m_0000000000003_up", "m_0000000000004_own"] {
        assert_eq!(interrupted(&outbox, id).await, "interrupted: max crashed mid-turn before it answered", "{id}");
    }
}

// ── K2-f.1: the host's own turn counts ─────────────────────────────────

fn resumed(agent: &str, session: &str, epoch: u64, alive: bool, turn: super::proto::TurnCounts) -> super::proto::ResumePlacement {
    super::proto::ResumePlacement {
        agent_id: agent.into(),
        session_id: session.into(),
        epoch,
        child_alive: alive,
        cwd: Some(format!("/w/{agent}")),
        program: Some("claude".into()),
        turn: Some(turn),
        ..Default::default()
    }
}

async fn back(url: &str, host: &str, resume: Vec<super::proto::ResumePlacement>) -> Ws {
    back_with_ended(url, host, resume, vec![]).await
}

async fn back_with_ended(url: &str, host: &str, resume: Vec<super::proto::ResumePlacement>, ended: Vec<super::proto::EndedPlacement>) -> Ws {
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.expect("dial");
    send(&mut ws, &Frame::Hello { protocol: PROTOCOL, host_name: host.into(), token: TOKEN.into(), labels: vec![], resume, ended }).await;
    assert!(matches!(recv(&mut ws).await, Frame::Welcome { .. }));
    ws
}

fn ended(agent: &str, session: &str, epoch: u64, turn: super::proto::TurnCounts) -> super::proto::EndedPlacement {
    super::proto::EndedPlacement { agent_id: agent.into(), session_id: session.into(), epoch, turn }
}

fn counts(prompts: u64, stops: u64, running: bool) -> super::proto::TurnCounts {
    super::proto::TurnCounts { prompts, stops, running }
}

/// #2: the link drops mid-turn; the turn ends while it is down (its Stop
/// hook is never forwarded). The host comes back and says so in its
/// resume: kay's turn ended, and a later close doesn't fail the message
/// it answered. lou's CLI exited during the outage after answering: gone,
/// its message delivered. mo's CLI died mid-turn during the outage: its
/// message fails "crashed" (the host knows).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_missed_in_a_link_outage_comes_back_in_the_hosts_resume() {
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "kay", "/w/kay", &sid(1)).await;
    spawn_on(remote, &mut a, "lou", "/w/lou", &sid(2)).await;
    spawn_on(remote, &mut a, "mo", "/w/mo", &sid(3)).await;
    let epoch = |agent: &str| remote.placement(agent).unwrap().epoch.0;
    let (ek, el, em) = (epoch("kay"), epoch("lou"), epoch("mo"));
    let mk = delivered(&outbox, &mut a, "kay", "for kay").await;
    let ml = delivered(&outbox, &mut a, "lou", "for lou").await;
    let mm = delivered(&outbox, &mut a, "mo", "for mo").await;
    turn_started(remote, &mut a, "kay", &sid(1)).await;
    turn_started(remote, &mut a, "lou", &sid(2)).await;
    turn_started(remote, &mut a, "mo", &sid(3)).await;
    drop(a);
    wait_lost(remote, "host-a").await;

    let mut a = back_with_ended(
        &url,
        "host-a",
        vec![resumed("kay", &sid(1), ek, true, counts(1, 1, false))],
        vec![ended("lou", &sid(2), el, counts(1, 1, false)), ended("mo", &sid(3), em, counts(1, 0, true))],
    )
    .await;
    assert_eq!(interrupted(&outbox, &mm).await, "interrupted: mo crashed mid-turn before it answered");
    assert!(remote.placement("lou").is_none() && remote.placement("mo").is_none(), "gone");
    assert!(!remote.in_turn("kay"), "its missed Stop applied");
    close_on(remote, &mut a, "kay").await;
    for _ in 0..3 {
        outbox.pass(k2_core::delivery::now_ms());
    }
    assert_eq!((outbox.get(&mk).unwrap().state, outbox.get(&ml).unwrap().state), (State::Delivered, State::Delivered), "answered");
}

/// #4: the host reports a CLI's exit with its turn counts. Its last Stop
/// is in them even if that hook arrives after `session_gone`, or never.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_gone_carries_the_turn_so_a_late_stop_hook_cant_fail_an_answer() {
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "ned", "/w/ned", &sid(1)).await;
    let m = delivered(&outbox, &mut a, "ned", "answered, then exited").await;
    turn_started(remote, &mut a, "ned", &sid(1)).await;
    // The CLI answers and exits; session_gone overtakes its Stop hook.
    send(&mut a, &Frame::SessionGone { agent_id: "ned".into(), epoch: None, exit_code: Some(0), turn: Some(counts(1, 1, false)) }).await;
    within("ned to go", async {
        while remote.placement("ned").is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    hook(&mut a, &sid(1), "Stop").await;
    for _ in 0..3 {
        outbox.pass(k2_core::delivery::now_ms());
    }
    assert_eq!(outbox.get(&m).unwrap().state, State::Delivered);
}

/// #3: a note can only be made after its delivery answered (the direct
/// path), or the ack comes late. If the prompt was submitted and its turn
/// ended meanwhile, the note doesn't carry into the next turn: a crash in
/// that one doesn't fail a message it never had. A note whose prompt runs
/// in the turn now is in that turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_note_lands_in_the_turn_that_had_its_prompt_not_the_next() {
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "oz", "/w/oz", &sid(1)).await;
    let body = json!({"kind": "msg", "text": "direct"});

    // A direct delivery whose turn ran and ended before it answered.
    let mark = outbox.turn_mark("oz");
    turn_started(remote, &mut a, "oz", &sid(1)).await;
    hook(&mut a, &sid(1), "Stop").await;
    within("oz's turn to end", async {
        while remote.in_turn("oz") {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    outbox.record_direct("m_0000000000005_late", "agent:alice", "oz", &body, &mark, k2_core::delivery::now_ms()).unwrap();
    // And one submitted in the turn that runs now: in it.
    let mark = outbox.turn_mark("oz");
    turn_started(remote, &mut a, "oz", &sid(1)).await;
    outbox.record_direct("m_0000000000006_now", "agent:alice", "oz", &body, &mark, k2_core::delivery::now_ms()).unwrap();
    // An outbox message whose ack comes after its turn's Stop: noted when
    // it was sent, so that Stop answers it.
    let m = outbox.send("agent:alice", "oz", json!({"kind": "msg", "text": "late ack"})).unwrap();
    let Frame::Deliver { msg_id, .. } = recv(&mut a).await else { panic!("expected the delivery") };
    assert_eq!(msg_id, m);
    hook(&mut a, &sid(1), "Stop").await;
    turn_started(remote, &mut a, "oz", &sid(1)).await; // the next turn: the late ack's prompt
    hook(&mut a, &sid(1), "Stop").await;
    send(&mut a, &Frame::DeliverAck { msg_id: m.clone(), status: crate::hosts::proto::DeliverStatus::Delivered, reason: None, prompt: None }).await;
    assert_eq!(wait_final(&outbox, &m).await.state, State::Delivered);

    // A new turn, then the CLI dies in it: none of the three was in it.
    turn_started(remote, &mut a, "oz", &sid(1)).await;
    gone(&mut a, remote, "oz").await;
    for _ in 0..3 {
        outbox.pass(k2_core::delivery::now_ms());
    }
    for id in ["m_0000000000005_late", "m_0000000000006_now", m.as_str()] {
        assert_eq!(outbox.get(id).unwrap().state, State::Delivered, "{id}");
    }
}

/// #3, the other side: the "now" note above is in its turn, and a crash in
/// that turn fails it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_note_for_the_running_turn_is_cut_with_it() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "pia", "/w/pia", &sid(1)).await;
    let mark = outbox.turn_mark("pia");
    turn_started(remote, &mut a, "pia", &sid(1)).await;
    outbox.record_direct("m_0000000000007_run", "agent:alice", "pia", &json!({"kind": "msg", "text": "x"}), &mark, k2_core::delivery::now_ms()).unwrap();
    gone(&mut a, remote, "pia").await;
    assert_eq!(interrupted(&outbox, "m_0000000000007_run").await, "interrupted: pia crashed mid-turn before it answered");
}

/// #1: the host's daemon restarts while an agent's turn runs. Without
/// ptyd adoption (S3) the CLI does not outlive its daemon, and the
/// restarted daemon knows nothing of its turn: the message fails with its
/// outcome unknown, not as a crash, and nothing is resent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_daemon_restart_mid_turn_leaves_the_outcome_unknown() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "quin", "/w/quin", &sid(1)).await;
    let m = delivered(&outbox, &mut a, "quin", "mid-turn").await;
    turn_started(remote, &mut a, "quin", &sid(1)).await;
    drop(a);
    wait_lost(remote, "host-a").await;
    let _a = back(&url, "host-a", vec![]).await;
    assert_eq!(interrupted(&outbox, &m).await, "outcome unknown: host lost mid-turn (quin had not answered when its host was lost)");
    assert_eq!(outbox.get(&m).unwrap().attempts, 1, "not resent");
}

/// #6: a direct delivery's row is written whole or not at all, and a
/// failure says so.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn record_direct_is_one_transaction_and_reports_failure() {
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let outbox = outbox_for(remote);
    let body = json!({"kind": "msg", "text": "x"});
    let mark = crate::agent_host::remote::TurnMark::default();
    let now = k2_core::delivery::now_ms();
    outbox.record_direct("m_0000000000008_one", "agent:alice", "rex", &body, &mark, now).unwrap();
    assert_eq!(outbox.get("m_0000000000008_one").unwrap().state, State::Delivered);
    let err = outbox.record_direct("m_0000000000008_one", "agent:alice", "rex", &body, &mark, now).unwrap_err();
    assert!(err.contains("already finished"), "{err}");
    assert!(outbox.record_direct("", "agent:alice", "rex", &body, &mark, now).is_err());
    assert_eq!(outbox.get("").map(|r| r.state), None, "nothing half written");
}

/// #5 (lab S10, seed 7 round 3): the controller restarts between a
/// delivery and the crash that cuts its turn. The turn and its unanswered
/// message are in the controller's store: restored, the host's resume
/// reconciled against them, and the crash after the restart fails the
/// message.
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
    let epoch = remote.placement("sam").unwrap().epoch.0;
    let m = delivered(&outbox, &mut a, "sam", "before the restart").await;
    turn_started(remote, &mut a, "sam", &sid(1)).await;
    drop(a);

    // A new controller process on the same databases.
    let (remote, outbox) = start(true);
    let url = serve(remote).await;
    let mut a = back(&url, "host-a", vec![resumed("sam", &sid(1), epoch, true, counts(1, 0, true))]).await;
    within("sam to be adopted mid-turn", async {
        while !remote.in_turn("sam") {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    gone(&mut a, remote, "sam").await;
    assert_eq!(interrupted(&outbox, &m).await, "interrupted: sam crashed mid-turn before it answered");
    std::fs::remove_file(&placements).unwrap();
    std::fs::remove_file(&outbox_db).unwrap();
}

// ── K2-f.2: the host's absolute counts and each message's prompt ───────

/// A hook carrying the host's counts after it (a K2-f.2 host).
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

/// The outbox sends `text` to `agent`; the host types it for its
/// `prompt`th prompt and says so in its ack.
async fn delivered_for(outbox: &crate::delivery_drain::Controller, host: &mut Ws, agent: &str, text: &str, prompt: u64) -> String {
    let id = outbox.send("agent:alice", agent, json!({"kind": "msg", "text": text})).unwrap();
    match recv(host).await {
        Frame::Deliver { msg_id, .. } => {
            assert_eq!(msg_id, id);
            send(host, &Frame::DeliverAck { msg_id, status: crate::hosts::proto::DeliverStatus::Delivered, reason: None, prompt: Some(prompt) }).await;
        }
        other => panic!("expected the delivery, got {other:?}"),
    }
    assert_eq!(wait_final(outbox, &id).await.state, k2_core::delivery::State::Delivered);
    id
}

async fn until(what: &str, f: impl Fn() -> bool) {
    within(what, async {
        while !f() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

/// #1: A goes in prompt 1; B, typed while that turn runs, in prompt 2.
/// The forwarder drops two hooks (A's Stop, B's prompt): the next hook's
/// counts carry them. Then the host is lost (nothing more from it): A is
/// known answered, B's outcome is unknown. (Counted from forwarded hooks
/// alone, A looked unanswered too.) And a CLI dying with counts in its
/// `session_gone` gets the same split.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absolute_counts_survive_hooks_the_forwarder_dropped() {
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "ada", "/w/ada", &sid(1)).await;
    let ma = delivered_for(&outbox, &mut a, "ada", "A", 1).await;
    hook_at(&mut a, &sid(1), "UserPromptSubmit", counts(1, 0, true)).await;
    until("ada's turn", || remote.in_turn("ada")).await;
    let mb = delivered_for(&outbox, &mut a, "ada", "B", 2).await;
    // (Stop 1,1 and UserPromptSubmit 2,1 are lost.) A tool hook in B's turn:
    hook_at(&mut a, &sid(1), "PreToolUse", counts(2, 1, true)).await;
    spawn_on(remote, &mut a, "bea", "/w/bea", &sid(2)).await; // the hook is in by now (one link, in order)
    drop(a);
    wait_lost(remote, "host-a").await;
    let moved = remote.move_lost(std::time::Instant::now(), Duration::ZERO);
    assert!(moved.contains(&"ada".to_string()), "{moved:?}");
    assert_eq!(interrupted(&outbox, &mb).await, "outcome unknown: host lost mid-turn (ada had not answered when its host was lost)");
    for _ in 0..3 {
        outbox.pass(k2_core::delivery::now_ms());
    }
    assert_eq!(outbox.get(&ma).unwrap().state, State::Delivered, "answered in prompt 1");

    let mut b = fake_host(&url, "host-b").await;
    spawn_on(remote, &mut b, "cal", "/w/cal", &sid(3)).await;
    let mc = delivered_for(&outbox, &mut b, "cal", "C", 1).await;
    hook_at(&mut b, &sid(3), "UserPromptSubmit", counts(1, 0, true)).await;
    let md = delivered_for(&outbox, &mut b, "cal", "D", 2).await;
    send(&mut b, &Frame::SessionGone { agent_id: "cal".into(), epoch: None, exit_code: Some(137), turn: Some(counts(2, 1, true)) }).await;
    assert_eq!(interrupted(&outbox, &md).await, "interrupted: cal crashed mid-turn before it answered");
    assert_eq!(outbox.get(&mc).unwrap().state, State::Delivered);
}

/// #1: a controller that never followed an agent's turn (adopted after an
/// upgrade or a restart without its table) takes it from the resume.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_adopted_agent_gets_its_turn_from_the_resume() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let _a = back(&url, "host-a", vec![resumed("bo", &sid(1), 7, true, counts(3, 2, true))]).await;
    until("bo adopted", || remote.placement("bo").is_some()).await;
    assert!(remote.in_turn("bo"), "in its third turn, as the host says");
}

/// #2: a CLI nested in the lead's (a `claude -p` run by a Bash tool) posts
/// hooks for the same pane; its Stop doesn't end the lead's turn, on the
/// host or on the controller.
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
    turn_started(remote, &mut a, "cy", &sid(1)).await;
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

/// #4: an idle agent; B is typed (prompt 1) and submitted, but its host
/// is lost before that UserPromptSubmit is forwarded. The agent moves: B's
/// outcome is unknown, and it says so (it isn't left delivered).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_host_leaves_a_submitted_message_unknown_not_delivered() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "dee", "/w/dee", &sid(1)).await;
    hook(&mut a, &sid(1), "SessionStart").await;
    let mb = delivered_for(&outbox, &mut a, "dee", "B", 1).await;
    drop(a);
    wait_lost(remote, "host-a").await;
    assert_eq!(remote.move_lost(std::time::Instant::now(), Duration::ZERO), vec!["dee"]);
    assert_eq!(interrupted(&outbox, &mb).await, "outcome unknown: host lost mid-turn (dee had not answered when its host was lost)");
}

/// #5: two messages typed while a turn runs are submitted as their own
/// prompts (2 and 3). The CLI dies in prompt 3's turn: only that one
/// fails; prompt 2's was answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_message_is_answered_by_its_own_prompts_turn() {
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "eli", "/w/eli", &sid(1)).await;
    hook_at(&mut a, &sid(1), "UserPromptSubmit", counts(1, 0, true)).await;
    until("eli's turn", || remote.in_turn("eli")).await;
    let m2 = delivered_for(&outbox, &mut a, "eli", "second", 2).await;
    let m3 = delivered_for(&outbox, &mut a, "eli", "third", 3).await;
    hook_at(&mut a, &sid(1), "Stop", counts(1, 1, false)).await;
    hook_at(&mut a, &sid(1), "UserPromptSubmit", counts(2, 1, true)).await;
    hook_at(&mut a, &sid(1), "Stop", counts(2, 2, false)).await;
    hook_at(&mut a, &sid(1), "UserPromptSubmit", counts(3, 2, true)).await;
    send(&mut a, &Frame::SessionGone { agent_id: "eli".into(), epoch: None, exit_code: Some(137), turn: Some(counts(3, 2, true)) }).await;
    assert_eq!(interrupted(&outbox, &m3).await, "interrupted: eli crashed mid-turn before it answered");
    assert_eq!(outbox.get(&m2).unwrap().state, State::Delivered);
}

/// #6: a message sent to host-a goes unanswered (no ack) and its agent
/// ends there; placed again on host-b, the resend is noted, and a crash in
/// its turn fails it. A resend the host calls a duplicate (typed and
/// answered before, its ack lost) is not noted again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resend_is_followed_and_a_duplicate_one_is_not() {
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    outbox.set_gone_grace(Duration::from_secs(3600));
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "fay", "/w/fay", &sid(1)).await;
    hook(&mut a, &sid(1), "SessionStart").await;
    let m = outbox.send("agent:alice", "fay", json!({"kind": "msg", "text": "resent"})).unwrap();
    assert!(matches!(recv(&mut a).await, Frame::Deliver { attempt: 1, .. }), "the first send; never acked");
    gone(&mut a, remote, "fay").await;
    assert_eq!(outbox.get(&m).unwrap().state, State::Sent, "not delivered: nothing to fail");
    let mut b = fake_host(&url, "host-b").await;
    drop(a);
    // host-a's link is down before fay is placed again: on host-b.
    wait_hosts(remote, 1).await;
    spawn_on(remote, &mut b, "fay", "/w/fay", &sid(2)).await;
    let Frame::Deliver { msg_id, attempt, .. } = recv(&mut b).await else { panic!("expected the resend") };
    assert_eq!((msg_id.as_str(), attempt > 1), (m.as_str(), true));
    send(&mut b, &Frame::DeliverAck { msg_id, status: crate::hosts::proto::DeliverStatus::Delivered, reason: None, prompt: Some(1) }).await;
    assert_eq!(wait_final(&outbox, &m).await.state, State::Delivered);
    turn_started(remote, &mut b, "fay", &sid(2)).await;
    gone(&mut b, remote, "fay").await;
    assert_eq!(interrupted(&outbox, &m).await, "interrupted: fay crashed mid-turn before it answered");

    // The duplicate: typed and answered on gus's first send, its ack lost.
    spawn_on(remote, &mut b, "gus", "/w/gus", &sid(3)).await;
    let g = outbox.send("agent:alice", "gus", json!({"kind": "msg", "text": "once"})).unwrap();
    assert!(matches!(recv(&mut b).await, Frame::Deliver { attempt: 1, .. }));
    turn_started(remote, &mut b, "gus", &sid(3)).await;
    hook(&mut b, &sid(3), "Stop").await;
    until("gus's turn to end", || !remote.in_turn("gus")).await;
    let Frame::Deliver { msg_id, .. } = recv(&mut b).await else { panic!("expected the resend") };
    send(&mut b, &Frame::DeliverAck { msg_id, status: crate::hosts::proto::DeliverStatus::Duplicate, reason: None, prompt: None }).await;
    assert_eq!(wait_final(&outbox, &g).await.state, State::Delivered);
    turn_started(remote, &mut b, "gus", &sid(3)).await;
    gone(&mut b, remote, "gus").await;
    for _ in 0..3 {
        outbox.pass(k2_core::delivery::now_ms());
    }
    assert_eq!(outbox.get(&g).unwrap().state, State::Delivered, "answered in its first turn");
}

/// #7: the v2 close route (HTTP) and a drain's release at its deadline
/// cut a running turn like `unplace` does.
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
    turn_started(remote, &mut a, "hy", &sid(1)).await;
    turn_started(remote, &mut a, "io", &sid(2)).await;

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

/// #6, lab's S10 (heavy chaos on f76dce00d, 1/10 green): turn three's
/// attempt 1 goes to host-1, which is killed before it acks. The agent
/// moves to host-2, where attempt 2 is the first real delivery: noted (its
/// ack, from a host that gives no prompt, keeps the estimate). The CLI is
/// killed mid-turn: the message fails `interrupted`, not left delivered.
/// Attempt 1's note, cut with the lost host, fails nothing: that row was
/// only `sent`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attempt_one_lost_unacked_attempt_two_delivered_then_a_crash_fails_it() {
    use k2_core::delivery::State;
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let outbox = outbox_for(remote);
    let mut a = fake_host(&url, "host-1").await;
    spawn_on(remote, &mut a, "notes", "/w/notes", &sid(1)).await;
    hook(&mut a, &sid(1), "SessionStart").await;
    let m = outbox.send("agent:alice", "notes", json!({"kind": "msg", "text": "turn three"})).unwrap();
    assert!(matches!(recv(&mut a).await, Frame::Deliver { attempt: 1, .. }));
    drop(a); // SIGKILL before the ack
    wait_lost(remote, "host-1").await;
    assert_eq!(remote.move_lost(std::time::Instant::now(), Duration::ZERO), vec!["notes"]);
    assert_eq!(outbox.get(&m).unwrap().state, State::Sent, "the cut on the move fails nothing that wasn't delivered");

    let mut b = fake_host(&url, "host-2").await;
    wait_hosts(remote, 1).await;
    spawn_on(remote, &mut b, "notes", "/w/notes", &sid(2)).await;
    let Frame::Deliver { msg_id, attempt, .. } = recv(&mut b).await else { panic!("expected the resend") };
    assert_eq!((msg_id.as_str(), attempt > 1), (m.as_str(), true));
    send(&mut b, &Frame::DeliverAck { msg_id, status: crate::hosts::proto::DeliverStatus::Delivered, reason: None, prompt: None }).await;
    assert_eq!(wait_final(&outbox, &m).await.state, State::Delivered);
    turn_started(remote, &mut b, "notes", &sid(2)).await;
    gone(&mut b, remote, "notes").await;
    assert_eq!(interrupted(&outbox, &m).await, "interrupted: notes crashed mid-turn before it answered");
}
