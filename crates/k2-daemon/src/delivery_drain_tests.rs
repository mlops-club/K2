//! The outbox over the host link on loopback sockets: the controller's real
//! hub, `RemoteHost` and drain against fake hosts written here frame by
//! frame, whose receiving side is a real [`HostSide`] with a counting
//! deliverer instead of a PTY. The three-process run with real hosts and
//! agents is `scripts/lab/topology/three-process-msg.sh`.
//!
//! Deadlines only stop a hung test; nothing asserts a duration.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use parking_lot::ReentrantMutex;
use rusqlite::Connection;
use serde_json::json;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use super::*;
use crate::hosts::hub;
use crate::hosts::proto::{ATTACH_PATH, PROTOCOL};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

const DEADLINE: Duration = Duration::from_secs(30);
const TOKEN: &str = "test-bootstrap-token";

/// Quick retries so a lost ack is resent soon; outcomes, not timings, are
/// asserted.
const FAST: Policy = Policy {
    backoff_base_ms: 100,
    backoff_max_ms: 400,
    max_attempts: 1_000,
    ttl_ms: 3_600_000,
    keep_finished_ms: 3_600_000,
    seen_cap: 1_000,
};

fn mem_db() -> Db {
    Arc::new(ReentrantMutex::new(Connection::open_in_memory().unwrap()))
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

/// A controller on loopback: the real `/cli/hosts/attach` hub over its own
/// `RemoteHost`, and its delivery drain on an in-memory database.
struct Ctl {
    remote: &'static RemoteHost,
    drain: Arc<Controller>,
    attach_url: String,
}

impl Ctl {
    async fn start(db: Db) -> Self {
        Self::start_with(db, Direct::none()).await
    }

    async fn start_with(db: Db, direct: Direct) -> Self {
        let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
        let drain = start_controller(remote, db, FAST, direct);
        let attach = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let attach_url = format!("ws://{}{ATTACH_PATH}", attach.local_addr().unwrap());
        tokio::spawn(async move {
            while let Ok((s, _)) = attach.accept().await {
                tokio::spawn(hub::serve_attach(s, remote, TOKEN));
            }
        });
        Ctl { remote, drain, attach_url }
    }

    async fn wait_hosts(&self, n: usize) {
        within("hosts to link", async {
            while self.remote.status_json()["hosts"].as_array().map(Vec::len) != Some(n) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
    }

    async fn wait_state(&self, msg_id: &str, state: State) -> Row {
        within("the message's final state", async {
            loop {
                if let Some(r) = self.drain.get(msg_id).filter(|r| r.state == state) {
                    return r;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
    }
}

async fn fake_host(c: &Ctl, name: &str) -> Ws {
    let (mut ws, _) = tokio_tungstenite::connect_async(c.attach_url.as_str()).await.expect("dial");
    send(&mut ws, &Frame::Hello { protocol: PROTOCOL, host_name: name.into(), token: TOKEN.into(), labels: vec![], resume: vec![], ended: vec![] }).await;
    assert!(matches!(recv(&mut ws).await, Frame::Welcome { .. }));
    ws
}

/// Place `agent` on the fake host; returns its epoch.
async fn place(c: &Ctl, host: &mut Ws, agent: &str) -> u64 {
    let body = json!({"agent_name": agent, "cwd": "/tmp", "command": "claude"}).to_string();
    let remote = c.remote;
    let spawn = tokio::spawn(async move { remote.spawn(body.as_bytes()).await });
    let (req, epoch) = match recv(host).await {
        Frame::Spawn { req, agent_id, epoch, .. } => {
            assert_eq!(agent_id, agent);
            (req, epoch)
        }
        other => panic!("expected spawn, got {other:?}"),
    };
    let reply = json!({"sessionId": format!("s-{agent}"), "reused": false}).to_string();
    send(host, &Frame::Reply { req, status: "200 OK".into(), body: reply }).await;
    let (status, _) = within("the spawn answer", spawn).await.unwrap();
    assert_eq!(status, "200 OK");
    epoch
}

fn msg(id: &str, to: &str, text: &str) -> Frame {
    Frame::Deliver {
        msg_id: id.into(),
        agent_id: to.into(),
        epoch: None,
        from: "agent:alice".into(),
        body: json!({"kind": "msg", "text": text}),
        attempt: 1,
    }
}

/// The next `deliver` the controller sends this host.
async fn next_deliver(host: &mut Ws) -> (Incoming, Option<u64>, u32) {
    match recv(host).await {
        Frame::Deliver { msg_id, agent_id, epoch, from, body, attempt } => {
            (Incoming { msg_id, agent_id, from, body }, epoch, attempt)
        }
        other => panic!("expected deliver, got {other:?}"),
    }
}

/// A receiving side that counts what reaches "the agent", per message.
fn counting_host(db: Db) -> (HostSide, Arc<Mutex<Vec<String>>>) {
    let got: Arc<Mutex<Vec<String>>> = Arc::default();
    let g = Arc::clone(&got);
    let h = HostSide::new(
        db,
        FAST,
        Box::new(move |m: &Incoming| {
            g.lock().push(m.msg_id.clone());
            LocalDelivery::Delivered
        }),
    );
    (h, got)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_from_host_a_reaches_host_b_exactly_once_when_the_link_to_b_drops_before_the_ack() {
    let c = Ctl::start(mem_db()).await;
    let mut a = fake_host(&c, "host-a").await;
    place(&c, &mut a, "alice").await;
    let mut b = fake_host(&c, "host-b").await;
    let epoch_b = place(&c, &mut b, "bob").await;
    let (host_b, got) = counting_host(mem_db());

    // Hop up: the controller accepts once, and a resend (lost ack) is a duplicate.
    send(&mut a, &msg("m_1", "bob", "hello bob")).await;
    assert_eq!(recv(&mut a).await, Frame::DeliverAck { msg_id: "m_1".into(), status: DeliverStatus::Delivered, reason: None });
    send(&mut a, &msg("m_1", "bob", "hello bob")).await;
    assert_eq!(recv(&mut a).await, Frame::DeliverAck { msg_id: "m_1".into(), status: DeliverStatus::Duplicate, reason: None });

    // Hop down: B gets it, delivers it, and the link drops before the ack.
    let (m, epoch, attempt) = next_deliver(&mut b).await;
    assert_eq!((m.msg_id.as_str(), m.agent_id.as_str(), m.from.as_str()), ("m_1", "bob", "agent:alice"));
    assert_eq!((epoch, attempt), (Some(epoch_b), 1));
    assert_eq!(m.body, json!({"kind": "msg", "text": "hello bob"}));
    let lost = host_b.receive(&m, 1).expect("an ack");
    assert_eq!(lost, Frame::DeliverAck { msg_id: "m_1".into(), status: DeliverStatus::Delivered, reason: None });
    drop(b);
    c.wait_hosts(1).await;
    assert_eq!(c.drain.get("m_1").unwrap().state, State::Sent, "not delivered until acked");

    // B relinks and bob is placed again: the controller resends, B dedupes.
    let mut b = fake_host(&c, "host-b").await;
    place(&c, &mut b, "bob").await;
    let (m, _, attempt) = next_deliver(&mut b).await;
    assert_eq!(m.msg_id, "m_1");
    assert!(attempt >= 2, "a resend: attempt {attempt}");
    let ack = host_b.receive(&m, 2).expect("an ack");
    assert_eq!(ack, Frame::DeliverAck { msg_id: "m_1".into(), status: DeliverStatus::Duplicate, reason: None });
    send(&mut b, &ack).await;
    let row = c.wait_state("m_1", State::Delivered).await;
    assert_eq!((row.sender.as_str(), row.recipient.as_str()), ("agent:alice", "bob"));
    assert_eq!(*got.lock(), ["m_1"], "bob got the message exactly once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pair_is_delivered_in_order_and_a_rejection_fails_loudly() {
    let c = Ctl::start(mem_db()).await;
    let mut a = fake_host(&c, "host-a").await;
    place(&c, &mut a, "alice").await;
    let mut b = fake_host(&c, "host-b").await;
    place(&c, &mut b, "bob").await;
    let (host_b, got) = counting_host(mem_db());
    for id in ["m_2", "m_3", "m_4"] {
        send(&mut a, &msg(id, "bob", id)).await;
        assert!(matches!(recv(&mut a).await, Frame::DeliverAck { status: DeliverStatus::Delivered, .. }));
    }
    // Only the head of (alice, bob) is in flight; resends of it may repeat
    // until it is acked, and the next one only follows the ack.
    let mut first_seen = Vec::new();
    while first_seen.len() < 2 {
        let (m, _, _) = next_deliver(&mut b).await;
        if first_seen.last() != Some(&m.msg_id) {
            first_seen.push(m.msg_id.clone());
        }
        if let Some(ack) = host_b.receive(&m, 1) {
            send(&mut b, &ack).await;
        }
    }
    assert_eq!(first_seen, ["m_2", "m_3"]);
    c.wait_state("m_3", State::Delivered).await;
    // B refuses m_4: it ends failed with the host's reason.
    let (m, _, _) = loop {
        let d = next_deliver(&mut b).await;
        if d.0.msg_id == "m_4" {
            break d;
        }
    };
    send(&mut b, &Frame::DeliverAck { msg_id: m.msg_id, status: DeliverStatus::Rejected, reason: Some("no agent bob here".into()) }).await;
    let row = c.wait_state("m_4", State::Failed).await;
    assert_eq!(row.reason.as_deref(), Some("rejected by host host-b: no agent bob here"));
    assert_eq!(*got.lock(), ["m_2", "m_3"]);
    assert_eq!(c.drain.get("m_2").unwrap().state, State::Delivered);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_controller_drains_what_it_had_queued() {
    let path = k2_core::test_env::unique_temp_path("delivery-ctl.db");
    let open = || -> Db { Arc::new(ReentrantMutex::new(Connection::open(&path).unwrap())) };
    {
        // The first controller accepts a message for an agent no host runs yet.
        let c = Ctl::start(open()).await;
        let (status, _) = c.drain.accept("m_9", "agent:alice", "bob", &json!({"kind": "msg", "text": "after restart"}), delivery::now_ms());
        assert_eq!(status, DeliverStatus::Delivered);
        assert_eq!(c.drain.get("m_9").unwrap().state, State::Queued);
    }
    // A new controller process on the same database.
    let c = Ctl::start(open()).await;
    let mut b = fake_host(&c, "host-b").await;
    place(&c, &mut b, "bob").await;
    let (host_b, got) = counting_host(mem_db());
    let (m, _, _) = next_deliver(&mut b).await;
    assert_eq!(m.msg_id, "m_9");
    send(&mut b, &host_b.receive(&m, 1).unwrap()).await;
    c.wait_state("m_9", State::Delivered).await;
    assert_eq!(*got.lock(), ["m_9"]);
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn the_receiving_side_dedupes_retries_and_rejects_and_remembers_across_a_restart() {
    let path = k2_core::test_env::unique_temp_path("delivery-host.db");
    let open = || -> Db { Arc::new(ReentrantMutex::new(Connection::open(&path).unwrap())) };
    let calls = Arc::new(AtomicUsize::new(0));
    let deliverer = |calls: Arc<AtomicUsize>| -> Deliverer {
        Box::new(move |m: &Incoming| {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            match m.body["text"].as_str() {
                None => LocalDelivery::Rejected("message without text".into()),
                // The first try finds the agent busy.
                Some(_) if n == 0 => LocalDelivery::Retry("busy".into()),
                Some(_) => LocalDelivery::Delivered,
            }
        })
    };
    let m = Incoming { msg_id: "m_1".into(), agent_id: "bob".into(), from: "agent:alice".into(), body: json!({"text": "hi"}) };
    let bad = Incoming { msg_id: "m_bad".into(), agent_id: "bob".into(), from: "agent:alice".into(), body: json!({}) };
    let ack = |id: &str, status, reason: Option<&str>| Some(Frame::DeliverAck { msg_id: id.into(), status, reason: reason.map(String::from) });
    {
        let h = HostSide::new(open(), FAST, deliverer(Arc::clone(&calls)));
        assert_eq!(h.receive(&m, 1), None, "retryable: no ack");
        assert_eq!(h.receive(&m, 2), ack("m_1", DeliverStatus::Delivered, None));
        assert_eq!(h.receive(&m, 3), ack("m_1", DeliverStatus::Duplicate, None));
        assert_eq!(h.receive(&bad, 4), ack("m_bad", DeliverStatus::Rejected, Some("message without text")));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }
    // The host restarts: its seen table still answers.
    let h = HostSide::new(open(), FAST, deliverer(Arc::clone(&calls)));
    assert_eq!(h.receive(&m, 5), ack("m_1", DeliverStatus::Duplicate, None));
    assert_eq!(h.receive(&bad, 6), ack("m_bad", DeliverStatus::Rejected, Some("rejected before")));
    assert_eq!(calls.load(Ordering::SeqCst), 3, "nothing reached the agent again");
    drop(h);
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn the_up_hop_resends_until_acked_and_survives_a_link_drop() {
    let (h, _) = counting_host(mem_db());
    let up = |rx: &mut mpsc::UnboundedReceiver<Frame>| -> Vec<(String, u32)> {
        let mut out = Vec::new();
        while let Ok(f) = rx.try_recv() {
            match f {
                Frame::Deliver { msg_id, attempt, from, agent_id, epoch, .. } => {
                    assert_eq!((from.as_str(), agent_id.as_str(), epoch), ("agent:alice", "bob", None));
                    out.push((msg_id, attempt));
                }
                other => panic!("expected deliver, got {other:?}"),
            }
        }
        out
    };
    // No link yet: k2 msg answers "queued" and the message waits on disk.
    let sent = host_send(&h, "agent:alice", "bob", json!({"kind": "msg", "text": "1"}), Duration::ZERO).unwrap();
    assert_eq!(sent.state, "queued");
    let id1 = sent.msg_id;
    h.pass(0);

    let (tx, mut rx) = mpsc::unbounded_channel();
    h.linked(tx);
    h.pass(0);
    assert_eq!(up(&mut rx), [(id1.clone(), 1)]);
    h.pass(FAST.backoff_ms(1) - 1);
    assert_eq!(up(&mut rx), [], "waiting for the ack");
    h.pass(FAST.backoff_ms(1));
    assert_eq!(up(&mut rx), [(id1.clone(), 2)], "no ack: resent with the same id");

    // The link drops; the resend after it goes on the new link.
    drop(rx);
    h.pass(FAST.backoff_ms(1) + FAST.backoff_ms(2));
    let (tx, mut rx) = mpsc::unbounded_channel();
    h.linked(tx);
    h.pass(FAST.backoff_ms(1) + FAST.backoff_ms(2));
    assert_eq!(up(&mut rx), [(id1.clone(), 3)]);

    let waiter = h.watch(&id1);
    h.on_ack(&id1, DeliverStatus::Delivered, None, 1_000);
    assert_eq!(waiter.recv().unwrap(), Accepted::Accepted);
    assert_eq!(outbox::get(&h.db.lock(), Hop::Up, &id1).unwrap().unwrap().state, State::Delivered);
    h.pass(10_000);
    assert_eq!(up(&mut rx), []);

    // A refusal fails the up hop loudly, and the sender hears it.
    let id2 = h.enqueue_up("agent:alice", "bob", json!({"kind": "msg", "text": "2"}), 2_000).unwrap();
    let waiter = h.watch(&id2);
    h.on_ack(&id2, DeliverStatus::Rejected, Some("bad body"), 2_001);
    assert_eq!(waiter.recv().unwrap(), Accepted::Failed("rejected by the controller: bad body".into()));
    let row = outbox::get(&h.db.lock(), Hop::Up, &id2).unwrap().unwrap();
    assert_eq!((row.state, row.reason.as_deref()), (State::Failed, Some("rejected by the controller: bad body")));
}

#[test]
fn senders_are_addressed_by_kind() {
    assert_eq!(sender_address("alice"), "agent:alice");
    assert_eq!(sender_address(""), "user:owner");
    assert_eq!(sender_address("external"), "user:owner");
    assert_eq!(sender_address("user:eric"), "user:eric");
}

// ── recipients the controller delivers to itself (G2 S4: wake) ─────────

/// Today's cascade, faked: `/ws/*` resolve on the controller; a message
/// with `wake: true` wakes the agent and lands, one without is
/// `dormant_no_wake`. `/ws/slow` holds until `release` is sent to.
struct FakeCascade {
    calls: Arc<Mutex<Vec<(String, String, bool)>>>,
    release: std::sync::mpsc::Sender<()>,
}

fn fake_cascade() -> (Direct, FakeCascade) {
    let calls: Arc<Mutex<Vec<(String, String, bool)>>> = Arc::default();
    let (release, held) = std::sync::mpsc::channel::<()>();
    let held = Mutex::new(held);
    let c = Arc::clone(&calls);
    let direct = Direct {
        resolves: Box::new(|r| r.starts_with("/ws/")),
        aliases: Box::new(|_| Vec::new()),
        deliver: Box::new(move |to: &str, from: &str, body: &Value| {
            if to == "/ws/slow" {
                held.lock().recv().expect("released");
            }
            let wake = body["wake"].as_bool() == Some(true);
            c.lock().push((to.to_string(), from.to_string(), wake));
            if wake { Ok(()) } else { Err("dormant_no_wake".into()) }
        }),
    };
    (direct, FakeCascade { calls, release })
}

fn wake_msg(id: &str, to: &str, wake: bool) -> Frame {
    Frame::Deliver {
        msg_id: id.into(),
        agent_id: to.into(),
        epoch: None,
        from: "agent:alice".into(),
        body: json!({"kind": "msg", "text": "wake up", "wake": wake, "wake_timeout_ms": 20_000}),
        attempt: 1,
    }
}

fn ack(id: &str, status: DeliverStatus, reason: Option<&str>) -> Frame {
    Frame::DeliverAck { msg_id: id.into(), status, reason: reason.map(str::to_string) }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sleeping_recipient_is_woken_by_the_controller_once_and_without_wake_is_refused() {
    let (direct, cascade) = fake_cascade();
    let c = Ctl::start_with(mem_db(), direct).await;
    let mut a = fake_host(&c, "host-a").await;

    // `k2 msg --wake` from host A to a sleeping agent: the controller wakes it.
    send(&mut a, &wake_msg("m_w", "/ws/sleepy", true)).await;
    assert_eq!(recv(&mut a).await, ack("m_w", DeliverStatus::Delivered, None));
    // A resend (lost ack) is a duplicate and wakes nothing again.
    send(&mut a, &wake_msg("m_w", "/ws/sleepy", true)).await;
    assert_eq!(recv(&mut a).await, ack("m_w", DeliverStatus::Duplicate, None));

    // Without --wake: today's answer, every time it is asked.
    send(&mut a, &wake_msg("m_n", "/ws/sleepy", false)).await;
    assert_eq!(recv(&mut a).await, ack("m_n", DeliverStatus::Rejected, Some("dormant_no_wake")));
    send(&mut a, &wake_msg("m_n", "/ws/sleepy", false)).await;
    assert_eq!(recv(&mut a).await, ack("m_n", DeliverStatus::Rejected, Some("dormant_no_wake")));

    assert_eq!(
        *cascade.calls.lock(),
        [("/ws/sleepy".to_string(), "agent:alice".to_string(), true), ("/ws/sleepy".to_string(), "agent:alice".to_string(), false)]
    );
    // Neither went through the outbox: nothing waits for a placement (I2).
    // K2-f: the delivered one is recorded, delivered, so a crash mid-turn
    // can still fail it; the refused one has no row.
    let row = c.drain.get("m_w").expect("the direct delivery's row");
    assert_eq!((row.state, row.attempts), (State::Delivered, 0));
    assert_eq!(c.drain.get("m_n"), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resend_during_a_wake_gets_no_ack_and_the_wake_runs_once() {
    let (direct, cascade) = fake_cascade();
    let c = Ctl::start_with(mem_db(), direct).await;
    // bob first: with both linked, a tie places on host-a.
    let mut b = fake_host(&c, "host-b").await;
    place(&c, &mut b, "bob").await;
    let mut a = fake_host(&c, "host-a").await;

    send(&mut a, &wake_msg("m_s", "/ws/slow", true)).await;
    // The first copy is still waking the agent; the host resends.
    within("the wake to start", async {
        while seen::status(&c.drain.db.lock(), "m_s").unwrap().as_deref() != Some(HANDLING) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    send(&mut a, &wake_msg("m_s", "/ws/slow", true)).await;
    // An outbox message right after: its ack is the next frame, so the
    // resend above got none.
    send(&mut a, &msg("m_q", "bob", "queued")).await;
    assert_eq!(recv(&mut a).await, ack("m_q", DeliverStatus::Delivered, None));
    cascade.release.send(()).unwrap();
    assert_eq!(recv(&mut a).await, ack("m_s", DeliverStatus::Delivered, None));
    send(&mut a, &wake_msg("m_s", "/ws/slow", true)).await;
    assert_eq!(recv(&mut a).await, ack("m_s", DeliverStatus::Duplicate, None));
    assert_eq!(cascade.calls.lock().len(), 1, "one wake");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn placed_agents_and_inbox_items_still_go_through_the_outbox() {
    let (direct, _cascade) = fake_cascade();
    let c = Ctl::start_with(mem_db(), Direct { resolves: Box::new(|_| true), aliases: direct.aliases, deliver: direct.deliver }).await;
    let mut b = fake_host(&c, "host-b").await;
    let msg_body = json!({"kind": "msg", "text": "hi", "wake": true});
    assert!(c.drain.is_direct("bob", &msg_body), "not placed, resolvable: today's cascade");
    place(&c, &mut b, "bob").await;
    assert!(!c.drain.is_direct("bob", &msg_body), "placed: the outbox");
    assert!(!c.drain.is_direct("carol", &json!({"kind": "inbox", "title": "t"})), "inbox items always queue");
}

// ── addresses resolved as K2_ROLE=all does (G2 fleet gap) ─────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_workspace_address_reaches_the_agent_placed_under_its_project_id() {
    // `bob-ws` is a workspace whose agent the app placed under its project
    // id `p-bob`; `s-carol` is the session id of the agent placed as `carol`.
    let direct = Direct {
        resolves: Box::new(|r| r == "bob-ws" || r == "s-carol"),
        aliases: Box::new(|r| match r {
            "bob-ws" => vec!["p-bob".into(), "bob-ws".into(), "sess-saved".into()],
            "s-carol" => vec!["s-carol".into()],
            _ => vec![],
        }),
        deliver: Box::new(|_, _, _| panic!("a placed recipient never takes the direct path")),
    };
    let c = Ctl::start_with(mem_db(), direct).await;
    let mut b = fake_host(&c, "host-b").await;
    let epoch = place(&c, &mut b, "p-bob").await;
    place(&c, &mut b, "carol").await;
    let mut a = fake_host(&c, "host-a").await;
    let (host_b, got) = counting_host(mem_db());

    let inbox = Frame::Deliver {
        msg_id: "m_in".into(),
        agent_id: "bob-ws".into(),
        epoch: None,
        from: "agent:alice".into(),
        body: json!({"kind": "inbox", "title": "t", "body": "b"}),
        attempt: 1,
    };
    for (f, id) in [(msg("m_ws", "bob-ws", "hi bob"), "m_ws"), (inbox, "m_in"), (msg("m_sid", "s-carol", "hi carol"), "m_sid")] {
        send(&mut a, &f).await;
        assert_eq!(recv(&mut a).await, ack(id, DeliverStatus::Delivered, None));
    }
    // Each reaches host B under the id it is placed as; the row keeps the address.
    let mut seen = vec![];
    for _ in 0..3 {
        let (m, e, _) = next_deliver(&mut b).await;
        if m.agent_id == "p-bob" {
            assert_eq!(e, Some(epoch));
        }
        seen.push((m.msg_id.clone(), m.agent_id.clone()));
        let reply = host_b.receive(&m, 1).expect("an ack");
        send(&mut b, &reply).await;
    }
    seen.sort();
    assert_eq!(
        seen,
        [("m_in".to_string(), "p-bob".to_string()), ("m_sid".to_string(), "carol".to_string()), ("m_ws".to_string(), "p-bob".to_string())]
    );
    for id in ["m_ws", "m_in", "m_sid"] {
        assert_eq!(c.wait_state(id, State::Delivered).await.state, State::Delivered);
    }
    assert_eq!(c.drain.get("m_ws").unwrap().recipient, "bob-ws");
    assert_eq!(got.lock().len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_workspace_address_queued_before_its_agent_is_placed_is_delivered_once_it_is() {
    let direct = Direct {
        resolves: Box::new(|_| false),
        aliases: Box::new(|r| if r == "bob-ws" { vec!["p-bob".into()] } else { vec![] }),
        deliver: Box::new(|_, _, _| Err("unused".into())),
    };
    let c = Ctl::start_with(mem_db(), direct).await;
    let id = c.drain.send("agent:alice", "bob-ws", json!({"kind": "msg", "text": "later"})).unwrap();
    let mut b = fake_host(&c, "host-b").await;
    place(&c, &mut b, "p-bob").await;
    let (m, _, _) = next_deliver(&mut b).await;
    assert_eq!((m.msg_id.as_str(), m.agent_id.as_str()), (id.as_str(), "p-bob"));
    let (host_b, _) = counting_host(mem_db());
    send(&mut b, &host_b.receive(&m, 1).expect("an ack")).await;
    c.wait_state(&id, State::Delivered).await;
}

/// Transcripts in a temp dir: conversation `conv-<agent>` is the file
/// `<dir>/conv-<agent>.jsonl`; a missing file is an agent home that's gone.
/// Agents in `busy` are mid-turn.
#[derive(Default)]
struct TempTranscripts {
    dir: std::path::PathBuf,
    busy: Mutex<std::collections::HashSet<String>>,
}

impl TempTranscripts {
    fn new(dir: std::path::PathBuf) -> Self {
        Self { dir, ..Default::default() }
    }
    fn path(&self, agent: &str) -> std::path::PathBuf {
        self.dir.join(format!("conv-{agent}.jsonl"))
    }
    fn line(&self, agent: &str, v: serde_json::Value) {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(self.path(agent)).unwrap();
        writeln!(f, "{v}").unwrap();
    }
    /// The agent's CLI recorded `text` as a prompt, now.
    fn prompt(&self, agent: &str, text: &str) {
        let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        self.line(agent, json!({"type": "user", "timestamp": ts, "message": {"role": "user", "content": text}}));
    }
    /// The agent answered, quoting `text`; a tool's output holds it too.
    fn quoted(&self, agent: &str, text: &str) {
        self.line(agent, json!({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": format!("You said: {text}")}]}}));
        self.line(agent, json!({"type": "user", "message": {"role": "user", "content": [{"type": "tool_result", "content": text}]}}));
    }
    fn set_busy(&self, agent: &str, busy: bool) {
        if busy {
            self.busy.lock().insert(agent.into());
        } else {
            self.busy.lock().remove(agent);
        }
    }
}

impl Transcripts for TempTranscripts {
    fn mark(&self, agent_id: &str) -> TranscriptMark {
        TranscriptMark {
            provider: Some("claude".into()),
            conversation: Some(format!("conv-{agent_id}")),
            cwd: Some("/w".into()),
            offset: std::fs::metadata(self.path(agent_id)).map(|m| m.len()).unwrap_or(0),
        }
    }
    fn locate(&self, w: &k2_core::delivery::seen::WriteAhead) -> Result<std::path::PathBuf, String> {
        let path = self.dir.join(format!("{}.jsonl", w.conversation.as_deref().unwrap_or("")));
        if path.exists() {
            Ok(path)
        } else {
            Err("home gone".into())
        }
    }
    fn busy(&self, agent_id: &str) -> bool {
        self.busy.lock().contains(agent_id)
    }
}

/// What `deliver_to_local_agent` injects for `text` from agent:alice.
const HELLO: &str = "[from alice] hello";

fn wa_msg(id: &str, to: &str, text: &str) -> Incoming {
    Incoming { msg_id: id.into(), agent_id: to.into(), from: "agent:alice".into(), body: json!({"kind": "msg", "text": text}) }
}

fn ack_of(f: Option<Frame>) -> (DeliverStatus, String) {
    match f {
        Some(Frame::DeliverAck { status, reason, .. }) => (status, reason.unwrap_or_default()),
        other => panic!("expected an ack, got {other:?}"),
    }
}

/// Every delivery to `agents` killed mid-inject; `before` runs in the
/// inject first (the CLI recording the prompt, or not).
fn kill_mid_inject(
    db: impl Fn() -> Db,
    t: &Arc<TempTranscripts>,
    before: impl Fn(&Incoming) + Send + Sync + 'static,
    msgs: &[Incoming],
) {
    let h = HostSide::new(
        db(),
        FAST,
        Box::new(move |m: &Incoming| {
            before(m);
            panic!("SIGKILL mid-inject");
        }),
    )
    .with_transcripts(Arc::clone(t) as Arc<dyn Transcripts>);
    for m in msgs {
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| h.receive(m, k2_core::delivery::now_ms())));
        assert!(r.is_err(), "killed");
        assert_eq!(k2_core::delivery::seen::status(&db().lock(), &m.msg_id).unwrap().as_deref(), Some(DELIVERING), "written ahead");
    }
}

/// The restarted host: counts what it injects.
fn restarted(db: Db, t: &Arc<TempTranscripts>) -> (HostSide, Arc<Mutex<Vec<String>>>) {
    let calls: Arc<Mutex<Vec<String>>> = Arc::default();
    let c = Arc::clone(&calls);
    let h = HostSide::new(
        db,
        FAST,
        Box::new(move |m: &Incoming| {
            c.lock().push(m.msg_id.clone());
            LocalDelivery::Delivered
        }),
    )
    .with_transcripts(Arc::clone(t) as Arc<dyn Transcripts>);
    (h, calls)
}

fn reported(rx: &mut mpsc::UnboundedReceiver<Frame>) -> Vec<(String, DeliverStatus, String)> {
    let mut out = Vec::new();
    while let Ok(Frame::DeliverAck { msg_id, status, reason }) = rx.try_recv() {
        out.push((msg_id, status, reason.unwrap_or_default()));
    }
    out
}

/// K2-b (G2 chaos, seed 4117 S8 r5): a host killed between the inject and
/// its ack. The id and the transcript's place are written ahead of the
/// inject; after the restart the transcript decides, three ways.
#[test]
fn an_interrupted_delivery_is_settled_from_the_agents_transcript() {
    let path = k2_core::test_env::unique_temp_path("delivery-host-wa.db");
    let dir = k2_core::test_env::unique_temp_path("delivery-transcripts");
    std::fs::create_dir_all(&dir).unwrap();
    let t = Arc::new(TempTranscripts::new(dir.clone()));
    let open = || -> Db { Arc::new(ReentrantMutex::new(Connection::open(&path).unwrap())) };
    // Each agent has a transcript with an older prompt (the same text, before the offset: it doesn't count).
    for a in ["got", "lost", "gone"] {
        t.prompt(a, HELLO);
    }
    // Before the restart: each delivery is killed mid-inject. "got"'s CLI
    // recorded the prompt first, every time; "lost"'s didn't; "gone"'s
    // home goes away.
    let t2 = Arc::clone(&t);
    kill_mid_inject(
        open,
        &t,
        move |m| {
            if m.agent_id == "got" {
                t2.prompt("got", HELLO);
            }
        },
        &[wa_msg("m_got", "got", "hello"), wa_msg("m_lost", "lost", "hello"), wa_msg("m_gone", "gone", "hello"), wa_msg("m_rep", "got", "hello")],
    );
    std::fs::remove_file(t.path("gone")).unwrap();

    let (h, calls) = restarted(open(), &t);
    // 1. The transcript holds it: delivered, not injected again.
    let (st, why) = ack_of(h.receive(&wa_msg("m_got", "got", "hello"), k2_core::delivery::now_ms()));
    assert_eq!(st, DeliverStatus::Delivered);
    assert!(why.contains("found in the agent's transcript"), "{why}");
    // 2. It lacks it (the agent is idle): the inject never happened; delivered now, once.
    assert_eq!(ack_of(h.receive(&wa_msg("m_lost", "lost", "hello"), k2_core::delivery::now_ms())), (DeliverStatus::Delivered, String::new()));
    // 3. It can't be read: uncertain, a visible failure; not injected.
    let (st, why) = ack_of(h.receive(&wa_msg("m_gone", "gone", "hello"), k2_core::delivery::now_ms()));
    assert_eq!(st, DeliverStatus::Rejected);
    assert!(why.starts_with(UNCERTAIN_REASON), "{why}");
    assert_eq!(*calls.lock(), ["m_lost"], "only the one that never arrived is delivered");
    // The same rules when the host links (m_rep was never resent): its own
    // prompt (the second one) reports it delivered.
    let (tx, mut rx) = mpsc::unbounded_channel();
    h.linked(tx);
    let reported = reported(&mut rx);
    let find = |id: &str| reported.iter().find(|(m, _, _)| m == id).cloned();
    assert!(matches!(find("m_rep"), Some((_, DeliverStatus::Delivered, w)) if w.contains("found in the agent's transcript")), "{reported:?}");
    assert!(matches!(find("m_gone"), Some((_, DeliverStatus::Rejected, w)) if w.starts_with(UNCERTAIN_REASON)), "{reported:?}");
    assert!(matches!(find("m_got"), Some((_, DeliverStatus::Delivered, _))), "{reported:?}");
    drop(h);
    std::fs::remove_file(&path).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A link report for an interrupted delivery whose transcript lacks it sends
/// nothing and forgets it: the controller delivers it again, normally.
#[test]
fn an_interrupted_delivery_the_transcript_lacks_is_left_to_the_controller_on_link() {
    let path = k2_core::test_env::unique_temp_path("delivery-host-lacks.db");
    let dir = k2_core::test_env::unique_temp_path("delivery-transcripts-lacks");
    std::fs::create_dir_all(&dir).unwrap();
    let t = Arc::new(TempTranscripts::new(dir.clone()));
    t.prompt("bob", "earlier");
    let open = || -> Db { Arc::new(ReentrantMutex::new(Connection::open(&path).unwrap())) };
    kill_mid_inject(open, &t, |_| {}, &[wa_msg("m_x", "bob", "new")]);
    let (h, _) = restarted(open(), &t);
    let (tx, mut rx) = mpsc::unbounded_channel();
    h.linked(tx);
    assert!(rx.try_recv().is_err(), "nothing reported");
    assert_eq!(k2_core::delivery::seen::status(&open().lock(), "m_x").unwrap(), None, "forgotten: a resend delivers it");
    drop(h);
    std::fs::remove_file(&path).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Only the agent's own prompt counts: its reply quoting the message, a
/// tool's output holding it, or a prompt that merely contains the text
/// don't. Each prompt settles one row: two copies of the same text need
/// two prompts.
#[test]
fn only_an_exact_user_prompt_settles_an_interrupted_delivery_once() {
    let path = k2_core::test_env::unique_temp_path("delivery-host-exact.db");
    let dir = k2_core::test_env::unique_temp_path("delivery-transcripts-exact");
    std::fs::create_dir_all(&dir).unwrap();
    let t = Arc::new(TempTranscripts::new(dir.clone()));
    let open = || -> Db { Arc::new(ReentrantMutex::new(Connection::open(&path).unwrap())) };
    for a in ["echo", "twice"] {
        t.prompt(a, "earlier");
    }
    let t2 = Arc::clone(&t);
    kill_mid_inject(
        open,
        &t,
        move |m| match (m.agent_id.as_str(), m.msg_id.as_str()) {
            // Never took the prompt, yet its transcript holds the text everywhere else.
            ("echo", _) => {
                t2.quoted("echo", HELLO);
                t2.prompt("echo", &format!("{HELLO} and more"));
                t2.prompt("echo", "hello");
            }
            // Took the first copy only.
            ("twice", "m_t1") => t2.prompt("twice", HELLO),
            _ => {}
        },
        &[wa_msg("m_echo", "echo", "hello"), wa_msg("m_t1", "twice", "hello"), wa_msg("m_t2", "twice", "hello")],
    );
    let (h, calls) = restarted(open(), &t);
    // m_t2's offset is past m_t1's prompt: give it m_t1's place, so both
    // rows see the one prompt.
    {
        let conn = open();
        let conn = conn.lock();
        let w1 = k2_core::delivery::seen::written_ahead(&conn, "m_t1").unwrap().unwrap();
        let mut w2 = k2_core::delivery::seen::written_ahead(&conn, "m_t2").unwrap().unwrap();
        w2.offset = w1.offset;
        k2_core::delivery::seen::write_ahead(&conn, &w2).unwrap();
    }
    assert_eq!(ack_of(h.receive(&wa_msg("m_echo", "echo", "hello"), k2_core::delivery::now_ms())), (DeliverStatus::Delivered, String::new()));
    let (_, why) = ack_of(h.receive(&wa_msg("m_t1", "twice", "hello"), k2_core::delivery::now_ms()));
    assert!(why.contains("found in the agent's transcript"), "{why}");
    assert_eq!(ack_of(h.receive(&wa_msg("m_t2", "twice", "hello"), k2_core::delivery::now_ms())), (DeliverStatus::Delivered, String::new()));
    assert_eq!(*calls.lock(), ["m_echo", "m_t2"], "delivered again: the ones whose prompt the agent never took");
    drop(h);
    std::fs::remove_file(&path).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A prompt typed into a busy agent may not be in its transcript yet: not
/// "lacks it". No answer and no report while it's mid-turn; settled once
/// it is idle, or once the transcript took a later prompt.
#[test]
fn an_interrupted_delivery_to_a_busy_agent_waits_for_the_transcript() {
    let path = k2_core::test_env::unique_temp_path("delivery-host-busy.db");
    let dir = k2_core::test_env::unique_temp_path("delivery-transcripts-busy");
    std::fs::create_dir_all(&dir).unwrap();
    let t = Arc::new(TempTranscripts::new(dir.clone()));
    let open = || -> Db { Arc::new(ReentrantMutex::new(Connection::open(&path).unwrap())) };
    for a in ["slow", "later", "moved"] {
        t.prompt(a, "earlier");
    }
    kill_mid_inject(open, &t, |_| {}, &[wa_msg("m_slow", "slow", "hello"), wa_msg("m_later", "later", "hello"), wa_msg("m_moved", "moved", "hello")]);
    for a in ["slow", "later", "moved"] {
        t.set_busy(a, true);
    }
    let (h, calls) = restarted(open(), &t);
    let now = k2_core::delivery::now_ms;
    assert!(h.receive(&wa_msg("m_slow", "slow", "hello"), now()).is_none(), "pending: no answer, the controller resends");
    let (tx, mut rx) = mpsc::unbounded_channel();
    h.linked(tx);
    assert!(reported(&mut rx).is_empty(), "pending rows aren't reported");
    for id in ["m_slow", "m_later", "m_moved"] {
        assert_eq!(k2_core::delivery::seen::status(&open().lock(), id).unwrap().as_deref(), Some(DELIVERING), "{id} kept");
    }
    // The turn ends and the CLI records the queued prompt: delivered.
    t.prompt("slow", HELLO);
    t.set_busy("slow", false);
    let (_, why) = ack_of(h.receive(&wa_msg("m_slow", "slow", "hello"), now()));
    assert!(why.contains("found in the agent's transcript"), "{why}");
    // Idle and still not there: not delivered, delivered now.
    t.set_busy("later", false);
    assert_eq!(ack_of(h.receive(&wa_msg("m_later", "later", "hello"), now())), (DeliverStatus::Delivered, String::new()));
    // Still busy, but the CLI took a prompt typed after the inject: ours never landed.
    t.prompt("moved", "something typed later");
    assert_eq!(ack_of(h.receive(&wa_msg("m_moved", "moved", "hello"), now())), (DeliverStatus::Delivered, String::new()));
    assert_eq!(*calls.lock(), ["m_later", "m_moved"]);
    // Busy past the bound: uncertain, never pending for ever.
    kill_mid_inject(open, &t, |_| {}, &[wa_msg("m_stuck", "moved", "hello")]);
    let (st, why) = ack_of(h.receive(&wa_msg("m_stuck", "moved", "hello"), now() + PENDING_MAX_MS + 1));
    assert_eq!(st, DeliverStatus::Rejected);
    assert!(why.starts_with(UNCERTAIN_REASON), "{why}");
    drop(h);
    std::fs::remove_file(&path).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The link report never settles a delivery mid-inject: it waits for the
/// receive (which holds the receiving lock) and then reports its outcome.
#[test]
fn a_link_report_waits_for_an_inject_in_flight() {
    let dir = k2_core::test_env::unique_temp_path("delivery-transcripts-race");
    std::fs::create_dir_all(&dir).unwrap();
    let t = Arc::new(TempTranscripts::new(dir.clone()));
    t.prompt("bob", "earlier");
    let (entered_tx, entered) = std::sync::mpsc::channel::<()>();
    let (release, released) = std::sync::mpsc::channel::<()>();
    let released = Mutex::new(released);
    let h = Arc::new(
        HostSide::new(
            mem_db(),
            FAST,
            Box::new(move |_: &Incoming| {
                entered_tx.send(()).unwrap();
                released.lock().recv().unwrap();
                LocalDelivery::Delivered
            }),
        )
        .with_transcripts(Arc::clone(&t) as Arc<dyn Transcripts>),
    );
    let r = {
        let h = Arc::clone(&h);
        std::thread::spawn(move || h.receive(&wa_msg("m_live", "bob", "hello"), k2_core::delivery::now_ms()))
    };
    entered.recv().unwrap();
    // Mid-inject: the row is `delivering`, the transcript lacks it, the agent is idle.
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (linked_tx, linked) = std::sync::mpsc::channel::<()>();
    let l = {
        let h = Arc::clone(&h);
        std::thread::spawn(move || {
            h.linked(tx);
            linked_tx.send(()).unwrap();
        })
    };
    assert!(linked.recv_timeout(Duration::from_millis(200)).is_err(), "the report waits for the inject");
    assert_eq!(k2_core::delivery::seen::status(&h.db.lock(), "m_live").unwrap().as_deref(), Some(DELIVERING), "not settled under it");
    release.send(()).unwrap();
    assert!(matches!(r.join().unwrap(), Some(Frame::DeliverAck { status: DeliverStatus::Delivered, .. })));
    l.join().unwrap();
    let reported = reported(&mut rx);
    assert!(matches!(&reported[..], [(id, DeliverStatus::Delivered, w)] if id == "m_live" && w == REPORTED), "{reported:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn user_prompts_are_read_from_claude_and_codex_transcripts() {
    let dir = k2_core::test_env::unique_temp_path("delivery-scan");
    std::fs::create_dir_all(&dir).unwrap();
    let p = dir.join("t.jsonl");
    let lines = [
        json!({"type": "user", "timestamp": "2026-10-26T07:00:00.000Z", "message": {"role": "user", "content": "claude string"}}),
        json!({"type": "user", "message": {"role": "user", "content": [{"type": "text", "text": "claude blocks"}]}}),
        json!({"type": "user", "message": {"role": "user", "content": [{"type": "tool_result", "content": "tool output"}]}}),
        json!({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "reply"}]}}),
        json!({"type": "response_item", "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "codex item"}]}}),
        json!({"type": "event_msg", "payload": {"type": "user_message", "message": "codex event"}}),
    ];
    let body: String = lines.iter().map(|l| format!("{l}\n")).collect();
    std::fs::write(&p, format!("not json\n{body}")).unwrap();
    let got = scan_user_prompts(&p, 0).unwrap();
    let texts: Vec<&str> = got.iter().map(|u| u.text.as_str()).collect();
    assert_eq!(texts, ["claude string", "claude blocks", "codex item", "codex event"]);
    assert_eq!(got[0].ts_ms, Some(chrono::DateTime::parse_from_rfc3339("2026-10-26T07:00:00Z").unwrap().timestamp_millis()));
    assert_eq!(got[0].at, "not json\n".len() as u64, "a prompt's place is its line");
    // From an offset: only what follows; a file shorter than it was rewritten.
    assert_eq!(scan_user_prompts(&p, got[2].at).unwrap().len(), 2);
    assert!(scan_user_prompts(&p, 1 << 20).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

/// The production transcripts: Claude's, under `~/.claude/projects`, by
/// the cwd or by the conversation id alone; a missing one is unreadable
/// (`delivery_uncertain`), never "lacks it".
#[test]
fn live_transcripts_locate_claudes_and_a_missing_one_is_uncertain() {
    let home = crate::test_support::TempHome::new();
    let cwd = home.path().join("work");
    std::fs::create_dir_all(&cwd).unwrap();
    let cwd = cwd.to_string_lossy().into_owned();
    let projects = home.path().join(".claude").join("projects");
    let by_cwd = projects.join(k2_core::chat_history::claude_project_hash(&cwd));
    std::fs::create_dir_all(&by_cwd).unwrap();
    std::fs::write(by_cwd.join("conv-a.jsonl"), "").unwrap();
    std::fs::create_dir_all(projects.join("-elsewhere")).unwrap();
    std::fs::write(projects.join("-elsewhere").join("conv-b.jsonl"), "").unwrap();
    let w = |conv: &str, cwd: &str| k2_core::delivery::seen::WriteAhead {
        msg_id: format!("m_{conv}"),
        agent_id: "nobody-here".into(),
        provider: Some("claude".into()),
        conversation: Some(conv.into()),
        cwd: Some(cwd.into()),
        offset: 0,
        needle: Some(HELLO.into()),
        started_ms: k2_core::delivery::now_ms(),
    };
    let live = LiveTranscripts;
    assert_eq!(live.locate(&w("conv-a", &cwd)).unwrap(), by_cwd.join("conv-a.jsonl"));
    assert_eq!(live.locate(&w("conv-b", &cwd)).unwrap(), projects.join("-elsewhere").join("conv-b.jsonl"), "by id when the cwd's dir lacks it");
    assert!(live.locate(&w("conv-missing", &cwd)).is_err());
    assert!(live.locate(&k2_core::delivery::seen::WriteAhead { conversation: None, ..w("x", &cwd) }).is_err());
    assert!(!live.busy("nobody-here"), "no session: not mid-turn");

    // Settled through a host: the missing transcript is uncertain; the
    // present one, with the agent gone (idle), lacks it.
    let h = HostSide::new(mem_db(), FAST, Box::new(|_: &Incoming| LocalDelivery::Delivered));
    let now = k2_core::delivery::now_ms();
    for conv in ["conv-a", "conv-missing"] {
        let w = w(conv, &cwd);
        k2_core::delivery::seen::write_ahead(&h.db.lock(), &w).unwrap();
        k2_core::delivery::seen::record(&h.db.lock(), &w.msg_id, DELIVERING, now, 100).unwrap();
    }
    let (st, why) = ack_of(h.receive(&wa_msg("m_conv-missing", "nobody-here", "hello"), now));
    assert_eq!(st, DeliverStatus::Rejected);
    assert!(why.starts_with(UNCERTAIN_REASON) && why.contains("no claude transcript"), "{why}");
    assert_eq!(h.settle_interrupted("m_conv-a", now), Settled::NotDelivered);
}

/// The agent's session ended on its host (its CLI was killed).
async fn session_gone(c: &Ctl, host: &mut Ws, agent: &str) {
    send(host, &Frame::SessionGone { agent_id: agent.into(), epoch: None, exit_code: Some(137), turn: None }).await;
    within("the placement to go", async {
        while c.remote.placement(agent).is_some() || c.remote.session_ended_at(agent).is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

/// A controller whose gone rule's direct path records what it is asked
/// to deliver (and the id the spawn it wakes with would carry).
async fn gone_ctl() -> (Ctl, Arc<Mutex<Vec<(String, Option<String>)>>>) {
    let calls: Arc<Mutex<Vec<(String, Option<String>)>>> = Arc::default();
    let seen = Arc::clone(&calls);
    let direct = Direct {
        resolves: Box::new(|_| true),
        aliases: Box::new(|_| Vec::new()),
        deliver: Box::new(move |to: &str, _: &str, _: &Value| {
            seen.lock().push((to.to_string(), crate::delivery_drain::direct_delivery_id()));
            Ok(())
        }),
    };
    (Ctl::start_with(mem_db(), direct).await, calls)
}

/// K2-c (G2 chaos, seed 2026 S3 r5, run 09-07-34Z): alice's message was
/// injected into bob's CLI (turn 1 at 09:10:12.055), chaos SIGKILLed the
/// CLI mid-turn (12.325), and host-2 answered `retry` (12.433) though it
/// had injected; after the 30 s grace the gone rule's direct path spawned
/// bob with `--resume <session> '[from s3-alice · …]'`: the prompt twice.
/// Now the host settles the interrupted inject from the transcript and
/// answers `delivered`, so the gone rule has nothing to deliver again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k2c_an_inject_whose_cli_died_mid_turn_is_delivered_not_retried_and_not_resent() {
    let dir = k2_core::test_env::unique_temp_path("delivery-transcripts-k2c");
    std::fs::create_dir_all(&dir).unwrap();
    let t = Arc::new(TempTranscripts::new(dir.clone()));
    t.prompt("bob", "earlier");
    let (c, direct_calls) = gone_ctl().await;
    let mut h2 = fake_host(&c, "host-2").await;
    place(&c, &mut h2, "bob").await;
    let id = c.drain.send("agent:alice", "bob", json!({"kind": "msg", "text": "hello", "wake": true})).unwrap();
    let (m, _, _) = next_deliver(&mut h2).await; // ctl.deliver 09:10:11.899
    let t2 = Arc::clone(&t);
    let host = HostSide::new(
        mem_db(),
        FAST,
        Box::new(move |m: &Incoming| {
            // 12.055: the CLI took the prompt (turn 1) ...
            t2.prompt(&m.agent_id, HELLO);
            // ... 12.325: SIGKILLed mid-turn; the inject flow's liveness
            // check finds the child gone.
            LocalDelivery::Interrupted("pty_died".into())
        }),
    )
    .with_transcripts(Arc::clone(&t) as Arc<dyn Transcripts>);
    // 12.433: the answer.
    let ack = host.receive(&m, k2_core::delivery::now_ms());
    let (st, why) = ack_of(ack.clone());
    assert_eq!(st, DeliverStatus::Delivered, "never `retry` once it may have landed: {why}");
    assert!(why.contains("found in the agent's transcript"), "{why}");
    send(&mut h2, &ack.unwrap()).await;
    session_gone(&c, &mut h2, "bob").await;
    // 42.37: past the grace.
    c.drain.set_gone_grace(Duration::ZERO);
    let r = c.wait_state(&id, State::Delivered).await;
    assert_eq!(r.reason, None);
    for _ in 0..3 {
        assert!(c.drain.pass(k2_core::delivery::now_ms()).is_empty());
    }
    assert!(direct_calls.lock().is_empty(), "the gone rule never delivers it again: {:?}", direct_calls.lock());
    let _ = std::fs::remove_dir_all(&dir);
}

/// K2-c, the other side: the CLI died before it took the prompt. The
/// transcript lacks it and the agent is gone (idle): forgotten, no ack,
/// and the gone rule's direct path delivers it (once), naming the id the
/// spawn it wakes the agent with carries for the host's dedup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k2c_an_inject_the_cli_died_before_taking_goes_to_the_gone_rule_with_its_id() {
    let dir = k2_core::test_env::unique_temp_path("delivery-transcripts-k2c-lost");
    std::fs::create_dir_all(&dir).unwrap();
    let t = Arc::new(TempTranscripts::new(dir.clone()));
    t.prompt("bob", "earlier");
    let (c, direct_calls) = gone_ctl().await;
    let mut h2 = fake_host(&c, "host-2").await;
    place(&c, &mut h2, "bob").await;
    let id = c.drain.send("agent:alice", "bob", json!({"kind": "msg", "text": "hello", "wake": true})).unwrap();
    let (m, _, _) = next_deliver(&mut h2).await;
    let db = mem_db();
    let host = HostSide::new(Arc::clone(&db), FAST, Box::new(|_: &Incoming| LocalDelivery::Interrupted("pty_died".into())))
        .with_transcripts(Arc::clone(&t) as Arc<dyn Transcripts>);
    assert!(host.receive(&m, k2_core::delivery::now_ms()).is_none(), "not delivered: the controller keeps it");
    assert_eq!(k2_core::delivery::seen::status(&db.lock(), &m.msg_id).unwrap(), None, "forgotten");
    session_gone(&c, &mut h2, "bob").await;
    c.drain.set_gone_grace(Duration::ZERO);
    c.wait_state(&id, State::Delivered).await;
    assert_eq!(*direct_calls.lock(), [("bob".to_string(), Some(id.clone()))]);
    assert_eq!(crate::delivery_drain::direct_delivery_id(), None, "scoped to the delivery");
    let _ = std::fs::remove_dir_all(&dir);
}

/// K2-c (b): a spawn made while the gone rule delivers names the message
/// (`deliveryId`) to the host, and the host's `alreadyDelivered` answer
/// reaches the caller.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k2c_a_gone_rule_spawn_carries_its_message_id_to_the_host() {
    let c = Ctl::start(mem_db()).await;
    let mut h = fake_host(&c, "host-2").await;
    c.wait_hosts(1).await;
    let remote = c.remote;
    let spawn = tokio::task::spawn_blocking(move || {
        let _id = DirectDelivery::during("m_K2c");
        let req = crate::spawn::SpawnWorkspaceSessionRequest {
            agent_name: "bob".into(),
            cwd: "/w/bob".into(),
            launch_prompt: Some(HELLO.into()),
            ..Default::default()
        };
        remote.spawn_workspace_blocking(&req).map(|(s, _)| s)
    });
    match recv(&mut h).await {
        Frame::SpawnWorkspace { req, request, .. } => {
            assert_eq!(request["deliveryId"], "m_K2c");
            let reply = crate::hosts::proto::SpawnedSession {
                session_id: uuid::Uuid::new_v4().to_string(),
                reused: false,
                pending_drained: 0,
                launch_prompt_attached: false,
                cwd: Some("/w/bob".into()),
                program: Some("claude".into()),
                args: vec![],
                label: "bob".into(),
                label_source: "seed".into(),
                already_delivered: true,
            };
            send(&mut h, &Frame::Reply { req, status: "200 OK".into(), body: serde_json::to_string(&reply).unwrap() }).await;
        }
        other => panic!("expected spawn.workspace, got {other:?}"),
    }
    let spawned = within("the spawn", spawn).await.unwrap().expect("spawned");
    assert!(spawned.already_delivered);
    // Outside a gone-rule delivery: no id.
    let req = crate::spawn::SpawnWorkspaceSessionRequest { agent_name: "carol".into(), cwd: "/w/carol".into(), ..Default::default() };
    let spawn = tokio::task::spawn_blocking(move || remote.spawn_workspace_blocking(&req).map(|(s, _)| s));
    match recv(&mut h).await {
        Frame::SpawnWorkspace { req, request, .. } => {
            assert!(request.get("deliveryId").is_none(), "{request}");
            let reply = json!({"sessionId": uuid::Uuid::new_v4().to_string(), "reused": false, "pendingDrained": 0, "launchPromptAttached": false});
            send(&mut h, &Frame::Reply { req, status: "200 OK".into(), body: reply.to_string() }).await;
        }
        other => panic!("expected spawn.workspace, got {other:?}"),
    }
    assert!(!within("the spawn", spawn).await.unwrap().expect("spawned").already_delivered);
}

/// K2-c (b), the host's half: a spawn that puts a message on argv goes
/// through the seen table first. Seen delivered, or found in the
/// transcript after an interrupted inject: spawned without the prompt.
/// New: spawned with it, then seen, so a second such spawn leaves it off.
/// Can't tell: refused with `delivery_uncertain`.
#[test]
fn k2c_a_spawn_with_a_message_on_argv_is_deduped_by_the_seen_table() {
    let path = k2_core::test_env::unique_temp_path("delivery-host-launch.db");
    let dir = k2_core::test_env::unique_temp_path("delivery-transcripts-launch");
    std::fs::create_dir_all(&dir).unwrap();
    let t = Arc::new(TempTranscripts::new(dir.clone()));
    let open = || -> Db { Arc::new(ReentrantMutex::new(Connection::open(&path).unwrap())) };
    for a in ["bob", "lost", "gone"] {
        t.prompt(a, "earlier");
    }
    let t2 = Arc::clone(&t);
    kill_mid_inject(
        open,
        &t,
        move |m| {
            if m.agent_id == "bob" {
                t2.prompt("bob", HELLO);
            }
        },
        &[wa_msg("m_bob", "bob", "hello"), wa_msg("m_lost", "lost", "hello"), wa_msg("m_gone", "gone", "hello")],
    );
    std::fs::remove_file(t.path("gone")).unwrap();
    let (h, _) = restarted(open(), &t);
    let spawns: Mutex<Vec<(String, bool)>> = Mutex::default();
    let launch = |id: &str| {
        h.launch_once(id, k2_core::delivery::now_ms(), |with_prompt| {
            spawns.lock().push((id.to_string(), with_prompt));
            Ok(((), with_prompt))
        })
        .map(|(_, had)| had)
    };
    // Interrupted, and the transcript holds it: the agent has it.
    assert_eq!(launch("m_bob"), Ok(true));
    // Interrupted, and the transcript lacks it: on argv now.
    assert_eq!(launch("m_lost"), Ok(false));
    // New: on argv, then seen.
    assert_eq!(launch("m_new"), Ok(false));
    assert_eq!(k2_core::delivery::seen::status(&open().lock(), "m_new").unwrap().as_deref(), Some("delivered"));
    // A second spawn with any of them: left off.
    for id in ["m_bob", "m_lost", "m_new"] {
        assert_eq!(launch(id), Ok(true), "{id}");
    }
    // Can't tell: refused, never spawned with it.
    let err = launch("m_gone").unwrap_err();
    assert!(err.starts_with(UNCERTAIN_REASON), "{err}");
    assert_eq!(
        *spawns.lock(),
        [
            ("m_bob".to_string(), false),
            ("m_lost".to_string(), true),
            ("m_new".to_string(), true),
            ("m_bob".to_string(), false),
            ("m_lost".to_string(), false),
            ("m_new".to_string(), false),
        ]
    );
    // A spawn that couldn't put it on argv (the caller injects instead) or
    // failed leaves no record: the next attempt carries it.
    assert_eq!(h.launch_once("m_noarg", 1, |_| Ok(((), false))), Ok(((), false)));
    assert_eq!(k2_core::delivery::seen::status(&open().lock(), "m_noarg").unwrap(), None);
    assert!(h.launch_once::<()>("m_fail", 1, |_| Err("exec failed".into())).is_err());
    assert_eq!(k2_core::delivery::seen::status(&open().lock(), "m_fail").unwrap(), None);
    drop(h);
    std::fs::remove_file(&path).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// K2-b with a real host side: the host was SIGKILLed mid-inject after the
/// CLI took the prompt, and came back without the agent. Its relink
/// report (`HostSide::linked`), forwarded over the link, settles the row
/// delivered before the gone rule could act.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k2b_a_real_host_relink_report_reconciles_the_controllers_row() {
    let path = k2_core::test_env::unique_temp_path("delivery-host-relink.db");
    let dir = k2_core::test_env::unique_temp_path("delivery-transcripts-relink");
    std::fs::create_dir_all(&dir).unwrap();
    let t = Arc::new(TempTranscripts::new(dir.clone()));
    t.prompt("erin", "earlier");
    let open = || -> Db { Arc::new(ReentrantMutex::new(Connection::open(&path).unwrap())) };
    let (c, direct_calls) = gone_ctl().await;
    let mut a = fake_host(&c, "host-a").await;
    place(&c, &mut a, "erin").await;
    let id = c.drain.send("agent:alice", "erin", json!({"kind": "msg", "text": "hello", "wake": true})).unwrap();
    let (m, _, _) = next_deliver(&mut a).await;
    let t2 = Arc::clone(&t);
    kill_mid_inject(open, &t, move |m| t2.prompt(&m.agent_id, HELLO), &[m]);
    drop(a); // the host died with it; the ack never left
    let mut a = fake_host(&c, "host-a").await;
    session_gone(&c, &mut a, "erin").await;
    // The restarted host links: its report goes up the link.
    let (h, _) = restarted(open(), &t);
    let (tx, mut rx) = mpsc::unbounded_channel();
    tokio::task::spawn_blocking(move || h.linked(tx)).await.unwrap();
    let mut forwarded = 0;
    while let Ok(f) = rx.try_recv() {
        send(&mut a, &f).await;
        forwarded += 1;
    }
    assert_eq!(forwarded, 1);
    let r = c.wait_state(&id, State::Delivered).await;
    assert_eq!(r.reason, None);
    // The gone rule would act at once now; the row is final.
    c.drain.set_gone_grace(Duration::ZERO);
    for _ in 0..3 {
        assert!(c.drain.pass(k2_core::delivery::now_ms()).is_empty());
    }
    assert!(direct_calls.lock().is_empty());
    std::fs::remove_file(&path).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
