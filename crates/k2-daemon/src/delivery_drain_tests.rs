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
    send(&mut ws, &Frame::Hello { protocol: PROTOCOL, host_name: name.into(), token: TOKEN.into(), labels: vec![], resume: vec![] }).await;
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
    assert_eq!(c.drain.get("m_w"), None);
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
