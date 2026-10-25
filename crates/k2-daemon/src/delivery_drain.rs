//! Message delivery across hosts (PRD "K2 Distributed Runtime": FR4,
//! scenarios S3 and S6, invariant I2), over the host link's protocol-2
//! `deliver` / `deliver.ack` frames.
//!
//! Two hops, each at least once with the receiver deduping on `msg_id`:
//!
//! - **up**, host → controller: `k2 msg` and inbox compose on a
//!   `K2_ROLE=host` daemon ([`host_send`]) write the message to the host's
//!   outbox (`Hop::Up`) and send `deliver` up the link, resending until
//!   the controller acks. The controller ([`Controller::accept`]) writes it
//!   to its own outbox (`Hop::Down`) and acks `delivered` (accepted) or
//!   `duplicate` (a resend it already has).
//! - **down**, controller → the recipient's host: [`Controller`]'s drain
//!   sends each pair's oldest message to the host the placement table
//!   names, resends after a doubling wait, marks `delivered` on the ack,
//!   and fails a message loudly (log line, trace event, `failed` row with
//!   a reason) when the host rejects it, after the attempt limit, or past
//!   the TTL. The host ([`HostSide::receive`]) checks its `delivery_seen`
//!   table, injects into the local agent, records the id, then acks.
//!
//! Under `K2_ROLE=all` none of this runs: `k2 msg` is today's direct
//! delivery. The store is `k2_core::delivery`; the protocol and state
//! machine are in `docs/prd/progress/outbox.md`.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use parking_lot::ReentrantMutex;
use k2_core::delivery::{self, outbox, seen, Enqueued, Hop, NewMessage, Policy, Row, State};
use k2_core::log_debug;
use parking_lot::Mutex;
use rusqlite::Connection;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Notify};

use crate::agent_host::remote::RemoteHost;
use crate::hosts::proto::{trace_events, DeliverStatus, Frame};

pub type Db = Arc<ReentrantMutex<Connection>>;

/// `GET /cli/hosts/outbox` (owner token): this daemon's outbox: counts by
/// state and the newest rows, or one message with `?msg_id=`.
pub const OUTBOX_PATH: &str = "/cli/hosts/outbox";

/// The drains look again at least this often (a recipient may have been
/// placed since the last pass).
const TICK: Duration = Duration::from_millis(500);
/// How long `k2 msg` on a host waits for the controller to accept the
/// message before answering "queued on this host".
const ACCEPT_WAIT: Duration = Duration::from_secs(5);

/// Trace events beyond the ones W0 named in `proto::trace_events`.
pub mod events {
    /// A host sent a message up (attrs: attempt).
    pub const HOST_MSG_SEND: &str = "host.msg_send";
    /// The controller took a message from a host (attrs: host, status).
    pub const CTL_MSG_ACCEPT: &str = "ctl.msg_accept";
    /// A host got the controller's ack for a message it sent up (attrs: status).
    pub const HOST_MSG_ACK: &str = "host.msg_ack";
    /// A message ended `failed` (attrs: hop, reason). I2: the loud end.
    pub const MSG_FAILED: &str = "delivery.failed";
}

fn ready(db: Db) -> Db {
    if let Err(e) = delivery::ensure_schema(&db.lock()) {
        log_debug!("[delivery] ERROR: can't create the delivery tables: {e}");
    }
    db
}

fn report_failed(hop: Hop, rows: &[Row]) {
    for r in rows {
        let reason = r.reason.as_deref().unwrap_or("");
        log_debug!(
            "[delivery] FAILED {} ({} → {}, hop {}): {reason}",
            r.msg_id,
            r.sender,
            r.recipient,
            hop.as_str()
        );
        k2_core::trace::emit(|| {
            k2_core::trace::Event::new(events::MSG_FAILED)
                .msg(r.msg_id.as_str())
                .agent(r.recipient.as_str())
                .attr("hop", hop.as_str())
                .attr("reason", reason)
        });
    }
}

fn outbox_json(db: &Db, hop: Hop, query: &str) -> Value {
    let params = crate::routes::http::parse_params(OUTBOX_PATH, query);
    let conn = db.lock();
    if let Some(id) = params.get("msg_id").filter(|s| !s.is_empty()) {
        return match outbox::get(&conn, hop, id) {
            Ok(Some(r)) => json!({"hop": hop.as_str(), "message": r.to_json()}),
            Ok(None) => json!({"hop": hop.as_str(), "message": null}),
            Err(e) => json!({"error": e.to_string()}),
        };
    }
    let (queued, sent, delivered, failed) = outbox::counts(&conn, hop).unwrap_or_default();
    let rows: Vec<Value> = outbox::list(&conn, hop, 500).unwrap_or_default().iter().map(Row::to_json).collect();
    json!({
        "hop": hop.as_str(),
        "counts": {"queued": queued, "sent": sent, "delivered": delivered, "failed": failed},
        "messages": rows,
    })
}

/// `GET /cli/hosts/outbox`: `None` under `K2_ROLE=all`.
pub fn outbox_status(query: &str) -> Option<Value> {
    match crate::runtime_role::current() {
        crate::runtime_role::Role::Controller => {
            Some(outbox_json(&ensure_controller_drain().db, Hop::Down, query))
        }
        crate::runtime_role::Role::Host => Some(outbox_json(&host_side().db, Hop::Up, query)),
        crate::runtime_role::Role::All => None,
    }
}

/// The deliver frame for an outbox row.
fn deliver_frame(r: &Row, epoch: Option<u64>, attempt: u32) -> Frame {
    Frame::Deliver {
        msg_id: r.msg_id.clone(),
        agent_id: r.recipient.clone(),
        epoch,
        from: r.sender.clone(),
        body: serde_json::from_str(&r.body).unwrap_or(Value::Null),
        attempt,
    }
}

// ── controller ─────────────────────────────────────────────────────────

/// The controller's half: accepts messages from hosts and drains its
/// outbox to the recipients' hosts through `remote`.
pub struct Controller {
    remote: &'static RemoteHost,
    db: Db,
    policy: Policy,
    nudge: Notify,
}

fn controllers() -> &'static Mutex<Vec<Arc<Controller>>> {
    static C: OnceLock<Mutex<Vec<Arc<Controller>>>> = OnceLock::new();
    C.get_or_init(Mutex::default)
}

/// The drain for `remote`, if one was started.
fn controller_for(remote: &RemoteHost) -> Option<Arc<Controller>> {
    controllers().lock().iter().find(|c| std::ptr::eq(c.remote, remote)).cloned()
}

/// Start the drain for `remote` on this runtime (once per `remote`).
pub fn start_controller(remote: &'static RemoteHost, db: Db, policy: Policy) -> Arc<Controller> {
    let mut all = controllers().lock();
    if let Some(c) = all.iter().find(|c| std::ptr::eq(c.remote, remote)) {
        return Arc::clone(c);
    }
    let c = Arc::new(Controller { remote, db: ready(db), policy, nudge: Notify::new() });
    all.push(Arc::clone(&c));
    tokio::spawn(Arc::clone(&c).run());
    log_debug!("[delivery] controller drain started");
    c
}

/// The production controller drain (the daemon's database, the global
/// `RemoteHost`), started on first use. The gate calls it when a host
/// links, so a restarted controller drains what it had queued.
pub fn ensure_controller_drain() -> Arc<Controller> {
    start_controller(crate::agent_host::remote::global(), k2_core::db::shared(), Policy::PRODUCTION)
}

/// `RemoteHost::on_frame`'s hook for `deliver` and `deliver.ack` from a host.
pub fn on_host_frame(remote: &RemoteHost, host_id: &str, frame: Frame) {
    let Some(c) = controller_for(remote) else {
        log_debug!("[delivery] no controller drain yet; dropped {} from host {host_id} (it resends)", frame.kind());
        return;
    };
    match frame {
        Frame::Deliver { msg_id, agent_id, from, body, attempt, .. } => {
            let (status, reason) = c.accept(&msg_id, &from, &agent_id, &body, delivery::now_ms());
            k2_core::trace::emit(|| {
                k2_core::trace::Event::new(events::CTL_MSG_ACCEPT)
                    .msg(msg_id.as_str())
                    .agent(agent_id.as_str())
                    .attr("host", host_id)
                    .attr("attempt", attempt)
                    .attr("status", format!("{status:?}").to_lowercase())
            });
            if let Err(e) = c.remote.send_frame(host_id, Frame::DeliverAck { msg_id, status, reason }) {
                log_debug!("[delivery] ack to host {host_id}: {e} (the host resends)");
            }
        }
        Frame::DeliverAck { msg_id, status, reason } => {
            c.on_ack(host_id, &msg_id, status, reason.as_deref(), delivery::now_ms());
        }
        other => log_debug!("[delivery] unexpected {} from host {host_id}", other.kind()),
    }
}

impl Controller {
    /// A message from a host (the up hop): queue it once per `msg_id`.
    pub fn accept(&self, msg_id: &str, from: &str, agent_id: &str, body: &Value, now: i64) -> (DeliverStatus, Option<String>) {
        if msg_id.is_empty() || agent_id.is_empty() || body.is_null() {
            return (DeliverStatus::Rejected, Some("msg_id, agent_id and body are required".into()));
        }
        let m = NewMessage { msg_id: msg_id.into(), sender: from.into(), recipient: agent_id.into(), body: body.to_string() };
        let r = outbox::enqueue(&self.db.lock(), Hop::Down, &m, now);
        match r {
            Ok(Enqueued::New) => {
                log_debug!("[delivery] queued {msg_id} {from} → {agent_id}");
                self.nudge.notify_one();
                (DeliverStatus::Delivered, None)
            }
            Ok(Enqueued::Exists(_)) => (DeliverStatus::Duplicate, None),
            // Not acked: the host keeps it and resends.
            Err(e) => {
                log_debug!("[delivery] ERROR: can't queue {msg_id}: {e}");
                (DeliverStatus::Rejected, Some(format!("controller database: {e}")))
            }
        }
    }

    /// A message that starts on the controller (`k2 msg` sent to it).
    pub fn send(&self, from: &str, agent_id: &str, body: Value) -> Result<String, String> {
        let now = delivery::now_ms();
        let msg_id = delivery::new_msg_id(now);
        let m = NewMessage { msg_id: msg_id.clone(), sender: from.into(), recipient: agent_id.into(), body: body.to_string() };
        outbox::enqueue(&self.db.lock(), Hop::Down, &m, now).map_err(|e| e.to_string())?;
        log_debug!("[delivery] queued {msg_id} {from} → {agent_id}");
        self.nudge.notify_one();
        Ok(msg_id)
    }

    pub fn on_ack(&self, host_id: &str, msg_id: &str, status: DeliverStatus, reason: Option<&str>, now: i64) {
        k2_core::trace::emit(|| {
            k2_core::trace::Event::new(trace_events::CTL_DELIVER_ACK)
                .msg(msg_id)
                .attr("host", host_id)
                .attr("status", format!("{status:?}").to_lowercase())
        });
        let conn = self.db.lock();
        let changed = match status {
            DeliverStatus::Delivered | DeliverStatus::Duplicate => outbox::mark_delivered(&conn, Hop::Down, msg_id, now),
            DeliverStatus::Rejected => {
                let why = format!("rejected by host {host_id}: {}", reason.unwrap_or("no reason given"));
                let r = outbox::mark_failed(&conn, Hop::Down, msg_id, &why, now);
                if let Ok(Some(row)) = outbox::get(&conn, Hop::Down, msg_id) {
                    report_failed(Hop::Down, &[row]);
                }
                r
            }
        };
        match changed {
            Ok(true) => log_debug!("[delivery] {msg_id} {status:?} by host {host_id}"),
            Ok(false) => log_debug!("[delivery] late or unknown ack {msg_id} ({status:?}) from host {host_id}"),
            Err(e) => log_debug!("[delivery] ERROR: ack {msg_id}: {e}"),
        }
        drop(conn);
        self.nudge.notify_one();
    }

    /// One drain pass at `now`: dead letters first, then send every due
    /// row whose recipient is placed on a linked host.
    pub fn pass(&self, now: i64) {
        let conn = self.db.lock();
        match outbox::sweep(&conn, Hop::Down, now, &self.policy) {
            Ok(failed) => report_failed(Hop::Down, &failed),
            Err(e) => log_debug!("[delivery] ERROR: sweep: {e}"),
        }
        let due = match outbox::due(&conn, Hop::Down, now) {
            Ok(d) => d,
            Err(e) => {
                log_debug!("[delivery] ERROR: due: {e}");
                return;
            }
        };
        for r in due {
            // Not placed: wait (no attempt spent) until it is, or the TTL.
            let Some(p) = self.remote.placement(&r.recipient) else { continue };
            let attempt = r.attempts + 1;
            match self.remote.send_frame(&p.host_id, deliver_frame(&r, Some(p.epoch.0), attempt)) {
                Ok(()) => {
                    if let Err(e) = outbox::mark_sent(&conn, Hop::Down, &r.msg_id, now, &self.policy) {
                        log_debug!("[delivery] ERROR: mark sent {}: {e}", r.msg_id);
                    }
                    log_debug!("[delivery] deliver {} to {} on host {} (attempt {attempt})", r.msg_id, r.recipient, p.host_id);
                    k2_core::trace::emit(|| {
                        k2_core::trace::Event::new(trace_events::CTL_DELIVER)
                            .msg(r.msg_id.as_str())
                            .agent(r.recipient.as_str())
                            .attr("host", p.host_id.as_str())
                            .attr("attempt", attempt)
                    });
                }
                Err(e) => log_debug!("[delivery] {} waits: {e}", r.msg_id),
            }
        }
    }

    /// The row for `msg_id` (tests and the status route).
    pub fn get(&self, msg_id: &str) -> Option<Row> {
        outbox::get(&self.db.lock(), Hop::Down, msg_id).ok().flatten()
    }

    async fn run(self: Arc<Self>) {
        loop {
            let now = delivery::now_ms();
            self.pass(now);
            let next = outbox::next_wakeup_ms(&self.db.lock(), Hop::Down).ok().flatten();
            let wait = next.map(|t| Duration::from_millis((t - now).clamp(0, TICK.as_millis() as i64) as u64)).unwrap_or(TICK);
            tokio::select! {
                _ = self.nudge.notified() => {}
                _ = tokio::time::sleep(wait.max(Duration::from_millis(10))) => {}
            }
        }
    }
}

// ── host ───────────────────────────────────────────────────────────────

/// What delivering to a local agent came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalDelivery {
    /// Written into the agent (PTY or inbox): ack `delivered`.
    Delivered,
    /// Not now (no live session, PTY busy or gone): no ack, the
    /// controller resends.
    Retry(String),
    /// Never (malformed body): ack `rejected`.
    Rejected(String),
}

/// A message for an agent on this host, from a `deliver` frame.
#[derive(Debug, Clone, PartialEq)]
pub struct Incoming {
    pub msg_id: String,
    pub agent_id: String,
    pub from: String,
    pub body: Value,
}

type Deliverer = Box<dyn Fn(&Incoming) -> LocalDelivery + Send + Sync>;

/// What a host's waiting `k2 msg` learns about its message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Accepted {
    /// The controller has it.
    Accepted,
    /// The controller refused it, or it failed on the up hop.
    Failed(String),
}

/// The host's half: its outbox for messages going up, and the receiving
/// side for messages coming down.
pub struct HostSide {
    db: Db,
    policy: Policy,
    link: Mutex<Option<mpsc::UnboundedSender<Frame>>>,
    nudge: Notify,
    waiters: Mutex<HashMap<String, Vec<std::sync::mpsc::Sender<Accepted>>>>,
    /// One receive at a time, so two copies of a message can't both pass
    /// the seen check.
    receiving: Mutex<()>,
    deliver: Deliverer,
}

impl HostSide {
    pub fn new(db: Db, policy: Policy, deliver: Deliverer) -> Self {
        Self {
            db: ready(db),
            policy,
            link: Mutex::new(None),
            nudge: Notify::new(),
            waiters: Mutex::default(),
            receiving: Mutex::new(()),
            deliver,
        }
    }

    /// A `deliver` from the controller: deliver once per `msg_id`. Returns
    /// the ack to send, or `None` to stay silent so the controller resends.
    pub fn receive(&self, m: &Incoming, now: i64) -> Option<Frame> {
        let _one = self.receiving.lock();
        let ack = |status, reason: Option<String>| Some(Frame::DeliverAck { msg_id: m.msg_id.clone(), status, reason });
        let before = seen::status(&self.db.lock(), &m.msg_id);
        let out = match before {
            Ok(Some(s)) if s == "rejected" => ack(DeliverStatus::Rejected, Some("rejected before".into())),
            Ok(Some(_)) => ack(DeliverStatus::Duplicate, None),
            Err(e) => {
                log_debug!("[delivery] ERROR: seen check {}: {e}", m.msg_id);
                None
            }
            Ok(None) => match (self.deliver)(m) {
                LocalDelivery::Delivered => {
                    self.record_seen(&m.msg_id, "delivered", now);
                    ack(DeliverStatus::Delivered, None)
                }
                LocalDelivery::Rejected(why) => {
                    self.record_seen(&m.msg_id, "rejected", now);
                    ack(DeliverStatus::Rejected, Some(why))
                }
                LocalDelivery::Retry(why) => {
                    log_debug!("[delivery] {} for {} not delivered yet: {why}", m.msg_id, m.agent_id);
                    None
                }
            },
        };
        let duplicate = matches!(out, Some(Frame::DeliverAck { status: DeliverStatus::Duplicate, .. }));
        let status = match &out {
            Some(Frame::DeliverAck { status, .. }) => format!("{status:?}").to_lowercase(),
            _ => "retry".into(),
        };
        k2_core::trace::emit(|| {
            k2_core::trace::Event::new(trace_events::HOST_DELIVER)
                .msg(m.msg_id.as_str())
                .agent(m.agent_id.as_str())
                .attr("duplicate", duplicate)
                .attr("status", status)
        });
        out
    }

    fn record_seen(&self, msg_id: &str, status: &str, now: i64) {
        if let Err(e) = seen::record(&self.db.lock(), msg_id, status, now, self.policy.seen_cap) {
            // The agent has it; a resend would deliver it again (at least once).
            log_debug!("[delivery] ERROR: record seen {msg_id}: {e}");
        }
    }

    /// Queue a message from a local agent for the controller. Returns its id.
    pub fn enqueue_up(&self, from: &str, agent_id: &str, body: Value, now: i64) -> Result<String, String> {
        let msg_id = delivery::new_msg_id(now);
        let m = NewMessage { msg_id: msg_id.clone(), sender: from.into(), recipient: agent_id.into(), body: body.to_string() };
        outbox::enqueue(&self.db.lock(), Hop::Up, &m, now).map_err(|e| e.to_string())?;
        self.nudge.notify_one();
        Ok(msg_id)
    }

    /// The controller's ack for a message this host sent up.
    pub fn on_ack(&self, msg_id: &str, status: DeliverStatus, reason: Option<&str>, now: i64) {
        k2_core::trace::emit(|| {
            k2_core::trace::Event::new(events::HOST_MSG_ACK).msg(msg_id).attr("status", format!("{status:?}").to_lowercase())
        });
        let outcome = {
            let conn = self.db.lock();
            match status {
                DeliverStatus::Delivered | DeliverStatus::Duplicate => {
                    let _ = outbox::mark_delivered(&conn, Hop::Up, msg_id, now);
                    Accepted::Accepted
                }
                DeliverStatus::Rejected => {
                    let why = format!("rejected by the controller: {}", reason.unwrap_or("no reason given"));
                    let _ = outbox::mark_failed(&conn, Hop::Up, msg_id, &why, now);
                    if let Ok(Some(row)) = outbox::get(&conn, Hop::Up, msg_id) {
                        report_failed(Hop::Up, &[row]);
                    }
                    Accepted::Failed(why)
                }
            }
        };
        self.wake(msg_id, outcome);
        self.nudge.notify_one();
    }

    fn wake(&self, msg_id: &str, outcome: Accepted) {
        for w in self.waiters.lock().remove(msg_id).unwrap_or_default() {
            let _ = w.send(outcome.clone());
        }
    }

    /// Register to hear how the controller answers `msg_id`.
    fn watch(&self, msg_id: &str) -> std::sync::mpsc::Receiver<Accepted> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.waiters.lock().entry(msg_id.to_string()).or_default().push(tx);
        rx
    }

    /// The link to the controller is up and writes to `tx`.
    pub fn linked(&self, tx: mpsc::UnboundedSender<Frame>) {
        *self.link.lock() = Some(tx);
        self.nudge.notify_one();
    }

    /// One pass of the up hop: dead letters, then send what is due while
    /// the link is up.
    pub fn pass(&self, now: i64) {
        let conn = self.db.lock();
        match outbox::sweep(&conn, Hop::Up, now, &self.policy) {
            Ok(failed) => {
                report_failed(Hop::Up, &failed);
                for r in failed {
                    self.wake(&r.msg_id, Accepted::Failed(r.reason.unwrap_or_default()));
                }
            }
            Err(e) => log_debug!("[delivery] ERROR: sweep: {e}"),
        }
        let mut link = self.link.lock();
        let Some(tx) = link.as_ref() else { return };
        for r in outbox::due(&conn, Hop::Up, now).unwrap_or_default() {
            let attempt = r.attempts + 1;
            if tx.send(deliver_frame(&r, None, attempt)).is_err() {
                // The link this sender belonged to is gone.
                *link = None;
                return;
            }
            let _ = outbox::mark_sent(&conn, Hop::Up, &r.msg_id, now, &self.policy);
            k2_core::trace::emit(|| {
                k2_core::trace::Event::new(events::HOST_MSG_SEND)
                    .msg(r.msg_id.as_str())
                    .agent(r.recipient.as_str())
                    .attr("attempt", attempt)
            });
        }
    }

    async fn run(self: Arc<Self>) {
        loop {
            let now = delivery::now_ms();
            self.pass(now);
            let next = outbox::next_wakeup_ms(&self.db.lock(), Hop::Up).ok().flatten();
            let wait = next.map(|t| Duration::from_millis((t - now).clamp(0, TICK.as_millis() as i64) as u64)).unwrap_or(TICK);
            tokio::select! {
                _ = self.nudge.notified() => {}
                _ = tokio::time::sleep(wait.max(Duration::from_millis(10))) => {}
            }
        }
    }
}

/// This daemon's host side (`K2_ROLE=host`).
fn host_side() -> &'static Arc<HostSide> {
    static H: OnceLock<Arc<HostSide>> = OnceLock::new();
    H.get_or_init(|| {
        Arc::new(HostSide::new(
            k2_core::db::shared(),
            Policy::PRODUCTION,
            Box::new(|m: &Incoming| crate::workspace_msg::deliver_to_local_agent(&m.agent_id, &m.from, &m.body)),
        ))
    })
}

/// `link::serve_once`'s hook: the link to the controller is up.
pub fn host_linked(tx: mpsc::UnboundedSender<Frame>) {
    static STARTED: OnceLock<()> = OnceLock::new();
    let h = host_side();
    h.linked(tx);
    STARTED.get_or_init(|| {
        tokio::spawn(Arc::clone(h).run());
    });
}

/// `link::handle`'s hook for `deliver` and `deliver.ack` from the controller.
pub fn on_controller_frame(f: Frame, tx: &mpsc::UnboundedSender<Frame>) {
    match f {
        Frame::Deliver { msg_id, agent_id, from, body, .. } => {
            let tx = tx.clone();
            let m = Incoming { msg_id, agent_id, from, body };
            // The inject path blocks (PTY readiness, locks).
            tokio::task::spawn_blocking(move || {
                if let Some(ack) = host_side().receive(&m, delivery::now_ms()) {
                    let _ = tx.send(ack);
                }
            });
        }
        Frame::DeliverAck { msg_id, status, reason } => {
            host_side().on_ack(&msg_id, status, reason.as_deref(), delivery::now_ms());
        }
        other => log_debug!("[delivery] unexpected {} from the controller", other.kind()),
    }
}

/// The sender address for a `from` label: `agent:<name>`, or
/// `user:owner` when the caller named nobody.
pub fn sender_address(from: &str) -> String {
    let from = from.trim();
    if from.is_empty() || from == "external" {
        "user:owner".to_string()
    } else if from.contains(':') {
        from.to_string()
    } else {
        format!("agent:{from}")
    }
}

/// What a send told its caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sent {
    pub msg_id: String,
    /// `accepted` (the controller has it), `queued` (durable on this
    /// host, waiting for the link) or `failed`.
    pub state: &'static str,
    pub reason: Option<String>,
}

/// `k2 msg` / inbox compose on a host or a controller: hand the message
/// to the controller's outbox. On a host this waits up to [`ACCEPT_WAIT`]
/// for the controller's ack. `None` under `K2_ROLE=all`.
pub fn send(from: &str, agent_id: &str, body: Value) -> Option<Result<Sent, String>> {
    let sender = sender_address(from);
    match crate::runtime_role::current() {
        crate::runtime_role::Role::All => None,
        crate::runtime_role::Role::Controller => Some(
            ensure_controller_drain()
                .send(&sender, agent_id, body)
                .map(|msg_id| Sent { msg_id, state: "accepted", reason: None }),
        ),
        crate::runtime_role::Role::Host => Some(host_send(host_side(), &sender, agent_id, body, ACCEPT_WAIT)),
    }
}

fn host_send(h: &HostSide, sender: &str, agent_id: &str, body: Value, wait: Duration) -> Result<Sent, String> {
    let msg_id = h.enqueue_up(sender, agent_id, body, delivery::now_ms())?;
    let rx = h.watch(&msg_id);
    // An ack may have landed before the watch.
    let state = outbox::get(&h.db.lock(), Hop::Up, &msg_id).ok().flatten().map(|r| (r.state, r.reason));
    let outcome = match state {
        Some((State::Delivered, _)) => Some(Accepted::Accepted),
        Some((State::Failed, why)) => Some(Accepted::Failed(why.unwrap_or_default())),
        _ => rx.recv_timeout(wait).ok(),
    };
    Ok(match outcome {
        Some(Accepted::Accepted) => Sent { msg_id, state: "accepted", reason: None },
        Some(Accepted::Failed(why)) => Sent { msg_id, state: "failed", reason: Some(why) },
        None => Sent { msg_id, state: "queued", reason: None },
    })
}

#[cfg(test)]
#[path = "delivery_drain_tests.rs"]
mod tests;
