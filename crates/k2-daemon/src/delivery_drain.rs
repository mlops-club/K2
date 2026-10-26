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
//! A `k2 msg` whose recipient isn't placed but which the controller
//! resolves itself (a workspace path or name, a sidecar, a session id)
//! skips the outbox: the controller runs today's `K2_ROLE=all` cascade
//! ([`Direct`]), so a sleeping agent answers `dormant_no_wake`, or with
//! `--wake` is woken on a host through `host()` and gets the message. A
//! host's resend of such a message is answered from `delivery_seen`, so
//! the cascade runs once per `msg_id`.
//!
//! Under `K2_ROLE=all` none of this runs: `k2 msg` is today's direct
//! delivery. The store is `k2_core::delivery`; the protocol and state
//! machine are in `docs/prd/progress/outbox.md`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use parking_lot::ReentrantMutex;
use k2_core::delivery::{self, outbox, seen, Enqueued, Hop, NewMessage, Policy, Row, State};
use k2_core::log_debug;
use parking_lot::Mutex;
use rusqlite::Connection;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Notify};

use crate::agent_host::remote::{RemoteHost, RemotePlacement};
use crate::hosts::proto::{trace_events, DeliverStatus, Frame};

pub type Db = Arc<ReentrantMutex<Connection>>;

/// `GET /cli/hosts/outbox` (owner token): this daemon's outbox: counts by
/// state and the newest rows, or one message with `?msg_id=`.
pub const OUTBOX_PATH: &str = "/cli/hosts/outbox";

/// The drains look again at least this often (a recipient may have been
/// placed since the last pass).
const TICK: Duration = Duration::from_millis(500);
/// Controller: how long a message waits for an agent whose session ended
/// (its CLI exited or was killed, it was closed) to be placed again before
/// it goes through the direct path, or fails `recipient_gone`.
pub const GONE_GRACE_VAR: &str = "K2_DELIVERY_GONE_GRACE_SECS";
pub const DEFAULT_GONE_GRACE: Duration = Duration::from_secs(120);

/// [`GONE_GRACE_VAR`] through `get`; a bad value is the default, logged.
pub fn gone_grace(get: impl Fn(&str) -> Option<String>) -> Duration {
    match get(GONE_GRACE_VAR).map(|v| v.trim().to_string()).filter(|v| !v.is_empty()) {
        None => DEFAULT_GONE_GRACE,
        Some(v) => v.parse::<u64>().map(Duration::from_secs).unwrap_or_else(|_| {
            log_debug!("[delivery] {GONE_GRACE_VAR}={v:?} is not a number of seconds; using {}", DEFAULT_GONE_GRACE.as_secs());
            DEFAULT_GONE_GRACE
        }),
    }
}

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
    /// A queued message whose recipient's session ended went through the
    /// direct path (attrs: ok, reason).
    pub const CTL_MSG_GONE_DIRECT: &str = "ctl.msg_gone_direct";
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
    direct: Direct,
    /// [`GONE_GRACE_VAR`], in ms.
    gone_grace_ms: AtomicI64,
    /// Rows handed to the direct path and not finished yet.
    handing: parking_lot::Mutex<HashSet<String>>,
}

/// `k2 msg` recipients the controller resolves itself instead of queueing
/// for a placement: today's cascade (live inject, `dormant_no_wake`,
/// `--wake` through `host()`).
pub struct Direct {
    /// Does this controller know `recipient` as a workspace, sidecar or session?
    pub resolves: Box<dyn Fn(&str) -> bool + Send + Sync>,
    /// The ids `recipient` may be placed under, as `K2_ROLE=all` resolves
    /// it: a workspace name or path gives its project id, its spawn name
    /// and its saved session ids; a session id gives itself. Each is
    /// matched against placement ids and placements' session ids.
    pub aliases: Box<dyn Fn(&str) -> Vec<String> + Send + Sync>,
    /// Deliver a `kind=msg` body to `recipient` now (blocking: it may wake
    /// the agent). `Err` is the reason it wasn't delivered.
    pub deliver: Box<dyn Fn(&str, &str, &Value) -> Result<(), String> + Send + Sync>,
}

impl Direct {
    /// Every recipient goes through the outbox.
    pub fn none() -> Self {
        Direct {
            resolves: Box::new(|_| false),
            aliases: Box::new(|_| Vec::new()),
            deliver: Box::new(|_, _, _| Err("no direct delivery".into())),
        }
    }

    /// The daemon's own resolver and cascade (`workspace_msg`).
    pub fn production() -> Self {
        Direct {
            resolves: Box::new(|r| crate::workspace_msg::resolve_msg_target(r).is_some()),
            aliases: Box::new(crate::workspace_msg::placement_aliases),
            deliver: Box::new(crate::workspace_msg::deliver_for_controller),
        }
    }
}

thread_local! {
    static DIRECT_DELIVERY: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

/// The message [`Controller::deliver_gone`] is delivering on this thread,
/// while it runs: a spawn it wakes the agent with names it (K2-c).
struct DirectDelivery;

impl DirectDelivery {
    fn during(msg_id: &str) -> Self {
        DIRECT_DELIVERY.with(|d| *d.borrow_mut() = Some(msg_id.to_string()));
        DirectDelivery
    }
}

impl Drop for DirectDelivery {
    fn drop(&mut self) {
        DIRECT_DELIVERY.with(|d| *d.borrow_mut() = None);
    }
}

/// The message the gone rule is delivering on this thread, if any.
pub(crate) fn direct_delivery_id() -> Option<String> {
    DIRECT_DELIVERY.with(|d| d.borrow().clone())
}

/// What the controller does with a `deliver` from a host.
#[derive(Debug, PartialEq, Eq)]
pub enum Taken {
    /// Ack now.
    Ack(DeliverStatus, Option<String>),
    /// Run [`Controller::deliver_direct`] off the link's task, then ack.
    Direct,
    /// The first copy is still being delivered directly: no ack; the host resends.
    InFlight,
}

const HANDLING: &str = "handling";

fn controllers() -> &'static Mutex<Vec<Arc<Controller>>> {
    static C: OnceLock<Mutex<Vec<Arc<Controller>>>> = OnceLock::new();
    C.get_or_init(Mutex::default)
}

/// The drain for `remote`, if one was started.
fn controller_for(remote: &RemoteHost) -> Option<Arc<Controller>> {
    controllers().lock().iter().find(|c| std::ptr::eq(c.remote, remote)).cloned()
}

/// Start the drain for `remote` on this runtime (once per `remote`).
pub fn start_controller(remote: &'static RemoteHost, db: Db, policy: Policy, direct: Direct) -> Arc<Controller> {
    let mut all = controllers().lock();
    if let Some(c) = all.iter().find(|c| std::ptr::eq(c.remote, remote)) {
        return Arc::clone(c);
    }
    let c = Arc::new(Controller {
        remote,
        db: ready(db),
        policy,
        nudge: Notify::new(),
        direct,
        gone_grace_ms: AtomicI64::new(DEFAULT_GONE_GRACE.as_millis() as i64),
        handing: Default::default(),
    });
    all.push(Arc::clone(&c));
    tokio::spawn(Arc::clone(&c).run());
    log_debug!("[delivery] controller drain started");
    c
}

/// The production controller drain (the daemon's database, the global
/// `RemoteHost`), started on first use. The gate calls it when a host
/// links, so a restarted controller drains what it had queued.
pub fn ensure_controller_drain() -> Arc<Controller> {
    let c = start_controller(crate::agent_host::remote::global(), k2_core::db::shared(), Policy::PRODUCTION, Direct::production());
    c.set_gone_grace(gone_grace(|k| std::env::var(k).ok()));
    c
}

/// `RemoteHost::on_frame`'s hook for `deliver` and `deliver.ack` from a host.
pub fn on_host_frame(remote: &RemoteHost, host_id: &str, frame: Frame) {
    let Some(c) = controller_for(remote) else {
        log_debug!("[delivery] no controller drain yet; dropped {} from host {host_id} (it resends)", frame.kind());
        return;
    };
    match frame {
        Frame::Deliver { msg_id, agent_id, from, body, attempt, .. } => {
            let (id, to) = (msg_id.clone(), agent_id.clone());
            let ack = move |c: &Controller, status: DeliverStatus, reason: Option<String>, host_id: &str| {
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
            };
            match c.take(&id, &from, &to, &body, delivery::now_ms()) {
                Taken::Ack(status, reason) => ack(&c, status, reason, host_id),
                Taken::InFlight => {}
                Taken::Direct => {
                    let host_id = host_id.to_string();
                    tokio::task::spawn_blocking(move || {
                        let (status, reason) = c.deliver_direct(&id, &from, &to, &body, delivery::now_ms());
                        ack(&c, status, reason, &host_id);
                    });
                }
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

    /// Does `recipient` skip the outbox? A `kind=msg` body for a recipient
    /// with no placement that this controller resolves itself.
    pub fn is_direct(&self, recipient: &str, body: &Value) -> bool {
        matches!(body.get("kind").and_then(Value::as_str), Some("msg") | None)
            && self.placed(recipient).is_none()
            && !self.awaiting_resume(recipient)
            && (self.direct.resolves)(recipient)
    }

    /// Where `recipient` runs: its own placement, else one of its
    /// [`Direct::aliases`] (a placement id, or a placement's session id).
    pub fn placed(&self, recipient: &str) -> Option<RemotePlacement> {
        if let Some(p) = self.remote.placement(recipient) {
            return Some(p);
        }
        let aliases = (self.direct.aliases)(recipient);
        if aliases.is_empty() {
            return None;
        }
        aliases.iter().find_map(|a| self.remote.placement(a)).or_else(|| {
            self.remote.placements().into_iter().find(|p| p.session_id.as_ref().is_some_and(|s| aliases.contains(s)))
        })
    }

    /// Was `recipient` (or one of its aliases) drained off a host and not
    /// placed again yet? Its messages wait in the outbox (S8).
    fn awaiting_resume(&self, recipient: &str) -> bool {
        self.remote.awaiting_resume(recipient) || (self.direct.aliases)(recipient).iter().any(|a| self.remote.awaiting_resume(a))
    }

    /// A `deliver` from a host: answered from `delivery_seen` when this
    /// controller handled it directly before, handed to
    /// [`Self::deliver_direct`] when [`Self::is_direct`], else [`Self::accept`].
    pub fn take(&self, msg_id: &str, from: &str, agent_id: &str, body: &Value, now: i64) -> Taken {
        if !msg_id.is_empty() {
            let conn = self.db.lock();
            match seen::status(&conn, msg_id) {
                Ok(Some(s)) if s == HANDLING => return Taken::InFlight,
                Ok(Some(s)) => {
                    return match s.strip_prefix("rejected: ") {
                        Some(why) => Taken::Ack(DeliverStatus::Rejected, Some(why.to_string())),
                        None => Taken::Ack(DeliverStatus::Duplicate, None),
                    }
                }
                Ok(None) => {}
                Err(e) => return Taken::Ack(DeliverStatus::Rejected, Some(format!("controller database: {e}"))),
            }
            let queued = outbox::get(&conn, Hop::Down, msg_id).ok().flatten().is_some();
            if !queued && !agent_id.is_empty() && self.is_direct(agent_id, body) {
                return match seen::record(&conn, msg_id, HANDLING, now, self.policy.seen_cap) {
                    Ok(()) => Taken::Direct,
                    Err(e) => Taken::Ack(DeliverStatus::Rejected, Some(format!("controller database: {e}"))),
                };
            }
        }
        let (status, reason) = self.accept(msg_id, from, agent_id, body, now);
        Taken::Ack(status, reason)
    }

    /// Deliver a host's message through [`Direct::deliver`] and remember
    /// the answer for its resends.
    pub fn deliver_direct(&self, msg_id: &str, from: &str, agent_id: &str, body: &Value, now: i64) -> (DeliverStatus, Option<String>) {
        let r = (self.direct.deliver)(agent_id, from, body);
        let (status, reason, seen_as) = match r {
            Ok(()) => (DeliverStatus::Delivered, None, "delivered".to_string()),
            Err(why) => (DeliverStatus::Rejected, Some(why.clone()), format!("rejected: {why}")),
        };
        log_debug!("[delivery] {msg_id} {from} → {agent_id}: delivered directly: {seen_as}");
        if let Err(e) = seen::record(&self.db.lock(), msg_id, &seen_as, now, self.policy.seen_cap) {
            log_debug!("[delivery] ERROR: can't record {msg_id}: {e}");
        }
        (status, reason)
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
                let why = match reason {
                    // The host can't tell whether the agent got it (I2: a
                    // visible failure, never silently delivered).
                    Some(r) if r.starts_with(UNCERTAIN_REASON) => format!("{r} (host {host_id})"),
                    _ => format!("rejected by host {host_id}: {}", reason.unwrap_or("no reason given")),
                };
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

    pub fn set_gone_grace(&self, d: Duration) {
        self.gone_grace_ms.store(d.as_millis() as i64, Ordering::SeqCst);
    }

    /// When `recipient`'s session ended (itself or through an alias), if
    /// it did and nothing placed it again: the latest such end.
    fn ended_at(&self, recipient: &str) -> Option<i64> {
        let mut ids = vec![recipient.to_string()];
        ids.extend((self.direct.aliases)(recipient));
        ids.iter().filter_map(|a| self.remote.session_ended_at(a)).max()
    }

    /// A due row with no placement. Its recipient's host being lost keeps
    /// it waiting (S6), as does a drain (S8). If its recipient's session
    /// ended and nothing placed it again within the grace: a `kind=msg`
    /// row whose address resolves on this controller goes through the
    /// direct path (returned, to run off this task; today's wake rules
    /// apply), anything else fails `recipient_gone` (I2: final, loud).
    fn after_gone(&self, conn: &Connection, r: &Row, now: i64) -> Option<Row> {
        if self.awaiting_resume(&r.recipient) {
            return None;
        }
        let ended = self.ended_at(&r.recipient)?;
        let grace = self.gone_grace_ms.load(Ordering::SeqCst);
        if now - ended < grace {
            return None;
        }
        let body: Value = serde_json::from_str(&r.body).unwrap_or(Value::Null);
        let is_msg = matches!(body.get("kind").and_then(Value::as_str), Some("msg") | None);
        if is_msg && (self.direct.resolves)(&r.recipient) {
            return self.handing.lock().insert(r.msg_id.clone()).then(|| r.clone());
        }
        let why = format!(
            "recipient_gone: {}'s session ended and nothing placed it again within {} s",
            r.recipient,
            grace / 1000
        );
        match outbox::mark_failed(conn, Hop::Down, &r.msg_id, &why, now) {
            Ok(true) => {
                if let Ok(Some(row)) = outbox::get(conn, Hop::Down, &r.msg_id) {
                    report_failed(Hop::Down, &[row]);
                }
            }
            Ok(false) => {}
            Err(e) => log_debug!("[delivery] ERROR: fail {}: {e}", r.msg_id),
        }
        None
    }

    /// Deliver a row [`Self::after_gone`] handed over through
    /// [`Direct::deliver`] (blocking: it may wake the agent).
    pub fn deliver_gone(&self, r: &Row) {
        let body: Value = serde_json::from_str(&r.body).unwrap_or(Value::Null);
        // K2-c: a host may have delivered it before its agent died; the
        // spawn that wakes the agent carries the id, so that host's seen
        // table decides before the prompt goes on argv.
        let res = {
            let _id = DirectDelivery::during(&r.msg_id);
            (self.direct.deliver)(&r.recipient, &r.sender, &body)
        };
        let now = delivery::now_ms();
        let conn = self.db.lock();
        match &res {
            Ok(()) => {
                let _ = outbox::mark_delivered(&conn, Hop::Down, &r.msg_id, now);
                log_debug!("[delivery] {} to {} delivered directly: its session had ended", r.msg_id, r.recipient);
            }
            Err(why) => {
                let reason = format!("{why}: {}'s session ended; the direct path didn't deliver it", r.recipient);
                if outbox::mark_failed(&conn, Hop::Down, &r.msg_id, &reason, now).unwrap_or(false) {
                    if let Ok(Some(row)) = outbox::get(&conn, Hop::Down, &r.msg_id) {
                        report_failed(Hop::Down, &[row]);
                    }
                }
            }
        }
        drop(conn);
        k2_core::trace::emit(|| {
            let e = k2_core::trace::Event::new(events::CTL_MSG_GONE_DIRECT)
                .msg(r.msg_id.as_str())
                .agent(r.recipient.as_str())
                .attr("ok", res.is_ok());
            match &res {
                Err(why) => e.attr("reason", why.as_str()),
                Ok(()) => e,
            }
        });
        self.handing.lock().remove(&r.msg_id);
    }

    /// One drain pass at `now`: dead letters first, then send every due
    /// row whose recipient is placed on a linked host. Returns the rows
    /// for [`Self::deliver_gone`].
    pub fn pass(&self, now: i64) -> Vec<Row> {
        let conn = self.db.lock();
        match outbox::sweep(&conn, Hop::Down, now, &self.policy) {
            Ok(failed) => report_failed(Hop::Down, &failed),
            Err(e) => log_debug!("[delivery] ERROR: sweep: {e}"),
        }
        let due = match outbox::due(&conn, Hop::Down, now) {
            Ok(d) => d,
            Err(e) => {
                log_debug!("[delivery] ERROR: due: {e}");
                return Vec::new();
            }
        };
        let mut gone = Vec::new();
        for r in due {
            if self.handing.lock().contains(&r.msg_id) {
                continue;
            }
            // Not placed: wait (no attempt spent) until it is, the TTL, or
            // (its session ended) the gone rule. The address stays the
            // row's; the host gets the placement id.
            let Some(p) = self.placed(&r.recipient) else {
                gone.extend(self.after_gone(&conn, &r, now));
                continue;
            };
            let attempt = r.attempts + 1;
            let mut frame = deliver_frame(&r, Some(p.epoch.0), attempt);
            if let Frame::Deliver { agent_id, .. } = &mut frame {
                agent_id.clone_from(&p.agent_id);
            }
            match self.remote.send_frame(&p.host_id, frame) {
                Ok(()) => {
                    if let Err(e) = outbox::mark_sent(&conn, Hop::Down, &r.msg_id, now, &self.policy) {
                        log_debug!("[delivery] ERROR: mark sent {}: {e}", r.msg_id);
                    }
                    log_debug!(
                        "[delivery] deliver {} to {} ({}) on host {} (attempt {attempt})",
                        r.msg_id,
                        r.recipient,
                        p.agent_id,
                        p.host_id
                    );
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
        gone
    }

    /// The row for `msg_id` (tests and the status route).
    pub fn get(&self, msg_id: &str) -> Option<Row> {
        outbox::get(&self.db.lock(), Hop::Down, msg_id).ok().flatten()
    }

    async fn run(self: Arc<Self>) {
        loop {
            let now = delivery::now_ms();
            for r in self.pass(now) {
                let c = Arc::clone(&self);
                tokio::task::spawn_blocking(move || c.deliver_gone(&r));
            }
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
    /// Not now, and nothing was written (no live session, PTY busy): no
    /// ack, the controller resends.
    Retry(String),
    /// The inject began and its outcome is unknown (the agent died
    /// mid-inject): the agent's transcript decides, as after a restart.
    Interrupted(String),
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
/// A host's write-ahead `seen` status: set before the inject, replaced by
/// `delivered` or `rejected` after it, dropped when nothing was delivered.
pub const DELIVERING: &str = "delivering";
/// A delivery a restart interrupted whose transcript couldn't tell.
pub const UNCERTAIN: &str = "uncertain";
/// The reason a controller fails such a row with (I2: loud, never silent).
pub const UNCERTAIN_REASON: &str = "delivery_uncertain";
const REPORTED: &str = "reported again when the host linked";

#[derive(Debug, Clone, PartialEq, Eq)]
enum Settled {
    Delivered(String),
    NotDelivered,
    /// Not in the transcript yet, and the agent is mid-turn: it may still
    /// take the prompt. No answer; decided on a later receive or link.
    Pending,
    Uncertain(String),
}

/// Where an agent's transcript stands before an inject.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TranscriptMark {
    pub provider: Option<String>,
    pub conversation: Option<String>,
    /// The session's cwd, canonical (`/tmp` and `/private/tmp` are one).
    pub cwd: Option<String>,
    /// Its length now (0 when it doesn't exist yet).
    pub offset: u64,
}

/// What the transcript says about a written-ahead delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TranscriptCheck {
    /// A user prompt after the write-ahead's offset is the injected text.
    Holds,
    /// It is there and readable, doesn't, and won't: the agent is idle, or
    /// took a later prompt.
    Lacks,
    /// Not yet, while the agent is mid-turn.
    Pending,
    /// It can't be read (missing, the agent's home is gone, no
    /// conversation known).
    Unreadable(String),
}

/// The agents' transcripts, as delivery settles an interrupted inject.
pub trait Transcripts: Send + Sync {
    fn mark(&self, agent_id: &str) -> TranscriptMark;
    /// The written-ahead conversation's transcript; `Err` when it can't
    /// be found.
    fn locate(&self, w: &k2_core::delivery::seen::WriteAhead) -> Result<std::path::PathBuf, String>;
    /// Is the agent mid-turn (a prompt typed into it may not be recorded
    /// yet)?
    fn busy(&self, agent_id: &str) -> bool;
}

/// One user prompt in a transcript.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserPrompt {
    /// The byte offset of its line: one prompt, one claim.
    pub at: u64,
    pub text: String,
    /// When the CLI recorded it, if the line says.
    pub ts_ms: Option<i64>,
}

/// The user prompts after `offset` in the transcript at `path`: lines whose
/// `type` or role is `user`, their text (a string, or the text blocks; a
/// tool result is not a prompt). Claude's and Codex's layouts.
pub fn scan_user_prompts(path: &std::path::Path, offset: u64) -> Result<Vec<UserPrompt>, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    if (bytes.len() as u64) < offset {
        return Err(format!("{} is shorter than before the inject (rewritten)", path.display()));
    }
    fn text_of(content: &Value) -> Option<String> {
        match content {
            Value::String(s) => Some(s.clone()),
            Value::Array(blocks) => {
                let parts: Vec<&str> = blocks
                    .iter()
                    .filter(|b| matches!(b.get("type").and_then(Value::as_str), Some("text" | "input_text")))
                    .filter_map(|b| b.get("text").and_then(Value::as_str))
                    .collect();
                (!parts.is_empty()).then(|| parts.join("\n"))
            }
            _ => None,
        }
    }
    fn prompt_of(v: &Value) -> Option<String> {
        let role = |x: &Value| x.get("role").and_then(Value::as_str) == Some("user");
        // Codex: {"type":"event_msg","payload":{"type":"user_message","message":…}}
        if let Some(p) = v.get("payload") {
            if p.get("type").and_then(Value::as_str) == Some("user_message") {
                return p.get("message").and_then(Value::as_str).map(str::to_string);
            }
            if role(p) {
                return p.get("content").and_then(text_of);
            }
        }
        // Claude: {"type":"user","message":{"role":"user","content":…}}
        if let Some(m) = v.get("message").filter(|m| m.is_object()) {
            if role(m) || v.get("type").and_then(Value::as_str) == Some("user") {
                return m.get("content").and_then(text_of);
            }
            return None;
        }
        if role(v) {
            return v.get("content").and_then(text_of);
        }
        None
    }
    let mut out = Vec::new();
    let mut at = offset;
    for line in bytes[offset as usize..].split_inclusive(|b| *b == b'\n') {
        let here = at;
        at += line.len() as u64;
        let Ok(v) = serde_json::from_slice::<Value>(line) else { continue };
        if let Some(text) = prompt_of(&v) {
            let ts_ms = v
                .get("timestamp")
                .and_then(Value::as_str)
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                .map(|t| t.timestamp_millis());
            out.push(UserPrompt { at: here, text, ts_ms });
        }
    }
    Ok(out)
}

/// A prompt as compared: no ESC (the injector strips it), one newline
/// style, no surrounding whitespace.
fn prompt_key(s: &str) -> String {
    s.replace("\r\n", "\n").chars().filter(|&c| c != '\u{1b}').collect::<String>().trim().to_string()
}

/// A delivery still pending this long after its inject is uncertain.
const PENDING_MAX_MS: i64 = 10 * 60 * 1000;

/// The transcript of `conversation`: by the cwd (canonical, then as
/// given), then by the conversation id alone.
fn locate_transcript(provider: &str, conversation: &str, cwd: Option<&str>) -> Option<std::path::PathBuf> {
    let locate = |dir: &str| k2_core::chat_continue::locate_continue_transcript(provider, conversation, dir);
    let canonical = cwd.and_then(|c| std::fs::canonicalize(c).ok()).map(|p| p.to_string_lossy().into_owned());
    canonical
        .as_deref()
        .and_then(locate)
        .or_else(|| cwd.filter(|c| Some(*c) != canonical.as_deref()).and_then(locate))
        .or_else(|| locate(""))
}

/// The transcripts of this host's agents: the session's CLI, conversation
/// id and cwd, found again after a restart by `locate_continue_transcript`.
pub struct LiveTranscripts;

impl Transcripts for LiveTranscripts {
    fn mark(&self, agent_id: &str) -> TranscriptMark {
        let Some(s) = crate::agent_host::host().find_agent(agent_id) else { return TranscriptMark::default() };
        let program = s.command().unwrap_or_default();
        let provider = std::path::Path::new(&program).file_name().map(|n| n.to_string_lossy().into_owned()).filter(|p| !p.is_empty());
        let conversation = crate::activity_transcript::conversation_of(&s.session_id().to_string())
            .map(|(c, _)| c)
            .or_else(|| k2_core::workspace::provider_resume::session_id_from_spawn_argv(&program, &s.args()));
        let cwd = s.cwd_path().map(|p| std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf()).to_string_lossy().into_owned());
        let offset = match (&provider, &conversation) {
            (Some(p), Some(c)) => locate_transcript(p, c, cwd.as_deref())
                .and_then(|path| std::fs::metadata(path).ok())
                .map(|m| m.len())
                .unwrap_or(0),
            _ => 0,
        };
        TranscriptMark { provider, conversation, cwd, offset }
    }

    fn locate(&self, w: &k2_core::delivery::seen::WriteAhead) -> Result<std::path::PathBuf, String> {
        let (Some(provider), Some(conv)) = (w.provider.as_deref(), w.conversation.as_deref()) else {
            return Err("the agent's conversation wasn't known".into());
        };
        // Missing is not "lacks it": a CLI that never wrote the file may
        // still have the prompt on screen, or its home may be gone.
        locate_transcript(provider, conv, w.cwd.as_deref())
            .ok_or_else(|| format!("no {provider} transcript for conversation {conv}"))
    }

    fn busy(&self, agent_id: &str) -> bool {
        // A dead CLI takes nothing more, whatever its last activity said.
        let Some(s) = crate::agent_host::host().find_agent(agent_id).filter(|s| s.is_child_alive()) else { return false };
        crate::activity_store::with_row(&s.session_id().to_string(), |r| r.display.is_live()).unwrap_or(false)
    }
}

/// What a host reports on each link: deliveries this recent, this many.
const RELINK_REPORT_WINDOW_MS: i64 = 10 * 60 * 1000;
const RELINK_REPORT_MAX: usize = 256;

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
    /// The agents' transcripts: the ground truth for a delivery a restart
    /// interrupted.
    transcripts: Arc<dyn Transcripts>,
    /// Transcript prompts (file, line offset) already taken as some
    /// delivery's: one prompt settles one write-ahead row.
    claimed: Mutex<HashSet<(std::path::PathBuf, u64)>>,
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
            transcripts: Arc::new(LiveTranscripts),
            claimed: Mutex::default(),
        }
    }

    pub fn with_transcripts(self, transcripts: Arc<dyn Transcripts>) -> Self {
        Self { transcripts, ..self }
    }

    /// A `deliver` from the controller: deliver once per `msg_id`. Returns
    /// the ack to send, or `None` to stay silent so the controller resends.
    pub fn receive(&self, m: &Incoming, now: i64) -> Option<Frame> {
        // A draining host takes nothing new: no ack, the controller keeps
        // the message and resends it once the agent is placed again.
        if crate::hosts::drain::draining() {
            return None;
        }
        let _one = self.receiving.lock();
        let ack = |status, reason: Option<String>| Some(Frame::DeliverAck { msg_id: m.msg_id.clone(), status, reason });
        let before = seen::status(&self.db.lock(), &m.msg_id);
        let out = match before {
            Ok(Some(s)) if s == "rejected" => ack(DeliverStatus::Rejected, Some("rejected before".into())),
            // This daemon died mid-delivery: the agent's transcript says
            // whether the message reached it.
            Ok(Some(s)) if s == DELIVERING => match self.settle_interrupted(&m.msg_id, now) {
                Settled::Delivered(why) => ack(DeliverStatus::Delivered, Some(why)),
                Settled::Uncertain(why) => ack(DeliverStatus::Rejected, Some(why)),
                Settled::NotDelivered => self.deliver_fresh(m, now),
                // The controller resends; the transcript decides then.
                Settled::Pending => None,
            },
            Ok(Some(s)) if s == UNCERTAIN => ack(DeliverStatus::Rejected, Some(format!("{UNCERTAIN_REASON}: decided before"))),
            Ok(Some(_)) => ack(DeliverStatus::Duplicate, None),
            Err(e) => {
                log_debug!("[delivery] ERROR: seen check {}: {e}", m.msg_id);
                None
            }
            Ok(None) => self.deliver_fresh(m, now),
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

    /// Deliver a message this host has no record of: write ahead (the seen
    /// row `delivering`, and where the agent's transcript stands), inject,
    /// then the answer.
    fn deliver_fresh(&self, m: &Incoming, now: i64) -> Option<Frame> {
        let ack = |status, reason: Option<String>| Some(Frame::DeliverAck { msg_id: m.msg_id.clone(), status, reason });
        let mark = self.transcripts.mark(&m.agent_id);
        let w = seen::WriteAhead {
            msg_id: m.msg_id.clone(),
            agent_id: m.agent_id.clone(),
            provider: mark.provider,
            conversation: mark.conversation,
            cwd: mark.cwd,
            offset: mark.offset,
            needle: crate::workspace_msg::injected_text(&m.from, &m.body),
            started_ms: now,
        };
        if let Err(e) = seen::write_ahead(&self.db.lock(), &w) {
            log_debug!("[delivery] ERROR: write ahead {}: {e}", m.msg_id);
        }
        // From here a kill leaves the id on record, settled from the
        // transcript on the next receive or link.
        self.record_seen(&m.msg_id, DELIVERING, now);
        let out = match (self.deliver)(m) {
            LocalDelivery::Delivered => {
                self.record_seen(&m.msg_id, "delivered", now);
                ack(DeliverStatus::Delivered, None)
            }
            LocalDelivery::Rejected(why) => {
                self.record_seen(&m.msg_id, "rejected", now);
                ack(DeliverStatus::Rejected, Some(why))
            }
            // K2-c: once written ahead, never a bare retry for an inject
            // that may have landed; the transcript settles it.
            LocalDelivery::Interrupted(why) => {
                log_debug!("[delivery] {} for {} interrupted mid-inject: {why}", m.msg_id, m.agent_id);
                return match self.settle_interrupted(&m.msg_id, now) {
                    Settled::Delivered(why) => ack(DeliverStatus::Delivered, Some(why)),
                    Settled::Uncertain(why) => ack(DeliverStatus::Rejected, Some(why)),
                    // Forgotten: a resend delivers it.
                    Settled::NotDelivered => None,
                    // Kept `delivering`: a resend or the next link decides.
                    Settled::Pending => None,
                };
            }
            LocalDelivery::Retry(why) => {
                // Nothing reached the agent: a resend delivers it.
                if let Err(e) = seen::forget(&self.db.lock(), &m.msg_id) {
                    log_debug!("[delivery] ERROR: forget {}: {e}", m.msg_id);
                }
                log_debug!("[delivery] {} for {} not delivered yet: {why}", m.msg_id, m.agent_id);
                None
            }
        };
        let _ = seen::clear_ahead(&self.db.lock(), &m.msg_id);
        out
    }

    /// An id a restart left `delivering`: the transcript decides. Delivered
    /// (it holds the message): `delivered`. Not delivered (it doesn't, and
    /// the agent is idle or took a later prompt): the record goes, so the
    /// message is delivered normally. Pending (not yet, mid-turn): kept as
    /// it is. Can't tell (the transcript is missing or unreadable):
    /// `uncertain`, which the controller fails with a reason (I2: never
    /// silently either way).
    fn settle_interrupted(&self, msg_id: &str, now: i64) -> Settled {
        let w = seen::written_ahead(&self.db.lock(), msg_id).ok().flatten();
        let found = match &w {
            Some(w) => self.check(w, now),
            None => TranscriptCheck::Unreadable("no write-ahead record".into()),
        };
        let settled = match found {
            TranscriptCheck::Holds => {
                self.record_seen(msg_id, "delivered", now);
                Settled::Delivered("found in the agent's transcript after an interrupted inject".into())
            }
            TranscriptCheck::Lacks => {
                if let Err(e) = seen::forget(&self.db.lock(), msg_id) {
                    log_debug!("[delivery] ERROR: forget {msg_id}: {e}");
                }
                Settled::NotDelivered
            }
            TranscriptCheck::Pending => {
                log_debug!("[delivery] {msg_id}: interrupted delivery not in the transcript yet; the agent is mid-turn");
                return Settled::Pending;
            }
            TranscriptCheck::Unreadable(why) => {
                self.record_seen(msg_id, UNCERTAIN, now);
                Settled::Uncertain(format!("{UNCERTAIN_REASON}: interrupted mid-inject (a host restart, or the agent died) and the agent's transcript can't tell ({why})"))
            }
        };
        let _ = seen::clear_ahead(&self.db.lock(), msg_id);
        log_debug!("[delivery] {msg_id}: interrupted delivery settled: {settled:?}");
        settled
    }

    /// Does the transcript hold the written-ahead prompt? Only a user
    /// prompt after the offset, on the exact injected text, that no other
    /// row has claimed, counts (an agent's reply quoting it doesn't).
    fn check(&self, w: &seen::WriteAhead, now: i64) -> TranscriptCheck {
        let Some(needle) = w.needle.as_deref().map(prompt_key) else {
            return TranscriptCheck::Unreadable("not a message a transcript records".into());
        };
        let path = match self.transcripts.locate(w) {
            Ok(p) => p,
            Err(why) => return TranscriptCheck::Unreadable(why),
        };
        let prompts = match scan_user_prompts(&path, w.offset) {
            Ok(p) => p,
            Err(why) => return TranscriptCheck::Unreadable(why),
        };
        let mut claimed = self.claimed.lock();
        if let Some(p) = prompts.iter().find(|p| prompt_key(&p.text) == needle && !claimed.contains(&(path.clone(), p.at))) {
            claimed.insert((path, p.at));
            return TranscriptCheck::Holds;
        }
        // A prompt the CLI took after the inject began: ours, typed first,
        // would be before it.
        let moved_on = prompts.iter().any(|p| p.ts_ms.is_some_and(|t| t >= w.started_ms));
        if moved_on || !self.transcripts.busy(&w.agent_id) {
            TranscriptCheck::Lacks
        } else if now - w.started_ms > PENDING_MAX_MS {
            TranscriptCheck::Unreadable(format!("not in the transcript {} s after the inject, the agent still mid-turn", PENDING_MAX_MS / 1000))
        } else {
            TranscriptCheck::Pending
        }
    }

    /// K2-c: a spawn that puts message `msg_id` on its agent's argv (the
    /// gone rule's direct path) goes through the same per-id dedup as a
    /// receive. `spawn(with_prompt)` spawns, with the prompt or without,
    /// and says whether the prompt went on argv. `Ok((_, true))`: the
    /// agent had it already (seen, or found in its transcript), so it was
    /// spawned without it. `Err` when it can't tell (`delivery_uncertain`),
    /// or the spawn failed.
    pub fn launch_once<T>(
        &self,
        msg_id: &str,
        now: i64,
        spawn: impl FnOnce(bool) -> Result<(T, bool), String>,
    ) -> Result<(T, bool), String> {
        let _one = self.receiving.lock();
        let had = match seen::status(&self.db.lock(), msg_id) {
            Ok(None) => false,
            Ok(Some(s)) if s == "delivered" => true,
            Ok(Some(s)) if s == DELIVERING => match self.settle_interrupted(msg_id, now) {
                Settled::Delivered(_) => true,
                Settled::NotDelivered => false,
                Settled::Uncertain(why) => return Err(why),
                Settled::Pending => return Err(format!("{UNCERTAIN_REASON}: its agent is mid-turn and its transcript doesn't show it yet")),
            },
            Ok(Some(s)) if s == UNCERTAIN => return Err(format!("{UNCERTAIN_REASON}: decided before")),
            // Rejected before: the agent never got it.
            Ok(Some(_)) => false,
            Err(e) => return Err(format!("seen check {msg_id}: {e}")),
        };
        if had {
            log_debug!("[delivery] {msg_id}: the agent has it already; spawned without it");
            return spawn(false).map(|(out, _)| (out, true));
        }
        // Written ahead with no transcript place: a kill mid-spawn leaves
        // it uncertain, never delivered twice.
        self.record_seen(msg_id, DELIVERING, now);
        match spawn(true) {
            Ok((out, true)) => {
                self.record_seen(msg_id, "delivered", now);
                Ok((out, false))
            }
            other => {
                // Not on argv (the caller injects it, or the spawn failed).
                if let Err(e) = seen::forget(&self.db.lock(), msg_id) {
                    log_debug!("[delivery] ERROR: forget {msg_id}: {e}");
                }
                other.map(|(out, _)| (out, false))
            }
        }
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

    /// The link to the controller is up and writes to `tx`. Blocks (the
    /// transcripts and SQLite): run it off the async workers.
    pub fn linked(&self, tx: mpsc::UnboundedSender<Frame>) {
        {
            // A delivery mid-inject holds this: its row is settled by its
            // own receive, never by the report.
            let _one = self.receiving.lock();
            self.report_delivered(&tx, delivery::now_ms());
        }
        *self.link.lock() = Some(tx);
        self.nudge.notify_one();
    }

    /// On every link: ack again what this host delivered (or was
    /// delivering when it died) lately. An ack lost to a crash or a link
    /// drop would otherwise leave the controller's row `sent`, and the gone
    /// rule would fail (or resend) a message the agent already has.
    fn report_delivered(&self, tx: &mpsc::UnboundedSender<Frame>, now: i64) {
        let rows = match seen::recent_delivered(&self.db.lock(), now - RELINK_REPORT_WINDOW_MS, RELINK_REPORT_MAX) {
            Ok(r) => r,
            Err(e) => {
                log_debug!("[delivery] ERROR: recent deliveries: {e}");
                return;
            }
        };
        if !rows.is_empty() {
            log_debug!("[delivery] link: reporting {} recent deliver(ies) to the controller", rows.len());
        }
        for (msg_id, status) in rows {
            let (status, reason) = if status == DELIVERING {
                match self.settle_interrupted(&msg_id, now) {
                    Settled::Delivered(why) => (DeliverStatus::Delivered, Some(why)),
                    Settled::Uncertain(why) => (DeliverStatus::Rejected, Some(why)),
                    // The controller still has it: it delivers it again.
                    Settled::NotDelivered => continue,
                    // Decided on its resend, or the next link.
                    Settled::Pending => continue,
                }
            } else if status == UNCERTAIN {
                (DeliverStatus::Rejected, Some(format!("{UNCERTAIN_REASON}: decided before")))
            } else {
                (DeliverStatus::Delivered, Some(REPORTED.to_string()))
            };
            let _ = tx.send(Frame::DeliverAck { msg_id, status, reason });
        }
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
    // The drain (S8) needs every turn this host runs from here on.
    crate::hosts::drain::track_turns();
    let h = host_side();
    let report = Arc::clone(h);
    tokio::task::spawn_blocking(move || report.linked(tx));
    STARTED.get_or_init(|| {
        tokio::spawn(Arc::clone(h).run());
    });
}

/// [`HostSide::launch_once`] on this host (`link`'s `spawn.workspace`).
pub fn host_launch_once<T>(msg_id: &str, spawn: impl FnOnce(bool) -> Result<(T, bool), String>) -> Result<(T, bool), String> {
    host_side().launch_once(msg_id, delivery::now_ms(), spawn)
}

/// The current link to the controller, if any (the drain's channel).
pub fn host_link() -> Option<mpsc::UnboundedSender<Frame>> {
    host_side().link.lock().clone()
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
/// (plus the wake timeout when the body asks for a wake) for the
/// controller's ack. `None` under `K2_ROLE=all`, and on a controller for a
/// message it delivers directly ([`Controller::is_direct`]): the caller
/// runs today's cascade.
pub fn send(from: &str, agent_id: &str, body: Value) -> Option<Result<Sent, String>> {
    let sender = sender_address(from);
    match crate::runtime_role::current() {
        crate::runtime_role::Role::All => None,
        crate::runtime_role::Role::Controller => {
            let c = ensure_controller_drain();
            if c.is_direct(agent_id, &body) {
                return None;
            }
            Some(c.send(&sender, agent_id, body).map(|msg_id| Sent { msg_id, state: "accepted", reason: None }))
        }
        crate::runtime_role::Role::Host => {
            let wait = ACCEPT_WAIT + wake_wait(&body);
            Some(host_send(host_side(), &sender, agent_id, body, wait))
        }
    }
}

/// The wake timeout a `kind=msg` body asks for (zero without a wake).
fn wake_wait(body: &Value) -> Duration {
    if body.get("wake").and_then(Value::as_bool) != Some(true) {
        return Duration::ZERO;
    }
    body.get("wake_timeout_ms").and_then(Value::as_u64).map(Duration::from_millis).unwrap_or(Duration::ZERO)
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
