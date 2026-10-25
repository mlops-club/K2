//! The remote agent host, controller side (PRD P2, first cut: scenario S1
//! plus hooks and `/login`).
//!
//! Hosts dial in over `/cli/hosts/attach` (`crate::hosts::hub`, the
//! socket half). This file is the state behind those links: which hosts
//! are connected, the placement table (agent id → host, session, epoch),
//! requests waiting for a host's reply, and the grid streams relayed
//! through each link. No socket code here; links talk to it through
//! [`RemoteHost::connect`], [`RemoteHost::on_frame`] and the frame
//! channel each link drains.
//!
//! Not an `impl AgentHost` yet: `AgentSession` is an in-process handle
//! (see `docs/prd/progress/dist-link.md`). The controller's S1 routes call
//! [`RemoteHost::spawn`], [`RemoteHost::close`] and
//! [`RemoteHost::open_grid`] directly.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use k2_core::log_debug;
use parking_lot::Mutex;
use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc, oneshot};

use super::PlacementEpoch;
use crate::hosts::proto::Frame;

/// How long the controller waits for a host to answer a spawn or close.
/// Only stops a request to a hung host; tests never depend on it.
const REPLY_DEADLINE: Duration = Duration::from_secs(120);

/// Message when a spawn finds no host (acceptance criterion 3).
pub const NO_HOSTS: &str =
    "no agent host is connected to this controller (K2_ROLE=controller runs no agents locally; start a K2_ROLE=host process)";

/// One agent placed on a remote host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemotePlacement {
    pub agent_id: String,
    pub host_id: String,
    /// The host's session id, once its spawn reply named one.
    pub session_id: Option<String>,
    pub epoch: PlacementEpoch,
    /// Hook frames accepted for this placement.
    pub hooks: u64,
}

/// A hook event a host attested, as the controller received it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostHook {
    pub host_id: String,
    pub agent_id: String,
    pub session_id: String,
    pub event: String,
    pub at_ms: i64,
}

/// What a relayed grid stream delivers to the controller-side relay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GridEvent {
    /// One grid protocol text message from the host.
    Data(String),
    /// The host ended the stream (or the link dropped).
    Closed(String),
}

/// A grid stream opened on a host ([`RemoteHost::open_grid`]). Dropping
/// it closes the stream on the host.
pub struct RemoteGrid {
    pub stream: u64,
    pub host_id: String,
    pub events: mpsc::UnboundedReceiver<GridEvent>,
    tx: mpsc::UnboundedSender<Frame>,
    closed: bool,
}

impl RemoteGrid {
    /// Send one client text message to the host's grid handler.
    pub fn send(&self, text: String) -> bool {
        self.tx.send(Frame::GridData { stream: self.stream, text }).is_ok()
    }

    /// Close the stream on the host (idempotent).
    pub fn close(&mut self, reason: &str) {
        if !self.closed {
            self.closed = true;
            let _ = self.tx.send(Frame::GridClose { stream: self.stream, reason: reason.to_string() });
        }
    }
}

impl Drop for RemoteGrid {
    fn drop(&mut self) {
        self.close("client gone");
    }
}

struct Link {
    conn: u64,
    host_name: String,
    tx: mpsc::UnboundedSender<Frame>,
    pending: HashMap<u64, oneshot::Sender<(String, String)>>,
    streams: HashMap<u64, mpsc::UnboundedSender<GridEvent>>,
}

#[derive(Default)]
struct Inner {
    /// Keyed by host id (protocol 1: the host's name).
    links: HashMap<String, Link>,
    /// Keyed by agent id.
    placements: HashMap<String, RemotePlacement>,
}

/// An HTTP-shaped answer for the controller's routes: status line and
/// JSON body, as `HandlerResult` has them.
pub type Answer = (String, String);

fn answer(status: &str, body: Value) -> Answer {
    (status.to_string(), body.to_string())
}

/// The controller's remote hosts and placements.
pub struct RemoteHost {
    inner: Mutex<Inner>,
    next_id: AtomicU64,
    next_epoch: AtomicU64,
    hooks: broadcast::Sender<HostHook>,
}

impl Default for RemoteHost {
    fn default() -> Self {
        Self::new()
    }
}

impl RemoteHost {
    pub fn new() -> Self {
        // Epochs only grow, also across controller restarts (I1): start
        // from the clock, then count.
        let start = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(1);
        Self {
            inner: Mutex::new(Inner::default()),
            next_id: AtomicU64::new(1),
            next_epoch: AtomicU64::new(start),
            hooks: broadcast::channel(1024).0,
        }
    }

    fn id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    // ── links ──────────────────────────────────────────────────────────

    /// Register a host that completed the handshake. Returns the link's
    /// connection id, the host id and the frames to write to it. A link
    /// already open under the same name is told `duplicate` and dropped.
    pub fn connect(&self, host_name: &str) -> (u64, String, mpsc::UnboundedReceiver<Frame>) {
        let conn = self.id();
        let host_id = host_name.to_string();
        let (tx, rx) = mpsc::unbounded_channel();
        let old = {
            let mut inner = self.inner.lock();
            inner.links.insert(
                host_id.clone(),
                Link { conn, host_name: host_name.to_string(), tx, pending: HashMap::new(), streams: HashMap::new() },
            )
        };
        if let Some(old) = old {
            let _ = old.tx.send(Frame::Refused {
                code: "duplicate".into(),
                message: format!("a newer link for host {host_id} replaced this one"),
            });
            Self::fail_link(old);
        }
        log_debug!("[hosts] host {host_id} linked (conn {conn})");
        k2_core::trace::emit(|| k2_core::trace::Event::new("ctl.host_linked").attr("host", host_id.as_str()));
        (conn, host_id, rx)
    }

    /// The link `conn` closed: fail what waits on it and forget its
    /// placements (the agents may still run on the host; re-adopting them
    /// is S9). A stale `conn` (already replaced) changes nothing.
    pub fn disconnect(&self, host_id: &str, conn: u64) {
        let link = {
            let mut inner = self.inner.lock();
            if inner.links.get(host_id).map(|l| l.conn) != Some(conn) {
                return;
            }
            inner.placements.retain(|_, p| p.host_id != host_id);
            inner.links.remove(host_id)
        };
        if let Some(link) = link {
            Self::fail_link(link);
        }
        log_debug!("[hosts] host {host_id} unlinked (conn {conn})");
        k2_core::trace::emit(|| k2_core::trace::Event::new("ctl.host_unlinked").attr("host", host_id));
    }

    fn fail_link(link: Link) {
        // Dropping the reply senders fails every waiting request.
        drop(link.pending);
        for (_, s) in link.streams {
            let _ = s.send(GridEvent::Closed("host link closed".into()));
        }
    }

    /// One frame from host `host_id` on link `conn`.
    pub fn on_frame(&self, host_id: &str, conn: u64, frame: Frame) {
        let mut inner = self.inner.lock();
        if inner.links.get(host_id).map(|l| l.conn) != Some(conn) {
            return;
        }
        match frame {
            Frame::Reply { req, status, body } => {
                if let Some(waiter) = inner.links.get_mut(host_id).and_then(|l| l.pending.remove(&req)) {
                    let _ = waiter.send((status, body));
                }
            }
            Frame::GridData { stream, text } => {
                if let Some(s) = inner.links.get(host_id).and_then(|l| l.streams.get(&stream)) {
                    let _ = s.send(GridEvent::Data(text));
                }
            }
            Frame::GridClose { stream, reason } => {
                if let Some(s) = inner.links.get_mut(host_id).and_then(|l| l.streams.remove(&stream)) {
                    let _ = s.send(GridEvent::Closed(reason));
                }
            }
            Frame::Hook { pane, event, at_ms, epoch, .. } => {
                // Only a session this controller placed on this host, at
                // the placement's epoch when the host sent one.
                let hit = inner.placements.values_mut().find(|p| {
                    p.host_id == host_id && p.session_id.as_deref() == Some(pane.as_str())
                });
                match hit {
                    Some(p) if epoch.is_none_or(|e| e == p.epoch.0) => {
                        p.hooks += 1;
                        let hook = HostHook {
                            host_id: host_id.to_string(),
                            agent_id: p.agent_id.clone(),
                            session_id: pane.clone(),
                            event: event.clone(),
                            at_ms,
                        };
                        log_debug!("[hosts] hook {event} from host {host_id} agent {} session {pane}", p.agent_id);
                        k2_core::trace::emit(|| {
                            k2_core::trace::Event::new("ctl.hook").pane(&pane).attr("host", host_id).attr("event", event.as_str())
                        });
                        let _ = self.hooks.send(hook);
                    }
                    _ => log_debug!("[hosts] dropped hook {event} from host {host_id}: session {pane} is not placed there at that epoch"),
                }
            }
            Frame::OpenUrl { url, source } => {
                drop(inner);
                log_debug!("[hosts] open_url from host {host_id} ({source})");
                k2_core::trace::emit(|| k2_core::trace::Event::new("ctl.open_url").attr("host", host_id));
                let _ = crate::session_events::emit(crate::session_events::SessionEvent::OpenUrl { url, source });
            }
            Frame::SessionGone { agent_id, epoch } => {
                let stale = match inner.placements.get(&agent_id) {
                    Some(p) => p.host_id != host_id || epoch.is_some_and(|e| e != p.epoch.0),
                    None => true,
                };
                if !stale {
                    inner.placements.remove(&agent_id);
                    log_debug!("[hosts] agent {agent_id} gone on host {host_id}");
                }
            }
            other => log_debug!("[hosts] ignored frame {} from host {host_id}", other.kind()),
        }
    }

    // ── requests ───────────────────────────────────────────────────────

    /// Send the request `make(req)` to `host_id` and wait for its reply.
    async fn request(&self, host_id: &str, make: impl FnOnce(u64) -> Frame) -> Result<Answer, Answer> {
        let req = self.id();
        let (tx, rx) = oneshot::channel();
        {
            let mut inner = self.inner.lock();
            let Some(link) = inner.links.get_mut(host_id) else {
                return Err(answer("502 Bad Gateway", json!({"error": format!("agent host {host_id} is not connected")})));
            };
            link.pending.insert(req, tx);
            if link.tx.send(make(req)).is_err() {
                link.pending.remove(&req);
                return Err(answer("502 Bad Gateway", json!({"error": format!("agent host {host_id} link is closing")})));
            }
        }
        match tokio::time::timeout(REPLY_DEADLINE, rx).await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(_)) => Err(answer(
                "502 Bad Gateway",
                json!({"error": format!("agent host {host_id} disconnected before it answered")}),
            )),
            Err(_) => {
                if let Some(link) = self.inner.lock().links.get_mut(host_id) {
                    link.pending.remove(&req);
                }
                Err(answer("504 Gateway Timeout", json!({"error": format!("agent host {host_id} did not answer")})))
            }
        }
    }

    /// The host for a new placement: the connected host with the fewest
    /// placements (ties by name).
    fn pick_host(inner: &Inner) -> Option<String> {
        let mut counts: Vec<(usize, &String)> = inner
            .links
            .keys()
            .map(|h| (inner.placements.values().filter(|p| &p.host_id == h).count(), h))
            .collect();
        counts.sort();
        counts.first().map(|(_, h)| (*h).clone())
    }

    /// `POST /cli/sessions/v2/spawn` on a controller. Find-or-spawn by
    /// `agent_name` as today: an agent already placed goes back to its
    /// host (which answers `reused: true`); a new one goes to
    /// [`Self::pick_host`] at a new epoch.
    pub async fn spawn(&self, body: &[u8]) -> Answer {
        let parsed: Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => {
                return answer(
                    "400 Bad Request",
                    json!({"error": format!("parse v2 SpawnRequest: {}", e.to_string().replace('"', "'"))}),
                )
            }
        };
        let agent_id = parsed.get("agent_name").and_then(Value::as_str).unwrap_or("").to_string();
        if agent_id.is_empty() {
            return answer("400 Bad Request", json!({"error": "agent_name required"}));
        }
        let (host_id, epoch) = {
            let mut inner = self.inner.lock();
            match inner.placements.get(&agent_id) {
                Some(p) => (p.host_id.clone(), p.epoch),
                None => {
                    let Some(host_id) = Self::pick_host(&inner) else {
                        log_debug!("[hosts] spawn {agent_id} refused: no host connected");
                        return answer("503 Service Unavailable", json!({"error": NO_HOSTS}));
                    };
                    let epoch = PlacementEpoch(self.next_epoch.fetch_add(1, Ordering::Relaxed));
                    // Reserve the placement now so a concurrent spawn of the
                    // same agent lands on the same host.
                    inner.placements.insert(
                        agent_id.clone(),
                        RemotePlacement { agent_id: agent_id.clone(), host_id: host_id.clone(), session_id: None, epoch, hooks: 0 },
                    );
                    (host_id, epoch)
                }
            }
        };
        k2_core::trace::emit(|| {
            k2_core::trace::Event::new("ctl.place").agent(agent_id.as_str()).attr("host", host_id.as_str()).attr("epoch", epoch.0)
        });
        let result = self
            .request(&host_id, |req| Frame::Spawn { req, agent_id: agent_id.clone(), epoch: epoch.0, body: parsed })
            .await;
        let mut inner = self.inner.lock();
        let ours = |p: &RemotePlacement| p.host_id == host_id && p.epoch == epoch;
        match result {
            Ok((status, body)) if status.starts_with('2') => {
                let session = serde_json::from_str::<Value>(&body)
                    .ok()
                    .and_then(|v| v.get("sessionId").and_then(Value::as_str).map(str::to_string));
                if let Some(p) = inner.placements.get_mut(&agent_id).filter(|p| ours(p)) {
                    p.session_id = session.clone();
                }
                log_debug!(
                    "[hosts] placed agent {agent_id} on host {host_id} epoch {} session {}",
                    epoch.0,
                    session.as_deref().unwrap_or("?")
                );
                (status, body)
            }
            Ok((status, body)) => {
                if inner.placements.get(&agent_id).is_some_and(|p| ours(p) && p.session_id.is_none()) {
                    inner.placements.remove(&agent_id);
                }
                (status, body)
            }
            Err(e) => {
                if inner.placements.get(&agent_id).is_some_and(|p| ours(p) && p.session_id.is_none()) {
                    inner.placements.remove(&agent_id);
                }
                e
            }
        }
    }

    /// `POST /cli/sessions/v2/close` on a controller: forwarded to the
    /// agent's host. An agent placed nowhere answers as today's close of
    /// an unknown agent does.
    pub async fn close(&self, body: &[u8]) -> Answer {
        let parsed: Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => {
                return answer(
                    "400 Bad Request",
                    json!({"error": format!("parse v2 CloseRequest: {}", e.to_string().replace('"', "'"))}),
                )
            }
        };
        let Some(agent_id) = parsed.get("agent_name").and_then(Value::as_str).map(str::to_string) else {
            return answer("400 Bad Request", json!({"error": "parse v2 CloseRequest: missing field `agent_name`"}));
        };
        let placed = self.inner.lock().placements.get(&agent_id).map(|p| (p.host_id.clone(), p.epoch));
        let Some((host_id, epoch)) = placed else {
            let cleared = parsed.get("clear_index").and_then(Value::as_bool).unwrap_or(false);
            return answer("200 OK", json!({"closed": false, "indexCleared": cleared}));
        };
        let result = self
            .request(&host_id, |req| Frame::Close { req, agent_id: agent_id.clone(), epoch: epoch.0, body: parsed })
            .await;
        match result {
            Ok((status, body)) => {
                let closed = serde_json::from_str::<Value>(&body)
                    .ok()
                    .and_then(|v| v.get("closed").and_then(Value::as_bool))
                    .unwrap_or(false);
                if closed {
                    let mut inner = self.inner.lock();
                    if inner.placements.get(&agent_id).is_some_and(|p| p.epoch == epoch) {
                        inner.placements.remove(&agent_id);
                    }
                }
                (status, body)
            }
            Err(e) => e,
        }
    }

    // ── grid ───────────────────────────────────────────────────────────

    /// Open a relayed grid stream on the host that runs `session_id`.
    pub fn open_grid(&self, session_id: &str, proto: Option<String>, as_user: Option<String>) -> Result<RemoteGrid, String> {
        let stream = self.id();
        let mut inner = self.inner.lock();
        let Some(p) = inner.placements.values().find(|p| p.session_id.as_deref() == Some(session_id)).cloned() else {
            return Err(format!("session {session_id} is not placed on any agent host"));
        };
        let Some(link) = inner.links.get_mut(&p.host_id) else {
            return Err(format!("agent host {} is not connected", p.host_id));
        };
        let (etx, erx) = mpsc::unbounded_channel();
        link.streams.insert(stream, etx);
        let tx = link.tx.clone();
        if tx.send(Frame::GridOpen { stream, session_id: session_id.to_string(), epoch: p.epoch.0, proto, as_user }).is_err() {
            link.streams.remove(&stream);
            return Err(format!("agent host {} link is closing", p.host_id));
        }
        Ok(RemoteGrid { stream, host_id: p.host_id, events: erx, tx, closed: false })
    }

    // ── reads ──────────────────────────────────────────────────────────

    pub fn placement(&self, agent_id: &str) -> Option<RemotePlacement> {
        self.inner.lock().placements.get(agent_id).cloned()
    }

    pub fn placements(&self) -> Vec<RemotePlacement> {
        self.inner.lock().placements.values().cloned().collect()
    }

    pub fn hook_events(&self) -> broadcast::Receiver<HostHook> {
        self.hooks.subscribe()
    }

    /// `GET /cli/hosts/status` on a controller (no secrets).
    pub fn status_json(&self) -> Value {
        let inner = self.inner.lock();
        let mut hosts: Vec<Value> = inner
            .links
            .iter()
            .map(|(id, l)| {
                let placed: Vec<&RemotePlacement> = inner.placements.values().filter(|p| &p.host_id == id).collect();
                json!({
                    "host_id": id,
                    "host_name": l.host_name,
                    "placements": placed.len(),
                    "hooks": placed.iter().map(|p| p.hooks).sum::<u64>(),
                })
            })
            .collect();
        hosts.sort_by(|a, b| a["host_id"].as_str().cmp(&b["host_id"].as_str()));
        json!({"role": "controller", "hosts": hosts})
    }
}

/// The controller's one [`RemoteHost`].
pub fn global() -> &'static RemoteHost {
    static REMOTE: OnceLock<RemoteHost> = OnceLock::new();
    REMOTE.get_or_init(RemoteHost::new)
}
