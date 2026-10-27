//! Host side of the host link (`K2_ROLE=host`): dial the controller,
//! serve its requests with this daemon's own in-process host, and forward
//! hook events and `open_url` to it.
//!
//! - `spawn` / `close` run today's `v2_spawn` handlers, so the agent is a
//!   child of this process with its hook socket, env and transcript here.
//! - `grid.open` runs today's `/cli/sessions/grid` handler on a private
//!   loopback socket pair and relays its text messages, so frames are
//!   encoded once, here, next to the Term.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k2_core::log_debug;
use parking_lot::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use super::proto::{Frame, PROTOCOL};
use super::HostConfig;
use crate::session_events::SessionEvent;

/// Every wait on the link has a deadline, so a dead or silent controller
/// always ends in a logged failure and a retry, never a silent stall.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Timings {
    /// TCP connect plus the WebSocket upgrade. A SYN to a vanished pod IP
    /// would otherwise wait out the kernel's SYN retries (~127 s on Linux),
    /// and a peer that accepts but never upgrades, forever.
    pub connect: Duration,
    /// `hello` → `welcome`.
    pub welcome: Duration,
    /// Nothing at all from the controller (it pings every 15 s,
    /// `hub::PING_EVERY`) for this long: the link is dead.
    pub idle: Duration,
    /// Sending our close on a link that already failed.
    pub close: Duration,
    pub backoff_min: Duration,
    pub backoff_max: Duration,
}

impl Timings {
    pub const PRODUCTION: Timings = Timings {
        connect: Duration::from_secs(10),
        welcome: Duration::from_secs(10),
        idle: Duration::from_secs(45),
        close: Duration::from_secs(2),
        backoff_min: Duration::from_secs(1),
        backoff_max: Duration::from_secs(30),
    };
}

/// The reconnect policy: after a link that was up, retry after
/// `backoff_min`; after a failed attempt, after `prev`. Returns (this
/// sleep, the next attempt's `prev`), doubling up to `backoff_max`.
pub(crate) fn next_backoff(prev: Duration, was_linked: bool, t: &Timings) -> (Duration, Duration) {
    let sleep = if was_linked { t.backoff_min } else { prev.clamp(t.backoff_min, t.backoff_max) };
    (sleep, (sleep * 2).min(t.backoff_max))
}

/// What the link loop did, for tests ([`run_with`]'s observer).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LinkEvent {
    Attempt(u64),
    Linked,
    Ended { was_linked: bool, error: Option<String> },
    Sleep(Duration),
}

#[derive(Default)]
struct LinkState {
    linked: bool,
    host_id: Option<String>,
    host_name: String,
    controller: String,
    last_error: Option<String>,
    attempts: u64,
    links: u64,
}

fn state() -> &'static Mutex<LinkState> {
    static S: std::sync::OnceLock<Mutex<LinkState>> = std::sync::OnceLock::new();
    S.get_or_init(|| Mutex::new(LinkState::default()))
}

/// `GET /cli/hosts/status` on a host.
pub fn status_json() -> serde_json::Value {
    let s = state().lock();
    serde_json::json!({
        "role": "host",
        "linked": s.linked,
        "host_id": s.host_id,
        "host_name": s.host_name,
        "controller": s.controller,
        "last_error": s.last_error,
        "attempts": s.attempts,
        "links": s.links,
    })
}

/// The link, forever: connect, serve, and on any end (connect error, read
/// error, close, silence) reconnect: 1 s after a link that was up, then
/// doubling to 30 s while attempts fail. Agents keep running while it is
/// down.
pub async fn run(cfg: HostConfig) {
    run_with(cfg, Timings::PRODUCTION, None).await
}

pub(crate) async fn run_with(cfg: HostConfig, t: Timings, observe: Option<mpsc::UnboundedSender<LinkEvent>>) {
    let note = |e: LinkEvent| {
        if let Some(o) = &observe {
            let _ = o.send(e);
        }
    };
    {
        let mut s = state().lock();
        s.host_name = cfg.host_name.clone();
        s.controller = cfg.attach_url.clone();
    }
    let mut backoff = t.backoff_min;
    let mut attempt = 0u64;
    loop {
        attempt += 1;
        state().lock().attempts += 1;
        log_debug!("[hosts/link] connecting to {} (attempt {attempt})", cfg.attach_url);
        note(LinkEvent::Attempt(attempt));
        // This attempt's own outcome, not the process-wide status.
        let mut was_linked = false;
        let result = serve_once(&cfg, &t, || {
            was_linked = true;
            note(LinkEvent::Linked);
        })
        .await;
        {
            let mut s = state().lock();
            s.linked = false;
            s.last_error = result.as_ref().err().cloned();
        }
        let (sleep, next) = next_backoff(backoff, was_linked, &t);
        match &result {
            Ok(()) => log_debug!("[hosts/link] link to {} closed; retrying in {sleep:?}", cfg.attach_url),
            Err(e) => log_debug!("[hosts/link] link to {}: {e}; retrying in {sleep:?}", cfg.attach_url),
        }
        note(LinkEvent::Ended { was_linked, error: result.err() });
        note(LinkEvent::Sleep(sleep));
        tokio::time::sleep(sleep).await;
        backoff = next;
    }
}

/// What this host placed for the controller: agent → epoch, and the
/// session ids its spawns returned → agent. Process-wide (W3), so it
/// survives link drops and becomes `resume` in the next `hello`.
#[derive(Default)]
struct Placed {
    epochs: HashMap<String, u64>,
    sessions: HashMap<String, String>,
    /// K2-f.3: agents whose CLI ended, with how their turn ended, until a
    /// `hello` has carried them: a `session_gone` lost with its link is
    /// told again in the next `hello.ended`.
    ended: Vec<super::proto::EndedPlacement>,
    /// ptyd S3: agents this daemon took over from a previous one at boot,
    /// resumed with `adopted: true` until a welcomed `hello` carried them,
    /// with the host id their meta recorded (S3.1 #3).
    adopted: HashMap<String, Option<String>>,
}

fn placed() -> Arc<Mutex<Placed>> {
    static P: std::sync::OnceLock<Arc<Mutex<Placed>>> = std::sync::OnceLock::new();
    Arc::clone(P.get_or_init(Arc::default))
}

/// The epoch this host runs `agent` at (the drain's home save).
pub(crate) fn placed_epoch(agent: &str) -> Option<u64> {
    placed().lock().epochs.get(agent).copied()
}

/// ptyd S3: `agent_id`'s session `session_id`, at `epoch`, was adopted
/// from ptyd at boot: placed again, as the previous daemon had it.
pub(crate) fn adopted(agent_id: &str, session_id: &str, epoch: u64, host_id: Option<String>) {
    let placed = placed();
    let mut p = placed.lock();
    p.epochs.insert(agent_id.to_string(), epoch);
    p.sessions.retain(|_, a| a != agent_id);
    p.sessions.insert(session_id.to_string(), agent_id.to_string());
    p.adopted.insert(agent_id.to_string(), host_id);
}

/// S3.1 #4: the adopted agents no welcomed `hello` has confirmed (their
/// sessions are ended by the caller). They stay placed, so the controller
/// is told they ended: `session_gone`, or the next hello's `ended`.
pub(crate) fn take_unconfirmed() -> Vec<String> {
    let placed = placed();
    let mut out: Vec<String> = placed.lock().adopted.drain().map(|(a, _)| a).collect();
    out.sort();
    out
}

/// S3.1 #3: a welcomed `hello` carried these adopted agents: confirmed.
/// Returns those whose meta was written for another host id than the one
/// the controller just gave this host (the volume moved under this name),
/// for the caller to end; they stay placed, so the controller hears their
/// `session_gone`.
pub(crate) fn confirm_adopted(carried: &[String], host_id: &str) -> Vec<String> {
    let placed = placed();
    let mut p = placed.lock();
    let mut wrong = Vec::new();
    for a in carried {
        if let Some(meta_host) = p.adopted.remove(a) {
            if meta_host.as_deref().is_some_and(|h| h != host_id) {
                wrong.push(a.clone());
            }
        }
    }
    wrong
}

/// This host's link identity for `adopt::mark_placed`: the controller's
/// attach URL, this host's name, and the host id of its last welcome.
pub(crate) fn identity() -> (String, String, Option<String>) {
    let s = state().lock();
    (s.controller.clone(), s.host_name.clone(), s.host_id.clone())
}

/// `hello.resume`: every placement this host still runs (S9), with its
/// turn (K2-f); and `hello.ended`: those whose CLI ended while the link was
/// down, reported once with how their turn ended, then dropped from
/// `placed`.
fn resume_list() -> (Vec<super::proto::ResumePlacement>, Vec<super::proto::EndedPlacement>) {
    let placed = placed();
    let mut p = placed.lock();
    let mut out = Vec::new();
    let mut dead = Vec::new();
    for (agent, epoch) in p.epochs.iter() {
        match crate::agent_host::host().find_agent(agent) {
            Some(s) if s.is_child_alive() => out.push(super::proto::ResumePlacement {
                agent_id: agent.clone(),
                session_id: s.session_id().to_string(),
                epoch: *epoch,
                // Grid streams replay from a snapshot (see `GridOpen`); no
                // byte ring on v2 sessions yet.
                ring: None,
                child_alive: true,
                cwd: s.cwd_path().map(|c| c.to_string_lossy().into_owned()),
                program: s.command(),
                args: s.args(),
                label: Some(s.label()),
                turn: super::drain::turn_counts(&s.session_id().to_string()),
                adopted: p.adopted.contains_key(agent),
            }),
            _ => dead.push((agent.clone(), *epoch)),
        }
    }
    for (agent, epoch) in dead {
        let pane = p.sessions.iter().find(|(_, a)| **a == agent).map(|(pane, _)| pane.clone());
        if let Some(pane) = pane {
            // `hooked: false`: it never sent a hook (nothing of its turn is known).
            let turn = super::drain::take_turn_counts(&pane);
            p.ended.push(ended_record(&agent, pane, epoch, turn));
        }
        p.epochs.remove(&agent);
        p.sessions.retain(|_, a| a != &agent);
    }
    let mut ended = p.ended.clone();
    ended.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));
    out.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));
    (out, ended)
}

/// K2-f.3: `agent`'s CLI ended, with its turn if it ever sent a hook. The
/// turn is always sent (K2-f.4: a K2-f.2 controller requires it); `hooked`
/// says whether it means anything.
fn ended_record(agent: &str, session_id: String, epoch: u64, turn: Option<super::proto::TurnCounts>) -> super::proto::EndedPlacement {
    super::proto::EndedPlacement {
        agent_id: agent.to_string(),
        session_id,
        epoch,
        hooked: turn.is_some(),
        turn: turn.unwrap_or_default(),
    }
}

/// K2-f.3: drop the ended records a welcomed `hello` carried (others, ended
/// since, wait for the next one).
fn forget_ended(sent: &[super::proto::EndedPlacement]) {
    placed().lock().ended.retain(|e| !sent.iter().any(|s| s.agent_id == e.agent_id && s.epoch == e.epoch));
}

/// The grid frame version a snapshot or delta carries (its offset in the
/// session's frame sequence), read without parsing the whole frame.
pub(crate) fn frame_version(text: &str) -> Option<u64> {
    let i = text.find(r#""version":"#)? + r#""version":"#.len();
    let digits: String = text[i..].chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// One connection: handshake, then serve until it closes, fails or goes
/// silent for `t.idle`. `linked` runs once the controller welcomed us.
pub(crate) async fn serve_once(cfg: &HostConfig, t: &Timings, linked: impl FnOnce()) -> Result<(), String> {
    // Subscribe before the handshake, so nothing between welcome and the
    // loop is missed.
    let mut hooks = crate::hook_ingest::subscribe();
    let mut events = crate::session_events::subscribe();

    let token = super::enroll::hello_token(cfg).await?;
    let (ws, _) = match tokio::time::timeout(t.connect, tokio_tungstenite::connect_async(cfg.attach_url.as_str())).await {
        Ok(r) => r.map_err(|e| format!("connect: {e}"))?,
        Err(_) => return Err(format!("connect: no answer within {:?}", t.connect)),
    };
    let (mut write, mut read) = ws.split();
    let (resume, ended) = resume_list();
    let hello = Frame::Hello {
        protocol: PROTOCOL,
        host_name: cfg.host_name.clone(),
        token,
        labels: cfg.labels.clone(),
        // W3: the placements this host still runs (S9).
        resume,
        ended,
    };
    write.send(Message::Text(hello.encode())).await.map_err(|e| format!("send hello: {e}"))?;
    let host_id = match tokio::time::timeout(t.welcome, read.next()).await {
        Ok(Some(Ok(Message::Text(t)))) => match Frame::decode(&t)? {
            Frame::Welcome { host_id, protocol, controller } => {
                log_debug!("[hosts/link] linked as {host_id} to {controller} (protocol {protocol})");
                // K2-f.3: the ended agents this hello carried are told.
                if let Frame::Hello { ended, resume, .. } = &hello {
                    forget_ended(ended);
                    // ptyd S3: the controller was told these were adopted.
                    let carried: Vec<String> = resume.iter().filter(|r| r.adopted).map(|r| r.agent_id.clone()).collect();
                    for a in confirm_adopted(&carried, &host_id) {
                        log_debug!("[hosts/link] agent {a}: adopted from meta written for another host id than {host_id}; ended");
                        tokio::task::spawn_blocking(move || super::adopt::end_agent(&a));
                    }
                }
                host_id
            }
            Frame::Refused { code, message } => return Err(format!("refused: {code}: {message}")),
            other => return Err(format!("expected welcome, got {}", other.kind())),
        },
        Ok(_) => return Err("link closed during the handshake".into()),
        Err(_) => return Err("no welcome from the controller".into()),
    };
    {
        let mut s = state().lock();
        s.linked = true;
        s.links += 1;
        s.host_id = Some(host_id.clone());
        s.last_error = None;
    }
    linked();
    k2_core::trace::emit(|| k2_core::trace::Event::new("host.linked").attr("host", host_id.as_str()));

    let (tx, mut out) = mpsc::unbounded_channel::<Frame>();
    crate::delivery_drain::host_linked(tx.clone());
    let streams: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<String>>>> = Arc::default();
    let placed = placed();
    if let Frame::Hello { resume, .. } = &hello {
        if !resume.is_empty() {
            log_debug!("[hosts/link] resumed {} placement(s) with the controller", resume.len());
            k2_core::trace::emit(|| {
                k2_core::trace::Event::new(super::proto::trace_events::HOST_RESUME).attr("placements", resume.len())
            });
        }
    }

    // Any message from the controller (its pings included) moves this on.
    let mut heard = tokio::time::Instant::now();
    let result = loop {
        tokio::select! {
            _ = tokio::time::sleep_until(heard + t.idle) => {
                break Err(format!("nothing from the controller for {:?}", t.idle));
            }
            f = out.recv() => {
                // `tx` lives in this loop, so the channel never closes here.
                let Some(f) = f else { break Ok(()) };
                if let Err(e) = write.send(Message::Text(f.encode())).await {
                    break Err(format!("send: {e}"));
                }
            }
            m = read.next() => match m {
                Some(Ok(Message::Text(text))) => match { heard = tokio::time::Instant::now(); Frame::decode(&text) } {
                    Ok(f) => {
                        if let Err(e) = handle(f, &tx, &streams, &placed) {
                            break Err(e);
                        }
                    }
                    Err(e) => log_debug!("[hosts/link] {e}"),
                },
                Some(Ok(Message::Close(_))) | None => break Ok(()),
                Some(Err(e)) => break Err(format!("read: {e}")),
                Some(Ok(_)) => heard = tokio::time::Instant::now(),
            },
            h = hooks.recv() => match h {
                Ok(crate::hook_ingest::IngestEvent::Envelope { envelope, received_at_ms, .. }) => {
                    let p = placed.lock();
                    let epoch = p.sessions.get(&envelope.pane).and_then(|a| p.epochs.get(a)).copied();
                    let _ = tx.send(Frame::Hook {
                        pane: envelope.pane.clone(),
                        event: envelope.event.clone(),
                        session_id: Some(envelope.pane.clone()),
                        conversation: envelope.session_id.clone(),
                        agent_pid: envelope.agent_pid,
                        at_ms: received_at_ms,
                        epoch,
                        subagent: super::drain::is_subagent(&envelope),
                        // K2-f: absolute, so a hook lost on the way (a
                        // lagging forwarder) is in the next one's counts.
                        turn: super::drain::turn_counts(&envelope.pane),
                    });
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    log_debug!("[hosts/link] hook forwarder lagged, {n} event(s) lost");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {}
            },
            e = events.recv() => match e {
                Ok(SessionEvent::OpenUrl { url, source }) => {
                    let _ = tx.send(Frame::OpenUrl { url, source });
                }
                Ok(SessionEvent::SessionRemoved { agent_name, .. }) => {
                    let mut p = placed.lock();
                    if let Some(epoch) = p.epochs.remove(&agent_name) {
                        // K2-f: how its turn ended, from this host's own
                        // hook stream (the forwarded hooks are another
                        // channel: its last Stop may not have gone yet).
                        let pane = p.sessions.iter().find(|(_, a)| **a == agent_name).map(|(pane, _)| pane.clone());
                        let turn = pane.as_deref().and_then(super::drain::take_turn_counts);
                        p.sessions.retain(|_, a| a != &agent_name);
                        // K2-f.3: kept until a hello carries it, in case this
                        // frame dies with the link before it is written.
                        if let Some(pane) = pane {
                            p.ended.push(ended_record(&agent_name, pane, epoch, turn.clone()));
                        }
                        let _ = tx.send(Frame::SessionGone { agent_id: agent_name, epoch: Some(epoch), exit_code: None, turn });
                    }
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    log_debug!("[hosts/link] session event forwarder lagged, {n} event(s) lost");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {}
            },
        }
    };
    // Ends every local grid stream (their senders drop).
    streams.lock().clear();
    let _ = tokio::time::timeout(t.close, write.close()).await;
    result
}

/// One frame from the controller. `Err` ends the connection.
fn handle(
    f: Frame,
    tx: &mpsc::UnboundedSender<Frame>,
    streams: &Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<String>>>>,
    placed: &Arc<Mutex<Placed>>,
) -> Result<(), String> {
    if let Some(reply) = super::drain::refuse(&f) {
        let _ = tx.send(reply);
        return Ok(());
    }
    match f {
        Frame::Spawn { req, agent_id, epoch, mut body } => {
            placed.lock().epochs.insert(agent_id.clone(), epoch);
            let tx = tx.clone();
            let placed = Arc::clone(placed);
            tokio::spawn(async move {
                if let Some(reply) = super::home::before_spawn(req, &agent_id, &mut body).await {
                    let _ = tx.send(reply);
                    return;
                }
                let bytes = serde_json::to_vec(&body).unwrap_or_default();
                let r = tokio::task::spawn_blocking(move || crate::v2_spawn::handle_v2_spawn(&bytes)).await;
                let (status, body) = match r {
                    Ok(r) => (r.status.to_string(), r.body),
                    Err(e) => ("500 Internal Server Error".to_string(), serde_json::json!({"error": format!("spawn: {e}")}).to_string()),
                };
                let session = serde_json::from_str::<serde_json::Value>(&body)
                    .ok()
                    .and_then(|v| v.get("sessionId").and_then(|s| s.as_str()).map(str::to_string));
                if let Some(session) = session {
                    log_debug!("[hosts/link] spawned agent {agent_id} session {session} epoch {epoch} (pid {})", std::process::id());
                    // ptyd S3: what a restarted daemon adopts it by.
                    if let Some(s) = k2_core::session::SessionId::parse(&session).and_then(|sid| crate::agent_host::host().find_session(&sid)) {
                        super::adopt::mark_placed(&s, &agent_id, epoch);
                    }
                    placed.lock().sessions.insert(session, agent_id);
                }
                let _ = tx.send(Frame::Reply { req, status, body });
            });
        }
        Frame::Close { req, agent_id, epoch, body } => {
            let tx = tx.clone();
            tokio::spawn(async move {
                let home = super::home::before_close(&agent_id, epoch).await;
                let bytes = serde_json::to_vec(&body).unwrap_or_default();
                let r = tokio::task::spawn_blocking(move || crate::v2_spawn::handle_v2_close(&bytes)).await;
                let (status, body) = match r {
                    Ok(r) => (r.status.to_string(), r.body),
                    Err(e) => ("500 Internal Server Error".to_string(), serde_json::json!({"error": format!("close: {e}")}).to_string()),
                };
                log_debug!("[hosts/link] close agent {agent_id}: {status}");
                let body = super::home::with_home(body, home);
                let _ = tx.send(Frame::Reply { req, status, body });
            });
        }
        Frame::GridOpen { stream, session_id, as_user, ring_offset, .. } => {
            log_debug!(
                "[hosts/link] grid stream {stream} session {session_id} for {}",
                as_user.as_deref().unwrap_or("the owner")
            );
            if let Some(from) = ring_offset {
                // Deltas aren't kept; the snapshot this stream starts with
                // holds the Term (screen + scrollback), i.e. every change
                // after `from`, so the replay is the snapshot.
                log_debug!("[hosts/link] grid stream {stream} replays from version {from} via its snapshot");
                k2_core::trace::emit(|| {
                    k2_core::trace::Event::new(super::proto::trace_events::GRID_REPLAY).pane(&session_id).attr("from", from)
                });
            }
            let (ctx, crx) = mpsc::unbounded_channel();
            streams.lock().insert(stream, ctx);
            let tx = tx.clone();
            let streams = Arc::clone(streams);
            tokio::spawn(async move {
                let reason = match local_grid(&session_id, crx, |text| {
                    let ring_offset = frame_version(&text);
                    let _ = tx.send(Frame::GridData { stream, text, ring_offset });
                })
                .await
                {
                    Ok(()) => "grid closed".to_string(),
                    Err(e) => {
                        log_debug!("[hosts/link] grid stream {stream} session {session_id}: {e}");
                        e
                    }
                };
                streams.lock().remove(&stream);
                let _ = tx.send(Frame::GridClose { stream, reason });
            });
        }
        Frame::GridData { stream, text, .. } => {
            if let Some(s) = streams.lock().get(&stream) {
                let _ = s.send(text);
            }
        }
        Frame::GridClose { stream, .. } | Frame::BytesClose { stream, .. } => {
            // Dropping the sender ends the local stream.
            streams.lock().remove(&stream);
        }
        Frame::BytesOpen { stream, session_id, from, .. } => {
            let (ctx, crx) = mpsc::unbounded_channel();
            streams.lock().insert(stream, ctx);
            let tx = tx.clone();
            let streams = Arc::clone(streams);
            tokio::spawn(async move {
                let reason = local_bytes(stream, &session_id, from, crx, &tx).await;
                log_debug!("[hosts/link] bytes stream {stream} session {session_id} ended: {reason}");
                streams.lock().remove(&stream);
                let _ = tx.send(Frame::BytesClose { stream, reason });
            });
        }
        Frame::SpawnWorkspace { req, agent_id, epoch, mut request } => {
            placed.lock().epochs.insert(agent_id.clone(), epoch);
            let tx = tx.clone();
            let placed = Arc::clone(placed);
            tokio::spawn(async move {
                if let Some(reply) = super::home::before_spawn(req, &agent_id, &mut request).await {
                    let _ = tx.send(reply);
                    return;
                }
                let r = tokio::task::spawn_blocking(move || spawn_workspace(request)).await;
                let (status, body) = match r {
                    Ok(Ok(spawned)) => {
                        log_debug!(
                            "[hosts/link] spawned agent {agent_id} session {} epoch {epoch} (workspace spawn, reused={}, pid {})",
                            spawned.session_id,
                            spawned.reused,
                            std::process::id()
                        );
                        if let Some(s) = k2_core::session::SessionId::parse(&spawned.session_id)
                            .and_then(|sid| crate::agent_host::host().find_session(&sid))
                        {
                            super::adopt::mark_placed(&s, &agent_id, epoch);
                        }
                        placed.lock().sessions.insert(spawned.session_id.clone(), agent_id);
                        ("200 OK".to_string(), serde_json::to_string(&spawned).unwrap_or_default())
                    }
                    Ok(Err(e)) => ("500 Internal Server Error".to_string(), serde_json::json!({"error": e}).to_string()),
                    Err(e) => ("500 Internal Server Error".to_string(), serde_json::json!({"error": format!("spawn: {e}")}).to_string()),
                };
                let _ = tx.send(Frame::Reply { req, status, body });
            });
        }
        Frame::Op { req, session_id, epoch, op } => {
            let current = {
                let p = placed.lock();
                p.sessions.get(&session_id).and_then(|a| p.epochs.get(a)).copied()
            };
            let session = k2_core::session::SessionId::parse(&session_id)
                .and_then(|sid| crate::agent_host::host().find_session(&sid));
            let found = match (session, current) {
                (Some(s), Some(e)) if e == epoch => Ok(s),
                (Some(_), Some(e)) => Err(format!("stale epoch {epoch} for session {session_id} (now {e})")),
                // Spawned before this link (not in `placed`): accept by id.
                (Some(s), None) => Ok(s),
                (None, _) => Err(format!("session {session_id} is not on this host")),
            };
            match (found, req) {
                (Err(e), Some(req)) => {
                    let _ = tx.send(Frame::OpResult { req, value: serde_json::Value::Null, error: Some(e) });
                }
                (Err(e), None) => log_debug!("[hosts/link] op on {session_id} dropped: {e}"),
                // Writes and resizes run here, in the order they arrived.
                (Ok(s), None) => {
                    if let Err(e) = run_op(&s, op) {
                        log_debug!("[hosts/link] op on {session_id}: {e}");
                    }
                }
                (Ok(s), Some(req)) => {
                    let tx = tx.clone();
                    tokio::task::spawn_blocking(move || {
                        let f = match run_op(&s, op) {
                            Ok(value) => Frame::OpResult { req, value, error: None },
                            Err(e) => Frame::OpResult { req, value: serde_json::Value::Null, error: Some(e) },
                        };
                        let _ = tx.send(f);
                    });
                }
            }
        }
        Frame::Inject { req, agent_id, epoch, steps, msg_id } => {
            let current = placed.lock().epochs.get(&agent_id).copied();
            let tx = tx.clone();
            tokio::task::spawn_blocking(move || {
                use super::proto::InjectOutcome;
                let session = crate::agent_host::host().find_agent(&agent_id);
                let result = match (session, current) {
                    (Some(_), Some(e)) if e != epoch => InjectOutcome::NotPlaced,
                    (None, _) => InjectOutcome::NotPlaced,
                    (Some(s), _) => {
                        // K2-f.3: the message it types, for the prompt that
                        // submits it.
                        if let Some(id) = &msg_id {
                            let text: String =
                                steps.iter().filter_map(|st| if let super::proto::InjectStep::Write { text } = st { Some(text.as_str()) } else { None }).collect();
                            super::drain::note_typed(&s.session_id().to_string(), id, &text);
                        }
                        let actions: Result<Vec<_>, String> = steps.iter().map(|st| st.to_action()).collect();
                        match actions {
                            Ok(actions) => match crate::agent_host::host().inject(&s, actions) {
                                crate::agent_host::InjectResult::Delivered => InjectOutcome::Delivered,
                                crate::agent_host::InjectResult::ChildGone => InjectOutcome::ChildGone,
                            },
                            Err(e) => {
                                log_debug!("[hosts/link] inject into {agent_id}: {e}");
                                InjectOutcome::ChildGone
                            }
                        }
                    }
                };
                let _ = tx.send(Frame::InjectResult { req, result });
            });
        }
        Frame::Release { req, agent_id, epoch, reason } => {
            // The controller holds this agent elsewhere or at a newer epoch
            // (I1): stop this copy, keep its home.
            let held = placed.lock().epochs.get(&agent_id).copied();
            let tx = tx.clone();
            let placed = Arc::clone(placed);
            tokio::task::spawn_blocking(move || {
                if held.is_some_and(|e| e <= epoch) || held.is_none() {
                    log_debug!("[hosts/link] releasing agent {agent_id} at epoch {epoch}: {reason}");
                    crate::agent_host::host().unplace(&agent_id);
                    let mut p = placed.lock();
                    p.epochs.remove(&agent_id);
                    p.sessions.retain(|_, a| a != &agent_id);
                    k2_core::trace::emit(|| {
                        k2_core::trace::Event::new(super::proto::trace_events::HOST_RELEASE)
                            .agent(agent_id.as_str())
                            .attr("epoch", epoch)
                    });
                } else {
                    log_debug!("[hosts/link] release of {agent_id} at epoch {epoch} ignored: holding newer {held:?}");
                }
                let _ = tx.send(Frame::Released { req, agent_id, epoch, ring_end: None });
            });
        }
        Frame::HttpRelay { req, method, path, query, body_b64 } => {
            let tx = tx.clone();
            tokio::spawn(async move {
                let _ = tx.send(super::fs_relay::serve(req, &method, &path, &query, &body_b64).await);
            });
        }
        Frame::TranscriptExists { req, provider, session_id, project_path } => {
            let tx = tx.clone();
            tokio::task::spawn_blocking(move || {
                // This host's own disk (no probe is installed on a host).
                let found = k2_core::sidecar::conversation_exists(&provider, &session_id, &project_path);
                let _ = tx.send(Frame::OpResult { req, value: serde_json::Value::Bool(found), error: None });
            });
        }
        Frame::Refused { code, message } => return Err(format!("refused: {code}: {message}")),
        f @ (Frame::Deliver { .. } | Frame::DeliverAck { .. }) => crate::delivery_drain::on_controller_frame(f, tx),
        other => log_debug!("[hosts/link] ignored frame {}", other.kind()),
    }
    Ok(())
}

/// Run this daemon's own `/cli/sessions/grid` handler for `session_id` on
/// a private loopback socket pair and pump it: its text messages go to
/// `emit`, `from_ctl` goes to it. Returns when either side closes.
///
/// The pair is made by connecting first and accepting second, and the
/// accepted peer must be our own socket, so no other local process can
/// take the stream. The handler sees an owner connection (its token is a
/// fresh random one); the controller already authorised the client.
/// Bytes per `bytes.data` frame.
const BYTES_CHUNK: usize = 32 * 1024;
/// How often a caught-up byte stream looks for new output.
const BYTES_POLL: Duration = Duration::from_millis(50);

/// Serve one `bytes.open`: `bytes.ack`, then the ring from the ack's start
/// as `bytes.data`, then new output as it lands. Returns why it ended:
/// the controller closed it (`closed` disconnected), the link went, the
/// session's ring is gone, or the ring dropped bytes this stream hadn't
/// sent yet (an overrun: the stream ends rather than skip them, and the
/// client's reconnect sees the gap in its ack).
async fn local_bytes(
    stream: u64,
    session_id: &str,
    from: u64,
    mut closed: mpsc::UnboundedReceiver<String>,
    tx: &mpsc::UnboundedSender<Frame>,
) -> String {
    use base64::Engine;
    let read = |at: u64| {
        let sid = session_id.to_string();
        async move {
            tokio::task::spawn_blocking(move || super::byte_ring::read(&sid, at, BYTES_CHUNK))
                .await
                .unwrap_or_else(|e| Err(format!("ring read: {e}")))
        }
    };
    let mut r = match read(from).await {
        Ok(r) => r,
        Err(e) => {
            let _ = tx.send(Frame::BytesAck { stream, start: from, end: from, error: Some(e.clone()) });
            return e;
        }
    };
    k2_core::trace::emit(|| {
        k2_core::trace::Event::new(super::proto::trace_events::BYTES_REPLAY)
            .pane(session_id)
            .attr("stream", stream)
            .attr("from", from)
            .attr("start", r.start)
            .attr("end", r.end)
    });
    let _ = tx.send(Frame::BytesAck { stream, start: r.start, end: r.end, error: None });
    let mut next = r.start;
    loop {
        if r.start != next {
            return format!("ring overrun: bytes [{next}, {}) were dropped before they were sent", r.start);
        }
        let caught_up = r.bytes.is_empty() || next + r.bytes.len() as u64 >= r.end;
        if !r.bytes.is_empty() {
            let b64 = base64::engine::general_purpose::STANDARD.encode(&r.bytes);
            if tx.send(Frame::BytesData { stream, offset: next, b64 }).is_err() {
                return "host link closed".into();
            }
            next += r.bytes.len() as u64;
        }
        if caught_up {
            tokio::select! {
                _ = closed.recv() => return "closed by the controller".into(),
                _ = tokio::time::sleep(BYTES_POLL) => {}
            }
        } else if matches!(closed.try_recv(), Err(mpsc::error::TryRecvError::Disconnected)) {
            return "closed by the controller".into();
        }
        r = match read(next).await {
            Ok(r) => r,
            Err(e) => return e,
        };
    }
}

pub(crate) async fn local_grid(
    session_id: &str,
    mut from_ctl: mpsc::UnboundedReceiver<String>,
    emit: impl Fn(String),
) -> Result<(), String> {
    let listener = TcpListener::bind("127.0.0.1:0").await.map_err(|e| format!("bind: {e}"))?;
    let addr = listener.local_addr().map_err(|e| e.to_string())?;
    let client = TcpStream::connect(addr).await.map_err(|e| format!("connect: {e}"))?;
    let ours = client.local_addr().map_err(|e| e.to_string())?;
    let (server, peer) = listener.accept().await.map_err(|e| format!("accept: {e}"))?;
    if peer != ours {
        return Err(format!("loopback grid socket taken by {peer}"));
    }
    drop(listener);
    let token = uuid::Uuid::new_v4().to_string();
    let mut params = HashMap::new();
    params.insert("session".to_string(), session_id.to_string());
    params.insert("token".to_string(), token.clone());
    let handler = tokio::spawn(async move {
        let mut server = server;
        crate::sessions_grid_ws::serve_session_grid_connection(&mut server, params, token).await;
    });
    let url = format!("ws://{addr}/cli/sessions/grid?session={session_id}");
    let result = async {
        let (ws, _) = tokio_tungstenite::client_async(url, client).await.map_err(|e| format!("grid upgrade: {e}"))?;
        let (mut write, mut read) = ws.split();
        loop {
            tokio::select! {
                m = read.next() => match m {
                    Some(Ok(Message::Text(t))) => emit(t),
                    Some(Ok(Message::Close(_))) | None => return Ok(()),
                    Some(Err(e)) => return Err(format!("grid read: {e}")),
                    Some(Ok(_)) => {}
                },
                c = from_ctl.recv() => match c {
                    Some(text) => {
                        if write.send(Message::Text(text)).await.is_err() {
                            return Ok(());
                        }
                    }
                    None => {
                        let _ = write.close().await;
                        return Ok(());
                    }
                },
            }
        }
    }
    .await;
    // The handler returns once its socket closes; don't wait on it.
    handler.abort();
    result
}

/// `spawn.workspace` on this host: today's workspace spawn, then the
/// metadata the controller caches.
fn spawn_workspace(request: serde_json::Value) -> Result<super::proto::SpawnedSession, String> {
    // K2-c: the gone rule's direct path names the message it puts on argv.
    let delivery = request.get("deliveryId").and_then(|v| v.as_str()).map(str::to_string);
    let req: crate::spawn::SpawnWorkspaceSessionRequest =
        serde_json::from_value(request).map_err(|e| format!("spawn request: {e}"))?;
    let launch_prompt = req.launch_prompt.clone();
    let (out, already_delivered) = match delivery.clone() {
        Some(msg_id) => crate::delivery_drain::host_launch_once(&msg_id, |with_prompt| {
            let mut req = req.clone();
            if !with_prompt {
                req.launch_prompt = None;
            }
            let out = crate::spawn::spawn_agent_session_v2_blocking(req)?;
            let attached = out.launch_prompt_attached;
            Ok((out, attached))
        })?,
        None => (crate::spawn::spawn_agent_session_v2_blocking(req)?, false),
    };
    // K2-f.3: the message it put on argv, for the prompt that submits it.
    if let (Some(id), Some(text), true, false) = (&delivery, &launch_prompt, out.launch_prompt_attached, already_delivered) {
        super::drain::note_typed(&out.session_id.to_string(), id, text);
    }
    let s = crate::agent_host::host()
        .find_session(&out.session_id)
        .ok_or_else(|| format!("session {} vanished right after its spawn", out.session_id))?;
    Ok(super::proto::SpawnedSession {
        session_id: out.session_id.to_string(),
        reused: out.reused,
        pending_drained: out.pending_drained,
        launch_prompt_attached: out.launch_prompt_attached,
        cwd: s.cwd_path().map(|p| p.to_string_lossy().into_owned()),
        program: s.command(),
        args: s.args(),
        label: s.label(),
        label_source: crate::agent_host::remote::label_source_wire(s.label_source()).to_string(),
        already_delivered,
    })
}

/// One `op` against an in-process session; the value `op.result` carries.
fn run_op(s: &crate::agent_host::AgentSession, op: super::proto::SessionOp) -> Result<serde_json::Value, String> {
    use super::proto::SessionOp;
    use serde_json::json;
    let h = crate::agent_host::host();
    Ok(match op {
        SessionOp::Write { text, b64 } => {
            let bytes = match (text, b64) {
                (Some(t), _) => t.into_bytes(),
                (None, Some(b)) => {
                    use base64::Engine;
                    base64::engine::general_purpose::STANDARD.decode(b).map_err(|e| format!("write b64: {e}"))?
                }
                (None, None) => Vec::new(),
            };
            h.write(s, bytes).map_err(|e| e.to_string())?;
            serde_json::Value::Null
        }
        SessionOp::Resize { cols, rows } => {
            h.resize(s, cols, rows);
            serde_json::Value::Null
        }
        SessionOp::ResizeNow { cols, rows } => json!(h.resize_now(s, cols, rows)),
        SessionOp::Size => json!(h.size(s)),
        SessionOp::ClaimedSize => json!(h.claimed_size(s)),
        SessionOp::SetClaimedSize { cols, rows } => {
            h.set_claimed_size(s, cols, rows);
            serde_json::Value::Null
        }
        SessionOp::Pin { dims, set_by } => {
            h.pin(s, dims, set_by);
            serde_json::Value::Null
        }
        SessionOp::SetLabel { label, lock } => json!(h.set_label(s, label, lock)),
        SessionOp::Kill => {
            h.kill(s);
            serde_json::Value::Null
        }
        SessionOp::Screen { lines } => json!(h.read_screen(s, lines)),
        SessionOp::VisibleRows => json!(s.visible_text_rows()),
        SessionOp::Meta => serde_json::to_value(super::proto::SessionMeta {
            alive: s.is_child_alive(),
            label: s.label(),
            label_source: crate::agent_host::remote::label_source_wire(s.label_source()).to_string(),
            bracketed_paste: s.bracketed_paste_active(),
            subscribers: s.subscriber_count(),
            ever_attached: s.ever_attached(),
        })
        .map_err(|e| e.to_string())?,
    })
}

#[cfg(test)]
mod ended_tests {
    use super::*;

    /// S3.1 #3/#4: a welcomed hello confirms the adopted agents it carried;
    /// one whose meta names another host id is returned to be ended; the
    /// ones no hello confirmed are what the fence ends. All stay placed
    /// until their sessions end (the controller is told).
    #[test]
    fn adopted_agents_are_confirmed_by_a_welcome_or_fenced() {
        adopted("s31-ok", "pane-s31-ok", 4, Some("h-1".into()));
        adopted("s31-moved", "pane-s31-moved", 4, Some("h-old".into()));
        adopted("s31-s3meta", "pane-s31-s3meta", 4, None);
        adopted("s31-unseen", "pane-s31-unseen", 4, Some("h-1".into()));
        let carried = vec!["s31-ok".to_string(), "s31-moved".to_string(), "s31-s3meta".to_string()];
        assert_eq!(confirm_adopted(&carried, "h-1"), vec!["s31-moved".to_string()]);
        let unconfirmed: Vec<String> = take_unconfirmed().into_iter().filter(|a| a.starts_with("s31-")).collect();
        assert_eq!(unconfirmed, vec!["s31-unseen".to_string()]);
        assert!(take_unconfirmed().iter().all(|a| !a.starts_with("s31-")), "taken once");
        for a in ["s31-ok", "s31-moved", "s31-unseen"] {
            assert_eq!(placed_epoch(a), Some(4), "{a} stays placed until its session ends");
        }
    }

    /// K2-f.3 #5: an agent whose CLI ended is in every `hello.ended` until
    /// a welcomed hello carried it (its `session_gone` may have died with
    /// the link); then it is told, and drops out.
    #[test]
    fn an_ended_agent_is_in_each_hello_until_one_is_welcomed() {
        let mine = |id: &str| super::super::proto::EndedPlacement {
            agent_id: id.into(),
            session_id: format!("pane-{id}"),
            epoch: 9,
            turn: super::super::proto::TurnCounts { prompts: 1, stops: 0, running: true, msgs: vec!["m_x".into()] },
            hooked: true,
        };
        let a = mine("k2f3-ended-a");
        let b = mine("k2f3-ended-b");
        placed().lock().ended.push(a.clone());
        let has = |list: &[super::super::proto::EndedPlacement], e: &super::super::proto::EndedPlacement| list.iter().any(|x| x == e);
        let (_, first) = resume_list();
        assert!(has(&first, &a));
        // That hello wasn't welcomed (the link dropped): the next one has it.
        let (_, again) = resume_list();
        assert!(has(&again, &a));
        // b ends between that hello and its welcome: told on the next one.
        placed().lock().ended.push(b.clone());
        forget_ended(&again);
        let (_, next) = resume_list();
        assert!(!has(&next, &a) && has(&next, &b));
        forget_ended(&next);
        let (_, last) = resume_list();
        assert!(!has(&last, &a) && !has(&last, &b));
    }
}
