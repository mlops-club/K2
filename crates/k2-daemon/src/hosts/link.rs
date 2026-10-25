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

const WELCOME_DEADLINE: Duration = Duration::from_secs(10);
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

#[derive(Default)]
struct LinkState {
    linked: bool,
    host_id: Option<String>,
    host_name: String,
    controller: String,
    last_error: Option<String>,
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
    })
}

/// The link, forever: connect, serve, and on any drop reconnect with
/// backoff (1 s doubling to 30 s). Agents keep running while it is down.
pub async fn run(cfg: HostConfig) {
    {
        let mut s = state().lock();
        s.host_name = cfg.host_name.clone();
        s.controller = cfg.attach_url.clone();
    }
    let mut backoff = BACKOFF_MIN;
    loop {
        let result = serve_once(&cfg).await;
        let was_linked = {
            let mut s = state().lock();
            let was = s.linked;
            s.linked = false;
            s.last_error = result.as_ref().err().cloned();
            was
        };
        match &result {
            Ok(()) => log_debug!("[hosts/link] link to {} closed", cfg.attach_url),
            Err(e) => log_debug!("[hosts/link] link to {}: {e}", cfg.attach_url),
        }
        if was_linked {
            backoff = BACKOFF_MIN;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

/// What this host placed for the controller: agent → epoch, and the
/// session ids its spawns returned → agent.
#[derive(Default)]
struct Placed {
    epochs: HashMap<String, u64>,
    sessions: HashMap<String, String>,
}

/// One connection: handshake, then serve until it closes.
pub(crate) async fn serve_once(cfg: &HostConfig) -> Result<(), String> {
    // Subscribe before the handshake, so nothing between welcome and the
    // loop is missed.
    let mut hooks = crate::hook_ingest::subscribe();
    let mut events = crate::session_events::subscribe();

    let (ws, _) = tokio_tungstenite::connect_async(cfg.attach_url.as_str())
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let (mut write, mut read) = ws.split();
    let hello = Frame::Hello {
        protocol: PROTOCOL,
        host_name: cfg.host_name.clone(),
        token: cfg.token.clone(),
        labels: cfg.labels.clone(),
    };
    write.send(Message::Text(hello.encode())).await.map_err(|e| format!("send hello: {e}"))?;
    let host_id = match tokio::time::timeout(WELCOME_DEADLINE, read.next()).await {
        Ok(Some(Ok(Message::Text(t)))) => match Frame::decode(&t)? {
            Frame::Welcome { host_id, protocol, controller } => {
                log_debug!("[hosts/link] linked as {host_id} to {controller} (protocol {protocol})");
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
        s.host_id = Some(host_id.clone());
        s.last_error = None;
    }
    k2_core::trace::emit(|| k2_core::trace::Event::new("host.linked").attr("host", host_id.as_str()));

    let (tx, mut out) = mpsc::unbounded_channel::<Frame>();
    let streams: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<String>>>> = Arc::default();
    let placed: Arc<Mutex<Placed>> = Arc::default();

    let result = loop {
        tokio::select! {
            f = out.recv() => {
                // `tx` lives in this loop, so the channel never closes here.
                let Some(f) = f else { break Ok(()) };
                if let Err(e) = write.send(Message::Text(f.encode())).await {
                    break Err(format!("send: {e}"));
                }
            }
            m = read.next() => match m {
                Some(Ok(Message::Text(t))) => match Frame::decode(&t) {
                    Ok(f) => {
                        if let Err(e) = handle(f, &tx, &streams, &placed) {
                            break Err(e);
                        }
                    }
                    Err(e) => log_debug!("[hosts/link] {e}"),
                },
                Some(Ok(Message::Close(_))) | None => break Ok(()),
                Some(Err(e)) => break Err(format!("read: {e}")),
                Some(Ok(_)) => {}
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
                        p.sessions.retain(|_, a| a != &agent_name);
                        let _ = tx.send(Frame::SessionGone { agent_id: agent_name, epoch: Some(epoch) });
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
    let _ = write.close().await;
    result
}

/// One frame from the controller. `Err` ends the connection.
fn handle(
    f: Frame,
    tx: &mpsc::UnboundedSender<Frame>,
    streams: &Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<String>>>>,
    placed: &Arc<Mutex<Placed>>,
) -> Result<(), String> {
    match f {
        Frame::Spawn { req, agent_id, epoch, body } => {
            placed.lock().epochs.insert(agent_id.clone(), epoch);
            let tx = tx.clone();
            let placed = Arc::clone(placed);
            tokio::spawn(async move {
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
                    placed.lock().sessions.insert(session, agent_id);
                }
                let _ = tx.send(Frame::Reply { req, status, body });
            });
        }
        Frame::Close { req, agent_id, body, .. } => {
            let tx = tx.clone();
            tokio::spawn(async move {
                let bytes = serde_json::to_vec(&body).unwrap_or_default();
                let r = tokio::task::spawn_blocking(move || crate::v2_spawn::handle_v2_close(&bytes)).await;
                let (status, body) = match r {
                    Ok(r) => (r.status.to_string(), r.body),
                    Err(e) => ("500 Internal Server Error".to_string(), serde_json::json!({"error": format!("close: {e}")}).to_string()),
                };
                log_debug!("[hosts/link] close agent {agent_id}: {status}");
                let _ = tx.send(Frame::Reply { req, status, body });
            });
        }
        Frame::GridOpen { stream, session_id, .. } => {
            let (ctx, crx) = mpsc::unbounded_channel();
            streams.lock().insert(stream, ctx);
            let tx = tx.clone();
            let streams = Arc::clone(streams);
            tokio::spawn(async move {
                let reason = match local_grid(&session_id, crx, |text| {
                    let _ = tx.send(Frame::GridData { stream, text });
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
        Frame::GridData { stream, text } => {
            if let Some(s) = streams.lock().get(&stream) {
                let _ = s.send(text);
            }
        }
        Frame::GridClose { stream, .. } => {
            // Dropping the sender ends the local stream.
            streams.lock().remove(&stream);
        }
        Frame::Refused { code, message } => return Err(format!("refused: {code}: {message}")),
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
