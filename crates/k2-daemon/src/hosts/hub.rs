//! Controller side of the host link: the `/cli/hosts/attach` socket and
//! the relays that serve a client's `/cli/sessions/grid` and
//! `/cli/sessions/bytes` from a host.
//!
//! Both are generic over the byte stream so tests drive them on loopback
//! sockets without the dispatcher.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k2_core::log_debug;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use super::proto::{negotiate, Frame, MIN_PROTOCOL, PROTOCOL};
use crate::agent_host::remote::{GridEvent, RemoteHost};

/// How long a host has to send `hello` after the upgrade.
const HELLO_DEADLINE: Duration = Duration::from_secs(10);
/// WebSocket ping cadence on an idle link.
const PING_EVERY: Duration = Duration::from_secs(15);

async fn refuse<S>(ws: &mut WebSocketStream<S>, code: &str, message: &str)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    log_debug!("[hosts] refused a host link: {code}: {message}");
    let f = Frame::Refused { code: code.into(), message: message.into() };
    let _ = ws.send(Message::Text(f.encode())).await;
    let _ = ws.close(None).await;
}

/// `GET /cli/hosts/attach`: handshake, then frames both ways until either
/// end closes. `token` is the controller's bootstrap token.
pub async fn serve_attach<S>(stream: S, remote: &'static RemoteHost, token: &str)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut ws = match tokio_tungstenite::accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            log_debug!("[hosts] attach handshake failed: {e}");
            return;
        }
    };
    let first = tokio::time::timeout(HELLO_DEADLINE, ws.next()).await;
    let hello = match first {
        Ok(Some(Ok(Message::Text(t)))) => Frame::decode(&t),
        _ => Err("expected hello".to_string()),
    };
    let (host_name, labels, protocol, resume) = match hello {
        Ok(Frame::Hello { protocol, host_name, token: presented, labels, resume }) => {
            let Some(protocol) = negotiate(protocol) else {
                return refuse(
                    &mut ws,
                    "protocol",
                    &format!("controller speaks protocols {MIN_PROTOCOL} to {PROTOCOL}, host sent {protocol}"),
                )
                .await;
            };
            if let Err(r) = super::enroll::admit_hello(remote, token, &presented, &host_name) {
                return refuse(&mut ws, r.code, &r.message).await;
            }
            if host_name.trim().is_empty() {
                return refuse(&mut ws, "protocol", "host_name is empty").await;
            }
            (host_name, labels, protocol, resume)
        }
        Ok(other) => return refuse(&mut ws, "protocol", &format!("expected hello, got {}", other.kind())).await,
        Err(e) => return refuse(&mut ws, "protocol", &e).await,
    };
    let (conn, host_id, mut out) = remote.connect(&host_name, protocol);
    log_debug!("[hosts] host {host_id} protocol {protocol} labels {labels:?}");
    remote.set_labels(&host_id, labels);
    if protocol >= 2 {
        // W3: reconcile what this host still runs with the table (S9).
        remote.adopt(&host_id, &resume);
    }
    let welcome = Frame::Welcome {
        protocol,
        host_id: host_id.clone(),
        controller: format!("k2-daemon {}", env!("CARGO_PKG_VERSION")),
    };
    if ws.send(Message::Text(welcome.encode())).await.is_err() {
        remote.disconnect(&host_id, conn);
        return;
    }
    let (mut write, mut read) = ws.split();
    let mut ping = tokio::time::interval(PING_EVERY);
    ping.tick().await;
    loop {
        tokio::select! {
            f = out.recv() => match f {
                Some(f) => {
                    let refused = matches!(f, Frame::Refused { .. });
                    if write.send(Message::Text(f.encode())).await.is_err() || refused {
                        break;
                    }
                }
                None => break,
            },
            m = read.next() => match m {
                Some(Ok(Message::Text(t))) => match Frame::decode(&t) {
                    Ok(f) => remote.on_frame(&host_id, conn, f),
                    Err(e) => log_debug!("[hosts] host {host_id}: {e}"),
                },
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                Some(Ok(_)) => {}
            },
            _ = ping.tick() => {
                if write.send(Message::Ping(Vec::new())).await.is_err() {
                    break;
                }
            }
        }
    }
    let _ = write.close().await;
    remote.disconnect(&host_id, conn);
}

/// The grid protocol's error event (`sessions_grid_ws::Outbound::Error`).
fn grid_error(message: &str) -> String {
    serde_json::json!({"event": "error", "payload": {"message": message}}).to_string()
}

/// Who a relayed grid client is, decided by the gate exactly as the
/// grid handler classifies a connection under `K2_ROLE=all`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GridClient {
    /// The daemon owner token.
    Owner,
    /// A Connect login (the desktop app signs in with `/cli/auth/login`).
    /// `token` is re-validated every 5 s against `owner_token`, as the
    /// grid handler does.
    ConnectUser { username: String, token: String, owner_token: String },
}

/// How often a Connect user's token is re-validated (the grid handler's
/// `auth_recheck`).
const REAUTH_EVERY: Duration = Duration::from_secs(5);

/// The set_mode the relay sends first for a Connect user, so the host's
/// handler starts the stream as a viewer like it does for one locally.
const START_AS_VIEWER: &str = r#"{"action":"set_mode","mode":"viewer"}"#;

/// The grid handler's capability rule for a Connect user: any account
/// that still exists (`sessions_grid_ws::connection_claimer_capable`).
fn user_capable(username: &str) -> bool {
    k2_core::connect_users::role_for_user(username).is_some()
}

/// A `mode` event's text with `capable` set from this controller's user
/// store, or `None` if `text` is not a `mode` event.
fn rewrite_mode(text: &str, capable: bool) -> Option<String> {
    if !text.starts_with(r#"{"event":"mode""#) {
        return None;
    }
    let mut v: serde_json::Value = serde_json::from_str(text).ok()?;
    v.get_mut("payload")?.as_object_mut()?.insert("capable".into(), capable.into());
    Some(v.to_string())
}

/// `GET /cli/sessions/grid` on a controller: upgrade, open the stream on
/// the session's host, then relay text messages both ways. Only the JSON
/// grid protocol (no `proto=k1` binary frames in protocol 1).
///
/// The host's handler sees an owner connection. For a Connect user the
/// relay keeps that user's rules from `K2_ROLE=all`: the stream starts as
/// a viewer (`set_mode viewer` goes first and the host's initial owner
/// `mode` frame is not passed on), `capable` in every `mode` event comes
/// from this controller's user store, the token is re-validated every 5 s,
/// and a removed user or revoked token ends the stream.
pub async fn serve_grid<S>(stream: S, remote: &RemoteHost, session_id: &str, client: GridClient)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let ws = match tokio_tungstenite::accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            log_debug!("[hosts] grid handshake failed: {e}");
            return;
        }
    };
    let (mut write, mut read) = ws.split();
    let as_user = match &client {
        GridClient::Owner => None,
        GridClient::ConnectUser { username, .. } => Some(username.clone()),
    };
    let mut grid = match remote.open_grid(session_id, None, as_user.clone()) {
        Ok(g) => g,
        Err(e) => {
            let _ = write.send(Message::Text(grid_error(&e))).await;
            let _ = write.close().await;
            return;
        }
    };
    k2_core::trace::attach(Some(session_id), "grid");
    let mut skip_initial_mode = false;
    if as_user.is_some() {
        grid.send(START_AS_VIEWER.to_string());
        skip_initial_mode = true;
    }
    let mut reauth = tokio::time::interval(REAUTH_EVERY);
    reauth.tick().await;
    loop {
        tokio::select! {
            ev = grid.events.recv() => match ev {
                Some(GridEvent::Data(mut text)) => {
                    if let Some(user) = &as_user {
                        if let Some(m) = rewrite_mode(&text, user_capable(user)) {
                            if skip_initial_mode {
                                skip_initial_mode = false;
                                continue;
                            }
                            text = m;
                        }
                    }
                    if write.send(Message::Text(text)).await.is_err() {
                        break;
                    }
                }
                Some(GridEvent::Closed(reason)) => {
                    log_debug!("[hosts] grid stream {} closed by host {}: {reason}", grid.stream, grid.host_id);
                    break;
                }
                Some(GridEvent::BytesAck { .. } | GridEvent::Bytes { .. }) => {}
                None => break,
            },
            m = read.next() => match m {
                Some(Ok(Message::Text(t))) => {
                    if as_user.as_deref().is_some_and(|u| !user_capable(u)) {
                        log_debug!("[hosts] grid stream {}: user {} is gone; closing", grid.stream, as_user.as_deref().unwrap_or(""));
                        break;
                    }
                    if !grid.send(t) {
                        break;
                    }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                Some(Ok(_)) => {}
            },
            _ = reauth.tick() => {
                if let GridClient::ConnectUser { token, owner_token, .. } = &client {
                    if !crate::routes::http::token_still_valid(token, owner_token) {
                        log_debug!("[hosts] grid stream {}: token revoked mid-session; closing", grid.stream);
                        break;
                    }
                }
            }
        }
    }
    grid.close("client closed");
    let _ = write.close().await;
}

/// `/cli/sessions/bytes?session&from` on a controller (W3b): the session's
/// raw output from offset `from`, read from the byte ring on its host, in
/// the same protocol as under `K2_ROLE=all`: a `session:ack` text event
/// (`fromOffset`; `currentFrontOffset` = the first byte sent, above
/// `fromOffset` when the ring had dropped some; `currentBackOffset` = the
/// ring's end), then binary messages in offset order, then live output.
/// A client that kept its offset reconnects after a controller restart
/// with `from` = its next byte and gets exactly what it missed. A host with
/// no byte ring answers an `error` event; the client falls back to
/// `/cli/sessions/grid`, whose first message is a snapshot. Whenever the
/// stream ends from the host's side (link lost, session gone, ring
/// overrun) an `error` event names why and the offset reached.
pub async fn serve_bytes<S>(mut stream: S, remote: &RemoteHost, session_id: &str, from: u64, client: GridClient)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut bytes = match remote.open_bytes(session_id, from) {
        Ok(b) => b,
        Err(e) => {
            use tokio::io::AsyncWriteExt;
            let body = serde_json::json!({ "error": e }).to_string();
            let resp = format!(
                "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{}Connection: close\r\n\r\n{}",
                body.len(),
                k2_core::frame_policy::CONNECT_HEADER_LINES,
                body
            );
            let _ = stream.write_all(resp.as_bytes()).await;
            return;
        }
    };
    let ws = match tokio_tungstenite::accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            log_debug!("[hosts] bytes handshake failed: {e}");
            return;
        }
    };
    let (mut write, mut read) = ws.split();
    k2_core::trace::attach(Some(session_id), "bytes");
    // The next offset the client must get; anything else is a gap and ends
    // the stream (the client reconnects and the ack shows the gap).
    let mut next = None;
    let mut reauth = tokio::time::interval(REAUTH_EVERY);
    reauth.tick().await;
    loop {
        tokio::select! {
            ev = bytes.events.recv() => match ev {
                Some(GridEvent::BytesAck { start, end, error: None }) if next.is_none() => {
                    let ack = serde_json::json!({"event": "session:ack", "payload": {
                        "sessionId": session_id,
                        "fromOffset": from,
                        "currentFrontOffset": start,
                        "currentBackOffset": end,
                    }});
                    log_debug!("[hosts] bytes stream {} session {session_id} from {from}: ring [{start}, {end}) on host {}", bytes.stream, bytes.host_id);
                    if write.send(Message::Text(ack.to_string())).await.is_err() {
                        break;
                    }
                    next = Some(start);
                }
                Some(GridEvent::BytesAck { error: Some(e), .. }) => {
                    let _ = write.send(Message::Text(grid_error(&e))).await;
                    break;
                }
                Some(GridEvent::Bytes { offset, data }) if next == Some(offset) => {
                    next = Some(offset + data.len() as u64);
                    if write.send(Message::Binary(data)).await.is_err() {
                        break;
                    }
                }
                Some(GridEvent::Bytes { offset, .. }) => {
                    let msg = format!("byte stream out of order: got offset {offset}, expected {next:?}; reconnect with from={}", next.unwrap_or(from));
                    log_debug!("[hosts] bytes stream {}: {msg}", bytes.stream);
                    let _ = write.send(Message::Text(grid_error(&msg))).await;
                    break;
                }
                Some(GridEvent::Closed(reason)) => {
                    log_debug!("[hosts] bytes stream {} closed by host {}: {reason}", bytes.stream, bytes.host_id);
                    let at = next.unwrap_or(from);
                    let _ = write.send(Message::Text(grid_error(&format!("byte stream ended at offset {at}: {reason}")))).await;
                    break;
                }
                Some(_) => {}
                None => break,
            },
            m = read.next() => match m {
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                Some(Ok(_)) => {}
            },
            _ = reauth.tick() => {
                if let GridClient::ConnectUser { token, owner_token, .. } = &client {
                    if !crate::routes::http::token_still_valid(token, owner_token) {
                        log_debug!("[hosts] bytes stream {}: token revoked mid-session; closing", bytes.stream);
                        break;
                    }
                }
            }
        }
    }
    bytes.close("client closed");
    let _ = write.close().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_events_get_the_controllers_capability() {
        let out = rewrite_mode(r#"{"event":"mode","payload":{"mode":"viewer","capable":true}}"#, false).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&out).unwrap(),
            serde_json::json!({"event": "mode", "payload": {"mode": "viewer", "capable": false}})
        );
        assert_eq!(rewrite_mode(r#"{"event":"delta","payload":{}}"#, false), None);
    }
}
