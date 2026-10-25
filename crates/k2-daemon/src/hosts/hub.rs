//! Controller side of the host link: the `/cli/hosts/attach` socket and
//! the relay that serves a client's `/cli/sessions/grid` from a host.
//!
//! Both are generic over the byte stream so tests drive them on loopback
//! sockets without the dispatcher.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k2_core::log_debug;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use super::proto::{Frame, PROTOCOL};
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
pub async fn serve_attach<S>(stream: S, remote: &RemoteHost, token: &str)
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
    let (host_name, labels) = match hello {
        Ok(Frame::Hello { protocol, host_name, token: presented, labels }) => {
            if protocol != PROTOCOL {
                return refuse(&mut ws, "protocol", &format!("controller speaks protocol {PROTOCOL}, host sent {protocol}")).await;
            }
            if token.is_empty() || !crate::routes::http::ct_eq_token(&presented, token) {
                return refuse(&mut ws, "bad_token", "the host token doesn't match this controller's").await;
            }
            if host_name.trim().is_empty() {
                return refuse(&mut ws, "protocol", "host_name is empty").await;
            }
            (host_name, labels)
        }
        Ok(other) => return refuse(&mut ws, "protocol", &format!("expected hello, got {}", other.kind())).await,
        Err(e) => return refuse(&mut ws, "protocol", &e).await,
    };
    let (conn, host_id, mut out) = remote.connect(&host_name);
    log_debug!("[hosts] host {host_id} labels {labels:?}");
    let welcome = Frame::Welcome {
        protocol: PROTOCOL,
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
