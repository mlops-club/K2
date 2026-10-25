//! The dispatcher's one call into this module ([`route`]): the host
//! routes, and under `K2_ROLE=controller` the S1 relays and the routes
//! that would start a PTY on the controller (501).
//!
//! Under `K2_ROLE=all` only `/cli/hosts/*` is looked at, and it falls
//! through to the dispatcher's usual 404: today's behaviour, unchanged.

use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

use crate::agent_host::remote;
use crate::routes::http;
use crate::runtime_role::Role;

pub const STATUS_PATH: &str = "/cli/hosts/status";

/// Routes a controller refuses because they start a PTY of their own,
/// without the `AgentHost` interface (login PTYs, raw byte streams, the
/// terminal API, the sidecar refresh). Exact paths and `/`-ended prefixes.
pub const CONTROLLER_NOT_IMPLEMENTED: &[&str] = &[
    "/cli/llm/accounts/login",
    "/cli/llm/accounts/login/cancel",
    "/cli/llm/accounts/login/input",
    "/cli/llm/accounts/login/status",
    "/cli/skin/wallet/login",
    "/cli/skin/wallet/login/cancel",
    "/cli/skin/wallet/login/input",
    "/cli/skin/wallet/login/status",
    "/cli/sessions/bytes",
    "/cli/sessions/subscribe",
    "/cli/sessions/v2/refresh",
    "/cli/terminal/",
];

pub const NOT_IMPLEMENTED_BODY: &str =
    r#"{"error":"not available with K2_ROLE=controller (dist-link first cut): agents run on agent hosts"}"#;

/// What the controller does with a request path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControllerRoute {
    Spawn,
    Close,
    Grid,
    NotImplemented,
    /// The dispatcher's usual handling.
    Local,
}

pub fn controller_route(path: &str) -> ControllerRoute {
    match path {
        "/cli/sessions/v2/spawn" => ControllerRoute::Spawn,
        "/cli/sessions/v2/close" => ControllerRoute::Close,
        "/cli/sessions/grid" => ControllerRoute::Grid,
        p if CONTROLLER_NOT_IMPLEMENTED
            .iter()
            .any(|r| if r.ends_with('/') { p.starts_with(r) } else { p == *r }) =>
        {
            ControllerRoute::NotImplemented
        }
        _ => ControllerRoute::Local,
    }
}

pub const STREAM_TOKEN_REFUSED: &str =
    r#"{"error":"per-session stream tokens are not relayed with K2_ROLE=controller (dist-link first cut)"}"#;

/// Why the controller refuses a grid connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GridRefusal {
    /// A Skin pass: never a terminal, as under `all`.
    Skin,
    /// A `/v1` per-session stream token (minted only for in-process
    /// sessions; not relayed in this cut).
    StreamToken,
    BadToken,
}

/// The `/cli/sessions/grid` decision, in the dispatcher's order under
/// `K2_ROLE=all`: Skin pass → 403; the owner token or a live Connect
/// session (`token_ok`) → relayed as that client; anything else → 403.
pub fn grid_client(query: &str, owner_token: &str) -> Result<super::hub::GridClient, GridRefusal> {
    use super::hub::GridClient;
    let token = http::extract_token(query).unwrap_or("");
    if k2_core::skin::is_skin_token(token) {
        return Err(GridRefusal::Skin);
    }
    if !token.is_empty() && http::ct_eq_token(token, owner_token) {
        return Ok(GridClient::Owner);
    }
    if !token.is_empty() {
        if let Some(username) = k2_core::connect_users::validate_session(token) {
            return Ok(GridClient::ConnectUser {
                username,
                token: token.to_string(),
                owner_token: owner_token.to_string(),
            });
        }
    }
    if crate::stream_token::query_authorizes(query) {
        return Err(GridRefusal::StreamToken);
    }
    Err(GridRefusal::BadToken)
}

async fn reply(stream: &mut TcpStream, buf: &mut [u8], status: &str, body: &str) {
    let _ = stream.read(buf).await;
    http::send_response(stream, status, "application/json", body).await;
}

/// Serve `path` if it is this module's. `true` = answered (the connection
/// is done); `false` = the dispatcher carries on as usual.
pub async fn route(stream: &mut TcpStream, buf: &mut [u8], path: &str, query: &str, owner_token: &str) -> bool {
    let role = crate::runtime_role::current();
    if path == STATUS_PATH {
        let Some(body) = super::status_json() else { return false };
        reply(stream, buf, "200 OK", &body.to_string()).await;
        return true;
    }
    if path == super::proto::ATTACH_PATH {
        return match super::controller() {
            Some(cfg) => {
                crate::delivery_drain::ensure_controller_drain();
                super::hub::serve_attach(&mut *stream, remote::global(), &cfg.token).await;
                true
            }
            None if role == Role::All => false,
            None => {
                reply(stream, buf, "404 Not Found", r#"{"error":"only a K2_ROLE=controller daemon takes host links"}"#).await;
                true
            }
        };
    }
    if path == crate::delivery_drain::OUTBOX_PATH {
        let Some(body) = crate::delivery_drain::outbox_status(query) else { return false };
        if !http::token_ok(query, owner_token) {
            reply(stream, buf, "403 Forbidden", r#"{"error":"invalid or missing token"}"#).await;
        } else {
            reply(stream, buf, "200 OK", &body.to_string()).await;
        }
        return true;
    }
    if role != Role::Controller {
        return false;
    }
    match controller_route(path) {
        ControllerRoute::Local => false,
        ControllerRoute::NotImplemented => {
            k2_core::log_debug!("[hosts] 501 {path} under K2_ROLE=controller");
            reply(stream, buf, "501 Not Implemented", NOT_IMPLEMENTED_BODY).await;
            true
        }
        ControllerRoute::Spawn | ControllerRoute::Close => {
            if !http::token_ok(query, owner_token) {
                reply(stream, buf, "403 Forbidden", r#"{"error":"invalid or missing token"}"#).await;
                return true;
            }
            let body = http::read_post_body(&mut *stream, buf).await;
            let (status, out) = if controller_route(path) == ControllerRoute::Spawn {
                remote::global().spawn(&body).await
            } else {
                remote::global().close(&body).await
            };
            http::send_response(stream, &status, "application/json", &out).await;
            true
        }
        ControllerRoute::Grid => {
            let client = match grid_client(query, owner_token) {
                Ok(c) => c,
                Err(GridRefusal::Skin) => {
                    let _ = stream.read(buf).await;
                    let r = crate::skin_routes::skin_terminal_forbidden();
                    http::send_response(stream, r.status, r.content_type, &r.body).await;
                    return true;
                }
                Err(GridRefusal::StreamToken) => {
                    reply(stream, buf, "403 Forbidden", STREAM_TOKEN_REFUSED).await;
                    return true;
                }
                Err(GridRefusal::BadToken) => {
                    reply(stream, buf, "403 Forbidden", r#"{"error":"invalid or missing token"}"#).await;
                    return true;
                }
            };
            let params = http::parse_params(path, query);
            let session = params.get("session").cloned().unwrap_or_default();
            super::hub::serve_grid(&mut *stream, remote::global(), &session, client).await;
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controller_routes_are_classified() {
        assert_eq!(controller_route("/cli/sessions/v2/spawn"), ControllerRoute::Spawn);
        assert_eq!(controller_route("/cli/sessions/v2/close"), ControllerRoute::Close);
        assert_eq!(controller_route("/cli/sessions/grid"), ControllerRoute::Grid);
        assert_eq!(controller_route("/cli/terminal/write"), ControllerRoute::NotImplemented);
        assert_eq!(controller_route("/cli/llm/accounts/login"), ControllerRoute::NotImplemented);
        assert_eq!(controller_route("/cli/sessions/bytes"), ControllerRoute::NotImplemented);
        // Exact entries are not prefixes.
        assert_eq!(controller_route("/cli/llm/accounts/list"), ControllerRoute::Local);
        assert_eq!(controller_route("/cli/sessions/events"), ControllerRoute::Local);
        assert_eq!(controller_route("/boot-status"), ControllerRoute::Local);
    }
}
