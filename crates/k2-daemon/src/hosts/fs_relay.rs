//! fs-relay: the app's file browser and git panel through a controller see
//! the agent's host's disk (design and decisions in
//! `docs/prd/progress/fs-relay.md`).
//!
//! Under `K2_ROLE=controller` the `/cli/fs/*` and `/cli/git/*` routes act on
//! the controller's own disk. A host on its own disk (`K2_WORK_ROOT`, home
//! sync) says so with the `k2.work_root` label; a request whose paths are in
//! a workspace an agent runs (or last ran) on such a host goes over the host
//! link (`http.relay`), and the host serves it with its own daemon's route,
//! from its own disk. Paths map between the controller's work root and the
//! host's (`K2_WORK_ROOT` on both; the same relative path, D2).
//!
//! Everything else stays local, byte for byte: `K2_ROLE=all`, hosts on the
//! shared volume (no label), paths no host holds, and the routes that are
//! about the controller's own machine or can't be routed by a path.

use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine;
use k2_core::log_debug;
use serde_json::Value;
use tokio::net::TcpStream;

use super::proto::Frame;
use crate::agent_host::remote::{RemoteHost, WorkspaceRoute};
use crate::routes::http;

/// A host's label: the work root its workspaces are under (its own disk).
pub const LABEL_WORK_ROOT: &str = "k2.work_root";

/// How long the controller waits for a host's answer.
const RELAY_DEADLINE: Duration = Duration::from_secs(120);

/// The largest request body the relay looks into (or carries).
const BODY_CAP: usize = 8 << 20;

/// Is `method path` one the relay carries? Reads and writes of files and
/// git; not what is about the controller's own machine (open in Finder,
/// the clipboard, thread links, the file events socket), the streaming
/// uploads, or the compress/extract jobs (their status polls name no path).
pub fn relayed(method: &str, path: &str) -> bool {
    let post = method.eq_ignore_ascii_case("POST");
    match path {
        "/cli/fs/info" | "/cli/fs/read-dir" | "/cli/fs/read-file" | "/cli/fs/read-binary" | "/cli/fs/read-range"
        | "/cli/fs/stat" | "/cli/fs/search-tree" | "/cli/fs/zip-list" => true,
        "/cli/fs/create" | "/cli/fs/delete" | "/cli/fs/move" | "/cli/fs/rename" | "/cli/fs/copy" | "/cli/fs/duplicate"
        | "/cli/fs/write-file" => post,
        "/cli/worktree" => true,
        p => p.starts_with("/cli/git/"),
    }
}

/// The work root a host's labels name, if it is on its own disk.
pub fn work_root_label(labels: &[String]) -> Option<PathBuf> {
    labels.iter().find_map(|l| {
        let (k, v) = l.split_once('=')?;
        (k.trim() == LABEL_WORK_ROOT && !v.trim().is_empty()).then(|| PathBuf::from(v.trim()))
    })
}

/// `s` with its `from` prefix (whole components) replaced by `to`.
pub fn map_path(s: &str, from: &Path, to: &Path) -> Option<String> {
    let rest = Path::new(s).strip_prefix(from).ok()?;
    let mapped = if rest.as_os_str().is_empty() { to.to_path_buf() } else { to.join(rest) };
    Some(mapped.to_string_lossy().into_owned())
}

/// `s` mapped by the first pair whose `from` it is under; unchanged if none.
pub fn map_any(s: &str, maps: &[(PathBuf, PathBuf)]) -> String {
    if !s.starts_with('/') {
        return s.to_string();
    }
    maps.iter().find_map(|(from, to)| map_path(s, from, to)).unwrap_or_else(|| s.to_string())
}

/// Every string in `v` that is a path, mapped.
pub fn map_json(v: &mut Value, maps: &[(PathBuf, PathBuf)]) {
    match v {
        Value::String(s) => *s = map_any(s, maps),
        Value::Array(a) => a.iter_mut().for_each(|x| map_json(x, maps)),
        Value::Object(o) => o.values_mut().for_each(|x| map_json(x, maps)),
        _ => {}
    }
}

fn decode(v: &str) -> String {
    url::form_urlencoded::parse(format!("x={v}").as_bytes()).next().map(|(_, v)| v.into_owned()).unwrap_or_default()
}

fn encode(v: &str) -> String {
    url::form_urlencoded::byte_serialize(v.as_bytes()).collect()
}

/// The query with its path values mapped and its `token` dropped (the host
/// uses its own).
pub fn map_query(query: &str, maps: &[(PathBuf, PathBuf)]) -> String {
    query
        .split('&')
        .filter(|kv| !kv.is_empty())
        .filter_map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (k != "token").then(|| format!("{k}={}", encode(&map_any(&decode(v), maps))))
        })
        .collect::<Vec<_>>()
        .join("&")
}

/// The absolute paths a request names: query values, and the strings of a
/// JSON (or form) body.
pub fn named_paths(query: &str, body: &[u8]) -> Vec<String> {
    let mut out = query_paths(query);
    if body.is_empty() {
        return out;
    }
    if let Ok(v) = serde_json::from_slice::<Value>(body) {
        fn walk(v: &Value, out: &mut Vec<String>) {
            match v {
                Value::String(s) if s.starts_with('/') => out.push(s.clone()),
                Value::Array(a) => a.iter().for_each(|x| walk(x, out)),
                Value::Object(o) => o.values().for_each(|x| walk(x, out)),
                _ => {}
            }
        }
        walk(&v, &mut out);
    } else if let Ok(form) = std::str::from_utf8(body) {
        out.extend(query_paths(form));
    }
    out
}

/// The absolute paths among a query's (or a form's) values.
fn query_paths(query: &str) -> Vec<String> {
    query
        .split('&')
        .filter_map(|kv| kv.split_once('=').map(|(k, v)| (k, decode(v))))
        .filter(|(k, v)| *k != "token" && v.starts_with('/'))
        .map(|(_, v)| v)
        .collect()
}

/// A body with its path strings mapped (JSON or a form; anything else as is).
fn map_body(body: &[u8], maps: &[(PathBuf, PathBuf)]) -> Vec<u8> {
    if body.is_empty() {
        return Vec::new();
    }
    if let Ok(mut v) = serde_json::from_slice::<Value>(body) {
        map_json(&mut v, maps);
        return v.to_string().into_bytes();
    }
    match std::str::from_utf8(body) {
        Ok(form) if form.contains('=') && !form.contains('\n') => map_query(form, maps).into_bytes(),
        _ => body.to_vec(),
    }
}

/// The controller's own work root (`K2_WORK_ROOT` on the controller): where
/// the app's workspace paths are, mapped to each host's root.
fn controller_root() -> Option<PathBuf> {
    super::home::work_root(|k| std::env::var(k).ok())
}

// ── controller ─────────────────────────────────────────────────────────

/// The request as sent, without consuming it: the whole body when it fits
/// in [`BODY_CAP`] (`None`: too big, or it never completed).
async fn peek_body(stream: &TcpStream) -> Option<Vec<u8>> {
    let mut buf = vec![0u8; BODY_CAP + 16 * 1024];
    for _ in 0..200 {
        let n = stream.peek(&mut buf).await.ok()?;
        let head_end = buf[..n].windows(4).position(|w| w == b"\r\n\r\n")? + 4;
        let head = String::from_utf8_lossy(&buf[..head_end]).to_ascii_lowercase();
        let len = head.lines().find_map(|l| l.strip_prefix("content-length:").and_then(|v| v.trim().parse::<usize>().ok()));
        let Some(len) = len else { return Some(Vec::new()) };
        if len > BODY_CAP {
            return None;
        }
        if n >= head_end + len {
            return Some(buf[head_end..head_end + len].to_vec());
        }
        // More of the body is on its way.
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    None
}

/// `gate::route`'s hook (controller). `true` = answered over a host; `false`
/// = the controller's own routes carry on, the request untouched.
pub async fn route(stream: &mut TcpStream, buf: &mut [u8], method: &str, path: &str, query: &str, owner_token: &str) -> bool {
    route_on(crate::agent_host::remote::global(), controller_root(), stream, buf, method, path, query, owner_token).await
}

/// [`route`] over `remote`, the controller's work root `ctl_root`.
#[allow(clippy::too_many_arguments)]
pub async fn route_on(
    remote: &'static RemoteHost,
    ctl_root: Option<PathBuf>,
    stream: &mut TcpStream,
    buf: &mut [u8],
    method: &str,
    path: &str,
    query: &str,
    owner_token: &str,
) -> bool {
    if !relayed(method, path) {
        return false;
    }
    // No host on its own disk: nothing to decide (the shared volume).
    if !remote.any_host_on_own_disk() {
        return false;
    }
    let body = if method.eq_ignore_ascii_case("POST") {
        match peek_body(stream).await {
            Some(b) => b,
            None => return false,
        }
    } else {
        Vec::new()
    };
    let route = remote.workspace_route(&named_paths(query, &body), ctl_root.as_deref());
    let (host, maps) = match route {
        WorkspaceRoute::Local => return false,
        WorkspaceRoute::Relay { host, maps } => (host, maps),
        WorkspaceRoute::Away { host, path: p } => {
            let _ = http::read_post_body_capped(&mut *stream, buf, BODY_CAP).await;
            let body = serde_json::json!({
                "error": format!("{p} is on agent host {host}, which is not linked; try again when it is back"),
                "code": "host_away",
            });
            http::send_response(stream, "503 Service Unavailable", "application/json", &body.to_string()).await;
            return true;
        }
        WorkspaceRoute::Split(hosts) => {
            let _ = http::read_post_body_capped(&mut *stream, buf, BODY_CAP).await;
            let body = serde_json::json!({
                "error": format!("this request names workspaces on different agent hosts ({}); one host at a time", hosts.join(", ")),
                "code": "cross_host",
            });
            http::send_response(stream, "501 Not Implemented", "application/json", &body.to_string()).await;
            return true;
        }
    };
    // Relayed: the controller authorises (the host acts as its own owner).
    let _ = http::read_post_body_capped(&mut *stream, buf, BODY_CAP).await;
    if !http::token_ok(query, owner_token) {
        http::send_response(stream, "403 Forbidden", "application/json", r#"{"error":"invalid or missing token"}"#).await;
        return true;
    }
    let back: Vec<(PathBuf, PathBuf)> = maps.iter().map(|(f, t)| (t.clone(), f.clone())).collect();
    let frame_query = map_query(query, &maps);
    let frame_body = base64::engine::general_purpose::STANDARD.encode(map_body(&body, &maps));
    let (m, p) = (method.to_ascii_uppercase(), path.to_string());
    let h = host.clone();
    let r = tokio::task::spawn_blocking(move || {
        remote.relay_blocking(&h, |req| Frame::HttpRelay { req, method: m, path: p, query: frame_query, body_b64: frame_body }, RELAY_DEADLINE)
    })
    .await
    .unwrap_or_else(|e| Err(format!("relay: {e}")));
    k2_core::trace::emit(|| {
        k2_core::trace::Event::new(events::CTL_FS_RELAY).attr("host", host.as_str()).attr("path", path).attr("ok", r.is_ok())
    });
    match r {
        Ok(Frame::HttpReply { status, content_type, body, .. }) => {
            let body = match serde_json::from_str::<Value>(&body) {
                Ok(mut v) => {
                    map_json(&mut v, &back);
                    v.to_string()
                }
                Err(_) => body,
            };
            http::send_response(stream, &status, &content_type, &body).await;
        }
        Ok(other) => {
            let e = serde_json::json!({"error": format!("agent host {host} answered the relay with {}", other.kind())});
            http::send_response(stream, "502 Bad Gateway", "application/json", &e.to_string()).await;
        }
        Err(e) => {
            log_debug!("[hosts/fs-relay] {path} on host {host}: {e}");
            let e = serde_json::json!({"error": format!("agent host {host}: {e}"), "code": "relay_failed"});
            http::send_response(stream, "502 Bad Gateway", "application/json", &e.to_string()).await;
        }
    }
    true
}

pub mod events {
    /// The controller relayed an fs/git request (attrs: host, path, ok).
    pub const CTL_FS_RELAY: &str = "ctl.fs_relay";
    /// A host served one (attrs: path, status).
    pub const HOST_FS_SERVED: &str = "host.fs_served";
}

// ── host ───────────────────────────────────────────────────────────────

/// This daemon's own address and owner token (set at boot by a host).
static LOCAL: parking_lot::Mutex<Option<(u16, String)>> = parking_lot::Mutex::new(None);

pub fn set_local_daemon(port: u16, token: &str) {
    *LOCAL.lock() = Some((port, token.to_string()));
}

/// The link's `http.relay`: run the request through this daemon's own
/// route (its own token, its own disk) and answer `http.reply`.
pub async fn serve(req: u64, method: &str, path: &str, query: &str, body_b64: &str) -> Frame {
    let reply = |status: &str, body: String| Frame::HttpReply {
        req,
        status: status.to_string(),
        content_type: "application/json".into(),
        body,
    };
    // Only what a controller may relay: never the host's other routes.
    if !relayed(method, path) {
        return reply("403 Forbidden", serde_json::json!({"error": format!("{method} {path} is not relayed to hosts")}).to_string());
    }
    let Some((port, token)) = LOCAL.lock().clone() else {
        return reply("503 Service Unavailable", r#"{"error":"this host's daemon address is not known yet"}"#.into());
    };
    let body = match base64::engine::general_purpose::STANDARD.decode(body_b64) {
        Ok(b) => b,
        Err(e) => return reply("400 Bad Request", serde_json::json!({"error": format!("relay body: {e}")}).to_string()),
    };
    let sep = if query.is_empty() { "" } else { "&" };
    let url = format!("http://127.0.0.1:{port}{path}?{query}{sep}token={}", encode(&token));
    // A fresh connection per request (the daemon closes each one).
    let client = match reqwest::Client::builder().timeout(RELAY_DEADLINE).pool_max_idle_per_host(0).build() {
        Ok(c) => c,
        Err(e) => return reply("500 Internal Server Error", serde_json::json!({"error": format!("http client: {e}")}).to_string()),
    };
    let rb = if method.eq_ignore_ascii_case("POST") {
        client.post(&url).header("Content-Type", "application/json").body(body)
    } else {
        client.get(&url)
    };
    let out = match rb.send().await {
        Ok(resp) => {
            let status = resp.status();
            let status = format!("{} {}", status.as_u16(), status.canonical_reason().unwrap_or(""));
            let ct = resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/json")
                .to_string();
            match resp.text().await {
                Ok(body) => Frame::HttpReply { req, status: status.trim().to_string(), content_type: ct, body },
                Err(e) => reply("502 Bad Gateway", serde_json::json!({"error": format!("read the answer: {e}")}).to_string()),
            }
        }
        Err(e) => reply("502 Bad Gateway", serde_json::json!({"error": format!("this host's daemon: {e}")}).to_string()),
    };
    if let Frame::HttpReply { status, .. } = &out {
        k2_core::trace::emit(|| k2_core::trace::Event::new(events::HOST_FS_SERVED).attr("path", path).attr("status", status.as_str()));
    }
    out
}

#[cfg(test)]
#[path = "fs_relay_tests.rs"]
mod tests;
