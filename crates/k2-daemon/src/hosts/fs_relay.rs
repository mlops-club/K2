//! fs-relay: the app's file browser and git panel through a controller see
//! the agent's host's disk (design, decisions and the security review's
//! fixes in `docs/prd/progress/fs-relay.md`).
//!
//! Under `K2_ROLE=controller` the `/cli/fs/*` and `/cli/git/*` routes act on
//! the controller's own disk. A host on its own disk (`K2_WORK_ROOT`, home
//! sync) says so with the `k2.work_root` label; a request whose paths are in
//! a workspace an agent runs (or last ran) on such a host goes over the host
//! link (`http.relay`), and the host serves it with its own daemon's route,
//! from its own disk. Paths map between the controller's work root and the
//! host's (`K2_WORK_ROOT` on both; the same relative path, D2).
//!
//! - GETs are decided at the gate ([`route`]), from the query, after the
//!   caller's token; POSTs by the dispatcher's arms once they have read the
//!   whole body ([`relay_post`]). A local one runs today's handler, as is;
//!   one for a host workspace is never served from the controller's copy.
//! - The controller maps only known path keys, refuses `..` and any path
//!   outside the workspace it relays to. The host serves only the exact
//!   route list, confines every path to its work root (symlinks resolved),
//!   sends its own token in a header, and caps its answer.
//!
//! Everything else stays local, byte for byte: `K2_ROLE=all`, hosts on the
//! shared volume (no label), paths no host holds, and the routes that are
//! about the controller's own machine or its database (git notes, open the
//! changes tab, the worktree setting).

use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use base64::Engine;
use k2_core::log_debug;
use serde_json::{json, Value};
use tokio::net::TcpStream;

use super::proto::Frame;
use crate::agent_host::remote::{RemoteHost, WorkspaceRoute};
use crate::routes::http;

/// A host's label: the work root its workspaces are under (its own disk).
pub const LABEL_WORK_ROOT: &str = "k2.work_root";

/// How long the controller waits for a host's answer.
const RELAY_DEADLINE: Duration = Duration::from_secs(120);

/// The largest request body, and the largest answer, one relay frame
/// carries (the link's frames are capped at 16 MiB). Bigger is a 413: send
/// smaller upload chunks, or read a file by ranges.
pub const MAX_RELAY_BODY: usize = 8 << 20;

/// The GETs the relay carries, exactly.
const GET_ROUTES: &[&str] = &[
    "/cli/fs/info",
    "/cli/fs/read-dir",
    "/cli/fs/read-file",
    "/cli/fs/read-binary",
    "/cli/fs/read-range",
    "/cli/fs/stat",
    "/cli/fs/zip-list",
    "/cli/git/branches",
    "/cli/git/changes",
    "/cli/git/diff-between",
    "/cli/git/diff-file",
    "/cli/git/diff-summary",
    "/cli/git/file-at-ref",
    "/cli/git/info",
    "/cli/git/merge-status",
    "/cli/git/snapshot",
    "/cli/git/worktrees",
];

/// The POSTs the relay carries, exactly. Not the git notes (they live in
/// the controller's database and are checked by author), not
/// `open-changes` (an event on the controller's bus).
const POST_ROUTES: &[&str] = &[
    "/cli/fs/create",
    "/cli/fs/delete",
    "/cli/fs/move",
    "/cli/fs/rename",
    "/cli/fs/copy",
    "/cli/fs/duplicate",
    "/cli/fs/write-file",
    "/cli/fs/upload-binary",
    "/cli/fs/upload-chunk",
    "/cli/fs/search-tree",
    "/cli/fs/resolve-links",
    "/cli/git/abort-merge",
    "/cli/git/commit",
    "/cli/git/create-worktree",
    "/cli/git/delete-branch",
    "/cli/git/discard",
    "/cli/git/init",
    "/cli/git/merge-branch",
    "/cli/git/prune-worktrees",
    "/cli/git/remove-worktree",
    "/cli/git/reopen-worktree",
    "/cli/git/resolve",
    "/cli/git/stage",
    "/cli/git/stage-all",
    "/cli/git/unstage",
];

/// POSTs the relay doesn't carry yet that act on a workspace's files: for a
/// workspace on a host they are refused (501), never run on the
/// controller's copy (re-review H6). Their cancels and status polls name a
/// job, not a path, and stay local.
const NOT_RELAYED_POST: &[&str] = &["/cli/fs/compress", "/cli/fs/extract"];

/// The largest encoded frame either side sends (the link's frames are
/// capped at 16 MiB; JSON escaping can grow a body several times).
pub const MAX_RELAY_FRAME: usize = 15 << 20;

/// Is `method path` one the relay carries? Exact routes only.
pub fn relayed(method: &str, path: &str) -> bool {
    if method.eq_ignore_ascii_case("GET") {
        GET_ROUTES.contains(&path)
    } else if method.eq_ignore_ascii_case("POST") {
        POST_ROUTES.contains(&path)
    } else {
        false
    }
}

/// The keys whose values are paths (or path lists), in requests and in
/// answers. Only these are mapped; file contents, messages, queries and
/// diffs never are.
const PATH_KEYS: &[&str] = &[
    "path",
    "paths",
    "dir",
    "root",
    "sources",
    "destination",
    "old_path",
    "oldPath",
    "new_path",
    "newPath",
    "project_path",
    "projectPath",
    "worktree_path",
    "worktreePath",
    "repo_root",
    "repoRoot",
    "workspace",
    "prefixes",
];

/// Keys whose values are relative names inside a path: never `..`.
const NAME_KEYS: &[&str] = &["file_path", "filePath", "new_name", "newName", "filename"];

/// Keys that hold text, not paths (a host doesn't confine their values).
const TEXT_KEYS: &[&str] = &["content", "base64", "message", "query", "text", "body"];

/// The work root a host's labels name, if it is on its own disk.
pub fn work_root_label(labels: &[String]) -> Option<PathBuf> {
    labels.iter().find_map(|l| {
        let (k, v) = l.split_once('=')?;
        (k.trim() == LABEL_WORK_ROOT && !v.trim().is_empty()).then(|| PathBuf::from(v.trim()))
    })
}

/// `s` with its `from` prefix (whole components) replaced by `to`, as
/// strings: the rest of `s` is kept byte for byte.
pub fn map_path(s: &str, from: &Path, to: &Path) -> Option<String> {
    let f = from.to_str()?.trim_end_matches('/');
    let t = to.to_str()?.trim_end_matches('/');
    if s == f {
        return Some(t.to_string());
    }
    let rest = s.strip_prefix(f)?;
    rest.starts_with('/').then(|| format!("{t}{rest}"))
}

/// `s` mapped by the first pair whose `from` it is under; unchanged if none.
pub fn map_any(s: &str, maps: &[(PathBuf, PathBuf)]) -> String {
    if !s.starts_with('/') {
        return s.to_string();
    }
    maps.iter().find_map(|(from, to)| map_path(s, from, to)).unwrap_or_else(|| s.to_string())
}

/// The path strings in `v`: under [`PATH_KEYS`] (a string, or a list of
/// them), at any depth.
fn path_values_mut<'a>(v: &'a mut Value, out: &mut Vec<&'a mut String>) {
    match v {
        Value::Object(o) => {
            for (k, x) in o.iter_mut() {
                if PATH_KEYS.contains(&k.as_str()) {
                    match x {
                        Value::String(s) => out.push(s),
                        Value::Array(a) => a.iter_mut().for_each(|y| match y {
                            Value::String(s) => out.push(s),
                            other => path_values_mut(other, out),
                        }),
                        other => path_values_mut(other, out),
                    }
                } else {
                    path_values_mut(x, out);
                }
            }
        }
        Value::Array(a) => a.iter_mut().for_each(|x| path_values_mut(x, out)),
        _ => {}
    }
}

/// Every path in `v` (known keys only), mapped.
pub fn map_json(v: &mut Value, maps: &[(PathBuf, PathBuf)]) {
    let mut paths = Vec::new();
    path_values_mut(v, &mut paths);
    for s in paths {
        *s = map_any(s, maps);
    }
}

fn encode(v: &str) -> String {
    url::form_urlencoded::byte_serialize(v.as_bytes()).collect()
}

/// The query's (key, value) pairs, decoded, without `token`.
fn pairs(query: &str) -> Vec<(String, String)> {
    url::form_urlencoded::parse(query.as_bytes()).filter(|(k, _)| k != "token").map(|(k, v)| (k.into_owned(), v.into_owned())).collect()
}

/// The query with its path values mapped and its `token` dropped (the host
/// uses its own).
pub fn map_query(query: &str, maps: &[(PathBuf, PathBuf)]) -> String {
    pairs(query)
        .into_iter()
        .map(|(k, v)| {
            let v = if PATH_KEYS.contains(&k.as_str()) { map_any(&v, maps) } else { v };
            format!("{}={}", encode(&k), encode(&v))
        })
        .collect::<Vec<_>>()
        .join("&")
}

/// The paths a request names (known keys): query values, and a JSON (or
/// form) body's.
pub fn named_paths(query: &str, body: &[u8]) -> Vec<String> {
    // Absolute paths route; a relative one (a repo-relative `old_path`) is
    // resolved on the host, against its request's base path.
    let mut out: Vec<String> =
        pairs(query).into_iter().filter(|(k, v)| PATH_KEYS.contains(&k.as_str()) && v.starts_with('/')).map(|(_, v)| v).collect();
    if body.is_empty() {
        return out;
    }
    if let Ok(mut v) = serde_json::from_slice::<Value>(body) {
        let mut ps = Vec::new();
        path_values_mut(&mut v, &mut ps);
        out.extend(ps.into_iter().map(|s| s.clone()).filter(|s| s.starts_with('/')));
    } else if let Ok(form) = std::str::from_utf8(body) {
        out.extend(named_paths(form, b""));
    }
    out
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

fn has_dot_dot(p: &str) -> bool {
    Path::new(p).components().any(|c| matches!(c, Component::ParentDir))
}

/// The controller's own work root (`K2_WORK_ROOT` on the controller): where
/// the app's workspace paths are, mapped to each host's root. As given and
/// canonical (a request may name it either way: `/tmp` and `/private/tmp`).
pub fn controller_roots() -> Vec<PathBuf> {
    let raw = std::env::var(super::home::WORK_ROOT_VAR).ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty());
    let mut roots: Vec<PathBuf> = raw.iter().map(PathBuf::from).collect();
    if let Some(c) = super::home::work_root(|k| std::env::var(k).ok()) {
        if !roots.contains(&c) {
            roots.push(c);
        }
    }
    roots
}

/// A host's answer, as the controller sends it on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    pub status: String,
    pub content_type: String,
    pub body: String,
}

impl Answer {
    fn json(status: &str, body: Value) -> Answer {
        Answer { status: status.to_string(), content_type: "application/json".into(), body: body.to_string() }
    }
}

// ── controller ─────────────────────────────────────────────────────────

/// What the controller does with a request.
#[derive(Debug, PartialEq, Eq)]
pub enum Decision {
    Local,
    Relay { host: String, maps: Vec<(PathBuf, PathBuf)> },
    Refuse(Answer),
}

/// Where a request goes, from the paths it names: refused when it names a
/// `..`, a path outside the workspace it would be relayed to, workspaces on
/// two hosts, or one on a host that is away.
pub fn decide(remote: &RemoteHost, query: &str, body: &[u8]) -> Decision {
    let paths = named_paths(query, body);
    if paths.iter().any(|p| has_dot_dot(p)) {
        return Decision::Refuse(Answer::json(
            "400 Bad Request",
            json!({"error": "a path with '..' is not accepted on a controller with agent hosts", "code": "bad_path"}),
        ));
    }
    match remote.workspace_route(&paths) {
        WorkspaceRoute::Local => Decision::Local,
        WorkspaceRoute::Relay { host, maps } => {
            // Every path must be in the workspace it goes to: a second path
            // (a copy's destination) elsewhere would be served unmapped.
            if let Some(out) = paths.iter().find(|p| !maps.iter().any(|(from, _)| map_path(p, from, from).is_some())) {
                return Decision::Refuse(Answer::json(
                    "400 Bad Request",
                    json!({"error": format!("{out} is outside the workspace this request is served from (agent host {host})"), "code": "outside_workspace"}),
                ));
            }
            Decision::Relay { host, maps }
        }
        WorkspaceRoute::Away { host, path } => Decision::Refuse(Answer::json(
            "503 Service Unavailable",
            json!({"error": format!("{path} is on agent host {host}, which is not linked; try again when it is back"), "code": "host_away"}),
        )),
        WorkspaceRoute::Split(hosts) => Decision::Refuse(Answer::json(
            "501 Not Implemented",
            json!({"error": format!("this request names workspaces on different agent hosts ({}); one host at a time", hosts.join(", ")), "code": "cross_host"}),
        )),
    }
}

/// A host's status line, rebuilt from its code (never its text).
fn safe_status(s: &str) -> String {
    let code = s.split_whitespace().next().and_then(|c| c.parse::<u16>().ok()).and_then(|c| reqwest::StatusCode::from_u16(c).ok());
    match code {
        Some(c) => format!("{} {}", c.as_u16(), c.canonical_reason().unwrap_or("")).trim().to_string(),
        None => "502 Bad Gateway".into(),
    }
}

/// A host's content type, from a fixed list.
fn safe_content_type(ct: &str) -> &'static str {
    match ct.split(';').next().unwrap_or("").trim().to_ascii_lowercase().as_str() {
        "text/plain" => "text/plain; charset=utf-8",
        "application/octet-stream" => "application/octet-stream",
        _ => "application/json",
    }
}

/// One request relayed to `host` (its paths mapped there, the answer's
/// back). Blocking.
fn exchange(remote: &RemoteHost, host: &str, maps: &[(PathBuf, PathBuf)], method: &str, path: &str, query: &str, body: &[u8]) -> Answer {
    let frame_query = map_query(query, maps);
    let body_b64 = base64::engine::general_purpose::STANDARD.encode(map_body(body, maps));
    let (m, p) = (method.to_ascii_uppercase(), path.to_string());
    let probe = Frame::HttpRelay { req: u64::MAX, method: m.clone(), path: p.clone(), query: frame_query.clone(), body_b64: body_b64.clone() };
    if probe.encode().len() > MAX_RELAY_FRAME {
        return Answer::json(
            "413 Payload Too Large",
            json!({"error": format!("too big for one relay frame ({MAX_RELAY_FRAME} bytes encoded)"), "code": "too_large"}),
        );
    }
    let r = remote.relay_blocking(host, |req| Frame::HttpRelay { req, method: m, path: p, query: frame_query, body_b64 }, RELAY_DEADLINE);
    k2_core::trace::emit(|| {
        k2_core::trace::Event::new(events::CTL_FS_RELAY).attr("host", host).attr("path", path).attr("ok", r.is_ok())
    });
    match r {
        Ok(Frame::HttpReply { status, content_type, body, .. }) => {
            let back: Vec<(PathBuf, PathBuf)> = maps.iter().map(|(f, t)| (t.clone(), f.clone())).collect();
            let body = match serde_json::from_str::<Value>(&body) {
                Ok(mut v) => {
                    map_json(&mut v, &back);
                    v.to_string()
                }
                Err(_) => body,
            };
            Answer { status: safe_status(&status), content_type: safe_content_type(&content_type).into(), body }
        }
        Ok(other) => Answer::json("502 Bad Gateway", json!({"error": format!("agent host {host} answered the relay with {}", other.kind())})),
        Err(e) => {
            log_debug!("[hosts/fs-relay] {path} on host {host}: {e}");
            Answer::json("502 Bad Gateway", json!({"error": format!("agent host {host}: {e}"), "code": "relay_failed"}))
        }
    }
}

/// An App pass (`k2skn_`): fs-relay doesn't carry its identity (its room,
/// its caps) to a host, so on a controller whose workspaces live on hosts
/// its file requests are refused, never served from the controller's copy
/// (re-review H6).
fn app_pass(query: &str) -> bool {
    http::extract_token(query).is_some_and(k2_core::skin::is_skin_token)
}

/// N6: an App pass's request goes to a host only when the pass is valid and
/// its workspace (the `workspace` it names, jailed to the pass) is one
/// that routes to a host. A revoked pass, or one whose workspace is the
/// controller's own, is the local route's to answer.
fn app_pass_to_host(remote: &RemoteHost, query: &str, body: &[u8]) -> bool {
    let Some(pass) = http::extract_token(query).and_then(crate::apps::gateway::resolve_pass) else { return false };
    let ws = pairs(query)
        .into_iter()
        .find(|(k, _)| k == "workspace")
        .map(|(_, v)| v)
        .or_else(|| serde_json::from_slice::<Value>(body).ok().and_then(|v| v.get("workspace").and_then(Value::as_str).map(str::to_string)));
    let Some(ws) = ws else { return false };
    match crate::fs_routes::resolve_skin_workspace(&pass, &ws) {
        Ok(w) => !matches!(remote.workspace_route(&[w.path]), WorkspaceRoute::Local),
        Err(_) => false,
    }
}

fn app_pass_refused() -> Answer {
    Answer::json(
        "501 Not Implemented",
        json!({"error": "App passes don't reach workspace files on a controller whose workspaces live on agent hosts (not relayed yet)", "code": "not_relayed"}),
    )
}

/// `gate::route`'s hook (controller), for GETs: `true` = answered (over a
/// host, or refused); `false` = the controller's own routes carry on,
/// untouched.
pub async fn route(stream: &mut TcpStream, buf: &mut [u8], method: &str, path: &str, query: &str, owner_token: &str) -> bool {
    route_on(crate::agent_host::remote::global(), stream, buf, method, path, query, owner_token).await
}

/// [`route`] over `remote`.
pub async fn route_on(
    remote: &'static RemoteHost,
    stream: &mut TcpStream,
    buf: &mut [u8],
    method: &str,
    path: &str,
    query: &str,
    owner_token: &str,
) -> bool {
    if !method.eq_ignore_ascii_case("GET") || !relayed(method, path) || !remote.any_host_on_own_disk() {
        return false;
    }
    let answer = if app_pass(query) {
        let q = query.to_string();
        if !tokio::task::spawn_blocking(move || app_pass_to_host(remote, &q, b"")).await.unwrap_or(false) {
            return false;
        }
        app_pass_refused()
    } else if !http::token_ok(query, owner_token) {
        // The caller first: no work, and nothing about the hosts, before it.
        return false;
    } else {
        match decide(remote, query, b"") {
            Decision::Local => return false,
            Decision::Refuse(a) => a,
            Decision::Relay { host, maps } => {
                let (p, q) = (path.to_string(), query.to_string());
                tokio::task::spawn_blocking(move || exchange(remote, &host, &maps, "GET", &p, &q, b""))
                    .await
                    .unwrap_or_else(|e| Answer::json("500 Internal Server Error", json!({"error": format!("relay: {e}")})))
            }
        }
    };
    let _ = http::read_post_body_capped(&mut *stream, buf, 1 << 20).await;
    http::send_response(stream, &answer.status, &answer.content_type, &answer.body).await;
    true
}

/// A dispatcher POST arm's hook, once it has read the whole body: `Some` =
/// answered (by a host, or refused); `None` = run the arm's own handler as
/// today.
pub async fn relay_post(path: &str, query: &str, owner_token: &str, body: &[u8]) -> Option<Answer> {
    if crate::runtime_role::current() != crate::runtime_role::Role::Controller {
        return None;
    }
    relay_post_on(crate::agent_host::remote::global(), path, query, owner_token, body).await
}

/// [`relay_post`] over `remote`.
pub async fn relay_post_on(remote: &'static RemoteHost, path: &str, query: &str, owner_token: &str, body: &[u8]) -> Option<Answer> {
    let not_relayed = NOT_RELAYED_POST.contains(&path);
    if !(relayed("POST", path) || not_relayed) || !remote.any_host_on_own_disk() {
        return None;
    }
    if app_pass(query) {
        let (q, b) = (query.to_string(), body.to_vec());
        let to_host = tokio::task::spawn_blocking(move || app_pass_to_host(remote, &q, &b)).await.unwrap_or(false);
        return to_host.then(app_pass_refused);
    }
    if !http::token_ok(query, owner_token) {
        return None;
    }
    let (path, query, body) = (path.to_string(), query.to_string(), body.to_vec());
    tokio::task::spawn_blocking(move || post_blocking(remote, &path, &query, &body, not_relayed))
        .await
        .unwrap_or_else(|e| Some(Answer::json("500 Internal Server Error", json!({"error": format!("relay: {e}")}))))
}

fn post_blocking(remote: &RemoteHost, path: &str, query: &str, body: &[u8], not_relayed: bool) -> Option<Answer> {
    // resolve-links names its workspace by address or id too: the
    // controller resolves it (its registry), the host gets the path.
    let resolved;
    let body = if path == "/cli/fs/resolve-links" {
        match resolve_links_body(body) {
            Some(b) => {
                resolved = b;
                resolved.as_slice()
            }
            None => body,
        }
    } else {
        body
    };
    match decide(remote, query, body) {
        Decision::Local => None,
        Decision::Refuse(a) => Some(a),
        Decision::Relay { host, .. } if not_relayed => Some(Answer::json(
            "501 Not Implemented",
            json!({"error": format!("{path} is not relayed to agent hosts yet; this workspace is on {host}"), "code": "not_relayed"}),
        )),
        Decision::Relay { host, maps } if path == "/cli/fs/upload-chunk" => Some(upload_chunk_split(remote, &host, &maps, query, body)),
        Decision::Relay { host, .. } if body.len() > MAX_RELAY_BODY => Some(Answer::json(
            "413 Payload Too Large",
            json!({"error": format!("this workspace is on agent host {host}; a request to it carries at most {MAX_RELAY_BODY} bytes (send smaller upload chunks)"), "code": "too_large"}),
        )),
        Decision::Relay { host, maps } => Some(exchange(remote, &host, &maps, "POST", path, query, body)),
    }
}

/// resolve-links' body with its `workspace` as the registered path.
fn resolve_links_body(body: &[u8]) -> Option<Vec<u8>> {
    let mut v: Value = serde_json::from_slice(body).ok()?;
    let token = v.get("workspace")?.as_str()?.trim().to_string();
    if token.starts_with('/') {
        return None;
    }
    let root = crate::workspace_msg::resolve_workspace_allowing_handle(&token)?;
    v["workspace"] = Value::String(root);
    Some(v.to_string().into_bytes())
}

/// The decoded bytes each relayed upload sub-chunk carries.
const UPLOAD_PIECE: usize = 4 << 20;

/// An upload chunk relayed as sub-chunks the link can carry (the app's
/// chunks are up to 16 MiB), each at its own offset; only the last is the
/// chunk's last. The answer is the last sub-chunk's (or the first refusal).
fn upload_chunk_split(remote: &RemoteHost, host: &str, maps: &[(PathBuf, PathBuf)], query: &str, body: &[u8]) -> Answer {
    let Ok(v) = serde_json::from_slice::<Value>(body) else {
        return exchange(remote, host, maps, "POST", "/cli/fs/upload-chunk", query, body);
    };
    let Some(bytes) = v.get("base64").and_then(Value::as_str).and_then(|b| base64::engine::general_purpose::STANDARD.decode(b).ok()) else {
        return exchange(remote, host, maps, "POST", "/cli/fs/upload-chunk", query, body);
    };
    // N5: an explicit offset (a missing one is not the start).
    let Some(offset) = v.get("offset").and_then(Value::as_u64) else {
        return Answer::json("400 Bad Request", json!({"error": "an upload chunk names its offset", "code": "bad_body"}));
    };
    let is_last = v.get("is_last").and_then(Value::as_bool).unwrap_or(false);
    let pieces: Vec<&[u8]> = if bytes.is_empty() { vec![&bytes[..]] } else { bytes.chunks(UPLOAD_PIECE).collect() };
    let n = pieces.len();
    let mut at = offset;
    let mut answer = None;
    for (i, piece) in pieces.into_iter().enumerate() {
        let mut sub = v.clone();
        sub["offset"] = json!(at);
        sub["base64"] = json!(base64::engine::general_purpose::STANDARD.encode(piece));
        sub["is_last"] = json!(is_last && i + 1 == n);
        if i > 0 {
            // The full size travels with the transfer's first chunk only.
            if let Some(o) = sub.as_object_mut() {
                o.remove("total_bytes");
            }
        }
        let a = exchange(remote, host, maps, "POST", "/cli/fs/upload-chunk", query, sub.to_string().as_bytes());
        if !a.status.starts_with('2') {
            return a;
        }
        at += piece.len() as u64;
        answer = Some(a);
    }
    answer.expect("at least one piece")
}

pub mod events {
    /// The controller relayed a request (attrs: host, path, ok).
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

/// A route path a host takes: one of the exact routes, spelled plainly
/// (no `%`, `?`, `#`, empty, `.` or `..` segments).
fn plain_route(method: &str, path: &str) -> bool {
    let plain = path.starts_with('/')
        && !path.contains(['%', '?', '#', '\\'])
        && path[1..].split('/').all(|seg| !seg.is_empty() && seg != "." && seg != "..");
    plain && relayed(method, path)
}

/// `p` resolved on this host's disk the way the kernel walks it: one
/// component at a time (`lstat`), a symlink's target spliced in where it
/// stands (an absolute one from `/`), `..` popping what is resolved so far
/// (the parent of where a link really points, never of its name), a
/// dangling link followed too (a write through it would land there).
/// `None` past 40 links (a loop).
fn resolve(p: &Path) -> Option<PathBuf> {
    use std::collections::VecDeque;
    use std::ffi::OsString;
    fn parts(p: &Path) -> Vec<OsString> {
        p.components()
            .filter_map(|c| match c {
                Component::Normal(s) => Some(s.to_os_string()),
                Component::ParentDir => Some(OsString::from("..")),
                _ => None,
            })
            .collect()
    }
    let mut todo: VecDeque<OsString> = parts(p).into();
    let mut out = PathBuf::from("/");
    let mut hops = 0u32;
    while let Some(part) = todo.pop_front() {
        if part == ".." {
            out.pop();
            continue;
        }
        let next = out.join(&part);
        match std::fs::symlink_metadata(&next) {
            Ok(md) if md.file_type().is_symlink() => {
                hops += 1;
                if hops > 40 {
                    return None;
                }
                let target = std::fs::read_link(&next).ok()?;
                if target.is_absolute() {
                    out = PathBuf::from("/");
                }
                for t in parts(&target).into_iter().rev() {
                    todo.push_front(t);
                }
            }
            _ => out = next,
        }
    }
    Some(out)
}

/// Every path a relayed request names is under this host's work root
/// (strictly: a workspace, never the root itself), after every symlink,
/// dangling ones included; a relative name is resolved against the
/// request's base path (`path`) and confined the same way. Absolute
/// strings under any key but the text keys count as paths.
fn confine(root: &Path, query: &[(String, String)], body: &Value) -> Result<(), String> {
    let base = query
        .iter()
        .find(|(k, v)| k == "path" && v.starts_with('/'))
        .map(|(_, v)| v.clone())
        .or_else(|| body.get("path").and_then(Value::as_str).filter(|v| v.starts_with('/')).map(str::to_string));
    let inside = |v: &str, p: &Path| -> Result<(), String> {
        let r = resolve(p).ok_or_else(|| format!("{v} can't be resolved"))?;
        if r == root || !r.starts_with(root) {
            return Err(format!("{v} is not in a workspace under this host's work root"));
        }
        Ok(())
    };
    let check = |key: &str, v: &str| -> Result<(), String> {
        if TEXT_KEYS.contains(&key) || v.is_empty() {
            return Ok(());
        }
        let named = NAME_KEYS.contains(&key) || PATH_KEYS.contains(&key);
        if (named || v.starts_with('/')) && has_dot_dot(v) {
            return Err(format!("{key} has '..'"));
        }
        if v.starts_with('/') {
            return inside(v, Path::new(v));
        }
        if named {
            if let Some(b) = &base {
                return inside(v, &Path::new(b).join(v));
            }
        }
        Ok(())
    };
    for (k, v) in query {
        check(k, v)?;
    }
    fn walk(key: &str, v: &Value, check: &dyn Fn(&str, &str) -> Result<(), String>) -> Result<(), String> {
        match v {
            Value::String(s) => check(key, s),
            Value::Array(a) => a.iter().try_for_each(|x| walk(key, x, check)),
            Value::Object(o) => o.iter().try_for_each(|(k, x)| if TEXT_KEYS.contains(&k.as_str()) { Ok(()) } else { walk(k, x, check) }),
            _ => Ok(()),
        }
    }
    walk("", body, &check)
}

/// N6: resolve-links' candidates, one by one: a link outside the work root
/// (absolute, `~`, or relative through a link out of the workspace) is
/// taken out and answered `linkable: false`; the rest go on. Returns the
/// body to send and the refused (index, input).
fn split_links(root: &Path, body: &Value) -> (Value, Vec<(usize, String)>) {
    let ws = body.get("workspace").and_then(Value::as_str).map(PathBuf::from);
    let paths: Vec<String> = body.get("paths").and_then(Value::as_array).map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect()).unwrap_or_default();
    let mut kept = Vec::new();
    let mut refused = Vec::new();
    for (i, p) in paths.into_iter().enumerate() {
        let candidate = if has_dot_dot(&p) {
            None
        } else if p.starts_with('/') {
            Some(PathBuf::from(&p))
        } else if p.starts_with('~') {
            None
        } else {
            ws.as_ref().map(|w| w.join(&p))
        };
        let inside = candidate.and_then(|c| resolve(&c)).is_some_and(|r| r.starts_with(root) && r != root);
        if inside {
            kept.push(Value::String(p));
        } else {
            refused.push((i, p));
        }
    }
    let mut out = body.clone();
    out["paths"] = Value::Array(kept);
    (out, refused)
}

/// resolve-links' answer with the refused candidates back in their places.
fn merge_links(answer: &str, refused: &[(usize, String)]) -> Option<String> {
    let mut v: Value = serde_json::from_str(answer).ok()?;
    let got = v.get("links")?.as_array()?.clone();
    let mut got = got.into_iter();
    let total = refused.len() + got.len();
    let links: Vec<Value> = (0..total)
        .map(|i| match refused.iter().find(|(j, _)| *j == i) {
            Some((_, input)) => json!({"input": input, "linkable": false}),
            None => got.next().unwrap_or(Value::Null),
        })
        .collect();
    v["links"] = Value::Array(links);
    Some(v.to_string())
}

/// N7: a relayed git request works on repositories inside the work root.
/// For every git path it names (`path`, `projectPath`, `worktreePath`, …),
/// the repository K2's git code (and the git CLI, from that directory)
/// would open is opened the same way, and its git dir, common dir and work
/// tree must resolve under the work root: a nested repository, a gitfile,
/// `core.worktree` in any config syntax, or a repository above the
/// workspace all fail. K2's own git commands run without hooks here
/// (`set_no_hooks`).
fn git_precheck(root: &Path, query: &[(String, String)], body: &Value) -> Result<(), String> {
    const GIT_PATH_KEYS: &[&str] = &["path", "projectPath", "project_path", "worktreePath", "worktree_path", "repo_root", "repoRoot"];
    let mut paths: Vec<String> =
        query.iter().filter(|(k, v)| GIT_PATH_KEYS.contains(&k.as_str()) && v.starts_with('/')).map(|(_, v)| v.clone()).collect();
    for k in GIT_PATH_KEYS {
        if let Some(v) = body.get(*k).and_then(Value::as_str).filter(|v| v.starts_with('/')) {
            paths.push(v.to_string());
        }
    }
    if paths.is_empty() {
        return Err("a git request names its repository's path".into());
    }
    let inside = |p: &Path| resolve(p).is_some_and(|r| r.starts_with(root) && r != root);
    for p in paths {
        // A path that doesn't exist yet (a worktree to create) opens its
        // nearest existing folder, as git would from there.
        let mut open_at = PathBuf::from(&p);
        while !open_at.exists() {
            if !open_at.pop() {
                break;
            }
        }
        let (git_dir, common, work) = match k2_core::git::repo_dirs(&open_at.to_string_lossy()) {
            Ok(d) => d,
            // No repository at all: nothing git can reach; the route answers.
            Err(_) => continue,
        };
        for (what, dir) in [("git dir", Some(git_dir)), ("common dir", Some(common)), ("work tree", work)] {
            if let Some(dir) = dir {
                if !inside(&dir) {
                    return Err(format!("the repository for {p} has its {what} {} outside this host's work root", dir.display()));
                }
            }
        }
    }
    Ok(())
}

/// The link's `http.relay`: run the request through this daemon's own
/// route (its own token, its own disk) and answer `http.reply`.
pub async fn serve(req: u64, method: &str, path: &str, query: &str, body_b64: &str) -> Frame {
    let reply = |status: &str, body: Value| Frame::HttpReply {
        req,
        status: status.to_string(),
        content_type: "application/json".into(),
        body: body.to_string(),
    };
    // Only the exact routes a controller may relay, spelled plainly.
    if !plain_route(method, path) {
        return reply("403 Forbidden", json!({"error": format!("{method} {path} is not relayed to hosts")}));
    }
    let Some(root) = super::home::work_root(|k| std::env::var(k).ok()) else {
        return reply("403 Forbidden", json!({"error": "this host has no K2_WORK_ROOT: nothing is relayed to it"}));
    };
    let body = match base64::engine::general_purpose::STANDARD.decode(body_b64) {
        Ok(b) if b.len() <= MAX_RELAY_BODY => b,
        Ok(_) => return reply("413 Payload Too Large", json!({"error": "relay body too large"})),
        Err(e) => return reply("400 Bad Request", json!({"error": format!("relay body: {e}")})),
    };
    let query_pairs = pairs(query);
    let body_json = if body.is_empty() { Value::Null } else { serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null) };
    if !body.is_empty() && body_json.is_null() {
        return reply("400 Bad Request", json!({"error": "a relayed body is JSON"}));
    }
    let mut refused_links = Vec::new();
    let (body, body_json) = if path == "/cli/fs/resolve-links" {
        let (b, refused) = split_links(&root, &body_json);
        refused_links = refused;
        (b.to_string().into_bytes(), b)
    } else {
        (body, body_json)
    };
    if let Err(e) = confine(&root, &query_pairs, &body_json) {
        return reply("403 Forbidden", json!({"error": e, "code": "outside_work_root"}));
    }
    if path.starts_with("/cli/git/") && path != "/cli/git/init" {
        if let Err(e) = git_precheck(&root, &query_pairs, &body_json) {
            return reply("403 Forbidden", json!({"error": e, "code": "outside_work_root"}));
        }
    }
    let Some((port, token)) = LOCAL.lock().clone() else {
        return reply("503 Service Unavailable", json!({"error": "this host's daemon address is not known yet"}));
    };
    let mut url = match url::Url::parse(&format!("http://127.0.0.1:{port}/")) {
        Ok(u) => u,
        Err(e) => return reply("500 Internal Server Error", json!({"error": format!("url: {e}")})),
    };
    url.set_path(path);
    if !query_pairs.is_empty() {
        url.query_pairs_mut().extend_pairs(query_pairs.iter());
    }
    // Still exactly the route asked for, after the URL's own normalisation.
    if url.path() != path {
        return reply("403 Forbidden", json!({"error": format!("{path} is not relayed to hosts")}));
    }
    // A fresh connection per request (the daemon closes each one); the token
    // in a header, never the URL; no redirects.
    let client = match reqwest::Client::builder()
        .timeout(RELAY_DEADLINE)
        .pool_max_idle_per_host(0)
        .redirect(reqwest::redirect::Policy::none())
        .build()
    {
        Ok(c) => c,
        Err(e) => return reply("500 Internal Server Error", json!({"error": format!("http client: {}", e.without_url())})),
    };
    let rb = if method.eq_ignore_ascii_case("POST") {
        client.post(url).header("Content-Type", "application/json").body(body)
    } else {
        client.get(url)
    };
    let out = match rb.bearer_auth(&token).send().await {
        Ok(resp) => {
            let status = resp.status();
            let status = format!("{} {}", status.as_u16(), status.canonical_reason().unwrap_or("")).trim().to_string();
            let ct = resp.headers().get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("application/json").to_string();
            let too_big = resp.content_length().is_some_and(|n| n as usize > MAX_RELAY_BODY);
            match if too_big { Ok(None) } else { resp.bytes().await.map(Some) } {
                Ok(Some(b)) if b.len() <= MAX_RELAY_BODY => {
                    let mut body = String::from_utf8_lossy(&b).into_owned();
                    if !refused_links.is_empty() {
                        body = merge_links(&body, &refused_links).unwrap_or(body);
                    }
                    Frame::HttpReply { req, status, content_type: ct, body }
                }
                Ok(_) => reply(
                    "413 Payload Too Large",
                    json!({"error": format!("the answer is over {MAX_RELAY_BODY} bytes, too big to relay (read the file by ranges)"), "code": "too_large"}),
                ),
                Err(e) => reply("502 Bad Gateway", json!({"error": format!("read the answer: {}", e.without_url())})),
            }
        }
        Err(e) => reply("502 Bad Gateway", json!({"error": format!("this host's daemon: {}", e.without_url())})),
    };
    // The whole frame, escaped, within what the link carries (M1).
    let out = if out.encode().len() > MAX_RELAY_FRAME {
        reply(
            "413 Payload Too Large",
            json!({"error": format!("the answer is too big for one relay frame ({MAX_RELAY_FRAME} bytes encoded)"), "code": "too_large"}),
        )
    } else {
        out
    };
    if let Frame::HttpReply { status, .. } = &out {
        k2_core::trace::emit(|| k2_core::trace::Event::new(events::HOST_FS_SERVED).attr("path", path).attr("status", status.as_str()));
    }
    out
}

#[cfg(test)]
#[path = "fs_relay_tests.rs"]
mod tests;
