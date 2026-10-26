//! P3 T1: an agent's home moves with it (FR6; design in
//! `docs/prd/progress/p3-plan.md`, "T1 in detail"; the format, store,
//! snapshot and restore are `k2_core::agent_home`).
//!
//! Controller (`K2_ROLE=controller`): the store in `<K2_HOME>/homes` and
//! its routes under `/cli/hosts/home/`. A host gets a transfer token with
//! its hello credential (T1-b: the bootstrap token, or W5's signed nonce),
//! then sends the objects the store lacks and commits the manifest. The
//! fence (I4, [`fence`]): while the agent is placed, only its placement's
//! host at its epoch; once it isn't, only the host that last ran it, at an
//! epoch no lower than the newest snapshot's.
//!
//! Host (`K2_ROLE=host`), only for agents whose cwd is under `K2_WORK_ROOT`
//! (T1-e). A workspace has the same path relative to every host's work root
//! (D2); a spawn whose cwd names it under another root runs it under this
//! one. One cwd per agent: a second agent in a cwd another placed agent
//! owns is warned about, and its home is neither saved nor restored there.
//! A new agent's cwd is created under the work root before its spawn. A
//! spawn that would start a conversation this host now holds (the
//! controller decided before the restore: `--session-id <id>`) resumes it
//! instead (`--resume <id>`).
//! - every release saves the home first: a `close` from the controller
//!   (sleep, a pool's scale-down, the v2 close) is answered after the push
//!   ([`before_close`], [`with_home`]); the SIGTERM drain pushes before its
//!   `released` (`hosts::drain`);
//! - every spawn restores the newest snapshot first ([`before_spawn`]); a
//!   restore that fails fails the spawn, loudly, rather than start the agent
//!   on a stale or empty home.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use k2_core::agent_home::restore::{self, Outcome, Target};
use k2_core::agent_home::snapshot::{self, Options, Pushed};
use k2_core::agent_home::store::{CommitError, Fence, Store};
use k2_core::agent_home::{Committed, Latest, Remote, MAX_BYTES_VAR, MAX_OBJECT_BYTES};
use k2_core::log_debug;
use parking_lot::Mutex;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::proto::Frame;
use crate::agent_host::remote::RemoteHost;
use crate::routes::http;

/// Host: homes are synced only for agents whose cwd is under this dir.
pub const WORK_ROOT_VAR: &str = "K2_WORK_ROOT";

pub const SESSION_PATH: &str = "/cli/hosts/home/session";
pub const HAVE_PATH: &str = "/cli/hosts/home/have";
pub const PUT_PATH: &str = "/cli/hosts/home/put";
pub const FETCH_PATH: &str = "/cli/hosts/home/fetch";
pub const COMMIT_PATH: &str = "/cli/hosts/home/commit";
pub const LATEST_PATH: &str = "/cli/hosts/home/latest";
/// Owner token: every agent's newest snapshot.
pub const STATUS_PATH: &str = "/cli/hosts/home/status";

/// How long a transfer token lasts.
const TOKEN_TTL: Duration = Duration::from_secs(600);

pub mod events {
    pub const HOST_HOME_SAVED: &str = "host.home_saved";
    pub const HOST_HOME_SAVE_FAILED: &str = "host.home_save_failed";
    pub const HOST_HOME_RESTORED: &str = "host.home_restored";
    pub const HOST_HOME_RESTORE_FAILED: &str = "host.home_restore_failed";
    /// A spawn's `--session-id` became `--resume`: the home here holds it.
    pub const HOST_HOME_RESUME: &str = "host.home_resume";
    pub const CTL_HOME_COMMIT: &str = "ctl.home_commit";
    pub const CTL_HOME_REFUSED: &str = "ctl.home_refused";
}

// ── controller ─────────────────────────────────────────────────────────

/// The I4 fence over `remote`'s placements (T1-c).
pub fn fence(remote: &RemoteHost, f: &Fence) -> Result<(), String> {
    if let Some(p) = remote.placement(f.agent) {
        if p.host_id == f.host && p.epoch.0 == f.epoch {
            return Ok(());
        }
        return Err(format!(
            "agent {} is placed on {} at epoch {}; {} at epoch {} may not write its home",
            f.agent, p.host_id, p.epoch.0, f.host, f.epoch
        ));
    }
    match remote.last_host_of(f.agent) {
        Some(last) if last == f.host => {}
        last => {
            return Err(format!(
                "{} is not the host that last ran agent {} ({})",
                f.host,
                f.agent,
                last.as_deref().unwrap_or("none known")
            ))
        }
    }
    match f.newest {
        Some(n) if f.epoch < n.epoch => {
            Err(format!("epoch {} is older than agent {}'s newest snapshot (seq {} at epoch {})", f.epoch, f.agent, n.seq, n.epoch))
        }
        _ => Ok(()),
    }
}

/// The controller's side: the store, the placements it fences on, and the
/// transfer tokens it gave out.
pub struct Server {
    store: Store,
    remote: &'static RemoteHost,
    bootstrap: String,
    tokens: Mutex<HashMap<String, (String, Instant)>>,
}

/// An answer: status line, content type, body.
pub type Reply = (&'static str, &'static str, Vec<u8>);

fn json_reply(status: &'static str, v: Value) -> Reply {
    (status, "application/json", v.to_string().into_bytes())
}

fn error(status: &'static str, msg: impl std::fmt::Display) -> Reply {
    json_reply(status, json!({"error": msg.to_string()}))
}

impl Server {
    /// The store in `root`, fenced on `remote`; `bootstrap` is the
    /// controller's host token (token mode, and the fallback of `admit_hello`).
    pub fn new(root: &Path, remote: &'static RemoteHost, bootstrap: &str) -> Result<Server, String> {
        Ok(Server { store: Store::open(root)?, remote, bootstrap: bootstrap.to_string(), tokens: Mutex::default() })
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    fn issue(&self, host_id: &str) -> Result<String, String> {
        let mut raw = [0u8; 32];
        getrandom::getrandom(&mut raw).map_err(|e| format!("random: {e}"))?;
        let t: String = raw.iter().map(|b| format!("{b:02x}")).collect();
        let now = Instant::now();
        let mut tokens = self.tokens.lock();
        tokens.retain(|_, (_, at)| now.duration_since(*at) < TOKEN_TTL);
        tokens.insert(t.clone(), (host_id.to_string(), now));
        Ok(t)
    }

    fn host_for(&self, token: &str) -> Option<String> {
        let tokens = self.tokens.lock();
        let (host, at) = tokens.get(token)?;
        (at.elapsed() < TOKEN_TTL).then(|| host.clone())
    }

    /// One request to `path` (the routes above but status), for a body
    /// already read.
    pub fn answer(&self, path: &str, query: &str, body: &[u8]) -> Reply {
        let params = http::parse_params(path, query);
        if path == SESSION_PATH {
            let Ok(v) = serde_json::from_slice::<Value>(body) else { return error("400 Bad Request", "expected {host_name, token}") };
            let host = v.get("host_name").and_then(Value::as_str).unwrap_or("");
            let presented = v.get("token").and_then(Value::as_str).unwrap_or("");
            if host.trim().is_empty() {
                return error("400 Bad Request", "host_name is empty");
            }
            if let Err(r) = super::enroll::admit_hello(self.remote, &self.bootstrap, presented, host) {
                return json_reply("403 Forbidden", json!({"error": r.message, "code": r.code}));
            }
            return match self.issue(host) {
                Ok(t) => json_reply("200 OK", json!({"ht": t, "host": host, "ttl_s": TOKEN_TTL.as_secs()})),
                Err(e) => error("500 Internal Server Error", e),
            };
        }
        let Some(host) = params.get("ht").and_then(|t| self.host_for(t)) else {
            return error("403 Forbidden", "missing or expired home transfer token (POST /cli/hosts/home/session)");
        };
        let sha = params.get("sha").map(String::as_str).unwrap_or("");
        let agent = params.get("agent").map(String::as_str).unwrap_or("");
        match path {
            HAVE_PATH => {
                let shas: Vec<String> = serde_json::from_slice::<Value>(body)
                    .ok()
                    .and_then(|v| serde_json::from_value(v.get("objects")?.clone()).ok())
                    .unwrap_or_default();
                json_reply("200 OK", json!({"missing": self.store.missing(&shas)}))
            }
            PUT_PATH => match self.store.put(sha, body) {
                Ok(new) => json_reply("201 Created", json!({"stored": new})),
                Err(e) => error("422 Unprocessable Entity", e),
            },
            FETCH_PATH => match self.store.get(sha) {
                Ok(bytes) => ("200 OK", "application/octet-stream", bytes),
                Err(e) => error("404 Not Found", e),
            },
            COMMIT_PATH => {
                let Some(epoch) = params.get("epoch").and_then(|e| e.parse::<u64>().ok()) else {
                    return error("400 Bad Request", "epoch must be a number");
                };
                let r = self.store.commit(agent, &host, epoch, body, &|f| fence(self.remote, f));
                match r {
                    Ok(c) => {
                        log_debug!("[hosts/home] {agent}: snapshot {} from {host} at epoch {epoch}", c.seq);
                        k2_core::trace::emit(|| {
                            k2_core::trace::Event::new(events::CTL_HOME_COMMIT)
                                .agent(agent)
                                .attr("host", host.as_str())
                                .attr("epoch", epoch)
                                .attr("seq", c.seq)
                        });
                        json_reply("201 Created", json!({"seq": c.seq, "manifest_sha": c.manifest_sha}))
                    }
                    Err(e) => {
                        log_debug!("[hosts/home] {agent}: commit from {host} at epoch {epoch}: {e}");
                        k2_core::trace::emit(|| {
                            k2_core::trace::Event::new(events::CTL_HOME_REFUSED)
                                .agent(agent)
                                .attr("host", host.as_str())
                                .attr("epoch", epoch)
                                .attr("why", e.to_string())
                        });
                        let status = match e {
                            CommitError::Refused(_) => "409 Conflict",
                            CommitError::Invalid(_) => "422 Unprocessable Entity",
                            CommitError::Internal(_) => "500 Internal Server Error",
                        };
                        error(status, e)
                    }
                }
            }
            LATEST_PATH => {
                // Only the host the agent is placed on reads its home.
                match self.remote.placement(agent) {
                    Some(p) if p.host_id == host => {}
                    _ => return error("403 Forbidden", format!("agent {agent} is not placed on {host}")),
                }
                match self.store.latest(agent) {
                    Ok(Some(l)) => json_reply("200 OK", serde_json::to_value(l).unwrap_or(Value::Null)),
                    Ok(None) => error("404 Not Found", format!("agent {agent} has no snapshot")),
                    Err(e) => error("500 Internal Server Error", e),
                }
            }
            _ => error("404 Not Found", format!("no such home route {path}")),
        }
    }

    pub fn status(&self) -> Value {
        match self.store.all_latest() {
            Ok(all) => json!({"snapshots": all.into_iter().map(|(agent, l)| {
                let mut v = serde_json::to_value(l).unwrap_or(Value::Null);
                v["agent"] = json!(agent);
                v
            }).collect::<Vec<_>>()}),
            Err(e) => json!({"error": e}),
        }
    }
}

static SERVER: OnceLock<Result<Server, String>> = OnceLock::new();

/// This controller's server; `None` on any other role.
fn server() -> Option<&'static Result<Server, String>> {
    let cfg = super::controller()?;
    Some(SERVER.get_or_init(|| {
        let root = k2_core::paths::k2_home().join("homes");
        Server::new(&root, crate::agent_host::remote::global(), &cfg.token)
    }))
}

/// The newest snapshot of `agent` on this controller (the pool checks it
/// before a scale-down; T2 phase B's `Need.home_seq`).
pub fn latest(agent: &str) -> Option<Latest> {
    server()?.as_ref().ok()?.store.latest(agent).ok().flatten()
}

fn is_post(path: &str) -> bool {
    matches!(path, SESSION_PATH | HAVE_PATH | PUT_PATH | COMMIT_PATH)
}

async fn write_reply(stream: &mut TcpStream, (status, ct, body): Reply) {
    let head = format!("HTTP/1.1 {status}\r\nContent-Type: {ct}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(&body).await;
}

/// `gate::route`'s hook. `true` = answered.
pub async fn route(stream: &mut TcpStream, buf: &mut [u8], path: &str, query: &str, owner_token: &str) -> bool {
    if !path.starts_with("/cli/hosts/home/") {
        return false;
    }
    let Some(s) = server() else { return false };
    let s = match s {
        Ok(s) => s,
        Err(e) => {
            let _ = stream.read(buf).await;
            write_reply(stream, error("500 Internal Server Error", format!("home store: {e}"))).await;
            return true;
        }
    };
    serve(stream, buf, s, path, query, owner_token).await;
    true
}

/// Read the request's body and answer it (also the tests' loopback server).
pub async fn serve(stream: &mut TcpStream, buf: &mut [u8], s: &'static Server, path: &str, query: &str, owner_token: &str) {
    let body = if is_post(path) {
        match http::read_post_body_capped(&mut *stream, buf, MAX_OBJECT_BYTES as usize + (1 << 20)).await {
            Ok(b) => b,
            Err(refused) => {
                let body = json!({"error": "a home object over the per-file cap"}).to_string();
                http::refuse_body_too_large(stream, &refused, "413 Payload Too Large", "application/json", &body).await;
                return;
            }
        }
    } else {
        let _ = stream.read(buf).await;
        Vec::new()
    };
    if path == STATUS_PATH {
        let r = if http::token_ok(query, owner_token) {
            json_reply("200 OK", s.status())
        } else {
            error("403 Forbidden", "invalid or missing token")
        };
        return write_reply(stream, r).await;
    }
    let (path, query) = (path.to_string(), query.to_string());
    let r = tokio::task::spawn_blocking(move || s.answer(&path, &query, &body))
        .await
        .unwrap_or_else(|e| error("500 Internal Server Error", format!("home: {e}")));
    write_reply(stream, r).await;
}

// ── host ───────────────────────────────────────────────────────────────

/// `K2_WORK_ROOT`, canonical; `None` = home sync is off on this host.
pub fn work_root(get: impl Fn(&str) -> Option<String>) -> Option<PathBuf> {
    let raw = get(WORK_ROOT_VAR).filter(|v| !v.trim().is_empty())?;
    let p = PathBuf::from(raw.trim());
    Some(std::fs::canonicalize(&p).unwrap_or(p))
}

fn canon(p: &Path) -> PathBuf {
    std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
}

/// Is `cwd` a workspace under `root` (not the root itself)?
pub fn in_scope(root: &Path, cwd: &Path) -> bool {
    let cwd = canon(cwd);
    cwd.starts_with(root) && cwd != root
}

/// Which agent owns each synced cwd on this host (the first placed there).
fn claims() -> &'static Mutex<HashMap<PathBuf, String>> {
    static C: OnceLock<Mutex<HashMap<PathBuf, String>>> = OnceLock::new();
    C.get_or_init(Mutex::default)
}

/// One cwd per agent: `agent` may sync `cwd` unless another agent that is
/// still `placed` owns it; the first claim wins.
pub(crate) fn claim_in(claims: &mut HashMap<PathBuf, String>, cwd: &Path, agent: &str, placed: &dyn Fn(&str) -> bool) -> Result<(), String> {
    match claims.get(cwd) {
        Some(owner) if owner != agent && placed(owner) => Err(format!(
            "{} is agent {owner}'s workspace; one cwd per agent, so {agent}'s home is not synced there",
            cwd.display()
        )),
        _ => {
            claims.insert(cwd.to_path_buf(), agent.to_string());
            Ok(())
        }
    }
}

/// [`claim_in`] on this host, with a warning when the cwd is shared.
fn claim(agent: &str, cwd: &Path) -> Result<(), String> {
    let placed = |a: &str| crate::agent_host::host().find_agent(a).is_some();
    let r = claim_in(&mut claims().lock(), &canon(cwd), agent, &placed);
    if let Err(why) = &r {
        log_debug!("[hosts/home] WARNING: {why}");
    }
    r
}

/// Where a spawn asked for `requested` runs on this host: the agent's
/// workspace here when `requested` names it under another host's work root
/// (the same path relative to the root, D2).
pub(crate) fn map_cwd(requested: &Path, here: Option<(PathBuf, Option<String>)>) -> Option<PathBuf> {
    let (local, rel) = here?;
    let rel = rel?;
    (requested != local && requested.ends_with(&rel)).then_some(local)
}

/// `--session-id <id>` → `--resume <id>` in `args` when `holds(id)`: the
/// restored home has that conversation, so starting it again would fail or
/// fork it. The id, when changed.
pub(crate) fn resume_held(args: &mut [Value], holds: &dyn Fn(&str) -> bool) -> Option<String> {
    let i = args.iter().position(|a| a.as_str() == Some("--session-id"))?;
    let id = args.get(i + 1)?.as_str()?.to_string();
    if !holds(&id) {
        return None;
    }
    args[i] = json!("--resume");
    Some(id)
}

/// Create a new agent's cwd before its spawn, under the work root only.
/// `Ok(true)` when it was created.
pub(crate) fn prepare_cwd(root: &Path, cwd: &Path) -> Result<bool, String> {
    if cwd.exists() || !cwd.is_absolute() || !cwd.starts_with(root) || cwd == root {
        return Ok(false);
    }
    std::fs::create_dir_all(cwd).map_err(|e| format!("create the agent's cwd {}: {e}", cwd.display()))?;
    Ok(true)
}

fn max_bytes(get: impl Fn(&str) -> Option<String>) -> u64 {
    get(MAX_BYTES_VAR).and_then(|v| v.trim().parse().ok()).unwrap_or(k2_core::agent_home::DEFAULT_MAX_BYTES)
}

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok()
}

/// The controller's store over HTTP, for one host.
pub struct HttpRemote {
    base: String,
    ht: String,
    client: reqwest::blocking::Client,
}

fn check(resp: reqwest::blocking::Response, what: &str) -> Result<reqwest::blocking::Response, String> {
    if resp.status().is_success() {
        return Ok(resp);
    }
    let status = resp.status().as_u16();
    let text = resp.text().unwrap_or_default();
    let msg = serde_json::from_str::<Value>(&text)
        .ok()
        .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_string))
        .unwrap_or(text);
    Err(format!("{what}: HTTP {status}: {msg}"))
}

/// A JSON body (reqwest's `json` feature isn't on in this build).
fn json_of<T: serde::de::DeserializeOwned>(resp: reqwest::blocking::Response, what: &str) -> Result<T, String> {
    let text = resp.text().map_err(|e| format!("{what}: {e}"))?;
    serde_json::from_str(&text).map_err(|e| format!("{what}: {e}"))
}

impl HttpRemote {
    /// Get a transfer token from the controller at `base` with this host's
    /// hello credential. Blocking: call it off the async workers.
    pub fn connect(base: &str, host_name: &str, hello_token: &str) -> Result<HttpRemote, String> {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(300))
            .build()
            .map_err(|e| format!("http client: {e}"))?;
        let resp = client
            .post(format!("{base}{SESSION_PATH}"))
            .body(json!({"host_name": host_name, "token": hello_token}).to_string())
            .send()
            .map_err(|e| format!("home session: {e}"))?;
        let v: Value = json_of(check(resp, "home session")?, "home session")?;
        let ht = v.get("ht").and_then(Value::as_str).ok_or("home session: no token in the answer")?.to_string();
        Ok(HttpRemote { base: base.to_string(), ht, client })
    }

    fn url(&self, path: &str, extra: &[(&str, &str)]) -> String {
        let mut q = vec![("ht", self.ht.as_str())];
        q.extend_from_slice(extra);
        let q: Vec<String> = q.iter().map(|(k, v)| format!("{k}={}", url::form_urlencoded::byte_serialize(v.as_bytes()).collect::<String>())).collect();
        format!("{}{path}?{}", self.base, q.join("&"))
    }
}

impl Remote for HttpRemote {
    fn missing(&self, shas: &[String]) -> Result<Vec<String>, String> {
        let resp = self.client.post(self.url(HAVE_PATH, &[])).body(json!({"objects": shas}).to_string()).send();
        let v: Value = json_of(check(resp.map_err(|e| format!("have: {e}"))?, "have")?, "have")?;
        serde_json::from_value(v.get("missing").cloned().unwrap_or(Value::Null)).map_err(|e| format!("have: {e}"))
    }

    fn put_object(&self, sha: &str, bytes: &[u8]) -> Result<(), String> {
        let resp = self.client.post(self.url(PUT_PATH, &[("sha", sha)])).body(bytes.to_vec()).send();
        check(resp.map_err(|e| format!("put {sha}: {e}"))?, "put").map(|_| ())
    }

    fn get_object(&self, sha: &str) -> Result<Vec<u8>, String> {
        let resp = self.client.get(self.url(FETCH_PATH, &[("sha", sha)])).send();
        let resp = check(resp.map_err(|e| format!("fetch {sha}: {e}"))?, "fetch")?;
        resp.bytes().map(|b| b.to_vec()).map_err(|e| format!("fetch {sha}: {e}"))
    }

    fn commit(&self, agent: &str, epoch: u64, manifest: &[u8]) -> Result<Committed, String> {
        let e = epoch.to_string();
        let resp = self.client.post(self.url(COMMIT_PATH, &[("agent", agent), ("epoch", &e)])).body(manifest.to_vec()).send();
        let resp = check(resp.map_err(|e| format!("commit: {e}"))?, "commit")?;
        json_of(resp, "commit")
    }

    fn latest(&self, agent: &str) -> Result<Option<Latest>, String> {
        let resp = self.client.get(self.url(LATEST_PATH, &[("agent", agent)])).send().map_err(|e| format!("latest: {e}"))?;
        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        json_of(check(resp, "latest")?, "latest").map(Some)
    }
}

/// Where this host keeps its homes, and its user's home.
fn host_target(root: PathBuf) -> Result<Target, String> {
    let home = dirs::home_dir().ok_or("this host has no home directory")?;
    Ok(Target { home, state: k2_core::paths::k2_home().join("homes"), work_root: Some(root) })
}

/// One transfer at a time on a host.
fn transfers() -> &'static tokio::sync::Mutex<()> {
    static T: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    T.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// A remote for this host: its hello credential, then a transfer token.
async fn connect() -> Result<HttpRemote, String> {
    let cfg = super::host_config(env)?;
    let token = super::enroll::hello_token(&cfg).await?;
    let base = super::enroll::http_base(&cfg.attach_url)?;
    tokio::task::spawn_blocking(move || HttpRemote::connect(&base, &cfg.host_name, &token))
        .await
        .map_err(|e| format!("home session: {e}"))?
}

fn this_host() -> String {
    super::host_config(env).map(|c| c.host_name).unwrap_or_default()
}

/// Save `agent`'s home as a snapshot at placement `epoch`, before it is
/// released. `Ok(None)`: home sync is off here, or the agent isn't in a
/// workspace under `K2_WORK_ROOT`.
pub async fn save(agent: &str, epoch: u64) -> Result<Option<Pushed>, String> {
    let Some(root) = work_root(env) else { return Ok(None) };
    let Some(cwd) = crate::agent_host::host().find_agent(agent).and_then(|s| s.cwd_path().map(|p| p.to_path_buf())) else {
        log_debug!("[hosts/home] {agent}: no session here, nothing to save");
        return Ok(None);
    };
    if !in_scope(&root, &cwd) {
        log_debug!("[hosts/home] {agent}: {} is not under {WORK_ROOT_VAR}={}; its home stays here", cwd.display(), root.display());
        return Ok(None);
    }
    claim(agent, &cwd)?;
    let _one = transfers().lock().await;
    let host = this_host();
    let result = async {
        let remote = connect().await?;
        let t = host_target(root)?;
        let (agent, host) = (agent.to_string(), host.clone());
        tokio::task::spawn_blocking(move || {
            let parent = restore::held(&t.state, &agent).map(|h| h.seq);
            let mut opts = Options::new(&t.home);
            opts.max_bytes = max_bytes(env);
            opts.work_root = t.work_root.clone();
            let p = snapshot::push(&remote, &agent, epoch, &host, &cwd.to_string_lossy(), parent, &opts)?;
            restore::mark(&t.state, &agent, p.seq, &p.manifest_sha, &p.manifest.encode())?;
            Ok::<_, String>(p)
        })
        .await
        .map_err(|e| format!("home save: {e}"))?
    }
    .await;
    match &result {
        Ok(p) => {
            log_debug!(
                "[hosts/home] {agent}: saved snapshot {} at epoch {epoch} ({} files, {} bytes; sent {} objects, {} bytes)",
                p.seq,
                p.files,
                p.bytes,
                p.sent_objects,
                p.sent_bytes
            );
            k2_core::trace::emit(|| {
                k2_core::trace::Event::new(events::HOST_HOME_SAVED)
                    .agent(agent)
                    .attr("epoch", epoch)
                    .attr("seq", p.seq)
                    .attr("files", p.files)
                    .attr("bytes", p.bytes)
                    .attr("sent_objects", p.sent_objects)
                    .attr("sent_bytes", p.sent_bytes)
            });
        }
        Err(e) => {
            log_debug!("[hosts/home] {agent}: saving its home failed: {e}");
            k2_core::trace::emit(|| {
                k2_core::trace::Event::new(events::HOST_HOME_SAVE_FAILED).agent(agent).attr("epoch", epoch).attr("error", e.as_str())
            });
        }
    }
    result.map(Some)
}

/// The link's `close` hook: save first. What the reply's `home` says.
pub async fn before_close(agent: &str, epoch: u64) -> Value {
    match save(agent, epoch).await {
        Ok(Some(p)) => json!({"seq": p.seq, "files": p.files, "bytes": p.bytes}),
        Ok(None) => Value::Null,
        Err(e) => json!({"error": e}),
    }
}

/// `body` (a JSON object) with `home` added, unless it is null.
pub fn with_home(body: String, home: Value) -> String {
    if home.is_null() {
        return body;
    }
    match serde_json::from_str::<Value>(&body) {
        Ok(Value::Object(mut m)) => {
            m.insert("home".into(), home);
            Value::Object(m).to_string()
        }
        _ => body,
    }
}

/// The link's spawn hook: restore `agent`'s newest snapshot first, run it
/// where its workspace is on this host (`body.cwd`), and create a new
/// agent's cwd. `Some` is the reply that fails the spawn.
pub async fn before_spawn(req: u64, agent: &str, body: &mut Value) -> Option<Frame> {
    let root = work_root(env)?;
    let t = host_target(root.clone()).ok()?;
    let local = |t: &Target, requested: PathBuf| map_cwd(&requested, restore::local_project(t, agent)).unwrap_or(requested);
    let requested = body.get("cwd").and_then(Value::as_str).map(PathBuf::from);
    let shared = requested.clone().map(|r| local(&t, r)).filter(|c| in_scope(&root, c)).map(|c| claim(agent, &c)).and_then(Result::err);
    if shared.is_none() {
        if let Some(reply) = restore_first(req, agent, root.clone()).await {
            return Some(reply);
        }
    }
    let cwd = requested.map(|r| local(&t, r))?;
    if body.get("cwd").and_then(Value::as_str) != Some(&*cwd.to_string_lossy()) {
        log_debug!("[hosts/home] {agent}: runs in {} (its workspace on this host)", cwd.display());
        body["cwd"] = json!(cwd.to_string_lossy());
        let _ = claim(agent, &cwd);
    }
    match prepare_cwd(&root, &cwd) {
        Ok(true) => log_debug!("[hosts/home] {agent}: created its cwd {}", cwd.display()),
        Ok(false) => {}
        Err(e) => {
            return Some(Frame::Reply { req, status: "500 Internal Server Error".into(), body: json!({"error": e}).to_string() })
        }
    }
    if shared.is_none() && in_scope(&root, &cwd) {
        let project = cwd.to_string_lossy().into_owned();
        let holds = |id: &str| k2_core::sidecar::conversation_exists("claude", id, &project);
        if let Some(id) = body.get_mut("args").and_then(Value::as_array_mut).and_then(|a| resume_held(a, &holds)) {
            log_debug!("[hosts/home] {agent}: this host holds conversation {id}: --resume, not --session-id");
            k2_core::trace::emit(|| k2_core::trace::Event::new(events::HOST_HOME_RESUME).agent(agent).attr("session", id.as_str()));
        }
    }
    None
}

async fn restore_first(req: u64, agent: &str, root: PathBuf) -> Option<Frame> {
    let _one = transfers().lock().await;
    let result = async {
        let remote = connect().await?;
        let t = host_target(root)?;
        let agent = agent.to_string();
        tokio::task::spawn_blocking(move || restore::restore(&remote, &agent, &t)).await.map_err(|e| format!("home restore: {e}"))?
    }
    .await;
    match result {
        Ok(Outcome::NoSnapshot) => None,
        Ok(Outcome::Current { seq }) => {
            log_debug!("[hosts/home] {agent}: this host holds its newest snapshot ({seq})");
            None
        }
        Ok(Outcome::Restored { seq, fetched, fetched_bytes, kept, deleted }) => {
            log_debug!("[hosts/home] {agent}: restored snapshot {seq} ({fetched} objects, {fetched_bytes} bytes fetched; {kept} kept, {deleted} deleted)");
            k2_core::trace::emit(|| {
                k2_core::trace::Event::new(events::HOST_HOME_RESTORED)
                    .agent(agent)
                    .attr("seq", seq)
                    .attr("fetched", fetched)
                    .attr("fetched_bytes", fetched_bytes)
                    .attr("kept", kept)
                    .attr("deleted", deleted)
            });
            None
        }
        Err(e) => {
            log_debug!("[hosts/home] {agent}: restoring its home failed: {e}");
            k2_core::trace::emit(|| k2_core::trace::Event::new(events::HOST_HOME_RESTORE_FAILED).agent(agent).attr("error", e.as_str()));
            Some(Frame::Reply {
                req,
                status: "500 Internal Server Error".into(),
                body: json!({"error": format!("home restore failed, the agent was not started: {e}")}).to_string(),
            })
        }
    }
}

#[cfg(test)]
#[path = "home_tests.rs"]
mod tests;
