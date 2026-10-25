//! Signed host enrollment (PRD P2 "k2-host: k2-node's enrollment, signed
//! channel"; FR2). Opt-in with `K2_HOST_AUTH=enroll` on both ends;
//! `K2_HOST_AUTH=token` (the default) keeps the shared bootstrap token.
//!
//! - A host has a P-256 key ([`load_or_create_key`], k2-node's node-key
//!   rule: made once, 0600, under the K2 home). It enrolls once with a one-time token the
//!   owner mints on the controller: `POST /cli/hosts/enroll` carries the
//!   token, the host name, the public key and a signature over the
//!   token's hash, the name and the key's fingerprint. The controller
//!   stores the key and spends the token.
//! - Every link then fetches a fresh nonce (`GET /cli/hosts/challenge`,
//!   single use, 60 s) and signs it with its name and fingerprint. The
//!   proof travels in `hello.token` as `k2host1.<fp>.<nonce>.<sig>`, so
//!   the wire protocol is unchanged.
//! - The controller refuses an unknown or revoked key, a name other than
//!   the enrolled one, a spent or stale nonce, a bad signature, and in
//!   enroll mode the bootstrap token itself.
//!
//! The sequence, the env names and the routes are in
//! `docs/prd/progress/host-enroll.md`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use k2_core::log_debug;
use k2_core::compute::proto::crypto::{self, SigningKey};
use parking_lot::{Mutex, ReentrantMutex};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

use super::proto::{Frame, ATTACH_PATH};
use super::HostConfig;
use crate::agent_host::remote::RemoteHost;
use crate::routes::http;

pub type Db = Arc<ReentrantMutex<Connection>>;

/// Both roles: `token` (default) or `enroll`.
pub const AUTH_VAR: &str = "K2_HOST_AUTH";
/// Host: its private key (default `<K2 home>/host-key.pem`, made 0600).
pub const KEY_FILE_VAR: &str = "K2_HOST_KEY_FILE";

/// Public: a fresh nonce for the next `hello`.
pub const CHALLENGE_PATH: &str = "/cli/hosts/challenge";
/// Public (the one-time token is the credential): enroll a key.
pub const ENROLL_PATH: &str = "/cli/hosts/enroll";
/// Owner: mint a one-time enrollment token.
pub const TOKENS_PATH: &str = "/cli/hosts/enroll-tokens";
/// Owner: the enrolled keys.
pub const KEYS_PATH: &str = "/cli/hosts/keys";
/// Owner: revoke a key (by host name or fingerprint).
pub const REVOKE_PATH: &str = "/cli/hosts/keys/revoke";

const DOMAIN_ENROLL: &str = "k2-host-enroll-v1";
const DOMAIN_HELLO: &str = "k2-host-hello-v1";
/// `hello.token` of a signed hello starts with this.
pub const SIGNED_PREFIX: &str = "k2host1.";

/// How long a nonce stays good, and how many may be outstanding.
pub const NONCE_TTL_MS: i64 = 60_000;
const NONCE_CAP: usize = 1024;
/// A minted token's life unless the owner asks otherwise, and the most it may ask.
pub const TOKEN_TTL_MS: i64 = 60 * 60 * 1000;
const TOKEN_TTL_MAX_MS: i64 = 7 * 24 * 60 * 60 * 1000;

pub mod events {
    pub const HOST_ENROLLED: &str = "host.enrolled";
    pub const CTL_HOST_ENROLLED: &str = "ctl.host_enrolled";
    pub const CTL_HOST_ADMITTED: &str = "ctl.host_admitted";
    pub const CTL_HOST_REFUSED: &str = "ctl.host_refused";
    pub const CTL_HOST_REVOKED: &str = "ctl.host_revoked";
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// The shared bootstrap token (dist-link's first cut).
    Token,
    /// Enrolled keys and signed hellos only.
    Enroll,
}

/// `K2_HOST_AUTH` through `get`. Unset or empty is `token`.
pub fn auth_mode(get: impl Fn(&str) -> Option<String>) -> Result<Mode, String> {
    match get(AUTH_VAR).as_deref().map(str::trim) {
        None | Some("") | Some("token") => Ok(Mode::Token),
        Some("enroll") => Ok(Mode::Enroll),
        Some(other) => Err(format!("{AUTH_VAR}={other:?} is not a host auth mode; allowed values: token, enroll")),
    }
}

fn env(var: &str) -> Option<String> {
    std::env::var(var).ok()
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// Why a host is turned away: the `refused` frame's code and message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub code: &'static str,
    pub message: String,
}

fn refusal(code: &'static str, message: impl Into<String>) -> Refusal {
    Refusal { code, message: message.into() }
}

fn short(fp: &str) -> &str {
    fp.get(..16).unwrap_or(fp)
}

// ── signatures (both ends) ─────────────────────────────────────────────

fn token_hash(token: &str) -> String {
    crypto::sha256_hex(token.as_bytes())
}

/// The host's proof that it holds the key it enrolls.
pub fn sign_enroll(key: &SigningKey, token: &str, host_name: &str) -> Result<String, String> {
    let fp = key.fingerprint();
    key.sign_domain(DOMAIN_ENROLL, &[token_hash(token).as_bytes(), host_name.as_bytes(), fp.as_bytes()])
}

/// The `hello.token` of a signed hello.
pub fn signed_hello_token(key: &SigningKey, nonce: &str, host_name: &str) -> Result<String, String> {
    let fp = key.fingerprint();
    let sig = key.sign_domain(DOMAIN_HELLO, &[nonce.as_bytes(), host_name.as_bytes(), fp.as_bytes()])?;
    Ok(format!("{SIGNED_PREFIX}{fp}.{nonce}.{sig}"))
}

/// `(fingerprint, nonce, signature)` of a signed hello token. Base64 and
/// hex never hold a `.`.
fn parse_signed(token: &str) -> Option<(&str, &str, &str)> {
    let mut parts = token.strip_prefix(SIGNED_PREFIX)?.split('.');
    let (fp, nonce, sig) = (parts.next()?, parts.next()?, parts.next()?);
    (parts.next().is_none() && fp.len() == 64 && !nonce.is_empty() && !sig.is_empty()).then_some((fp, nonce, sig))
}

// ── controller ─────────────────────────────────────────────────────────

/// One enrolled key, as `GET /cli/hosts/keys` lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HostKey {
    pub fingerprint: String,
    pub host_name: String,
    pub labels: Vec<String>,
    pub enrolled_ms: i64,
    pub revoked_ms: Option<i64>,
    pub revoked_reason: Option<String>,
}

/// `POST /cli/hosts/enroll`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrollRequest {
    pub token: String,
    pub host_name: String,
    pub public_key_pem: String,
    #[serde(default)]
    pub labels: Vec<String>,
    pub sig: String,
}

/// A minted one-time token. Only its hash is stored.
#[derive(Debug, Clone, Serialize)]
pub struct Minted {
    pub token: String,
    pub expires_ms: i64,
}

pub fn ensure_schema(c: &Connection) -> rusqlite::Result<()> {
    c.execute_batch(
        "CREATE TABLE IF NOT EXISTS host_enroll_tokens (
            token_sha TEXT PRIMARY KEY,
            created_ms INTEGER NOT NULL,
            expires_ms INTEGER NOT NULL,
            used_ms INTEGER,
            used_by TEXT
        );
        CREATE TABLE IF NOT EXISTS host_keys (
            fingerprint TEXT PRIMARY KEY,
            host_name TEXT NOT NULL,
            public_key_pem TEXT NOT NULL,
            labels TEXT NOT NULL DEFAULT '[]',
            enrolled_ms INTEGER NOT NULL,
            revoked_ms INTEGER,
            revoked_reason TEXT
        );
        CREATE INDEX IF NOT EXISTS host_keys_name ON host_keys(host_name);",
    )
}

fn row_key(r: &rusqlite::Row<'_>) -> rusqlite::Result<(HostKey, String)> {
    let labels: String = r.get(3)?;
    Ok((
        HostKey {
            fingerprint: r.get(0)?,
            host_name: r.get(1)?,
            labels: serde_json::from_str(&labels).unwrap_or_default(),
            enrolled_ms: r.get(4)?,
            revoked_ms: r.get(5)?,
            revoked_reason: r.get(6)?,
        },
        r.get(2)?,
    ))
}

const KEY_COLS: &str = "fingerprint, host_name, public_key_pem, labels, enrolled_ms, revoked_ms, revoked_reason";

/// The controller's enrollment state: keys and tokens in SQLite, nonces
/// in memory (a restart only voids nonces nobody used yet).
pub struct Registry {
    db: Db,
    mode: Mode,
    nonces: Mutex<HashMap<String, i64>>,
}

fn db_err(e: rusqlite::Error) -> String {
    format!("host enrollment database: {e}")
}

impl Registry {
    pub fn new(db: Db, mode: Mode) -> Result<Arc<Self>, String> {
        ensure_schema(&db.lock()).map_err(db_err)?;
        Ok(Arc::new(Registry { db, mode, nonces: Mutex::default() }))
    }

    pub fn mode(&self) -> Mode {
        self.mode
    }

    pub fn mint_token(&self, now: i64, ttl_ms: i64) -> Result<Minted, String> {
        let token = format!("k2e_{}", crypto::hex(&crypto::random_bytes(16)?));
        let expires_ms = now + ttl_ms.clamp(1, TOKEN_TTL_MAX_MS);
        self.db
            .lock()
            .execute(
                "INSERT INTO host_enroll_tokens (token_sha, created_ms, expires_ms) VALUES (?1, ?2, ?3)",
                params![token_hash(&token), now, expires_ms],
            )
            .map_err(db_err)?;
        Ok(Minted { token, expires_ms })
    }

    fn key(&self, fingerprint: &str) -> Result<Option<(HostKey, String)>, String> {
        self.db
            .lock()
            .query_row(&format!("SELECT {KEY_COLS} FROM host_keys WHERE fingerprint = ?1"), [fingerprint], row_key)
            .optional()
            .map_err(db_err)
    }

    pub fn keys(&self) -> Result<Vec<HostKey>, String> {
        let db = self.db.lock();
        let mut st = db.prepare(&format!("SELECT {KEY_COLS} FROM host_keys ORDER BY enrolled_ms")).map_err(db_err)?;
        let rows = st.query_map([], row_key).map_err(db_err)?;
        rows.map(|r| r.map(|(k, _)| k).map_err(db_err)).collect()
    }

    /// Store `req`'s key and spend its token. A key that is already
    /// enrolled under the same name answers as enrolled without touching
    /// a token, so a host can call this on every start.
    pub fn enroll(&self, req: &EnrollRequest, now: i64) -> Result<HostKey, Refusal> {
        let internal = |e: String| refusal("internal", e);
        let name = req.host_name.trim();
        if name.is_empty() || name != req.host_name || name.len() > 128 {
            return Err(refusal("bad_request", "host_name must be 1-128 characters without surrounding spaces"));
        }
        let der = crypto::pem_decode("PUBLIC KEY", &req.public_key_pem)
            .and_then(|d| crypto::p256_point_from_spki(&d).map(|_| d.clone()))
            .map_err(|e| refusal("bad_request", format!("public_key_pem: {e}")))?;
        let fp = crypto::fingerprint_of_spki_der(&der);
        let th = token_hash(&req.token);
        let parts: [&[u8]; 3] = [th.as_bytes(), name.as_bytes(), fp.as_bytes()];
        if !crypto::verify_domain(&der, DOMAIN_ENROLL, &parts, &req.sig) {
            return Err(refusal("bad_signature", "the enrollment signature doesn't verify for the key sent"));
        }
        let db = self.db.lock();
        if let Some((k, _)) = self.key(&fp).map_err(internal)? {
            if let Some(at) = k.revoked_ms {
                return Err(revoked(&k, at));
            }
            if k.host_name != name {
                return Err(refusal(
                    "already_enrolled",
                    format!("host key {}… is enrolled as {:?}, not {name:?}", short(&fp), k.host_name),
                ));
            }
            return Ok(k);
        }
        let holder: Option<String> = db
            .query_row(
                "SELECT fingerprint FROM host_keys WHERE host_name = ?1 AND revoked_ms IS NULL",
                [name],
                |r| r.get(0),
            )
            .optional()
            .map_err(|e| internal(db_err(e)))?;
        if let Some(other) = holder {
            return Err(refusal(
                "name_taken",
                format!(
                    "host name {name:?} is enrolled with another key ({}…): revoke it first (POST {REVOKE_PATH})",
                    short(&other)
                ),
            ));
        }
        let spent = db
            .execute(
                "UPDATE host_enroll_tokens SET used_ms = ?2, used_by = ?3
                 WHERE token_sha = ?1 AND used_ms IS NULL AND expires_ms > ?2",
                params![token_hash(&req.token), now, fp],
            )
            .map_err(|e| internal(db_err(e)))?;
        if spent != 1 {
            return Err(refusal(
                "token_invalid",
                "the enrollment token is unknown, already used or expired: mint a new one on the controller \
                 (POST /cli/hosts/enroll-tokens)",
            ));
        }
        let labels = serde_json::to_string(&req.labels).unwrap_or_else(|_| "[]".into());
        db.execute(
            "INSERT INTO host_keys (fingerprint, host_name, public_key_pem, labels, enrolled_ms) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![fp, name, crypto::pem_encode("PUBLIC KEY", &der), labels, now],
        )
        .map_err(|e| internal(db_err(e)))?;
        drop(db);
        log_debug!("[hosts/enroll] enrolled {name} with key {}…", short(&fp));
        k2_core::trace::emit(|| {
            k2_core::trace::Event::new(events::CTL_HOST_ENROLLED).attr("host", name).attr("fp", short(&fp))
        });
        Ok(HostKey {
            fingerprint: fp,
            host_name: name.to_string(),
            labels: req.labels.clone(),
            enrolled_ms: now,
            revoked_ms: None,
            revoked_reason: None,
        })
    }

    /// A fresh single-use nonce.
    pub fn challenge(&self, now: i64) -> Result<String, String> {
        let nonce = crypto::hex(&crypto::random_bytes(24)?);
        let mut n = self.nonces.lock();
        n.retain(|_, at| now - *at < NONCE_TTL_MS);
        if n.len() >= NONCE_CAP {
            // Full of live nonces: drop the oldest; its host asks again.
            if let Some(oldest) = n.iter().min_by_key(|(_, at)| **at).map(|(k, _)| k.clone()) {
                n.remove(&oldest);
            }
        }
        n.insert(nonce.clone(), now);
        Ok(nonce)
    }

    /// The `hello` check. `bootstrap` is the controller's bootstrap token
    /// (only `token` mode looks at it).
    pub fn admit(&self, bootstrap: &str, presented: &str, host_name: &str, now: i64) -> Result<(), Refusal> {
        let r = self.check(bootstrap, presented, host_name, now);
        match &r {
            Ok(()) if self.mode == Mode::Enroll => k2_core::trace::emit(|| {
                k2_core::trace::Event::new(events::CTL_HOST_ADMITTED).attr("host", host_name)
            }),
            Ok(()) => {}
            Err(e) => k2_core::trace::emit(|| {
                k2_core::trace::Event::new(events::CTL_HOST_REFUSED).attr("host", host_name).attr("code", e.code)
            }),
        }
        r
    }

    fn check(&self, bootstrap: &str, presented: &str, host_name: &str, now: i64) -> Result<(), Refusal> {
        if self.mode == Mode::Token {
            return token_check(bootstrap, presented);
        }
        let Some((fp, nonce, sig)) = parse_signed(presented) else {
            return Err(refusal(
                "enrollment_required",
                format!(
                    "this controller takes only enrolled hosts ({AUTH_VAR}=enroll); a bootstrap token is not \
                     accepted. Mint a one-time token (POST {TOKENS_PATH}) and start the host with {AUTH_VAR}=enroll \
                     and the token in K2_HOST_ENROLL_TOKEN_FILE"
                ),
            ));
        };
        let (k, pem) = self
            .key(fp)
            .map_err(|e| refusal("internal", e))?
            .ok_or_else(|| {
                refusal(
                    "unknown_host_key",
                    format!(
                        "host key {}… is not enrolled on this controller: enroll it with a one-time token \
                         (POST {TOKENS_PATH})",
                        short(fp)
                    ),
                )
            })?;
        if let Some(at) = k.revoked_ms {
            return Err(revoked(&k, at));
        }
        if k.host_name != host_name {
            return Err(refusal(
                "host_name_mismatch",
                format!("host key {}… is enrolled as {:?}, not {host_name:?}", short(fp), k.host_name),
            ));
        }
        let issued = self.nonces.lock().remove(nonce);
        if !issued.is_some_and(|at| now - at < NONCE_TTL_MS) {
            return Err(refusal(
                "bad_nonce",
                format!("the hello nonce is unknown, used or older than {} s: fetch a new one", NONCE_TTL_MS / 1000),
            ));
        }
        let der = crypto::pem_decode("PUBLIC KEY", &pem).map_err(|e| refusal("internal", e))?;
        if !crypto::verify_domain(&der, DOMAIN_HELLO, &[nonce.as_bytes(), host_name.as_bytes(), fp.as_bytes()], sig) {
            return Err(refusal("bad_signature", format!("the hello signature doesn't verify for host key {}…", short(fp))));
        }
        Ok(())
    }

    /// Revoke the active key of `who` (a host name or a fingerprint).
    pub fn revoke(&self, who: &str, reason: &str, now: i64) -> Result<HostKey, String> {
        let db = self.db.lock();
        let fp: Option<String> = db
            .query_row(
                "SELECT fingerprint FROM host_keys WHERE revoked_ms IS NULL AND (host_name = ?1 OR fingerprint = ?1)",
                [who],
                |r| r.get(0),
            )
            .optional()
            .map_err(db_err)?;
        let fp = fp.ok_or_else(|| format!("no active host key for {who:?}"))?;
        db.execute(
            "UPDATE host_keys SET revoked_ms = ?2, revoked_reason = ?3 WHERE fingerprint = ?1",
            params![fp, now, reason],
        )
        .map_err(db_err)?;
        let k = self.key(&fp)?.map(|(k, _)| k).ok_or_else(|| format!("host key {fp} vanished"))?;
        log_debug!("[hosts/enroll] revoked {} ({}…): {reason}", k.host_name, short(&fp));
        k2_core::trace::emit(|| {
            k2_core::trace::Event::new(events::CTL_HOST_REVOKED).attr("host", k.host_name.as_str()).attr("fp", short(&fp))
        });
        Ok(k)
    }
}

fn revoked(k: &HostKey, at: i64) -> Refusal {
    refusal(
        "revoked_host_key",
        format!(
            "host key {}… ({}) was revoked at {at}{}: enroll a new key",
            short(&k.fingerprint),
            k.host_name,
            k.revoked_reason.as_deref().filter(|r| !r.is_empty()).map(|r| format!(" ({r})")).unwrap_or_default()
        ),
    )
}

/// Today's check (`K2_HOST_AUTH=token`), with a clear word for a host
/// that signs.
fn token_check(bootstrap: &str, presented: &str) -> Result<(), Refusal> {
    if presented.starts_with(SIGNED_PREFIX) {
        return Err(refusal(
            "bad_token",
            format!(
                "this host signs its hello ({AUTH_VAR}=enroll) but this controller runs {AUTH_VAR}=token: \
                 set the same mode on both"
            ),
        ));
    }
    if bootstrap.is_empty() || !http::ct_eq_token(presented, bootstrap) {
        return Err(refusal("bad_token", "the host token doesn't match this controller's"));
    }
    Ok(())
}

fn registries() -> &'static Mutex<Vec<(&'static RemoteHost, Arc<Registry>)>> {
    static R: OnceLock<Mutex<Vec<(&'static RemoteHost, Arc<Registry>)>>> = OnceLock::new();
    R.get_or_init(Mutex::default)
}

/// Use `reg` for the links `remote` takes (tests; production uses
/// [`controller_registry`]).
pub fn install(remote: &'static RemoteHost, reg: Arc<Registry>) {
    let mut all = registries().lock();
    all.retain(|(r, _)| !std::ptr::eq(*r, remote));
    all.push((remote, reg));
}

/// The production registry: the daemon's database, the global
/// `RemoteHost`, the mode from the environment (boot refused a bad one;
/// fail closed if it changed).
pub fn controller_registry() -> Result<Arc<Registry>, String> {
    let remote = crate::agent_host::remote::global();
    if let Some(r) = registry_for(remote) {
        return Ok(r);
    }
    let reg = Registry::new(k2_core::db::shared(), auth_mode(env).unwrap_or(Mode::Enroll))?;
    let mut all = registries().lock();
    if let Some((_, r)) = all.iter().find(|(r, _)| std::ptr::eq(*r, remote)) {
        return Ok(Arc::clone(r));
    }
    all.push((remote, Arc::clone(&reg)));
    Ok(reg)
}

fn registry_for(remote: &RemoteHost) -> Option<Arc<Registry>> {
    registries().lock().iter().find(|(r, _)| std::ptr::eq(*r, remote)).map(|(_, r)| Arc::clone(r))
}

/// `hub::serve_attach`'s hook: may the host that sent this `hello` link?
/// A `RemoteHost` with no registry (the hub's own tests) and a token-mode
/// controller check the bootstrap token as before.
pub fn admit_hello(remote: &RemoteHost, bootstrap: &str, presented: &str, host_name: &str) -> Result<(), Refusal> {
    let reg = if std::ptr::eq(remote, crate::agent_host::remote::global()) {
        Some(controller_registry().map_err(|e| refusal("internal", e))?)
    } else {
        registry_for(remote)
    };
    match reg {
        Some(reg) => reg.admit(bootstrap, presented, host_name, now_ms()),
        None => token_check(bootstrap, presented),
    }
}

// ── controller routes ──────────────────────────────────────────────────

async fn reply(stream: &mut TcpStream, status: &str, body: &Value) {
    http::send_response(stream, status, "application/json", &body.to_string()).await;
}

fn refusal_status(code: &str) -> &'static str {
    match code {
        "bad_request" => "400 Bad Request",
        "internal" => "500 Internal Server Error",
        "already_enrolled" | "name_taken" => "409 Conflict",
        _ => "403 Forbidden",
    }
}

fn is_ours(path: &str) -> bool {
    [CHALLENGE_PATH, ENROLL_PATH, TOKENS_PATH, KEYS_PATH, REVOKE_PATH].contains(&path)
}

/// `gate::route`'s hook for the enrollment routes. `true` = answered.
/// Only a controller has them; elsewhere the dispatcher's 404 stands.
pub async fn route(stream: &mut TcpStream, buf: &mut [u8], path: &str, query: &str, owner_token: &str) -> bool {
    if !is_ours(path) || crate::runtime_role::current() != crate::runtime_role::Role::Controller {
        return false;
    }
    let body = if matches!(path, ENROLL_PATH | TOKENS_PATH | REVOKE_PATH) {
        http::read_post_body(&mut *stream, buf).await
    } else {
        let _ = stream.read(buf).await;
        Vec::new()
    };
    let (status, out) = match controller_registry() {
        Ok(reg) => answer(&reg, crate::agent_host::remote::global(), path, query, owner_token, &body, now_ms()),
        Err(e) => ("500 Internal Server Error", json!({"error": e})),
    };
    reply(stream, status, &out).await;
    true
}

/// One enrollment route, without the socket (tested directly).
pub fn answer(
    reg: &Registry,
    remote: &RemoteHost,
    path: &str,
    query: &str,
    owner_token: &str,
    body: &[u8],
    now: i64,
) -> (&'static str, Value) {
    let owner = || http::token_is_owner(query, owner_token);
    let forbidden = ("403 Forbidden", json!({"error": "this route needs the controller's owner token"}));
    let body: Value = if body.is_empty() { json!({}) } else { serde_json::from_slice(body).unwrap_or(Value::Null) };
    match path {
        CHALLENGE_PATH => match reg.challenge(now) {
            Ok(nonce) => ("200 OK", json!({"nonce": nonce, "expires_in_ms": NONCE_TTL_MS})),
            Err(e) => ("500 Internal Server Error", json!({"error": e})),
        },
        ENROLL_PATH => match serde_json::from_value::<EnrollRequest>(body) {
            Err(e) => ("400 Bad Request", json!({"error": format!("enroll body: {e}"), "code": "bad_request"})),
            Ok(req) => match reg.enroll(&req, now) {
                Ok(k) => ("200 OK", json!({"enrolled": k, "mode": format!("{:?}", reg.mode()).to_lowercase()})),
                Err(r) => (refusal_status(r.code), json!({"error": r.message, "code": r.code})),
            },
        },
        TOKENS_PATH if owner() => {
            let ttl = body.get("ttl_secs").and_then(Value::as_i64).map(|s| s.saturating_mul(1000)).unwrap_or(TOKEN_TTL_MS);
            match reg.mint_token(now, ttl) {
                Ok(m) => ("200 OK", json!(m)),
                Err(e) => ("500 Internal Server Error", json!({"error": e})),
            }
        }
        KEYS_PATH if owner() => match reg.keys() {
            Ok(keys) => ("200 OK", json!({"mode": format!("{:?}", reg.mode()).to_lowercase(), "keys": keys})),
            Err(e) => ("500 Internal Server Error", json!({"error": e})),
        },
        REVOKE_PATH if owner() => {
            let who = ["host_name", "fingerprint"].iter().find_map(|k| body.get(*k).and_then(Value::as_str)).unwrap_or("");
            let reason = body.get("reason").and_then(Value::as_str).unwrap_or("");
            if who.is_empty() {
                return ("400 Bad Request", json!({"error": "name the key: {\"host_name\": …} or {\"fingerprint\": …}"}));
            }
            match reg.revoke(who, reason, now) {
                Ok(k) => {
                    // A linked host is cut off now, not at its next hello.
                    let kicked = remote
                        .send_frame(&k.host_name, Frame::Refused { code: "revoked_host_key".into(), message: revoked(&k, now).message })
                        .is_ok();
                    ("200 OK", json!({"revoked": k, "disconnected": kicked}))
                }
                Err(e) => ("404 Not Found", json!({"error": e})),
            }
        }
        _ => forbidden,
    }
}

// ── host ───────────────────────────────────────────────────────────────

/// The controller's HTTP base for `attach_url` (`ws://c:7420/cli/hosts/attach`
/// → `http://c:7420`).
pub fn http_base(attach_url: &str) -> Result<String, String> {
    let rest = attach_url.trim_end_matches('/');
    let rest = rest.strip_suffix(ATTACH_PATH).unwrap_or(rest);
    if let Some(r) = rest.strip_prefix("ws://") {
        Ok(format!("http://{r}"))
    } else if let Some(r) = rest.strip_prefix("wss://") {
        Ok(format!("https://{r}"))
    } else {
        Err(format!("{attach_url:?} is not a ws:// or wss:// URL"))
    }
}

/// The host's key file: `K2_HOST_KEY_FILE`, or `host-key.pem` in the K2 home.
pub fn key_path(get: impl Fn(&str) -> Option<String>) -> PathBuf {
    get(KEY_FILE_VAR)
        .filter(|p| !p.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| k2_core::paths::k2_home().join("host-key.pem"))
}

/// Load the host key at `path`, or make one (0600) when it's missing:
/// `k2_node::identity::load_or_create_key`, which the daemon doesn't link.
pub fn load_or_create_key(path: &std::path::Path) -> Result<SigningKey, String> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    match std::fs::read_to_string(path) {
        Ok(pem) => SigningKey::from_pkcs8_pem(&pem).map_err(|e| format!("{}: {e}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let key = SigningKey::generate()?;
            let dir = path.parent().ok_or_else(|| format!("{} has no parent", path.display()))?;
            std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
            let tmp = dir.join(format!(".host-key.{}.tmp", std::process::id()));
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&tmp)
                .map_err(|e| format!("create {}: {e}", tmp.display()))?;
            f.write_all(key.to_pkcs8_pem()?.as_bytes()).and_then(|()| f.sync_all()).map_err(|e| format!("write {}: {e}", tmp.display()))?;
            std::fs::rename(&tmp, path).map_err(|e| format!("rename to {}: {e}", path.display()))?;
            Ok(key)
        }
        Err(e) => Err(format!("read {}: {e}", path.display())),
    }
}

async fn call(req: reqwest::RequestBuilder, what: &str) -> Result<Value, String> {
    let resp = req.send().await.map_err(|e| format!("{what}: {e}"))?;
    let status = resp.status();
    let text = resp.text().await.map_err(|e| format!("{what}: {e}"))?;
    let v: Value = serde_json::from_str(&text).unwrap_or_else(|_| json!({"error": text}));
    if status.is_success() {
        return Ok(v);
    }
    let code = v.get("code").and_then(Value::as_str).unwrap_or("http");
    let msg = v.get("error").and_then(Value::as_str).unwrap_or("");
    Err(format!("{what} refused: {code}: {msg} (HTTP {})", status.as_u16()))
}

/// Enroll `key` (a no-op answer when it already is), then fetch a nonce
/// and sign it: the `hello.token` for one link.
pub async fn signed_hello(cfg: &HostConfig, key: &SigningKey) -> Result<String, String> {
    let base = http_base(&cfg.attach_url)?;
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|e| format!("http client: {e}"))?;
    let req = EnrollRequest {
        token: cfg.token.clone(),
        host_name: cfg.host_name.clone(),
        public_key_pem: key.spki_pem(),
        labels: cfg.labels.clone(),
        sig: sign_enroll(key, &cfg.token, &cfg.host_name)?,
    };
    let body = serde_json::to_string(&req).map_err(|e| format!("encode enroll: {e}"))?;
    let enrolled = call(client.post(format!("{base}{ENROLL_PATH}")).body(body), "enroll").await?;
    let at = enrolled.pointer("/enrolled/enrolled_ms").and_then(Value::as_i64).unwrap_or(0);
    k2_core::trace::emit(|| {
        k2_core::trace::Event::new(events::HOST_ENROLLED)
            .attr("host", cfg.host_name.as_str())
            .attr("fp", short(&key.fingerprint()))
            .attr("enrolled_ms", at)
    });
    let ch = call(client.get(format!("{base}{CHALLENGE_PATH}")), "challenge").await?;
    let nonce = ch.get("nonce").and_then(Value::as_str).ok_or("challenge: no nonce in the answer")?;
    signed_hello_token(key, nonce, &cfg.host_name)
}

/// `link::serve_once`'s hook: the `hello.token` for the next link. The
/// bootstrap token under `K2_HOST_AUTH=token`; a signed proof under
/// `enroll`, after enrolling with the one-time token if needed.
pub async fn hello_token(cfg: &HostConfig) -> Result<String, String> {
    match auth_mode(env)? {
        Mode::Token => Ok(cfg.token.clone()),
        Mode::Enroll => {
            let path = key_path(env);
            let key = load_or_create_key(&path)?;
            signed_hello(cfg, &key).await
        }
    }
}

#[cfg(test)]
#[path = "enroll_tests.rs"]
mod tests;
