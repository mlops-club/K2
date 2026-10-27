//! Signed host enrollment: the registry's rules, and a loopback controller
//! (the enrollment routes and `/cli/hosts/attach` through the real hub)
//! against the host's own enroll-and-sign code.
//!
//! Deadlines only stop a hung test; nothing asserts a duration.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k2_core::compute::proto::crypto::SigningKey;
use parking_lot::ReentrantMutex;
use rusqlite::Connection;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;

use super::*;
use crate::hosts::hub;
use crate::hosts::proto::{Frame, PROTOCOL};

const BOOTSTRAP: &str = "test-bootstrap-token";
const OWNER: &str = "test-owner-token";
const DEADLINE: Duration = Duration::from_secs(30);

fn memory_db() -> Db {
    Arc::new(ReentrantMutex::new(Connection::open_in_memory().unwrap()))
}

fn registry(mode: Mode) -> Arc<Registry> {
    Registry::new(memory_db(), mode).unwrap()
}

fn request(key: &SigningKey, token: &str, name: &str) -> EnrollRequest {
    EnrollRequest {
        token: token.into(),
        host_name: name.into(),
        public_key_pem: key.spki_pem(),
        labels: vec!["pool=lab".into()],
        sig: sign_enroll(key, token, name).unwrap(),
    }
}

/// Enroll a fresh key as `name`.
fn enrolled(reg: &Registry, name: &str) -> SigningKey {
    let key = SigningKey::generate().unwrap();
    let t = reg.mint_token(1_000, TOKEN_TTL_MS).unwrap();
    reg.enroll(&request(&key, &t.token, name), 1_001).unwrap();
    key
}

fn hello(reg: &Registry, key: &SigningKey, name: &str, now: i64) -> Result<(), Refusal> {
    let nonce = reg.challenge(now).unwrap();
    reg.admit(BOOTSTRAP, &signed_hello_token(key, &nonce, name).unwrap(), name, now)
}

fn code(r: Result<impl std::fmt::Debug, Refusal>) -> &'static str {
    r.expect_err("expected a refusal").code
}

#[test]
fn auth_mode_defaults_to_token_and_refuses_unknown_values() {
    let get = |v: Option<&'static str>| move |k: &str| (k == AUTH_VAR).then(|| v.map(str::to_string)).flatten();
    assert_eq!(auth_mode(get(None)), Ok(Mode::Token));
    assert_eq!(auth_mode(get(Some(""))), Ok(Mode::Token));
    assert_eq!(auth_mode(get(Some("token"))), Ok(Mode::Token));
    assert_eq!(auth_mode(get(Some(" enroll "))), Ok(Mode::Enroll));
    assert_eq!(
        auth_mode(get(Some("signed"))),
        Err("K2_HOST_AUTH=\"signed\" is not a host auth mode; allowed values: token, enroll".into())
    );
}

#[test]
fn the_host_key_is_made_once_0600_and_reloads() {
    use std::os::unix::fs::PermissionsExt;
    let p = k2_core::test_env::unique_temp_path("host-key-dir").join("host-key.pem");
    let a = load_or_create_key(&p).unwrap();
    let b = load_or_create_key(&p).unwrap();
    assert_eq!(a.fingerprint(), b.fingerprint());
    assert_eq!(std::fs::metadata(&p).unwrap().permissions().mode() & 0o777, 0o600);
    std::fs::remove_dir_all(p.parent().unwrap()).unwrap();
}

#[test]
fn http_base_follows_the_attach_url() {
    assert_eq!(http_base("ws://ctl:7420/cli/hosts/attach").unwrap(), "http://ctl:7420");
    assert_eq!(http_base("wss://k2.example.com/cli/hosts/attach").unwrap(), "https://k2.example.com");
    assert!(http_base("http://ctl:7420/cli/hosts/attach").is_err());
}

#[test]
fn a_token_enrolls_one_key_once() {
    let reg = registry(Mode::Enroll);
    let t = reg.mint_token(1_000, TOKEN_TTL_MS).unwrap();
    assert!(t.token.starts_with("k2e_"), "{}", t.token);
    let key = SigningKey::generate().unwrap();
    let k = reg.enroll(&request(&key, &t.token, "host-1"), 1_001).unwrap();
    assert_eq!((k.host_name.as_str(), k.fingerprint.clone(), k.labels.clone()), ("host-1", key.fingerprint(), vec!["pool=lab".to_string()]));

    // The same key again answers enrolled (a host restarting), token spent or not.
    assert_eq!(reg.enroll(&request(&key, &t.token, "host-1"), 2_000).unwrap(), k);
    // The spent token enrolls nothing else.
    let other = SigningKey::generate().unwrap();
    assert_eq!(code(reg.enroll(&request(&other, &t.token, "host-2"), 2_001)), "token_invalid");
    // Nor does a made-up or expired one.
    assert_eq!(code(reg.enroll(&request(&other, "k2e_made_up", "host-2"), 2_002)), "token_invalid");
    let short_lived = reg.mint_token(3_000, 10).unwrap();
    assert_eq!(code(reg.enroll(&request(&other, &short_lived.token, "host-2"), 3_010)), "token_invalid");

    // A second key can't take a name that's held, nor rename the first.
    let t2 = reg.mint_token(4_000, TOKEN_TTL_MS).unwrap();
    assert_eq!(code(reg.enroll(&request(&other, &t2.token, "host-1"), 4_001)), "name_taken");
    assert_eq!(code(reg.enroll(&request(&key, &t2.token, "host-9"), 4_002)), "already_enrolled");
    // The refusals above left t2 unspent.
    reg.enroll(&request(&other, &t2.token, "host-2"), 4_003).unwrap();
    assert_eq!(reg.keys().unwrap().iter().map(|k| k.host_name.as_str()).collect::<Vec<_>>(), ["host-1", "host-2"]);
}

#[test]
fn enrolling_needs_the_private_key() {
    let reg = registry(Mode::Enroll);
    let t = reg.mint_token(1_000, TOKEN_TTL_MS).unwrap();
    let key = SigningKey::generate().unwrap();
    let thief = SigningKey::generate().unwrap();
    // Someone else's public key, signed with the thief's key.
    let mut req = request(&thief, &t.token, "host-1");
    req.public_key_pem = key.spki_pem();
    assert_eq!(code(reg.enroll(&req, 1_001)), "bad_signature");
    let mut req = request(&key, &t.token, "host-1");
    req.public_key_pem = "-----BEGIN PUBLIC KEY-----\nAAAA\n-----END PUBLIC KEY-----\n".into();
    assert_eq!(code(reg.enroll(&req, 1_002)), "bad_request");
    assert!(reg.keys().unwrap().is_empty());
}

#[test]
fn a_signed_hello_from_an_enrolled_key_is_admitted() {
    let reg = registry(Mode::Enroll);
    let key = enrolled(&reg, "host-1");
    assert_eq!(hello(&reg, &key, "host-1", 5_000), Ok(()));
    // Every link signs a fresh nonce.
    assert_eq!(hello(&reg, &key, "host-1", 6_000), Ok(()));
}

#[test]
fn a_bad_signature_is_refused() {
    let reg = registry(Mode::Enroll);
    let key = enrolled(&reg, "host-1");
    let thief = SigningKey::generate().unwrap();
    let nonce = reg.challenge(5_000).unwrap();
    // The enrolled fingerprint, signed by another key.
    let forged = signed_hello_token(&thief, &nonce, "host-1").unwrap().replace(&thief.fingerprint(), &key.fingerprint());
    assert_eq!(code(reg.admit(BOOTSTRAP, &forged, "host-1", 5_001)), "bad_signature");
    // A real signature over a different nonce than the one presented.
    let n1 = reg.challenge(5_000).unwrap();
    let n2 = reg.challenge(5_000).unwrap();
    let swapped = signed_hello_token(&key, &n1, "host-1").unwrap().replace(&n1, &n2);
    assert_eq!(code(reg.admit(BOOTSTRAP, &swapped, "host-1", 5_002)), "bad_signature");
}

#[test]
fn nonces_are_single_use_and_short_lived() {
    let reg = registry(Mode::Enroll);
    let key = enrolled(&reg, "host-1");
    let nonce = reg.challenge(5_000).unwrap();
    let token = signed_hello_token(&key, &nonce, "host-1").unwrap();
    assert_eq!(reg.admit(BOOTSTRAP, &token, "host-1", 5_001), Ok(()));
    // A captured hello replayed.
    assert_eq!(code(reg.admit(BOOTSTRAP, &token, "host-1", 5_002)), "bad_nonce");
    // A nonce the controller never issued.
    let made_up = signed_hello_token(&key, "00ff", "host-1").unwrap();
    assert_eq!(code(reg.admit(BOOTSTRAP, &made_up, "host-1", 5_003)), "bad_nonce");
    // One used after its minute.
    let old = reg.challenge(10_000).unwrap();
    let late = signed_hello_token(&key, &old, "host-1").unwrap();
    assert_eq!(code(reg.admit(BOOTSTRAP, &late, "host-1", 10_000 + NONCE_TTL_MS)), "bad_nonce");
}

#[test]
fn an_unknown_key_or_another_hosts_name_is_refused() {
    let reg = registry(Mode::Enroll);
    let key = enrolled(&reg, "host-1");
    let stranger = SigningKey::generate().unwrap();
    let r = hello(&reg, &stranger, "host-1", 5_000).unwrap_err();
    assert_eq!(r.code, "unknown_host_key");
    assert!(r.message.contains(&stranger.fingerprint()[..16]), "{}", r.message);
    // An enrolled key may only link under its own name.
    assert_eq!(code(hello(&reg, &key, "host-2", 5_001)), "host_name_mismatch");
}

#[test]
fn the_bootstrap_token_alone_is_refused_in_enroll_mode() {
    let reg = registry(Mode::Enroll);
    enrolled(&reg, "host-1");
    let r = reg.admit(BOOTSTRAP, BOOTSTRAP, "host-1", 5_000).unwrap_err();
    assert_eq!(r.code, "enrollment_required");
    assert!(r.message.contains("K2_HOST_AUTH=enroll"), "{}", r.message);
    assert_eq!(code(reg.admit(BOOTSTRAP, "k2host1.garbage", "host-1", 5_000)), "enrollment_required");
}

#[test]
fn token_mode_is_todays_check_and_names_a_mode_mismatch() {
    let reg = registry(Mode::Token);
    assert_eq!(reg.admit(BOOTSTRAP, BOOTSTRAP, "host-1", 1), Ok(()));
    let r = reg.admit(BOOTSTRAP, "wrong", "host-1", 1).unwrap_err();
    assert_eq!(r, Refusal { code: "bad_token", message: "the host token doesn't match this controller's".into() });
    assert_eq!(code(reg.admit("", "", "host-1", 1)), "bad_token");
    let key = SigningKey::generate().unwrap();
    let r = reg.admit(BOOTSTRAP, &signed_hello_token(&key, "n", "host-1").unwrap(), "host-1", 1).unwrap_err();
    assert_eq!(r.code, "bad_token");
    assert!(r.message.contains("K2_HOST_AUTH=token"), "{}", r.message);
}

#[test]
fn a_revoked_key_is_refused_and_cannot_enroll_again() {
    let reg = registry(Mode::Enroll);
    let key = enrolled(&reg, "host-1");
    let k = reg.revoke("host-1", "laptop stolen", 7_000).unwrap();
    assert_eq!((k.revoked_ms, k.revoked_reason.as_deref()), (Some(7_000), Some("laptop stolen")));
    let r = hello(&reg, &key, "host-1", 7_001).unwrap_err();
    assert_eq!(r.code, "revoked_host_key");
    assert!(r.message.contains("laptop stolen"), "{}", r.message);
    // A fresh token doesn't bring the key back.
    let t = reg.mint_token(7_002, TOKEN_TTL_MS).unwrap();
    assert_eq!(code(reg.enroll(&request(&key, &t.token, "host-1"), 7_003)), "revoked_host_key");
    assert!(reg.revoke("host-1", "", 7_004).is_err(), "nothing active left to revoke");
    // A new key may take the name (and the token the refusal left unspent).
    let fresh = SigningKey::generate().unwrap();
    reg.enroll(&request(&fresh, &t.token, "host-1"), 7_005).unwrap();
    assert_eq!(hello(&reg, &fresh, "host-1", 7_006), Ok(()));
    // Revoke by fingerprint too.
    reg.revoke(&fresh.fingerprint(), "", 7_007).unwrap();
    assert_eq!(code(hello(&reg, &fresh, "host-1", 7_008)), "revoked_host_key");
}

#[test]
fn enrollments_survive_a_controller_restart() {
    let path = k2_core::test_env::unique_temp_path("host-enroll.db");
    let open = || Arc::new(ReentrantMutex::new(Connection::open(&path).unwrap()));
    let key = {
        let reg = Registry::new(open(), Mode::Enroll).unwrap();
        enrolled(&reg, "host-1")
    };
    let reg = Registry::new(open(), Mode::Enroll).unwrap();
    assert_eq!(hello(&reg, &key, "host-1", 9_000), Ok(()));
    drop(reg);
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn owner_routes_need_the_owner_token() {
    let reg = registry(Mode::Enroll);
    let remote = crate::agent_host::remote::RemoteHost::new();
    let q = format!("token={OWNER}");
    for path in [TOKENS_PATH, KEYS_PATH, REVOKE_PATH] {
        let (status, _) = answer(&reg, &remote, path, "token=nope", OWNER, b"{}", 1);
        assert_eq!(status, "403 Forbidden", "{path}");
    }
    let (status, minted) = answer(&reg, &remote, TOKENS_PATH, &q, OWNER, br#"{"ttl_secs": 60}"#, 1_000);
    assert_eq!(status, "200 OK");
    assert_eq!(minted["expires_ms"], json!(61_000));
    // Challenge and enroll are public; the token is the credential.
    let (status, ch) = answer(&reg, &remote, CHALLENGE_PATH, "", OWNER, b"", 1_000);
    assert_eq!(status, "200 OK");
    assert!(ch["nonce"].as_str().is_some_and(|n| n.len() == 48), "{ch}");
    let key = SigningKey::generate().unwrap();
    let body = serde_json::to_vec(&request(&key, minted["token"].as_str().unwrap(), "host-1")).unwrap();
    let (status, out) = answer(&reg, &remote, ENROLL_PATH, "", OWNER, &body, 1_001);
    assert_eq!((status, out["enrolled"]["host_name"].clone()), ("200 OK", json!("host-1")));
    let (status, out) = answer(&reg, &remote, ENROLL_PATH, "", OWNER, &body.iter().rev().copied().collect::<Vec<_>>(), 1_002);
    assert_eq!((status, out["code"].clone()), ("400 Bad Request", json!("bad_request")));
    let (_, keys) = answer(&reg, &remote, KEYS_PATH, &q, OWNER, b"", 1_003);
    assert_eq!(keys["mode"], json!("enroll"));
    assert_eq!(keys["keys"][0]["fingerprint"], json!(key.fingerprint()));
    let (status, out) = answer(&reg, &remote, REVOKE_PATH, &q, OWNER, br#"{"host_name":"host-1","reason":"r"}"#, 1_004);
    assert_eq!((status, out["disconnected"].clone()), ("200 OK", json!(false)));
    let (status, _) = answer(&reg, &remote, REVOKE_PATH, &q, OWNER, br#"{"host_name":"host-1"}"#, 1_005);
    assert_eq!(status, "404 Not Found");
}

// ── loopback: the routes and the hub on one port ───────────────────────

struct Controller {
    remote: &'static RemoteHost,
    reg: Arc<Registry>,
    base: String,
    attach_url: String,
}

/// Read one HTTP request: `(path, query, body)`.
async fn read_request(s: &mut TcpStream) -> (String, String, Vec<u8>) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        let n = s.read(&mut chunk).await.unwrap();
        assert!(n > 0, "client hung up mid-request");
        buf.extend_from_slice(&chunk[..n]);
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let target = head.split(' ').nth(1).unwrap_or("/").to_string();
    let len: usize = head
        .lines()
        .find_map(|l| l.to_ascii_lowercase().strip_prefix("content-length:").map(|v| v.trim().parse().unwrap()))
        .unwrap_or(0);
    while buf.len() < head_end + len {
        let n = s.read(&mut chunk).await.unwrap();
        assert!(n > 0, "client hung up mid-body");
        buf.extend_from_slice(&chunk[..n]);
    }
    let (path, query) = target.split_once('?').unwrap_or((&target, ""));
    (path.to_string(), query.to_string(), buf[head_end..head_end + len].to_vec())
}

impl Controller {
    async fn start(mode: Mode) -> Self {
        let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
        let reg = registry(mode);
        install(remote, Arc::clone(&reg));
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let routes = Arc::clone(&reg);
        tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                let reg = Arc::clone(&routes);
                tokio::spawn(async move {
                    let mut peek = [0u8; 1024];
                    let n = s.peek(&mut peek).await.unwrap();
                    if String::from_utf8_lossy(&peek[..n]).contains(ATTACH_PATH) {
                        return hub::serve_attach(s, remote, BOOTSTRAP).await;
                    }
                    let (path, query, body) = read_request(&mut s).await;
                    let (status, out) = answer(&reg, remote, &path, &query, OWNER, &body, now_ms());
                    let out = out.to_string();
                    let resp = format!(
                        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{out}",
                        out.len()
                    );
                    s.write_all(resp.as_bytes()).await.unwrap();
                });
            }
        });
        Controller { remote, reg, base: format!("http://{addr}"), attach_url: format!("ws://{addr}{ATTACH_PATH}") }
    }

    fn host(&self, name: &str, token: &str) -> HostConfig {
        HostConfig { attach_url: self.attach_url.clone(), token: token.into(), host_name: name.into(), labels: vec![] }
    }
}

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(DEADLINE, f).await.unwrap_or_else(|_| panic!("timed out waiting for {what}"))
}

async fn recv(ws: &mut Ws) -> Frame {
    within("a frame", async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(t))) => return Frame::decode(&t).expect("frame"),
                Some(Ok(Message::Close(_))) | None => panic!("link closed"),
                Some(Ok(_)) => continue,
                Some(Err(e)) => panic!("link error: {e}"),
            }
        }
    })
    .await
}

/// Dial and say `hello` with `token`; the controller's answer.
async fn link(c: &Controller, name: &str, token: &str) -> (Ws, Frame) {
    let (mut ws, _) = within("dial", tokio_tungstenite::connect_async(c.attach_url.as_str())).await.expect("dial");
    let hello = Frame::Hello { protocol: PROTOCOL, host_name: name.into(), token: token.into(), labels: vec![], resume: vec![], ended: vec![] };
    ws.send(Message::Text(hello.encode())).await.unwrap();
    let answer = recv(&mut ws).await;
    (ws, answer)
}

fn refused_code(f: &Frame) -> &str {
    match f {
        Frame::Refused { code, .. } => code,
        other => panic!("expected refused, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_enrolls_over_http_links_signed_and_is_cut_off_when_revoked() {
    let c = Controller::start(Mode::Enroll).await;
    let http = reqwest::Client::new();
    let minted = http.post(format!("{}{TOKENS_PATH}?token={OWNER}", c.base)).body("{}").send().await.unwrap();
    let minted: Value = serde_json::from_str(&minted.text().await.unwrap()).unwrap();
    let one_time = minted["token"].as_str().unwrap().to_string();
    let key = SigningKey::generate().unwrap();

    // The host's own code: enroll, fetch a nonce, sign.
    let token = within("signed hello", signed_hello(&c.host("host-1", &one_time), &key)).await.unwrap();
    let (mut ws, welcome) = link(&c, "host-1", &token).await;
    assert!(matches!(&welcome, Frame::Welcome { host_id, .. } if host_id == "host-1"), "{welcome:?}");

    // A later link (the host restarted): the token is spent, the key isn't.
    let again = within("second hello", signed_hello(&c.host("host-1", &one_time), &key)).await.unwrap();
    // The hello token is single use.
    let (_, replay) = link(&c, "host-1", &token).await;
    assert_eq!(refused_code(&replay), "bad_nonce");

    // A stolen bootstrap token (or the spent one-time token) gets nowhere.
    let (_, stolen) = link(&c, "host-1", BOOTSTRAP).await;
    assert_eq!(refused_code(&stolen), "enrollment_required");
    let thief = SigningKey::generate().unwrap();
    let err = within("thief", signed_hello(&c.host("host-1", &one_time), &thief)).await.unwrap_err();
    assert!(err.starts_with("enroll refused: name_taken:"), "{err}");

    // Revoke: the live link gets a `refused` and closes.
    let (status, out) = answer(&c.reg, c.remote, REVOKE_PATH, &format!("token={OWNER}"), OWNER, br#"{"host_name":"host-1"}"#, now_ms());
    assert_eq!((status, out["disconnected"].clone()), ("200 OK", json!(true)), "{out}");
    assert_eq!(refused_code(&recv(&mut ws).await), "revoked_host_key");
    // And its next hello, signed over a nonce fetched before the revoke, is refused.
    let (_, after) = link(&c, "host-1", &again).await;
    assert_eq!(refused_code(&after), "revoked_host_key");
    let err = within("re-enroll", signed_hello(&c.host("host-1", &one_time), &key)).await.unwrap_err();
    assert!(err.starts_with("enroll refused: revoked_host_key:"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_token_mode_controller_links_bootstrap_hosts_as_before() {
    let c = Controller::start(Mode::Token).await;
    let (_ws, welcome) = link(&c, "host-1", BOOTSTRAP).await;
    assert!(matches!(&welcome, Frame::Welcome { host_id, .. } if host_id == "host-1"), "{welcome:?}");
    let (_, wrong) = link(&c, "host-2", "wrong").await;
    assert_eq!(refused_code(&wrong), "bad_token");
}

/// After a controller DB restore the host's key is forgotten and its boot
/// token is spent. The token file is read on every attempt: once the owner
/// mints a new one into it, the next retry enrolls the same key again, with
/// no host restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forgotten_key_enrolls_again_with_a_token_minted_after_the_refusal() {
    let http = reqwest::Client::new();
    let mint = |c: &Controller| {
        let (http, base) = (http.clone(), c.base.clone());
        async move {
            let r = http.post(format!("{base}{TOKENS_PATH}?token={OWNER}")).body("{}").send().await.unwrap();
            serde_json::from_str::<Value>(&r.text().await.unwrap()).unwrap()["token"].as_str().unwrap().to_string()
        }
    };
    let file = k2_core::test_env::unique_temp_path("host-enroll-token");
    let key = SigningKey::generate().unwrap();
    // Enrolled on the first controller; the file holds the token used.
    let c1 = Controller::start(Mode::Enroll).await;
    std::fs::write(&file, mint(&c1).await).unwrap();
    let f = || Some(file.to_string_lossy().into_owned());
    within("enroll", enroll_hello_from(&c1.host("host-1", "boot-token"), &key, f())).await.unwrap();
    // The DB is restored: a controller that never saw this key. The file's token is spent there.
    let c2 = Controller::start(Mode::Enroll).await;
    let err = within("refused", enroll_hello_from(&c2.host("host-1", "boot-token"), &key, f())).await.unwrap_err();
    assert!(err.contains("token_invalid"), "{err}");
    let again = within("still refused", enroll_hello_from(&c2.host("host-1", "boot-token"), &key, f())).await.unwrap_err();
    assert!(again.contains("token_invalid"), "keeps retrying: {again}");
    // The owner mints a new token into the file: the next retry enrolls the same key.
    std::fs::write(&file, mint(&c2).await).unwrap();
    let token = within("re-enroll", enroll_hello_from(&c2.host("host-1", "boot-token"), &key, f())).await.unwrap();
    let (_, welcome) = link(&c2, "host-1", &token).await;
    assert!(matches!(&welcome, Frame::Welcome { host_id, .. } if host_id == "host-1"), "{welcome:?}");
    let keys = c2.reg.keys().unwrap();
    assert_eq!(keys.iter().map(|k| (k.host_name.as_str(), k.fingerprint.as_str())).collect::<Vec<_>>(), [("host-1", key.fingerprint().as_str())]);
    let _ = std::fs::remove_file(&file);
}

/// A file that can't be read leaves the token read at boot in use.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreadable_token_file_falls_back_to_the_boot_token() {
    let c = Controller::start(Mode::Enroll).await;
    let http = reqwest::Client::new();
    let r = http.post(format!("{}{TOKENS_PATH}?token={OWNER}", c.base)).body("{}").send().await.unwrap();
    let boot = serde_json::from_str::<Value>(&r.text().await.unwrap()).unwrap()["token"].as_str().unwrap().to_string();
    let key = SigningKey::generate().unwrap();
    let missing = k2_core::test_env::unique_temp_path("no-such-token").to_string_lossy().into_owned();
    within("enroll", enroll_hello_from(&c.host("host-1", &boot), &key, Some(missing))).await.unwrap();
}

/// The controller's routes answer once per connection and close it
/// without `Connection: close`. Requests in a row on the enroll client
/// (enroll, then challenge) never reuse such a dead connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_enroll_client_never_reuses_a_connection_the_controller_closed() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((mut s, _)) = l.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = s.read(&mut buf).await;
                let body = r#"{"nonce":"n"}"#;
                let head = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n", body.len());
                let _ = s.write_all(head.as_bytes()).await;
                let _ = s.write_all(body.as_bytes()).await;
                // Closed unannounced, and (as through kind's service proxy)
                // only after the next request on it was sent: it is lost.
                let _ = s.read(&mut buf).await;
            });
        }
    });
    let client = controller_client().unwrap();
    for i in 0..50 {
        let r = call(client.get(format!("{base}{CHALLENGE_PATH}")), "challenge").await;
        assert!(r.is_ok(), "request {i}: {r:?}");
    }
}
