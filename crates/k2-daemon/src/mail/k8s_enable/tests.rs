//! The Kubernetes enable path against fakes: the pod (restart, the
//! recovery Secret), the management API (every call recorded with its
//! URL) and the secret store. The mail row is the real test DB, under the
//! mail test lock.

use super::*;
use crate::mail::supervisor::AdminCredentials;
use std::cell::RefCell;
use std::sync::Mutex;

const URL: &str = "http://k2-mail:8080";
const RECOVERY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const POD_CIDR: &str = "10.244.0.0/16";

fn cidr() -> Vec<String> {
    vec![POD_CIDR.to_string()]
}

#[derive(Default)]
struct FakePod {
    log: RefCell<Vec<String>>,
    recovery: RefCell<String>,
}

impl FakePod {
    fn new() -> FakePod {
        let p = FakePod::default();
        *p.recovery.borrow_mut() = RECOVERY.into();
        p
    }
}

impl PodOps for FakePod {
    fn recovery_password(&self) -> Result<String, String> {
        self.log.borrow_mut().push("read-recovery".into());
        Ok(self.recovery.borrow().clone())
    }
    fn overwrite_recovery_password(&self) -> Result<(), String> {
        self.log.borrow_mut().push("overwrite-recovery".into());
        *self.recovery.borrow_mut() = "gone".into();
        Ok(())
    }
    fn restart_stalwart(&self) -> Result<(), String> {
        self.log.borrow_mut().push("restart".into());
        Ok(())
    }
    fn sleep_ms(&self, _ms: u64) {}
}

#[derive(Default)]
struct FakeApi {
    calls: Vec<String>,
    /// One failure of this call name (to test resume).
    fail_once: Option<&'static str>,
    /// The first rotation is refused as unauthenticated (seen live).
    rotate_401_once: bool,
    /// Only the Secret's recovery password logs in (the vaulted ones were
    /// rotated by a run that then stopped), and this many ApiKey logins
    /// are refused (a key minted for the loopback, seen live).
    only_recovery_pw: bool,
    bearer_403s: u32,
}

impl FakeApi {
    fn rec(&mut self, call: String) -> Result<(), String> {
        let name = call.split(' ').next().unwrap_or("").to_string();
        self.calls.push(call);
        if self.fail_once == Some(name.as_str()) {
            self.fail_once = None;
            return Err(format!("{name} failed (test)"));
        }
        if name == "rotate_admin_secret" && self.rotate_401_once {
            self.rotate_401_once = false;
            return Err("POST http://k2-mail:8080/jmap/: HTTP 401 Unauthorized: You have to authenticate first.".into());
        }
        Ok(())
    }
}

impl BootstrapApi for FakeApi {
    fn authenticate(&mut self, base_url: &str, username: &str, password: &str) -> Result<(), String> {
        self.rec(format!("authenticate {base_url} {username}"))?;
        if self.only_recovery_pw && password != RECOVERY {
            return Err("GET /jmap/session: HTTP 401 Unauthorized: You have to authenticate first.".into());
        }
        Ok(())
    }
    fn authenticate_bearer(&mut self, base_url: &str, _key: &str) -> Result<(), String> {
        self.rec(format!("authenticate_bearer {base_url}"))?;
        if self.bearer_403s > 0 {
            self.bearer_403s -= 1;
            return Err("GET /jmap/session: HTTP 403 Forbidden: You do not have enough permissions".into());
        }
        Ok(())
    }
    fn complete_bootstrap(&mut self, hostname: &str, domain: &str, tls: bool) -> Result<AdminCredentials, String> {
        self.rec(format!("complete_bootstrap {hostname} {domain} tls={tls}"))?;
        Ok(AdminCredentials { username: format!("admin@{domain}"), secret: "provisioned".into() })
    }
    fn set_server_hostname(&mut self, h: &str) -> Result<(), String> {
        self.rec(format!("set_server_hostname {h}"))
    }
    fn bind_setup_http_loopback(&mut self) -> Result<(), String> {
        self.rec("bind_setup_http_loopback".into())
    }
    fn configure_listeners(&mut self, plan: &str) -> Result<(), String> {
        self.rec(format!("configure_listeners {plan}"))
    }
    fn lock_acme_cert_names(&mut self, h: &str) -> Result<(), String> {
        self.rec(format!("lock_acme_cert_names {h}"))
    }
    fn create_service_account(&mut self, domain: &str) -> Result<String, String> {
        self.rec(format!("create_service_account {domain}"))?;
        Ok("acct-1".into())
    }
    fn mint_api_key(&mut self, id: &str) -> Result<String, String> {
        self.rec(format!("mint_api_key {id}"))?;
        Ok("api-key-secret".into())
    }
    fn mint_api_key_allowed(&mut self, id: &str, allowed_ips: &[String]) -> Result<String, String> {
        self.rec(format!("mint_api_key_allowed {id} {}", allowed_ips.join(",")))?;
        Ok("api-key-secret".into())
    }
    fn rotate_admin_secret(&mut self, username: &str, _current: &str, _new: &str) -> Result<(), String> {
        self.rec(format!("rotate_admin_secret {username}"))
    }
}

#[derive(Default)]
struct FakeSecrets {
    stored: Mutex<Vec<(String, String, String)>>, // (ref, kind, secret)
}

impl SecretStore for FakeSecrets {
    fn store(&self, kind: &str, secret: &str) -> Result<String, String> {
        let mut s = self.stored.lock().unwrap();
        let r = format!("mailsec_{kind}_{}", s.len());
        s.push((r.clone(), kind.into(), secret.into()));
        Ok(r)
    }
    fn resolve(&self, sref: &str) -> Result<Option<String>, String> {
        Ok(self.stored.lock().unwrap().iter().find(|(r, _, _)| r == sref).map(|(_, _, s)| s.clone()))
    }
    fn delete(&self, sref: &str) -> Result<(), String> {
        self.stored.lock().unwrap().retain(|(r, _, _)| r != sref);
        Ok(())
    }
}

fn clean_row() {
    let db = k2_core::db::shared();
    let conn = db.lock();
    let _ = conn.execute("DELETE FROM mail_server WHERE id = 1", []);
}

#[test]
fn external_url_only_when_mail_is_delegated() {
    let get = |pairs: &'static [(&'static str, &'static str)]| {
        move |k: &str| pairs.iter().find(|(n, _)| *n == k).map(|(_, v)| v.to_string())
    };
    assert_eq!(external_url_from(&get(&[])), None);
    assert_eq!(external_url_from(&get(&[("K2_STALWART_URL", URL)])), None, "host mail: never this path");
    assert_eq!(external_url_from(&get(&[("K2_CAP_MAIL", "external")])), None, "no URL: not this path");
    assert_eq!(
        external_url_from(&get(&[("K2_CAP_MAIL", "external"), ("K2_STALWART_URL", "http://k2-mail:8080/")])),
        Some(URL.to_string())
    );
}

#[test]
fn the_full_sequence_talks_only_to_the_pod_and_lands_running() {
    let _g = crate::mail::mail_server_test_lock();
    clean_row();
    let (pod, mut api, secrets) = (FakePod::new(), FakeApi::default(), FakeSecrets::default());
    run_enable_k8s(&pod, &mut api, &secrets, URL, "mail.acme.dev", &cidr()).unwrap();

    assert_eq!(*pod.log.borrow(), vec!["read-recovery", "restart", "overwrite-recovery", "restart"]);
    assert_eq!(
        api.calls,
        vec![
            format!("authenticate {URL} admin"),
            "complete_bootstrap mail.acme.dev acme.dev tls=false".into(),
            format!("authenticate {URL} admin@acme.dev"),
            "create_service_account acme.dev".into(),
            format!("mint_api_key_allowed acct-1 {POD_CIDR}"),
            format!("authenticate {URL} admin@acme.dev"),
            "rotate_admin_secret admin".into(),
            "rotate_admin_secret admin@acme.dev".into(),
            format!("authenticate_bearer {URL}"),
        ]
    );
    // No listener is moved off the pod network, no host step happened.
    assert!(!api.calls.iter().any(|c| c.starts_with("bind_setup_http_loopback") || c.starts_with("configure_listeners")));
    assert_eq!(sup::current_status().as_deref(), Some("running"));
    assert_eq!(sup::row_field("api_url").as_deref(), Some(URL));
    assert_eq!(sup::progress_extra("placement").as_deref(), Some("kubernetes"));
    assert_eq!(sup::progress_extra("stalwartUrl").as_deref(), Some(URL));
    assert_eq!(sup::progress_extra("apiKeyAllowedIps").as_deref(), Some(POD_CIDR), "never the loopback");
    for step in K8S_STEPS {
        assert!(sup::step_is_done(step), "{step} not marked");
    }
    assert!(is_enabled(URL));
    // The recovery password from the Secret was rotated away in the vault
    // and overwritten in the Secret; the ApiKey is what remains.
    let kinds: Vec<String> = secrets.stored.lock().unwrap().iter().map(|(_, k, _)| k.clone()).collect();
    assert!(kinds.contains(&"api-key".to_string()), "{kinds:?}");
    assert!(!secrets.stored.lock().unwrap().iter().any(|(_, _, s)| s == RECOVERY), "the Secret's recovery password is not kept");
    assert_eq!(*pod.recovery.borrow(), "gone");
    clean_row();
}

#[test]
fn a_failed_step_is_resumed_without_redoing_setup() {
    let _g = crate::mail::mail_server_test_lock();
    clean_row();
    let (pod, secrets) = (FakePod::new(), FakeSecrets::default());
    let mut api = FakeApi { fail_once: Some("mint_api_key_allowed"), ..FakeApi::default() };
    let err = run_enable_k8s(&pod, &mut api, &secrets, URL, "mail.acme.dev", &cidr()).unwrap_err();
    assert!(err.starts_with("api-key: "), "{err}");
    assert_eq!(sup::current_status().as_deref(), Some("error"));
    assert!(sup::row_field("last_error").unwrap_or_default().starts_with("api-key: "));

    api.calls.clear();
    run_enable_k8s(&pod, &mut api, &secrets, URL, "mail.acme.dev", &cidr()).unwrap();
    assert!(!api.calls.iter().any(|c| c.starts_with("complete_bootstrap")), "setup ran once: {:?}", api.calls);
    assert!(!api.calls.iter().any(|c| c.starts_with("create_service_account")), "{:?}", api.calls);
    assert!(api.calls.iter().any(|c| c.starts_with("mint_api_key")));
    assert_eq!(sup::current_status().as_deref(), Some("running"));
    assert_eq!(*pod.log.borrow(), vec!["read-recovery", "restart", "overwrite-recovery", "restart"]);
    clean_row();
}

#[test]
fn a_malformed_recovery_secret_is_refused() {
    let _g = crate::mail::mail_server_test_lock();
    clean_row();
    let (pod, mut api, secrets) = (FakePod::new(), FakeApi::default(), FakeSecrets::default());
    *pod.recovery.borrow_mut() = "not-hex".into();
    let err = run_enable_k8s(&pod, &mut api, &secrets, URL, "mail.acme.dev", &cidr()).unwrap_err();
    assert!(err.contains("64 lowercase hex"), "{err}");
    assert!(api.calls.is_empty(), "nothing sent with a bad credential: {:?}", api.calls);
    clean_row();
}

#[test]
fn health_is_the_ping_only() {
    assert_eq!(health(&|| Ok(())), sup::Health::Running);
    assert!(matches!(health(&|| Err("refused".into())), sup::Health::Degraded(d) if d.contains("refused")));
}

#[test]
fn the_hooks_are_inert_unless_mail_is_delegated() {
    // Host mail (no K2_CAP_MAIL): is_already_enabled and refresh_health
    // take their native paths; this module's external_url is None.
    let _env = k2_core::test_env::EnvVar::remove("K2_CAP_MAIL");
    assert_eq!(external_url(), None);
}

#[test]
fn a_refused_rotation_retries_with_the_api_key() {
    let _g = crate::mail::mail_server_test_lock();
    clean_row();
    let (pod, secrets) = (FakePod::new(), FakeSecrets::default());
    let mut api = FakeApi { rotate_401_once: true, ..FakeApi::default() };
    run_enable_k8s(&pod, &mut api, &secrets, URL, "mail.acme.dev", &cidr()).unwrap();
    let i = api.calls.iter().position(|c| c.starts_with("rotate_admin_secret")).unwrap();
    assert_eq!(api.calls[i + 1], format!("authenticate_bearer {URL}"), "{:?}", api.calls);
    assert!(api.calls[i + 2..].iter().any(|c| c == "rotate_admin_secret admin@acme.dev"), "{:?}", api.calls);
    assert_eq!(sup::current_status().as_deref(), Some("running"));
    assert_eq!(*pod.recovery.borrow(), "gone");
    clean_row();
}

#[test]
fn allowed_ips_come_from_the_env_and_default_to_any() {
    let get = |v: Option<&'static str>| move |k: &str| (k == "K2_STALWART_ALLOWED_IPS").then(|| v.map(String::from)).flatten();
    assert!(allowed_ips_from(&get(None)).is_empty());
    assert!(allowed_ips_from(&get(Some(" "))).is_empty());
    assert_eq!(allowed_ips_from(&get(Some("10.244.0.0/16, 10.96.0.0/12,"))), vec!["10.244.0.0/16", "10.96.0.0/12"]);
}

#[test]
fn an_unrestricted_key_is_recorded_as_any() {
    let _g = crate::mail::mail_server_test_lock();
    clean_row();
    let (pod, mut api, secrets) = (FakePod::new(), FakeApi::default(), FakeSecrets::default());
    run_enable_k8s(&pod, &mut api, &secrets, URL, "mail.acme.dev", &[]).unwrap();
    assert!(api.calls.contains(&"mint_api_key_allowed acct-1 ".to_string()), "{:?}", api.calls);
    assert_eq!(sup::progress_extra("apiKeyAllowedIps").as_deref(), Some("any"));
    clean_row();
}

#[test]
fn a_run_that_stopped_mid_rotation_resumes_with_the_secret() {
    // Seen live: recovery-off rotated a principal, then stopped; every
    // vaulted credential was refused and the ApiKey was 403. Until
    // recovery-off is done the Secret still holds what Stalwart reads.
    let _g = crate::mail::mail_server_test_lock();
    clean_row();
    let (pod, secrets) = (FakePod::new(), FakeSecrets::default());
    let mut api = FakeApi { fail_once: Some("rotate_admin_secret"), ..FakeApi::default() };
    let err = run_enable_k8s(&pod, &mut api, &secrets, URL, "mail.acme.dev", &cidr()).unwrap_err();
    assert!(err.starts_with("recovery-off: "), "{err}");
    // As live: the principal's new password is vaulted, Stalwart's
    // recovery login (the Secret) is not it.
    let rotated = secrets.store("recovery-admin", "rotated-before-the-stop").unwrap();
    sup::progress_extra_set("recoveryAdminRef", &rotated);

    let mut api = FakeApi { only_recovery_pw: true, bearer_403s: 1, ..FakeApi::default() };
    run_enable_k8s(&pod, &mut api, &secrets, URL, "mail.acme.dev", &cidr()).unwrap();
    // The provisioned admin, the vaulted recovery password and the ApiKey
    // are refused; the Secret's password logs in.
    let login: Vec<String> =
        [format!("authenticate {URL} admin@acme.dev"), format!("authenticate {URL} admin"), format!("authenticate_bearer {URL}"), format!("authenticate {URL} admin")]
            .into();
    assert_eq!(api.calls[..4], login[..], "{:?}", api.calls);
    assert!(api.calls.contains(&"rotate_admin_secret admin@acme.dev".to_string()), "{:?}", api.calls);
    assert_eq!(sup::current_status().as_deref(), Some("running"));
    assert_eq!(*pod.recovery.borrow(), "gone", "the Secret is overwritten after the resume");
    clean_row();
}
