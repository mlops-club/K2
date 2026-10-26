//! Mail enable on Kubernetes (PRD P4 T3, decision 4; docs/prd/progress/
//! k8s-capabilities.md): Stalwart already runs in its own pod
//! (deploy/kubernetes/core/mail.yaml, bootstrap mode, recovery credential
//! from the Secret `k2-mail-recovery`), and the controller sets it up over
//! the pod network at `K2_STALWART_URL`.
//!
//! The same management steps, markers, secrets and row as
//! [`super::supervisor::run_enable`], without the steps that touch this
//! machine (download, system user, dirs, unit, systemd): those are the
//! pod's. What changes:
//!
//! - **recovery credential**: read from the Secret (not generated into a
//!   unit), vaulted under the same `recoveryAdminRef`;
//! - **restart**: delete pod `k2-mail-0` and wait for the StatefulSet's
//!   new pod (a new uid, Ready), never answer from the old one;
//! - **listeners**: left as they are. Management stays on the pod network
//!   (port 8080, reachable from the controller only: mail-ingress); the
//!   native port plan moves it to a loopback the controller can't reach;
//! - **recovery-off**: rotate the leftover admins (as natively), then
//!   overwrite the Secret with a random value nobody keeps and restart, so
//!   the recovery credential Stalwart reads is unknown to anyone.
//!
//! - **ApiKey**: allowed from `K2_STALWART_ALLOWED_IPS` (IPs or CIDRs,
//!   normally the pod CIDR), not the loopback: the controller connects
//!   from its pod IP. Unset, the key takes any address and mail-ingress is
//!   the only boundary; the progress state records which (`apiKeyAllowedIps`).
//!
//! Steady state needs nothing new: the row's `api_url` is
//! `K2_STALWART_URL` and the vaulted ApiKey works as on a host.

use std::time::{Duration, Instant};

use serde_json::Value;

use super::secrets::{generate_secret, SecretStore};
use super::supervisor::{self as sup, BootstrapApi};
use crate::k8s;

/// The StatefulSet's one pod (mail.yaml).
pub const STALWART_POD: &str = "k2-mail-0";
/// The Secret mail.yaml reads `STALWART_RECOVERY_ADMIN` from.
pub const RECOVERY_SECRET: &str = "k2-mail-recovery";
pub const RECOVERY_KEY: &str = "password";

/// The steps this path marks, in order (a subset of `ENABLE_STEPS`).
pub const K8S_STEPS: &[&str] =
    &["preflight", "bootstrap", "restart-normal", "server-config", "service-account", "api-key", "recovery-off", "restart"];

const AUTH_RETRIES: u32 = 30;
const POD_WAIT: Duration = Duration::from_secs(300);

/// `K2_STALWART_URL` when mail is delegated to a pod (`K2_CAP_MAIL=external`).
pub fn external_url() -> Option<String> {
    external_url_from(&|k| std::env::var(k).ok())
}

pub fn external_url_from(get: &dyn Fn(&str) -> Option<String>) -> Option<String> {
    if get("K2_CAP_MAIL").as_deref() != Some("external") {
        return None;
    }
    get("K2_STALWART_URL").map(|u| u.trim().trim_end_matches('/').to_string()).filter(|u| !u.is_empty())
}

/// `K2_STALWART_ALLOWED_IPS`: comma-separated IPs or CIDRs the ApiKey is
/// accepted from (empty: any).
pub fn allowed_ips() -> Vec<String> {
    allowed_ips_from(&|k| std::env::var(k).ok())
}

pub fn allowed_ips_from(get: &dyn Fn(&str) -> Option<String>) -> Vec<String> {
    get("K2_STALWART_ALLOWED_IPS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// What the Kubernetes path does to Stalwart's pod and its Secret. The
/// real one talks to the API ([`KubePodOps`]); tests record.
pub trait PodOps {
    /// The recovery password Stalwart reads (64 lowercase hex).
    fn recovery_password(&self) -> Result<String, String>;
    /// Replace it with a random value nobody keeps.
    fn overwrite_recovery_password(&self) -> Result<(), String>;
    /// Delete the pod and wait for its replacement to be Ready.
    fn restart_stalwart(&self) -> Result<(), String>;
    fn sleep_ms(&self, ms: u64);
}

/// The steady-state check for a delegated Stalwart: the authenticated
/// ping only (there is no unit to ask).
pub fn health(api_ping: &dyn Fn() -> Result<(), String>) -> sup::Health {
    match api_ping() {
        Ok(()) => sup::Health::Running,
        Err(e) => sup::Health::Degraded(format!("the management API at K2_STALWART_URL did not answer: {e}")),
    }
}

/// Enabled when this path finished and the row points at `url`.
pub fn is_enabled(url: &str) -> bool {
    sup::step_is_done("restart") && sup::row_field("api_url").as_deref() == Some(url)
}

fn authenticate_retry(pod: &dyn PodOps, api: &mut dyn BootstrapApi, url: &str, user: &str, pw: &str) -> Result<(), String> {
    let mut last = String::new();
    for attempt in 0..AUTH_RETRIES {
        match api.authenticate(url, user, pw) {
            Ok(()) => return Ok(()),
            Err(e) if sup::is_auth_rejected(&e) => return Err(format!("management API rejected {user} at {url}: {e}")),
            Err(e) => last = e,
        }
        if attempt + 1 < AUTH_RETRIES {
            pod.sleep_ms(1000);
        }
    }
    Err(format!("management API not reachable at {url}: {last}"))
}

/// The provisioned admin, else the recovery admin, else the ApiKey, else
/// (until recovery-off) the recovery password in the Secret: a run that
/// stopped after rotating a principal resumes from there.
fn authenticate_saved(pod: &dyn PodOps, api: &mut dyn BootstrapApi, secrets: &dyn SecretStore, url: &str) -> Result<(), String> {
    let mut errors = Vec::new();
    let resolve = |r: Option<String>| r.and_then(|r| secrets.resolve(&r).ok().flatten());
    if let (Some(user), Some(pw)) = (sup::progress_extra("adminUsername"), resolve(sup::row_field("admin_secret_ref"))) {
        match authenticate_retry(pod, api, url, &user, &pw) {
            Ok(()) => return Ok(()),
            Err(e) => errors.push(e),
        }
    }
    if let Some(pw) = resolve(sup::progress_extra("recoveryAdminRef")) {
        match api.authenticate(url, "admin", &pw) {
            Ok(()) => return Ok(()),
            Err(e) => errors.push(format!("admin: {e}")),
        }
    }
    if let Some(key) = resolve(sup::row_field("api_key_ref")) {
        match api.authenticate_bearer(url, &key) {
            Ok(()) => return Ok(()),
            Err(e) => errors.push(format!("api-key: {e}")),
        }
    }
    if !sup::step_is_done("recovery-off") {
        match pod.recovery_password() {
            Ok(pw) => match api.authenticate(url, "admin", &pw) {
                Ok(()) => return Ok(()),
                Err(e) => errors.push(format!("admin (Secret {RECOVERY_SECRET}): {e}")),
            },
            Err(e) => errors.push(format!("Secret {RECOVERY_SECRET}: {e}")),
        }
    }
    Err(if errors.is_empty() { "no saved mail credential".into() } else { errors.join(" | ") })
}

/// Set up the Stalwart pod at `url`, resumably (the native machine's
/// markers). On failure the row reads `error` with `<step>: <why>`.
pub fn run_enable_k8s(
    pod: &dyn PodOps,
    api: &mut dyn BootstrapApi,
    secrets: &dyn SecretStore,
    url: &str,
    hostname: &str,
    allowed_ips: &[String],
) -> Result<(), String> {
    // The row needs a legal port plan (a CHECK constraint); `http-01`
    // asks Stalwart for no TLS certificate and no listener is applied on
    // this path: the pod's listeners stay as mail.yaml declares them.
    // Where mail lives is recorded in the progress state instead.
    sup::ensure_installing_row(hostname, "http-01")?;
    sup::progress_extra_set("placement", "kubernetes");
    sup::progress_extra_set("stalwartUrl", url);
    let default_domain = sup::default_domain_for(hostname);
    let fail = |step: &str, err: String| -> String {
        let msg = format!("{step}: {err}");
        sup::set_status("error");
        sup::set_last_error(Some(&msg));
        sup::emit_state_change("installing", "error", Some(&msg));
        msg
    };
    if !sup::step_is_done("preflight") {
        sup::mark_step("preflight");
    }

    if !sup::step_is_done("bootstrap") {
        sup::set_current("bootstrap");
        let pw = pod.recovery_password().map_err(|e| fail("bootstrap", e))?;
        if !k2_root_proto::templates::recovery_pw_ok(&pw) {
            return Err(fail("bootstrap", format!("Secret {RECOVERY_SECRET} key {RECOVERY_KEY} is not 64 lowercase hex")));
        }
        let sref = secrets.store("recovery-admin", &pw).map_err(|e| fail("bootstrap", e))?;
        sup::progress_extra_set("recoveryAdminRef", &sref);
        authenticate_retry(pod, api, url, "admin", &pw).map_err(|e| fail("bootstrap", e))?;
        // No ACME from the pod: TLS for mail on Kubernetes is not this
        // step's (the native tls-alpn plan needs :443 on this machine).
        let creds = api.complete_bootstrap(hostname, &default_domain, false).map_err(|e| fail("bootstrap", e))?;
        let aref = secrets.store("admin", &creds.secret).map_err(|e| fail("bootstrap", e))?;
        sup::set_row_field("admin_secret_ref", &aref);
        sup::progress_extra_set("adminUsername", &creds.username);
        sup::mark_step("bootstrap");
    }

    // config.json is on the pod's volume now: a new pod boots normal mode.
    if !sup::step_is_done("restart-normal") {
        sup::set_current("restart-normal");
        pod.restart_stalwart().map_err(|e| fail("restart-normal", e))?;
        sup::mark_step("restart-normal");
    }

    let api_steps_remain =
        !(sup::step_is_done("service-account") && sup::step_is_done("api-key") && sup::step_is_done("recovery-off"));
    if api_steps_remain {
        authenticate_saved(pod, api, secrets, url).map_err(|e| fail("server-config", e))?;
    }
    if !sup::step_is_done("server-config") {
        // Nothing to change: no loopback rebinding in a pod.
        sup::mark_step("server-config");
    }

    if !sup::step_is_done("service-account") {
        sup::set_current("service-account");
        let id = api.create_service_account(&default_domain).map_err(|e| fail("service-account", e))?;
        sup::progress_extra_set("serviceAccountId", &id);
        sup::mark_step("service-account");
    }

    if !sup::step_is_done("api-key") {
        sup::set_current("api-key");
        let id = sup::progress_extra("serviceAccountId")
            .ok_or_else(|| fail("api-key", "service account id missing from progress state".into()))?;
        let key = api.mint_api_key_allowed(&id, allowed_ips).map_err(|e| fail("api-key", e))?;
        sup::progress_extra_set("apiKeyAllowedIps", &if allowed_ips.is_empty() { "any".to_string() } else { allowed_ips.join(",") });
        let kref = secrets.store("api-key", &key).map_err(|e| fail("api-key", e))?;
        sup::set_row_field("api_key_ref", &kref);
        sup::set_row_field("api_url", url);
        sup::mark_step("api-key");
    }

    // The recovery credential and the bootstrap principal both still
    // authenticate in normal mode: rotate the principals, then make the
    // env credential unknown (Secret overwritten, pod restarted below).
    if !sup::step_is_done("recovery-off") {
        sup::set_current("recovery-off");
        (|| -> Result<(), String> {
            if !api.has_session() {
                authenticate_saved(pod, api, secrets, url)?;
            }
            // As natively (iascm): a session that the steps above used
            // can be refused here; the ApiKey from the api-key step still
            // opens the management API. Seen live on kind as well.
            if let Err(e) = sup::rotate_leftover_admins(api, secrets, &default_domain) {
                if !sup::is_auth_rejected(&e) {
                    return Err(e);
                }
                let key = sup::row_field("api_key_ref")
                    .and_then(|r| secrets.resolve(&r).ok().flatten())
                    .ok_or_else(|| format!("{e} (no api-key to retry)"))?;
                api.authenticate_bearer(url, &key).map_err(|b| format!("{e} | api-key: {b}"))?;
                sup::rotate_leftover_admins(api, secrets, &default_domain)?;
            }
            pod.overwrite_recovery_password()
        })()
        .map_err(|e| fail("recovery-off", e))?;
        sup::mark_step("recovery-off");
    }

    if !sup::step_is_done("restart") {
        sup::set_current("restart");
        pod.restart_stalwart().map_err(|e| fail("restart", e))?;
        // The new pod answers with the ApiKey (normal mode, setup kept).
        let key = sup::row_field("api_key_ref")
            .and_then(|r| secrets.resolve(&r).ok().flatten())
            .ok_or_else(|| fail("restart", "api-key missing from the mail secret store".into()))?;
        let mut last = String::new();
        let mut done = false;
        for attempt in 0..AUTH_RETRIES {
            match api.authenticate_bearer(url, &key) {
                Ok(()) => {
                    done = true;
                    break;
                }
                Err(e) => last = e,
            }
            if attempt + 1 < AUTH_RETRIES {
                pod.sleep_ms(1000);
            }
        }
        if !done {
            return Err(fail("restart", format!("the ApiKey is not accepted after the restart: {last}")));
        }
        sup::mark_step("restart");
    }

    sup::set_row_field("installed_version", super::supervisor::STALWART_PINNED_VERSION);
    let mut p = sup::progress_load();
    p["completedAt"] = serde_json::json!(sup::now_secs());
    p["current"] = Value::Null;
    sup::progress_save(&p);
    sup::set_status("running");
    sup::set_last_error(None);
    sup::emit_state_change("installing", "running", None);
    Ok(())
}

// ── The route ───────────────────────────────────────────────────────────

/// `POST /cli/mail/server/enable` when mail is delegated to a pod: no
/// host preflight, helper or download (they are the pod's). Starts the
/// machine on a thread, as the native route does, and answers at once.
pub(crate) fn handle_enable(hostname: String, url: String) -> crate::cli_response::CliResponse {
    use crate::cli_response::CliResponse;
    if !sup::try_begin_enable() {
        return CliResponse {
            status: "409 Conflict",
            content_type: "application/json",
            body: serde_json::json!({"ok": false, "error": {"code": "enable_in_progress",
                "hint": "an enable run is already in progress — poll /cli/mail/status"}})
            .to_string(),
        };
    }
    let pod = match KubePodOps::in_cluster() {
        Ok(p) => p,
        Err(e) => {
            sup::end_enable();
            return CliResponse::internal_error(format!("K2_CAP_MAIL=external needs the Kubernetes API: {e}"));
        }
    };
    let target = url.clone();
    std::thread::spawn(move || {
        let secrets = super::secrets::FileSecretStore::default();
        let mut api = super::jmap::StalwartBootstrap::new();
        if let Err(e) = run_enable_k8s(&pod, &mut api, &secrets, &target, &hostname, &allowed_ips()) {
            k2_core::log_debug!("[mail/k8s] enable failed: {e}");
        }
        sup::end_enable();
    });
    CliResponse::ok_json(
        serde_json::json!({
            "ok": true,
            "state": "installing",
            "hint": "setting up the Stalwart pod in the background — poll GET /cli/mail/status for enableProgress",
            "stalwartUrl": url,
        })
        .to_string(),
    )
}

// ── The real PodOps: the Kubernetes API from inside the controller pod ──

/// Blocking adapter over [`k8s::Client`] for the enable thread (which is
/// a plain thread, like the native machine's).
pub struct KubePodOps {
    client: k8s::Client,
    rt: tokio::runtime::Runtime,
}

impl KubePodOps {
    pub fn in_cluster() -> Result<KubePodOps, String> {
        let cfg = k8s::Config::in_cluster(std::path::Path::new(k8s::SA_DIR)).map_err(|e| e.to_string())?;
        let client = k8s::Client::new(cfg).map_err(|e| e.to_string())?;
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().map_err(|e| e.to_string())?;
        Ok(KubePodOps { client, rt })
    }

    fn pod_state(&self) -> Result<(String, bool), String> {
        let doc = self.rt.block_on(self.client.get(&self.client.path("v1", "pods", Some(STALWART_POD)))).map_err(|e| e.to_string())?;
        let uid = doc.pointer("/metadata/uid").and_then(Value::as_str).unwrap_or("").to_string();
        let terminating = doc.pointer("/metadata/deletionTimestamp").is_some();
        let ready = !terminating
            && doc
                .pointer("/status/conditions")
                .and_then(Value::as_array)
                .is_some_and(|cs| cs.iter().any(|c| c["type"] == "Ready" && c["status"] == "True"));
        Ok((uid, ready))
    }
}

impl PodOps for KubePodOps {
    fn recovery_password(&self) -> Result<String, String> {
        let data = self.rt.block_on(self.client.secret_data(RECOVERY_SECRET)).map_err(|e| e.to_string())?;
        let pw = data.get(RECOVERY_KEY).ok_or_else(|| format!("Secret {RECOVERY_SECRET} has no key {RECOVERY_KEY}"))?;
        Ok(String::from_utf8_lossy(pw).trim().to_string())
    }

    fn overwrite_recovery_password(&self) -> Result<(), String> {
        use base64::Engine as _;
        let unknown = generate_secret()?;
        let patch = serde_json::json!({"data": {RECOVERY_KEY: base64::engine::general_purpose::STANDARD.encode(unknown)}});
        self.rt
            .block_on(self.client.merge_patch(&self.client.path("v1", "secrets", Some(RECOVERY_SECRET)), &patch))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    fn restart_stalwart(&self) -> Result<(), String> {
        let (old, _) = self.pod_state()?;
        self.rt
            .block_on(self.client.delete(&self.client.path("v1", "pods", Some(STALWART_POD))))
            .map_err(|e| e.to_string())?;
        let end = Instant::now() + POD_WAIT;
        while Instant::now() < end {
            if let Ok((uid, ready)) = self.pod_state() {
                if !uid.is_empty() && uid != old && ready {
                    return Ok(());
                }
            }
            std::thread::sleep(Duration::from_secs(2));
        }
        Err(format!("pod {STALWART_POD} was not replaced and Ready within {} s", POD_WAIT.as_secs()))
    }

    fn sleep_ms(&self, ms: u64) {
        std::thread::sleep(Duration::from_millis(ms));
    }
}

#[cfg(test)]
mod tests;
