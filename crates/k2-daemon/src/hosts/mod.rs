//! Remote agent hosts (PRD "K2 Distributed Runtime", P2 first cut).
//!
//! Two roles of the same binary:
//!
//! - `K2_ROLE=controller` serves `/cli/hosts/attach` ([`hub`]) and relays
//!   the S1 routes (`/cli/sessions/v2/spawn`, `/close`, `/grid`) to the
//!   host that runs the agent ([`gate`]); state lives in
//!   `agent_host::remote::RemoteHost`.
//! - `K2_ROLE=host` is today's daemon plus [`link`]: it dials the
//!   controller and serves the controller's requests with its own
//!   in-process host.
//!
//! The wire protocol is [`proto`]; the env contract and the table of
//! frames are in `docs/prd/progress/dist-link.md`.

pub mod byte_ring;
pub mod enroll;
pub mod gate;
pub mod hub;
pub mod link;
pub mod proto;
#[cfg(test)]
mod tests;

use std::sync::OnceLock;

/// Controller: must be `1` with `K2_ROLE=controller`.
pub const HOSTS_VAR: &str = "K2_HOSTS";
/// Controller: file holding the token hosts present in `hello`.
pub const BOOTSTRAP_TOKEN_FILE_VAR: &str = "K2_HOST_BOOTSTRAP_TOKEN_FILE";
/// Host: the controller's base URL (`ws://controller:7420`).
pub const CONTROLLER_URL_VAR: &str = "K2_CONTROLLER_URL";
/// Host: file holding the same token.
pub const ENROLL_TOKEN_FILE_VAR: &str = "K2_HOST_ENROLL_TOKEN_FILE";
/// Host: its name (default: the machine's hostname).
pub const HOST_NAME_VAR: &str = "K2_HOST_NAME";
/// Host: comma-separated labels, passed through in `hello`.
pub const HOST_LABELS_VAR: &str = "K2_HOST_LABELS";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerConfig {
    pub token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostConfig {
    /// `ws://…/cli/hosts/attach`, the exact URL the link dials.
    pub attach_url: String,
    pub token: String,
    pub host_name: String,
    pub labels: Vec<String>,
}

fn read_token(var: &str, path: Option<String>) -> Result<String, String> {
    let path = path.filter(|p| !p.trim().is_empty()).ok_or_else(|| format!("{var} must name the host token file"))?;
    let raw = std::fs::read_to_string(&path).map_err(|e| format!("{var}={path:?}: can't read the token file: {e}"))?;
    let token = raw.trim().to_string();
    if token.is_empty() {
        return Err(format!("{var}={path:?}: the token file is empty"));
    }
    Ok(token)
}

/// The controller's settings, from `get` (the process environment in
/// production).
pub fn controller_config(get: impl Fn(&str) -> Option<String>) -> Result<ControllerConfig, String> {
    match get(HOSTS_VAR).as_deref().map(str::trim) {
        Some("1") => {}
        other => {
            return Err(format!(
                "K2_ROLE=controller runs agents only on agent hosts: set {HOSTS_VAR}=1 (got {})",
                other.map(|v| format!("{v:?}")).unwrap_or_else(|| "unset".into())
            ))
        }
    }
    Ok(ControllerConfig { token: read_token(BOOTSTRAP_TOKEN_FILE_VAR, get(BOOTSTRAP_TOKEN_FILE_VAR))? })
}

/// The host's settings, from `get`.
pub fn host_config(get: impl Fn(&str) -> Option<String>) -> Result<HostConfig, String> {
    let base = get(CONTROLLER_URL_VAR)
        .map(|v| v.trim().trim_end_matches('/').to_string())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| format!("K2_ROLE=host needs {CONTROLLER_URL_VAR} (for example ws://controller:7420)"))?;
    if !(base.starts_with("ws://") || base.starts_with("wss://")) {
        return Err(format!("{CONTROLLER_URL_VAR}={base:?} must start with ws:// or wss://"));
    }
    let attach_url = if base.ends_with(proto::ATTACH_PATH) { base } else { format!("{base}{}", proto::ATTACH_PATH) };
    let token = read_token(ENROLL_TOKEN_FILE_VAR, get(ENROLL_TOKEN_FILE_VAR))?;
    let host_name = get(HOST_NAME_VAR)
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .or_else(machine_hostname)
        .unwrap_or_else(|| "host".to_string());
    let mut labels: Vec<String> = get(HOST_LABELS_VAR)
        .map(|v| v.split(',').map(str::trim).filter(|l| !l.is_empty()).map(str::to_string).collect())
        .unwrap_or_default();
    // P3: capacity travels as a label (the protocol doesn't change).
    let max_awake_var = crate::provisioner::MAX_AWAKE_VAR;
    if let Some(v) = get(max_awake_var).map(|v| v.trim().to_string()).filter(|v| !v.is_empty()) {
        let n: u32 = v.parse().map_err(|_| format!("{max_awake_var}={v:?} is not a whole number"))?;
        let label = crate::provisioner::LABEL_MAX_AWAKE;
        if !labels.iter().any(|l| l.split('=').next().map(str::trim) == Some(label)) {
            labels.push(format!("{label}={n}"));
        }
    }
    Ok(HostConfig { attach_url, token, host_name, labels })
}

/// The machine's hostname (a pod's name on Kubernetes).
fn machine_hostname() -> Option<String> {
    let mut buf = [0u8; 256];
    // SAFETY: `buf` is writable for its whole length; gethostname writes
    // at most that many bytes.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    if rc != 0 {
        return None;
    }
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    Some(String::from_utf8_lossy(&buf[..end]).into_owned()).filter(|h| !h.is_empty())
}

fn env(var: &str) -> Option<String> {
    std::env::var(var).ok()
}

static CONTROLLER: OnceLock<ControllerConfig> = OnceLock::new();

/// The controller's settings, read once. `None` unless this daemon is a
/// controller and they parse (boot already refused anything else).
pub fn controller() -> Option<&'static ControllerConfig> {
    if crate::runtime_role::current() != crate::runtime_role::Role::Controller {
        return None;
    }
    if let Some(c) = CONTROLLER.get() {
        return Some(c);
    }
    let c = controller_config(env).ok()?;
    Some(CONTROLLER.get_or_init(|| c))
}

/// Start this role's background half after boot is ready: the host's
/// link. A controller has nothing to start (hosts dial in).
pub fn start() {
    match crate::runtime_role::current() {
        crate::runtime_role::Role::Host => match host_config(env) {
            Ok(cfg) => {
                k2_core::log_debug!("[hosts] K2_ROLE=host: linking to {} as {}", cfg.attach_url, cfg.host_name);
                byte_ring::set(Some(host_ptyd_ring));
                tokio::spawn(link::run(cfg));
            }
            Err(e) => {
                // Boot validated this; a token file removed since is fatal.
                k2_core::log_debug!("[hosts] FATAL: {e}");
                std::process::exit(2);
            }
        },
        crate::runtime_role::Role::Controller => {
            k2_core::log_debug!("[hosts] K2_ROLE=controller: waiting for agent hosts on {}", proto::ATTACH_PATH);
            // Transcripts are on the agents' hosts: the resume decision asks them.
            k2_core::transcript_probe::set(Some(controller_transcript_probe));
        }
        crate::runtime_role::Role::All => {}
    }
}

/// A host's [`byte_ring`] source (W3b): k2-ptyd's ring of a ptyd-backed
/// session on this host (`K2_PTYD=1`), read without taking its data
/// stream. Any other session has no ring ([`byte_ring::NO_RING`]: the
/// client falls back to the grid snapshot), and ptyd is never started for
/// one.
fn host_ptyd_ring(session_id: &str, from: u64, max: usize) -> Result<byte_ring::RingRead, String> {
    let id = k2_core::session::SessionId::parse(session_id).ok_or_else(|| format!("bad session id {session_id}"))?;
    let session = crate::agent_host::host()
        .find_session(&id)
        .ok_or_else(|| format!("session {session_id} is not running on this host"))?;
    if session.is_remote() || session.pty().backend_name() != "ptyd" {
        return Err(byte_ring::NO_RING.to_string());
    }
    let owner = k2_core::terminal::ptyd::client::global().owner()?;
    let (start, end, bytes) = owner.read_ring(session_id, from, max).map_err(|e| e.to_string())?;
    Ok(byte_ring::RingRead { start, end, bytes })
}

/// The controller's `k2_core::transcript_probe`.
fn controller_transcript_probe(provider: &str, session_id: &str, project_path: &str) -> Option<bool> {
    crate::agent_host::remote::global().transcript_exists(provider, session_id, project_path)
}

/// `GET /cli/hosts/status` (public, no secrets): the role and its link
/// state. `None` under `K2_ROLE=all`, where the route doesn't exist.
pub fn status_json() -> Option<serde_json::Value> {
    match crate::runtime_role::current() {
        crate::runtime_role::Role::Controller => Some(crate::agent_host::remote::global().status_json()),
        crate::runtime_role::Role::Host => Some(link::status_json()),
        crate::runtime_role::Role::All => None,
    }
}

#[cfg(test)]
mod config_tests {
    use super::*;
    use std::collections::HashMap;

    fn token_file(content: &str) -> String {
        let p = k2_core::test_env::unique_temp_path("hosts-token");
        std::fs::write(&p, content).unwrap();
        p.to_string_lossy().into_owned()
    }

    fn getter(pairs: &[(&str, String)]) -> impl Fn(&str) -> Option<String> {
        let m: HashMap<String, String> = pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect();
        move |k| m.get(k).cloned()
    }

    #[test]
    fn controller_needs_k2_hosts_and_a_token_file() {
        let tok = token_file("s3cret\n");
        assert_eq!(
            controller_config(getter(&[(HOSTS_VAR, "1".into()), (BOOTSTRAP_TOKEN_FILE_VAR, tok.clone())])),
            Ok(ControllerConfig { token: "s3cret".into() })
        );
        assert_eq!(
            controller_config(getter(&[(BOOTSTRAP_TOKEN_FILE_VAR, tok.clone())])),
            Err("K2_ROLE=controller runs agents only on agent hosts: set K2_HOSTS=1 (got unset)".into())
        );
        assert_eq!(
            controller_config(getter(&[(HOSTS_VAR, "1".into())])),
            Err("K2_HOST_BOOTSTRAP_TOKEN_FILE must name the host token file".into())
        );
        let empty = token_file(" \n");
        let err = controller_config(getter(&[(HOSTS_VAR, "1".into()), (BOOTSTRAP_TOKEN_FILE_VAR, empty)])).unwrap_err();
        assert!(err.ends_with("the token file is empty"), "{err}");
    }

    #[test]
    fn host_needs_a_ws_url_and_a_token_file() {
        let tok = token_file("s3cret");
        let cfg = host_config(getter(&[
            (CONTROLLER_URL_VAR, "ws://ctl:7420/".into()),
            (ENROLL_TOKEN_FILE_VAR, tok.clone()),
            (HOST_NAME_VAR, "host-1".into()),
            (HOST_LABELS_VAR, "pool=default, gpu".into()),
        ]))
        .unwrap();
        assert_eq!(
            cfg,
            HostConfig {
                attach_url: "ws://ctl:7420/cli/hosts/attach".into(),
                token: "s3cret".into(),
                host_name: "host-1".into(),
                labels: vec!["pool=default".into(), "gpu".into()],
            }
        );
        assert_eq!(
            host_config(getter(&[(ENROLL_TOKEN_FILE_VAR, tok.clone())])),
            Err("K2_ROLE=host needs K2_CONTROLLER_URL (for example ws://controller:7420)".into())
        );
        // P3: K2_HOST_MAX_AWAKE is sent as the k2.max_awake label.
        let with = |max: &str, labels: &str| {
            host_config(getter(&[
                (CONTROLLER_URL_VAR, "ws://ctl:7420".into()),
                (ENROLL_TOKEN_FILE_VAR, tok.clone()),
                (HOST_LABELS_VAR, labels.into()),
                (crate::provisioner::MAX_AWAKE_VAR, max.into()),
            ]))
        };
        assert_eq!(with("2", "k2.kind=laptop").unwrap().labels, vec!["k2.kind=laptop", "k2.max_awake=2"]);
        assert_eq!(with("2", "k2.max_awake=5").unwrap().labels, vec!["k2.max_awake=5"], "an explicit label wins");
        assert_eq!(with("lots", "").unwrap_err(), "K2_HOST_MAX_AWAKE=\"lots\" is not a whole number");
        assert_eq!(
            host_config(getter(&[(CONTROLLER_URL_VAR, "http://ctl".into()), (ENROLL_TOKEN_FILE_VAR, tok)])),
            Err("K2_CONTROLLER_URL=\"http://ctl\" must start with ws:// or wss://".into())
        );
    }
}
