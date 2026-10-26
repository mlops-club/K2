//! Capability drivers on Kubernetes (PRD P4 T3, docs/prd/progress/
//! k8s-capabilities.md): for each feature the report marks `delegated`, a
//! driver checks the thing it is delegated to, every [`PERIOD`], and
//! `/boot-status` shows the answer next to the mode
//! (`capabilities.<feature>.driver: {ok, detail, checkedMs}`).
//!
//! What is checked, by mode:
//!
//! - `K2_CAP_POSTGRES=kubernetes`: `postgresql/<K2_POSTGRES_CLUSTER>` (the
//!   Zalando operator's object) is `Running`, and the operator's credential
//!   Secret for the controller's own user is readable with `username` and
//!   `password` keys (values never leave this function).
//! - `K2_CAP_APPS=kubernetes`: the controller may list Deployments in its
//!   namespace (the RBAC the App driver needs).
//! - `external` (mail, Sew, or Postgres by URL): the management URL
//!   (`K2_STALWART_URL`, `K2_SEW_MGMT_URL`) answers; Stalwart on
//!   `/healthz/ready`.
//! - `listen` (front door): nothing to reach; reported as such.
//!
//! The drivers that act (create databases, deploy Apps) build on this and
//! wait for the coordinator's decisions 1–4. Nothing here changes any
//! object: every call is a GET.

use k2_core::log_debug;
use std::collections::BTreeMap;
use std::sync::{OnceLock, RwLock};
use std::time::Duration;

use serde_json::Value;

use super::{Feature, Report, State};
use crate::k8s;

/// How often each driver checks again.
pub const PERIOD: Duration = Duration::from_secs(30);
const HTTP_TIMEOUT: Duration = Duration::from_secs(5);

/// One driver's latest answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    pub ok: bool,
    pub detail: String,
    pub checked_ms: u64,
}

/// What a driver checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Check {
    /// The operator's cluster object and the controller user's Secret.
    PostgresOperator { cluster: String, user: String },
    /// The controller may list Deployments (App driver RBAC).
    AppsRbac,
    /// An HTTP management endpoint answers 2xx.
    Http { url: String },
    /// Nothing to reach from here (the front door listens itself).
    Listen,
    /// Delegated, but the setting it needs is missing.
    Misconfigured(String),
}

/// The checks for this report: one per delegated feature. `get` reads a
/// setting (`std::env::var` in the daemon; a map in tests).
pub fn plan(report: &Report, get: &dyn Fn(&str) -> Option<String>) -> Vec<(Feature, Check)> {
    let mut out = Vec::new();
    for e in &report.0 {
        if e.state != State::Delegated {
            continue;
        }
        let check = match (e.feature, e.mode) {
            (Feature::Postgres, "kubernetes") => match get("K2_POSTGRES_CLUSTER") {
                Some(cluster) if !cluster.is_empty() => {
                    let user = get("K2_POSTGRES_USER_FILE")
                        .and_then(|f| std::fs::read_to_string(f).ok())
                        .map(|u| u.trim().to_string())
                        .filter(|u| !u.is_empty())
                        .unwrap_or_else(|| "k2".into());
                    Check::PostgresOperator { cluster, user }
                }
                _ => Check::Misconfigured("K2_CAP_POSTGRES=kubernetes needs K2_POSTGRES_CLUSTER".into()),
            },
            (Feature::Apps, "kubernetes") => Check::AppsRbac,
            (Feature::Mail, "external") => url_check(get("K2_STALWART_URL"), "/healthz/ready", "K2_STALWART_URL"),
            (Feature::Sew, "external") => url_check(get("K2_SEW_MGMT_URL"), "", "K2_SEW_MGMT_URL"),
            (Feature::Postgres, "external") => match get("K2_POSTGRES_HOST") {
                Some(h) if !h.is_empty() => Check::Misconfigured(format!("Postgres by URL ({h}): no driver yet")),
                _ => Check::Misconfigured("K2_CAP_POSTGRES=external needs K2_POSTGRES_HOST".into()),
            },
            (Feature::Front, "listen") => Check::Listen,
            (f, mode) => Check::Misconfigured(format!("no driver for {}={mode}", f.key())),
        };
        out.push((e.feature, check));
    }
    out
}

fn url_check(base: Option<String>, path: &str, var: &str) -> Check {
    match base {
        Some(b) if !b.is_empty() => Check::Http { url: format!("{}{path}", b.trim_end_matches('/')) },
        _ => Check::Misconfigured(format!("{var} is not set")),
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn status(ok: bool, detail: impl Into<String>) -> Status {
    Status { ok, detail: detail.into(), checked_ms: now_ms() }
}

/// Run one check. `client` is None outside a pod.
pub async fn run(check: &Check, client: Option<&k8s::Client>) -> Status {
    match check {
        Check::Listen => status(true, "the front door listens itself; nothing to reach"),
        Check::Misconfigured(why) => status(false, why.clone()),
        Check::Http { url } => {
            let c = reqwest::Client::builder().no_proxy().timeout(HTTP_TIMEOUT).build();
            match c {
                Err(e) => status(false, format!("http client: {e}")),
                Ok(c) => match c.get(url).send().await {
                    Ok(r) if r.status().is_success() => status(true, format!("{url}: {}", r.status().as_u16())),
                    Ok(r) => status(false, format!("{url}: HTTP {}", r.status().as_u16())),
                    Err(e) => status(false, format!("{url}: {}", e.without_url())),
                },
            }
        }
        Check::AppsRbac => {
            let Some(c) = client else { return status(false, "not in a pod: no Kubernetes API") };
            match c.get(&format!("{}?limit=1", c.path("apps/v1", "deployments", None))).await {
                Ok(_) => status(true, format!("may list Deployments in {}", c.namespace())),
                Err(e) => status(false, e.to_string()),
            }
        }
        Check::PostgresOperator { cluster, user } => {
            let Some(c) = client else { return status(false, "not in a pod: no Kubernetes API") };
            let pg = match c.get(&c.path("acid.zalan.do/v1", "postgresqls", Some(cluster))).await {
                Ok(v) => v,
                Err(e) => return status(false, format!("postgresql/{cluster}: {e}")),
            };
            let phase = pg.pointer("/status/PostgresClusterStatus").and_then(Value::as_str).unwrap_or("unknown");
            if phase != "Running" {
                return status(false, format!("postgresql/{cluster} is {phase}"));
            }
            // The operator's Secret name: "_" in the user becomes "-".
            let secret = format!("{}.{cluster}.credentials.postgresql.acid.zalan.do", user.replace('_', "-"));
            match c.secret_data(&secret).await {
                Ok(d) if d.contains_key("username") && d.contains_key("password") => {
                    status(true, format!("postgresql/{cluster} Running; credentials for {user} readable"))
                }
                Ok(_) => status(false, format!("Secret {secret} has no username/password")),
                Err(e) => status(false, format!("Secret {secret}: {e}")),
            }
        }
    }
}

static STATUS: OnceLock<RwLock<BTreeMap<Feature, Status>>> = OnceLock::new();

fn store() -> &'static RwLock<BTreeMap<Feature, Status>> {
    STATUS.get_or_init(|| RwLock::new(BTreeMap::new()))
}

/// The latest answer for `feature`, as `/boot-status` shows it.
pub fn status_json(feature: Feature) -> Option<Value> {
    let m = store().read().ok()?;
    m.get(&feature).map(|s| serde_json::json!({"ok": s.ok, "detail": s.detail, "checkedMs": s.checked_ms}))
}

/// Start the drivers for this report (boot, once, inside the runtime). A
/// report with nothing delegated starts nothing.
pub fn start(report: &Report) {
    let checks = plan(report, &|k| std::env::var(k).ok());
    if checks.is_empty() {
        return;
    }
    let client = match k8s::Config::in_cluster(std::path::Path::new(k8s::SA_DIR)).and_then(k8s::Client::new) {
        Ok(c) => Some(c),
        Err(e) => {
            log_debug!("[capabilities] no Kubernetes API ({e}); kubernetes drivers report it");
            None
        }
    };
    tokio::spawn(async move {
        loop {
            for (feature, check) in &checks {
                let s = run(check, client.as_ref()).await;
                if let Ok(mut m) = store().write() {
                    let changed = m.get(feature).map(|old| (old.ok, &old.detail) != (s.ok, &s.detail)).unwrap_or(true);
                    if changed {
                        log_debug!("[capabilities] {} driver: {} ({})", feature.key(), if s.ok { "ok" } else { "not ok" }, s.detail);
                    }
                    m.insert(*feature, s);
                }
            }
            tokio::time::sleep(PERIOD).await;
        }
    });
}

#[cfg(test)]
mod tests;
