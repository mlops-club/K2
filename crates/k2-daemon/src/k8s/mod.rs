//! A small typed Kubernetes API client (PRD "K2 Distributed Runtime" P4,
//! task T1, decision D1): the dozen calls the controller's Kubernetes
//! provisioner makes, on the daemon's own HTTP stack (blocking `reqwest`,
//! rustls) with the pod's ServiceAccount. No `kube-rs`.
//!
//! Every call blocks: run it on a blocking thread
//! (`tokio::task::spawn_blocking`), never on a runtime worker.
//!
//! RBAC this needs (namespace-scoped, `deploy/kubernetes/core/rbac.yaml`
//! plus D8's rule): get/patch `statefulsets/scale` on `k2-host`, get/list/
//! watch `pods`, patch the Secret `k2-host-enroll`.
//!
//! The host pool that uses it is `provisioner::kubernetes`.

#[cfg(test)]
pub(crate) mod fake;

use std::fmt;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::Duration;

use base64::Engine;
use serde_json::{json, Value};

/// Where a pod's ServiceAccount is mounted.
pub const SA_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// One API call's deadline (a watch has none: it streams until closed).
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// How to reach the API server.
#[derive(Debug, Clone)]
pub struct Config {
    /// `https://10.96.0.1:443` in a pod; `http://127.0.0.1:N` for a test.
    pub base: String,
    pub namespace: String,
    /// Bearer token: re-read from `token_file` on every call (projected
    /// ServiceAccount tokens rotate), or fixed.
    pub token: Token,
    /// PEM of the cluster CA (`ca.crt`); `None` for plain HTTP tests.
    pub ca_pem: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub enum Token {
    File(PathBuf),
    Fixed(String),
    None,
}

impl Config {
    /// The pod's own: `KUBERNETES_SERVICE_HOST`/`_PORT` and the
    /// ServiceAccount files under [`SA_DIR`].
    pub fn in_cluster() -> Result<Config, String> {
        Self::in_cluster_from(|k| std::env::var(k).ok(), &PathBuf::from(SA_DIR))
    }

    pub fn in_cluster_from(get: impl Fn(&str) -> Option<String>, sa_dir: &std::path::Path) -> Result<Config, String> {
        let host = get("KUBERNETES_SERVICE_HOST").filter(|v| !v.is_empty()).ok_or("not in a Kubernetes pod (KUBERNETES_SERVICE_HOST unset)")?;
        let port = get("KUBERNETES_SERVICE_PORT").filter(|v| !v.is_empty()).unwrap_or_else(|| "443".into());
        // An IPv6 service host needs brackets in a URL.
        let host = if host.contains(':') { format!("[{host}]") } else { host };
        let read = |name: &str| std::fs::read(sa_dir.join(name)).map_err(|e| format!("{}: {e}", sa_dir.join(name).display()));
        let namespace = String::from_utf8_lossy(&read("namespace")?).trim().to_string();
        let ca_pem = read("ca.crt")?;
        let token = sa_dir.join("token");
        if !token.is_file() {
            return Err(format!("{}: no ServiceAccount token (automountServiceAccountToken off?)", token.display()));
        }
        Ok(Config { base: format!("https://{host}:{port}"), namespace, token: Token::File(token), ca_pem: Some(ca_pem) })
    }
}

/// A failed call: the API's own answer, or the transport's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The API server answered `code` with a `Status` (`reason`: `NotFound`,
    /// `Forbidden`, `Conflict`, …).
    Api { code: u16, reason: String, message: String },
    Transport(String),
}

impl Error {
    pub fn is_not_found(&self) -> bool {
        matches!(self, Error::Api { code: 404, .. })
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Api { code, reason, message } => write!(f, "Kubernetes API {code} {reason}: {message}"),
            Error::Transport(e) => write!(f, "Kubernetes API unreachable: {e}"),
        }
    }
}

impl From<Error> for String {
    fn from(e: Error) -> String {
        e.to_string()
    }
}

/// `statefulsets/<name>/scale`: what was asked and what runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scale {
    pub spec: u32,
    pub status: u32,
}

/// What the provisioner reads of a pod.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pod {
    pub name: String,
    /// `Pending`, `Running`, `Succeeded`, `Failed`, `Unknown`.
    pub phase: String,
    /// The `Ready` condition (never the provisioner's signal: a host counts
    /// once its signed hello arrives).
    pub ready: bool,
    pub node: Option<String>,
    /// `metadata.deletionTimestamp` set: terminating.
    pub deleting: bool,
}

impl Pod {
    fn from_json(v: &Value) -> Pod {
        let ready = v["status"]["conditions"]
            .as_array()
            .is_some_and(|cs| cs.iter().any(|c| c["type"] == "Ready" && c["status"] == "True"));
        Pod {
            name: v["metadata"]["name"].as_str().unwrap_or_default().to_string(),
            phase: v["status"]["phase"].as_str().unwrap_or("Unknown").to_string(),
            ready,
            node: v["spec"]["nodeName"].as_str().map(str::to_string),
            deleting: !v["metadata"]["deletionTimestamp"].is_null(),
        }
    }
}

/// One event of a pod watch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodEvent {
    Added(Pod),
    Modified(Pod),
    Deleted(Pod),
    /// The watch's resourceVersion expired (410 Gone): list again.
    Expired,
}

pub struct Client {
    /// Built on the first call, which runs on a blocking thread: a blocking
    /// `reqwest` client owns a runtime, and building or dropping one on an
    /// async worker panics. A `Client` lives as long as the provisioner.
    http: std::sync::OnceLock<reqwest::blocking::Client>,
    ca: Option<reqwest::Certificate>,
    cfg: Config,
}

impl Client {
    /// Checks the CA now; connects on the first call.
    pub fn new(cfg: Config) -> Result<Client, String> {
        let ca = match &cfg.ca_pem {
            Some(pem) => Some(reqwest::Certificate::from_pem(pem).map_err(|e| format!("cluster CA: {e}"))?),
            None => None,
        };
        Ok(Client { http: std::sync::OnceLock::new(), ca, cfg })
    }

    fn http(&self) -> Result<&reqwest::blocking::Client, Error> {
        if let Some(h) = self.http.get() {
            return Ok(h);
        }
        let mut b = reqwest::blocking::Client::builder().connect_timeout(Duration::from_secs(10)).timeout(None::<Duration>);
        if let Some(ca) = &self.ca {
            b = b.tls_built_in_root_certs(false).add_root_certificate(ca.clone());
        }
        let built = b.build().map_err(|e| Error::Transport(format!("Kubernetes client: {e}")))?;
        Ok(self.http.get_or_init(|| built))
    }

    pub fn namespace(&self) -> &str {
        &self.cfg.namespace
    }

    fn token(&self) -> Result<Option<String>, Error> {
        match &self.cfg.token {
            Token::File(p) => std::fs::read_to_string(p)
                .map(|t| Some(t.trim().to_string()))
                .map_err(|e| Error::Transport(format!("ServiceAccount token {}: {e}", p.display()))),
            Token::Fixed(t) => Ok(Some(t.clone())),
            Token::None => Ok(None),
        }
    }

    fn request(&self, method: reqwest::Method, path: &str, body: Option<(&str, Value)>, timeout: Option<Duration>) -> Result<reqwest::blocking::Response, Error> {
        let mut r = self.http()?.request(method, format!("{}{path}", self.cfg.base)).header("Accept", "application/json");
        if let Some(t) = timeout {
            r = r.timeout(t);
        }
        if let Some(t) = self.token()? {
            r = r.bearer_auth(t);
        }
        if let Some((content_type, v)) = body {
            r = r.header("Content-Type", content_type).body(v.to_string());
        }
        let resp = r.send().map_err(|e| Error::Transport(e.to_string()))?;
        if resp.status().is_success() {
            return Ok(resp);
        }
        let code = resp.status().as_u16();
        let text = resp.text().unwrap_or_default();
        let status: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        Err(Error::Api {
            code,
            reason: status["reason"].as_str().unwrap_or("").to_string(),
            message: status["message"].as_str().map(str::to_string).unwrap_or_else(|| text.chars().take(200).collect()),
        })
    }

    fn json(&self, method: reqwest::Method, path: &str, body: Option<(&str, Value)>) -> Result<Value, Error> {
        let resp = self.request(method, path, body, Some(CALL_TIMEOUT))?;
        let text = resp.text().map_err(|e| Error::Transport(format!("read {path}: {e}")))?;
        serde_json::from_str(&text).map_err(|e| Error::Transport(format!("bad JSON from {path}: {e}")))
    }

    fn ns(&self) -> &str {
        &self.cfg.namespace
    }

    // ── StatefulSet scale ───────────────────────────────────────────────

    pub fn get_scale(&self, statefulset: &str) -> Result<Scale, Error> {
        let v = self.json(reqwest::Method::GET, &format!("/apis/apps/v1/namespaces/{}/statefulsets/{statefulset}/scale", self.ns()), None)?;
        Ok(scale_of(&v))
    }

    /// Set `replicas` (a merge patch of the scale subresource).
    pub fn set_replicas(&self, statefulset: &str, replicas: u32) -> Result<Scale, Error> {
        let v = self.json(
            reqwest::Method::PATCH,
            &format!("/apis/apps/v1/namespaces/{}/statefulsets/{statefulset}/scale", self.ns()),
            Some(("application/merge-patch+json", json!({"spec": {"replicas": replicas}}))),
        )?;
        Ok(scale_of(&v))
    }

    // ── Pods ───────────────────────────────────────────────────────────

    pub fn list_pods(&self, label_selector: &str) -> Result<(Vec<Pod>, String), Error> {
        let v = self.json(
            reqwest::Method::GET,
            &format!("/api/v1/namespaces/{}/pods?labelSelector={}", self.ns(), urlencode(label_selector)),
            None,
        )?;
        let pods = v["items"].as_array().map(|a| a.iter().map(Pod::from_json).collect()).unwrap_or_default();
        Ok((pods, v["metadata"]["resourceVersion"].as_str().unwrap_or_default().to_string()))
    }

    pub fn get_pod(&self, name: &str) -> Result<Option<Pod>, Error> {
        match self.json(reqwest::Method::GET, &format!("/api/v1/namespaces/{}/pods/{name}", self.ns()), None) {
            Ok(v) => Ok(Some(Pod::from_json(&v))),
            Err(e) if e.is_not_found() => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Watch pods matching `label_selector` from `resource_version` (from
    /// [`Self::list_pods`]): events until the server ends the watch.
    pub fn watch_pods(&self, label_selector: &str, resource_version: &str) -> Result<impl Iterator<Item = Result<PodEvent, Error>>, Error> {
        let resp = self.request(
            reqwest::Method::GET,
            &format!(
                "/api/v1/namespaces/{}/pods?watch=1&labelSelector={}&resourceVersion={}&allowWatchBookmarks=false",
                self.ns(),
                urlencode(label_selector),
                urlencode(resource_version)
            ),
            None,
            None,
        )?;
        Ok(BufReader::new(resp).lines().filter_map(|line| match line {
            Err(e) => Some(Err(Error::Transport(e.to_string()))),
            Ok(l) if l.trim().is_empty() => None,
            Ok(l) => Some(watch_event(&l)),
        }))
    }

    // ── Secrets ────────────────────────────────────────────────────────

    /// Set `data[key]` of the Secret `name` to `value` (merge patch; other
    /// keys stay). The Secret must exist: RBAC grants `patch` by name only.
    pub fn put_secret_key(&self, name: &str, key: &str, value: &[u8]) -> Result<(), Error> {
        let b64 = base64::engine::general_purpose::STANDARD.encode(value);
        self.json(
            reqwest::Method::PATCH,
            &format!("/api/v1/namespaces/{}/secrets/{name}", self.ns()),
            Some(("application/merge-patch+json", json!({"data": {key: b64}}))),
        )
        .map(|_| ())
    }

    /// Remove `data[key]` from the Secret `name`.
    pub fn remove_secret_key(&self, name: &str, key: &str) -> Result<(), Error> {
        self.json(
            reqwest::Method::PATCH,
            &format!("/api/v1/namespaces/{}/secrets/{name}", self.ns()),
            Some(("application/merge-patch+json", json!({"data": {key: null}}))),
        )
        .map(|_| ())
    }
}

fn scale_of(v: &Value) -> Scale {
    let n = |x: &Value| x.as_u64().unwrap_or(0) as u32;
    Scale { spec: n(&v["spec"]["replicas"]), status: n(&v["status"]["replicas"]) }
}

fn watch_event(line: &str) -> Result<PodEvent, Error> {
    let v: Value = serde_json::from_str(line).map_err(|e| Error::Transport(format!("bad watch line: {e}")))?;
    let pod = || Pod::from_json(&v["object"]);
    match v["type"].as_str() {
        Some("ADDED") => Ok(PodEvent::Added(pod())),
        Some("MODIFIED") => Ok(PodEvent::Modified(pod())),
        Some("DELETED") => Ok(PodEvent::Deleted(pod())),
        Some("ERROR") if v["object"]["code"] == 410 => Ok(PodEvent::Expired),
        Some("ERROR") => Err(Error::Api {
            code: v["object"]["code"].as_u64().unwrap_or(0) as u16,
            reason: v["object"]["reason"].as_str().unwrap_or("").to_string(),
            message: v["object"]["message"].as_str().unwrap_or("").to_string(),
        }),
        other => Err(Error::Transport(format!("unknown watch event type {other:?}"))),
    }
}

/// Percent-encode a query value (label selectors carry `=`, `,`, `!`).
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests;
