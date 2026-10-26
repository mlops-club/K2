//! A small Kubernetes API client for the controller (PRD P4; decision D1 in
//! docs/prd/progress/p4-plan.md: a typed REST client on the daemon's own
//! reqwest + rustls, not kube-rs).
//!
//! The controller runs in a pod with a ServiceAccount whose Role is limited
//! to its own namespace (deploy/kubernetes/core/rbac.yaml). This client
//! speaks to the API server the way that pod sees it:
//!
//! - base URL `https://$KUBERNETES_SERVICE_HOST:$KUBERNETES_SERVICE_PORT`;
//! - the CA in `/var/run/secrets/kubernetes.io/serviceaccount/ca.crt`, the
//!   only root it trusts;
//! - the bearer token in `.../token`, read again for every request
//!   (projected tokens rotate under a running pod);
//! - the namespace in `.../namespace`.
//!
//! Only JSON: `get`, `create`, `merge_patch` (RFC 7386, what K2's drivers
//! need: set fields, add map entries) and `delete` (a 404 counts as done).
//! An API refusal comes back as [`K8sError::Api`] with the server's own
//! `Status` reason and message, so a missing RBAC rule reads as
//! `Forbidden: … cannot patch resource "postgresqls" …`.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::Value;

/// Where the kubelet mounts a pod's ServiceAccount.
pub const SA_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";
const TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum K8sError {
    /// Not in a pod, or its ServiceAccount files are unreadable.
    Config(String),
    /// The request never got an HTTP answer.
    Http(String),
    /// The API server answered with an error status.
    Api { status: u16, reason: String, message: String },
}

impl std::fmt::Display for K8sError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            K8sError::Config(m) => write!(f, "kubernetes config: {m}"),
            K8sError::Http(m) => write!(f, "kubernetes request: {m}"),
            K8sError::Api { status, reason, message } => write!(f, "kubernetes {status} {reason}: {message}"),
        }
    }
}

impl std::error::Error for K8sError {}

impl K8sError {
    pub fn is_not_found(&self) -> bool {
        matches!(self, K8sError::Api { status: 404, .. })
    }
    pub fn is_conflict(&self) -> bool {
        matches!(self, K8sError::Api { status: 409, .. })
    }
}

/// How to reach the API server.
#[derive(Debug, Clone)]
pub struct Config {
    pub base: String,
    pub namespace: String,
    token_file: PathBuf,
    ca_pem: Option<Vec<u8>>,
}

impl Config {
    /// The pod's own view: `KUBERNETES_SERVICE_HOST`/`_PORT` and the
    /// ServiceAccount files under `sa_dir` (normally [`SA_DIR`]).
    pub fn in_cluster(sa_dir: &Path) -> Result<Config, K8sError> {
        let host = std::env::var("KUBERNETES_SERVICE_HOST")
            .map_err(|_| K8sError::Config("KUBERNETES_SERVICE_HOST is not set (not in a pod?)".into()))?;
        let port = std::env::var("KUBERNETES_SERVICE_PORT").unwrap_or_else(|_| "443".into());
        let host = if host.contains(':') { format!("[{host}]") } else { host };
        let ca = std::fs::read(sa_dir.join("ca.crt"))
            .map_err(|e| K8sError::Config(format!("{}: {e}", sa_dir.join("ca.crt").display())))?;
        let namespace = std::fs::read_to_string(sa_dir.join("namespace"))
            .map_err(|e| K8sError::Config(format!("{}: {e}", sa_dir.join("namespace").display())))?
            .trim()
            .to_string();
        Ok(Config { base: format!("https://{host}:{port}"), namespace, token_file: sa_dir.join("token"), ca_pem: Some(ca) })
    }

    /// Any base URL, a token file and a namespace; `ca_pem` None trusts the
    /// built-in roots (tests use plain `http://` and never get there).
    pub fn new(base: &str, namespace: &str, token_file: &Path, ca_pem: Option<Vec<u8>>) -> Config {
        Config {
            base: base.trim_end_matches('/').to_string(),
            namespace: namespace.to_string(),
            token_file: token_file.to_path_buf(),
            ca_pem,
        }
    }
}

/// The client. Cheap to clone (reqwest's client is an `Arc`).
#[derive(Debug, Clone)]
pub struct Client {
    http: reqwest::Client,
    cfg: Config,
}

impl Client {
    pub fn new(cfg: Config) -> Result<Client, K8sError> {
        let mut b = reqwest::Client::builder()
            .use_rustls_tls()
            .min_tls_version(reqwest::tls::Version::TLS_1_2)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(TIMEOUT)
            .user_agent(concat!("k2-daemon/", env!("CARGO_PKG_VERSION")));
        if let Some(pem) = &cfg.ca_pem {
            let ca = reqwest::Certificate::from_pem(pem).map_err(|e| K8sError::Config(format!("ca.crt: {e}")))?;
            b = b.tls_built_in_root_certs(false).add_root_certificate(ca);
        }
        let http = b.build().map_err(|e| K8sError::Config(format!("http client: {e}")))?;
        Ok(Client { http, cfg })
    }

    /// The namespace this controller lives in (and the only one it may touch).
    pub fn namespace(&self) -> &str {
        &self.cfg.namespace
    }

    /// `/api/v1/namespaces/<ns>/<resource>[/<name>]` for core kinds
    /// (`group_version` "v1") or `/apis/<group>/<version>/...` otherwise.
    pub fn path(&self, group_version: &str, resource: &str, name: Option<&str>) -> String {
        let root = if group_version == "v1" { "/api/v1".to_string() } else { format!("/apis/{group_version}") };
        let ns = &self.cfg.namespace;
        match name {
            Some(n) => format!("{root}/namespaces/{ns}/{resource}/{n}"),
            None => format!("{root}/namespaces/{ns}/{resource}"),
        }
    }

    fn token(&self) -> Result<String, K8sError> {
        let t = std::fs::read_to_string(&self.cfg.token_file)
            .map_err(|e| K8sError::Config(format!("{}: {e}", self.cfg.token_file.display())))?;
        Ok(t.trim().to_string())
    }

    async fn send(&self, method: reqwest::Method, path: &str, body: Option<(&str, &Value)>) -> Result<Value, K8sError> {
        let url = format!("{}{}", self.cfg.base, path);
        let mut req = self.http.request(method, &url).bearer_auth(self.token()?).header("Accept", "application/json");
        if let Some((content_type, v)) = body {
            req = req.header("Content-Type", content_type).body(v.to_string());
        }
        let resp = req.send().await.map_err(|e| K8sError::Http(e.without_url().to_string()))?;
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(|e| K8sError::Http(e.without_url().to_string()))?;
        let doc: Value = if text.trim().is_empty() { Value::Null } else { serde_json::from_str(&text).unwrap_or(Value::Null) };
        if (200..300).contains(&status) {
            return Ok(doc);
        }
        // A metav1.Status: {"reason": "Forbidden", "message": "..."}.
        let reason = doc.get("reason").and_then(Value::as_str).unwrap_or("").to_string();
        let message = doc.get("message").and_then(Value::as_str).map(str::to_string).unwrap_or_else(|| {
            let t: String = text.chars().take(300).collect();
            t
        });
        Err(K8sError::Api { status, reason, message })
    }

    pub async fn get(&self, path: &str) -> Result<Value, K8sError> {
        self.send(reqwest::Method::GET, path, None).await
    }

    /// POST to a collection path (`path(.., None)`).
    pub async fn create(&self, collection: &str, obj: &Value) -> Result<Value, K8sError> {
        self.send(reqwest::Method::POST, collection, Some(("application/json", obj))).await
    }

    /// RFC 7386 merge patch of one object.
    pub async fn merge_patch(&self, path: &str, patch: &Value) -> Result<Value, K8sError> {
        self.send(reqwest::Method::PATCH, path, Some(("application/merge-patch+json", patch))).await
    }

    /// Delete one object; true if it was there, false on 404.
    pub async fn delete(&self, path: &str) -> Result<bool, K8sError> {
        match self.send(reqwest::Method::DELETE, path, None).await {
            Ok(_) => Ok(true),
            Err(e) if e.is_not_found() => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Create, or merge-patch when it already exists (409): converge on
    /// `obj` without a server-side apply field manager.
    pub async fn create_or_patch(&self, collection: &str, name: &str, obj: &Value) -> Result<Value, K8sError> {
        match self.create(collection, obj).await {
            Err(e) if e.is_conflict() => self.merge_patch(&format!("{collection}/{name}"), obj).await,
            other => other,
        }
    }

    /// A Secret's `data` decoded: key → bytes. Missing keys are absent.
    pub async fn secret_data(&self, name: &str) -> Result<std::collections::BTreeMap<String, Vec<u8>>, K8sError> {
        use base64::Engine as _;
        let doc = self.get(&self.path("v1", "secrets", Some(name))).await?;
        let mut out = std::collections::BTreeMap::new();
        if let Some(m) = doc.get("data").and_then(Value::as_object) {
            for (k, v) in m {
                if let Some(s) = v.as_str() {
                    if let Ok(b) = base64::engine::general_purpose::STANDARD.decode(s) {
                        out.insert(k.clone(), b);
                    }
                }
            }
        }
        Ok(out)
    }
}

/// A future [`blocking`] runs.
pub type CallFuture<T> = std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, String>> + Send>>;

/// One call to this pod's API server (in-cluster config) from blocking
/// code: on its own thread with a current-thread runtime, so it is safe
/// from route handlers, the App supervisor's threads and plain threads.
pub fn blocking<T: Send + 'static>(f: impl FnOnce(Client) -> CallFuture<T> + Send + 'static) -> Result<T, String> {
    std::thread::spawn(move || {
        let cfg = Config::in_cluster(std::path::Path::new(SA_DIR)).map_err(|e| e.to_string())?;
        let client = Client::new(cfg).map_err(|e| e.to_string())?;
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().map_err(|e| e.to_string())?;
        rt.block_on(f(client))
    })
    .join()
    .map_err(|_| "the Kubernetes call panicked".to_string())?
}

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
