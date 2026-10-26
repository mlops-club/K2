//! The Kubernetes engine (PRD P4 T3, decision 2; docs/prd/progress/
//! k8s-capabilities.md): with `K2_CAP_APPS=kubernetes`, each App instance
//! is a Deployment in the controller's namespace instead of a Podman unit
//! on this machine. `RuntimeEngine` is unchanged; the shared pipeline
//! (health gate, switch, drain, rollback) runs as it does for the guarded
//! engine, against the instance's Service ([`upstream_host`]).
//!
//! One instance = three objects named `app-<app>-<deployment>`:
//! - **Deployment**: the pinned base image the guarded engine uses (no
//!   per-App image, no registry: D6 revised). An init container fetches the
//!   Seed from the controller ([`handle_seed_fetch`]) with a per-App token,
//!   checks its sha256 (from the pod spec, which the API server holds) and
//!   unpacks it into `/app`; the App runs read-only from there;
//! - **Service**: the instance's port, for the controller only;
//! - **NetworkPolicy**: in from the controller on that port; out to the
//!   controller, the operator's Postgres, DNS and HTTPS on the internet (as
//!   `deploy/kubernetes/examples/app.yaml`).
//! The Service and policy are owned by the Deployment: deleting it (stop)
//! removes all three. The Seed token is in the init container's env, not a
//! Secret: it opens only this App's own code, which the pod holds anyway,
//! and the controller then needs no right to create Secrets.
//!
//! Not here yet (recorded in the progress doc): the App gateway link
//! (`K2_GATEWAY_URL`, S14), `exec`, and data that survives the instance
//! (the data and home folders are emptyDirs).

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use k2_core::deploy::engine::{
    AppSpec, CgroupStats, EngineCaps, EngineError, ExecStream, Isolation, Liveness, Prepared, Result, RunHandle, RuntimeEngine,
    SeedRef,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// `engine_ref` prefix that marks a handle as this engine's; the rest is
/// the instance's object name, which is also its Service's host name.
pub const REF_PREFIX: &str = "k8s:";
/// The uid the App runs as in its pod (not the controller's).
pub const APP_UID: i64 = 20001;
const SEED_ROUTE: &str = "/cli/apps/k8s-seed/";

/// `K2_CAP_APPS=kubernetes`.
pub fn enabled() -> bool {
    enabled_from(&|k| std::env::var(k).ok())
}

pub fn enabled_from(get: &dyn Fn(&str) -> Option<String>) -> bool {
    get("K2_CAP_APPS").as_deref() == Some("kubernetes")
}

/// Does `h` belong to this engine?
pub fn owns(h: &RunHandle) -> bool {
    h.engine_ref.starts_with(REF_PREFIX)
}

/// Where the router and the health probes reach an instance: its
/// Service's name (same namespace) for this engine's handles; `None`
/// (host loopback) for every other engine's.
pub fn upstream_host(h: &RunHandle) -> Option<String> {
    h.engine_ref.strip_prefix(REF_PREFIX).filter(|n| !n.is_empty()).map(str::to_string)
}

/// A DNS-1123 label piece: lower-case alphanumerics and '-'.
fn dns_piece(s: &str) -> String {
    let mut out: String = s
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '-' })
        .collect();
    while out.contains("--") {
        out = out.replace("--", "-");
    }
    out.trim_matches('-').to_string()
}

/// `app-<app>-<first 8 of the deployment>`, at most 63 characters.
pub fn object_name(app_id: &str, deployment_id: &str) -> String {
    let dep: String = dns_piece(deployment_id).chars().filter(|c| *c != '-').take(8).collect();
    let mut app = dns_piece(app_id);
    let room = 63 - "app-".len() - 1 - dep.len();
    app.truncate(room);
    let app = app.trim_end_matches('-');
    format!("app-{app}-{dep}")
}

fn labels(spec_app: &str, name: &str) -> Value {
    json!({
        "k2.dev/role": "app",
        "k2.dev/app": dns_piece(spec_app).chars().take(63).collect::<String>().trim_end_matches('-'),
        "k2.dev/instance": name,
        "app.kubernetes.io/managed-by": "k2-controller",
    })
}

// ── the Seed: packed once, served to the instance's init container ─────

/// Where a packed Seed lives on the controller.
pub fn seed_tar_path(hash: &str) -> PathBuf {
    k2_core::paths::k2_home().join("apps").join("k8s-seeds").join(format!("{hash}.tar"))
}

/// Tar the stored Seed (deterministic, as the guarded engine does) once,
/// and answer its sha256.
pub fn pack_seed(seed: &SeedRef) -> std::result::Result<(PathBuf, String), String> {
    if !k2_core::deploy::seed::valid_hash(&seed.hash) {
        return Err(format!("not a Seed hash: {}", seed.hash));
    }
    let path = seed_tar_path(&seed.hash);
    if !path.exists() {
        let dir = path.parent().ok_or("no seeds folder")?;
        std::fs::create_dir_all(dir).map_err(|e| format!("mkdir {}: {e}", dir.display()))?;
        let tmp = dir.join(format!(".tmp-{}-{}", &seed.hash[..12], std::process::id()));
        let res = (|| -> std::result::Result<(), String> {
            let f = std::fs::File::create(&tmp).map_err(|e| e.to_string())?;
            let mut b = tar::Builder::new(f);
            b.follow_symlinks(false);
            b.mode(tar::HeaderMode::Deterministic);
            // Each top-level entry by its own name, no "." entry: `/app`
            // is the volume's root, which the App's uid can't chmod or
            // utime, so tar would fail on it.
            let mut top: Vec<_> = std::fs::read_dir(&seed.path)
                .map_err(|e| format!("read the Seed: {e}"))?
                .map(|e| e.map(|e| e.file_name()))
                .collect::<std::io::Result<_>>()
                .map_err(|e| format!("read the Seed: {e}"))?;
            top.sort();
            for name in top {
                let p = seed.path.join(&name);
                let md = std::fs::symlink_metadata(&p).map_err(|e| format!("{}: {e}", p.display()))?;
                if md.is_dir() {
                    b.append_dir_all(&name, &p)
                } else {
                    b.append_path_with_name(&p, &name)
                }
                .map_err(|e| format!("tar the Seed: {e}"))?;
            }
            b.into_inner().and_then(|f| f.sync_all()).map_err(|e| e.to_string())?;
            std::fs::rename(&tmp, &path).map_err(|e| e.to_string())
        })();
        if res.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        res?;
    }
    Ok((path.clone(), sha256_file(&path)?))
}

fn sha256_file(p: &Path) -> std::result::Result<String, String> {
    let mut f = std::fs::File::open(p).map_err(|e| format!("{}: {e}", p.display()))?;
    let mut h = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(hex(&h.finalize()))
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// HMAC-SHA256 (RFC 2104).
fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let (mut ipad, mut opad) = ([0x36u8; 64], [0x5cu8; 64]);
    for i in 0..64 {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let inner = Sha256::new().chain_update(ipad).chain_update(msg).finalize();
    Sha256::new().chain_update(opad).chain_update(inner).finalize().into()
}

/// The controller's Seed key (32 random bytes, 0600), made once.
fn seed_key() -> std::result::Result<Vec<u8>, String> {
    let p = k2_core::paths::k2_home().join("apps").join("k8s-seed.key");
    if let Ok(k) = std::fs::read(&p) {
        if k.len() == 32 {
            return Ok(k);
        }
    }
    let mut k = vec![0u8; 32];
    getrandom::getrandom(&mut k).map_err(|e| format!("os rng: {e}"))?;
    if let Some(d) = p.parent() {
        std::fs::create_dir_all(d).map_err(|e| e.to_string())?;
    }
    let mut o = std::fs::OpenOptions::new();
    o.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.mode(0o600);
    }
    o.open(&p).and_then(|mut f| f.write_all(&k)).map_err(|e| format!("{}: {e}", p.display()))?;
    Ok(k)
}

/// The token that lets `app_id`'s instance fetch Seed `hash` (and nothing
/// else): HMAC(controller key, app ∥ hash). Stateless, so a controller
/// restart keeps it valid.
pub fn seed_token_with(key: &[u8], app_id: &str, hash: &str) -> String {
    hex(&hmac_sha256(key, format!("k2-k8s-seed\0{app_id}\0{hash}").as_bytes()))
}

pub fn seed_token(app_id: &str, hash: &str) -> std::result::Result<String, String> {
    Ok(seed_token_with(&seed_key()?, app_id, hash))
}

/// Constant-time compare.
fn same(a: &str, b: &str) -> bool {
    a.len() == b.len() && a.bytes().zip(b.bytes()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// `GET /cli/apps/k8s-seed/<app>/<hash>` with `Authorization: Bearer
/// <token>`: the packed Seed's path, or (status, why). Checks the token
/// against the App and hash it names; serves only a Seed already packed
/// for an instance.
pub fn handle_seed_fetch(path: &str, bearer: Option<&str>) -> std::result::Result<PathBuf, (&'static str, &'static str)> {
    if !enabled() {
        return Err(("404 Not Found", "not_found"));
    }
    let rest = path.strip_prefix(SEED_ROUTE).ok_or(("404 Not Found", "not_found"))?;
    let (app, hash) = rest.split_once('/').ok_or(("404 Not Found", "not_found"))?;
    if app.is_empty() || !k2_core::deploy::seed::valid_hash(hash) {
        return Err(("404 Not Found", "not_found"));
    }
    let want = seed_token(app, hash).map_err(|_| ("503 Service Unavailable", "seed_key"))?;
    match bearer {
        Some(t) if same(t, &want) => {}
        _ => return Err(("403 Forbidden", "seed_token")),
    }
    let p = seed_tar_path(hash);
    if !p.is_file() {
        return Err(("404 Not Found", "seed_not_packed"));
    }
    Ok(p)
}

// ── the objects ─────────────────────────────────────────────────────────

/// What `start` needs beyond the spec (the cluster side).
#[derive(Debug, Clone, PartialEq)]
pub struct Placement {
    /// `docker.io/library/node@sha256:…` (guarded: node24; open: node24-full).
    pub image: String,
    /// The controller's Service, as the instance reaches it.
    pub controller: String,
    pub controller_port: u16,
    /// The operator's Postgres cluster (`K2_POSTGRES_CLUSTER`).
    pub postgres_cluster: String,
    pub seed_hash: String,
    pub seed_sha256: String,
}

impl Placement {
    pub fn from_env(spec: &AppSpec, seed_hash: &str, seed_sha256: &str) -> std::result::Result<Placement, String> {
        let arch = std::env::consts::ARCH;
        let base = if spec.wall.open {
            super::guarded::full_image_for(arch)
        } else {
            super::guarded::base_image_for(arch)
        }
        .ok_or_else(|| format!("no pinned base image for {arch}"))?;
        let env = |k: &str, d: &str| std::env::var(k).ok().filter(|v| !v.trim().is_empty()).unwrap_or_else(|| d.to_string());
        Ok(Placement {
            image: format!("docker.io/library/node@{}", base.digest),
            controller: env("K2_CONTROLLER_SERVICE", "k2-controller"),
            controller_port: env("K2_DAEMON_PORT", "7420").parse().unwrap_or(7420),
            postgres_cluster: env("K2_POSTGRES_CLUSTER", "k2-pg"),
            seed_hash: seed_hash.to_string(),
            seed_sha256: seed_sha256.to_string(),
        })
    }
}

/// The init container's fetch: the Seed over HTTP from the controller with
/// the token, its sha256 checked against the pod spec's, unpacked into
/// `/app`. Node's own `fetch` and Debian's `tar`: nothing else in the image.
pub const FETCH_JS: &str = r#"const c=require('crypto'),fs=require('fs'),cp=require('child_process');
(async()=>{const r=await fetch(process.env.K2_SEED_URL,{headers:{authorization:'Bearer '+process.env.K2_SEED_TOKEN}});
if(!r.ok){console.error('k2 seed: HTTP '+r.status);process.exit(1)}
const b=Buffer.from(await r.arrayBuffer());const h=c.createHash('sha256').update(b).digest('hex');
if(h!==process.env.K2_SEED_SHA256){console.error('k2 seed: sha256 mismatch');process.exit(1)}
const t='/app/.k2-seed.tar';fs.writeFileSync(t,b);cp.execFileSync('tar',['-xmf',t,'-C','/app','--no-same-owner']);fs.unlinkSync(t);
console.log('k2 seed: '+b.length+' bytes, sha256 ok')})().catch(e=>{console.error('k2 seed: '+e.message);process.exit(1)})"#;

fn container_security() -> Value {
    json!({"allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true, "capabilities": {"drop": ["ALL"]}})
}

/// The App's environment: its manifest's, then K2's own (they win).
pub fn app_env(spec: &AppSpec, port: u16) -> BTreeMap<String, String> {
    let mut env = spec.env.clone();
    env.insert("PORT".into(), port.to_string());
    env.insert("HOME".into(), "/home/app".into());
    env.insert("K2_APP_ID".into(), spec.app_id.clone());
    if let Some(o) = spec.public_origins.first() {
        env.insert("K2_APP_ORIGIN".into(), o.clone());
        env.insert("K2_APP_ORIGINS".into(), spec.public_origins.join(","));
    }
    env
}

pub fn deployment(spec: &AppSpec, port: u16, pl: &Placement, name: &str, token: &str) -> Value {
    let l = labels(&spec.app_id, name);
    let env: Vec<Value> = app_env(spec, port).into_iter().map(|(k, v)| json!({"name": k, "value": v})).collect();
    let mut limits = json!({});
    if let Some(m) = spec.memory_limit_bytes.filter(|m| *m > 0) {
        limits = json!({"memory": m.to_string()});
    }
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": name,
            "labels": l,
            "annotations": {
                "k2.dev/app-id": spec.app_id,
                "k2.dev/deployment-id": spec.deployment_id,
                "k2.dev/seed": pl.seed_hash,
                "k2.dev/port": port.to_string(),
            },
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"k2.dev/instance": name}},
            "template": {
                "metadata": {"labels": l},
                "spec": {
                    "automountServiceAccountToken": false,
                    "enableServiceLinks": false,
                    "terminationGracePeriodSeconds": spec.drain_secs.max(1),
                    "securityContext": {
                        "runAsNonRoot": true, "runAsUser": APP_UID, "runAsGroup": APP_UID, "fsGroup": APP_UID,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "initContainers": [{
                        "name": "seed",
                        "image": pl.image,
                        "command": ["node", "-e", FETCH_JS],
                        "env": [
                            {"name": "K2_SEED_URL", "value": format!("http://{}:{}{SEED_ROUTE}{}/{}", pl.controller, pl.controller_port, spec.app_id, pl.seed_hash)},
                            {"name": "K2_SEED_SHA256", "value": pl.seed_sha256},
                            {"name": "K2_SEED_TOKEN", "value": token},
                        ],
                        "volumeMounts": [{"name": "app", "mountPath": "/app"}],
                        "resources": {"requests": {"cpu": "50m", "memory": "64Mi"}, "limits": {"memory": "256Mi"}},
                        "securityContext": container_security(),
                    }],
                    "containers": [{
                        "name": "app",
                        "image": pl.image,
                        "workingDir": "/app",
                        "command": spec.argv,
                        "env": env,
                        "ports": [{"name": "http", "containerPort": port}],
                        "volumeMounts": [
                            {"name": "app", "mountPath": "/app", "readOnly": true},
                            {"name": "data", "mountPath": "/data"},
                            {"name": "home", "mountPath": "/home/app"},
                            {"name": "tmp", "mountPath": "/tmp"},
                        ],
                        "resources": {"requests": {"cpu": "50m", "memory": "128Mi"}, "limits": limits},
                        "securityContext": container_security(),
                    }],
                    "volumes": [
                        {"name": "app", "emptyDir": {}},
                        {"name": "data", "emptyDir": {}},
                        {"name": "home", "emptyDir": {}},
                        {"name": "tmp", "emptyDir": {}},
                    ],
                },
            },
        },
    })
}

fn owned_by(name: &str, uid: &str) -> Value {
    json!([{"apiVersion": "apps/v1", "kind": "Deployment", "name": name, "uid": uid}])
}

pub fn service(spec: &AppSpec, port: u16, name: &str, owner_uid: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": name, "labels": labels(&spec.app_id, name), "ownerReferences": owned_by(name, owner_uid)},
        "spec": {"selector": {"k2.dev/instance": name}, "ports": [{"name": "http", "port": port, "targetPort": "http"}]},
    })
}

pub fn network_policy(spec: &AppSpec, port: u16, pl: &Placement, name: &str, owner_uid: &str) -> Value {
    json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"name": name, "labels": labels(&spec.app_id, name), "ownerReferences": owned_by(name, owner_uid)},
        "spec": {
            "podSelector": {"matchLabels": {"k2.dev/instance": name}},
            "policyTypes": ["Ingress", "Egress"],
            "ingress": [{"from": [{"podSelector": {"matchLabels": {"k2.dev/role": "controller"}}}], "ports": [{"port": port}]}],
            "egress": [
                {"to": [{"podSelector": {"matchLabels": {"k2.dev/role": "controller"}}}], "ports": [{"port": pl.controller_port}]},
                {"to": [{"podSelector": {"matchLabels": {"cluster-name": pl.postgres_cluster}}}], "ports": [{"port": 5432}]},
                {"to": [{"namespaceSelector": {}, "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}}}],
                 "ports": [{"port": 53, "protocol": "UDP"}, {"port": 53, "protocol": "TCP"}]},
                {"to": [{"ipBlock": {"cidr": "0.0.0.0/0",
                                     "except": ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "169.254.0.0/16", "100.64.0.0/10"]}}],
                 "ports": [{"port": 443}]},
            ],
        },
    })
}

/// The handle an instance's Deployment stands for (adopt).
pub fn handle_of(d: &Value) -> Option<RunHandle> {
    let name = d.pointer("/metadata/name")?.as_str()?;
    let ann = d.pointer("/metadata/annotations")?;
    Some(RunHandle {
        app_id: ann.get("k2.dev/app-id")?.as_str()?.to_string(),
        deployment_id: ann.get("k2.dev/deployment-id")?.as_str()?.to_string(),
        port: ann.get("k2.dev/port")?.as_str()?.parse().ok()?,
        pid: None,
        engine_ref: format!("{REF_PREFIX}{name}"),
    })
}

/// A Deployment's state as engine liveness: available → Running; gone →
/// Exited; anything in between (pulling, the init container) → Unknown.
pub fn liveness_of(d: Option<&Value>) -> Liveness {
    let Some(d) = d else { return Liveness::Exited(None) };
    if d.pointer("/metadata/deletionTimestamp").is_some() {
        return Liveness::Exited(None);
    }
    match d.pointer("/status/availableReplicas").and_then(Value::as_i64) {
        Some(n) if n > 0 => Liveness::Running,
        _ => Liveness::Unknown,
    }
}

// ── the engine ──────────────────────────────────────────────────────────

/// The Kubernetes calls this engine makes. The real one is [`KubeApi`];
/// tests record.
pub trait AppsApi: Send + Sync {
    /// Create (or patch) an object of `kind` ("deployments", "services",
    /// "networkpolicies"); answers the object as stored.
    fn apply(&self, kind: &str, obj: &Value) -> std::result::Result<Value, String>;
    fn get_deployment(&self, name: &str) -> std::result::Result<Option<Value>, String>;
    /// Delete a Deployment and, by owner reference, its Service and policy.
    fn delete_deployment(&self, name: &str) -> std::result::Result<(), String>;
    /// This controller's App Deployments.
    fn list_deployments(&self) -> std::result::Result<Vec<Value>, String>;
}

pub struct KubeEngine<A: AppsApi> {
    pub api: A,
}

fn eng_err(code: &'static str, e: impl std::fmt::Display) -> EngineError {
    EngineError::new(code, e.to_string())
}

impl<A: AppsApi> KubeEngine<A> {
    fn name_of(h: &RunHandle) -> Result<&str> {
        upstream_host(h)
            .map(|_| &h.engine_ref[REF_PREFIX.len()..])
            .ok_or_else(|| eng_err("engine_unavailable", format!("{} is not a Kubernetes instance", h.engine_ref)))
    }

    /// Start with an explicit placement (the trait's `start` reads it from
    /// the environment).
    pub fn start_at(&self, spec: &AppSpec, port: u16, pl: &Placement, token: &str) -> Result<RunHandle> {
        let name = object_name(&spec.app_id, &spec.deployment_id);
        let d = self.api.apply("deployments", &deployment(spec, port, pl, &name, token)).map_err(|e| eng_err("engine_unavailable", e))?;
        let uid = d.pointer("/metadata/uid").and_then(Value::as_str).unwrap_or_default().to_string();
        if uid.is_empty() {
            return Err(eng_err("engine_unavailable", format!("deployment {name}: no uid in the API's answer")));
        }
        for (kind, obj) in [
            ("services", service(spec, port, &name, &uid)),
            ("networkpolicies", network_policy(spec, port, pl, &name, &uid)),
        ] {
            if let Err(e) = self.api.apply(kind, &obj) {
                let _ = self.api.delete_deployment(&name);
                return Err(eng_err("engine_unavailable", format!("{kind} {name}: {e}")));
            }
        }
        Ok(RunHandle {
            app_id: spec.app_id.clone(),
            deployment_id: spec.deployment_id.clone(),
            port,
            pid: None,
            engine_ref: format!("{REF_PREFIX}{name}"),
        })
    }
}

impl<A: AppsApi> RuntimeEngine for KubeEngine<A> {
    fn isolation(&self) -> Isolation {
        Isolation::Container
    }

    fn caps(&self) -> EngineCaps {
        EngineCaps {
            separate_user: true,
            memory_limit: true,
            cpu_limit: false,
            read_only_seed: true,
            egress_gate: false,
            exec: false,
            adopt: true,
        }
    }

    fn prepare(&self, seed: &SeedRef, _spec: &AppSpec) -> Result<Prepared> {
        let (path, _) = pack_seed(seed).map_err(|e| eng_err("engine_unavailable", e))?;
        Ok(Prepared { workdir: path })
    }

    fn start(&self, spec: &AppSpec, p: &Prepared, port: u16) -> Result<RunHandle> {
        let hash = p
            .workdir
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| eng_err("engine_unavailable", "prepare did not pack a Seed"))?
            .to_string();
        let sha = sha256_file(&p.workdir).map_err(|e| eng_err("engine_unavailable", e))?;
        let pl = Placement::from_env(spec, &hash, &sha).map_err(|e| eng_err("engine_unavailable", e))?;
        let token = seed_token(&spec.app_id, &hash).map_err(|e| eng_err("engine_unavailable", e))?;
        self.start_at(spec, port, &pl, &token)
    }

    fn liveness(&self, h: &RunHandle) -> Liveness {
        match Self::name_of(h).map(|n| self.api.get_deployment(n)) {
            Ok(Ok(d)) => liveness_of(d.as_ref()),
            _ => Liveness::Unknown,
        }
    }

    fn stop(&self, h: &RunHandle, grace: Duration) -> Result<()> {
        let name = Self::name_of(h)?;
        self.api.delete_deployment(name).map_err(|e| eng_err("engine_unavailable", e))?;
        // The pod gets its own terminationGracePeriodSeconds; wait for the
        // Deployment to go, up to the caller's grace.
        let end = std::time::Instant::now() + grace;
        while std::time::Instant::now() < end {
            match self.api.get_deployment(name) {
                Ok(None) => break,
                _ => std::thread::sleep(Duration::from_millis(500)),
            }
        }
        Ok(())
    }

    fn exec(&self, _h: &RunHandle, _argv: &[String], _tty: bool) -> Result<ExecStream> {
        Err(eng_err("engine_unavailable", "k2 app exec is not available for Apps on Kubernetes yet"))
    }

    fn stats(&self, _h: &RunHandle) -> Result<CgroupStats> {
        Ok(CgroupStats::default())
    }

    fn adopt(&self) -> Result<Vec<RunHandle>> {
        let list = self.api.list_deployments().map_err(|e| eng_err("engine_unavailable", e))?;
        Ok(list.iter().filter(|d| liveness_of(Some(d)) != Liveness::Exited(None)).filter_map(handle_of).collect())
    }
}

// ── the real API ────────────────────────────────────────────────────────

/// [`AppsApi`] over the controller's own namespace ([`crate::k8s::blocking`]).
pub struct KubeApi;

impl AppsApi for KubeApi {
    fn apply(&self, kind: &str, obj: &Value) -> std::result::Result<Value, String> {
        let (kind, obj) = (kind.to_string(), obj.clone());
        crate::k8s::blocking(move |c| {
            Box::pin(async move {
                let gv = match kind.as_str() {
                    "deployments" => "apps/v1",
                    "networkpolicies" => "networking.k8s.io/v1",
                    _ => "v1",
                };
                let name = obj.pointer("/metadata/name").and_then(Value::as_str).unwrap_or_default().to_string();
                c.create_or_patch(&c.path(gv, &kind, None), &name, &obj).await.map_err(|e| format!("{kind}/{name}: {e}"))
            })
        })
    }

    fn get_deployment(&self, name: &str) -> std::result::Result<Option<Value>, String> {
        let name = name.to_string();
        crate::k8s::blocking(move |c| {
            Box::pin(async move {
                match c.get(&c.path("apps/v1", "deployments", Some(&name))).await {
                    Ok(d) => Ok(Some(d)),
                    Err(e) if e.is_not_found() => Ok(None),
                    Err(e) => Err(e.to_string()),
                }
            })
        })
    }

    fn delete_deployment(&self, name: &str) -> std::result::Result<(), String> {
        let name = name.to_string();
        crate::k8s::blocking(move |c| {
            Box::pin(async move {
                // Background: the Service and policy go by owner reference.
                let path = format!("{}?propagationPolicy=Background", c.path("apps/v1", "deployments", Some(&name)));
                c.delete(&path).await.map(|_| ()).map_err(|e| e.to_string())
            })
        })
    }

    fn list_deployments(&self) -> std::result::Result<Vec<Value>, String> {
        crate::k8s::blocking(move |c| {
            Box::pin(async move {
                let path = format!(
                    "{}?labelSelector=k2.dev%2Frole%3Dapp%2Capp.kubernetes.io%2Fmanaged-by%3Dk2-controller",
                    c.path("apps/v1", "deployments", None)
                );
                let l = c.get(&path).await.map_err(|e| e.to_string())?;
                Ok(l.get("items").and_then(Value::as_array).cloned().unwrap_or_default())
            })
        })
    }
}

/// The engine `K2_CAP_APPS=kubernetes` runs Apps on.
pub fn engine() -> &'static KubeEngine<KubeApi> {
    static E: KubeEngine<KubeApi> = KubeEngine { api: KubeApi };
    &E
}

#[cfg(test)]
mod tests;
