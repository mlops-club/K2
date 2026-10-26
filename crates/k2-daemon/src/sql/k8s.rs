//! Per-workspace databases on Kubernetes (PRD P4 T3, decision 1;
//! docs/prd/progress/k8s-capabilities.md). `K2_CAP_POSTGRES=kubernetes`:
//! the databases live in the Zalando operator's cluster `K2_POSTGRES_CLUSTER`
//! (Service `K2_POSTGRES_HOST`), not in a supervised unit on this machine.
//!
//! - **the database and its logins**: a `preparedDatabases` entry on the
//!   `postgresql` object. The operator creates the database and the users
//!   `{db}_owner_user` and `{db}_writer_user`, with their passwords in
//!   Secrets; K2's migrator is the owner user, its agent the writer user
//!   ([`super::ops::migrator_role`], [`super::ops::agent_role`]);
//! - **K2's own SQL** (ownership, NOINHERIT, grants, `_k2_*` tables, the
//!   `k2` helpers, the seal) runs unchanged, as the operator's superuser
//!   (Secret `postgres.<cluster>.credentials…`) instead of through the
//!   root helper: [`KubeSqlOps`] is the `SystemOps` for this mode.
//!   Checked live on k2lab: the operator's syncs keep a NOINHERIT owner user
//!   and a database re-owned to it;
//! - **not here**: the unit, the RAM fence, GUC pins, other majors and
//!   `k2 db upgrade` are the operator's (its spec), so those paths refuse.

use std::time::Duration;

use serde_json::{json, Value};

use super::sysops::{RealSystemOps, SystemOps};
use crate::k8s;

pub const PG_PORT: u16 = 5432;
const SECRET_WAIT: Duration = Duration::from_secs(180);

/// `K2_CAP_POSTGRES=kubernetes`.
pub fn kubernetes() -> bool {
    kubernetes_from(&|k| std::env::var(k).ok())
}

pub fn kubernetes_from(get: &dyn Fn(&str) -> Option<String>) -> bool {
    get("K2_CAP_POSTGRES").as_deref() == Some("kubernetes")
}

/// The operator's `postgresql` object (`K2_POSTGRES_CLUSTER`, default k2-pg).
pub fn cluster_name() -> String {
    std::env::var("K2_POSTGRES_CLUSTER").ok().filter(|s| !s.trim().is_empty()).unwrap_or_else(|| "k2-pg".into())
}

/// Where clients connect: the operator's Service for the primary.
pub fn host() -> String {
    std::env::var("K2_POSTGRES_HOST").ok().filter(|s| !s.trim().is_empty()).unwrap_or_else(cluster_name)
}

/// The operator's credential Secret for `user` (`_` becomes `-`).
pub fn credentials_secret(cluster: &str, user: &str) -> String {
    format!("{}.{cluster}.credentials.postgresql.acid.zalan.do", user.replace('_', "-"))
}

/// The merge patch that declares `db`: its owner and writer users and
/// schema `public` (no operator schema roles; K2 grants its own).
pub fn prepared_database_patch(db: &str) -> Value {
    json!({"spec": {"preparedDatabases": {db: {
        "defaultUsers": true,
        "schemas": {"public": {"defaultRoles": false}},
    }}}})
}

/// The merge patch that forgets `db` (the operator never drops a database;
/// K2 does, as superuser).
pub fn forget_database_patch(db: &str) -> Value {
    json!({"spec": {"preparedDatabases": {db: Value::Null}}})
}

/// What this mode asks of the Kubernetes API. The real one is
/// [`KubeOperator`]; tests record.
pub trait Operator: Send + Sync {
    fn patch_cluster(&self, patch: &Value) -> Result<(), String>;
    /// `(username, password)` from an operator Secret; None while it does
    /// not exist yet.
    fn login(&self, secret: &str) -> Result<Option<(String, String)>, String>;
    fn sleep_ms(&self, ms: u64);
}

/// A login the operator made, with its password.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Login {
    pub user: String,
    pub password: String,
}

/// Declare `db` on the operator's cluster and wait for its two logins.
/// Idempotent: an existing entry and its Secrets are reused.
pub fn prepare_database(op: &dyn Operator, db: &str, migrator: &str, agent: &str) -> Result<(Login, Login), String> {
    op.patch_cluster(&prepared_database_patch(db))?;
    let cluster = cluster_name();
    let wait_for = |user: &str| -> Result<Login, String> {
        let secret = credentials_secret(&cluster, user);
        let tries = SECRET_WAIT.as_millis() / 1000;
        for i in 0..tries {
            if let Some((u, password)) = op.login(&secret)? {
                if u != user {
                    return Err(format!("Secret {secret} names user '{u}', expected '{user}'"));
                }
                return Ok(Login { user: u, password });
            }
            if i + 1 < tries {
                op.sleep_ms(1000);
            }
        }
        Err(format!(
            "the operator made no Secret {secret} within {}s (is postgresql/{cluster} syncing? kubectl describe postgresql {cluster})",
            SECRET_WAIT.as_secs()
        ))
    };
    Ok((wait_for(migrator)?, wait_for(agent)?))
}

/// The operator's superuser, for K2's own SQL.
pub fn superuser(op: &dyn Operator) -> Result<Login, String> {
    let cluster = cluster_name();
    let secret = credentials_secret(&cluster, "postgres");
    op.login(&secret)?
        .map(|(user, password)| Login { user, password })
        .ok_or_else(|| format!("Secret {secret} is missing: the operator has not set up postgresql/{cluster}"))
}

// ── SystemOps: K2's psql, pointed at the operator's cluster ─────────────

/// `psql` argv for a helper call (`psql [--cluster N] -d DB FLAGS…`),
/// rewritten to connect to `host` as `su`. None for any other verb.
pub fn helper_psql_to_client(argv: &[&str], host: &str, su: &str) -> Option<Vec<String>> {
    if argv.first() != Some(&"psql") {
        return None;
    }
    let mut rest = &argv[1..];
    if rest.first() == Some(&"--cluster") && rest.len() >= 2 {
        rest = &rest[2..];
    }
    let mut out = vec!["-h".to_string(), host.to_string(), "-p".to_string(), PG_PORT.to_string(), "-U".to_string(), su.to_string(), "-X".to_string()];
    out.extend(rest.iter().map(|s| s.to_string()));
    Some(out)
}

/// A role's client argv (`-h 127.0.0.1 -p <port> …`, see ops.rs) pointed
/// at `host`:5432. Other arguments are untouched.
pub fn client_argv_to_cluster(args: &[&str], host: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(args.len());
    let mut i = 0;
    while i < args.len() {
        match args[i] {
            "-h" if i + 1 < args.len() => {
                out.push("-h".into());
                out.push(host.to_string());
                i += 2;
            }
            "-p" if i + 1 < args.len() => {
                out.push("-p".into());
                out.push(PG_PORT.to_string());
                i += 2;
            }
            a => {
                out.push(a.to_string());
                i += 1;
            }
        }
    }
    out
}

/// Spilo refuses connections without TLS from other pods (`hostnossl …
/// reject`); its certificate is its own, so encrypted without a CA check,
/// as `psql`'s default `prefer` does against it.
const CLIENT_ENV: &[(&str, &str)] = &[("PGSSLMODE", "require"), ("PGCONNECT_TIMEOUT", "10")];

/// As the root helper's `psql` verb (k2-root-helper `pg::psql`): `-X`,
/// search_path and role pinned through PGOPTIONS, and on failure
/// `psql exit N: <stderr>` with every line naming a password dropped.
fn run_superuser_psql(bin: &str, argv: &[String], password: &str, stdin: Option<&[u8]>) -> Result<String, String> {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let pgoptions = k2_root_proto::pg_major::SAFE_PGOPTIONS.trim_start_matches("PGOPTIONS=");
    let mut c = Command::new(bin);
    c.args(argv).env("PGPASSWORD", password).env("PGOPTIONS", pgoptions);
    for (k, v) in CLIENT_ENV {
        c.env(k, v);
    }
    c.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = c.spawn().map_err(|e| format!("{bin}: {e}"))?;
    if let Some(mut sin) = child.stdin.take() {
        sin.write_all(stdin.unwrap_or_default()).map_err(|e| format!("{bin} stdin: {e}"))?;
    }
    let out = child.wait_with_output().map_err(|e| format!("{bin}: {e}"))?;
    if !out.status.success() {
        let err = redact_passwords(&String::from_utf8_lossy(&out.stderr));
        let err: String = err.trim().chars().take(2000).collect();
        return Err(format!("psql exit {}: {err}", out.status.code().unwrap_or(-1)));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// The root helper's `redact_passwords`: Postgres echoes a failing
/// statement, so any line naming a password or a verifier goes.
pub fn redact_passwords(stderr: &str) -> String {
    stderr
        .lines()
        .map(|l| {
            let low = l.to_ascii_lowercase();
            if low.contains("password") || l.contains("SCRAM-SHA-256$") || l.contains("$4096:") {
                "[line with a password redacted]"
            } else {
                l
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The Kubernetes API this mode uses: [`KubeOperator`], or a test's.
pub fn operator() -> &'static dyn Operator {
    #[cfg(test)]
    if let Some(op) = TEST_OPERATOR.with(|c| c.get()) {
        return op;
    }
    &KubeOperator
}

#[cfg(test)]
thread_local! {
    static TEST_OPERATOR: std::cell::Cell<Option<&'static dyn Operator>> = const { std::cell::Cell::new(None) };
}

/// Tests: this thread's operator (leaked, like the routes' test ops).
#[cfg(test)]
pub(crate) fn set_test_operator(op: Option<&'static dyn Operator>) {
    TEST_OPERATOR.with(|c| c.set(op));
}

/// The `SystemOps` for `K2_CAP_POSTGRES=kubernetes`: K2's psql calls go
/// to the operator's cluster; the host-only verbs refuse.
pub struct KubeSqlOps {
    local: RealSystemOps,
}

pub static KUBE_SQL_OPS: KubeSqlOps = KubeSqlOps { local: RealSystemOps };

impl KubeSqlOps {
    fn psql_bin(&self) -> String {
        super::sysops::PSQL_PATH.to_string()
    }
}

fn not_here(what: &str) -> String {
    format!("{what}: not on Kubernetes — postgresql/{} is the operator's (K2_CAP_POSTGRES=kubernetes)", cluster_name())
}

impl SystemOps for KubeSqlOps {
    fn write_file(&self, path: &str, contents: &[u8], mode: u32) -> Result<(), String> {
        self.local.write_file(path, contents, mode)
    }
    fn read_file(&self, path: &str) -> Result<Vec<u8>, String> {
        self.local.read_file(path)
    }
    fn create_dir_all(&self, path: &str) -> Result<(), String> {
        self.local.create_dir_all(path)
    }
    fn remove_path(&self, path: &str) -> Result<(), String> {
        self.local.remove_path(path)
    }
    fn path_exists(&self, path: &str) -> bool {
        self.local.path_exists(path)
    }
    fn systemctl(&self, args: &[&str]) -> Result<String, String> {
        Err(not_here(&format!("systemctl {}", args.join(" "))))
    }
    fn systemctl_query(&self, _args: &[&str]) -> String {
        String::new()
    }
    fn run_helper(&self, args: &[&str], stdin: Option<&[u8]>) -> Result<String, String> {
        let su = superuser(operator())?;
        let Some(argv) = helper_psql_to_client(args, &host(), &su.user) else {
            return Err(not_here(&format!("root helper {}", args.first().copied().unwrap_or(""))));
        };
        run_superuser_psql(&self.psql_bin(), &argv, &su.password, stdin)
    }
    fn run_cmd(&self, cmd: &str, args: &[&str], env: &[(&str, &str)], stdin: Option<&[u8]>) -> Result<Vec<u8>, String> {
        let argv = client_argv_to_cluster(args, &host());
        let refs: Vec<&str> = argv.iter().map(String::as_str).collect();
        let mut all: Vec<(&str, &str)> = CLIENT_ENV.to_vec();
        all.extend_from_slice(env);
        self.local.run_cmd(cmd, &refs, &all, stdin)
    }
    fn sleep_ms(&self, ms: u64) {
        std::thread::sleep(Duration::from_millis(ms));
    }
}

// ── The real Operator: the Kubernetes API from inside the controller ───

/// Blocking calls over [`k8s::Client`] ([`k8s::blocking`]).
pub struct KubeOperator;

impl KubeOperator {
    fn call<T: Send + 'static>(f: impl FnOnce(k8s::Client) -> k8s::CallFuture<T> + Send + 'static) -> Result<T, String> {
        k8s::blocking(f)
    }
}

impl Operator for KubeOperator {
    fn patch_cluster(&self, patch: &Value) -> Result<(), String> {
        let patch = patch.clone();
        Self::call(move |c| {
            Box::pin(async move {
                let path = c.path("acid.zalan.do/v1", "postgresqls", Some(&cluster_name()));
                c.merge_patch(&path, &patch).await.map(|_| ()).map_err(|e| format!("patch postgresql/{}: {e}", cluster_name()))
            })
        })
    }

    fn login(&self, secret: &str) -> Result<Option<(String, String)>, String> {
        let secret = secret.to_string();
        Self::call(move |c| {
            Box::pin(async move {
                match c.secret_data(&secret).await {
                    Ok(d) => {
                        let s = |k: &str| d.get(k).map(|b| String::from_utf8_lossy(b).to_string());
                        match (s("username"), s("password")) {
                            (Some(u), Some(p)) if !u.is_empty() && !p.is_empty() => Ok(Some((u, p))),
                            _ => Err(format!("Secret {secret} has no username/password")),
                        }
                    }
                    Err(e) if e.is_not_found() => Ok(None),
                    Err(e) => Err(format!("Secret {secret}: {e}")),
                }
            })
        })
    }

    fn sleep_ms(&self, ms: u64) {
        std::thread::sleep(Duration::from_millis(ms));
    }
}

#[cfg(test)]
mod tests;
