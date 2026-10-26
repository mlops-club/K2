//! Per-workspace databases on Kubernetes against fakes: the operator
//! (patches recorded, its Secrets a map) and the superuser psql
//! (`FakeSystemOps`, the helper allowlist included). The catalog is the
//! real test DB under the sql test lock.

use super::*;
use crate::sql::ops::{self, OpsError};
use crate::sql::secrets::MemSecretStore;
use crate::sql::sql_server_test_lock;
use crate::sql::sysops::FakeSystemOps;
use k2_core::test_env::EnvVar;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Default)]
struct FakeOperator {
    patches: Mutex<Vec<Value>>,
    /// Secret name → (username, password).
    secrets: Mutex<HashMap<String, (String, String)>>,
    /// Reads of a Secret before it appears (the operator is still syncing).
    pending_reads: Mutex<u32>,
}

impl FakeOperator {
    fn leaked() -> &'static FakeOperator {
        let op: &'static FakeOperator = Box::leak(Box::default());
        op.put("postgres", "postgres", "su-pw");
        op
    }
    fn put(&self, user_for_name: &str, user: &str, pw: &str) {
        self.secrets.lock().unwrap().insert(credentials_secret("k2-pg", user_for_name), (user.into(), pw.into()));
    }
    fn patches(&self) -> Vec<Value> {
        self.patches.lock().unwrap().clone()
    }
}

impl Operator for FakeOperator {
    fn patch_cluster(&self, patch: &Value) -> Result<(), String> {
        self.patches.lock().unwrap().push(patch.clone());
        // The operator answers a new entry with its two logins.
        if let Some(dbs) = patch.pointer("/spec/preparedDatabases").and_then(Value::as_object) {
            for (db, v) in dbs {
                if !v.is_null() {
                    for u in [format!("{db}_owner_user"), format!("{db}_writer_user")] {
                        let pw = format!("{u}-pw");
                        self.secrets.lock().unwrap().entry(credentials_secret("k2-pg", &u)).or_insert((u.clone(), pw));
                    }
                }
            }
        }
        Ok(())
    }
    fn login(&self, secret: &str) -> Result<Option<(String, String)>, String> {
        let mut pending = self.pending_reads.lock().unwrap();
        if *pending > 0 && !secret.starts_with("postgres.") {
            *pending -= 1;
            return Ok(None);
        }
        Ok(self.secrets.lock().unwrap().get(secret).cloned())
    }
    fn sleep_ms(&self, _ms: u64) {}
}

struct Mode {
    _env: Vec<EnvVar>,
}

/// `K2_CAP_POSTGRES=kubernetes` on k2-pg, this thread's operator, and a
/// running sidecar row with the 16 cluster recorded (the fake's major).
fn kubernetes_mode(op: &'static FakeOperator) -> Mode {
    let env = vec![
        EnvVar::set("K2_CAP_POSTGRES", "kubernetes"),
        EnvVar::set("K2_POSTGRES_CLUSTER", "k2-pg"),
        EnvVar::set("K2_POSTGRES_HOST", "k2-pg"),
    ];
    set_test_operator(Some(op));
    k2_core::db::init_for_tests();
    let db = k2_core::db::shared();
    let conn = db.lock();
    for t in ["sql_grants", "sql_server", "sql_clusters", "sql_databases"] {
        let _ = conn.execute(&format!("DELETE FROM {t}"), []);
    }
    conn.execute(
        "INSERT INTO sql_server (id, status, installed_major, listen, updated_at) VALUES (1, 'running', 16, 'k2-pg', 1)",
        [],
    )
    .unwrap();
    crate::sql::cluster::record_primary_major(&conn, 16).unwrap();
    Mode { _env: env }
}

impl Drop for Mode {
    fn drop(&mut self) {
        set_test_operator(None);
    }
}

fn project(tag: &str) -> String {
    let dir = std::env::temp_dir().join(format!("k2-sql-k8s-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let db = k2_core::db::shared();
    db.lock()
        .execute("INSERT INTO projects (id, name, path) VALUES (?1, ?2, ?3)", rusqlite::params![id, tag, dir.to_string_lossy()])
        .unwrap();
    id
}

fn helper_sql(ops: &FakeSystemOps) -> Vec<(String, String)> {
    ops.pg.lock().unwrap().helper_psql.clone()
}

#[test]
fn the_mode_is_k2_cap_postgres_kubernetes() {
    let get = |v: Option<&'static str>| move |k: &str| (k == "K2_CAP_POSTGRES").then(|| v.map(String::from)).flatten();
    assert!(!kubernetes_from(&get(None)));
    assert!(!kubernetes_from(&get(Some("host"))));
    assert!(kubernetes_from(&get(Some("kubernetes"))));
}

#[test]
fn the_helpers_psql_goes_to_the_cluster_as_the_superuser() {
    let argv = helper_psql_to_client(&["psql", "--cluster", "17", "-d", "ws_a", "-v", "ON_ERROR_STOP=1", "-tA"], "k2-pg", "postgres").unwrap();
    assert_eq!(argv, ["-h", "k2-pg", "-p", "5432", "-U", "postgres", "-X", "-d", "ws_a", "-v", "ON_ERROR_STOP=1", "-tA"]);
    assert_eq!(helper_psql_to_client(&["systemctl", "restart", "postgresql"], "k2-pg", "postgres"), None);
    assert_eq!(helper_psql_to_client(&["install-ram-fence"], "k2-pg", "postgres"), None);
}

#[test]
fn a_roles_client_argv_points_at_the_service() {
    let argv = client_argv_to_cluster(&["-h", "127.0.0.1", "-p", "5433", "-U", "ws_a_writer_user", "-d", "ws_a", "-c", "SELECT -p"], "k2-pg");
    assert_eq!(argv, ["-h", "k2-pg", "-p", "5432", "-U", "ws_a_writer_user", "-d", "ws_a", "-c", "SELECT -p"]);
}

#[test]
fn psql_errors_never_carry_a_password() {
    let e = "ERROR:  syntax error\nLINE 1: ALTER ROLE a PASSWORD 'hunter2' x;\nverifier SCRAM-SHA-256$4096:abc";
    let r = redact_passwords(e);
    assert!(!r.contains("hunter2") && !r.contains("SCRAM"), "{r}");
    assert!(r.contains("syntax error"));
}

#[test]
fn prepare_declares_the_database_and_waits_for_both_logins() {
    let _g = sql_server_test_lock();
    let _e = EnvVar::set("K2_POSTGRES_CLUSTER", "k2-pg");
    let op = FakeOperator::leaked();
    *op.pending_reads.lock().unwrap() = 3;
    let (m, a) = prepare_database(op, "ws_a", "ws_a_owner_user", "ws_a_writer_user").unwrap();
    assert_eq!(op.patches(), vec![json!({"spec": {"preparedDatabases": {"ws_a": {
        "defaultUsers": true, "schemas": {"public": {"defaultRoles": false}}}}}})]);
    assert_eq!(m, Login { user: "ws_a_owner_user".into(), password: "ws_a_owner_user-pw".into() });
    assert_eq!(a.user, "ws_a_writer_user");

    // A Secret naming another user is refused, never used.
    op.put("ws_b_owner_user", "someone_else", "x");
    let err = prepare_database(op, "ws_b", "ws_b_owner_user", "ws_b_writer_user").unwrap_err();
    assert!(err.contains("names user 'someone_else'"), "{err}");
}

#[test]
fn create_on_kubernetes_uses_the_operators_logins_and_keeps_k2s_shape() {
    let _g = sql_server_test_lock();
    let op = FakeOperator::leaked();
    let _m = kubernetes_mode(op);
    let pid = project("create");
    let ops = FakeSystemOps::baked();
    let secrets = MemSecretStore::default();
    let v = ops::create_database(&ops, &secrets, &pid, 1, None, None).expect("create");
    let name = v["name"].as_str().unwrap().to_string();
    let (mig, agent) = (format!("{name}_owner_user"), format!("{name}_writer_user"));
    assert_eq!(ops::migrator_role(&name), mig);
    assert_eq!(ops::agent_role(&name), agent);

    // The operator declared it; K2 created neither the database nor a role.
    assert_eq!(op.patches()[0].pointer(&format!("/spec/preparedDatabases/{name}/defaultUsers")), Some(&json!(true)));
    let all: String = helper_sql(&ops).iter().map(|(_, s)| s.as_str()).collect::<Vec<_>>().join("\n");
    assert!(!all.contains("CREATE DATABASE") && !all.contains("CREATE ROLE"), "{all}");
    // K2's shape on the operator's logins, as superuser.
    assert!(all.contains(&format!("ALTER ROLE \"{mig}\" NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOINHERIT")), "{all}");
    assert!(all.contains(&format!("ALTER ROLE \"{agent}\" NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOINHERIT")), "{all}");
    assert!(all.contains(&format!("ALTER DATABASE \"{name}\" OWNER TO \"{mig}\"")), "{all}");
    assert!(all.contains(&format!("GRANT \"{agent}\" TO \"{mig}\"")), "{all}");
    assert!(all.contains(&format!("ALTER TABLE public._k2_migrations OWNER TO \"{mig}\"")), "{all}");
    assert!(all.contains(&format!("GRANT CONNECT ON DATABASE \"{name}\" TO \"{agent}\"")), "{all}");

    // The agent's DSN: the operator's password, the Service, TLS.
    assert_eq!(v["dsn"], json!(format!("postgres://{agent}:{agent}-pw@k2-pg:5432/{name}?sslmode=require")));
    let s = v.to_string();
    assert!(!s.contains("su-pw") && !s.contains("postgres://postgres"), "{s}");
    // Asked again: the same database, nothing new declared.
    let again = ops::create_database(&ops, &secrets, &pid, 1, None, None).expect("again");
    assert_eq!(again["existing"], json!(true));
    assert_eq!(op.patches().len(), 1);
}

#[test]
fn drop_on_kubernetes_forgets_the_entry_then_drops() {
    let _g = sql_server_test_lock();
    let op = FakeOperator::leaked();
    let _m = kubernetes_mode(op);
    let pid = project("drop");
    let ops = FakeSystemOps::baked();
    let secrets = MemSecretStore::default();
    let name = ops::create_database(&ops, &secrets, &pid, 1, None, None).unwrap()["name"].as_str().unwrap().to_string();
    ops::drop_database(&ops, &pid).expect("drop");
    assert_eq!(op.patches().last(), Some(&json!({"spec": {"preparedDatabases": {name.clone(): null}}})));
    let all: String = helper_sql(&ops).iter().map(|(_, s)| s.as_str()).collect::<Vec<_>>().join("\n");
    assert!(all.contains(&format!("DROP DATABASE IF EXISTS \"{name}\"")), "{all}");
}

#[test]
fn a_major_other_than_the_operators_is_refused() {
    let _g = sql_server_test_lock();
    let op = FakeOperator::leaked();
    let _m = kubernetes_mode(op);
    let pid = project("major");
    let ops = FakeSystemOps::baked();
    let secrets = MemSecretStore::default();
    match ops::create_database_on(&ops, &secrets, &pid, 1, None, None, Some(17)) {
        Err(OpsError::Refused(code, hint)) => {
            assert_eq!(code, "major_is_the_operators");
            assert!(hint.contains("postgresql/k2-pg"), "{hint}");
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert!(op.patches().is_empty(), "nothing declared");
    ops::create_database_on(&ops, &secrets, &pid, 1, None, None, Some(16)).expect("its own major is fine");
}

#[test]
fn enable_on_kubernetes_records_the_operators_cluster_without_a_unit() {
    let _g = sql_server_test_lock();
    let op = FakeOperator::leaked();
    let _m = kubernetes_mode(op);
    {
        let db = k2_core::db::shared();
        let conn = db.lock();
        let _ = conn.execute("DELETE FROM sql_server", []);
        let _ = conn.execute("DELETE FROM sql_clusters", []);
    }
    let ops = FakeSystemOps::baked();
    let v = crate::sql::supervisor::enable_with(&ops).expect("enable");
    assert_eq!(v["state"], "running");
    assert_eq!(v["installedMajor"], 16);
    assert_eq!(v["cluster"], "k2-pg");
    let rec = ops.recorded().join("\n");
    assert!(!rec.contains("systemctl") && !rec.contains("install-ram-fence"), "no unit, no fence: {rec}");
    assert_eq!(crate::sql::cluster::primary().major, 16);
    // Disable leaves the operator's Postgres running.
    crate::sql::supervisor::disable_with(&ops).expect("disable");
    assert!(!ops.recorded().join("\n").contains("systemctl"));
}

#[test]
fn host_mode_is_unchanged() {
    let _e = EnvVar::remove("K2_CAP_POSTGRES");
    assert_eq!(ops::migrator_role("ws_a"), "ws_a_migrator");
    assert_eq!(ops::agent_role("ws_a"), "ws_a_agent");
    assert!(!kubernetes());
}
