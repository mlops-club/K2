use super::*;
use crate::capabilities::Entry;
use crate::k8s::test_support::fake_api;

fn report(entries: &[(Feature, State, &'static str)]) -> Report {
    Report(entries.iter().map(|&(feature, state, mode)| Entry { feature, state, mode, via: String::new() }).collect())
}

fn settings(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let m: BTreeMap<String, String> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    move |k| m.get(k).cloned()
}

fn client(base: &str, dir: &std::path::Path) -> k8s::Client {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(dir.join("token"), "t").unwrap();
    k8s::Client::new(k8s::Config::new(base, "k2-p4-caps", &dir.join("token"), None)).unwrap()
}

#[test]
fn plan_has_one_check_per_delegated_feature() {
    let r = report(&[
        (Feature::Postgres, State::Delegated, "kubernetes"),
        (Feature::Mail, State::Delegated, "external"),
        (Feature::Sew, State::Off, "off"),
        (Feature::Apps, State::Delegated, "kubernetes"),
        (Feature::Front, State::Delegated, "listen"),
        (Feature::Vault, State::HostNative, "host"),
    ]);
    let get = settings(&[("K2_POSTGRES_CLUSTER", "k2-pg"), ("K2_STALWART_URL", "http://k2-mail:8080/")]);
    assert_eq!(
        plan(&r, &get),
        vec![
            (Feature::Postgres, Check::PostgresOperator { cluster: "k2-pg".into(), user: "k2".into() }),
            (Feature::Mail, Check::Http { url: "http://k2-mail:8080/healthz/ready".into() }),
            (Feature::Apps, Check::AppsRbac),
            (Feature::Front, Check::Listen),
        ]
    );
}

#[test]
fn plan_names_a_missing_setting_and_reads_the_user_from_its_file() {
    let r = report(&[(Feature::Postgres, State::Delegated, "kubernetes"), (Feature::Sew, State::Delegated, "external")]);
    let none = settings(&[]);
    let p = plan(&r, &none);
    assert!(matches!(&p[0].1, Check::Misconfigured(m) if m.contains("K2_POSTGRES_CLUSTER")), "{p:?}");
    assert!(matches!(&p[1].1, Check::Misconfigured(m) if m.contains("K2_SEW_MGMT_URL")), "{p:?}");

    // The operator's Secret for the controller's user, mounted as a file.
    let dir = k2_core::test_env::unique_temp_path("caps-plan");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("username"), "k2_ctl\n").unwrap();
    let user_file = dir.join("username").to_string_lossy().to_string();
    let get = settings(&[("K2_POSTGRES_CLUSTER", "k2-pg"), ("K2_POSTGRES_USER_FILE", &user_file)]);
    assert_eq!(plan(&r, &get)[0].1, Check::PostgresOperator { cluster: "k2-pg".into(), user: "k2_ctl".into() });
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn postgres_ok_when_running_and_its_secret_is_readable() {
    let (base, seen) = fake_api(vec![
        (200, r#"{"status":{"PostgresClusterStatus":"Running"}}"#),
        (200, r#"{"data":{"username":"azJfY3Rs","password":"eA=="}}"#),
    ])
    .await;
    let dir = k2_core::test_env::unique_temp_path("caps-pg");
    let c = client(&base, &dir);
    let check = Check::PostgresOperator { cluster: "k2-pg".into(), user: "k2_ctl".into() };
    let s = run(&check, Some(&c)).await;
    assert!(s.ok, "{s:?}");
    assert!(!s.detail.contains("eA=="), "no credential in the detail: {s:?}");
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen[0].path, "/apis/acid.zalan.do/v1/namespaces/k2-p4-caps/postgresqls/k2-pg");
    // "_" in the user becomes "-" in the operator's Secret name.
    assert_eq!(seen[1].path, "/api/v1/namespaces/k2-p4-caps/secrets/k2-ctl.k2-pg.credentials.postgresql.acid.zalan.do");
    assert!(seen.iter().all(|s| s.method == "GET"), "a driver check only reads");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn postgres_not_ok_while_creating_or_when_the_secret_is_forbidden() {
    let (base, _) = fake_api(vec![
        (200, r#"{"status":{"PostgresClusterStatus":"Creating"}}"#),
        (200, r#"{"status":{"PostgresClusterStatus":"Running"}}"#),
        (403, r#"{"kind":"Status","reason":"Forbidden","code":403,"message":"cannot get resource \"secrets\""}"#),
    ])
    .await;
    let dir = k2_core::test_env::unique_temp_path("caps-pg2");
    let c = client(&base, &dir);
    let check = Check::PostgresOperator { cluster: "k2-pg".into(), user: "k2".into() };
    let s = run(&check, Some(&c)).await;
    assert!(!s.ok && s.detail.contains("Creating"), "{s:?}");
    let s = run(&check, Some(&c)).await;
    assert!(!s.ok && s.detail.contains("Forbidden"), "{s:?}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn apps_rbac_lists_deployments_and_reports_a_refusal() {
    let (base, seen) = fake_api(vec![
        (200, r#"{"kind":"DeploymentList","items":[]}"#),
        (403, r#"{"kind":"Status","reason":"Forbidden","code":403,"message":"cannot list resource \"deployments\""}"#),
    ])
    .await;
    let dir = k2_core::test_env::unique_temp_path("caps-apps");
    let c = client(&base, &dir);
    assert!(run(&Check::AppsRbac, Some(&c)).await.ok);
    let s = run(&Check::AppsRbac, Some(&c)).await;
    assert!(!s.ok && s.detail.contains("Forbidden"), "{s:?}");
    assert_eq!(seen.lock().unwrap()[0].path, "/apis/apps/v1/namespaces/k2-p4-caps/deployments?limit=1");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn http_checks_and_the_kinds_with_nothing_to_reach() {
    let (base, _) = fake_api(vec![(200, "{}"), (503, "{}")]).await;
    let check = Check::Http { url: format!("{base}/healthz/ready") };
    assert!(run(&check, None).await.ok);
    let s = run(&check, None).await;
    assert!(!s.ok && s.detail.contains("503"), "{s:?}");
    assert!(run(&Check::Listen, None).await.ok);
    assert!(!run(&Check::Misconfigured("x".into()), None).await.ok);
    // Kubernetes checks outside a pod say so instead of failing obscurely.
    let s = run(&Check::AppsRbac, None).await;
    assert!(!s.ok && s.detail.contains("not in a pod"), "{s:?}");
}

#[test]
fn boot_status_shows_the_latest_answer() {
    store().write().unwrap().insert(Feature::Front, Status { ok: true, detail: "d".into(), checked_ms: 7 });
    assert_eq!(status_json(Feature::Front), Some(serde_json::json!({"ok": true, "detail": "d", "checkedMs": 7})));
    let r = report(&[(Feature::Front, State::Delegated, "listen")]);
    assert_eq!(r.to_json()["front"]["driver"]["ok"], true);
    // A feature that is not delegated never shows a driver.
    let off = report(&[(Feature::Front, State::Off, "off")]);
    assert!(off.to_json()["front"].get("driver").is_none());
    store().write().unwrap().remove(&Feature::Front);
}
