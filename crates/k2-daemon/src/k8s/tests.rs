use super::fake::{pod_json, FakeApi, NAMESPACE, TOKEN};
use super::*;

#[test]
fn in_cluster_config_reads_the_service_account() {
    let dir = k2_core::test_env::unique_temp_path("k8s-sa");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("namespace"), "k2\n").unwrap();
    std::fs::write(dir.join("ca.crt"), "-----BEGIN CERTIFICATE-----\n").unwrap();
    std::fs::write(dir.join("token"), "t").unwrap();
    let env = |pairs: &'static [(&'static str, &'static str)]| move |k: &str| pairs.iter().find(|(n, _)| *n == k).map(|(_, v)| v.to_string());

    let c = Config::in_cluster_from(env(&[("KUBERNETES_SERVICE_HOST", "10.96.0.1"), ("KUBERNETES_SERVICE_PORT", "443")]), &dir).unwrap();
    assert_eq!((c.base.as_str(), c.namespace.as_str()), ("https://10.96.0.1:443", "k2"));
    assert!(matches!(c.token, Token::File(ref p) if p == &dir.join("token")));
    let c = Config::in_cluster_from(env(&[("KUBERNETES_SERVICE_HOST", "fd00::1")]), &dir).unwrap();
    assert_eq!(c.base, "https://[fd00::1]:443");
    let e = Config::in_cluster_from(env(&[]), &dir).unwrap_err();
    assert!(e.contains("KUBERNETES_SERVICE_HOST"), "{e}");
    std::fs::remove_file(dir.join("token")).unwrap();
    let e = Config::in_cluster_from(env(&[("KUBERNETES_SERVICE_HOST", "10.96.0.1")]), &dir).unwrap_err();
    assert!(e.contains("no ServiceAccount token"), "{e}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn scale_get_and_patch() {
    let api = FakeApi::start();
    api.state.lock().replicas.insert("k2-host".into(), 1);
    let c = api.client();
    assert_eq!(c.get_scale("k2-host").unwrap(), Scale { spec: 1, status: 1 });
    assert_eq!(c.set_replicas("k2-host", 2).unwrap().spec, 2);
    assert_eq!(api.state.lock().replicas["k2-host"], 2);
    let (_, path, body) = api.state.lock().log.last().cloned().unwrap();
    assert_eq!(path, format!("/apis/apps/v1/namespaces/{NAMESPACE}/statefulsets/k2-host/scale"));
    assert_eq!(serde_json::from_str::<Value>(&body).unwrap(), json!({"spec": {"replicas": 2}}));
    let e = c.get_scale("nope").unwrap_err();
    assert!(e.is_not_found(), "{e}");
}

#[test]
fn api_errors_carry_the_status_reason() {
    let api = FakeApi::start();
    api.state.lock().replicas.insert("k2-host".into(), 1);
    api.state.lock().forbidden.push(format!("/apis/apps/v1/namespaces/{NAMESPACE}/statefulsets/k2-host/scale"));
    let e = api.client().set_replicas("k2-host", 3).unwrap_err();
    assert!(matches!(&e, Error::Api { code: 403, reason, .. } if reason == "Forbidden"), "{e}");
    assert!(e.to_string().contains("forbidden for the ServiceAccount"), "{e}");
    // Nothing listening: a transport error, never a panic.
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let c = Client::new(Config { base: format!("http://127.0.0.1:{port}"), namespace: "k2".into(), token: Token::None, ca_pem: None }).unwrap();
    assert!(matches!(c.get_scale("k2-host"), Err(Error::Transport(_))));
}

#[test]
fn the_token_file_is_read_on_every_call() {
    let api = FakeApi::start();
    api.state.lock().replicas.insert("k2-host".into(), 1);
    let file = k2_core::test_env::unique_temp_path("k8s-token");
    std::fs::write(&file, "stale\n").unwrap();
    let c = Client::new(Config { base: api.base.clone(), namespace: NAMESPACE.into(), token: Token::File(file.clone()), ca_pem: None }).unwrap();
    assert!(matches!(c.get_scale("k2-host"), Err(Error::Api { code: 401, .. })));
    // The kubelet rotated the projected token.
    std::fs::write(&file, format!("{TOKEN}\n")).unwrap();
    assert_eq!(c.get_scale("k2-host").unwrap().spec, 1);
    let _ = std::fs::remove_file(&file);
}

#[test]
fn secret_keys_are_merge_patched() {
    let api = FakeApi::start();
    api.state.lock().secrets.insert("k2-host-enroll".into(), [("k2-host-0".to_string(), "b2xk".to_string())].into());
    let c = api.client();
    c.put_secret_key("k2-host-enroll", "k2-host-1", b"one-time").unwrap();
    {
        let st = api.state.lock();
        let data = &st.secrets["k2-host-enroll"];
        assert_eq!(data["k2-host-1"], "b25lLXRpbWU=", "base64 of the value");
        assert_eq!(data["k2-host-0"], "b2xk", "other keys stay");
    }
    c.remove_secret_key("k2-host-enroll", "k2-host-0").unwrap();
    assert!(!api.state.lock().secrets["k2-host-enroll"].contains_key("k2-host-0"));
    // RBAC grants patch by name on a pre-created Secret: a missing one is an error.
    assert!(c.put_secret_key("k2-sbx-x", "token", b"t").unwrap_err().is_not_found());
}

#[test]
fn pods_list_get_and_watch() {
    let api = FakeApi::start();
    api.state.lock().pods = vec![pod_json("k2-host-0", "Running", true, Some("node-a")), pod_json("k2-host-1", "Pending", false, None)];
    let c = api.client();
    let (pods, rv) = c.list_pods("app.kubernetes.io/name=k2-host").unwrap();
    assert_eq!(rv, "42");
    assert_eq!(
        pods[0],
        Pod { name: "k2-host-0".into(), phase: "Running".into(), ready: true, node: Some("node-a".into()), deleting: false }
    );
    assert_eq!((pods[1].phase.as_str(), pods[1].ready), ("Pending", false));
    let (_, path) = api.calls().last().cloned().unwrap();
    assert!(path.ends_with("labelSelector=app.kubernetes.io%2Fname%3Dk2-host"), "{path}");
    assert_eq!(c.get_pod("k2-host-1").unwrap().unwrap().name, "k2-host-1");
    assert_eq!(c.get_pod("k2-host-9").unwrap(), None);

    let mut deleting = pod_json("k2-host-0", "Running", false, Some("node-a"));
    deleting["metadata"]["deletionTimestamp"] = json!("2026-10-26T00:00:00Z");
    api.state.lock().watch_lines = vec![
        json!({"type": "MODIFIED", "object": pod_json("k2-host-1", "Running", true, Some("node-b"))}).to_string(),
        json!({"type": "MODIFIED", "object": deleting}).to_string(),
        json!({"type": "ERROR", "object": {"kind": "Status", "code": 410, "reason": "Expired", "message": "too old resource version"}}).to_string(),
    ];
    let events: Vec<PodEvent> = c.watch_pods("app.kubernetes.io/name=k2-host", &rv).unwrap().map(Result::unwrap).collect();
    assert!(matches!(&events[0], PodEvent::Modified(p) if p.name == "k2-host-1" && p.ready));
    assert!(matches!(&events[1], PodEvent::Modified(p) if p.deleting));
    assert_eq!(events[2], PodEvent::Expired);
    let (_, path) = api.calls().last().cloned().unwrap();
    assert!(path.contains("watch=1") && path.contains("resourceVersion=42"), "{path}");
}
