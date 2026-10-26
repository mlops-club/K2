//! The Kubernetes engine against a recording API: the four objects, the
//! Seed route's token, start/stop/adopt, and the host the router and the
//! health probes use.

use super::*;
use k2_core::deploy::engine::WallSpec;
use k2_core::test_env::EnvVar;
use std::sync::Mutex;

fn spec() -> AppSpec {
    AppSpec {
        app_id: "notes".into(),
        service_id: "svc-1".into(),
        workspace_id: "ws-1".into(),
        deployment_id: "d0f1e2d3-c4b5-a697-8877-665544332211".into(),
        isolation: Isolation::Container,
        argv: vec!["node".into(), "server.js".into()],
        env: [("GREETING".to_string(), "hi".to_string()), ("PORT".to_string(), "1".to_string())].into_iter().collect(),
        memory_limit_bytes: Some(268_435_456),
        drain_secs: 20,
        io_max_json: None,
        devices: vec![],
        public_origins: vec!["https://notes.example.k2.dev".into()],
        data_dir: "/x/data".into(),
        cache_dir: "/x/cache".into(),
        home_dir: "/x/home".into(),
        log_path: "/x/log".into(),
        wall: WallSpec::default(),
    }
}

fn placement() -> Placement {
    Placement {
        image: "docker.io/library/node@sha256:abc".into(),
        controller: "k2-controller".into(),
        controller_port: 7420,
        postgres_cluster: "k2-pg".into(),
        seed_hash: "a".repeat(64),
        seed_sha256: "b".repeat(64),
    }
}

#[test]
fn names_are_dns_labels_and_the_handle_names_the_service() {
    let n = object_name("notes", "d0f1e2d3-c4b5-a697");
    assert_eq!(n, "app-notes-d0f1e2d3");
    let long = object_name(&"My_App.".repeat(20), "D0F1-E2D3-C4B5");
    assert!(long.len() <= 63 && long.starts_with("app-my-app-my-app") && long.ends_with("-d0f1e2d3"), "{long}");
    assert!(long.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'), "{long}");

    let h = RunHandle { app_id: "notes".into(), deployment_id: "d".into(), port: 8080, pid: None, engine_ref: format!("{REF_PREFIX}{n}") };
    assert!(owns(&h));
    assert_eq!(upstream_host(&h).as_deref(), Some("app-notes-d0f1e2d3"));
    let local = RunHandle { engine_ref: "podman:k2app-notes".into(), ..h };
    assert!(!owns(&local));
    assert_eq!(upstream_host(&local), None, "every other engine stays on host loopback");
}

#[test]
fn the_mode_is_k2_cap_apps_kubernetes() {
    let get = |v: Option<&'static str>| move |k: &str| (k == "K2_CAP_APPS").then(|| v.map(String::from)).flatten();
    assert!(!enabled_from(&get(None)));
    assert!(!enabled_from(&get(Some("host"))));
    assert!(enabled_from(&get(Some("kubernetes"))));
}

#[test]
fn hmac_sha256_matches_rfc_4231() {
    // Test case 2 ("Jefe") and test case 6 (a key longer than the block).
    assert_eq!(hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")), "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
    assert_eq!(
        hex(&hmac_sha256(&[0xaa; 131], b"Test Using Larger Than Block-Size Key - Hash Key First")),
        "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
    );
    let a = seed_token_with(b"k", "notes", &"a".repeat(64));
    assert_eq!(a.len(), 64);
    assert_ne!(a, seed_token_with(b"k", "other", &"a".repeat(64)), "bound to the App");
    assert_ne!(a, seed_token_with(b"k", "notes", &"c".repeat(64)), "bound to the Seed");
}

#[test]
fn the_deployment_runs_the_seed_read_only_on_the_pinned_image() {
    let d = deployment(&spec(), 8080, &placement(), "app-notes-d0f1e2d3", "tok");
    let pod = &d["spec"]["template"]["spec"];
    assert_eq!(pod["automountServiceAccountToken"], false);
    assert_eq!(pod["securityContext"]["runAsUser"], APP_UID);
    assert_eq!(pod["terminationGracePeriodSeconds"], 20);
    let init = &pod["initContainers"][0];
    assert_eq!(init["image"], "docker.io/library/node@sha256:abc");
    assert_eq!(init["command"][2], FETCH_JS);
    let ienv = init["env"].as_array().unwrap();
    assert!(ienv.contains(&json!({"name": "K2_SEED_URL", "value": format!("http://k2-controller:7420/cli/apps/k8s-seed/notes/{}", "a".repeat(64))})));
    assert!(ienv.contains(&json!({"name": "K2_SEED_SHA256", "value": "b".repeat(64)})));
    assert!(ienv.contains(&json!({"name": "K2_SEED_TOKEN", "value": "tok"})), "only the init container has the token");
    assert!(!pod["containers"][0]["env"].to_string().contains("tok\""), "never the App");
    let app = &pod["containers"][0];
    assert_eq!(app["image"], init["image"], "no per-App image (D6 revised)");
    assert_eq!(app["command"], json!(["node", "server.js"]));
    assert_eq!(app["ports"][0]["containerPort"], 8080);
    assert_eq!(app["resources"]["limits"]["memory"], "268435456");
    assert!(app["volumeMounts"].as_array().unwrap().contains(&json!({"name": "app", "mountPath": "/app", "readOnly": true})));
    let env: BTreeMap<String, String> =
        app["env"].as_array().unwrap().iter().map(|e| (e["name"].as_str().unwrap().into(), e["value"].as_str().unwrap().into())).collect();
    assert_eq!(env["PORT"], "8080", "K2's PORT wins over the manifest's");
    assert_eq!(env["GREETING"], "hi");
    assert_eq!(env["K2_APP_ORIGIN"], "https://notes.example.k2.dev");
    assert_eq!(d["metadata"]["annotations"]["k2.dev/port"], "8080");
    assert_eq!(d["spec"]["selector"]["matchLabels"]["k2.dev/instance"], "app-notes-d0f1e2d3");
}

#[test]
fn the_policy_lets_the_controller_in_and_only_its_needs_out() {
    let p = network_policy(&spec(), 8080, &placement(), "app-notes-d0f1e2d3", "uid-1");
    assert_eq!(p["metadata"]["ownerReferences"][0]["uid"], "uid-1");
    assert_eq!(p["spec"]["ingress"], json!([{"from": [{"podSelector": {"matchLabels": {"k2.dev/role": "controller"}}}], "ports": [{"port": 8080}]}]));
    let egress = p["spec"]["egress"].as_array().unwrap();
    assert_eq!(egress.len(), 4);
    assert_eq!(egress[0]["ports"][0]["port"], 7420);
    assert_eq!(egress[1]["to"][0]["podSelector"]["matchLabels"]["cluster-name"], "k2-pg");
    assert_eq!(egress[3]["ports"][0]["port"], 443);
    let svc = service(&spec(), 8080, "app-notes-d0f1e2d3", "uid-1");
    assert_eq!(svc["spec"]["ports"][0]["port"], 8080);
    assert_eq!(svc["metadata"]["ownerReferences"][0]["kind"], "Deployment");
}

#[derive(Default)]
struct FakeApi {
    calls: Mutex<Vec<String>>,
    deployments: Mutex<BTreeMap<String, Value>>,
    fail_kind: Option<&'static str>,
}

impl AppsApi for FakeApi {
    fn apply(&self, kind: &str, obj: &Value) -> std::result::Result<Value, String> {
        let name = obj["metadata"]["name"].as_str().unwrap().to_string();
        self.calls.lock().unwrap().push(format!("apply {kind} {name}"));
        if self.fail_kind == Some(kind) {
            return Err("forbidden (test)".into());
        }
        let mut stored = obj.clone();
        stored["metadata"]["uid"] = json!(format!("uid-{name}"));
        if kind == "deployments" {
            stored["status"] = json!({"availableReplicas": 1});
            self.deployments.lock().unwrap().insert(name, stored.clone());
        }
        Ok(stored)
    }
    fn get_deployment(&self, name: &str) -> std::result::Result<Option<Value>, String> {
        Ok(self.deployments.lock().unwrap().get(name).cloned())
    }
    fn delete_deployment(&self, name: &str) -> std::result::Result<(), String> {
        self.calls.lock().unwrap().push(format!("delete {name}"));
        self.deployments.lock().unwrap().remove(name);
        Ok(())
    }
    fn list_deployments(&self) -> std::result::Result<Vec<Value>, String> {
        Ok(self.deployments.lock().unwrap().values().cloned().collect())
    }
}

#[test]
fn start_makes_three_objects_owned_by_the_deployment_and_stop_removes_them() {
    let e = KubeEngine { api: FakeApi::default() };
    let h = e.start_at(&spec(), 8080, &placement(), "tok").unwrap();
    assert_eq!(h.engine_ref, "k8s:app-notes-d0f1e2d3");
    assert_eq!(h.port, 8080);
    assert_eq!(
        *e.api.calls.lock().unwrap(),
        vec![
            "apply deployments app-notes-d0f1e2d3",
            "apply services app-notes-d0f1e2d3",
            "apply networkpolicies app-notes-d0f1e2d3",
        ]
    );
    assert_eq!(e.liveness(&h), Liveness::Running);
    assert_eq!(e.adopt().unwrap(), vec![h.clone()], "a restarted controller finds it again");
    e.stop(&h, Duration::from_millis(10)).unwrap();
    assert_eq!(e.api.calls.lock().unwrap().last().unwrap(), "delete app-notes-d0f1e2d3");
    assert_eq!(e.liveness(&h), Liveness::Exited(None));
    assert!(e.adopt().unwrap().is_empty());
}

#[test]
fn a_failed_object_takes_the_deployment_back() {
    let e = KubeEngine { api: FakeApi { fail_kind: Some("networkpolicies"), ..FakeApi::default() } };
    let err = e.start_at(&spec(), 8080, &placement(), "tok").unwrap_err();
    assert!(err.message.contains("networkpolicies"), "{err}");
    assert_eq!(e.api.calls.lock().unwrap().last().unwrap(), "delete app-notes-d0f1e2d3", "no App without its policy");
}

#[test]
fn liveness_follows_the_deployment() {
    assert_eq!(liveness_of(None), Liveness::Exited(None));
    assert_eq!(liveness_of(Some(&json!({"status": {}}))), Liveness::Unknown, "starting: not a failure");
    assert_eq!(liveness_of(Some(&json!({"status": {"availableReplicas": 1}}))), Liveness::Running);
    assert_eq!(
        liveness_of(Some(&json!({"metadata": {"deletionTimestamp": "x"}, "status": {"availableReplicas": 1}}))),
        Liveness::Exited(None)
    );
}

#[test]
fn the_seed_route_serves_a_packed_seed_to_its_own_token_only() {
    let _home = crate::test_support::TempHome::new();
    let _mode = EnvVar::set("K2_CAP_APPS", "kubernetes");
    let src = k2_core::paths::k2_home().join("seed-src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(src.join("server.js"), "console.log('hi')").unwrap();
    let hash = "c".repeat(64);
    let path = format!("/cli/apps/k8s-seed/notes/{hash}");
    let token = seed_token("notes", &hash).unwrap();

    assert_eq!(handle_seed_fetch(&path, Some(&token)).unwrap_err(), ("404 Not Found", "seed_not_packed"));
    let (tar, sha) = pack_seed(&SeedRef { hash: hash.clone(), path: src.clone() }).unwrap();
    assert_eq!(sha, sha256_file(&tar).unwrap());
    assert_eq!(handle_seed_fetch(&path, Some(&token)).unwrap(), tar);
    assert_eq!(handle_seed_fetch(&path, Some("nope")).unwrap_err().1, "seed_token");
    assert_eq!(handle_seed_fetch(&path, None).unwrap_err().1, "seed_token");
    let other = seed_token("other", &hash).unwrap();
    assert_eq!(handle_seed_fetch(&path, Some(&other)).unwrap_err().1, "seed_token", "another App's token");
    assert_eq!(handle_seed_fetch("/cli/apps/k8s-seed/notes/../x", Some(&token)).unwrap_err().1, "not_found");
    // The same key after a restart: the token still holds.
    assert_eq!(seed_token("notes", &hash).unwrap(), token);
    let _off = EnvVar::remove("K2_CAP_APPS");
    assert_eq!(handle_seed_fetch(&path, Some(&token)).unwrap_err().1, "not_found", "off unless K2_CAP_APPS=kubernetes");
}

#[test]
fn the_router_dials_the_instance_host_and_loopback_is_unchanged() {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            s.write_all(b"from-instance").await.unwrap();
        });
        let (be, _) = crate::apps::router::switch_to("k8s-router-test", Some("127.0.0.1".into()), port, false);
        assert_eq!(be.dial_host(), "127.0.0.1");
        let (mut a, mut b) = tokio::io::duplex(1024);
        let splice = tokio::spawn(async move { crate::apps::router::splice(&mut b, be, None).await });
        let mut got = [0u8; 13];
        a.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"from-instance");
        drop(a);
        let _ = splice.await;

        let (bad, _) = crate::apps::router::switch_to("k8s-router-test", Some("app-x.invalid".into()), port, false);
        let (_a2, mut b2) = tokio::io::duplex(64);
        let err = crate::apps::router::splice(&mut b2, bad, None).await.unwrap_err();
        assert!(err.contains("app-x.invalid"), "{err}");

        let (local, _) = crate::apps::router::switch_with("k8s-router-test", port, false);
        assert_eq!(local.host, None, "switch_with is host loopback, as before");
        assert_eq!(local.dial_host(), "127.0.0.1");
        crate::apps::router::clear("k8s-router-test");
    });
}

#[test]
fn probes_keep_the_loopback_request_and_take_a_host() {
    use std::io::{Read as _, Write as _};
    let seen = |host: Option<&'static str>| {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let t = std::thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            let mut buf = [0u8; 512];
            let n = s.read(&mut buf).unwrap();
            s.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n").unwrap();
            String::from_utf8_lossy(&buf[..n]).to_string()
        });
        let p = match host {
            None => k2_core::deploy::health::probe_once(port, "/healthz", &[], Duration::from_secs(2)),
            Some(h) => k2_core::deploy::health::probe_once_at(Some(h), port, "/healthz", &[], Duration::from_secs(2)),
        };
        (p.healthy(), t.join().unwrap(), port)
    };
    let (ok, req, port) = seen(None);
    assert!(ok);
    assert!(req.starts_with(&format!("GET /healthz HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUser-Agent: k2-health/1\r\n")), "{req}");
    // Through the name path (resolved, not the fixed loopback address).
    let (ok, req, port) = seen(Some("127.0.0.1"));
    assert!(ok, "the host is dialed");
    assert!(req.contains(&format!("Host: 127.0.0.1:{port}\r\n")), "{req}");
}
