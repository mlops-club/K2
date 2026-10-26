//! The client against a fake API server: a real listener that records each
//! request and answers from a script (no network beyond loopback, no env
//! changes except through k2_core::test_env).

use super::*;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone)]
struct Seen {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: String,
}

impl Seen {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
    }
}

/// Answers each request with the next (status, body) of `script`, then 500.
async fn fake_api(script: Vec<(u16, &'static str)>) -> (String, Arc<Mutex<Vec<Seen>>>) {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", l.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    tokio::spawn(async move {
        let mut script = script.into_iter();
        loop {
            let Ok((mut s, _)) = l.accept().await else { return };
            let mut buf = Vec::new();
            let mut tmp = [0u8; 4096];
            // Headers, then Content-Length bytes of body.
            let (head_end, len) = loop {
                let n = s.read(&mut tmp).await.unwrap_or(0);
                if n == 0 {
                    break (None, 0);
                }
                buf.extend_from_slice(&tmp[..n]);
                if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&buf[..i]).to_string();
                    let len = head
                        .lines()
                        .find_map(|l| l.to_ascii_lowercase().strip_prefix("content-length:").map(|v| v.trim().parse::<usize>().unwrap_or(0)))
                        .unwrap_or(0);
                    break (Some(i + 4), len);
                }
            };
            let Some(head_end) = head_end else { continue };
            while buf.len() < head_end + len {
                let n = s.read(&mut tmp).await.unwrap_or(0);
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
            }
            let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
            let mut lines = head.lines();
            let mut first = lines.next().unwrap_or("").split(' ');
            let method = first.next().unwrap_or("").to_string();
            let path = first.next().unwrap_or("").to_string();
            let headers = lines
                .filter_map(|l| l.split_once(':').map(|(k, v)| (k.trim().to_string(), v.trim().to_string())))
                .collect();
            let body = String::from_utf8_lossy(&buf[head_end..]).to_string();
            log.lock().unwrap().push(Seen { method, path, headers, body });
            let (status, body) = script.next().unwrap_or((500, "{}"));
            let resp = format!(
                "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = s.write_all(resp.as_bytes()).await;
            let _ = s.shutdown().await;
        }
    });
    (base, seen)
}

struct Files {
    dir: PathBuf,
}

impl Files {
    fn new(token: &str) -> Files {
        let dir = k2_core::test_env::unique_temp_path("k8s-client");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("token"), format!("{token}\n")).unwrap();
        Files { dir }
    }
    fn token_file(&self) -> PathBuf {
        self.dir.join("token")
    }
}

impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn client(base: &str, files: &Files) -> Client {
    Client::new(Config::new(base, "k2-p4-caps", &files.token_file(), None)).unwrap()
}

#[test]
fn paths_for_core_apps_and_custom_resources() {
    let f = Files::new("t");
    let c = client("http://127.0.0.1:1", &f);
    assert_eq!(c.path("v1", "secrets", Some("s")), "/api/v1/namespaces/k2-p4-caps/secrets/s");
    assert_eq!(c.path("apps/v1", "deployments", None), "/apis/apps/v1/namespaces/k2-p4-caps/deployments");
    assert_eq!(
        c.path("acid.zalan.do/v1", "postgresqls", Some("k2-pg")),
        "/apis/acid.zalan.do/v1/namespaces/k2-p4-caps/postgresqls/k2-pg"
    );
    assert_eq!(c.namespace(), "k2-p4-caps");
}

#[tokio::test]
async fn get_sends_the_bearer_token_and_reads_it_again_each_time() {
    let (base, seen) = fake_api(vec![(200, r#"{"kind":"Pod"}"#), (200, r#"{"kind":"Pod"}"#)]).await;
    let f = Files::new("first-token");
    let c = client(&base, &f);
    let doc = c.get("/api/v1/namespaces/k2-p4-caps/pods/p").await.unwrap();
    assert_eq!(doc["kind"], "Pod");
    // The kubelet rotates a projected token in place.
    std::fs::write(f.token_file(), "second-token\n").unwrap();
    c.get("/api/v1/namespaces/k2-p4-caps/pods/p").await.unwrap();
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen[0].method, "GET");
    assert_eq!(seen[0].path, "/api/v1/namespaces/k2-p4-caps/pods/p");
    assert_eq!(seen[0].header("authorization"), Some("Bearer first-token"));
    assert_eq!(seen[0].header("accept"), Some("application/json"));
    assert_eq!(seen[1].header("authorization"), Some("Bearer second-token"));
}

#[tokio::test]
async fn merge_patch_uses_the_merge_patch_content_type() {
    let (base, seen) = fake_api(vec![(200, r#"{"metadata":{"name":"k2-pg"}}"#)]).await;
    let f = Files::new("t");
    let c = client(&base, &f);
    let path = c.path("acid.zalan.do/v1", "postgresqls", Some("k2-pg"));
    let patch = serde_json::json!({"spec": {"users": {"app_x": []}}});
    c.merge_patch(&path, &patch).await.unwrap();
    let s = seen.lock().unwrap()[0].clone();
    assert_eq!(s.method, "PATCH");
    assert_eq!(s.header("content-type"), Some("application/merge-patch+json"));
    assert_eq!(serde_json::from_str::<Value>(&s.body).unwrap(), patch);
}

#[tokio::test]
async fn an_api_refusal_carries_the_servers_reason_and_message() {
    let status = r#"{"kind":"Status","status":"Failure","reason":"Forbidden","code":403,
        "message":"postgresqls.acid.zalan.do \"k2-pg\" is forbidden: User \"system:serviceaccount:k2:k2-controller\" cannot patch resource"}"#;
    let (base, _) = fake_api(vec![(403, status)]).await;
    let f = Files::new("t");
    let c = client(&base, &f);
    let err = c.merge_patch("/x", &serde_json::json!({})).await.unwrap_err();
    match &err {
        K8sError::Api { status, reason, message } => {
            assert_eq!(*status, 403);
            assert_eq!(reason, "Forbidden");
            assert!(message.contains("cannot patch resource"), "{message}");
        }
        other => panic!("expected an Api error, got {other:?}"),
    }
    assert!(err.to_string().starts_with("kubernetes 403 Forbidden: "), "{err}");
}

#[tokio::test]
async fn delete_is_true_when_gone_and_false_on_404() {
    let (base, seen) = fake_api(vec![
        (200, r#"{"kind":"Status","status":"Success"}"#),
        (404, r#"{"kind":"Status","reason":"NotFound","code":404,"message":"not found"}"#),
        (500, r#"{"kind":"Status","reason":"InternalError","code":500,"message":"etcd down"}"#),
    ])
    .await;
    let f = Files::new("t");
    let c = client(&base, &f);
    assert_eq!(c.delete("/a").await, Ok(true));
    assert_eq!(c.delete("/a").await, Ok(false));
    assert!(matches!(c.delete("/a").await, Err(K8sError::Api { status: 500, .. })));
    assert!(seen.lock().unwrap().iter().all(|s| s.method == "DELETE"));
}

#[tokio::test]
async fn create_or_patch_patches_when_the_object_exists() {
    let (base, seen) = fake_api(vec![
        (409, r#"{"kind":"Status","reason":"AlreadyExists","code":409,"message":"exists"}"#),
        (200, r#"{"metadata":{"name":"app-x"}}"#),
    ])
    .await;
    let f = Files::new("t");
    let c = client(&base, &f);
    let coll = c.path("apps/v1", "deployments", None);
    let obj = serde_json::json!({"metadata": {"name": "app-x"}, "spec": {"replicas": 1}});
    let out = c.create_or_patch(&coll, "app-x", &obj).await.unwrap();
    assert_eq!(out["metadata"]["name"], "app-x");
    let seen = seen.lock().unwrap().clone();
    assert_eq!((seen[0].method.as_str(), seen[0].path.as_str()), ("POST", coll.as_str()));
    assert_eq!(seen[0].header("content-type"), Some("application/json"));
    assert_eq!((seen[1].method.as_str(), seen[1].path.as_str()), ("PATCH", format!("{coll}/app-x").as_str()));
}

#[tokio::test]
async fn secret_data_is_decoded() {
    // {"username": "app_x", "password": "p@ss"} as the API returns it.
    let (base, _) = fake_api(vec![(200, r#"{"data":{"username":"YXBwX3g=","password":"cEBzcw=="}}"#)]).await;
    let f = Files::new("t");
    let c = client(&base, &f);
    let d = c.secret_data("app-x.k2-pg.credentials.postgresql.acid.zalan.do").await.unwrap();
    assert_eq!(d.get("username").map(Vec::as_slice), Some(&b"app_x"[..]));
    assert_eq!(d.get("password").map(Vec::as_slice), Some(&b"p@ss"[..]));
}

#[tokio::test]
async fn a_closed_port_is_an_http_error_and_a_missing_token_a_config_error() {
    let port = k2_core::test_env::ClosedPort::new();
    let f = Files::new("t");
    let c = client(&format!("http://127.0.0.1:{}", port.port()), &f);
    assert!(matches!(c.get("/x").await, Err(K8sError::Http(_))));
    std::fs::remove_file(f.token_file()).unwrap();
    assert!(matches!(c.get("/x").await, Err(K8sError::Config(_))));
}

#[test]
fn in_cluster_needs_the_service_env() {
    let _env = k2_core::test_env::EnvVar::remove("KUBERNETES_SERVICE_HOST");
    let f = Files::new("t");
    let err = Config::in_cluster(&f.dir).unwrap_err();
    assert!(matches!(err, K8sError::Config(ref m) if m.contains("KUBERNETES_SERVICE_HOST")), "{err}");
}
