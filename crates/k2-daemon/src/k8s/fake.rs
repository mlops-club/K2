//! A fake Kubernetes API server on loopback for the client's and the pool's
//! tests: the StatefulSet `k2-host`'s scale, Secrets, pods, a pod watch, a
//! bearer token check, and a log of every request in order.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

use parking_lot::Mutex;
use serde_json::{json, Value};

use super::{Client, Config, Token};

pub(crate) const NAMESPACE: &str = "k2-test";
pub(crate) const TOKEN: &str = "fake-sa-token";

#[derive(Default)]
pub(crate) struct State {
    /// `statefulsets/<name>` → spec replicas.
    pub replicas: HashMap<String, u32>,
    /// Secret name → data (key → base64).
    pub secrets: HashMap<String, HashMap<String, String>>,
    pub pods: Vec<Value>,
    /// Lines a pod watch streams, then the watch ends.
    pub watch_lines: Vec<String>,
    /// (method, path with query, body) of every request, in order.
    pub log: Vec<(String, String, String)>,
    /// Paths (without query) answered 403, as RBAC would.
    pub forbidden: Vec<String>,
}

pub(crate) struct FakeApi {
    pub state: Arc<Mutex<State>>,
    pub base: String,
}

impl FakeApi {
    pub fn start() -> FakeApi {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", l.local_addr().unwrap());
        let state: Arc<Mutex<State>> = Arc::default();
        let s = Arc::clone(&state);
        std::thread::spawn(move || {
            for conn in l.incoming() {
                let Ok(conn) = conn else { return };
                let s = Arc::clone(&s);
                std::thread::spawn(move || serve(conn, &s));
            }
        });
        FakeApi { state, base }
    }

    pub fn client(&self) -> Client {
        Client::new(Config { base: self.base.clone(), namespace: NAMESPACE.into(), token: Token::Fixed(TOKEN.into()), ca_pem: None }).unwrap()
    }

    /// The (method, path) of every request so far.
    pub fn calls(&self) -> Vec<(String, String)> {
        self.state.lock().log.iter().map(|(m, p, _)| (m.clone(), p.clone())).collect()
    }
}

pub(crate) fn pod_json(name: &str, phase: &str, ready: bool, node: Option<&str>) -> Value {
    json!({
        "metadata": {"name": name, "labels": {"app.kubernetes.io/name": "k2-host"}},
        "spec": {"nodeName": node},
        "status": {"phase": phase, "conditions": [{"type": "Ready", "status": if ready { "True" } else { "False" }}]},
    })
}

fn status(code: u16, reason: &str, message: &str) -> (u16, Value) {
    (code, json!({"kind": "Status", "status": "Failure", "reason": reason, "message": message, "code": code}))
}

fn serve(conn: TcpStream, state: &Mutex<State>) {
    let mut r = BufReader::new(conn.try_clone().unwrap());
    let mut line = String::new();
    if r.read_line(&mut line).unwrap_or(0) == 0 {
        return;
    }
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("").to_string();
    let (mut len, mut auth) = (0usize, String::new());
    loop {
        let mut h = String::new();
        if r.read_line(&mut h).unwrap_or(0) == 0 || h == "\r\n" {
            break;
        }
        let (k, v) = h.split_once(':').unwrap_or((&h, ""));
        match k.to_ascii_lowercase().as_str() {
            "content-length" => len = v.trim().parse().unwrap_or(0),
            "authorization" => auth = v.trim().to_string(),
            _ => {}
        }
    }
    let mut body = vec![0u8; len];
    let _ = r.read_exact(&mut body);
    let body = String::from_utf8_lossy(&body).to_string();
    let (path, query) = target.split_once('?').unwrap_or((&target, ""));
    let mut conn = conn;
    let (code, v, watch) = {
        let mut st = state.lock();
        st.log.push((method.clone(), target.clone(), body.clone()));
        if auth != format!("Bearer {TOKEN}") {
            let (c, v) = status(401, "Unauthorized", "no or wrong bearer token");
            (c, v, None)
        } else if st.forbidden.iter().any(|f| f == path) {
            let (c, v) = status(403, "Forbidden", &format!("{path} is forbidden for the ServiceAccount"));
            (c, v, None)
        } else {
            route(&mut st, &method, path, query, &body)
        }
    };
    if let Some(lines) = watch {
        let _ = write!(conn, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n");
        for l in lines {
            let _ = writeln!(conn, "{l}");
        }
        return;
    }
    let text = v.to_string();
    let reason = if code < 300 { "OK" } else { "Error" };
    let _ = write!(conn, "HTTP/1.1 {code} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}", text.len());
}

fn route(st: &mut State, method: &str, path: &str, query: &str, body: &str) -> (u16, Value, Option<Vec<String>>) {
    let ns = format!("/namespaces/{NAMESPACE}/");
    let Some(rest) = path.strip_prefix("/apis/apps/v1").or_else(|| path.strip_prefix("/api/v1")).and_then(|p| p.strip_prefix(&ns)) else {
        let (c, v) = status(404, "NotFound", &format!("no route {path}"));
        return (c, v, None);
    };
    let seg: Vec<&str> = rest.split('/').collect();
    let patch: Value = serde_json::from_str(body).unwrap_or(Value::Null);
    match (method, seg.as_slice()) {
        ("GET" | "PATCH", ["statefulsets", name, "scale"]) => {
            let Some(n) = st.replicas.get(*name).copied() else {
                let (c, v) = status(404, "NotFound", &format!("statefulsets.apps \"{name}\" not found"));
                return (c, v, None);
            };
            let n = if method == "PATCH" { patch["spec"]["replicas"].as_u64().unwrap_or(n as u64) as u32 } else { n };
            st.replicas.insert(name.to_string(), n);
            (200, json!({"kind": "Scale", "spec": {"replicas": n}, "status": {"replicas": n}}), None)
        }
        ("GET", ["pods"]) if query.contains("watch=1") => (200, Value::Null, Some(std::mem::take(&mut st.watch_lines))),
        ("GET", ["pods"]) => (200, json!({"kind": "PodList", "metadata": {"resourceVersion": "42"}, "items": st.pods}), None),
        ("GET", ["pods", name]) => match st.pods.iter().find(|p| p["metadata"]["name"] == *name) {
            Some(p) => (200, p.clone(), None),
            None => {
                let (c, v) = status(404, "NotFound", &format!("pods \"{name}\" not found"));
                (c, v, None)
            }
        },
        ("PATCH", ["secrets", name]) => {
            let Some(data) = st.secrets.get_mut(*name) else {
                let (c, v) = status(404, "NotFound", &format!("secrets \"{name}\" not found"));
                return (c, v, None);
            };
            for (k, v) in patch["data"].as_object().cloned().unwrap_or_default() {
                match v.as_str() {
                    Some(b64) => {
                        data.insert(k, b64.to_string());
                    }
                    None => {
                        data.remove(&k);
                    }
                }
            }
            (200, json!({"kind": "Secret", "metadata": {"name": name}, "data": data}), None)
        }
        _ => {
            let (c, v) = status(405, "MethodNotAllowed", &format!("{method} {path}"));
            (c, v, None)
        }
    }
}
