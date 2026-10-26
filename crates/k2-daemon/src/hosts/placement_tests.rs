//! G2 fixes on loopback sockets, against fake hosts written here frame by
//! frame: placement affinity (S7: a re-placed agent goes back to the host
//! that holds its home) and the host drain (S8).
//!
//! Deadlines only stop a hung test; nothing asserts a duration.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use super::hub;
use super::proto::{Frame, ATTACH_PATH, PROTOCOL};
use crate::agent_host::remote::{PlacementStore, RemoteHost};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

const DEADLINE: Duration = Duration::from_secs(30);
const TOKEN: &str = "test-bootstrap-token";

async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(DEADLINE, f).await.unwrap_or_else(|_| panic!("timed out waiting for {what}"))
}

async fn send(ws: &mut Ws, f: &Frame) {
    ws.send(Message::Text(f.encode())).await.expect("send frame");
}

async fn recv(ws: &mut Ws) -> Frame {
    within("a frame", async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(t))) => return Frame::decode(&t).expect("frame"),
                Some(Ok(Message::Close(_))) | None => panic!("link closed"),
                Some(Ok(_)) => continue,
                Some(Err(e)) => panic!("link error: {e}"),
            }
        }
    })
    .await
}

/// The hub on loopback over `remote`; returns the attach URL.
async fn serve(remote: &'static RemoteHost) -> String {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}{ATTACH_PATH}", l.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((s, _)) = l.accept().await {
            tokio::spawn(hub::serve_attach(s, remote, TOKEN));
        }
    });
    url
}

async fn fake_host(url: &str, name: &str) -> Ws {
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.expect("dial");
    send(&mut ws, &Frame::Hello { protocol: PROTOCOL, host_name: name.into(), token: TOKEN.into(), labels: vec![], resume: vec![] }).await;
    assert!(matches!(recv(&mut ws).await, Frame::Welcome { .. }));
    ws
}

async fn wait_hosts(remote: &RemoteHost, n: usize) {
    within("hosts to link", async {
        while remote.status_json()["hosts"].as_array().map(Vec::len) != Some(n) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

/// Spawn `agent` (cwd `cwd`) through the controller; `host` is the fake
/// host the test expects the spawn on, which answers with `session`.
/// Returns the controller's answer body.
async fn spawn_on(remote: &'static RemoteHost, host: &mut Ws, agent: &str, cwd: &str, session: &str) -> Value {
    let body = json!({"agent_name": agent, "cwd": cwd, "command": "claude"}).to_string();
    let spawn = tokio::spawn(async move { remote.spawn(body.as_bytes()).await });
    match recv(host).await {
        Frame::Spawn { req, agent_id, .. } => {
            assert_eq!(agent_id, agent);
            let reply = json!({"sessionId": session, "reused": false}).to_string();
            send(host, &Frame::Reply { req, status: "200 OK".into(), body: reply }).await;
        }
        other => panic!("expected spawn, got {other:?}"),
    }
    let (status, body) = within("the spawn answer", spawn).await.unwrap();
    assert_eq!(status, "200 OK", "{body}");
    serde_json::from_str(&body).unwrap()
}

fn sid(n: u8) -> String {
    format!("00000000-0000-4000-8000-0000000000{n:02}")
}

async fn gone(host: &mut Ws, remote: &RemoteHost, agent: &str) {
    send(host, &Frame::SessionGone { agent_id: agent.into(), epoch: None, exit_code: Some(137) }).await;
    within("the placement to go", async {
        while remote.placement(agent).is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_re_placed_agent_goes_back_to_the_host_with_its_home_even_when_another_is_less_loaded() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "s7", "/w/s7", &sid(1)).await;
    spawn_on(remote, &mut a, "other", "/w/other", &sid(2)).await;
    let mut b = fake_host(&url, "host-b").await;
    wait_hosts(remote, 2).await;

    // The CLI dies (S7). host-b now has fewer placements, but s7's
    // conversation is on host-a.
    gone(&mut a, remote, "s7").await;
    let answer = spawn_on(remote, &mut a, "s7", "/w/s7", &sid(3)).await;
    assert_eq!(remote.placement("s7").unwrap().host_id, "host-a");
    assert!(answer.get("placementNote").is_none(), "{answer}");

    // A new agent with no home anywhere still goes by load.
    spawn_on(remote, &mut b, "fresh", "/w/fresh", &sid(4)).await;
    assert_eq!(remote.placement("fresh").unwrap().host_id, "host-b");

    // A new agent in a cwd host-a ran follows the cwd (its transcripts).
    gone(&mut a, remote, "other").await;
    spawn_on(remote, &mut a, "other-sidecar", "/w/other", &sid(5)).await;
    assert_eq!(remote.placement("other-sidecar").unwrap().host_id, "host-a");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn when_the_home_host_is_away_the_agent_is_placed_by_load_and_the_answer_says_so() {
    let remote: &'static RemoteHost = Box::leak(Box::new(RemoteHost::new()));
    let url = serve(remote).await;
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "s7", "/w/s7", &sid(1)).await;
    gone(&mut a, remote, "s7").await;
    drop(a);
    let mut b = fake_host(&url, "host-b").await;
    wait_hosts(remote, 1).await;

    let answer = spawn_on(remote, &mut b, "s7", "/w/s7", &sid(2)).await;
    assert_eq!(remote.placement("s7").unwrap().host_id, "host-b");
    let note = answer["placementNote"].as_str().unwrap_or_default();
    assert!(note.contains("home is on host host-a, which is not linked") && note.contains("placed on host-b"), "{answer}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_home_host_is_remembered_across_a_controller_restart() {
    let path = k2_core::test_env::unique_temp_path("affinity.db");
    let open = || {
        let store = Arc::new(PlacementStore::open_file(&path).unwrap());
        let r: &'static RemoteHost = Box::leak(Box::new(RemoteHost::with_store(store, None)));
        r
    };
    {
        let remote = open();
        let url = serve(remote).await;
        let mut a = fake_host(&url, "host-a").await;
        spawn_on(remote, &mut a, "s7", "/w/s7", &sid(1)).await;
        gone(&mut a, remote, "s7").await;
    }
    // A new controller on the same database: host-a holds two other agents,
    // host-b none, and s7 still goes to host-a.
    let remote = open();
    remote.restore();
    let url = serve(remote).await;
    let mut a = fake_host(&url, "host-a").await;
    spawn_on(remote, &mut a, "x1", "/w/x1", &sid(2)).await;
    let _b = fake_host(&url, "host-b").await;
    wait_hosts(remote, 2).await;
    spawn_on(remote, &mut a, "s7", "/w/s7", &sid(3)).await;
    assert_eq!(remote.placement("s7").unwrap().host_id, "host-a");
    std::fs::remove_file(&path).unwrap();
}
