//! P4 T1's unit acceptance, against the fake API server and fake hosts.

use std::time::{Duration, Instant};

use super::super::fake::{pod_json, FakeApi};
use super::*;

/// Enrolled host names and every token minted, in order.
#[derive(Default)]
struct FakeEnroll {
    enrolled: Mutex<Vec<String>>,
    minted: Mutex<Vec<String>>,
}

impl Enroller for FakeEnroll {
    fn enrolled(&self, host_name: &str) -> bool {
        self.enrolled.lock().iter().any(|h| h == host_name)
    }
    fn mint(&self, _ttl_ms: i64) -> Result<String, String> {
        let t = format!("k2e_test_{}", self.minted.lock().len());
        self.minted.lock().push(t.clone());
        Ok(t)
    }
}

const IDLE: Duration = Duration::from_secs(900);

fn pool(replicas: u32, max: u32) -> (Pool, FakeApi, Arc<FakeEnroll>) {
    let api = FakeApi::start();
    api.state.lock().replicas.insert("k2-host".into(), replicas);
    api.state.lock().secrets.insert("k2-host-enroll".into(), Default::default());
    let enroll = Arc::new(FakeEnroll::default());
    let settings = PoolSettings { max_hosts: max, idle_after: IDLE, ..PoolSettings::default() };
    let k8s = K8sPool { client: api.client(), statefulset: "k2-host".into(), enroll_secret: "k2-host-enroll".into() };
    (Pool::new(settings, Box::new(k8s), enroll.clone()), api, enroll)
}

fn host(id: &str, awake: u32, max_awake: u32) -> HostView {
    HostView { host_id: id.into(), linked: true, away: false, draining: false, awake, max_awake }
}

fn replicas(api: &FakeApi) -> u32 {
    api.state.lock().replicas["k2-host"]
}

/// The decoded value of `k2-host-enroll`'s key `host`.
fn enroll_secret(api: &FakeApi, host: &str) -> Option<String> {
    use base64::Engine;
    let st = api.state.lock();
    let b64 = st.secrets["k2-host-enroll"].get(host)?.clone();
    Some(String::from_utf8(base64::engine::general_purpose::STANDARD.decode(b64).unwrap()).unwrap())
}

#[test]
fn a_host_with_room_is_ready_without_touching_the_api() {
    let (p, api, _) = pool(2, 3);
    let hosts = [host("k2-host-0", 2, 2), host("k2-host-1", 1, 2)];
    assert_eq!(p.ensure_host(&hosts), Ensure::Ready("k2-host-1".into()));
    assert!(api.calls().is_empty(), "{:?}", api.calls());
}

#[test]
fn with_max_awake_2_a_third_wake_scales_1_to_2_minting_the_token_first() {
    let (p, api, enroll) = pool(1, 3);
    // Two agents awake on the one host, its k2.max_awake = 2: the third wake.
    let hosts = [host("k2-host-0", 2, 2)];
    match p.ensure_host(&hosts) {
        Ensure::Wait(why) => assert!(why.contains("k2-host-1's signed hello"), "{why}"),
        other => panic!("expected Wait, got {other:?}"),
    }
    assert_eq!(replicas(&api), 2);
    assert_eq!(enroll_secret(&api, "k2-host-1").as_deref(), Some("k2e_test_0"), "the token minted for ordinal 1");
    assert_eq!(enroll.minted.lock().len(), 1);
    let calls = api.calls();
    let secret = calls.iter().position(|(m, p)| m == "PATCH" && p.ends_with("/secrets/k2-host-enroll")).expect("Secret patched");
    let scale = calls.iter().position(|(m, p)| m == "PATCH" && p.ends_with("/statefulsets/k2-host/scale")).expect("scaled");
    assert!(secret < scale, "the token is in the Secret before the scale: {calls:?}");
}

#[test]
fn the_need_is_retried_on_the_new_hosts_hello_never_on_pod_ready() {
    let (p, api, _) = pool(1, 3);
    let full = [host("k2-host-0", 2, 2)];
    assert!(matches!(p.ensure_host(&full), Ensure::Wait(_)));
    // The pod is Running and Ready, but it hasn't said hello: still waiting,
    // and no second scale-up.
    api.state.lock().pods = vec![pod_json("k2-host-1", "Running", true, Some("node-b"))];
    let n = api.calls().len();
    assert!(matches!(p.ensure_host(&full), Ensure::Wait(why) if why.contains("k2-host-1")));
    assert_eq!(replicas(&api), 2);
    assert_eq!(api.calls().len(), n, "Ready is never read, nothing scaled again");
    // Another host's hello doesn't count; k2-host-1's does.
    assert!(!p.on_hello("k2-host-0"));
    assert!(p.on_hello("k2-host-1"));
    let hosts = [host("k2-host-0", 2, 2), host("k2-host-1", 0, 2)];
    assert_eq!(p.ensure_host(&hosts), Ensure::Ready("k2-host-1".into()));
}

#[test]
fn a_pod_without_a_hello_is_waited_for_not_added_to() {
    // A controller restart while k2-host-1 was starting: replicas 2, one hello.
    let (p, api, enroll) = pool(2, 3);
    let ensure = p.ensure_host(&[host("k2-host-0", 2, 2)]);
    assert!(matches!(&ensure, Ensure::Wait(why) if why.contains("k2-host-1 is starting")), "{ensure:?}");
    assert_eq!(replicas(&api), 2);
    assert!(enroll.minted.lock().is_empty());
    assert!(p.on_hello("k2-host-1"));
}

#[test]
fn at_the_maximum_the_refusal_names_the_limit() {
    let (p, api, _) = pool(3, 3);
    let hosts = [host("k2-host-0", 2, 2), host("k2-host-1", 2, 2), host("k2-host-2", 2, 2)];
    match p.ensure_host(&hosts) {
        Ensure::Refused(why) => assert!(why.contains("K2_PROVISIONER_MAX_HOSTS=3") && why.contains("k2.max_awake"), "{why}"),
        other => panic!("expected Refused, got {other:?}"),
    }
    assert_eq!(replicas(&api), 3);
}

#[test]
fn an_enrolled_ordinal_scales_up_without_a_new_token() {
    // k2-host-1 was scaled down before: its key is still on its volume.
    let (p, api, enroll) = pool(1, 3);
    enroll.enrolled.lock().push("k2-host-1".into());
    assert!(matches!(p.ensure_host(&[host("k2-host-0", 2, 2)]), Ensure::Wait(_)));
    assert_eq!(replicas(&api), 2);
    assert!(enroll.minted.lock().is_empty());
    assert_eq!(enroll_secret(&api, "k2-host-1"), None);
}

#[test]
fn after_the_idle_window_the_highest_ordinal_drains_then_scales_down() {
    let (p, api, _) = pool(3, 3);
    let t0 = Instant::now();
    // k2-host-2 has one agent; it fits in the others' room.
    let mut hosts = vec![host("k2-host-0", 1, 2), host("k2-host-1", 0, 2), host("k2-host-2", 1, 2)];
    assert_eq!(p.idle(t0, &hosts), IdleStep::Nothing);
    assert_eq!(p.idle(t0 + IDLE / 2, &hosts), IdleStep::Nothing);
    assert_eq!(p.idle(t0 + IDLE, &hosts), IdleStep::Drain("k2-host-2".into()));
    assert!(p.is_draining("k2-host-2"));
    // While draining it is never chosen, even with room.
    hosts[0].awake = 2;
    hosts[1].awake = 2;
    assert!(matches!(p.ensure_host(&hosts), Ensure::Wait(_) | Ensure::Refused(_)));
    hosts[0].awake = 1;
    hosts[1].awake = 0;
    assert_eq!(p.ensure_host(&hosts), Ensure::Ready("k2-host-1".into()));
    // Its agent is still being released: no scale yet.
    assert_eq!(p.idle(t0 + IDLE * 2, &hosts), IdleStep::Nothing);
    assert_eq!(replicas(&api), 3);
    // Released (P3 sleep moved it): scale down by one.
    hosts[2].awake = 0;
    hosts[1].awake = 1;
    assert_eq!(p.idle(t0 + IDLE * 2, &hosts), IdleStep::ScaledDown { host: "k2-host-2".into(), to: 2 });
    assert_eq!(replicas(&api), 2);
    assert!(!p.is_draining("k2-host-2"));
}

#[test]
fn never_below_the_minimum() {
    let (p, api, _) = pool(1, 3);
    let t0 = Instant::now();
    let hosts = [host("k2-host-0", 0, 2)];
    assert_eq!(p.idle(t0, &hosts), IdleStep::Nothing);
    assert_eq!(p.idle(t0 + IDLE * 10, &hosts), IdleStep::Nothing);
    assert_eq!(replicas(&api), 1);
}

#[test]
fn a_draining_or_away_host_is_never_chosen_and_never_scaled() {
    let (p, api, _) = pool(2, 3);
    let t0 = Instant::now();
    // Away: link down (W3 lost). Not chosen, and no scale-down of it.
    let mut away = host("k2-host-1", 0, 2);
    away.linked = false;
    away.away = true;
    let hosts = [host("k2-host-0", 2, 2), away.clone()];
    // Its room doesn't count: a new host comes rather than wait on it.
    assert!(matches!(p.ensure_host(&hosts), Ensure::Wait(why) if why.contains("k2-host-2")));
    assert_eq!(replicas(&api), 3);
    let (p, api, _) = pool(2, 3);
    let hosts = [host("k2-host-0", 0, 2), away];
    assert_eq!(p.idle(t0, &hosts), IdleStep::Nothing);
    assert_eq!(p.idle(t0 + IDLE * 2, &hosts), IdleStep::Nothing);
    assert_eq!(replicas(&api), 2);

    // Draining on its own (SIGTERM, S8): the same.
    let (p, api, _) = pool(2, 3);
    let mut sigterm = host("k2-host-1", 0, 2);
    sigterm.draining = true;
    let hosts = [host("k2-host-0", 1, 2), sigterm];
    assert_eq!(p.ensure_host(&hosts), Ensure::Ready("k2-host-0".into()));
    assert_eq!(p.idle(t0, &hosts), IdleStep::Nothing);
    assert_eq!(p.idle(t0 + IDLE * 2, &hosts), IdleStep::Nothing);
    assert_eq!(replicas(&api), 2);
}

#[test]
fn settings_parse_and_refuse_nonsense() {
    let env = |pairs: &'static [(&'static str, &'static str)]| move |k: &str| pairs.iter().find(|(n, _)| *n == k).map(|(_, v)| v.to_string());
    assert_eq!(PoolSettings::from_env(env(&[])).unwrap(), PoolSettings::default());
    let s = PoolSettings::from_env(env(&[("K2_PROVISIONER_MAX_HOSTS", "5"), ("K2_PROVISIONER_IDLE_AFTER", "60")])).unwrap();
    assert_eq!((s.min_hosts, s.max_hosts, s.idle_after), (1, 5, Duration::from_secs(60)));
    assert!(PoolSettings::from_env(env(&[("K2_PROVISIONER_MIN_HOSTS", "4"), ("K2_PROVISIONER_MAX_HOSTS", "3")])).unwrap_err().contains("above"));
    assert!(PoolSettings::from_env(env(&[("K2_PROVISIONER_MAX_HOSTS", "three")])).unwrap_err().contains("whole number"));
    assert!(PoolSettings::from_env(env(&[("K2_PROVISIONER_MAX_HOSTS", "0"), ("K2_PROVISIONER_MIN_HOSTS", "0")])).is_err());
}

#[test]
fn ordinals_round_trip() {
    let (p, _, _) = pool(1, 3);
    assert_eq!(p.host_name(4), "k2-host-4");
    assert_eq!(p.ordinal("k2-host-4"), Some(4));
    assert_eq!(p.ordinal("k2-hostile-4"), None);
    assert_eq!(p.ordinal("laptop"), None);
}
