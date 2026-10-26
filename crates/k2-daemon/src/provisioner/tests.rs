use super::policy::PolicyStore;
use super::static_::Static;
use super::*;
use crate::runtime_role::Role;

fn host(id: &str, awake: u32, labels: &[&str]) -> HostView {
    let mut v = HostView::from_labels(id, &labels.iter().map(|l| l.to_string()).collect::<Vec<_>>());
    v.linked = true;
    v.awake = awake;
    v
}

fn away(mut v: HostView) -> HostView {
    v.linked = false;
    v.away = true;
    v
}

fn need(agent: &str, mode: Mode, last: Option<&str>) -> Need {
    Need { agent: agent.into(), mode, last_host: last.map(str::to_string), home_seq: None, labels: vec![] }
}

#[test]
fn labels_give_capacity_and_kind() {
    let v = host("eric-mbp", 1, &["k2.kind=laptop", "k2.max_awake=3", "pool=default", "gpu"]);
    assert_eq!((v.kind, v.max_awake, v.room()), (HostKind::Laptop, Some(3), Some(2)));
    assert!(v.matches(&["pool=default".into(), "gpu".into()]));
    assert!(!v.matches(&["pool=other".into()]));
    let s = host("host-1", 5, &["pool=default"]);
    assert_eq!((s.kind, s.max_awake, s.room()), (HostKind::Server, None, None));
    assert!(s.has_room(), "no k2.max_awake: no limit");
}

#[test]
fn with_no_limits_it_is_todays_choice_fewest_awake_then_name() {
    let hosts = [host("host-b", 1, &[]), host("host-a", 1, &[]), host("host-c", 0, &[])];
    assert_eq!(Static.choose(&Need::new("x"), &hosts).unwrap(), Choice { host: "host-c".into(), why: Why::MostRoom });
    let hosts = [host("host-b", 1, &[]), host("host-a", 1, &[])];
    assert_eq!(Static.choose(&Need::new("x"), &hosts).unwrap().host, "host-a");
    assert_eq!(Static.choose(&Need::new("x"), &[]), Err(Refusal::NoHosts));
    assert_eq!(Refusal::NoHosts.message(), crate::agent_host::remote::NO_HOSTS);
}

#[test]
fn a_wake_goes_back_to_the_last_host() {
    // host-2 has more room, but the agent's home is on host-1.
    let hosts = [host("host-1", 3, &["k2.max_awake=4"]), host("host-2", 0, &["k2.max_awake=4"])];
    let n = need("bob", Mode::Portable, Some("host-1"));
    assert_eq!(Static.choose(&n, &hosts).unwrap(), Choice { host: "host-1".into(), why: Why::LastHost });
    // Its last host full or away: the most room elsewhere.
    let full = [host("host-1", 4, &["k2.max_awake=4"]), host("host-2", 1, &["k2.max_awake=4"])];
    assert_eq!(Static.choose(&n, &full).unwrap(), Choice { host: "host-2".into(), why: Why::MostRoom });
    let gone = [away(host("host-1", 0, &[])), host("host-2", 1, &[])];
    assert_eq!(Static.choose(&n, &gone).unwrap().host, "host-2");
}

#[test]
fn a_pinned_agent_waits_for_its_away_host_and_is_never_placed_elsewhere() {
    let n = need("notes", Mode::Pinned { host: "eric-mbp".into() }, Some("eric-mbp"));
    let hosts = [away(host("eric-mbp", 0, &["k2.kind=laptop"])), host("host-1", 0, &[]), host("host-2", 0, &[])];
    let r = Static.choose(&n, &hosts).unwrap_err();
    assert_eq!(r, Refusal::PinnedAway { agent: "notes".into(), host: "eric-mbp".into() });
    assert!(r.message().contains("never placed elsewhere"), "{}", r.message());
    // ensure_host makes that a wait (retried when eric-mbp says hello).
    assert!(matches!(Static.ensure_host(&n, &hosts), Ensure::Wait(_)));
    // Linked again: it goes there, even with less room than elsewhere.
    let hosts = [host("eric-mbp", 1, &["k2.kind=laptop", "k2.max_awake=2"]), host("host-1", 0, &[])];
    assert_eq!(Static.choose(&n, &hosts).unwrap(), Choice { host: "eric-mbp".into(), why: Why::Pinned });
    // Linked but full: refused naming the limit, still not elsewhere.
    let hosts = [host("eric-mbp", 2, &["k2.kind=laptop", "k2.max_awake=2"]), host("host-1", 0, &[])];
    let r = Static.choose(&n, &hosts).unwrap_err();
    assert!(matches!(r, Refusal::PinnedFull { max_awake: 2, .. }), "{r:?}");
    assert!(r.message().contains("K2_HOST_MAX_AWAKE"), "{}", r.message());
}

#[test]
fn capacity_is_respected_and_the_refusal_names_the_limit() {
    let hosts = [host("host-1", 2, &["k2.max_awake=2"]), host("host-2", 2, &["k2.max_awake=2"])];
    let r = Static.choose(&Need::new("x"), &hosts).unwrap_err();
    assert_eq!(r, Refusal::Full { hosts: 2 });
    assert!(r.message().contains("K2_HOST_MAX_AWAKE"), "{}", r.message());
    assert!(matches!(Static.ensure_host(&Need::new("x"), &hosts), Ensure::Refused(m) if m.contains("k2.max_awake")));
    // The most room wins; an unlimited host counts as the most.
    let hosts = [host("host-1", 1, &["k2.max_awake=4"]), host("host-2", 0, &["k2.max_awake=2"])];
    assert_eq!(Static.choose(&Need::new("x"), &hosts).unwrap().host, "host-1");
    let hosts = [host("host-1", 0, &["k2.max_awake=9"]), host("host-2", 7, &[])];
    assert_eq!(Static.choose(&Need::new("x"), &hosts).unwrap().host, "host-2");
}

#[test]
fn choice_order_with_labels() {
    let hosts = [host("host-1", 0, &["pool=cpu"]), host("host-2", 3, &["pool=gpu", "gpu"])];
    let mut n = Need::new("x");
    n.labels = vec!["pool=gpu".into()];
    assert_eq!(Static.choose(&n, &hosts).unwrap().host, "host-2");
    // The last host without the labels isn't used.
    n.last_host = Some("host-1".into());
    assert_eq!(Static.choose(&n, &hosts).unwrap().host, "host-2");
    n.labels = vec!["tpu".into()];
    assert_eq!(Static.choose(&n, &hosts), Err(Refusal::NoMatch { labels: vec!["tpu".into()] }));
}

#[test]
fn away_and_draining_hosts_are_skipped() {
    let mut drain = host("host-1", 0, &[]);
    drain.draining = true;
    let hosts = [drain, away(host("host-2", 0, &[])), host("host-3", 9, &[])];
    assert_eq!(Static.choose(&Need::new("x"), &hosts).unwrap().host, "host-3");
    let hosts = [away(host("host-2", 0, &[]))];
    assert_eq!(Static.choose(&Need::new("x"), &hosts), Err(Refusal::NoHosts));
}

#[test]
fn policy_mode_comes_from_the_first_hosts_kind_and_last_host_from_every_placement() {
    let p = PolicyStore::in_memory();
    assert_eq!(p.need("bob"), Need::new("bob"), "no row: a new portable agent");
    // First placed on a laptop: pinned there (D5).
    let row = p.record_placement("notes", "eric-mbp", HostKind::Laptop).unwrap();
    assert_eq!((row.mode, row.last_host.as_deref()), (Mode::Pinned { host: "eric-mbp".into() }, Some("eric-mbp")));
    // On a server: portable; later placements only move last_host.
    p.record_placement("bob", "host-1", HostKind::Server).unwrap();
    let row = p.record_placement("bob", "host-2", HostKind::Laptop).unwrap();
    assert_eq!((row.mode, row.last_host.as_deref()), (Mode::Portable, Some("host-2")), "the mode is set once");
    assert_eq!(p.need("bob").last_host.as_deref(), Some("host-2"));
    // Adopted without a row (from before the table): by the host's kind.
    p.record_adopted("old-server", "host-1", HostKind::Server).unwrap();
    assert_eq!(p.get("old-server").unwrap().mode, Mode::Portable, "a server's agent is portable");
    assert_eq!(p.get("old-server").unwrap().last_host.as_deref(), Some("host-1"));
    p.record_adopted("old-laptop", "eric-mbp", HostKind::Laptop).unwrap();
    assert_eq!(p.get("old-laptop").unwrap().mode, Mode::Pinned { host: "eric-mbp".into() }, "a laptop's agent is pinned");
    p.record_adopted("bob", "host-3", HostKind::Laptop).unwrap();
    assert_eq!(p.get("bob").unwrap().mode, Mode::Portable, "an existing row keeps its mode");
    p.set_mode("bob", &Mode::Pinned { host: "host-3".into() }).unwrap();
    assert_eq!(p.need("bob").mode, Mode::Pinned { host: "host-3".into() });
    // Only pinned hosts are kept known: a last host alone isn't.
    assert_eq!(p.pinned_hosts(), vec!["eric-mbp", "host-3"]);
}

#[test]
fn policy_rows_survive_a_reopen() {
    let db = k2_core::test_env::unique_temp_path("agent-placement").with_extension("db");
    PolicyStore::open_file(&db).unwrap().record_placement("notes", "eric-mbp", HostKind::Laptop).unwrap();
    let again = PolicyStore::open_file(&db).unwrap();
    assert_eq!(again.need("notes").mode, Mode::Pinned { host: "eric-mbp".into() });
    let _ = std::fs::remove_file(&db);
}

#[test]
fn k2_provisioner_by_role() {
    assert_eq!(select(Role::All, None).unwrap().kind(), "local");
    assert_eq!(select(Role::All, Some("")).unwrap().kind(), "local");
    assert!(select(Role::All, Some("static")).err().unwrap().contains("K2_ROLE=controller"));
    assert_eq!(select(Role::Controller, None).unwrap().kind(), "static");
    assert_eq!(select(Role::Controller, Some("static")).unwrap().kind(), "static");
    assert_eq!(select(Role::Controller, Some("kubernetes")).unwrap().kind(), "static", "until P4 lands");
    assert!(select(Role::Controller, Some("local")).is_err());
    assert!(select(Role::Controller, Some("k8s")).err().unwrap().contains("use local, static or kubernetes"));
    assert_eq!(select(Role::Host, Some("static")).unwrap().kind(), "local");
    assert_eq!(Local.choose(&Need::new("x"), &[]).unwrap().host, LOCAL_HOST);
}
