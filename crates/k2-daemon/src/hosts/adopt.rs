//! ptyd S3 (`docs/prd/progress/ptyd-s3.md`): a restarted host daemon takes
//! over the agent sessions ptyd kept alive, instead of ending them.
//!
//! What it needs is in each session's ptyd `meta`, written while the
//! previous daemon ran it:
//! - `k2_host: {agent_id, epoch, controller, host_name, host_id}`: the
//!   placement the controller gave it, and which controller and host that
//!   was ([`mark_placed`], when the link's spawn returns);
//! - `k2_turn: {counts, typed}`: its turn as this host followed it
//!   (`drain::Turns`, rewritten on every lead hook and typed message);
//! - `k2_exit`: what its exit observer was given at spawn.
//!
//! The adopter is set at process start ([`at_process_start`], host role)
//! and adopts only in the boot window: until [`install`] returns, before
//! the link's first `hello` (S3.1, review #1). Within it, ptyd's sessions
//! are sorted by [`pick`]: per agent the highest epoch is adopted if it was
//! placed by this controller for this host and its hook passport is live;
//! anything else is ended, as before S3. An adopted session is rebuilt
//! (`DaemonPtySession::adopt_ptyd`: the ring replayed into a new Term),
//! wired in as a spawn would be (the session map, the exit observer, its
//! hook socket), and `Placed` and `Turns` get its placement and counts back.
//! The next `hello` resumes it with `adopted: true`; one no welcomed hello
//! confirms within `K2_HOST_MOVE_AFTER` is ended ([`fence`]).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(unix)]
use std::sync::Arc;
use std::sync::OnceLock;

#[cfg(unix)]
use k2_core::log_debug;
#[cfg(unix)]
use k2_core::session::SessionId;
#[cfg(unix)]
use k2_core::terminal::ptyd::client::{self, Owner};
use k2_core::terminal::ptyd::{SessionInfo, SessionState};
use serde_json::{json, Value};

use super::proto::TurnCounts;

/// The placement a session's meta records (`meta.k2_host`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostMeta {
    pub agent_id: String,
    pub epoch: u64,
    /// The controller's attach URL and this host's name and id when it was
    /// placed. Absent in meta an S3 daemon wrote.
    pub controller: Option<String>,
    pub host_name: Option<String>,
    pub host_id: Option<String>,
}

/// Who this host is now: its link's controller and its name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub controller: String,
    pub host_name: String,
}

pub const META_HOST: &str = "k2_host";
pub const META_TURN: &str = "k2_turn";
pub const META_EXIT: &str = "k2_exit";

/// The trace event for each adopted session.
pub const HOST_ADOPT: &str = "host.adopt";

/// `meta.k2_host` of a session this host may take over: running, and
/// placed by a controller. `None` for anything else (ended at boot).
pub fn adoptable(info: &SessionInfo) -> Option<HostMeta> {
    if !matches!(info.state, SessionState::Running) {
        return None;
    }
    let h = info.meta.get(META_HOST)?;
    let agent_id = h.get("agent_id")?.as_str().filter(|a| !a.is_empty())?.to_string();
    let epoch = h.get("epoch")?.as_u64()?;
    let s = |k: &str| h.get(k).and_then(|v| v.as_str()).map(str::to_string);
    Some(HostMeta { agent_id, epoch, controller: s("controller"), host_name: s("host_name"), host_id: s("host_id") })
}

/// S3.1: which of ptyd's sessions this host adopts, and which it ends.
/// - not [`adoptable`]: ended;
/// - placed by another controller, or for another host name (#3): ended.
///   Meta without them (an S3 daemon's) is taken as this host's;
/// - its hook passport is not live (`live_passport`, #7: revoked, or
///   never minted by this host): ended;
/// - two or more for one agent (#2): the highest epoch is adopted, the
///   rest ended.
pub fn pick(
    sessions: Vec<SessionInfo>,
    me: &Identity,
    live_passport: impl Fn(&str) -> bool,
) -> (Vec<(SessionInfo, HostMeta)>, Vec<(SessionInfo, String)>) {
    let mut end = Vec::new();
    let mut best: HashMap<String, (SessionInfo, HostMeta)> = HashMap::new();
    for info in sessions {
        let Some(m) = adoptable(&info) else {
            end.push((info, "not placed by a controller, or not running".to_string()));
            continue;
        };
        if m.controller.as_deref().is_some_and(|c| c != me.controller) {
            let why = format!("placed by controller {}, not {}", m.controller.as_deref().unwrap_or("?"), me.controller);
            end.push((info, why));
            continue;
        }
        if m.host_name.as_deref().is_some_and(|h| h != me.host_name) {
            let why = format!("placed on host {}, not {}", m.host_name.as_deref().unwrap_or("?"), me.host_name);
            end.push((info, why));
            continue;
        }
        if !live_passport(&info.sid) {
            end.push((info, "its hook passport is not live (revoked or never minted here)".to_string()));
            continue;
        }
        match best.remove(&m.agent_id) {
            Some((older, om)) if om.epoch >= m.epoch => {
                end.push((info, format!("{} has a newer session at epoch {}", m.agent_id, om.epoch)));
                best.insert(om.agent_id.clone(), (older, om));
            }
            Some((older, _)) => {
                end.push((older, format!("{} has a newer session at epoch {}", m.agent_id, m.epoch)));
                best.insert(m.agent_id.clone(), (info, m));
            }
            None => {
                best.insert(m.agent_id.clone(), (info, m));
            }
        }
    }
    let mut adopt: Vec<_> = best.into_values().collect();
    adopt.sort_by(|a, b| a.1.agent_id.cmp(&b.1.agent_id));
    (adopt, end)
}

/// `meta.k2_turn`: the counts and the typed messages, as `Turns` keeps them.
pub fn turn_meta(counts: &TurnCounts, typed: Value) -> Value {
    json!({ "counts": counts, "typed": typed })
}

/// The link's spawn returned `session` for `agent_id` at `epoch`: record
/// the placement, and the controller and host it was made for, in its ptyd
/// meta, so a restarted daemon can adopt it. A no-op for an in-process
/// session.
pub fn mark_placed(session: &crate::agent_host::AgentSession, agent_id: &str, epoch: u64) {
    if session.is_remote() {
        return;
    }
    let (controller, host_name, host_id) = super::link::identity();
    session.pty().set_ptyd_meta(
        META_HOST,
        json!({ "agent_id": agent_id, "epoch": epoch, "controller": controller, "host_name": host_name, "host_id": host_id }),
    );
}

/// S3.1 #8: what a session's exit observer was given at spawn, kept in its
/// ptyd meta so an adopted session's observer gets the same.
pub fn mark_exit(pty: &k2_core::terminal::DaemonPtySession, ephemeral_cwd: Option<&std::path::Path>, principal_key: Option<&str>, quota_workspace: Option<&str>) {
    if ephemeral_cwd.is_none() && principal_key.is_none() && quota_workspace.is_none() {
        return;
    }
    pty.set_ptyd_meta(
        META_EXIT,
        json!({
            "ephemeral_cwd": ephemeral_cwd.map(|p| p.to_string_lossy().into_owned()),
            "principal_key": principal_key,
            "quota_workspace": quota_workspace,
        }),
    );
}

/// The exit observer's inputs from `meta.k2_exit`: (ephemeral cwd,
/// principal key, quota workspace).
pub fn exit_meta(meta: &Value) -> (Option<std::path::PathBuf>, Option<String>, Option<String>) {
    let e = meta.get(META_EXIT);
    let s = |k: &str| e.and_then(|e| e.get(k)).and_then(|v| v.as_str()).map(str::to_string);
    (s("ephemeral_cwd").map(std::path::PathBuf::from), s("principal_key"), s("quota_workspace"))
}

static IDENTITY: OnceLock<Identity> = OnceLock::new();
/// The boot window: closed once [`install`] returns (the link starts then).
static CLOSED: AtomicBool = AtomicBool::new(false);

/// Is the boot window still open (adoption allowed)?
pub fn window_open() -> bool {
    !CLOSED.load(Ordering::SeqCst)
}

/// S3.1 #6: the host role's adopter, set at process start (before the HTTP
/// API can spawn anything and so make the first ptyd connection). Without
/// a valid host config nothing is set: boot fails on it later anyway.
pub fn at_process_start() {
    if crate::runtime_role::current() != crate::runtime_role::Role::Host {
        return;
    }
    let Ok(cfg) = super::host_config(|k| std::env::var(k).ok()) else { return };
    let _ = IDENTITY.set(Identity { controller: cfg.attach_url, host_name: cfg.host_name });
    #[cfg(unix)]
    client::set_adopter(Box::new(adopt_leftovers));
}

#[cfg(unix)]
static RUNTIME: OnceLock<tokio::runtime::Handle> = OnceLock::new();

/// How long a host waits at boot for ptyd before it links without adopting
/// (`K2_PTYD_ADOPT_WAIT` seconds; default 10, or 60 with a sidecar ptyd,
/// `K2_PTYD_SIDECAR`, which may come up after the daemon's container).
pub const ADOPT_WAIT_VAR: &str = "K2_PTYD_ADOPT_WAIT";
pub const DEFAULT_ADOPT_WAIT: std::time::Duration = std::time::Duration::from_secs(10);
pub const DEFAULT_SIDECAR_ADOPT_WAIT: std::time::Duration = std::time::Duration::from_secs(60);

pub fn adopt_wait(get: impl Fn(&str) -> Option<String>) -> std::time::Duration {
    let sidecar = matches!(get("K2_PTYD_SIDECAR").map(|v| v.trim().to_ascii_lowercase()).as_deref(), Some("1" | "on" | "true"));
    let default = if sidecar { DEFAULT_SIDECAR_ADOPT_WAIT } else { DEFAULT_ADOPT_WAIT };
    match get(ADOPT_WAIT_VAR).map(|v| v.trim().to_string()).filter(|v| !v.is_empty()) {
        None => default,
        Some(v) => match v.parse::<u64>() {
            Ok(n) => std::time::Duration::from_secs(n),
            Err(_) => {
                k2_core::log_debug!("[hosts/adopt] {ADOPT_WAIT_VAR}={v:?} is not a number of seconds; using {}", default.as_secs());
                default
            }
        },
    }
}

/// Host role, at boot: take over what ptyd kept (when ptyd is on), before
/// the link's first `hello`; then close the window. ptyd is retried for
/// [`adopt_wait`] (#1): a connection after the link is up adopts nothing.
/// Then [`fence`] runs once `K2_HOST_MOVE_AFTER` has passed. Blocking; call
/// it off the runtime's workers.
pub fn install(rt: tokio::runtime::Handle) {
    #[cfg(unix)]
    {
        let _ = RUNTIME.set(rt.clone());
        if k2_core::terminal::ptyd::Switch::resolve() == k2_core::terminal::ptyd::Switch::On {
            let wait = adopt_wait(|k| std::env::var(k).ok());
            let start = std::time::Instant::now();
            loop {
                match client::global().owner() {
                    Ok(_) => break,
                    Err(e) if start.elapsed() >= wait => {
                        log_debug!("[hosts/adopt] ptyd unavailable at boot ({e}): nothing adopted; a later connection ends what it kept");
                        break;
                    }
                    Err(_) => std::thread::sleep(std::time::Duration::from_millis(250)),
                }
            }
        }
    }
    CLOSED.store(true, Ordering::SeqCst);
    let after = super::drain::move_after(|k| std::env::var(k).ok());
    rt.spawn(async move {
        tokio::time::sleep(after).await;
        tokio::task::spawn_blocking(fence);
    });
}

/// S3.1 #4: adopted agents no welcomed `hello` confirmed (the controller
/// was unreachable, or refused this host) are ended: by now the controller
/// has moved them, so this copy is stale.
pub fn fence() {
    for agent in super::link::take_unconfirmed() {
        k2_core::log_debug!("[hosts/adopt] agent {agent}: adopted at boot but no welcomed hello confirmed it; ended");
        end_agent(&agent);
    }
}

/// End `agent`'s session on this host (unplaced, its CLI killed).
pub fn end_agent(agent: &str) {
    let h = crate::agent_host::host();
    if let Some(s) = h.unplace(agent) {
        h.kill(&s);
    }
}

/// S3.1 #1: after the boot window, a first ptyd connection adopts nothing:
/// every leftover is returned, to be ended. `None` while the window is open.
pub fn too_late(sessions: Vec<SessionInfo>) -> Result<Vec<SessionInfo>, Vec<SessionInfo>> {
    if window_open() {
        Err(sessions)
    } else {
        k2_core::log_debug!("[hosts/adopt] ptyd connected after the link started: {} leftover session(s) ended, none adopted", sessions.len());
        Ok(sessions)
    }
}

/// The adopter: every session ptyd lists; returns those not taken.
#[cfg(unix)]
fn adopt_leftovers(owner: &Arc<Owner>, sessions: Vec<SessionInfo>) -> Vec<SessionInfo> {
    let sessions = match too_late(sessions) {
        Ok(all) => return all,
        Err(sessions) => sessions,
    };
    let Some(me) = IDENTITY.get() else { return sessions };
    let _rt = RUNTIME.get().map(|h| h.enter());
    let live = |sid: &str| !crate::session_token::scoped_hooks_enabled() || crate::session_token::has_live_passport(sid);
    let (adopt, end) = pick(sessions, me, live);
    let mut rest: Vec<SessionInfo> = Vec::new();
    for (info, why) in end {
        if adoptable(&info).is_some() {
            log_debug!("[hosts/adopt] session {}: not adopted ({why}); ended", info.sid);
        }
        rest.push(info);
    }
    for (info, placed) in adopt {
        match adopt_one(owner, &info, &placed) {
            Ok(()) => {
                log_debug!(
                    "[hosts/adopt] adopted agent {} session {} epoch {} (pid {})",
                    placed.agent_id,
                    info.sid,
                    placed.epoch,
                    info.pid
                );
                k2_core::trace::emit(|| {
                    k2_core::trace::Event::new(HOST_ADOPT)
                        .agent(placed.agent_id.as_str())
                        .pane(&info.sid)
                        .attr("epoch", placed.epoch)
                        .attr("pid", info.pid as i64)
                });
            }
            Err(e) => {
                log_debug!("[hosts/adopt] agent {} session {}: not adopted ({e}); ended", placed.agent_id, info.sid);
                rest.push(info);
            }
        }
    }
    rest
}

#[cfg(unix)]
fn adopt_one(owner: &Arc<Owner>, info: &SessionInfo, placed: &HostMeta) -> Result<(), String> {
    let sid = SessionId::parse(&info.sid).ok_or_else(|| format!("bad session id {}", info.sid))?;
    let pty = k2_core::terminal::DaemonPtySession::adopt_ptyd(Arc::clone(owner), info).map_err(|e| e.to_string())?;
    // As a spawn does once its PTY is open: the map (and the activity
    // row), the per-cell hook socket, the exit observer with what it was
    // given at spawn (#8).
    crate::v2_session_map::register(placed.agent_id.clone(), Arc::clone(&pty));
    crate::session_token::activate_cell_uds(sid, None);
    let (ephemeral_cwd, principal_key, quota_workspace) = exit_meta(&info.meta);
    crate::v2_spawn::spawn_child_exit_observer(
        placed.agent_id.clone(),
        crate::agent_host::AgentSession::in_process(pty),
        ephemeral_cwd,
        principal_key,
        quota_workspace,
        None,
    );
    if let Some(t) = info.meta.get(META_TURN) {
        super::drain::restore_turn(&info.sid, t);
    }
    super::link::adopted(&placed.agent_id, &info.sid, placed.epoch, placed.host_id.clone());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(sid: &str, state: SessionState, meta: Value) -> SessionInfo {
        SessionInfo { sid: sid.into(), pid: 4242, state, cols: 80, rows: 24, ring_start: 0, ring_end: 0, started_at: 0, meta }
    }

    fn me() -> Identity {
        Identity { controller: "ws://ctl/cli/hosts/attach".into(), host_name: "host-1".into() }
    }

    fn placed(agent: &str, epoch: u64) -> Value {
        json!({ "k2_host": { "agent_id": agent, "epoch": epoch, "controller": "ws://ctl/cli/hosts/attach", "host_name": "host-1", "host_id": "h-1" } })
    }

    fn sids(v: &[(SessionInfo, HostMeta)]) -> Vec<String> {
        v.iter().map(|(i, _)| i.sid.clone()).collect()
    }

    /// S3: only a running session a controller placed is adopted; an
    /// exited one, one with no placement (an old daemon's, a non-host
    /// role's), or a malformed one is ended as before.
    #[test]
    fn only_placed_running_sessions_are_adopted() {
        let p = placed("bob", 7);
        let m = adoptable(&info("s", SessionState::Running, p.clone())).unwrap();
        assert_eq!((m.agent_id.as_str(), m.epoch, m.host_id.as_deref()), ("bob", 7, Some("h-1")));
        assert_eq!(adoptable(&info("s", SessionState::Exited { code: Some(0), signal: None, at: 1 }, p)), None);
        assert_eq!(adoptable(&info("s", SessionState::Running, json!({ "label": "x" }))), None);
        assert_eq!(adoptable(&info("s", SessionState::Running, json!({ "k2_host": { "agent_id": "", "epoch": 7 } }))), None);
        assert_eq!(adoptable(&info("s", SessionState::Running, json!({ "k2_host": { "agent_id": "bob" } }))), None);
        assert_eq!(adoptable(&info("s", SessionState::Running, Value::Null)), None);
    }

    /// S3.1 #2: two sessions for one agent (a respawn the old daemon never
    /// ended): the highest epoch is adopted, whatever the listing order;
    /// the other is ended.
    #[test]
    fn one_session_per_agent_the_highest_epoch_wins() {
        for order in [[3u64, 9], [9, 3]] {
            let sessions = order.iter().map(|e| info(&format!("s{e}"), SessionState::Running, placed("bob", *e))).collect();
            let (adopt, end) = pick(sessions, &me(), |_| true);
            assert_eq!(sids(&adopt), vec!["s9".to_string()], "{order:?}");
            assert_eq!(end.len(), 1);
            assert_eq!(end[0].0.sid, "s3");
            assert!(end[0].1.contains("newer session at epoch 9"), "{}", end[0].1);
        }
        // Different agents are each adopted.
        let (adopt, end) = pick(
            vec![info("a", SessionState::Running, placed("ann", 1)), info("b", SessionState::Running, placed("bob", 2))],
            &me(),
            |_| true,
        );
        assert_eq!((sids(&adopt), end.len()), (vec!["a".to_string(), "b".to_string()], 0));
    }

    /// S3.1 #3: a session another controller placed, or placed for another
    /// host name (a volume reused under another name), is ended; meta from
    /// before these were kept (an S3 daemon's) is this host's.
    #[test]
    fn a_session_placed_by_another_controller_or_for_another_host_is_ended() {
        let other_ctl = json!({ "k2_host": { "agent_id": "bob", "epoch": 1, "controller": "ws://old/cli/hosts/attach", "host_name": "host-1" } });
        let other_host = json!({ "k2_host": { "agent_id": "cy", "epoch": 1, "controller": "ws://ctl/cli/hosts/attach", "host_name": "host-2" } });
        let s3_meta = json!({ "k2_host": { "agent_id": "dee", "epoch": 1 } });
        let (adopt, end) = pick(
            vec![
                info("a", SessionState::Running, other_ctl),
                info("b", SessionState::Running, other_host),
                info("c", SessionState::Running, s3_meta),
            ],
            &me(),
            |_| true,
        );
        assert_eq!(sids(&adopt), vec!["c".to_string()]);
        let why: Vec<&str> = end.iter().map(|(_, w)| w.as_str()).collect();
        assert!(why[0].contains("controller ws://old"), "{why:?}");
        assert!(why[1].contains("host host-2"), "{why:?}");
    }

    /// S3.1 #7: a session whose hook passport is not live here (revoked at
    /// its exit, or never minted by this host: meta an agent rewrote) is
    /// ended, never adopted.
    #[test]
    fn a_session_without_a_live_passport_is_ended() {
        let (adopt, end) = pick(
            vec![info("good", SessionState::Running, placed("ann", 1)), info("forged", SessionState::Running, placed("bob", 1))],
            &me(),
            |sid| sid == "good",
        );
        assert_eq!(sids(&adopt), vec!["good".to_string()]);
        assert!(end[0].1.contains("passport"), "{}", end[0].1);
    }

    /// S3.1 #8: what the exit observer was given round-trips through the
    /// meta; a session spawned with none of it has no `k2_exit`.
    #[test]
    fn the_exit_observers_inputs_come_back_from_the_meta() {
        let m = json!({ "k2_exit": { "ephemeral_cwd": "/tmp/eph", "principal_key": "p1", "quota_workspace": null } });
        assert_eq!(exit_meta(&m), (Some("/tmp/eph".into()), Some("p1".to_string()), None));
        assert_eq!(exit_meta(&json!({})), (None, None, None));
    }

    /// S3.1 #1: the boot window closes when `install` returns (the link
    /// starts then); a first ptyd connection after it adopts nothing, so a
    /// late one can't overwrite a placement the controller made since.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn after_the_boot_window_leftovers_are_ended_not_adopted() {
        let leftovers = || vec![info("late", SessionState::Running, placed("bob", 1))];
        assert!(too_late(leftovers()).is_err(), "open at process start");
        let rt = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || install(rt)).await.unwrap();
        assert!(!window_open());
        assert_eq!(too_late(leftovers()).map(|v| v.len()), Ok(1), "everything goes back to be ended");
    }

    #[test]
    fn the_adopt_wait_comes_from_the_env() {
        let env = |v: Option<&str>| adopt_wait(move |_| v.map(str::to_string));
        assert_eq!(env(None), DEFAULT_ADOPT_WAIT);
        assert_eq!(env(Some("3")), std::time::Duration::from_secs(3));
        assert_eq!(env(Some("soon")), DEFAULT_ADOPT_WAIT);
        let sidecar = |v: Option<&str>| adopt_wait(move |k| if k == "K2_PTYD_SIDECAR" { Some("1".into()) } else { v.map(str::to_string) });
        assert_eq!(sidecar(None), DEFAULT_SIDECAR_ADOPT_WAIT, "a sidecar ptyd may start after the daemon");
        assert_eq!(sidecar(Some("5")), std::time::Duration::from_secs(5));
    }
}
