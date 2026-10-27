//! ptyd S3 (`docs/prd/progress/ptyd-s3.md`): a restarted host daemon takes
//! over the agent sessions ptyd kept alive, instead of ending them.
//!
//! What it needs is in each session's ptyd `meta`, written while the
//! previous daemon ran it:
//! - `k2_host: {agent_id, epoch}`: the placement the controller gave it
//!   ([`mark_placed`], when the link's spawn returns);
//! - `k2_turn: {counts, typed}`: its turn as this host followed it
//!   (`drain::Turns`, rewritten on every lead hook and typed message).
//!
//! At boot ([`install`], before the link starts) the host's adopter gets
//! every session ptyd lists. A running one with `k2_host` is adopted: its
//! session is rebuilt (`DaemonPtySession::adopt_ptyd`: the ring replayed
//! into a new Term), wired in as a spawn would be (the session map, the
//! exit observer, its hook socket), and `Placed` and `Turns` get its
//! placement and counts back. The next `hello` resumes it with
//! `adopted: true`. Anything else is ended, as before S3.

#[cfg(unix)]
use std::sync::{Arc, OnceLock};

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
}

pub const META_HOST: &str = "k2_host";
pub const META_TURN: &str = "k2_turn";

/// The trace event for each adopted session.
pub const HOST_ADOPT: &str = "host.adopt";

/// `meta.k2_host` of a session this host can take over: running, and
/// placed by a controller. `None` for anything else (ended at boot).
pub fn adoptable(info: &SessionInfo) -> Option<HostMeta> {
    if !matches!(info.state, SessionState::Running) {
        return None;
    }
    let h = info.meta.get(META_HOST)?;
    let agent_id = h.get("agent_id")?.as_str().filter(|a| !a.is_empty())?.to_string();
    let epoch = h.get("epoch")?.as_u64()?;
    Some(HostMeta { agent_id, epoch })
}

/// `meta.k2_turn`: the counts and the typed messages, as `Turns` keeps them.
pub fn turn_meta(counts: &TurnCounts, typed: Value) -> Value {
    json!({ "counts": counts, "typed": typed })
}

/// The link's spawn returned `session` for `agent_id` at `epoch`: record
/// the placement in its ptyd meta, so a restarted daemon can adopt it. A
/// no-op for an in-process session.
pub fn mark_placed(session: &crate::agent_host::AgentSession, agent_id: &str, epoch: u64) {
    if session.is_remote() {
        return;
    }
    session.pty().set_ptyd_meta(META_HOST, json!({ "agent_id": agent_id, "epoch": epoch }));
}

#[cfg(unix)]
static RUNTIME: OnceLock<tokio::runtime::Handle> = OnceLock::new();

/// Host role, at boot: take over what ptyd kept (when ptyd is on), before
/// the link's first `hello`. Blocking; call it off the runtime's workers.
#[cfg(unix)]
pub fn install(rt: tokio::runtime::Handle) {
    let _ = RUNTIME.set(rt);
    client::set_adopter(Box::new(adopt_leftovers));
    if k2_core::terminal::ptyd::Switch::resolve() == k2_core::terminal::ptyd::Switch::Off {
        return;
    }
    // The first owner connection runs the adopter (and ends the rest).
    if let Err(e) = client::global().owner() {
        log_debug!("[hosts/adopt] ptyd unavailable at boot ({e}): nothing to adopt");
    }
}

/// The adopter: every session ptyd lists; returns those not taken.
#[cfg(unix)]
fn adopt_leftovers(owner: &Arc<Owner>, sessions: Vec<SessionInfo>) -> Vec<SessionInfo> {
    let _rt = RUNTIME.get().map(|h| h.enter());
    let mut rest = Vec::new();
    for info in sessions {
        let Some(placed) = adoptable(&info) else {
            rest.push(info);
            continue;
        };
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
    // row), the per-cell hook socket, the exit observer.
    crate::v2_session_map::register(placed.agent_id.clone(), Arc::clone(&pty));
    crate::session_token::activate_cell_uds(sid, None);
    crate::v2_spawn::spawn_child_exit_observer(
        placed.agent_id.clone(),
        crate::agent_host::AgentSession::in_process(pty),
        None,
        None,
        None,
        None,
    );
    if let Some(t) = info.meta.get(META_TURN) {
        super::drain::restore_turn(&info.sid, t);
    }
    super::link::adopted(&placed.agent_id, &info.sid, placed.epoch);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(state: SessionState, meta: Value) -> SessionInfo {
        SessionInfo {
            sid: "11111111-1111-4111-8111-111111111111".into(),
            pid: 4242,
            state,
            cols: 80,
            rows: 24,
            ring_start: 0,
            ring_end: 0,
            started_at: 0,
            meta,
        }
    }

    /// S3: only a running session a controller placed is adopted; an
    /// exited one, one with no placement (an old daemon's, a non-host
    /// role's), or a malformed one is ended as before.
    #[test]
    fn only_placed_running_sessions_are_adopted() {
        let placed = json!({ "k2_host": { "agent_id": "bob", "epoch": 7 } });
        assert_eq!(adoptable(&info(SessionState::Running, placed.clone())), Some(HostMeta { agent_id: "bob".into(), epoch: 7 }));
        assert_eq!(adoptable(&info(SessionState::Exited { code: Some(0), signal: None, at: 1 }, placed)), None);
        assert_eq!(adoptable(&info(SessionState::Running, json!({ "label": "x" }))), None);
        assert_eq!(adoptable(&info(SessionState::Running, json!({ "k2_host": { "agent_id": "", "epoch": 7 } }))), None);
        assert_eq!(adoptable(&info(SessionState::Running, json!({ "k2_host": { "agent_id": "bob" } }))), None);
        assert_eq!(adoptable(&info(SessionState::Running, Value::Null)), None);
    }
}

/// Not on unix: no ptyd, nothing to adopt.
#[cfg(not(unix))]
pub fn install(_rt: tokio::runtime::Handle) {}
