//! Unified lookup over the daemon's agent sessions.
//!
//! Every lookup goes through the daemon's [`crate::agent_host::AgentHost`]
//! (P1: the in-process host, whose placement table is
//! [`crate::v2_session_map`]).
//!
//! Historically there was a parallel `session_map` for the Kessel-T0
//! `SessionStreamSession` path; that was retired in 0.39.0 along with
//! the legacy renderer. [`LiveSession`] is the older name of
//! [`crate::agent_host::AgentSession`], kept so call sites that
//! previously branched on Legacy/V2 keep their signatures stable.

use k2_core::session::SessionId;

use crate::agent_host::host;

/// One live session: the [`crate::agent_host::AgentSession`] handle under
/// its older name. Cloning is cheap; dropping it doesn't tear the session
/// down unless it's the last reference.
pub use crate::agent_host::AgentSession as LiveSession;

/// Look up an agent in the v2 session map.
pub fn lookup_any(agent: &str) -> Option<LiveSession> {
    host().find_agent(agent)
}

/// Look up by `SessionId` rather than agent name. Used by the
/// `/cli/terminal/{write,resize}` HTTP routes — those callers know
/// the session uuid but not the agent name behind it.
pub fn lookup_by_session_id(id: &SessionId) -> Option<LiveSession> {
    host().find_session(id)
}

/// Every (agent_name, session) pair registered in the v2 map.
/// Returning owned `LiveSession`s lets the caller drop the
/// underlying mutex before doing expensive per-session work
/// (PTY writes, child kills, frame emits).
pub fn snapshot_all() -> Vec<(String, LiveSession)> {
    host().placements()
}

/// All registered agent names in the v2 map.
#[allow(dead_code)]
pub fn list_agents() -> Vec<String> {
    let mut names: Vec<String> = host().placements().into_iter().map(|(name, _)| name).collect();
    names.sort();
    names.dedup();
    names
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `lookup_any` returns None when the map has no entry. Sanity
    /// check that the lookup path doesn't error.
    #[test]
    fn lookup_any_returns_none_for_unknown_agent() {
        let result = lookup_any("nonexistent-agent-xyz-test-only");
        assert!(result.is_none());
    }

    /// `snapshot_all` returns an empty vec when the map is empty.
    /// Tests are isolated per binary run, so the global map may
    /// hold entries from other tests in the same process — we don't
    /// assert emptiness, just that the call succeeds without panic.
    #[test]
    fn snapshot_all_does_not_panic() {
        let _ = snapshot_all();
    }
}
