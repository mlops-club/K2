//! `agent_placement`: each agent's placement policy in the controller's
//! `k2.db` (P3 plan section 2): pinned to a host or portable, its last host,
//! the home snapshot it was last saved at.
//!
//! - Written on every placement: `last_host` always; the mode once, when
//!   the agent is first placed (D5: on a `k2.kind=laptop` host it is pinned
//!   there, anywhere else it is portable). Agents a controller adopts
//!   without a row (they existed before this table) get the same rule from
//!   the host that runs them.
//! - Its own connection to the controller's database file, as
//!   `PlacementStore`'s: it is used while `RemoteHost::inner` is held, so it
//!   must never wait on `k2_core::db::shared()` (W3's boot deadlock).

use std::path::Path;
use std::sync::Arc;

use parking_lot::ReentrantMutex;
use rusqlite::{params, Connection, OptionalExtension};

use super::{HostKind, Mode, Need};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    pub agent: String,
    pub mode: Mode,
    pub last_host: Option<String>,
    pub home_seq: Option<u64>,
}

pub struct PolicyStore {
    db: Arc<ReentrantMutex<Connection>>,
}

impl PolicyStore {
    pub fn open(db: Arc<ReentrantMutex<Connection>>) -> Result<PolicyStore, String> {
        db.lock()
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS agent_placement (
                    agent TEXT PRIMARY KEY,
                    mode TEXT NOT NULL CHECK (mode IN ('pinned', 'portable')),
                    pinned_host TEXT,
                    last_host TEXT,
                    home_seq INTEGER,
                    updated_ms INTEGER NOT NULL
                );",
            )
            .map_err(|e| format!("agent_placement table: {e}"))?;
        Ok(PolicyStore { db })
    }

    /// On its own connection to the database file at `path`.
    pub fn open_file(path: &Path) -> Result<PolicyStore, String> {
        let conn = k2_core::db::open_private_with_resilience(path).map_err(|e| format!("open {}: {e}", path.display()))?;
        Self::open(Arc::new(ReentrantMutex::new(conn)))
    }

    /// A store that lives as long as the process (no database file).
    pub fn in_memory() -> PolicyStore {
        Self::open(Arc::new(ReentrantMutex::new(Connection::open_in_memory().expect("in-memory sqlite"))))
            .expect("agent_placement in memory")
    }

    pub fn get(&self, agent: &str) -> Option<Policy> {
        let c = self.db.lock();
        c.query_row(
            "SELECT agent, mode, pinned_host, last_host, home_seq FROM agent_placement WHERE agent = ?1",
            [agent],
            row_policy,
        )
        .optional()
        .ok()
        .flatten()
    }

    /// What placing `agent` needs: its row, or a new portable agent.
    pub fn need(&self, agent: &str) -> Need {
        match self.get(agent) {
            Some(p) => Need { agent: p.agent, mode: p.mode, last_host: p.last_host, home_seq: p.home_seq, labels: vec![] },
            None => Need::new(agent),
        }
    }

    /// `agent` was placed on `host`: its last host is `host`; a new agent
    /// gets its mode from the host's kind (D5).
    pub fn record_placement(&self, agent: &str, host: &str, kind: HostKind) -> Result<Policy, String> {
        let (mode, pinned) = match kind {
            HostKind::Laptop => ("pinned", Some(host)),
            HostKind::Server => ("portable", None),
        };
        let c = self.db.lock();
        c.execute(
            "INSERT INTO agent_placement (agent, mode, pinned_host, last_host, updated_ms) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(agent) DO UPDATE SET last_host = ?4, updated_ms = ?5",
            params![agent, mode, pinned, host, now_ms()],
        )
        .map_err(|e| format!("agent_placement: {e}"))?;
        drop(c);
        self.get(agent).ok_or_else(|| format!("agent_placement: {agent} vanished"))
    }

    /// An agent a host resumed and the controller had no row for (it
    /// existed before this table): D5 by the host's kind, as a first
    /// placement (pinned on a laptop, portable on a server). An existing row
    /// only gets its last host.
    pub fn record_adopted(&self, agent: &str, host: &str, kind: HostKind) -> Result<(), String> {
        self.record_placement(agent, host, kind).map(|_| ())
    }

    pub fn set_mode(&self, agent: &str, mode: &Mode) -> Result<(), String> {
        let (m, pinned) = match mode {
            Mode::Pinned { host } => ("pinned", Some(host.as_str())),
            Mode::Portable => ("portable", None),
        };
        self.db
            .lock()
            .execute(
                "INSERT INTO agent_placement (agent, mode, pinned_host, updated_ms) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(agent) DO UPDATE SET mode = ?2, pinned_host = ?3, updated_ms = ?4",
                params![agent, m, pinned, now_ms()],
            )
            .map(|_| ())
            .map_err(|e| format!("agent_placement: {e}"))
    }

    pub fn list(&self) -> Vec<Policy> {
        let c = self.db.lock();
        let Ok(mut st) = c.prepare("SELECT agent, mode, pinned_host, last_host, home_seq FROM agent_placement ORDER BY agent") else {
            return vec![];
        };
        st.query_map([], row_policy).map(|rows| rows.filter_map(Result::ok).collect()).unwrap_or_default()
    }

    /// Every host some agent is pinned to: those agents wait for it, so it
    /// stays known (away) while it is gone, however long.
    pub fn pinned_hosts(&self) -> Vec<String> {
        let mut hs: Vec<String> = self
            .list()
            .into_iter()
            .filter_map(|p| match p.mode {
                Mode::Pinned { host } => Some(host),
                Mode::Portable => None,
            })
            .collect();
        hs.sort();
        hs.dedup();
        hs
    }
}

fn row_policy(r: &rusqlite::Row) -> rusqlite::Result<Policy> {
    let mode: String = r.get(1)?;
    let pinned: Option<String> = r.get(2)?;
    let mode = match (mode.as_str(), pinned) {
        ("pinned", Some(host)) => Mode::Pinned { host },
        _ => Mode::Portable,
    };
    Ok(Policy { agent: r.get(0)?, mode, last_host: r.get(3)?, home_seq: r.get::<_, Option<i64>>(4)?.map(|s| s as u64) })
}

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}
