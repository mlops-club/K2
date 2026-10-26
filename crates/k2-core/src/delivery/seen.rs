//! The receiving side's dedupe table: `msg_id`s this daemon already
//! handled, with the answer it gave (`delivered` or `rejected`), so a
//! resend after a lost ack is answered `duplicate` and never reaches the
//! agent twice. In the database, so it survives a restart (unlike an
//! in-memory cache). Bounded to [`super::Policy::seen_cap`] ids, oldest
//! dropped first.

use rusqlite::{params, Connection, OptionalExtension};

/// The answer recorded for `msg_id`, if it was handled before.
pub fn status(conn: &Connection, msg_id: &str) -> rusqlite::Result<Option<String>> {
    conn.query_row("SELECT status FROM delivery_seen WHERE msg_id = ?1", params![msg_id], |r| r.get(0))
        .optional()
}

/// Where a delivery was going, written before the inject (with the seen
/// row `delivering`): enough to read the agent's transcript after a
/// restart and tell whether the message reached it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WriteAhead {
    pub msg_id: String,
    pub agent_id: String,
    /// The agent's CLI (`claude`, …) and conversation id, and its cwd.
    pub provider: Option<String>,
    pub conversation: Option<String>,
    pub cwd: Option<String>,
    /// The transcript's length before the inject: only what follows counts.
    pub offset: u64,
    /// The text the agent's transcript shows once it has the message.
    pub needle: Option<String>,
    pub started_ms: i64,
}

pub fn write_ahead(conn: &Connection, w: &WriteAhead) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO delivery_writeahead
           (msg_id, agent_id, provider, conversation, cwd, offset_bytes, needle, started_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![w.msg_id, w.agent_id, w.provider, w.conversation, w.cwd, w.offset as i64, w.needle, w.started_ms],
    )
    .map(|_| ())
}

pub fn written_ahead(conn: &Connection, msg_id: &str) -> rusqlite::Result<Option<WriteAhead>> {
    conn.query_row(
        "SELECT msg_id, agent_id, provider, conversation, cwd, offset_bytes, needle, started_ms
         FROM delivery_writeahead WHERE msg_id = ?1",
        params![msg_id],
        |r| {
            Ok(WriteAhead {
                msg_id: r.get(0)?,
                agent_id: r.get(1)?,
                provider: r.get(2)?,
                conversation: r.get(3)?,
                cwd: r.get(4)?,
                offset: r.get::<_, i64>(5)? as u64,
                needle: r.get(6)?,
                started_ms: r.get(7)?,
            })
        },
    )
    .optional()
}

pub fn clear_ahead(conn: &Connection, msg_id: &str) -> rusqlite::Result<()> {
    conn.execute("DELETE FROM delivery_writeahead WHERE msg_id = ?1", params![msg_id]).map(|_| ())
}

/// Forget `msg_id` (a write-ahead `delivering` row whose delivery didn't
/// happen: the agent wasn't ready, nothing reached it).
pub fn forget(conn: &Connection, msg_id: &str) -> rusqlite::Result<()> {
    conn.execute("DELETE FROM delivery_seen WHERE msg_id = ?1", params![msg_id]).map(|_| ())
}

/// The ids handled since `since_ms` whose message may have reached the
/// agent (`delivered`; `delivering`: the daemon died mid-delivery;
/// `uncertain`: it did and the transcript couldn't tell),
/// newest first, at most `limit`: what a host reports when it links, so
/// the controller never fails a message its agent already has.
pub fn recent_delivered(conn: &Connection, since_ms: i64, limit: usize) -> rusqlite::Result<Vec<(String, String)>> {
    let mut st = conn.prepare(
        "SELECT msg_id, status FROM delivery_seen WHERE seen_ms >= ?1 AND status IN ('delivered', 'delivering', 'uncertain')
         ORDER BY seen_ms DESC, rowid DESC LIMIT ?2",
    )?;
    let rows = st.query_map(params![since_ms, limit as i64], |r| Ok((r.get(0)?, r.get(1)?)))?;
    rows.collect()
}

/// Remember `msg_id` as handled with `status` (a later call replaces the
/// status: the controller's `handling` becomes its final answer), keeping
/// at most `cap` ids.
pub fn record(conn: &Connection, msg_id: &str, status: &str, now_ms: i64, cap: usize) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO delivery_seen (msg_id, seen_ms, status) VALUES (?1, ?2, ?3)
         ON CONFLICT(msg_id) DO UPDATE SET status = excluded.status",
        params![msg_id, now_ms, status],
    )?;
    conn.execute(
        "DELETE FROM delivery_seen WHERE msg_id NOT IN
            (SELECT msg_id FROM delivery_seen ORDER BY seen_ms DESC, rowid DESC LIMIT ?1)",
        params![cap as i64],
    )?;
    Ok(())
}
