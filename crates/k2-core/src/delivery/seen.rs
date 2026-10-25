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

/// Remember `msg_id` as handled with `status`, keeping at most `cap` ids.
pub fn record(conn: &Connection, msg_id: &str, status: &str, now_ms: i64, cap: usize) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO delivery_seen (msg_id, seen_ms, status) VALUES (?1, ?2, ?3)",
        params![msg_id, now_ms, status],
    )?;
    conn.execute(
        "DELETE FROM delivery_seen WHERE msg_id NOT IN
            (SELECT msg_id FROM delivery_seen ORDER BY seen_ms DESC, rowid DESC LIMIT ?1)",
        params![cap as i64],
    )?;
    Ok(())
}
