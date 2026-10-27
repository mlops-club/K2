//! Durable message delivery between agents on different hosts (PRD
//! "K2 Distributed Runtime", P2: FR4, scenarios S3 and S6, invariant I2).
//!
//! Modelled on the federation outbox (`federation::outbox`, `seen`), but
//! kept in the daemon's SQLite database instead of files, because in a
//! distributed K2 the controller's database owns messages (ADR-0002).
//!
//! - [`outbox`]: one row per message per hop. States
//!   `queued → sent → delivered | failed` (with a reason), attempts and
//!   backoff, a dead-letter rule (no ack after [`Policy::max_attempts`]
//!   sends, or older than [`Policy::ttl_ms`]), and order per
//!   (sender, recipient) pair.
//! - [`seen`]: the receiving side's memory of `msg_id`s it already
//!   handled, so at-least-once delivery dedupes by id across restarts.
//!
//! Every function takes the connection and the time (`now_ms`) from the
//! caller, so tests drive the state machine without a clock. The wire side
//! (`deliver` / `deliver.ack` frames) is `k2-daemon`'s `delivery_drain`;
//! the protocol and state machine are documented in
//! `docs/prd/progress/outbox.md`.

pub mod outbox;
pub mod seen;

use rusqlite::Connection;

pub use outbox::{Enqueued, Hop, NewMessage, Row, State};

/// Retry and dead-letter settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    /// Wait for an ack after the first send; doubles per attempt.
    pub backoff_base_ms: i64,
    /// Ceiling for that wait.
    pub backoff_max_ms: i64,
    /// Sends without an ack before the message fails (`no_ack`).
    pub max_attempts: u32,
    /// A message not delivered within this long fails (`expired`). The PRD
    /// keeps messages for an away host for 7 days.
    pub ttl_ms: i64,
    /// Finished rows (`delivered`, `failed`) are kept this long for
    /// inspection, then pruned.
    pub keep_finished_ms: i64,
    /// `msg_id`s the receiving side remembers.
    pub seen_cap: usize,
}

impl Policy {
    pub const PRODUCTION: Policy = Policy {
        backoff_base_ms: 2_000,
        backoff_max_ms: 60_000,
        max_attempts: 10,
        ttl_ms: 7 * 24 * 3600 * 1000,
        keep_finished_ms: 7 * 24 * 3600 * 1000,
        seen_cap: 20_000,
    };

    /// How long to wait for an ack after send number `attempt` (1-based).
    pub fn backoff_ms(&self, attempt: u32) -> i64 {
        let shift = attempt.saturating_sub(1).min(30);
        self.backoff_base_ms.saturating_mul(1i64 << shift).min(self.backoff_max_ms)
    }
}

/// Create the tables if they don't exist. Idempotent; every entry point
/// in the daemon calls it once per connection.
pub fn ensure_schema(conn: &Connection) -> rusqlite::Result<()> {
    ensure_tables(conn)?;
    // K2-f.3 review #4: the host (and its placement epoch) a `down` row was
    // last sent to and acked by. Rows from before have neither (NULL), and
    // keep the old behaviour: any cut of their recipient may fail them.
    for (col, ty) in [("host", "TEXT"), ("epoch", "INTEGER")] {
        let has: bool = conn.query_row(
            "SELECT COUNT(*) > 0 FROM pragma_table_info('delivery_outbox') WHERE name = ?1",
            [col],
            |r| r.get(0),
        )?;
        if !has {
            conn.execute_batch(&format!("ALTER TABLE delivery_outbox ADD COLUMN {col} {ty}"))?;
        }
    }
    Ok(())
}

fn ensure_tables(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS delivery_outbox (
            seq         INTEGER PRIMARY KEY AUTOINCREMENT,
            hop         TEXT NOT NULL,
            msg_id      TEXT NOT NULL,
            sender      TEXT NOT NULL,
            recipient   TEXT NOT NULL,
            body        TEXT NOT NULL,
            state       TEXT NOT NULL,
            attempts    INTEGER NOT NULL DEFAULT 0,
            next_at_ms  INTEGER NOT NULL DEFAULT 0,
            created_ms  INTEGER NOT NULL,
            updated_ms  INTEGER NOT NULL,
            reason      TEXT,
            UNIQUE (hop, msg_id)
        );
        CREATE INDEX IF NOT EXISTS delivery_outbox_pair
            ON delivery_outbox (hop, state, sender, recipient, seq);
        CREATE TABLE IF NOT EXISTS delivery_seen (
            msg_id   TEXT PRIMARY KEY,
            seen_ms  INTEGER NOT NULL,
            status   TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS delivery_writeahead (
            msg_id        TEXT PRIMARY KEY,
            agent_id      TEXT NOT NULL,
            provider      TEXT,
            conversation  TEXT,
            cwd           TEXT,
            offset_bytes  INTEGER NOT NULL DEFAULT 0,
            needle        TEXT,
            started_ms    INTEGER NOT NULL
        );",
    )
}

/// A new message id: `m_<13-digit ms>_<16 hex>`. Sorts by creation time;
/// the random half makes it unique across hosts.
pub fn new_msg_id(now_ms: i64) -> String {
    let r = uuid::Uuid::new_v4().simple().to_string();
    format!("m_{:013}_{}", now_ms.max(0), &r[..16])
}

/// Milliseconds since the Unix epoch, for callers outside tests.
pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests;
