//! The outbox table: one row per message per hop.
//!
//! ```text
//! queued ──send──▶ sent ──ack delivered|duplicate──▶ delivered
//!   │               │ ╲──ack rejected──────────────▶ failed (reason)
//!   │               │ ╲──no ack after max_attempts─▶ failed (no_ack)
//!   └──── older than the TTL ──────────────────────▶ failed (expired)
//! ```
//!
//! A `sent` row whose ack wait (`next_at_ms`) ran out is due again: the
//! drain resends it with the same `msg_id` and `attempts + 1`. Only the
//! oldest unfinished row of each (sender, recipient) pair is ever due, so
//! a pair's messages arrive in the order they were sent.

use rusqlite::{params, Connection, OptionalExtension};

use super::Policy;

/// Which hop a row is for. A host only holds `Up` rows (its agents'
/// messages, on their way to the controller); the controller only holds
/// `Down` rows (on their way to the recipient's host).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Hop {
    Up,
    Down,
}

impl Hop {
    pub fn as_str(self) -> &'static str {
        match self {
            Hop::Up => "up",
            Hop::Down => "down",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Queued,
    Sent,
    Delivered,
    Failed,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            State::Queued => "queued",
            State::Sent => "sent",
            State::Delivered => "delivered",
            State::Failed => "failed",
        }
    }

    fn parse(s: &str) -> State {
        match s {
            "queued" => State::Queued,
            "sent" => State::Sent,
            "delivered" => State::Delivered,
            // Only this module writes the column; anything else is a
            // corrupted row, which must never be resent.
            _ => State::Failed,
        }
    }

    pub fn is_final(self) -> bool {
        matches!(self, State::Delivered | State::Failed)
    }
}

/// A message to enqueue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewMessage {
    pub msg_id: String,
    /// `agent:<name>` or `user:<name>`.
    pub sender: String,
    /// The recipient's agent id.
    pub recipient: String,
    /// The `deliver` frame's `body`, as JSON text.
    pub body: String,
}

/// One outbox row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub seq: i64,
    pub hop: Hop,
    pub msg_id: String,
    pub sender: String,
    pub recipient: String,
    pub body: String,
    pub state: State,
    pub attempts: u32,
    pub next_at_ms: i64,
    pub created_ms: i64,
    pub updated_ms: i64,
    pub reason: Option<String>,
}

impl Row {
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "seq": self.seq,
            "hop": self.hop.as_str(),
            "msg_id": self.msg_id,
            "sender": self.sender,
            "recipient": self.recipient,
            "state": self.state.as_str(),
            "attempts": self.attempts,
            "created_ms": self.created_ms,
            "updated_ms": self.updated_ms,
            "reason": self.reason,
        })
    }
}

const COLUMNS: &str =
    "seq, hop, msg_id, sender, recipient, body, state, attempts, next_at_ms, created_ms, updated_ms, reason";

fn row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Row> {
    let hop: String = r.get(1)?;
    let state: String = r.get(6)?;
    Ok(Row {
        seq: r.get(0)?,
        hop: if hop == "up" { Hop::Up } else { Hop::Down },
        msg_id: r.get(2)?,
        sender: r.get(3)?,
        recipient: r.get(4)?,
        body: r.get(5)?,
        state: State::parse(&state),
        attempts: r.get::<_, i64>(7)?.max(0) as u32,
        next_at_ms: r.get(8)?,
        created_ms: r.get(9)?,
        updated_ms: r.get(10)?,
        reason: r.get(11)?,
    })
}

/// What [`enqueue`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Enqueued {
    /// A new row, `queued`.
    New,
    /// This hop already has `msg_id` (a resend from the previous hop);
    /// nothing changed. Carries the existing row's state.
    Exists(State),
}

/// Add `m` to `hop`'s queue. Idempotent on `msg_id`: a second enqueue of
/// the same id changes nothing, which is how the controller dedupes a
/// host's resend.
pub fn enqueue(conn: &Connection, hop: Hop, m: &NewMessage, now_ms: i64) -> rusqlite::Result<Enqueued> {
    if m.msg_id.is_empty() || m.recipient.is_empty() {
        return Err(rusqlite::Error::InvalidParameterName("msg_id and recipient are required".into()));
    }
    let n = conn.execute(
        "INSERT OR IGNORE INTO delivery_outbox
            (hop, msg_id, sender, recipient, body, state, attempts, next_at_ms, created_ms, updated_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, 'queued', 0, 0, ?6, ?6)",
        params![hop.as_str(), m.msg_id, m.sender, m.recipient, m.body, now_ms],
    )?;
    if n == 1 {
        return Ok(Enqueued::New);
    }
    let existing = get(conn, hop, &m.msg_id)?.map(|r| r.state).unwrap_or(State::Failed);
    Ok(Enqueued::Exists(existing))
}

pub fn get(conn: &Connection, hop: Hop, msg_id: &str) -> rusqlite::Result<Option<Row>> {
    conn.query_row(
        &format!("SELECT {COLUMNS} FROM delivery_outbox WHERE hop = ?1 AND msg_id = ?2"),
        params![hop.as_str(), msg_id],
        row,
    )
    .optional()
}

/// The rows to send now: for each (sender, recipient) pair of `hop`, its
/// oldest unfinished row, if it is `queued` or its ack wait ran out.
/// Oldest first.
pub fn due(conn: &Connection, hop: Hop, now_ms: i64) -> rusqlite::Result<Vec<Row>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {COLUMNS} FROM delivery_outbox o
         WHERE o.hop = ?1 AND o.state IN ('queued', 'sent') AND o.next_at_ms <= ?2
           AND o.seq = (SELECT MIN(i.seq) FROM delivery_outbox i
                        WHERE i.hop = o.hop AND i.sender = o.sender AND i.recipient = o.recipient
                          AND i.state IN ('queued', 'sent'))
         ORDER BY o.seq"
    ))?;
    let rows = stmt.query_map(params![hop.as_str(), now_ms], row)?;
    rows.collect()
}

/// The ack wait of the next due row, if any row is unfinished: when the
/// drain should look again without a nudge.
pub fn next_wakeup_ms(conn: &Connection, hop: Hop) -> rusqlite::Result<Option<i64>> {
    conn.query_row(
        "SELECT MIN(next_at_ms) FROM delivery_outbox WHERE hop = ?1 AND state IN ('queued', 'sent')",
        params![hop.as_str()],
        |r| r.get(0),
    )
}

/// A `deliver` for `msg_id` went out: `attempts + 1`, `sent`, and the ack
/// wait for that attempt. Returns the attempt number. A finished row is
/// left alone (returns its attempts).
pub fn mark_sent(conn: &Connection, hop: Hop, msg_id: &str, now_ms: i64, policy: &Policy) -> rusqlite::Result<u32> {
    let Some(r) = get(conn, hop, msg_id)? else {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    };
    if r.state.is_final() {
        return Ok(r.attempts);
    }
    let attempt = r.attempts + 1;
    conn.execute(
        "UPDATE delivery_outbox SET state = 'sent', attempts = ?3, next_at_ms = ?4, updated_ms = ?5
         WHERE hop = ?1 AND msg_id = ?2",
        params![hop.as_str(), msg_id, attempt as i64, now_ms + policy.backoff_ms(attempt), now_ms],
    )?;
    Ok(attempt)
}

/// The receiver acked `delivered` or `duplicate`. Returns false if there
/// was no unfinished row (an ack for a finished or unknown message).
pub fn mark_delivered(conn: &Connection, hop: Hop, msg_id: &str, now_ms: i64) -> rusqlite::Result<bool> {
    let n = conn.execute(
        "UPDATE delivery_outbox SET state = 'delivered', reason = NULL, updated_ms = ?3
         WHERE hop = ?1 AND msg_id = ?2 AND state IN ('queued', 'sent')",
        params![hop.as_str(), msg_id, now_ms],
    )?;
    Ok(n == 1)
}

/// Dead letter: the message ends `failed` with `reason`. Returns false if
/// there was no unfinished row.
pub fn mark_failed(conn: &Connection, hop: Hop, msg_id: &str, reason: &str, now_ms: i64) -> rusqlite::Result<bool> {
    let n = conn.execute(
        "UPDATE delivery_outbox SET state = 'failed', reason = ?3, updated_ms = ?4
         WHERE hop = ?1 AND msg_id = ?2 AND state IN ('queued', 'sent')",
        params![hop.as_str(), msg_id, reason, now_ms],
    )?;
    Ok(n == 1)
}

/// The dead-letter rule, run by the drain before each pass: a `sent` row
/// whose last ack wait ran out after `max_attempts` sends fails `no_ack`;
/// any unfinished row older than the TTL fails `expired`; finished rows
/// older than `keep_finished_ms` are pruned. Returns the rows it failed,
/// with their reasons set, so the caller can report each one loudly.
pub fn sweep(conn: &Connection, hop: Hop, now_ms: i64, policy: &Policy) -> rusqlite::Result<Vec<Row>> {
    let mut failed = Vec::new();
    let mut stmt = conn.prepare(&format!(
        "SELECT {COLUMNS} FROM delivery_outbox WHERE hop = ?1 AND state IN ('queued', 'sent') ORDER BY seq"
    ))?;
    let open: Vec<Row> = stmt.query_map(params![hop.as_str()], row)?.collect::<rusqlite::Result<_>>()?;
    for r in open {
        let reason = if now_ms - r.created_ms >= policy.ttl_ms {
            Some(format!("expired: not delivered within {} s ({} attempts)", policy.ttl_ms / 1000, r.attempts))
        } else if r.state == State::Sent && r.attempts >= policy.max_attempts && r.next_at_ms <= now_ms {
            Some(format!("no_ack: {} attempts without an ack", r.attempts))
        } else {
            None
        };
        if let Some(reason) = reason {
            if mark_failed(conn, hop, &r.msg_id, &reason, now_ms)? {
                if let Some(done) = get(conn, hop, &r.msg_id)? {
                    failed.push(done);
                }
            }
        }
    }
    conn.execute(
        "DELETE FROM delivery_outbox WHERE hop = ?1 AND state IN ('delivered', 'failed') AND updated_ms < ?2",
        params![hop.as_str(), now_ms - policy.keep_finished_ms],
    )?;
    Ok(failed)
}

/// Newest first, at most `limit` rows of `hop`.
pub fn list(conn: &Connection, hop: Hop, limit: usize) -> rusqlite::Result<Vec<Row>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {COLUMNS} FROM delivery_outbox WHERE hop = ?1 ORDER BY seq DESC LIMIT ?2"
    ))?;
    let rows = stmt.query_map(params![hop.as_str(), limit as i64], row)?;
    rows.collect()
}

/// Rows per state for `hop`: (queued, sent, delivered, failed).
pub fn counts(conn: &Connection, hop: Hop) -> rusqlite::Result<(u64, u64, u64, u64)> {
    let mut stmt = conn.prepare("SELECT state, COUNT(*) FROM delivery_outbox WHERE hop = ?1 GROUP BY state")?;
    let mut c = (0, 0, 0, 0);
    for r in stmt.query_map(params![hop.as_str()], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))? {
        let (s, n) = r?;
        let n = n.max(0) as u64;
        match State::parse(&s) {
            State::Queued => c.0 = n,
            State::Sent => c.1 = n,
            State::Delivered => c.2 = n,
            State::Failed => c.3 += n,
        }
    }
    Ok(c)
}
