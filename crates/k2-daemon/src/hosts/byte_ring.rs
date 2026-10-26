//! Where an agent host reads a session's output bytes for a `bytes.open`
//! stream (PRD "K2 Distributed Runtime", dist-link W3b).
//!
//! A client that kept its offset into a session's byte stream (the
//! `/cli/sessions/bytes?from=` protocol) asks the controller for the bytes
//! after it, and the controller relays the ask to the session's host. The
//! host answers from the session's byte ring, which lives with the PTY and
//! so survives a controller restart. v2 sessions keep no ring of their own
//! yet; the ring comes from a [`Source`] installed at boot (k2-ptyd's ring
//! for ptyd-backed sessions). With none installed, [`read`] says so and the
//! client falls back to the grid snapshot.

use parking_lot::RwLock;

/// What a read of a session's ring returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RingRead {
    /// Absolute offset of the first byte in `bytes`: the asked-for offset,
    /// or the ring's start when the ring already dropped that byte (a gap
    /// the client sees as `start > from`).
    pub start: u64,
    /// The ring's end when read: the offset the next output byte gets.
    pub end: u64,
    pub bytes: Vec<u8>,
}

/// Read at most `max` bytes of `session_id`'s output from offset `from`.
pub type Source = fn(session_id: &str, from: u64, max: usize) -> Result<RingRead, String>;

pub const NO_RING: &str =
    "this agent host keeps no byte ring for its sessions; reattach to /cli/sessions/grid for a snapshot";

static SOURCE: RwLock<Option<Source>> = RwLock::new(None);

/// Install (or with `None`, remove) the process-wide ring source.
pub fn set(source: Option<Source>) {
    *SOURCE.write() = source;
}

pub fn read(session_id: &str, from: u64, max: usize) -> Result<RingRead, String> {
    let source = *SOURCE.read();
    match source {
        Some(s) => s(session_id, from, max),
        None => Err(NO_RING.to_string()),
    }
}
