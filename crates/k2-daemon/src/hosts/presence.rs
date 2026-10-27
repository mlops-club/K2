//! P3 T3: host presence on the controller (a laptop that comes and goes).
//!
//! A host whose link drops is away at once. A host that stops talking
//! with its link still open (a laptop's lid closed, `SIGSTOP`, a dead NAT
//! mapping) looked linked for ever: frames went into its socket and were
//! counted as sent until the outbox gave up (`no_ack`). Now every message
//! from the host (its pongs to the hub's pings included) is a sign of
//! life, and a link silent for `K2_HOST_AWAY_AFTER` (90 s) is closed by
//! the hub: the host is away from the moment it was last heard.
//!
//! Away is today's "link down" in every rule: its agents are paused (kept,
//! W3), a pinned agent waits for it, the outbox holds its messages
//! (`held`), and a portable agent moves after `K2_HOST_MOVE_AFTER`,
//! counted from when it was last heard. When the host wakes, its own link
//! finds the socket gone and links again (hello with resume, W3).

use std::time::{Duration, Instant};

use k2_core::log_debug;

/// Controller: seconds of silence after which a host is away.
pub const AWAY_AFTER_VAR: &str = "K2_HOST_AWAY_AFTER";
pub const DEFAULT_AWAY_AFTER: Duration = Duration::from_secs(90);

pub mod events {
    /// A host went away: `why` is `silent` (the read deadline) or `unlinked`.
    pub const CTL_HOST_AWAY: &str = "ctl.host_away";
}

/// `K2_HOST_AWAY_AFTER` through `get`, in seconds; a bad or zero value is
/// the default, logged.
pub fn away_after(get: impl Fn(&str) -> Option<String>) -> Duration {
    match get(AWAY_AFTER_VAR).map(|v| v.trim().to_string()).filter(|v| !v.is_empty()) {
        None => DEFAULT_AWAY_AFTER,
        Some(v) => match v.parse::<u64>() {
            Ok(n) if n > 0 => Duration::from_secs(n),
            _ => {
                log_debug!("[hosts] {AWAY_AFTER_VAR}={v:?} is not a positive number of seconds; using {}", DEFAULT_AWAY_AFTER.as_secs());
                DEFAULT_AWAY_AFTER
            }
        },
    }
}

/// How often the hub pings a link whose deadline is `away_after`: every
/// 15 s, or more often for a short deadline, so a live host answers at
/// least twice in each window.
pub fn ping_every(away_after: Duration) -> Duration {
    super::hub::PING_EVERY.min(away_after / 3).max(Duration::from_millis(10))
}

/// When one link's host was last heard from.
#[derive(Debug, Clone, Copy)]
pub struct Presence {
    heard: Instant,
    away_after: Duration,
}

impl Presence {
    pub fn new(now: Instant, away_after: Duration) -> Self {
        Self { heard: now, away_after }
    }

    /// Any message from the host.
    pub fn heard(&mut self, now: Instant) {
        self.heard = self.heard.max(now);
    }

    pub fn last_heard(&self) -> Instant {
        self.heard
    }

    /// When the host is away if nothing comes first.
    pub fn deadline(&self) -> Instant {
        self.heard + self.away_after
    }

    /// `Some(silence)` once the host has been silent for `away_after`.
    pub fn away(&self, now: Instant) -> Option<Duration> {
        let silent = now.saturating_duration_since(self.heard);
        (silent >= self.away_after).then_some(silent)
    }
}

/// Why a host is away, for status, `held` and the trace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Why {
    /// Its link was open but nothing came for this long.
    Silent(Duration),
    /// Its link closed (the host stopped, crashed, or its network went).
    Unlinked,
}

impl Why {
    pub fn kind(&self) -> &'static str {
        match self {
            Why::Silent(_) => "silent",
            Why::Unlinked => "unlinked",
        }
    }
}

impl std::fmt::Display for Why {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Why::Silent(d) => write!(f, "silent for {} s", d.as_secs()),
            Why::Unlinked => f.write_str("its link closed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_host_is_away_only_after_the_whole_deadline_of_silence() {
        let t0 = Instant::now();
        let after = Duration::from_secs(90);
        let mut p = Presence::new(t0, after);
        assert_eq!(p.deadline(), t0 + after);
        assert_eq!(p.away(t0 + Duration::from_secs(89)), None);
        assert_eq!(p.away(t0 + after), Some(after));
        // A pong at 60 s moves the deadline: 89 s later it is still here.
        p.heard(t0 + Duration::from_secs(60));
        assert_eq!(p.away(t0 + Duration::from_secs(149)), None);
        assert_eq!(p.away(t0 + Duration::from_secs(151)), Some(Duration::from_secs(91)));
        assert_eq!(p.last_heard(), t0 + Duration::from_secs(60));
        // A late, out-of-order timestamp never moves it back.
        p.heard(t0 + Duration::from_secs(10));
        assert_eq!(p.last_heard(), t0 + Duration::from_secs(60));
    }

    #[test]
    fn k2_host_away_after_is_seconds_and_a_bad_value_is_the_default() {
        let get = |v: &'static str| move |k: &str| (k == AWAY_AFTER_VAR).then(|| v.to_string());
        assert_eq!(away_after(|_| None), DEFAULT_AWAY_AFTER);
        assert_eq!(away_after(get("3")), Duration::from_secs(3));
        assert_eq!(away_after(get(" 120 ")), Duration::from_secs(120));
        assert_eq!(away_after(get("0")), DEFAULT_AWAY_AFTER);
        assert_eq!(away_after(get("soon")), DEFAULT_AWAY_AFTER);
        assert_eq!(away_after(get("")), DEFAULT_AWAY_AFTER);
    }

    #[test]
    fn pings_come_at_least_three_times_per_deadline() {
        assert_eq!(ping_every(DEFAULT_AWAY_AFTER), super::super::hub::PING_EVERY);
        assert_eq!(ping_every(Duration::from_secs(3)), Duration::from_secs(1));
    }

    #[test]
    fn why_reads_as_a_reason() {
        assert_eq!(Why::Silent(Duration::from_secs(92)).to_string(), "silent for 92 s");
        assert_eq!((Why::Silent(Duration::ZERO).kind(), Why::Unlinked.kind()), ("silent", "unlinked"));
        assert_eq!(Why::Unlinked.to_string(), "its link closed");
    }
}
