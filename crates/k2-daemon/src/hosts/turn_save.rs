//! P3 T2: an agent's home is saved at the end of each turn (D3's third
//! trigger; design and decisions in `docs/prd/progress/turn-snapshots.md`),
//! so a host that dies without a drain (`kill -9`, a lost node) loses at
//! most the turn in progress: the agent moves (`RemoteHost::move_lost`)
//! with the home its last turn left.
//!
//! Host (`K2_ROLE=host`), from the host's own hook stream: the lead's
//! `Stop` (or `StopFailure`) for a pane this host runs for the controller
//! asks for a save of that agent ([`home::save_as`], trigger `turn`).
//! Saves are coalesced per agent ([`Debounce`]): one runs at a time, and
//! turns that end meanwhile make one more save after it, of the newest
//! state. `K2_HOME_TURN_SECS` spaces an agent's saves further (default 0:
//! every turn end); `off` turns turn-end saves off. Snapshots are
//! incremental and the hash cache reads only changed files, so a turn's
//! save sends only what the turn changed. A draining host doesn't save on
//! turn ends: its drain saves each agent before it lets it go.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use k2_core::log_debug;
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::home;

/// Host: the least time between two turn-end saves of one agent, in
/// seconds; `off` = no turn-end saves.
pub const TURN_SECS_VAR: &str = "K2_HOME_TURN_SECS";

/// `K2_HOME_TURN_SECS` through `get`: `None` = off. A bad value is the
/// default (0), logged.
pub fn window(get: impl Fn(&str) -> Option<String>) -> Option<Duration> {
    match get(TURN_SECS_VAR).map(|v| v.trim().to_ascii_lowercase()).filter(|v| !v.is_empty()) {
        None => Some(Duration::ZERO),
        Some(v) if v == "off" => None,
        Some(v) => match v.parse::<u64>() {
            Ok(n) => Some(Duration::from_secs(n)),
            Err(_) => {
                log_debug!("[hosts/turn_save] {TURN_SECS_VAR}={v:?} is not a number of seconds or off; using 0");
                Some(Duration::ZERO)
            }
        },
    }
}

/// Does this hook end the lead's turn? A subagent's never does.
pub fn ends_turn(event: &str, subagent: bool) -> bool {
    !subagent && matches!(event, "Stop" | "StopFailure")
}

#[derive(Debug, Default)]
struct State {
    /// When its last save started.
    last: Option<Instant>,
    running: bool,
    /// A save is wanted at this time.
    due: Option<Instant>,
}

/// When each agent's turn-end save runs: at once, unless one is running
/// (then right after it) or the last started less than `window` ago (then
/// at `window` after it). Turn ends meanwhile make no extra save: the one
/// waiting takes the newest state.
#[derive(Debug)]
pub struct Debounce {
    window: Duration,
    agents: HashMap<String, State>,
}

impl Debounce {
    pub fn new(window: Duration) -> Debounce {
        Debounce { window, agents: HashMap::new() }
    }

    pub fn turn_ended(&mut self, agent: &str, now: Instant) {
        let window = self.window;
        let st = self.agents.entry(agent.to_string()).or_default();
        if st.due.is_none() {
            st.due = Some(match st.last {
                Some(l) if l + window > now => l + window,
                _ => now,
            });
        }
    }

    /// The saves to start now (marked running), by agent name.
    pub fn take_due(&mut self, now: Instant) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for (agent, st) in self.agents.iter_mut() {
            if !st.running && st.due.is_some_and(|d| d <= now) {
                st.running = true;
                st.due = None;
                st.last = Some(now);
                out.push(agent.clone());
            }
        }
        out.sort();
        out
    }

    /// `agent`'s save ended (saved, skipped or failed).
    pub fn done(&mut self, agent: &str, now: Instant) {
        let window = self.window;
        let Some(st) = self.agents.get_mut(agent) else { return };
        st.running = false;
        if st.due.is_none() && st.last.is_none_or(|l| l + window <= now) {
            self.agents.remove(agent);
        }
    }

    /// The earliest save waiting to start.
    pub fn next_due(&self) -> Option<Instant> {
        self.agents.values().filter(|s| !s.running).filter_map(|s| s.due).min()
    }

    pub fn running(&self, agent: &str) -> bool {
        self.agents.get(agent).is_some_and(|s| s.running)
    }
}

enum Msg {
    TurnEnded(String),
    Done(String),
}

/// The agent this host runs for the controller in v2 session `pane`.
fn agent_of(pane: &str) -> Option<String> {
    crate::agent_host::host()
        .placements()
        .into_iter()
        .find(|(_, s)| s.session_id().to_string() == pane)
        .map(|(agent, _)| agent)
        .filter(|agent| super::link::placed_epoch(agent).is_some())
}

/// Start turn-end saves on this host (once per process; the outbox calls
/// it when the host links). Nothing when home sync is off here.
pub fn start() {
    static STARTED: OnceLock<()> = OnceLock::new();
    STARTED.get_or_init(|| {
        if home::work_root(|k| std::env::var(k).ok()).is_none() {
            return;
        }
        let Some(window) = window(|k| std::env::var(k).ok()) else {
            log_debug!("[hosts/turn_save] {TURN_SECS_VAR}=off: homes are saved only when agents are released");
            return;
        };
        log_debug!("[hosts/turn_save] homes are saved at each turn's end (at most one per {window:?} per agent)");
        let (tx, rx) = mpsc::unbounded_channel();
        let mut hooks = crate::hook_ingest::subscribe();
        let hook_tx = tx.clone();
        tokio::spawn(async move {
            use crate::hook_ingest::IngestEvent;
            use tokio::sync::broadcast::error::RecvError;
            loop {
                match hooks.recv().await {
                    Ok(IngestEvent::Envelope { envelope, .. }) if ends_turn(&envelope.event, envelope.agent_id.is_some()) => {
                        if let Some(agent) = agent_of(&envelope.pane) {
                            let _ = hook_tx.send(Msg::TurnEnded(agent));
                        }
                    }
                    Ok(_) => {}
                    Err(RecvError::Lagged(n)) => log_debug!("[hosts/turn_save] lagged, {n} hook(s) missed"),
                    Err(RecvError::Closed) => break,
                }
            }
        });
        tokio::spawn(run(window, tx, rx));
    });
}

async fn run(window: Duration, tx: mpsc::UnboundedSender<Msg>, mut rx: mpsc::UnboundedReceiver<Msg>) {
    let mut d = Debounce::new(window);
    loop {
        let next = d.next_due();
        let msg = match next {
            Some(at) => tokio::select! {
                m = rx.recv() => m,
                _ = tokio::time::sleep_until(at) => None,
            },
            None => match rx.recv().await {
                Some(m) => Some(m),
                None => return,
            },
        };
        let now = Instant::now();
        match msg {
            Some(Msg::TurnEnded(agent)) => d.turn_ended(&agent, now),
            Some(Msg::Done(agent)) => d.done(&agent, now),
            None => {}
        }
        for agent in d.take_due(now) {
            let tx = tx.clone();
            tokio::spawn(async move {
                save_turn(&agent).await;
                let _ = tx.send(Msg::Done(agent));
            });
        }
    }
}

/// One turn-end save of `agent`, while this host still runs it.
async fn save_turn(agent: &str) {
    if super::drain::draining() {
        return;
    }
    let Some(epoch) = super::link::placed_epoch(agent) else { return };
    // Logged and traced by `save_as`.
    let _ = home::save_as(agent, epoch, home::Trigger::Turn).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(t0: Instant, secs: u64) -> Instant {
        t0 + Duration::from_secs(secs)
    }

    #[test]
    fn every_turn_end_saves_and_turns_that_end_during_a_save_make_one_more() {
        let t0 = Instant::now();
        let mut d = Debounce::new(Duration::ZERO);
        assert_eq!(d.next_due(), None);
        d.turn_ended("notes", t0);
        assert_eq!(d.take_due(t0), vec!["notes"], "the first turn end saves at once");
        assert!(d.running("notes"));
        for s in 1..4 {
            d.turn_ended("notes", at(t0, s));
        }
        assert_eq!(d.take_due(at(t0, 4)), Vec::<String>::new(), "never two saves of one agent at once");
        assert_eq!(d.next_due(), None, "the waiting save is held while one runs");
        d.done("notes", at(t0, 5));
        assert_eq!(d.next_due(), Some(at(t0, 1)));
        assert_eq!(d.take_due(at(t0, 5)), vec!["notes"], "three turn ends during the save: one more save, of the newest state");
        d.done("notes", at(t0, 6));
        assert_eq!(d.take_due(at(t0, 7)), Vec::<String>::new());
        assert!(d.agents.is_empty(), "nothing is kept for an agent with no save waiting");
    }

    #[test]
    fn a_window_spaces_one_agents_saves_but_never_drops_the_last_turn() {
        let t0 = Instant::now();
        let mut d = Debounce::new(Duration::from_secs(60));
        d.turn_ended("notes", t0);
        assert_eq!(d.take_due(t0), vec!["notes"]);
        d.done("notes", at(t0, 2));
        d.turn_ended("notes", at(t0, 10));
        assert_eq!(d.take_due(at(t0, 10)), Vec::<String>::new(), "within the window: later");
        assert_eq!(d.next_due(), Some(at(t0, 60)), "at the window's end after the last save started");
        d.turn_ended("notes", at(t0, 30));
        assert_eq!(d.take_due(at(t0, 59)), Vec::<String>::new());
        assert_eq!(d.take_due(at(t0, 60)), vec!["notes"], "one save for both turns, with the newest state");
        d.done("notes", at(t0, 61));
        d.turn_ended("notes", at(t0, 200));
        assert_eq!(d.take_due(at(t0, 200)), vec!["notes"], "after a quiet window: at once again");
    }

    #[test]
    fn agents_are_saved_independently() {
        let t0 = Instant::now();
        let mut d = Debounce::new(Duration::ZERO);
        d.turn_ended("a", t0);
        d.turn_ended("b", t0);
        assert_eq!(d.take_due(t0), vec!["a", "b"]);
        d.turn_ended("a", at(t0, 1));
        d.done("b", at(t0, 1));
        assert_eq!(d.take_due(at(t0, 1)), Vec::<String>::new(), "a is still saving");
        d.done("a", at(t0, 2));
        assert_eq!(d.take_due(at(t0, 2)), vec!["a"]);
    }

    #[test]
    fn only_the_leads_stop_ends_a_turn() {
        assert!(ends_turn("Stop", false));
        assert!(ends_turn("StopFailure", false));
        assert!(!ends_turn("Stop", true), "a subagent's Stop");
        assert!(!ends_turn("UserPromptSubmit", false));
        assert!(!ends_turn("SessionEnd", false), "the session ending is a release: the close or drain saves it");
    }

    #[test]
    fn the_window_comes_from_the_env() {
        let get = |v: &'static str| move |k: &str| (k == TURN_SECS_VAR).then(|| v.to_string());
        assert_eq!(window(|_| None), Some(Duration::ZERO), "default: every turn end");
        assert_eq!(window(get("60")), Some(Duration::from_secs(60)));
        assert_eq!(window(get("off")), None);
        assert_eq!(window(get(" OFF ")), None);
        assert_eq!(window(get("soon")), Some(Duration::ZERO), "a bad value is the default");
    }
}
