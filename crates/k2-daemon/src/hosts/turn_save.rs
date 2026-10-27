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
//!
//! A turn-end save that fails (the controller away: lab-g2 S10, seed 4117)
//! is retried: with backoff while the host stays linked ([`retry_after`]),
//! and at once when the host links to the controller again
//! ([`relinked`]). A retry saves the agent's newest state, so the newest
//! snapshot wins; the controller numbers each snapshot it takes, so a
//! retry never repeats a seq. An agent no longer placed here is not
//! retried.

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
    /// Saves that failed in a row (0: the last one landed, or none ran).
    failures: u32,
}

/// The wait before retrying a turn-end save that failed `failures` times
/// in a row, while linked: 2 s, doubling, at most a minute.
pub fn retry_after(failures: u32) -> Duration {
    Duration::from_secs(2u64.saturating_pow(failures.clamp(1, 6)).min(60))
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

    /// `agent`'s save ended (saved, or skipped: nothing to retry).
    pub fn done(&mut self, agent: &str, now: Instant) {
        let window = self.window;
        let Some(st) = self.agents.get_mut(agent) else { return };
        st.running = false;
        st.failures = 0;
        if st.due.is_none() && st.last.is_none_or(|l| l + window <= now) {
            self.agents.remove(agent);
        }
    }

    /// `agent`'s save failed: it is retried after [`retry_after`] (or
    /// sooner, for a turn that ends meanwhile, or a relink).
    pub fn failed(&mut self, agent: &str, now: Instant) {
        let Some(st) = self.agents.get_mut(agent) else { return };
        st.running = false;
        st.failures += 1;
        let retry = now + retry_after(st.failures);
        st.due = Some(st.due.map_or(retry, |d| d.min(retry)));
    }

    /// The host linked to the controller again: every failed save is
    /// retried now.
    pub fn relinked(&mut self, now: Instant) {
        for st in self.agents.values_mut().filter(|s| s.failures > 0 && !s.running) {
            st.due = Some(now);
        }
    }

    /// Saves that failed and wait to be retried, by agent.
    pub fn failing(&self, agent: &str) -> u32 {
        self.agents.get(agent).map_or(0, |s| s.failures)
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
    /// A save ended: did it fail (and so is to be retried)?
    Done(String, bool),
    /// The host linked to the controller (again).
    Linked,
}

/// The running loop's inbox, once started.
static INBOX: OnceLock<mpsc::UnboundedSender<Msg>> = OnceLock::new();

/// The host linked to the controller (`delivery_drain::host_linked`):
/// failed turn-end saves are retried now.
pub fn relinked() {
    if let Some(tx) = INBOX.get() {
        let _ = tx.send(Msg::Linked);
    }
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
        let _ = INBOX.set(tx.clone());
        tokio::spawn(run(window, tx, rx, |agent: String| async move { save_turn(&agent).await }));
    });
}

/// The loop: `save` makes one agent's save and says whether it failed.
async fn run<F, Fut>(window: Duration, tx: mpsc::UnboundedSender<Msg>, mut rx: mpsc::UnboundedReceiver<Msg>, save: F)
where
    F: Fn(String) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = bool> + Send + 'static,
{
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
            Some(Msg::Done(agent, false)) => d.done(&agent, now),
            Some(Msg::Done(agent, true)) => {
                d.failed(&agent, now);
                log_debug!("[hosts/turn_save] {agent}: its turn-end save failed; retried on relink, or in {:?}", retry_after(d.failing(&agent)));
            }
            Some(Msg::Linked) => d.relinked(now),
            None => {}
        }
        for agent in d.take_due(now) {
            let (tx, save) = (tx.clone(), save.clone());
            tokio::spawn(async move {
                let failed = save(agent.clone()).await;
                let _ = tx.send(Msg::Done(agent, failed));
            });
        }
    }
}

/// One turn-end save of `agent`, while this host still runs it. `true`:
/// it failed, and is to be retried. Draining, or the agent no longer
/// placed here: nothing to save, nothing to retry.
async fn save_turn(agent: &str) -> bool {
    if super::drain::draining() {
        return false;
    }
    let Some(epoch) = super::link::placed_epoch(agent) else { return false };
    // Logged and traced by `save_as`.
    home::save_as(agent, epoch, home::Trigger::Turn).await.is_err()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// lab-g2 S10 (seed 4117): a turn-end save fails because the controller
    /// is away; when the host links again it is retried at once (no waiting
    /// out the backoff), lands as the next snapshot, with the newest state,
    /// and is not repeated.
    #[tokio::test(start_paused = true)]
    async fn a_failed_turn_end_save_is_retried_when_the_host_links_again() {
        // The controller's store: up or away; each snapshot it takes is
        // numbered (seq) and holds the agent's state at that save.
        #[derive(Default)]
        struct Store {
            up: bool,
            snapshots: Vec<(u64, String)>,
            attempts: u32,
        }
        let store = Arc::new(Mutex::new(Store::default()));
        let state = Arc::new(Mutex::new(String::from("turn one")));
        let save = {
            let (store, state) = (store.clone(), state.clone());
            move |_agent: String| {
                let (store, state) = (store.clone(), state.clone());
                async move {
                    let mut s = store.lock().unwrap();
                    s.attempts += 1;
                    if !s.up {
                        return true;
                    }
                    let seq = s.snapshots.len() as u64 + 1;
                    let now = state.lock().unwrap().clone();
                    s.snapshots.push((seq, now));
                    false
                }
            }
        };
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run(Duration::ZERO, tx.clone(), rx, save));
        let settle = || tokio::time::sleep(Duration::from_millis(10));
        // Turn one ends: saved.
        store.lock().unwrap().up = true;
        tx.send(Msg::TurnEnded("notes".into())).unwrap();
        settle().await;
        assert_eq!(store.lock().unwrap().snapshots, vec![(1, "turn one".to_string())]);
        // Turn two ends with the controller gone: the save fails.
        store.lock().unwrap().up = false;
        *state.lock().unwrap() = "turn two".into();
        tx.send(Msg::TurnEnded("notes".into())).unwrap();
        settle().await;
        assert_eq!(store.lock().unwrap().attempts, 2);
        assert_eq!(store.lock().unwrap().snapshots.len(), 1, "turn two isn't saved yet");
        // The controller comes back and the host links again, before any
        // backoff ends: the save is retried at once and lands.
        store.lock().unwrap().up = true;
        tx.send(Msg::Linked).unwrap();
        settle().await;
        let s = store.lock().unwrap();
        assert_eq!(s.snapshots, vec![(1, "turn one".to_string()), (2, "turn two".to_string())], "turn two lands, the newest state, seq 2");
        assert_eq!(s.attempts, 3, "one retry, not more");
        drop(s);
        // Nothing more is retried after it landed.
        tokio::time::sleep(Duration::from_secs(300)).await;
        assert_eq!(store.lock().unwrap().attempts, 3);
    }

    /// While the host stays linked, a failed save is retried with backoff
    /// (2 s, doubling, at most a minute) until it lands.
    #[tokio::test(start_paused = true)]
    async fn a_failed_save_is_retried_with_backoff_while_linked() {
        let attempts = Arc::new(Mutex::new(Vec::<Instant>::new()));
        let save = {
            let attempts = attempts.clone();
            move |_agent: String| {
                let attempts = attempts.clone();
                async move {
                    let mut a = attempts.lock().unwrap();
                    a.push(Instant::now());
                    a.len() < 4 // the first three fail, the fourth lands
                }
            }
        };
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run(Duration::ZERO, tx.clone(), rx, save));
        let t0 = Instant::now();
        tx.send(Msg::TurnEnded("notes".into())).unwrap();
        tokio::time::sleep(Duration::from_secs(120)).await;
        let a = attempts.lock().unwrap().clone();
        assert_eq!(a.len(), 4, "retried until it landed, then no more");
        let gaps: Vec<u64> = a.windows(2).map(|w| (w[1] - w[0]).as_secs()).collect();
        assert_eq!(gaps, vec![2, 4, 8], "backoff 2 s, 4 s, 8 s");
        assert!(a[0] - t0 < Duration::from_secs(1));
        assert_eq!((retry_after(1), retry_after(6), retry_after(20)), (Duration::from_secs(2), Duration::from_secs(60), Duration::from_secs(60)));
    }

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
