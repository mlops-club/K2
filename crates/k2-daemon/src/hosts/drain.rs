//! S8: a host drains on SIGTERM (PRD section 6, "Host pod stopped"), and
//! the controller resumes what it drained.
//!
//! Host (`K2_ROLE=host`), from the daemon's SIGTERM handler ([`on_sigterm`]):
//! 1. stop accepting spawns ([`refuse`], the link's hook: `503`) and
//!    deliveries (the outbox's receiving side leaves them unacked, so the
//!    controller keeps them queued);
//! 2. tell the controller: `refused {code: "draining"}` on the link;
//! 3. wait for each placed agent's turn to end: from its `UserPromptSubmit`
//!    hook to its `Stop` (or `StopFailure`, `SessionEnd`, the CLI exiting),
//!    followed from the host's hook stream since the host linked
//!    ([`track_turns`]), or its activity row being `Working`. Up to
//!    `K2_HOST_DRAIN_SECS` (default 600, the PRD's grace); an agent still in
//!    its turn then is released anyway and the log says so;
//! 4. per agent, `released {req: 0}` to the controller, then stop it;
//! 5. return: the daemon's usual shutdown follows and the process exits.
//!
//! Controller (`agent_host::remote::RemoteHost`): a draining host gets no
//! new placements; a `released` with `req: 0` from it drops the placement
//! and records the agent for resume (`host_resume`, kept across restarts).
//! [`ensure_controller_resumes`] runs the resume loop: once the agent's
//! host links again (a restart, or a replacement with the same name, the
//! same volume), the agent is spawned there again and resumes its
//! conversation. A need first (the app, a wake) places it the usual way,
//! which also ends its resume. Messages for an agent awaiting resume stay
//! in the outbox and are delivered once it is placed (I2).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use k2_core::log_debug;
use parking_lot::Mutex;
use serde_json::json;

use super::proto::Frame;
use crate::agent_host::remote::{RemoteHost, StoredPlacement};

/// Host: how long a drain waits for turns to end, in seconds.
pub const DRAIN_SECS_VAR: &str = "K2_HOST_DRAIN_SECS";
pub const DEFAULT_DRAIN: Duration = Duration::from_secs(600);
/// The `refused` code a draining host sends the controller.
pub const DRAINING_CODE: &str = "draining";
/// How often the drain looks at its agents' turns.
const POLL: Duration = Duration::from_millis(200);
/// Before the first look: hooks already on their way in (a prompt
/// submitted just before the SIGTERM; its `UserPromptSubmit` is still
/// running) land, so a turn that just started isn't taken for idle.
pub const SETTLE: Duration = Duration::from_secs(2);
/// How often the controller's resume loop looks for linked hosts.
const RESUME_TICK: Duration = Duration::from_secs(1);

pub mod events {
    pub const HOST_DRAIN_START: &str = "host.drain_start";
    pub const HOST_DRAIN_RELEASE: &str = "host.drain_release";
    pub const HOST_DRAIN_DONE: &str = "host.drain_done";
    pub const CTL_HOST_DRAINING: &str = "ctl.host_draining";
    pub const CTL_DRAIN_RELEASE: &str = "ctl.drain_release";
    pub const CTL_RESUME: &str = "ctl.resume";
}

static DRAINING: AtomicBool = AtomicBool::new(false);

/// True once this host started draining.
pub fn draining() -> bool {
    DRAINING.load(Ordering::SeqCst)
}

/// `K2_HOST_DRAIN_SECS` through `get`; a bad value is the default, logged.
pub fn drain_deadline(get: impl Fn(&str) -> Option<String>) -> Duration {
    match get(DRAIN_SECS_VAR).map(|v| v.trim().to_string()).filter(|v| !v.is_empty()) {
        None => DEFAULT_DRAIN,
        Some(v) => match v.parse::<u64>() {
            Ok(n) => Duration::from_secs(n),
            Err(_) => {
                log_debug!("[hosts/drain] {DRAIN_SECS_VAR}={v:?} is not a number of seconds; using {}", DEFAULT_DRAIN.as_secs());
                DEFAULT_DRAIN
            }
        },
    }
}

/// Panes (v2 session ids) whose turn is running, from their own hooks.
#[derive(Default)]
pub struct Turns(Mutex<HashMap<String, ()>>);

impl Turns {
    /// One hook from the pane's owner. A subagent's (`agent_id` set) never
    /// starts or ends the lead's turn.
    pub fn note(&self, pane: &str, event: &str, subagent: bool) {
        if subagent {
            return;
        }
        match event {
            "UserPromptSubmit" => {
                self.0.lock().insert(pane.to_string(), ());
            }
            "Stop" | "StopFailure" | "SessionEnd" => {
                self.0.lock().remove(pane);
            }
            _ => {}
        }
    }

    /// The pane's owner exited.
    pub fn gone(&self, pane: &str) {
        self.0.lock().remove(pane);
    }

    pub fn in_turn(&self, pane: &str) -> bool {
        self.0.lock().contains_key(pane)
    }
}

fn turns() -> &'static Turns {
    static T: OnceLock<Turns> = OnceLock::new();
    T.get_or_init(Turns::default)
}

/// Follow this host's hook stream into [`Turns`] (once per process). The
/// outbox calls it when the host links, before any agent of this link runs.
pub fn track_turns() {
    static STARTED: OnceLock<()> = OnceLock::new();
    STARTED.get_or_init(|| {
        let mut rx = crate::hook_ingest::subscribe();
        tokio::spawn(async move {
            use crate::hook_ingest::IngestEvent;
            use tokio::sync::broadcast::error::RecvError;
            loop {
                match rx.recv().await {
                    Ok(IngestEvent::Envelope { envelope, .. }) => {
                        turns().note(&envelope.pane, &envelope.event, envelope.agent_id.is_some())
                    }
                    Ok(IngestEvent::OwnerReleased { pane, .. }) => turns().gone(&pane),
                    Ok(_) => {}
                    Err(RecvError::Lagged(n)) => log_debug!("[hosts/drain] turn tracker lagged, {n} hook(s) missed"),
                    Err(RecvError::Closed) => break,
                }
            }
        });
    });
}

/// The link's hook: the answer to a spawn while `draining`, else `None`.
pub fn refuse_while(draining: bool, f: &Frame) -> Option<Frame> {
    if !draining {
        return None;
    }
    let req = match f {
        Frame::Spawn { req, .. } | Frame::SpawnWorkspace { req, .. } => *req,
        _ => return None,
    };
    Some(Frame::Reply {
        req,
        status: "503 Service Unavailable".into(),
        body: json!({"error": "agent host is draining (SIGTERM): it takes no new agents"}).to_string(),
    })
}

/// [`refuse_while`] for this process.
pub fn refuse(f: &Frame) -> Option<Frame> {
    refuse_while(draining(), f)
}

/// What a drain needs from the host (the daemon's own in production).
pub trait DrainHost {
    /// Mark the host draining (spawns and deliveries refused from now on).
    fn start(&self);
    /// Every placed agent and its session id.
    fn agents(&self) -> Vec<(String, String)>;
    /// Is this session's turn still running?
    fn in_turn(&self, session_id: &str) -> bool;
    /// Send a frame to the controller; false when there is no link.
    fn send(&self, f: Frame) -> bool;
    /// Stop the agent's session.
    fn stop(&self, agent_id: &str);
}

/// What a drain did, for the log and the tests.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Drained {
    /// Released after their turn ended (or idle).
    pub after_turn: Vec<String>,
    /// Released at the deadline, mid-turn.
    pub cut: Vec<String>,
}

/// Run a drain on `h`: let in-flight hooks land for `settle`, then wait at
/// most `deadline` for turns to end.
pub fn drain(h: &dyn DrainHost, settle: Duration, deadline: Duration) -> Drained {
    h.start();
    let linked = h.send(Frame::Refused {
        code: DRAINING_CODE.into(),
        message: "host is draining (SIGTERM): no new placements; its agents are released as their turns end".into(),
    });
    let mut left = h.agents();
    log_debug!("[hosts/drain] draining {} agent(s), up to {:?} (controller told: {linked})", left.len(), deadline);
    k2_core::trace::emit(|| k2_core::trace::Event::new(events::HOST_DRAIN_START).attr("agents", left.len()));
    let start = Instant::now();
    std::thread::sleep(settle.min(deadline));
    let mut out = Drained::default();
    let release = |agent: &str, session: &str, cut: bool| {
        // The controller records the resume before it can see the session go.
        h.send(Frame::Released { req: 0, agent_id: agent.to_string(), epoch: 0, ring_end: None });
        h.stop(agent);
        k2_core::trace::emit(|| {
            k2_core::trace::Event::new(events::HOST_DRAIN_RELEASE).agent(agent).pane(session).attr("cut", cut)
        });
    };
    loop {
        left.retain(|(agent, session)| {
            if h.in_turn(session) {
                return true;
            }
            release(agent, session, false);
            out.after_turn.push(agent.clone());
            false
        });
        if left.is_empty() || start.elapsed() >= deadline {
            break;
        }
        std::thread::sleep(POLL);
    }
    for (agent, session) in left {
        log_debug!("[hosts/drain] agent {agent} still in its turn after {:?}: released mid-turn", deadline);
        release(&agent, &session, true);
        out.cut.push(agent);
    }
    log_debug!("[hosts/drain] done: {} after their turn, {} cut", out.after_turn.len(), out.cut.len());
    k2_core::trace::emit(|| {
        k2_core::trace::Event::new(events::HOST_DRAIN_DONE).attr("after_turn", out.after_turn.len()).attr("cut", out.cut.len())
    });
    out
}

/// This daemon as a [`DrainHost`].
struct LiveHost;

impl DrainHost for LiveHost {
    fn start(&self) {
        DRAINING.store(true, Ordering::SeqCst);
    }

    fn agents(&self) -> Vec<(String, String)> {
        crate::agent_host::host().placements().into_iter().map(|(a, s)| (a, s.session_id().to_string())).collect()
    }

    fn in_turn(&self, session_id: &str) -> bool {
        turns().in_turn(session_id)
            || crate::activity_store::with_row(session_id, |r| r.lead.state == k2_core::activity::LeadState::Working)
                .unwrap_or(false)
    }

    fn send(&self, f: Frame) -> bool {
        crate::delivery_drain::host_link().is_some_and(|tx| tx.send(f).is_ok())
    }

    fn stop(&self, agent_id: &str) {
        crate::agent_host::host().unplace(agent_id);
    }
}

/// The daemon's SIGTERM hook: under `K2_ROLE=host`, drain before the
/// shutdown goes on. Nothing under any other role.
pub async fn on_sigterm() {
    if crate::runtime_role::current() != crate::runtime_role::Role::Host {
        return;
    }
    let deadline = drain_deadline(|k| std::env::var(k).ok());
    let _ = tokio::task::spawn_blocking(move || drain(&LiveHost, SETTLE, deadline)).await;
    // Let the link write the last `released` frames before the process goes.
    tokio::time::sleep(Duration::from_millis(500)).await;
}

// ── controller: the resume loop ────────────────────────────────────────

/// Spawns one drained agent again on its host. `Err` is retried next tick.
pub type Resumer = Arc<dyn Fn(&'static RemoteHost, &StoredPlacement) -> Result<(), String> + Send + Sync>;

fn started() -> &'static Mutex<Vec<usize>> {
    static S: OnceLock<Mutex<Vec<usize>>> = OnceLock::new();
    S.get_or_init(Mutex::default)
}

/// Start the resume loop for `remote` (once per `remote`).
pub fn ensure_controller_resumes_with(remote: &'static RemoteHost, resumer: Resumer) {
    let key = remote as *const RemoteHost as usize;
    {
        let mut s = started().lock();
        if s.contains(&key) {
            return;
        }
        s.push(key);
    }
    let inflight: Arc<Mutex<HashSet<String>>> = Arc::default();
    tokio::spawn(async move {
        loop {
            for r in remote.resume_due() {
                if !inflight.lock().insert(r.agent_id.clone()) {
                    continue;
                }
                let (resumer, inflight) = (Arc::clone(&resumer), Arc::clone(&inflight));
                tokio::task::spawn_blocking(move || {
                    let res = resumer(remote, &r);
                    match &res {
                        Ok(()) => log_debug!("[hosts/drain] resumed agent {} on host {}", r.agent_id, r.host_id),
                        Err(e) => log_debug!("[hosts/drain] resume of agent {} on host {}: {e} (retrying)", r.agent_id, r.host_id),
                    }
                    k2_core::trace::emit(|| {
                        k2_core::trace::Event::new(events::CTL_RESUME)
                            .agent(r.agent_id.as_str())
                            .attr("host", r.host_id.as_str())
                            .attr("ok", res.is_ok())
                    });
                    inflight.lock().remove(&r.agent_id);
                });
            }
            tokio::time::sleep(RESUME_TICK).await;
        }
    });
}

/// The production loop: the global `RemoteHost`, [`resume_production`].
pub fn ensure_controller_resumes() {
    ensure_controller_resumes_with(crate::agent_host::remote::global(), Arc::new(resume_production));
}

/// `--session-id <id>` becomes `--resume <id>`: the same conversation.
pub fn resume_args(args: &[String]) -> Vec<String> {
    args.iter().map(|a| if a == "--session-id" { "--resume".to_string() } else { a.clone() }).collect()
}

/// Spawn `r` again through `remote` as a v2 agent with its stored command,
/// resuming its conversation.
pub fn resume_v2(remote: &'static RemoteHost, r: &StoredPlacement) -> Result<(), String> {
    let body = json!({
        "agent_name": r.agent_id,
        "cwd": r.cwd,
        "command": r.program,
        "args": resume_args(&r.args),
    })
    .to_string();
    let (status, answer) = tokio::runtime::Handle::current().block_on(remote.spawn(body.as_bytes()));
    if status.starts_with('2') {
        Ok(())
    } else {
        Err(format!("{status}: {answer}"))
    }
}

/// A workspace's canonical chat (agent id = project id) comes back the
/// way the app opens it (`ensure_pinned_chat`: resume or fresh by the
/// workspace's saved session); any other agent through [`resume_v2`].
fn resume_production(remote: &'static RemoteHost, r: &StoredPlacement) -> Result<(), String> {
    let project_path = {
        let db = k2_core::db::shared();
        let conn = db.lock();
        k2_core::db::schema::Project::get(&conn, &r.agent_id).ok().map(|p| p.path)
    };
    match project_path {
        Some(path) => crate::pinned_chat::ensure_pinned_chat(&path, false, false).map(drop),
        None => resume_v2(remote, r),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spawns_are_refused_only_while_draining() {
        let spawn = Frame::Spawn { req: 7, agent_id: "a".into(), epoch: 1, body: json!({}) };
        assert_eq!(refuse_while(false, &spawn), None);
        match refuse_while(true, &spawn) {
            Some(Frame::Reply { req: 7, status, .. }) => assert!(status.starts_with("503"), "{status}"),
            other => panic!("expected a 503 reply, got {other:?}"),
        }
        let deliver = Frame::OpenUrl { url: "https://x".into(), source: "t".into() };
        assert_eq!(refuse_while(true, &deliver), None, "only spawns are refused");
    }

    #[test]
    fn a_turn_runs_from_the_prompt_to_its_stop_and_subagents_dont_count() {
        let t = Turns::default();
        assert!(!t.in_turn("p1"));
        t.note("p1", "SessionStart", false);
        assert!(!t.in_turn("p1"));
        t.note("p1", "UserPromptSubmit", false);
        assert!(t.in_turn("p1"));
        t.note("p1", "Stop", true);
        assert!(t.in_turn("p1"), "a subagent's Stop doesn't end the lead's turn");
        t.note("p1", "PreToolUse", false);
        assert!(t.in_turn("p1"));
        t.note("p1", "Stop", false);
        assert!(!t.in_turn("p1"));
        t.note("p2", "UserPromptSubmit", false);
        t.gone("p2");
        assert!(!t.in_turn("p2"), "the CLI exited");
        t.note("p3", "UserPromptSubmit", false);
        t.note("p3", "StopFailure", false);
        assert!(!t.in_turn("p3"));
    }

    #[test]
    fn the_drain_deadline_comes_from_the_env() {
        let get = |v: &'static str| move |k: &str| (k == DRAIN_SECS_VAR).then(|| v.to_string());
        assert_eq!(drain_deadline(get("90")), Duration::from_secs(90));
        assert_eq!(drain_deadline(get("")), DEFAULT_DRAIN);
        assert_eq!(drain_deadline(get("soon")), DEFAULT_DRAIN);
        assert_eq!(drain_deadline(|_| None), DEFAULT_DRAIN);
    }

    #[test]
    fn resume_args_turn_a_session_id_into_a_resume() {
        let a = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(resume_args(&a(&["--x", "--session-id", "abc"])), a(&["--x", "--resume", "abc"]));
        assert_eq!(resume_args(&a(&["--resume", "abc"])), a(&["--resume", "abc"]));
    }

    /// A host with agents whose turns end when the test says so.
    struct Fake {
        log: Mutex<Vec<String>>,
        in_turn: Mutex<HashSet<String>>,
    }

    impl DrainHost for Fake {
        fn start(&self) {
            self.log.lock().push("start".into());
        }
        fn agents(&self) -> Vec<(String, String)> {
            vec![("idle".into(), "s-idle".into()), ("busy".into(), "s-busy".into()), ("stuck".into(), "s-stuck".into())]
        }
        fn in_turn(&self, session_id: &str) -> bool {
            let mut t = self.in_turn.lock();
            // `busy`'s Stop hook comes on the third look.
            if session_id == "s-busy" {
                let n = self.log.lock().iter().filter(|l| *l == "look s-busy").count();
                self.log.lock().push("look s-busy".into());
                if n >= 2 {
                    t.remove("s-busy");
                }
            }
            t.contains(session_id)
        }
        fn send(&self, f: Frame) -> bool {
            self.log.lock().push(match f {
                Frame::Refused { code, .. } => format!("refused {code}"),
                Frame::Released { req, agent_id, .. } => format!("released {agent_id} req {req}"),
                other => format!("sent {}", other.kind()),
            });
            true
        }
        fn stop(&self, agent_id: &str) {
            self.log.lock().push(format!("stop {agent_id}"));
        }
    }

    #[test]
    fn a_drain_releases_each_agent_after_its_turn_and_cuts_one_still_running_at_the_deadline() {
        let h = Fake { log: Mutex::default(), in_turn: Mutex::new(["s-busy".to_string(), "s-stuck".to_string()].into()) };
        // The deadline only bounds `stuck`; `busy` ends well before it.
        let out = drain(&h, Duration::ZERO, Duration::from_secs(2));
        assert_eq!(out, Drained { after_turn: vec!["idle".into(), "busy".into()], cut: vec!["stuck".into()] });
        let log: Vec<String> = h.log.lock().iter().filter(|l| !l.starts_with("look")).cloned().collect();
        assert_eq!(
            log,
            [
                "start",
                "refused draining",
                "released idle req 0",
                "stop idle",
                "released busy req 0",
                "stop busy",
                "released stuck req 0",
                "stop stuck"
            ]
        );
    }
}
