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
//!    counted from the host's hook ingest ([`note_ingest`]), or its
//!    activity row being `Working`. Up to
//!    `K2_HOST_DRAIN_SECS` (default 600, the PRD's grace); an agent still in
//!    its turn then is released anyway and the log says so;
//! 4. per agent, save its home (P3 T1, `hosts::home`: under `K2_WORK_ROOT`
//!    only), then `released {req: 0}` to the controller, then stop it;
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
//!
//! P3 T2 (S10), a host that dies without a drain (`kill -9`, a lost node):
//! the same loop moves each portable agent of a host lost for
//! `K2_HOST_MOVE_AFTER` (default 300 s; `RemoteHost::move_lost`). It then
//! resumes on the next host that can take it, from the home its last turn
//! saved (`hosts::turn_save`). A pinned agent waits for its host.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use k2_core::log_debug;
use parking_lot::Mutex;
use serde_json::json;

use super::proto::{Frame, TurnCounts};
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
/// Controller: how long a host may be lost before its portable agents move.
pub const MOVE_AFTER_VAR: &str = "K2_HOST_MOVE_AFTER";
pub const DEFAULT_MOVE_AFTER: Duration = Duration::from_secs(300);
/// A resume row's host for a moved agent: any host that can take it.
pub const ANY_HOST: &str = "*";

pub mod events {
    pub const HOST_DRAIN_START: &str = "host.drain_start";
    pub const HOST_DRAIN_RELEASE: &str = "host.drain_release";
    pub const HOST_DRAIN_DONE: &str = "host.drain_done";
    pub const CTL_HOST_DRAINING: &str = "ctl.host_draining";
    pub const CTL_DRAIN_RELEASE: &str = "ctl.drain_release";
    pub const CTL_RESUME: &str = "ctl.resume";
    /// A lost host's portable agent moves (T2, S10).
    pub const CTL_MOVE: &str = "ctl.move_decision";
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

/// `K2_HOST_MOVE_AFTER` through `get`, in seconds; a bad value is the
/// default, logged.
pub fn move_after(get: impl Fn(&str) -> Option<String>) -> Duration {
    match get(MOVE_AFTER_VAR).map(|v| v.trim().to_string()).filter(|v| !v.is_empty()) {
        None => DEFAULT_MOVE_AFTER,
        Some(v) => match v.parse::<u64>() {
            Ok(n) => Duration::from_secs(n),
            Err(_) => {
                log_debug!("[hosts/drain] {MOVE_AFTER_VAR}={v:?} is not a number of seconds; using {}", DEFAULT_MOVE_AFTER.as_secs());
                DEFAULT_MOVE_AFTER
            }
        },
    }
}

/// Each pane's (v2 session id's) turns, from its own hooks: whether one
/// is running, and (K2-f) how many prompts and turn ends it had. Kept
/// after its CLI exits until the controller has been told
/// ([`Turns::take`]), so a link outage loses nothing.
pub struct Turns {
    panes: Mutex<HashMap<String, PaneTurns>>,
    /// Panes kept at most: the least recently heard go first, the exited
    /// ones before the rest (a pane handed over before its CLI exited is
    /// never told `gone`).
    keep: usize,
    /// Orders the panes by when they were last heard of.
    tick: std::sync::atomic::AtomicU64,
}

#[derive(Debug, Default, Clone)]
struct PaneTurns {
    counts: TurnCounts,
    /// The pane's owner exited.
    gone: bool,
    heard: u64,
    /// K2-f.3: messages this host typed into the pane and whose prompt
    /// hasn't been submitted yet.
    typed: Vec<Typed>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Typed {
    msg_id: String,
    /// [`prompt_digest`] of the text.
    digest: String,
    /// K2-f.4: the pane's prompt count when it was typed. Two prompts on
    /// and still here, it was folded into a turn (or never submitted): no
    /// later prompt is its own. (One prompt on it may still be: the second
    /// of two messages typed back to back.)
    gen: u64,
}

/// Typed messages kept per pane, at most (the oldest go first).
const KEEP_TYPED: usize = 64;

/// [`Turns::keep`] in production.
const KEEP_PANES: usize = 4096;

impl Default for Turns {
    fn default() -> Self {
        Self::with_keep(KEEP_PANES)
    }
}

impl Turns {
    pub fn with_keep(keep: usize) -> Self {
        Turns { panes: Mutex::default(), keep: keep.max(1), tick: Default::default() }
    }

    fn touch(&self, m: &mut HashMap<String, PaneTurns>, pane: &str) -> u64 {
        let t = self.tick.fetch_add(1, Ordering::Relaxed) + 1;
        if !m.contains_key(pane) && m.len() >= self.keep {
            let mut order: Vec<(bool, u64, String)> = m.iter().map(|(p, v)| (!v.gone, v.heard, p.clone())).collect();
            order.sort();
            for (_, _, p) in order.into_iter().take(m.len() + 1 - self.keep) {
                m.remove(&p);
            }
        }
        t
    }

    /// One hook from the pane's owner. A subagent's (`agent_id` set) or a
    /// nested CLI's (`from_descendant`) never starts or ends the lead's turn.
    /// A prompt whose `digest` is a message this host typed into the pane
    /// takes that message into its turn (K2-f.3: the only evidence that the
    /// turn had it); a turn's end lets go of them, answered.
    /// Returns whether the pane's turn changed.
    pub fn note(&self, pane: &str, event: &str, subagent: bool, digest: Option<&str>) -> bool {
        if subagent {
            return false;
        }
        let mut m = self.panes.lock();
        let (prompt, stop) = match event {
            "UserPromptSubmit" => (true, false),
            "Stop" | "StopFailure" | "SessionEnd" => (false, true),
            _ => return false,
        };
        let heard = self.touch(&mut m, pane);
        let t = m.entry(pane.to_string()).or_default();
        t.heard = heard;
        if prompt {
            // K2-f.4: what two prompts have passed is no prompt's own any
            // more; this prompt takes the one message whose text it is. Two
            // or more with that text (one folded, one its own): which one
            // this is can't be told, so none is tied.
            let previous = t.counts.prompts;
            t.typed.retain(|e| e.gen + 1 >= previous);
            let matching: Vec<usize> =
                t.typed.iter().enumerate().filter(|(_, e)| Some(e.digest.as_str()) == digest).map(|(i, _)| i).collect();
            t.counts.msgs = match matching.as_slice() {
                [i] => vec![t.typed.remove(*i).msg_id],
                [] => Vec::new(),
                _ => {
                    log_debug!("[hosts/drain] {} typed messages match one prompt in {pane}: none is tied to it", matching.len());
                    t.typed.retain(|e| Some(e.digest.as_str()) != digest);
                    Vec::new()
                }
            };
            t.counts.prompts += 1;
            t.counts.running = true;
        }
        if stop {
            t.counts.stops += 1;
            t.counts.running = false;
            t.counts.msgs.clear();
        }
        true
    }

    /// ptyd S3: the pane's counts and typed messages, as ptyd's meta keeps
    /// them for a restarted daemon (`adopt::turn_meta`).
    pub fn snapshot(&self, pane: &str) -> Option<serde_json::Value> {
        let m = self.panes.lock();
        let t = m.get(pane)?;
        Some(super::adopt::turn_meta(&t.counts, serde_json::to_value(&t.typed).unwrap_or_default()))
    }

    /// ptyd S3: a pane adopted from ptyd gets back the counts and typed
    /// messages its meta kept ([`Self::snapshot`]). Anything unreadable is
    /// left out: a pane with no counts reports no turn.
    pub fn restore(&self, pane: &str, v: &serde_json::Value) {
        let counts: Option<TurnCounts> = v.get("counts").and_then(|c| serde_json::from_value(c.clone()).ok());
        let typed: Vec<Typed> = v.get("typed").and_then(|t| serde_json::from_value(t.clone()).ok()).unwrap_or_default();
        let mut m = self.panes.lock();
        let heard = self.touch(&mut m, pane);
        let t = m.entry(pane.to_string()).or_default();
        t.heard = heard;
        t.gone = false;
        if let Some(c) = counts {
            t.counts = c;
        }
        t.typed = typed;
    }

    /// K2-f.3: this host typed message `msg_id` (`text`) into `pane`; the
    /// prompt that submits exactly that text takes it into its turn.
    pub fn typed(&self, pane: &str, msg_id: &str, text: &str) {
        // Nothing typed (an inject with no text): no prompt can be its own.
        if text.trim().is_empty() {
            return;
        }
        let mut m = self.panes.lock();
        let heard = self.touch(&mut m, pane);
        let t = m.entry(pane.to_string()).or_default();
        t.heard = heard;
        t.typed.retain(|e| e.msg_id != msg_id);
        let gen = t.counts.prompts;
        t.typed.push(Typed { msg_id: msg_id.to_string(), digest: k2_core::agent_hooks::envelope::prompt_digest(text), gen });
        if t.typed.len() > KEEP_TYPED {
            t.typed.remove(0);
        }
    }

    /// The pane's owner exited. Its counts stay (running or not: a turn
    /// still running is one its CLI died in) until [`Self::take`].
    pub fn gone(&self, pane: &str) {
        let mut m = self.panes.lock();
        let heard = self.tick.fetch_add(1, Ordering::Relaxed) + 1;
        if let Some(t) = m.get_mut(pane) {
            t.gone = true;
            t.heard = heard;
        }
    }

    pub fn in_turn(&self, pane: &str) -> bool {
        self.panes.lock().get(pane).is_some_and(|t| t.counts.running && !t.gone)
    }

    /// K2-f: the pane's counts, for the controller. None until it has a
    /// prompt or a turn end (a pane with only typed messages has no turns).
    pub fn counts(&self, pane: &str) -> Option<TurnCounts> {
        self.panes.lock().get(pane).filter(|t| t.counts.prompts + t.counts.stops > 0).map(|t| t.counts.clone())
    }

    /// K2-f: the counts of a pane whose CLI exited, now reported: forgotten.
    pub fn take(&self, pane: &str) -> Option<TurnCounts> {
        let mut m = self.panes.lock();
        let counts = m.get(pane).filter(|t| t.counts.prompts + t.counts.stops > 0).map(|t| t.counts.clone());
        if m.get(pane).is_some_and(|t| t.gone) {
            m.remove(pane);
        }
        counts
    }
}

fn turns() -> &'static Turns {
    static T: OnceLock<Turns> = OnceLock::new();
    T.get_or_init(Turns::default)
}

/// K2-f: a hook that is not the lead's own: a subagent's (`agent_id` set)
/// or a CLI nested in the lead's (`from_descendant`, a `claude -p` run from
/// a Bash tool) shares the pane but not its turn.
pub fn is_subagent(envelope: &k2_core::agent_hooks::envelope::HookEnvelope) -> bool {
    envelope.agent_id.is_some() || envelope.from_descendant
}

/// [`Turns`] from this process's hook ingest, as each hook is posted
/// (`hook_ingest::publish`): synchronous, so a pane's last `Stop` is counted
/// before its CLI exits and the exit is reported.
pub fn note_ingest(ev: &crate::hook_ingest::IngestEvent) {
    use crate::hook_ingest::IngestEvent;
    match ev {
        IngestEvent::Envelope { envelope, .. } => {
            if turns().note(&envelope.pane, &envelope.event, is_subagent(envelope), envelope.prompt_digest.as_deref()) {
                save_turn(&envelope.pane);
            }
        }
        IngestEvent::OwnerReleased { pane, .. } => turns().gone(pane),
        _ => {}
    }
}

/// Tests (integration): a hook from the lead of `pane`, as [`note_ingest`]
/// would see it.
#[doc(hidden)]
pub fn note_turn_hook(pane: &str, event: &str) {
    turns().note(pane, event, false, None);
}

/// K2-f.3: this host typed message `msg_id` (`text`) into `pane` (a
/// delivery, a wake's launch prompt, an inject the controller named).
pub fn note_typed(pane: &str, msg_id: &str, text: &str) {
    turns().typed(pane, msg_id, text);
    save_turn(pane);
}

/// ptyd S3: `pane`'s turn into its ptyd meta, so a restarted daemon that
/// adopts it goes on from these counts. Written before the hook is
/// forwarded (the ingest is synchronous), so what a restarted host reports
/// is never behind what the controller was told. A no-op for a session
/// not on ptyd.
fn save_turn(pane: &str) {
    let Some(id) = k2_core::session::SessionId::parse(pane) else { return };
    let Some(pty) = crate::v2_session_map::lookup_by_session_id(&id) else { return };
    if pty.backend_name() != "ptyd" {
        return;
    }
    if let Some(v) = turns().snapshot(pane) {
        pty.set_ptyd_meta(super::adopt::META_TURN, v);
    }
}

/// ptyd S3: an adopted pane's turn, from its ptyd meta.
pub fn restore_turn(pane: &str, v: &serde_json::Value) {
    turns().restore(pane, v);
}

/// K2-f: `pane`'s turn counts on this host (reported in `hello`'s resume).
pub fn turn_counts(pane: &str) -> Option<TurnCounts> {
    turns().counts(pane)
}

/// K2-f: the counts of `pane`, whose CLI exited, as the controller is told
/// of it (`session_gone`, or `hello`'s resume after an outage).
pub fn take_turn_counts(pane: &str) -> Option<TurnCounts> {
    turns().take(pane)
}

/// Is this process's session `session_id` in its turn: from its hooks
/// ([`note_ingest`]), or its activity row being `Working`?
pub fn session_in_turn(session_id: &str) -> bool {
    turns().in_turn(session_id)
        || crate::activity_store::with_row(session_id, |r| r.lead.state == k2_core::activity::LeadState::Working).unwrap_or(false)
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
    /// Save the agent's home to the controller's store (P3 T1), while it is
    /// still placed here. A failure is logged and traced; the drain goes on.
    fn save_home(&self, agent_id: &str);
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
        // Its home is in the store before the controller drops the placement.
        h.save_home(agent);
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
        session_in_turn(session_id)
    }

    fn send(&self, f: Frame) -> bool {
        crate::delivery_drain::host_link().is_some_and(|tx| tx.send(f).is_ok())
    }

    fn stop(&self, agent_id: &str) {
        crate::agent_host::host().unplace(agent_id);
    }

    fn save_home(&self, agent_id: &str) {
        let epoch = super::link::placed_epoch(agent_id).unwrap_or(0);
        // The drain runs on a blocking thread of the runtime.
        let _ = tokio::runtime::Handle::current().block_on(super::home::save(agent_id, epoch));
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
    let after = move_after(|k| std::env::var(k).ok());
    tokio::spawn(async move {
        loop {
            remote.move_lost(Instant::now(), after);
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
    let resumed = match project_path {
        Some(path) => crate::pinned_chat::ensure_pinned_chat(&path, false, false).map(drop),
        None => resume_v2(remote, r),
    };
    // K2-d: a resume is a wake: the idle reaper's clock starts over.
    if resumed.is_ok() {
        crate::active_reaper::note_agent_activity(&r.agent_id);
    }
    resumed
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
        t.note("p1", "SessionStart", false, None);
        assert!(!t.in_turn("p1"));
        t.note("p1", "UserPromptSubmit", false, None);
        assert!(t.in_turn("p1"));
        t.note("p1", "Stop", true, None);
        assert!(t.in_turn("p1"), "a subagent's Stop doesn't end the lead's turn");
        t.note("p1", "PreToolUse", false, None);
        assert!(t.in_turn("p1"));
        t.note("p1", "Stop", false, None);
        assert!(!t.in_turn("p1"));
        t.note("p2", "UserPromptSubmit", false, None);
        t.gone("p2");
        assert!(!t.in_turn("p2"), "the CLI exited");
        t.note("p3", "UserPromptSubmit", false, None);
        t.note("p3", "StopFailure", false, None);
        assert!(!t.in_turn("p3"));
    }

    #[test]
    fn turns_are_counted_and_kept_after_the_cli_exits_until_reported() {
        let t = Turns::default();
        t.note("p1", "UserPromptSubmit", false, None);
        t.note("p1", "Stop", false, None);
        t.note("p1", "UserPromptSubmit", false, None);
        t.note("p1", "Stop", true, None);
        assert_eq!(t.counts("p1"), Some(TurnCounts { prompts: 2, stops: 1, running: true, msgs: vec![] }), "a subagent's Stop isn't counted");
        t.gone("p1");
        assert!(!t.in_turn("p1"));
        assert_eq!(t.take("p1"), Some(TurnCounts { prompts: 2, stops: 1, running: true, msgs: vec![] }), "it died mid-turn");
        assert_eq!(t.take("p1"), None, "reported once");
        t.note("p2", "UserPromptSubmit", false, None);
        assert_eq!(t.take("p2"), Some(TurnCounts { prompts: 1, stops: 0, running: true, msgs: vec![] }), "a live pane is kept");
        assert!(t.in_turn("p2"));
    }

    /// K2-f.3 #2, #3: a typed message is in the turn whose prompt is its
    /// text, and only that one. One typed while a turn runs and folded into
    /// it (no prompt of its own) is in no turn; two typed in a row each get
    /// their own prompt's turn; a turn's end lets go of its messages.
    #[test]
    fn a_message_is_in_the_turn_whose_prompt_is_its_text() {
        use k2_core::agent_hooks::envelope::prompt_digest;
        let t = Turns::default();
        let d = |s: &str| Some(prompt_digest(s));
        t.note("p1", "UserPromptSubmit", false, d("the user's own").as_deref());
        t.typed("p1", "m_folded", "[from alice] folded in");
        t.note("p1", "Stop", false, None);
        assert_eq!(t.counts("p1").unwrap().msgs, Vec::<String>::new());
        t.typed("p1", "m_a", "[from alice] A");
        t.typed("p1", "m_b", "[from alice] B");
        t.note("p1", "UserPromptSubmit", false, d("\u{1b}[200~[from alice] A\u{1b}[201~\r").as_deref());
        assert_eq!(t.counts("p1").unwrap().msgs, vec!["m_a".to_string()]);
        t.note("p1", "Stop", false, None);
        assert_eq!(t.counts("p1").unwrap().msgs, Vec::<String>::new(), "answered");
        t.note("p1", "UserPromptSubmit", false, d("[from alice] B").as_deref());
        assert_eq!(t.counts("p1").unwrap(), TurnCounts { prompts: 3, stops: 2, running: true, msgs: vec!["m_b".into()] });
        // The folded one never matches a later prompt that isn't its text.
        t.note("p1", "Stop", false, None);
        t.note("p1", "UserPromptSubmit", false, d("[from alice] something else").as_deref());
        assert_eq!(t.counts("p1").unwrap().msgs, Vec::<String>::new());
    }

    /// K2-f.4 #1: a message folded into a turn (and answered there) is
    /// never tied to a later prompt with the same text: two prompts on it
    /// is dropped, and before that a prompt matching two typed messages
    /// ties neither. Two messages typed back to back still each get their
    /// own prompt. Nothing is registered for empty text.
    #[test]
    fn a_folded_message_is_never_tied_to_a_later_identical_prompt() {
        use k2_core::agent_hooks::envelope::prompt_digest;
        let d = |s: &str| Some(prompt_digest(s));
        let same = "[from ci] tests passed";
        // M1 folded into turn 1; M2, the same text, as its own prompt 2.
        let t = Turns::default();
        t.note("p1", "UserPromptSubmit", false, d("the user's own").as_deref());
        t.typed("p1", "m1", same);
        t.note("p1", "Stop", false, None);
        t.typed("p1", "m2", same);
        t.note("p1", "UserPromptSubmit", false, d(same).as_deref());
        assert_eq!(t.counts("p1").unwrap().msgs, Vec::<String>::new(), "two match: neither is tied");
        // M2 several prompts later: M1 is gone; M2 is tied.
        let t = Turns::default();
        t.note("p2", "UserPromptSubmit", false, d("the user's own").as_deref());
        t.typed("p2", "m1", same);
        t.note("p2", "Stop", false, None);
        for n in 2..=3 {
            t.note("p2", "UserPromptSubmit", false, d(&format!("other {n}")).as_deref());
            t.note("p2", "Stop", false, None);
        }
        t.typed("p2", "m2", same);
        t.note("p2", "UserPromptSubmit", false, d(same).as_deref());
        assert_eq!(t.counts("p2").unwrap().msgs, vec!["m2".to_string()]);
        // A and B typed back to back: each its own prompt.
        let t = Turns::default();
        t.typed("p3", "a", "A");
        t.typed("p3", "b", "B");
        t.note("p3", "UserPromptSubmit", false, d("A").as_deref());
        assert_eq!(t.counts("p3").unwrap().msgs, vec!["a".to_string()]);
        t.note("p3", "Stop", false, None);
        t.note("p3", "UserPromptSubmit", false, d("B").as_deref());
        assert_eq!(t.counts("p3").unwrap().msgs, vec!["b".to_string()]);
        // An inject with no text registers nothing: an empty prompt ties nothing.
        let t = Turns::default();
        t.typed("p4", "m_empty", "  \r");
        t.note("p4", "UserPromptSubmit", false, d("").as_deref());
        assert_eq!(t.counts("p4").unwrap().msgs, Vec::<String>::new());
    }

    /// K2-f.2 #8: past `keep` panes, the least recently heard go first,
    /// the exited ones before the live ones.
    /// ptyd S3: what ptyd's meta keeps of a pane (`snapshot`) gives a
    /// restarted daemon the same counts and typed messages (`restore`):
    /// its next prompt takes the message typed before the restart, and its
    /// counts go on from where they were.
    #[test]
    fn a_pane_restored_from_its_meta_goes_on_where_it_was() {
        let before = Turns::default();
        before.typed("p1", "m_a", "first");
        before.note("p1", "UserPromptSubmit", false, Some(&k2_core::agent_hooks::envelope::prompt_digest("first")));
        before.note("p1", "Stop", false, None);
        before.typed("p1", "m_b", "second");
        let meta = before.snapshot("p1").expect("a pane with turns");

        let after = Turns::default();
        after.restore("p1", &meta);
        assert_eq!(after.snapshot("p1"), Some(meta.clone()), "the same pane, round trip");
        assert_eq!(after.counts("p1").map(|c| (c.prompts, c.stops, c.running)), Some((1, 1, false)));
        after.note("p1", "UserPromptSubmit", false, Some(&k2_core::agent_hooks::envelope::prompt_digest("second")));
        let c = after.counts("p1").unwrap();
        assert_eq!((c.prompts, c.stops, c.running), (2, 1, true));
        assert_eq!(c.msgs, vec!["m_b".to_string()], "the message typed before the restart is this prompt's");

        // Unreadable meta: no counts, nothing typed.
        let empty = Turns::default();
        empty.restore("p2", &serde_json::json!({ "counts": "x" }));
        assert_eq!(empty.counts("p2"), None);
    }

    #[test]
    fn panes_are_evicted_oldest_first_exited_ones_before_live_ones() {
        let t = Turns::with_keep(2);
        t.note("p1", "UserPromptSubmit", false, None);
        t.note("p2", "UserPromptSubmit", false, None);
        t.gone("p2");
        t.note("p3", "UserPromptSubmit", false, None);
        assert!(t.counts("p2").is_none(), "the exited one goes first");
        assert!(t.counts("p1").is_some() && t.counts("p3").is_some());
        t.note("p1", "Stop", false, None);
        t.note("p4", "UserPromptSubmit", false, None);
        assert!(t.counts("p3").is_none(), "then the least recently heard");
        assert!(t.counts("p1").is_some() && t.counts("p4").is_some());
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
    fn the_move_delay_comes_from_the_env() {
        let get = |v: &'static str| move |k: &str| (k == MOVE_AFTER_VAR).then(|| v.to_string());
        assert_eq!(move_after(get("3")), Duration::from_secs(3));
        assert_eq!(move_after(get("0")), Duration::ZERO);
        assert_eq!(move_after(get("later")), DEFAULT_MOVE_AFTER);
        assert_eq!(move_after(|_| None), DEFAULT_MOVE_AFTER);
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
        fn save_home(&self, agent_id: &str) {
            self.log.lock().push(format!("save {agent_id}"));
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
                "save idle",
                "released idle req 0",
                "stop idle",
                "save busy",
                "released busy req 0",
                "stop busy",
                "save stuck",
                "released stuck req 0",
                "stop stuck"
            ]
        );
    }
}
