//! `AgentHost`: the one interface the controller side of the daemon uses
//! to drive agent sessions (PRD "K2 Distributed Runtime", stage P1, FR1).
//!
//! Spawn, place, attach, stream, write, inject, resize, pin, kill, hook
//! events, and read and write the agent's home all go through
//! [`AgentHost`]. P1 has one implementation, [`InProcessHost`], a thin
//! wrapper over today's `DaemonPtySession` and its placement table
//! (`v2_session_map`), so behavior is unchanged. P2 adds a remote host over
//! the signed WebSocket; it implements the same trait.
//!
//! Shape rules that keep a remote host possible:
//! - ids are explicit: an agent id (the placement key), a [`SessionId`]
//!   and a [`PlacementEpoch`] (invariant I1). The in-process host always
//!   uses [`IN_PROCESS_EPOCH`].
//! - no `&DaemonPtySession` and no file descriptors cross the trait.
//!   Sessions travel as [`AgentSession`] handles; arguments and results
//!   are owned data.
//! - every method is non-blocking in intent, apart from `spawn`, `kill`
//!   and `inject`, which block today exactly as the code they replaced
//!   did (see `docs/prd/progress/agenthost.md`, "What P2 needs").
//!
//! The grid pipeline (`grid_emitter` and the Term-reading half of
//! `sessions_grid_ws`) is host-side code: it renders frames from the
//! host's Term. It reaches the in-process session through
//! [`AgentSession::pty`], the one documented escape hatch. In P2 it runs
//! inside `k2-host` and the controller relays its frames.

mod in_process;
pub mod remote;
#[cfg(test)]
mod tests;

use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Weak};

use k2_core::session::SessionId;
use k2_core::terminal::{AlacEvent, DaemonPtyConfig, DaemonPtySession, LabelSource, ViewerRegistration};
use tokio::sync::broadcast;

pub(crate) use in_process::InProcessHost;

/// A placement's epoch (invariant I1: an agent has at most one live
/// placement; a host holding a stale epoch is refused). Reserved in P1:
/// every in-process placement carries [`IN_PROCESS_EPOCH`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlacementEpoch(pub u64);

/// The epoch of every placement on the in-process host.
pub const IN_PROCESS_EPOCH: PlacementEpoch = PlacementEpoch(1);

/// What [`AgentHost::place`] recorded: which agent id now names which
/// session, at which epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    pub agent_id: String,
    pub session_id: SessionId,
    pub epoch: PlacementEpoch,
}

/// Result of [`AgentHost::inject`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InjectResult {
    /// Every step ran against a live child.
    Delivered,
    /// A write failed or a `CheckAlive` step found the child gone.
    ChildGone,
}

/// The agent's home: the directory tree [`AgentHost::read_home`] and
/// [`AgentHost::write_home`] work in (the workspace, its `.k2/`, the
/// provider's memory). Paths inside it are always relative.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentHome {
    pub root: PathBuf,
}

impl AgentHome {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// `rel` joined under the root. Refuses absolute paths and any `..`,
    /// so a caller can never reach outside the home.
    pub fn resolve(&self, rel: &Path) -> io::Result<PathBuf> {
        let mut out = self.root.clone();
        let mut any = false;
        for c in rel.components() {
            match c {
                Component::Normal(part) => {
                    out.push(part);
                    any = true;
                }
                Component::CurDir => {}
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("agent home path must be relative and stay inside: {}", rel.display()),
                    ))
                }
            }
        }
        if !any {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty agent home path"));
        }
        Ok(out)
    }
}

/// A live viewer's hold on a session ([`AgentHost::attach`]). Holding it
/// counts as one attached client (`subscriber_count`); dropping it, on
/// any exit path, releases that.
pub struct Attachment {
    /// The session's terminal events (output wakeups, title, bell, exit).
    pub events: broadcast::Receiver<AlacEvent>,
    /// Out-of-band label changes.
    pub labels: broadcast::Receiver<String>,
    /// Pin-to-size changes.
    pub pins: broadcast::Receiver<Option<(u16, u16)>>,
    /// In-process: the viewer registration. Remote: `None` (the host
    /// counts the relayed grid stream as its viewer).
    _viewer: Option<ViewerRegistration>,
}

/// One subscriber's share of the session's grid frame stream
/// ([`AgentHost::stream`]). Field meanings are `grid_emitter::attach`'s.
pub(crate) struct GridStream {
    pub frames: broadcast::Receiver<crate::grid_emitter::GridFrame>,
    pub emit_state: Arc<parking_lot::Mutex<k2_core::terminal::EmitState>>,
    pub format_reg: crate::grid_emitter::FormatRegistration,
}

/// What an [`AgentSession`] points at: a session in this process, or one
/// on a remote agent host (`K2_ROLE=controller`, `remote::RemoteSession`).
///
/// Also the `.0` older call sites read (`.0.label()`, `.0.set_label()`,
/// `.0.kill()`), so it answers those directly.
#[derive(Clone)]
pub enum SessionRef {
    Local(Arc<DaemonPtySession>),
    Remote(Arc<remote::RemoteSession>),
}

impl SessionRef {
    pub fn label(&self) -> String {
        match self {
            SessionRef::Local(p) => p.label(),
            SessionRef::Remote(r) => r.label(),
        }
    }

    pub fn label_source(&self) -> LabelSource {
        match self {
            SessionRef::Local(p) => p.label_source(),
            SessionRef::Remote(r) => r.label_source(),
        }
    }

    /// Set the label; `lock` stops PTY titles overwriting it. Returns the
    /// label now in effect.
    pub fn set_label(&self, label: String, lock: bool) -> String {
        match self {
            SessionRef::Local(p) => p.set_label(label, lock),
            SessionRef::Remote(r) => r.set_label(label, lock),
        }
    }

    /// Kill and reap the child. Idempotent.
    pub fn kill(&self) {
        match self {
            SessionRef::Local(p) => p.kill(),
            SessionRef::Remote(r) => r.kill(),
        }
    }
}

/// A handle to one agent session on some host. Cheap to clone; dropping
/// it never tears the session down unless it was the last reference.
///
/// Also known as `session_lookup::LiveSession` (the name older call sites
/// use). Its accessors read session metadata; everything that drives the
/// session goes through [`AgentHost`] (`write` and `resize` here are
/// shorthands for [`host`]`().write` / `.resize`).
///
/// A remote handle answers the fixed metadata (id, cwd, program, args)
/// from what its spawn returned, liveness from host events, and the live
/// terminal state (screen rows, bracketed paste, viewers) by asking the
/// host.
#[derive(Clone)]
pub struct AgentSession(pub(crate) SessionRef);

impl AgentSession {
    /// Wrap an in-process session (one the placement table already holds).
    pub(crate) fn in_process(pty: Arc<DaemonPtySession>) -> Self {
        Self(SessionRef::Local(pty))
    }

    pub(crate) fn remote(r: Arc<remote::RemoteSession>) -> Self {
        Self(SessionRef::Remote(r))
    }

    /// True for a session on a remote agent host.
    pub fn is_remote(&self) -> bool {
        matches!(self.0, SessionRef::Remote(_))
    }

    pub(crate) fn as_remote(&self) -> Option<&Arc<remote::RemoteSession>> {
        match &self.0 {
            SessionRef::Remote(r) => Some(r),
            SessionRef::Local(_) => None,
        }
    }

    /// The in-process session behind this handle. Host-side code only
    /// (the in-process host, the grid pipeline and the viewport
    /// arbitration it owns), which never sees a remote handle: on a
    /// controller the gate relays `/cli/sessions/grid` before the grid
    /// handler runs, and the in-process host only hands out local handles.
    pub(crate) fn pty(&self) -> &Arc<DaemonPtySession> {
        match &self.0 {
            SessionRef::Local(p) => p,
            SessionRef::Remote(r) => panic!(
                "in-process session expected, got remote session {} on host {} (host-side code reached on a controller)",
                r.session_id, r.host_id
            ),
        }
    }

    pub fn session_id(&self) -> SessionId {
        match &self.0 {
            SessionRef::Local(p) => p.session_id,
            SessionRef::Remote(r) => r.session_id,
        }
    }

    pub fn epoch(&self) -> PlacementEpoch {
        match &self.0 {
            SessionRef::Local(_) => IN_PROCESS_EPOCH,
            SessionRef::Remote(r) => r.epoch,
        }
    }

    /// Write bytes to the child's stdin (through [`AgentHost::write`]).
    pub fn write(&self, bytes: &[u8]) -> io::Result<()> {
        // A borrowed slice with no lifetime extension available, so copy.
        // Hot path is single-keystroke injection (~tens of bytes).
        host().write(self, bytes.to_vec())
    }

    /// Debounced resize (through [`AgentHost::resize`]): bursts coalesce
    /// to their final geometry and a same-dims request is skipped. The
    /// in-process resize is infallible, so this is always `Ok(())`; the
    /// stringly error keeps the older `LiveSession` signature.
    pub fn resize(&self, cols: u16, rows: u16) -> Result<(), String> {
        host().resize(self, cols, rows);
        Ok(())
    }

    /// The cwd the child was spawned in.
    pub fn cwd_path(&self) -> Option<&Path> {
        match &self.0 {
            SessionRef::Local(p) => p.cwd.as_deref(),
            SessionRef::Remote(r) => r.cwd.as_deref(),
        }
    }

    /// The cwd as a string, empty when unknown. Companion endpoints match
    /// this against `projects.path` to attribute sessions to workspaces.
    pub fn cwd(&self) -> String {
        self.cwd_path().map(|p| p.to_string_lossy().into_owned()).unwrap_or_default()
    }

    /// The program the child runs, when one was given.
    pub fn program(&self) -> Option<&str> {
        match &self.0 {
            SessionRef::Local(p) => p.program.as_deref(),
            SessionRef::Remote(r) => r.program.as_deref(),
        }
    }

    /// Owned [`Self::program`] (the older `LiveSession` name).
    pub fn command(&self) -> Option<String> {
        self.program().map(str::to_string)
    }

    /// The durable spawn args (never a fire-once launch prompt).
    pub fn args(&self) -> Vec<String> {
        self.args_ref().to_vec()
    }

    /// Borrowed [`Self::args`].
    pub fn args_ref(&self) -> &[String] {
        match &self.0 {
            SessionRef::Local(p) => &p.args,
            SessionRef::Remote(r) => &r.args,
        }
    }

    pub fn sandbox(&self) -> k2_core::terminal::SandboxSpec {
        match &self.0 {
            SessionRef::Local(p) => p.sandbox,
            // Daemon spawns on hosts are passthrough in this build.
            SessionRef::Remote(_) => k2_core::terminal::SandboxSpec::Passthrough,
        }
    }

    /// Kept for older call sites that branched on the renderer; every
    /// session is a v2 session.
    pub fn is_v2(&self) -> bool {
        true
    }

    pub fn is_child_alive(&self) -> bool {
        match &self.0 {
            SessionRef::Local(p) => p.is_child_alive(),
            SessionRef::Remote(r) => r.is_child_alive(),
        }
    }

    /// The child's pid. `None` for a remote session: its pid is in the
    /// host's process namespace, meaningless here.
    pub fn child_pid(&self) -> Option<i32> {
        match &self.0 {
            SessionRef::Local(p) => p.child_pid(),
            SessionRef::Remote(_) => None,
        }
    }

    pub fn label(&self) -> String {
        self.0.label()
    }

    pub fn label_source(&self) -> LabelSource {
        self.0.label_source()
    }

    /// Plain-text projection of the visible grid rows (verified injection
    /// reads it to confirm the input box cleared).
    pub fn visible_text_rows(&self) -> Vec<String> {
        match &self.0 {
            SessionRef::Local(p) => p.visible_text_rows(),
            SessionRef::Remote(r) => r.visible_text_rows(),
        }
    }

    /// True when the child has bracketed-paste mode on.
    pub fn bracketed_paste_active(&self) -> bool {
        match &self.0 {
            SessionRef::Local(p) => p.bracketed_paste_active(),
            SessionRef::Remote(r) => r.bracketed_paste_active(),
        }
    }

    /// Clients attached right now ([`Attachment`]s held), not internal
    /// event observers. The age-out reaper and the close guard read it.
    pub fn subscriber_count(&self) -> usize {
        match &self.0 {
            SessionRef::Local(p) => p.subscriber_count(),
            SessionRef::Remote(r) => r.subscriber_count(),
        }
    }

    /// True once any viewer has ever attached (the bare-tab cap counts
    /// only shells nobody ever watched).
    pub fn ever_attached(&self) -> bool {
        match &self.0 {
            SessionRef::Local(p) => p.ever_attached.load(std::sync::atomic::Ordering::Relaxed),
            SessionRef::Remote(r) => r.ever_attached(),
        }
    }

    /// True when both handles name the same live session.
    pub fn same_session(&self, other: &AgentSession) -> bool {
        match (&self.0, &other.0) {
            (SessionRef::Local(a), SessionRef::Local(b)) => Arc::ptr_eq(a, b),
            (SessionRef::Remote(a), SessionRef::Remote(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }

    /// A handle that does not keep the session alive (an observer that
    /// must not outlive the session's last real holder).
    pub fn downgrade(&self) -> WeakAgentSession {
        match &self.0 {
            SessionRef::Local(p) => WeakAgentSession::Local(Arc::downgrade(p)),
            SessionRef::Remote(r) => WeakAgentSession::Remote(Arc::downgrade(r)),
        }
    }
}

/// [`AgentSession::downgrade`]'s handle.
#[derive(Clone)]
pub enum WeakAgentSession {
    Local(Weak<DaemonPtySession>),
    Remote(Weak<remote::RemoteSession>),
}

impl WeakAgentSession {
    /// The session, if anything still holds it.
    pub fn upgrade(&self) -> Option<AgentSession> {
        match self {
            WeakAgentSession::Local(w) => w.upgrade().map(AgentSession::in_process),
            WeakAgentSession::Remote(w) => w.upgrade().map(AgentSession::remote),
        }
    }
}

/// Every way the controller drives an agent session. Object safe, so the
/// daemon holds one `&'static dyn AgentHost` ([`host`]).
pub(crate) trait AgentHost: Send + Sync {
    /// Short name for logs and the progress page (`in-process`).
    fn kind(&self) -> &'static str;

    /// The epoch new placements on this host get.
    fn epoch(&self) -> PlacementEpoch;

    // ── spawn and placement ────────────────────────────────────────────

    /// Start a session (PTY plus child). Not yet findable: [`Self::place`]
    /// makes it so.
    fn spawn(&self, config: DaemonPtyConfig) -> io::Result<AgentSession>;

    /// Record `session` under `agent_id`: findable from now on, with the
    /// activity row, `SessionAdded` and the durable tab row. A session
    /// already placed under the id is displaced and killed.
    fn place(&self, agent_id: &str, session: &AgentSession) -> Placement;

    /// Remove the placement under `agent_id`, run the "session goes away"
    /// cleanup, and kill the child. Returns the session that was placed.
    fn unplace(&self, agent_id: &str) -> Option<AgentSession>;

    fn find_agent(&self, agent_id: &str) -> Option<AgentSession>;

    fn find_session(&self, session_id: &SessionId) -> Option<AgentSession>;

    /// Every (agent id, session) placed on this host, unordered.
    fn placements(&self) -> Vec<(String, AgentSession)>;

    // ── attach and stream ──────────────────────────────────────────────

    /// Attach a viewer: registers it (counted by `subscriber_count` until
    /// the [`Attachment`] drops) and subscribes to events, labels, pins.
    fn attach(&self, session: &AgentSession) -> Attachment;

    /// Release a viewer's viewport claim when its connection ends (the
    /// next active viewer is elected from the remaining viewports).
    fn detach(&self, session: &AgentSession, subscriber_id: u64);

    /// Join the session's shared grid frame stream.
    fn stream(&self, session: &AgentSession, pane_id: &str, wants_k1: bool) -> GridStream;

    /// The session's terminal events for an internal observer. Never
    /// counts as an attached viewer.
    fn events(&self, session: &AgentSession) -> broadcast::Receiver<AlacEvent>;

    /// Record that the child exited (its `ChildExit` event was seen), so a
    /// lookup racing the exit cleanup never reports it live.
    fn mark_exited(&self, session: &AgentSession);

    /// The last `lines` lines of scrollback plus screen, as plain text,
    /// trailing empty rows trimmed.
    fn read_screen(&self, session: &AgentSession, lines: usize) -> Vec<String>;

    // ── input, inject, resize, kill ────────────────────────────────────

    /// Queue bytes for the child's stdin. Never waits on the child.
    fn write(&self, session: &AgentSession, bytes: Vec<u8>) -> io::Result<()>;

    /// Run an inject flow (writes, settles, liveness checks) as one unit.
    /// Callers hold the session's injection lock.
    fn inject(&self, session: &AgentSession, steps: Vec<k2_core::inject_flow::InjectAction>) -> InjectResult;

    /// Debounced resize: bursts coalesce, same dims are skipped.
    fn resize(&self, session: &AgentSession, cols: u16, rows: u16);

    /// Resize now (pin clamp and same-dims skip, no debounce). Returns
    /// the size the terminal has afterwards.
    fn resize_now(&self, session: &AgentSession, cols: u16, rows: u16) -> (u16, u16);

    /// The terminal's current size.
    fn size(&self, session: &AgentSession) -> (u16, u16);

    /// The active viewer's last claimed size, `(0, 0)` when none.
    fn claimed_size(&self, session: &AgentSession) -> (u16, u16);

    /// Record the active viewer's size (what the next attach pre-snaps to).
    fn set_claimed_size(&self, session: &AgentSession, cols: u16, rows: u16);

    /// Pin the terminal to a size, or unpin with `None`.
    fn pin(&self, session: &AgentSession, dims: Option<(u16, u16)>, set_by: Option<String>);

    /// Set the session's label; `lock` stops PTY titles overwriting it.
    /// Returns the label now in effect.
    fn set_label(&self, session: &AgentSession, label: String, lock: bool) -> String;

    /// Kill and reap the child. Idempotent.
    fn kill(&self, session: &AgentSession);

    // ── hook events ────────────────────────────────────────────────────

    /// Hook envelopes that passed the owner check, for every session on
    /// this host.
    fn hook_events(&self) -> broadcast::Receiver<crate::hook_ingest::IngestEvent>;

    // ── the agent's home ───────────────────────────────────────────────

    fn read_home(&self, home: &AgentHome, rel: &Path) -> io::Result<Vec<u8>>;

    /// Write a file in the home, creating parent directories; readers see
    /// the old or the new content, never a torn one.
    fn write_home(&self, home: &AgentHome, rel: &Path, bytes: &[u8]) -> io::Result<()>;
}

static IN_PROCESS: InProcessHost = InProcessHost;

/// The daemon's agent host: the remote hosts on a `K2_ROLE=controller`
/// daemon (`remote::RemoteHost`), the in-process host otherwise.
pub(crate) fn host() -> &'static dyn AgentHost {
    match crate::runtime_role::current() {
        crate::runtime_role::Role::Controller => remote::global(),
        crate::runtime_role::Role::All | crate::runtime_role::Role::Host => &IN_PROCESS,
    }
}
