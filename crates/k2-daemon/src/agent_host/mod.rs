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
    _viewer: ViewerRegistration,
}

/// One subscriber's share of the session's grid frame stream
/// ([`AgentHost::stream`]). Field meanings are `grid_emitter::attach`'s.
pub(crate) struct GridStream {
    pub frames: broadcast::Receiver<crate::grid_emitter::GridFrame>,
    pub emit_state: Arc<parking_lot::Mutex<k2_core::terminal::EmitState>>,
    pub format_reg: crate::grid_emitter::FormatRegistration,
}

/// A handle to one agent session on some host. Cheap to clone; dropping
/// it never tears the session down unless it was the last reference.
///
/// Also known as `session_lookup::LiveSession` (the name older call sites
/// use). Its accessors read session metadata; everything that drives the
/// session goes through [`AgentHost`] (`write` and `resize` here are
/// shorthands for [`host`]`().write` / `.resize`).
#[derive(Clone)]
pub struct AgentSession(pub(crate) Arc<DaemonPtySession>);

impl AgentSession {
    /// Wrap an in-process session (one the placement table already holds).
    pub(crate) fn in_process(pty: Arc<DaemonPtySession>) -> Self {
        Self(pty)
    }

    /// The in-process session behind this handle. Host-side code only
    /// (the grid pipeline and the viewport arbitration it owns); the
    /// controller side uses [`AgentHost`]. P2 turns this into a match on
    /// the host kind.
    pub(crate) fn pty(&self) -> &Arc<DaemonPtySession> {
        &self.0
    }

    pub fn session_id(&self) -> SessionId {
        self.0.session_id
    }

    pub fn epoch(&self) -> PlacementEpoch {
        IN_PROCESS_EPOCH
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
        self.0.cwd.as_deref()
    }

    /// The cwd as a string, empty when unknown. Companion endpoints match
    /// this against `projects.path` to attribute sessions to workspaces.
    pub fn cwd(&self) -> String {
        self.0
            .cwd
            .as_deref()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default()
    }

    /// The program the child runs, when one was given.
    pub fn program(&self) -> Option<&str> {
        self.0.program.as_deref()
    }

    /// Owned [`Self::program`] (the older `LiveSession` name).
    pub fn command(&self) -> Option<String> {
        self.0.program.clone()
    }

    /// The durable spawn args (never a fire-once launch prompt).
    pub fn args(&self) -> Vec<String> {
        self.0.args.clone()
    }

    /// Borrowed [`Self::args`].
    pub fn args_ref(&self) -> &[String] {
        &self.0.args
    }

    pub fn sandbox(&self) -> k2_core::terminal::SandboxSpec {
        self.0.sandbox
    }

    /// Kept for older call sites that branched on the renderer; every
    /// session is a v2 session.
    pub fn is_v2(&self) -> bool {
        true
    }

    pub fn is_child_alive(&self) -> bool {
        self.0.is_child_alive()
    }

    pub fn child_pid(&self) -> Option<i32> {
        self.0.child_pid()
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
        self.0.visible_text_rows()
    }

    /// True when the child has bracketed-paste mode on.
    pub fn bracketed_paste_active(&self) -> bool {
        self.0.bracketed_paste_active()
    }

    /// Clients attached right now ([`Attachment`]s held), not internal
    /// event observers. The age-out reaper and the close guard read it.
    pub fn subscriber_count(&self) -> usize {
        self.0.subscriber_count()
    }

    /// True once any viewer has ever attached (the bare-tab cap counts
    /// only shells nobody ever watched).
    pub fn ever_attached(&self) -> bool {
        self.0.ever_attached.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// True when both handles name the same live session.
    pub fn same_session(&self, other: &AgentSession) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// A handle that does not keep the session alive (an observer that
    /// must not outlive the session's last real holder).
    pub fn downgrade(&self) -> WeakAgentSession {
        WeakAgentSession(Arc::downgrade(&self.0))
    }
}

/// [`AgentSession::downgrade`]'s handle.
#[derive(Clone)]
pub struct WeakAgentSession(Weak<DaemonPtySession>);

impl WeakAgentSession {
    /// The session, if anything still holds it.
    pub fn upgrade(&self) -> Option<AgentSession> {
        self.0.upgrade().map(AgentSession)
    }
}

/// Every way the controller drives an agent session. Object safe, so the
/// daemon holds one `&'static dyn AgentHost` ([`host`]).
pub(crate) trait AgentHost: Send + Sync {
    #[allow(dead_code)] // interface surface: tests today, the remote host and placement checks in P2
    /// Short name for logs and the progress page (`in-process`).
    fn kind(&self) -> &'static str;

    #[allow(dead_code)] // interface surface: tests today, the remote host and placement checks in P2
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

    #[allow(dead_code)] // interface surface: tests today, the remote host and placement checks in P2
    /// Hook envelopes that passed the owner check, for every session on
    /// this host.
    fn hook_events(&self) -> broadcast::Receiver<crate::hook_ingest::IngestEvent>;

    // ── the agent's home ───────────────────────────────────────────────

    #[allow(dead_code)] // interface surface: tests today, the remote host and placement checks in P2
    fn read_home(&self, home: &AgentHome, rel: &Path) -> io::Result<Vec<u8>>;

    #[allow(dead_code)] // interface surface: tests today, the remote host and placement checks in P2
    /// Write a file in the home, creating parent directories; readers see
    /// the old or the new content, never a torn one.
    fn write_home(&self, home: &AgentHome, rel: &Path, bytes: &[u8]) -> io::Result<()>;
}

static IN_PROCESS: InProcessHost = InProcessHost;

/// The daemon's agent host. P1: always the in-process host.
pub(crate) fn host() -> &'static dyn AgentHost {
    &IN_PROCESS
}
