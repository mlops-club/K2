//! The remote agent host, controller side (PRD P2, first cut: scenario S1
//! plus hooks and `/login`).
//!
//! Hosts dial in over `/cli/hosts/attach` (`crate::hosts::hub`, the
//! socket half). This file is the state behind those links: which hosts
//! are connected, the placement table (agent id → host, session, epoch),
//! requests waiting for a host's reply, and the grid streams relayed
//! through each link. No socket code here; links talk to it through
//! [`RemoteHost::connect`], [`RemoteHost::on_frame`] and the frame
//! channel each link drains.
//!
//! W1: `RemoteHost` is the controller's `AgentHost` (`agent_host::host()`
//! on `K2_ROLE=controller`). Placed sessions are [`RemoteSession`]
//! handles inside `AgentSession`: fixed metadata from the spawn, liveness
//! from `session.gone`, live terminal state by asking the host (`op`).
//! The workspace spawn (chats, heartbeats, wake, agents routes) is
//! forwarded whole (`spawn.workspace`, [`RemoteHost::spawn_workspace_blocking`]);
//! the HTTP S1 routes still call [`RemoteHost::spawn`], [`RemoteHost::close`]
//! and [`RemoteHost::open_grid`].

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use k2_core::log_debug;
use k2_core::session::SessionId;
use k2_core::terminal::{AlacEvent, DaemonPtyConfig, LabelSource};
use parking_lot::Mutex;
use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc, oneshot};

use super::{
    AgentHome, AgentHost, AgentSession, Attachment, GridStream, InjectResult, Placement, PlacementEpoch,
};
use crate::hosts::proto::{
    trace_events, Frame, InjectOutcome, InjectStep, ResumePlacement, SessionMeta, SessionOp, SpawnedSession,
};

/// How long the controller waits for a host to answer a spawn or close.
/// Only stops a request to a hung host; tests never depend on it.
const REPLY_DEADLINE: Duration = Duration::from_secs(120);
/// The same for one session op (screen read, kill, inject).
const OP_DEADLINE: Duration = Duration::from_secs(30);

/// Message when a spawn finds no host (acceptance criterion 3).
pub const NO_HOSTS: &str =
    "no agent host is connected to this controller (K2_ROLE=controller runs no agents locally; start a K2_ROLE=host process)";

/// One agent placed on a remote host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemotePlacement {
    pub agent_id: String,
    pub host_id: String,
    /// The host's session id, once its spawn reply named one.
    pub session_id: Option<String>,
    pub epoch: PlacementEpoch,
    /// Hook frames accepted for this placement.
    pub hooks: u64,
}

/// A hook event a host attested, as the controller received it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostHook {
    pub host_id: String,
    pub agent_id: String,
    pub session_id: String,
    pub event: String,
    pub at_ms: i64,
}

/// What a relayed grid stream delivers to the controller-side relay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GridEvent {
    /// One grid protocol text message from the host.
    Data(String),
    /// The host ended the stream (or the link dropped).
    Closed(String),
}

/// A grid stream opened on a host ([`RemoteHost::open_grid`]). Dropping
/// it closes the stream on the host.
pub struct RemoteGrid {
    pub stream: u64,
    pub host_id: String,
    pub events: mpsc::UnboundedReceiver<GridEvent>,
    tx: mpsc::UnboundedSender<Frame>,
    closed: bool,
}

impl RemoteGrid {
    /// Send one client text message to the host's grid handler.
    pub fn send(&self, text: String) -> bool {
        self.tx.send(Frame::GridData { stream: self.stream, text, ring_offset: None }).is_ok()
    }

    /// Close the stream on the host (idempotent).
    pub fn close(&mut self, reason: &str) {
        if !self.closed {
            self.closed = true;
            let _ = self.tx.send(Frame::GridClose { stream: self.stream, reason: reason.to_string() });
        }
    }
}

impl Drop for RemoteGrid {
    fn drop(&mut self) {
        self.close("client gone");
    }
}

/// Who waits for a request's answer: async route code or a sync
/// `AgentHost` call on a blocking thread.
enum Waiter {
    Async(oneshot::Sender<Frame>),
    Sync(std::sync::mpsc::Sender<Frame>),
}

impl Waiter {
    fn send(self, f: Frame) {
        match self {
            Waiter::Async(t) => {
                let _ = t.send(f);
            }
            Waiter::Sync(t) => {
                let _ = t.send(f);
            }
        }
    }
}

struct Link {
    conn: u64,
    host_name: String,
    /// The protocol this link speaks (`proto::negotiate`). Protocol-2
    /// frames (`Frame::min_protocol` 2) are never sent on a 1 link.
    protocol: u32,
    tx: mpsc::UnboundedSender<Frame>,
    pending: HashMap<u64, Waiter>,
    streams: HashMap<u64, mpsc::UnboundedSender<GridEvent>>,
}

#[derive(Default)]
struct Inner {
    /// Keyed by host id (protocol 1: the host's name).
    links: HashMap<String, Link>,
    /// Keyed by agent id.
    placements: HashMap<String, RemotePlacement>,
    /// The session handle for each placement whose spawn answered, keyed
    /// by agent id like `placements`.
    sessions: HashMap<String, Arc<RemoteSession>>,
    /// The host that last ran an agent in each cwd: where that project's
    /// transcripts are ([`RemoteHost::transcript_exists`]).
    last_host_for_cwd: HashMap<PathBuf, String>,
    /// The host that last ran each agent: where its home and conversation
    /// are (each host has its own). A new placement goes there while it is
    /// linked ([`RemoteHost::pick_host_for`]). Kept after the agent's
    /// session ends, and in the store across restarts (`host_affinity`).
    last_host_for_agent: HashMap<String, String>,
    /// Hosts that said they are draining (S8): no new placements there.
    draining: std::collections::HashSet<String>,
    /// Agents a draining host released, keyed by agent id: resumed when
    /// their host links again, or placed by the next need
    /// (`hosts::drain`). In the store as `host_resume`.
    resume: HashMap<String, StoredPlacement>,
}

/// Where a new placement goes and why ([`RemoteHost::pick_host_for`]).
struct Picked {
    host_id: String,
    /// `home`: the host that last ran the agent (or its cwd); `load`: none
    /// is known; `fallback`: it is known but not linked.
    affinity: &'static str,
    /// Set for `fallback`: what the caller is told.
    note: Option<String>,
}

/// An HTTP-shaped answer for the controller's routes: status line and
/// JSON body, as `HandlerResult` has them.
pub type Answer = (String, String);

fn answer(status: &str, body: Value) -> Answer {
    (status.to_string(), body.to_string())
}

/// One placement as the controller's store keeps it (W3): enough to
/// rebuild the placement and its session handle after a restart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredPlacement {
    pub agent_id: String,
    pub host_id: String,
    pub session_id: String,
    pub epoch: u64,
    pub cwd: Option<String>,
    pub program: Option<String>,
    pub args: Vec<String>,
    pub label: String,
    pub label_source: String,
}

/// The controller's placement table and epoch counter in its DB (W3: S9,
/// I1). Two tables, created on first use (`CREATE TABLE IF NOT EXISTS`;
/// k2-core's migrations don't know them): `host_placements` (one row per
/// placed agent) and `host_epoch` (the last epoch issued).
pub struct PlacementStore {
    db: Arc<parking_lot::ReentrantMutex<rusqlite::Connection>>,
}

impl PlacementStore {
    /// A store in `db` (the controller's `k2.db`, or a test's own file).
    pub fn open(db: Arc<parking_lot::ReentrantMutex<rusqlite::Connection>>) -> Result<Self, String> {
        {
            let c = db.lock();
            c.execute_batch(
                "CREATE TABLE IF NOT EXISTS host_placements (
                    agent_id TEXT PRIMARY KEY,
                    host_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    epoch INTEGER NOT NULL,
                    cwd TEXT,
                    program TEXT,
                    args_json TEXT NOT NULL DEFAULT '[]',
                    label TEXT NOT NULL DEFAULT '',
                    label_source TEXT NOT NULL DEFAULT 'pty',
                    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
                );
                CREATE TABLE IF NOT EXISTS host_epoch (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    last INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS host_resume (
                    agent_id TEXT PRIMARY KEY,
                    host_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    epoch INTEGER NOT NULL,
                    cwd TEXT,
                    program TEXT,
                    args_json TEXT NOT NULL DEFAULT '[]',
                    label TEXT NOT NULL DEFAULT '',
                    label_source TEXT NOT NULL DEFAULT 'pty',
                    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
                );
                CREATE TABLE IF NOT EXISTS host_affinity (
                    agent_id TEXT PRIMARY KEY,
                    host_id TEXT NOT NULL,
                    cwd TEXT,
                    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
                );",
            )
            .map_err(|e| format!("host placement tables: {e}"))?;
        }
        Ok(Self { db })
    }

    /// A store on its own connection to the database file at `path`. The
    /// controller's store must not share `k2_core::db::shared()`: its lock
    /// is taken while `RemoteHost::inner` is held (persist, forget) and
    /// inside `global()`'s first call (restore), and other threads hold the
    /// shared lock while they call `host()` (the active reaper does), so a
    /// shared connection deadlocks the controller's boot after a restart.
    /// Its own lock is a leaf: nothing is locked while it is held.
    pub fn open_file(path: &Path) -> Result<Self, String> {
        let conn = k2_core::db::open_private_with_resilience(path).map_err(|e| format!("open {}: {e}", path.display()))?;
        Self::open(Arc::new(parking_lot::ReentrantMutex::new(conn)))
    }

    pub fn load(&self) -> Result<Vec<StoredPlacement>, String> {
        self.load_from("host_placements")
    }

    /// The agents a drained host released, waiting to be resumed (S8).
    pub fn load_resume(&self) -> Result<Vec<StoredPlacement>, String> {
        self.load_from("host_resume")
    }

    pub fn add_resume(&self, p: &StoredPlacement) -> Result<(), String> {
        let c = self.db.lock();
        c.execute(
            "INSERT OR REPLACE INTO host_resume
               (agent_id, host_id, session_id, epoch, cwd, program, args_json, label, label_source, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, unixepoch())",
            rusqlite::params![
                p.agent_id,
                p.host_id,
                p.session_id,
                p.epoch as i64,
                p.cwd,
                p.program,
                serde_json::to_string(&p.args).unwrap_or_else(|_| "[]".into()),
                p.label,
                p.label_source
            ],
        )
        .map(drop)
        .map_err(|e| format!("record the resume of {}: {e}", p.agent_id))
    }

    pub fn remove_resume(&self, agent_id: &str) -> Result<(), String> {
        let c = self.db.lock();
        c.execute("DELETE FROM host_resume WHERE agent_id = ?1", [agent_id])
            .map(drop)
            .map_err(|e| format!("forget the resume of {agent_id}: {e}"))
    }

    fn load_from(&self, table: &str) -> Result<Vec<StoredPlacement>, String> {
        let c = self.db.lock();
        let mut st = c
            .prepare(&format!(
                "SELECT agent_id, host_id, session_id, epoch, cwd, program, args_json, label, label_source
                 FROM {table} ORDER BY agent_id"
            ))
            .map_err(|e| e.to_string())?;
        let rows = st
            .query_map([], |r| {
                let args: String = r.get(6)?;
                Ok(StoredPlacement {
                    agent_id: r.get(0)?,
                    host_id: r.get(1)?,
                    session_id: r.get(2)?,
                    epoch: r.get::<_, i64>(3)? as u64,
                    cwd: r.get(4)?,
                    program: r.get(5)?,
                    args: serde_json::from_str(&args).unwrap_or_default(),
                    label: r.get(7)?,
                    label_source: r.get(8)?,
                })
            })
            .map_err(|e| e.to_string())?;
        rows.collect::<Result<Vec<_>, _>>().map_err(|e| e.to_string())
    }

    pub fn upsert(&self, p: &StoredPlacement) -> Result<(), String> {
        let c = self.db.lock();
        c.execute(
            "INSERT INTO host_placements (agent_id, host_id, session_id, epoch, cwd, program, args_json, label, label_source, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, unixepoch())
             ON CONFLICT(agent_id) DO UPDATE SET host_id = ?2, session_id = ?3, epoch = ?4, cwd = ?5, program = ?6,
               args_json = ?7, label = ?8, label_source = ?9, updated_at = unixepoch()",
            rusqlite::params![
                p.agent_id,
                p.host_id,
                p.session_id,
                p.epoch as i64,
                p.cwd,
                p.program,
                serde_json::to_string(&p.args).unwrap_or_else(|_| "[]".into()),
                p.label,
                p.label_source
            ],
        )
        .map(drop)
        .map_err(|e| format!("store placement {}: {e}", p.agent_id))
    }

    /// Remember that `host_id` ran `agent_id` (in `cwd`): its home is there.
    /// Unlike a placement row, this outlives the session.
    pub fn remember_host(&self, agent_id: &str, host_id: &str, cwd: Option<&str>) -> Result<(), String> {
        let c = self.db.lock();
        c.execute(
            "INSERT INTO host_affinity (agent_id, host_id, cwd, updated_at) VALUES (?1, ?2, ?3, unixepoch())
             ON CONFLICT(agent_id) DO UPDATE SET host_id = ?2, cwd = ?3, updated_at = unixepoch()",
            rusqlite::params![agent_id, host_id, cwd],
        )
        .map(drop)
        .map_err(|e| format!("remember the host of {agent_id}: {e}"))
    }

    /// Every remembered (agent, host, cwd), oldest first.
    pub fn load_affinity(&self) -> Result<Vec<(String, String, Option<String>)>, String> {
        let c = self.db.lock();
        let mut st = c
            .prepare("SELECT agent_id, host_id, cwd FROM host_affinity ORDER BY updated_at, agent_id")
            .map_err(|e| e.to_string())?;
        let rows = st.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).map_err(|e| e.to_string())?;
        rows.collect::<Result<Vec<_>, _>>().map_err(|e| e.to_string())
    }

    /// Forget `agent_id`'s row, only if it is still at `epoch`.
    pub fn delete(&self, agent_id: &str, epoch: u64) -> Result<(), String> {
        let c = self.db.lock();
        c.execute("DELETE FROM host_placements WHERE agent_id = ?1 AND epoch = ?2", rusqlite::params![agent_id, epoch as i64])
            .map(drop)
            .map_err(|e| format!("forget placement {agent_id}: {e}"))
    }

    fn last_epoch(&self) -> Result<Option<u64>, String> {
        let c = self.db.lock();
        match c.query_row("SELECT last FROM host_epoch WHERE id = 1", [], |r| r.get::<_, i64>(0)) {
            Ok(n) => Ok(Some(n as u64)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.to_string()),
        }
    }

    fn set_last_epoch(&self, n: u64) -> io::Result<()> {
        let c = self.db.lock();
        c.execute(
            "INSERT INTO host_epoch (id, last) VALUES (1, ?1) ON CONFLICT(id) DO UPDATE SET last = max(last, ?1)",
            rusqlite::params![n as i64],
        )
        .map(drop)
        .map_err(|e| io::Error::other(format!("persist the placement epoch: {e}")))
    }
}

/// Placement epochs (invariant I1): a monotonic counter the controller
/// persists, so epochs only grow across controller restarts.
///
/// W3: in the controller's DB ([`PlacementStore`], `host_epoch`), written
/// before an epoch is used. W0's file (`<K2 home>/hosts/placement-epoch`) is
/// still read once, and the counter starts at the highest of the DB, the
/// file and (when neither has a value) the current Unix time in ms:
/// protocol-1 controllers used ms timestamps as epochs, so it stays above
/// any epoch a protocol-1 host may still hold. Never restarts at 0.
pub struct EpochCounter {
    path: Option<PathBuf>,
    store: Option<Arc<PlacementStore>>,
    last: Mutex<u64>,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(1)
}

fn read_epoch_file(path: &Path) -> Option<u64> {
    match std::fs::read_to_string(path) {
        Ok(t) => match t.trim().parse::<u64>() {
            Ok(n) => Some(n),
            Err(e) => {
                log_debug!("[hosts] placement epoch file {} unreadable ({e})", path.display());
                None
            }
        },
        Err(e) if e.kind() == io::ErrorKind::NotFound => None,
        Err(e) => {
            log_debug!("[hosts] placement epoch file {}: {e}", path.display());
            None
        }
    }
}

impl EpochCounter {
    /// Not persisted (tests).
    pub fn in_memory(last: u64) -> Self {
        Self { path: None, store: None, last: Mutex::new(last) }
    }

    /// Persisted in a file at `path` (W0).
    pub fn persisted(path: PathBuf) -> Self {
        let last = read_epoch_file(&path).unwrap_or_else(now_ms);
        Self { path: Some(path), store: None, last: Mutex::new(last) }
    }

    /// Persisted in the controller's DB (W3), reading W0's file at
    /// `legacy_file` once.
    pub fn in_store(store: Arc<PlacementStore>, legacy_file: Option<&Path>) -> Self {
        let db = match store.last_epoch() {
            Ok(n) => n,
            Err(e) => {
                log_debug!("[hosts] placement epoch in the DB unreadable ({e}); starting above the clock");
                None
            }
        };
        let file = legacy_file.and_then(read_epoch_file);
        let last = match (db, file) {
            (None, None) => now_ms(),
            (a, b) => a.unwrap_or(0).max(b.unwrap_or(0)),
        };
        Self { path: None, store: Some(store), last: Mutex::new(last) }
    }

    /// W0's file: `<K2 home>/hosts/placement-epoch`.
    pub fn default_path() -> PathBuf {
        k2_core::paths::k2_home().join("hosts").join("placement-epoch")
    }

    /// The next epoch, persisted before it is returned.
    pub fn next(&self) -> io::Result<u64> {
        let mut last = self.last.lock();
        let n = *last + 1;
        if let Some(store) = &self.store {
            store.set_last_epoch(n)?;
        }
        if let Some(path) = &self.path {
            let dir = path.parent().unwrap_or(Path::new("."));
            std::fs::create_dir_all(dir)?;
            let tmp = dir.join(format!(".placement-epoch.{}", uuid::Uuid::new_v4()));
            let write = || -> io::Result<()> {
                use std::io::Write;
                let mut f = std::fs::File::create(&tmp)?;
                f.write_all(n.to_string().as_bytes())?;
                f.sync_all()?;
                std::fs::rename(&tmp, path)
            };
            if let Err(e) = write() {
                let _ = std::fs::remove_file(&tmp);
                return Err(e);
            }
        }
        *last = n;
        Ok(n)
    }

    /// Never issue `epoch` or anything below it again (an adopted
    /// placement reported it; S9). Persisted on the next [`Self::next`].
    pub fn observe(&self, epoch: u64) {
        let mut last = self.last.lock();
        *last = (*last).max(epoch);
    }
}

/// What [`RemoteHost::adopt`] decided for one placement when a host linked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdoptDecision {
    /// The host still runs it at the epoch the controller holds (or the
    /// controller had no placement for the agent): live again.
    Adopted,
    /// The host runs it at an epoch the controller doesn't hold (older, or
    /// placed on another host): told to release it (I1).
    Released(String),
    /// The controller had it on this host but the host no longer runs it:
    /// the child is gone (`ChildExit`), the placement is dropped.
    Gone,
}

/// The controller's remote hosts and placements.
pub struct RemoteHost {
    inner: Mutex<Inner>,
    next_id: AtomicU64,
    epochs: EpochCounter,
    hooks: broadcast::Sender<HostHook>,
    /// W3: where placements survive a controller restart (`None`: memory).
    store: Option<Arc<PlacementStore>>,
}

impl Default for RemoteHost {
    fn default() -> Self {
        Self::new()
    }
}

impl RemoteHost {
    /// Epochs from an in-memory counter starting at 1 (tests).
    pub fn new() -> Self {
        Self::with_epochs(EpochCounter::in_memory(0))
    }

    pub fn with_epochs(epochs: EpochCounter) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            next_id: AtomicU64::new(1),
            epochs,
            hooks: broadcast::channel(1024).0,
            store: None,
        }
    }

    /// Placements and epochs in `store` (W3). Call [`Self::restore`] once
    /// the host is `'static`.
    pub fn with_store(store: Arc<PlacementStore>, legacy_epoch_file: Option<&Path>) -> Self {
        let epochs = EpochCounter::in_store(Arc::clone(&store), legacy_epoch_file);
        Self { store: Some(store), ..Self::with_epochs(epochs) }
    }

    /// Reload the stored placements after a controller restart (S9). Every
    /// one is **lost** until its host links again and [`Self::adopt`]
    /// decides: its agent may still run there, so it is neither forgotten
    /// nor placed elsewhere (I1). Returns how many were loaded.
    pub fn restore(&'static self) -> usize {
        let Some(store) = &self.store else { return 0 };
        match store.load_affinity() {
            Ok(known) => {
                let mut inner = self.inner.lock();
                for (agent, host, cwd) in known {
                    if let Some(cwd) = cwd {
                        inner.last_host_for_cwd.insert(PathBuf::from(cwd), host.clone());
                    }
                    inner.last_host_for_agent.insert(agent, host);
                }
            }
            Err(e) => log_debug!("[hosts] host affinity unreadable: {e}"),
        }
        match store.load_resume() {
            Ok(waiting) => {
                let mut inner = self.inner.lock();
                for r in waiting {
                    inner.resume.insert(r.agent_id.clone(), r);
                }
            }
            Err(e) => log_debug!("[hosts] drained agents unreadable: {e}"),
        }
        let rows = match store.load() {
            Ok(r) => r,
            Err(e) => {
                log_debug!("[hosts] placement store unreadable: {e}");
                return 0;
            }
        };
        let mut inner = self.inner.lock();
        let mut n = 0;
        for r in rows {
            let Some(sid) = SessionId::parse(&r.session_id) else { continue };
            self.epochs.observe(r.epoch);
            let epoch = PlacementEpoch(r.epoch);
            inner.placements.insert(
                r.agent_id.clone(),
                RemotePlacement {
                    agent_id: r.agent_id.clone(),
                    host_id: r.host_id.clone(),
                    session_id: Some(r.session_id.clone()),
                    epoch,
                    hooks: 0,
                },
            );
            let meta = SpawnedMeta {
                cwd: r.cwd.map(PathBuf::from),
                program: r.program,
                args: r.args,
                label: r.label,
                label_source: label_source_from_wire(&r.label_source),
            };
            if let Some(cwd) = &meta.cwd {
                inner.last_host_for_cwd.insert(cwd.clone(), r.host_id.clone());
            }
            inner.last_host_for_agent.insert(r.agent_id.clone(), r.host_id.clone());
            let s = Arc::new(RemoteSession::new(self, &r.agent_id, &r.host_id, epoch, sid, meta));
            s.cached.lock().lost = true;
            inner.sessions.insert(r.agent_id.clone(), s);
            n += 1;
        }
        log_debug!("[hosts] restored {n} placement(s), lost until their hosts link");
        n
    }

    fn stored(s: &RemoteSession) -> StoredPlacement {
        let (label, label_source) = {
            let c = s.cached.lock();
            (c.label.clone(), label_source_wire(c.label_source).to_string())
        };
        StoredPlacement {
            agent_id: s.agent_id.clone(),
            host_id: s.host_id.clone(),
            session_id: s.session_id.to_string(),
            epoch: s.epoch.0,
            cwd: s.cwd.as_ref().map(|p| p.to_string_lossy().into_owned()),
            program: s.program.clone(),
            args: s.args.clone(),
            label,
            label_source,
        }
    }

    fn persist(&self, s: &RemoteSession) {
        let Some(store) = &self.store else { return };
        let row = Self::stored(s);
        if let Err(e) = store.upsert(&row) {
            log_debug!("[hosts] {e}");
        }
    }

    fn forget(&self, agent_id: &str, epoch: PlacementEpoch) {
        if let Some(store) = &self.store {
            if let Err(e) = store.delete(agent_id, epoch.0) {
                log_debug!("[hosts] {e}");
            }
        }
    }

    fn id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    // ── links ──────────────────────────────────────────────────────────

    /// Register a host that completed the handshake. Returns the link's
    /// connection id, the host id and the frames to write to it. A link
    /// already open under the same name is told `duplicate` and dropped.
    pub fn connect(&self, host_name: &str, protocol: u32) -> (u64, String, mpsc::UnboundedReceiver<Frame>) {
        let conn = self.id();
        let host_id = host_name.to_string();
        let (tx, rx) = mpsc::unbounded_channel();
        let old = {
            let mut inner = self.inner.lock();
            inner.links.insert(
                host_id.clone(),
                Link { conn, host_name: host_name.to_string(), protocol, tx, pending: HashMap::new(), streams: HashMap::new() },
            )
        };
        if let Some(old) = old {
            let _ = old.tx.send(Frame::Refused {
                code: "duplicate".into(),
                message: format!("a newer link for host {host_id} replaced this one"),
            });
            Self::fail_link(old);
        }
        log_debug!("[hosts] host {host_id} linked (conn {conn})");
        k2_core::trace::emit(|| k2_core::trace::Event::new("ctl.host_linked").attr("host", host_id.as_str()));
        (conn, host_id, rx)
    }

    /// The link `conn` closed: fail what waits on it and forget its
    /// placements (the agents may still run on the host; re-adopting them
    /// is S9). A stale `conn` (already replaced) changes nothing.
    pub fn disconnect(&self, host_id: &str, conn: u64) {
        let link = {
            let mut inner = self.inner.lock();
            if inner.links.get(host_id).map(|l| l.conn) != Some(conn) {
                return;
            }
            let protocol = inner.links.get(host_id).map(|l| l.protocol).unwrap_or(1);
            if protocol >= 2 {
                // W3: the host's agents may still run. Keep every placement,
                // marked lost, until the host links again and resumes them
                // (adopt) or doesn't (gone). Nothing is placed elsewhere.
                for s in inner.sessions.values().filter(|s| s.host_id == host_id) {
                    s.cached.lock().lost = true;
                }
                log_debug!(
                    "[hosts] host {host_id} lost: {} placement(s) kept until it links again",
                    inner.placements.values().filter(|p| p.host_id == host_id).count()
                );
            } else {
                // A protocol-1 host can't resume: forget, as before W3.
                inner.placements.retain(|_, p| p.host_id != host_id);
                let gone: Vec<String> =
                    inner.sessions.iter().filter(|(_, s)| s.host_id == host_id).map(|(a, _)| a.clone()).collect();
                for agent in gone {
                    if let Some(s) = inner.sessions.remove(&agent) {
                        s.mark_exited();
                    }
                }
            }
            inner.draining.remove(host_id);
            inner.links.remove(host_id)
        };
        if let Some(link) = link {
            Self::fail_link(link);
        }
        log_debug!("[hosts] host {host_id} unlinked (conn {conn})");
        k2_core::trace::emit(|| k2_core::trace::Event::new("ctl.host_unlinked").attr("host", host_id));
    }

    fn fail_link(link: Link) {
        // Dropping the reply senders fails every waiting request.
        drop(link.pending);
        for (_, s) in link.streams {
            let _ = s.send(GridEvent::Closed("host link closed".into()));
        }
    }

    /// One frame from host `host_id` on link `conn`.
    pub fn on_frame(&self, host_id: &str, conn: u64, frame: Frame) {
        let mut inner = self.inner.lock();
        if inner.links.get(host_id).map(|l| l.conn) != Some(conn) {
            return;
        }
        match frame {
            reply @ (Frame::Reply { req, .. } | Frame::OpResult { req, .. } | Frame::InjectResult { req, .. }) => {
                if let Some(waiter) = inner.links.get_mut(host_id).and_then(|l| l.pending.remove(&req)) {
                    waiter.send(reply);
                }
            }
            Frame::GridData { stream, text, .. } => {
                if let Some(s) = inner.links.get(host_id).and_then(|l| l.streams.get(&stream)) {
                    let _ = s.send(GridEvent::Data(text));
                }
            }
            Frame::GridClose { stream, reason } => {
                if let Some(s) = inner.links.get_mut(host_id).and_then(|l| l.streams.remove(&stream)) {
                    let _ = s.send(GridEvent::Closed(reason));
                }
            }
            Frame::Hook { pane, event, at_ms, epoch, .. } => {
                // Only a session this controller placed on this host, at
                // the placement's epoch when the host sent one.
                let hit = inner.placements.values_mut().find(|p| {
                    p.host_id == host_id && p.session_id.as_deref() == Some(pane.as_str())
                });
                match hit {
                    Some(p) if epoch.is_none_or(|e| e == p.epoch.0) => {
                        p.hooks += 1;
                        let hook = HostHook {
                            host_id: host_id.to_string(),
                            agent_id: p.agent_id.clone(),
                            session_id: pane.clone(),
                            event: event.clone(),
                            at_ms,
                        };
                        log_debug!("[hosts] hook {event} from host {host_id} agent {} session {pane}", p.agent_id);
                        k2_core::trace::emit(|| {
                            k2_core::trace::Event::new("ctl.hook").pane(&pane).attr("host", host_id).attr("event", event.as_str())
                        });
                        let _ = self.hooks.send(hook);
                    }
                    _ => log_debug!("[hosts] dropped hook {event} from host {host_id}: session {pane} is not placed there at that epoch"),
                }
            }
            Frame::OpenUrl { url, source } => {
                drop(inner);
                log_debug!("[hosts] open_url from host {host_id} ({source})");
                k2_core::trace::emit(|| k2_core::trace::Event::new("ctl.open_url").attr("host", host_id));
                let _ = crate::session_events::emit(crate::session_events::SessionEvent::OpenUrl { url, source });
            }
            Frame::SessionGone { agent_id, epoch, exit_code } => {
                let stale = match inner.placements.get(&agent_id) {
                    Some(p) => p.host_id != host_id || epoch.is_some_and(|e| e != p.epoch.0),
                    None => true,
                };
                if !stale {
                    if let Some(p) = inner.placements.remove(&agent_id) {
                        self.forget(&agent_id, p.epoch);
                    }
                    if let Some(s) = inner.sessions.remove(&agent_id) {
                        s.exited(exit_code);
                    }
                    log_debug!("[hosts] agent {agent_id} gone on host {host_id}");
                }
            }
            Frame::Refused { code, message } if code == crate::hosts::drain::DRAINING_CODE => {
                log_debug!("[hosts] host {host_id} is draining: {message}");
                inner.draining.insert(host_id.to_string());
                k2_core::trace::emit(|| k2_core::trace::Event::new(crate::hosts::drain::events::CTL_HOST_DRAINING).attr("host", host_id));
            }
            Frame::Released { req: 0, agent_id, .. } => self.released_by_drain(&mut inner, host_id, &agent_id),
            Frame::Released { agent_id, epoch, .. } => {
                log_debug!("[hosts] host {host_id} released agent {agent_id} at epoch {epoch}");
            }
            Frame::Deliver { .. } | Frame::DeliverAck { .. } => {
                drop(inner);
                crate::delivery_drain::on_host_frame(self, host_id, frame);
            }
            other => log_debug!("[hosts] ignored frame {} from host {host_id}", other.kind()),
        }
    }

    /// Send `frame` to `host_id` on its link, with no reply expected (the
    /// outbox's `deliver` and `deliver.ack`). Refused when the host isn't
    /// linked or its link speaks an older protocol than the frame needs.
    pub fn send_frame(&self, host_id: &str, frame: Frame) -> Result<(), String> {
        let inner = self.inner.lock();
        let Some(link) = inner.links.get(host_id) else {
            return Err(format!("agent host {host_id} is not connected"));
        };
        if link.protocol < frame.min_protocol() {
            return Err(format!(
                "agent host {host_id} speaks protocol {}; {} needs {}",
                link.protocol,
                frame.kind(),
                frame.min_protocol()
            ));
        }
        link.tx.send(frame).map_err(|_| format!("agent host {host_id} link is closing"))
    }

    // ── requests ───────────────────────────────────────────────────────

    /// Send the request `make(req)` to `host_id` and wait for its reply.
    async fn request(&self, host_id: &str, make: impl FnOnce(u64) -> Frame) -> Result<Answer, Answer> {
        let req = self.id();
        let (tx, rx) = oneshot::channel();
        {
            let mut inner = self.inner.lock();
            let Some(link) = inner.links.get_mut(host_id) else {
                return Err(answer("502 Bad Gateway", json!({"error": lost_or_unknown(host_id)})));
            };
            link.pending.insert(req, Waiter::Async(tx));
            if link.tx.send(make(req)).is_err() {
                link.pending.remove(&req);
                return Err(answer("502 Bad Gateway", json!({"error": format!("agent host {host_id} link is closing")})));
            }
        }
        match tokio::time::timeout(REPLY_DEADLINE, rx).await {
            Ok(Ok(Frame::Reply { status, body, .. })) => Ok((status, body)),
            Ok(Ok(other)) => Err(answer(
                "502 Bad Gateway",
                json!({"error": format!("agent host {host_id} answered with {}", other.kind())}),
            )),
            Ok(Err(_)) => Err(answer(
                "502 Bad Gateway",
                json!({"error": format!("agent host {host_id} disconnected before it answered")}),
            )),
            Err(_) => {
                if let Some(link) = self.inner.lock().links.get_mut(host_id) {
                    link.pending.remove(&req);
                }
                Err(answer("504 Gateway Timeout", json!({"error": format!("agent host {host_id} did not answer")})))
            }
        }
    }

    /// The host for a new placement: the connected host with the fewest
    /// placements (ties by name).
    fn pick_host(inner: &Inner) -> Option<String> {
        let mut counts: Vec<(usize, &String)> = inner
            .links
            .keys()
            .filter(|h| !inner.draining.contains(*h))
            .map(|h| (inner.placements.values().filter(|p| &p.host_id == h).count(), h))
            .collect();
        counts.sort();
        counts.first().map(|(_, h)| (*h).clone())
    }

    /// The host for `agent_id`'s new placement. Each host has its own home,
    /// so the agent's conversation and memory are on the host that last ran
    /// it: that host while it is linked, else the linked host that last ran
    /// its cwd (that project's transcripts). Otherwise [`Self::pick_host`]
    /// by load, and when the agent's own host is known but not linked, a
    /// note saying its conversation can't resume where it lands (S7).
    fn pick_host_for(inner: &Inner, agent_id: &str, cwd: Option<&Path>) -> Option<Picked> {
        let own = inner.last_host_for_agent.get(agent_id).cloned();
        let linked = |h: &&String| inner.links.contains_key(*h) && !inner.draining.contains(*h);
        let preferred = own.as_ref().filter(linked).or_else(|| cwd.and_then(|c| inner.last_host_for_cwd.get(c)).filter(linked));
        if let Some(h) = preferred {
            return Some(Picked { host_id: h.clone(), affinity: "home", note: None });
        }
        let host_id = Self::pick_host(inner)?;
        // Only the agent's own home host being away is worth a word; a cwd
        // other agents ran is a preference, not a home.
        Some(match own {
            None => Picked { host_id, affinity: "load", note: None },
            Some(h) => {
                let note = format!(
                    "agent {agent_id}'s home is on host {h}, which is not linked; placed on {host_id} by load, \
                     where its conversation and memory are not"
                );
                log_debug!("[hosts] {note}");
                Picked { host_id, affinity: "fallback", note: Some(note) }
            }
        })
    }

    /// `POST /cli/sessions/v2/spawn` on a controller. Find-or-spawn by
    /// `agent_name` as today: an agent already placed goes back to its
    /// host (which answers `reused: true`); a new one goes to
    /// [`Self::pick_host`] at a new epoch.
    pub async fn spawn(&'static self, body: &[u8]) -> Answer {
        let parsed: Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => {
                return answer(
                    "400 Bad Request",
                    json!({"error": format!("parse v2 SpawnRequest: {}", e.to_string().replace('"', "'"))}),
                )
            }
        };
        let agent_id = parsed.get("agent_name").and_then(Value::as_str).unwrap_or("").to_string();
        if agent_id.is_empty() {
            return answer("400 Bad Request", json!({"error": "agent_name required"}));
        }
        let cwd = parsed.get("cwd").and_then(Value::as_str).map(PathBuf::from);
        let mut placement_note = None;
        let (host_id, epoch) = {
            let mut inner = self.inner.lock();
            match inner.placements.get(&agent_id) {
                Some(p) => (p.host_id.clone(), p.epoch),
                None => {
                    let Some(picked) = Self::pick_host_for(&inner, &agent_id, cwd.as_deref()) else {
                        log_debug!("[hosts] spawn {agent_id} refused: no host connected");
                        return answer("503 Service Unavailable", json!({"error": NO_HOSTS}));
                    };
                    let epoch = match self.epochs.next() {
                        Ok(e) => PlacementEpoch(e),
                        Err(e) => {
                            log_debug!("[hosts] spawn {agent_id} refused: can't persist the placement epoch: {e}");
                            return answer(
                                "500 Internal Server Error",
                                json!({"error": format!("can't persist the placement epoch: {e}")}),
                            );
                        }
                    };
                    k2_core::trace::emit(|| {
                        k2_core::trace::Event::new(trace_events::CTL_EPOCH).agent(agent_id.as_str()).attr("epoch", epoch.0)
                    });
                    let host_id = picked.host_id;
                    Self::trace_affinity(&agent_id, &host_id, picked.affinity, picked.note.as_deref());
                    placement_note = picked.note;
                    // Reserve the placement now so a concurrent spawn of the
                    // same agent lands on the same host.
                    inner.placements.insert(
                        agent_id.clone(),
                        RemotePlacement { agent_id: agent_id.clone(), host_id: host_id.clone(), session_id: None, epoch, hooks: 0 },
                    );
                    (host_id, epoch)
                }
            }
        };
        k2_core::trace::emit(|| {
            k2_core::trace::Event::new("ctl.place").agent(agent_id.as_str()).attr("host", host_id.as_str()).attr("epoch", epoch.0)
        });
        let meta = SpawnedMeta {
            cwd: parsed.get("cwd").and_then(Value::as_str).map(PathBuf::from),
            program: parsed.get("command").and_then(Value::as_str).filter(|c| !c.is_empty()).map(str::to_string),
            args: parsed
                .get("args")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
                .unwrap_or_default(),
            label: String::new(),
            label_source: LabelSource::Pty,
        };
        let result = self
            .request(&host_id, |req| Frame::Spawn { req, agent_id: agent_id.clone(), epoch: epoch.0, body: parsed })
            .await;
        let mut inner = self.inner.lock();
        let ours = |p: &RemotePlacement| p.host_id == host_id && p.epoch == epoch;
        match result {
            Ok((status, body)) if status.starts_with('2') => {
                let session = serde_json::from_str::<Value>(&body)
                    .ok()
                    .and_then(|v| v.get("sessionId").and_then(Value::as_str).map(str::to_string));
                if let Some(p) = inner.placements.get_mut(&agent_id).filter(|p| ours(p)) {
                    p.session_id = session.clone();
                    if let Some(sid) = session.as_deref().and_then(SessionId::parse) {
                        self.keep_session(&mut inner, &agent_id, &host_id, epoch, sid, meta);
                    }
                }
                log_debug!(
                    "[hosts] placed agent {agent_id} on host {host_id} epoch {} session {}",
                    epoch.0,
                    session.as_deref().unwrap_or("?")
                );
                // Say so in the answer when the agent's home host was away.
                let body = match (placement_note, serde_json::from_str::<Value>(&body)) {
                    (Some(note), Ok(Value::Object(mut m))) => {
                        m.insert("placementNote".into(), Value::String(note));
                        Value::Object(m).to_string()
                    }
                    _ => body,
                };
                (status, body)
            }
            Ok((status, body)) => {
                if inner.placements.get(&agent_id).is_some_and(|p| ours(p) && p.session_id.is_none()) {
                    inner.placements.remove(&agent_id);
                }
                (status, body)
            }
            Err(e) => {
                if inner.placements.get(&agent_id).is_some_and(|p| ours(p) && p.session_id.is_none()) {
                    inner.placements.remove(&agent_id);
                }
                e
            }
        }
    }

    /// `POST /cli/sessions/v2/close` on a controller: forwarded to the
    /// agent's host. An agent placed nowhere answers as today's close of
    /// an unknown agent does.
    pub async fn close(&self, body: &[u8]) -> Answer {
        let parsed: Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => {
                return answer(
                    "400 Bad Request",
                    json!({"error": format!("parse v2 CloseRequest: {}", e.to_string().replace('"', "'"))}),
                )
            }
        };
        let Some(agent_id) = parsed.get("agent_name").and_then(Value::as_str).map(str::to_string) else {
            return answer("400 Bad Request", json!({"error": "parse v2 CloseRequest: missing field `agent_name`"}));
        };
        let placed = self.inner.lock().placements.get(&agent_id).map(|p| (p.host_id.clone(), p.epoch));
        let Some((host_id, epoch)) = placed else {
            let cleared = parsed.get("clear_index").and_then(Value::as_bool).unwrap_or(false);
            return answer("200 OK", json!({"closed": false, "indexCleared": cleared}));
        };
        let result = self
            .request(&host_id, |req| Frame::Close { req, agent_id: agent_id.clone(), epoch: epoch.0, body: parsed })
            .await;
        match result {
            Ok((status, body)) => {
                let closed = serde_json::from_str::<Value>(&body)
                    .ok()
                    .and_then(|v| v.get("closed").and_then(Value::as_bool))
                    .unwrap_or(false);
                if closed {
                    let mut inner = self.inner.lock();
                    if inner.placements.get(&agent_id).is_some_and(|p| p.epoch == epoch) {
                        inner.placements.remove(&agent_id);
                        self.forget(&agent_id, epoch);
                        if let Some(s) = inner.sessions.remove(&agent_id) {
                            s.mark_exited();
                        }
                    }
                }
                (status, body)
            }
            Err(e) => e,
        }
    }

    // ── the workspace spawn and the trait's transport (W1) ─────────────

    /// The handle for `agent_id`'s session `sid`: the existing one when the
    /// host answered with the same session (reuse), a new one otherwise.
    fn keep_session(
        &'static self,
        inner: &mut Inner,
        agent_id: &str,
        host_id: &str,
        epoch: PlacementEpoch,
        sid: SessionId,
        meta: SpawnedMeta,
    ) -> Arc<RemoteSession> {
        if let Some(s) = inner.sessions.get(agent_id).filter(|s| s.session_id == sid) {
            return Arc::clone(s);
        }
        if let Some(cwd) = &meta.cwd {
            inner.last_host_for_cwd.insert(cwd.clone(), host_id.to_string());
        }
        inner.last_host_for_agent.insert(agent_id.to_string(), host_id.to_string());
        if inner.resume.remove(agent_id).is_some() {
            if let Some(store) = &self.store {
                if let Err(e) = store.remove_resume(agent_id) {
                    log_debug!("[hosts] {e}");
                }
            }
        }
        if let Some(store) = &self.store {
            let cwd = meta.cwd.as_ref().map(|c| c.to_string_lossy().into_owned());
            if let Err(e) = store.remember_host(agent_id, host_id, cwd.as_deref()) {
                log_debug!("[hosts] {e}");
            }
        }
        let s = Arc::new(RemoteSession::new(self, agent_id, host_id, epoch, sid, meta));
        if let Some(old) = inner.sessions.insert(agent_id.to_string(), Arc::clone(&s)) {
            old.mark_exited();
        }
        self.persist(&s);
        s
    }

    /// Reserve `agent_id`'s placement: its current host and epoch, or a
    /// new placement on [`Self::pick_host`] at a new epoch.
    fn reserve(&self, agent_id: &str, cwd: Option<&Path>) -> Result<(String, PlacementEpoch), String> {
        let mut inner = self.inner.lock();
        if let Some(p) = inner.placements.get(agent_id) {
            return Ok((p.host_id.clone(), p.epoch));
        }
        let picked = Self::pick_host_for(&inner, agent_id, cwd).ok_or_else(|| NO_HOSTS.to_string())?;
        let epoch = PlacementEpoch(self.epochs.next().map_err(|e| format!("can't persist the placement epoch: {e}"))?);
        k2_core::trace::emit(|| k2_core::trace::Event::new(trace_events::CTL_EPOCH).agent(agent_id).attr("epoch", epoch.0));
        let host_id = picked.host_id;
        Self::trace_affinity(agent_id, &host_id, picked.affinity, picked.note.as_deref());
        inner.placements.insert(
            agent_id.to_string(),
            RemotePlacement { agent_id: agent_id.to_string(), host_id: host_id.clone(), session_id: None, epoch, hooks: 0 },
        );
        Ok((host_id, epoch))
    }

    /// `ctl.place_affinity`: why a new placement went where it did.
    fn trace_affinity(agent_id: &str, host_id: &str, affinity: &str, note: Option<&str>) {
        k2_core::trace::emit(|| {
            let e = k2_core::trace::Event::new("ctl.place_affinity")
                .agent(agent_id)
                .attr("host", host_id)
                .attr("affinity", affinity);
            match note {
                Some(n) => e.attr("note", n),
                None => e,
            }
        });
    }

    /// Drop a reservation whose spawn failed (only if it is still ours and
    /// never got a session).
    fn unreserve(&self, agent_id: &str, host_id: &str, epoch: PlacementEpoch) {
        let mut inner = self.inner.lock();
        if inner
            .placements
            .get(agent_id)
            .is_some_and(|p| p.host_id == host_id && p.epoch == epoch && p.session_id.is_none())
        {
            inner.placements.remove(agent_id);
        }
    }

    /// The workspace spawn (`spawn::spawn_agent_session_v2_blocking` on a
    /// controller): find-or-spawn under the request's canonical key, run
    /// whole on the agent's host. Blocks until the host answered.
    pub(crate) fn spawn_workspace_blocking(
        &'static self,
        req: &crate::spawn::SpawnWorkspaceSessionRequest,
    ) -> Result<(SpawnedSession, AgentSession), String> {
        let agent_id = crate::spawn::canonical_key_for_request(req);
        let (host_id, epoch) = self.reserve(&agent_id, Some(Path::new(&req.cwd)))?;
        let request = serde_json::to_value(req).map_err(|e| format!("encode the spawn request: {e}"))?;
        k2_core::trace::emit(|| {
            k2_core::trace::Event::new("ctl.place").agent(agent_id.as_str()).attr("host", host_id.as_str()).attr("epoch", epoch.0)
        });
        let reply = self.request_blocking(
            &host_id,
            |r| Frame::SpawnWorkspace { req: r, agent_id: agent_id.clone(), epoch: epoch.0, request },
            REPLY_DEADLINE,
        );
        let (status, body) = match reply {
            Ok(Frame::Reply { status, body, .. }) => (status, body),
            Ok(other) => {
                self.unreserve(&agent_id, &host_id, epoch);
                return Err(format!("agent host {host_id} answered the spawn with {}", other.kind()));
            }
            Err(e) => {
                self.unreserve(&agent_id, &host_id, epoch);
                return Err(e);
            }
        };
        if !status.starts_with('2') {
            self.unreserve(&agent_id, &host_id, epoch);
            let why = serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_string))
                .unwrap_or(body);
            return Err(format!("agent host {host_id}: {why}"));
        }
        let spawned: SpawnedSession =
            serde_json::from_str(&body).map_err(|e| format!("agent host {host_id} spawn reply: {e}"))?;
        let sid = SessionId::parse(&spawned.session_id)
            .ok_or_else(|| format!("agent host {host_id} answered a bad session id {:?}", spawned.session_id))?;
        let meta = SpawnedMeta {
            cwd: spawned.cwd.clone().map(PathBuf::from),
            program: spawned.program.clone(),
            args: spawned.args.clone(),
            label: spawned.label.clone(),
            label_source: label_source_from_wire(&spawned.label_source),
        };
        let session = {
            let mut inner = self.inner.lock();
            match inner.placements.get_mut(&agent_id) {
                Some(p) if p.host_id == host_id && p.epoch == epoch => p.session_id = Some(spawned.session_id.clone()),
                // Closed while the spawn was in flight: answer, don't place.
                _ => return Err(format!("agent {agent_id} was unplaced while its spawn ran")),
            }
            self.keep_session(&mut inner, &agent_id, &host_id, epoch, sid, meta)
        };
        log_debug!(
            "[hosts] placed agent {agent_id} on host {host_id} epoch {} session {} (workspace spawn, reused={})",
            epoch.0,
            spawned.session_id,
            spawned.reused
        );
        Ok((spawned, AgentSession::remote(session)))
    }

    /// Send `make(req)` to `host_id` and wait, blocking this thread, for
    /// the frame that answers it. For the sync `AgentHost` methods; inside
    /// a multi-thread runtime the wait runs under `block_in_place`.
    fn request_blocking(&self, host_id: &str, make: impl FnOnce(u64) -> Frame, deadline: Duration) -> Result<Frame, String> {
        let req = self.id();
        let (tx, rx) = std::sync::mpsc::channel();
        {
            let mut inner = self.inner.lock();
            let link = inner.links.get_mut(host_id).ok_or_else(|| lost_or_unknown(host_id))?;
            let frame = make(req);
            if frame.min_protocol() > link.protocol {
                return Err(format!(
                    "agent host {host_id} speaks protocol {}; {} needs {} (update k2 on the host)",
                    link.protocol,
                    frame.kind(),
                    frame.min_protocol()
                ));
            }
            link.pending.insert(req, Waiter::Sync(tx));
            if link.tx.send(frame).is_err() {
                link.pending.remove(&req);
                return Err(format!("agent host {host_id} link is closing"));
            }
        }
        let wait = || rx.recv_timeout(deadline);
        let got = match tokio::runtime::Handle::try_current() {
            Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => tokio::task::block_in_place(wait),
            Ok(_) => {
                if let Some(link) = self.inner.lock().links.get_mut(host_id) {
                    link.pending.remove(&req);
                }
                return Err("a sync host request can't wait inside a current-thread runtime".into());
            }
            Err(_) => wait(),
        };
        match got {
            Ok(f) => Ok(f),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                Err(format!("agent host {host_id} disconnected before it answered"))
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if let Some(link) = self.inner.lock().links.get_mut(host_id) {
                    link.pending.remove(&req);
                }
                Err(format!("agent host {host_id} did not answer within {deadline:?}"))
            }
        }
    }

    /// One session op, waiting for its value.
    fn op(&self, s: &RemoteSession, op: SessionOp) -> Result<Value, String> {
        let kind = format!("{op:?}");
        let session_id = s.session_id.to_string();
        let reply = self.request_blocking(
            &s.host_id,
            |req| Frame::Op { req: Some(req), session_id, epoch: s.epoch.0, op },
            OP_DEADLINE,
        )?;
        match reply {
            Frame::OpResult { error: None, value, .. } => Ok(value),
            Frame::OpResult { error: Some(e), .. } => Err(e),
            other => Err(format!("op {kind} answered with {}", other.kind())),
        }
    }

    /// One session op, not waiting (write, resize). Ops on one link run on
    /// the host in the order they were sent.
    fn op_send(&self, s: &RemoteSession, op: SessionOp) -> io::Result<()> {
        let inner = self.inner.lock();
        let link = inner
            .links
            .get(&s.host_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, format!("agent host {} is not connected", s.host_id)))?;
        if link.protocol < 2 {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("agent host {} speaks protocol 1 (update k2 on the host)", s.host_id),
            ));
        }
        link.tx
            .send(Frame::Op { req: None, session_id: s.session_id.to_string(), epoch: s.epoch.0, op })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, format!("agent host {} link is closing", s.host_id)))
    }

    /// The resume decision's disk check, asked of the hosts (the
    /// controller's `k2_core::transcript_probe`). The host that last ran an
    /// agent in this project (or a worktree of it) is asked; when none is
    /// known, every linked protocol-2 host. `Some(true)` if any holds the
    /// conversation, `Some(false)` if every one asked said no, `None` if
    /// none could answer (the caller then checks this machine, as before).
    pub fn transcript_exists(&self, provider: &str, session_id: &str, project_path: &str) -> Option<bool> {
        let hosts: Vec<String> = {
            let inner = self.inner.lock();
            let project = Path::new(project_path);
            let mut known: Vec<String> = inner
                .last_host_for_cwd
                .iter()
                .filter(|(cwd, _)| cwd.starts_with(project) || project.starts_with(cwd))
                .map(|(_, h)| h.clone())
                .filter(|h| inner.links.contains_key(h))
                .collect();
            known.sort();
            known.dedup();
            if known.is_empty() {
                inner.links.iter().filter(|(_, l)| l.protocol >= 2).map(|(h, _)| h.clone()).collect()
            } else {
                known
            }
        };
        let mut answered = false;
        for host in hosts {
            let r = self.request_blocking(
                &host,
                |req| Frame::TranscriptExists {
                    req,
                    provider: provider.to_string(),
                    session_id: session_id.to_string(),
                    project_path: project_path.to_string(),
                },
                OP_DEADLINE,
            );
            match r {
                Ok(Frame::OpResult { value: Value::Bool(found), error: None, .. }) => {
                    answered = true;
                    if found {
                        log_debug!("[hosts] {provider} conversation {session_id} is on host {host}");
                        return Some(true);
                    }
                }
                Ok(other) => log_debug!("[hosts] transcript probe on {host}: {other:?}"),
                Err(e) => log_debug!("[hosts] transcript probe on {host} failed: {e}"),
            }
        }
        answered.then_some(false)
    }

    fn session_for(&self, agent_id: &str) -> Option<Arc<RemoteSession>> {
        self.inner.lock().sessions.get(agent_id).cloned()
    }

    // ── grid ───────────────────────────────────────────────────────────

    /// Open a relayed grid stream on the host that runs `session_id`.
    pub fn open_grid(&self, session_id: &str, proto: Option<String>, as_user: Option<String>) -> Result<RemoteGrid, String> {
        let stream = self.id();
        let mut inner = self.inner.lock();
        let Some(p) = inner.placements.values().find(|p| p.session_id.as_deref() == Some(session_id)).cloned() else {
            return Err(format!("session {session_id} is not placed on any agent host"));
        };
        let Some(link) = inner.links.get_mut(&p.host_id) else {
            return Err(format!(
                "agent host {} is lost (its link is down); the session stays placed there until it returns",
                p.host_id
            ));
        };
        let (etx, erx) = mpsc::unbounded_channel();
        link.streams.insert(stream, etx);
        let tx = link.tx.clone();
        let open = Frame::GridOpen { stream, session_id: session_id.to_string(), epoch: p.epoch.0, proto, as_user, ring_offset: None };
        if tx.send(open).is_err() {
            link.streams.remove(&stream);
            return Err(format!("agent host {} link is closing", p.host_id));
        }
        Ok(RemoteGrid { stream, host_id: p.host_id, events: erx, tx, closed: false })
    }

    // ── protocol 2 stubs (wave 2) ──────────────────────────────────────

    /// A draining host gave up `agent_id` (S8): drop the placement and
    /// keep what's needed to resume it. Its `session.gone` comes later and
    /// finds no placement.
    fn released_by_drain(&self, inner: &mut Inner, host_id: &str, agent_id: &str) {
        let Some(p) = inner.placements.get(agent_id).filter(|p| p.host_id == host_id).cloned() else {
            log_debug!("[hosts] host {host_id} released {agent_id}, which it doesn't hold; ignored");
            return;
        };
        inner.placements.remove(agent_id);
        self.forget(agent_id, p.epoch);
        let Some(s) = inner.sessions.remove(agent_id) else { return };
        let row = Self::stored(&s);
        if let Some(store) = &self.store {
            if let Err(e) = store.add_resume(&row) {
                log_debug!("[hosts] {e}");
            }
        }
        inner.resume.insert(agent_id.to_string(), row);
        s.exited(None);
        log_debug!("[hosts] host {host_id} drained agent {agent_id}: resumed when the host is back or on the next need");
        k2_core::trace::emit(|| {
            k2_core::trace::Event::new(crate::hosts::drain::events::CTL_DRAIN_RELEASE).agent(agent_id).attr("host", host_id)
        });
    }

    /// True while `agent_id` was drained and is not placed again yet: its
    /// messages wait in the outbox.
    pub fn awaiting_resume(&self, agent_id: &str) -> bool {
        self.inner.lock().resume.contains_key(agent_id)
    }

    /// The drained agents whose host is linked (and not draining) now.
    pub fn resume_due(&self) -> Vec<StoredPlacement> {
        let inner = self.inner.lock();
        inner
            .resume
            .values()
            .filter(|r| inner.links.contains_key(&r.host_id) && !inner.draining.contains(&r.host_id))
            .filter(|r| !inner.placements.contains_key(&r.agent_id))
            .cloned()
            .collect()
    }

    /// The protocol the link to `host_id` speaks, if it is linked.
    pub fn link_protocol(&self, host_id: &str) -> Option<u32> {
        self.inner.lock().links.get(host_id).map(|l| l.protocol)
    }

    /// A protocol-2 host linked: reconcile its `resume` (the placements it
    /// still runs) with the table (S9, I1). Per agent:
    /// - same host, same epoch → [`AdoptDecision::Adopted`] (live again);
    /// - the controller has no placement for it → adopted, and its epoch is
    ///   observed so new epochs stay above it;
    /// - another epoch (older), or placed on another host →
    ///   [`AdoptDecision::Released`]: the host is told to release it;
    /// - placed on this host but not resumed → [`AdoptDecision::Gone`]: the
    ///   child is gone (`ChildExit`), the placement and its row dropped.
    pub fn adopt(&'static self, host_id: &str, resume: &[ResumePlacement]) -> Vec<(String, AdoptDecision)> {
        let mut out = Vec::new();
        let mut releases = Vec::new();
        {
            let mut inner = self.inner.lock();
            for r in resume {
                let decision = match inner.placements.get(&r.agent_id).cloned() {
                    Some(p) if p.host_id == host_id && p.epoch.0 == r.epoch => {
                        if let Some(s) = inner.sessions.get(&r.agent_id) {
                            let mut c = s.cached.lock();
                            c.lost = false;
                            c.alive = r.child_alive;
                        }
                        AdoptDecision::Adopted
                    }
                    Some(p) if p.host_id == host_id && r.epoch > p.epoch.0 => {
                        // Can't be issued by this controller; a newer one's.
                        // Take it, like an unknown placement.
                        self.adopt_unknown(&mut inner, host_id, r);
                        AdoptDecision::Adopted
                    }
                    Some(p) => AdoptDecision::Released(format!(
                        "controller holds agent {} at epoch {} on host {}; host resumed epoch {}",
                        r.agent_id, p.epoch.0, p.host_id, r.epoch
                    )),
                    None => {
                        self.adopt_unknown(&mut inner, host_id, r);
                        AdoptDecision::Adopted
                    }
                };
                if let AdoptDecision::Released(why) = &decision {
                    releases.push((r.agent_id.clone(), r.epoch, why.clone()));
                }
                out.push((r.agent_id.clone(), decision));
            }
            // Placed here but not resumed: gone while the host was away.
            let gone: Vec<(String, PlacementEpoch)> = inner
                .placements
                .values()
                .filter(|p| p.host_id == host_id && p.session_id.is_some() && !resume.iter().any(|r| r.agent_id == p.agent_id))
                .map(|p| (p.agent_id.clone(), p.epoch))
                .collect();
            for (agent, epoch) in gone {
                inner.placements.remove(&agent);
                self.forget(&agent, epoch);
                if let Some(s) = inner.sessions.remove(&agent) {
                    s.exited(None);
                }
                out.push((agent, AdoptDecision::Gone));
            }
            // Reach the host after the table is settled.
            if let Some(link) = inner.links.get(host_id) {
                for (agent_id, epoch, reason) in &releases {
                    let _ = link.tx.send(Frame::Release {
                        req: self.id(),
                        agent_id: agent_id.clone(),
                        epoch: *epoch,
                        reason: format!("stale placement: {reason}"),
                    });
                }
            }
        }
        for (agent, d) in &out {
            log_debug!("[hosts] host {host_id}: agent {agent} {d:?}");
            let decision = match d {
                AdoptDecision::Adopted => "adopted",
                AdoptDecision::Released(_) => "released",
                AdoptDecision::Gone => "gone",
            };
            k2_core::trace::emit(|| {
                k2_core::trace::Event::new(trace_events::CTL_ADOPT).agent(agent.as_str()).attr("host", host_id).attr("decision", decision)
            });
        }
        out
    }

    fn adopt_unknown(&'static self, inner: &mut Inner, host_id: &str, r: &ResumePlacement) {
        let Some(sid) = SessionId::parse(&r.session_id) else { return };
        self.epochs.observe(r.epoch);
        let epoch = PlacementEpoch(r.epoch);
        inner.placements.insert(
            r.agent_id.clone(),
            RemotePlacement {
                agent_id: r.agent_id.clone(),
                host_id: host_id.to_string(),
                session_id: Some(r.session_id.clone()),
                epoch,
                hooks: 0,
            },
        );
        let meta = SpawnedMeta {
            cwd: r.cwd.clone().map(PathBuf::from),
            program: r.program.clone(),
            args: r.args.clone(),
            label: r.label.clone().unwrap_or_default(),
            label_source: LabelSource::Locked,
        };
        let s = self.keep_session(inner, &r.agent_id, host_id, epoch, sid, meta);
        s.cached.lock().alive = r.child_alive;
    }

    // ── reads ──────────────────────────────────────────────────────────

    pub fn placement(&self, agent_id: &str) -> Option<RemotePlacement> {
        self.inner.lock().placements.get(agent_id).cloned()
    }

    pub fn placements(&self) -> Vec<RemotePlacement> {
        self.inner.lock().placements.values().cloned().collect()
    }

    pub fn hook_events(&self) -> broadcast::Receiver<HostHook> {
        self.hooks.subscribe()
    }

    /// `GET /cli/hosts/status` on a controller (no secrets).
    pub fn status_json(&self) -> Value {
        let inner = self.inner.lock();
        let mut hosts: Vec<Value> = inner
            .links
            .iter()
            .map(|(id, l)| {
                let placed: Vec<&RemotePlacement> = inner.placements.values().filter(|p| &p.host_id == id).collect();
                json!({
                    "host_id": id,
                    "host_name": l.host_name,
                    "placements": placed.len(),
                    "hooks": placed.iter().map(|p| p.hooks).sum::<u64>(),
                    "draining": inner.draining.contains(id),
                })
            })
            .collect();
        hosts.sort_by(|a, b| a["host_id"].as_str().cmp(&b["host_id"].as_str()));
        let mut lost: Vec<&String> =
            inner.placements.values().map(|p| &p.host_id).filter(|h| !inner.links.contains_key(*h)).collect();
        lost.sort();
        lost.dedup();
        let mut resuming: Vec<&String> = inner.resume.keys().collect();
        resuming.sort();
        json!({"role": "controller", "hosts": hosts, "lost": lost, "resuming": resuming})
    }
}

fn lost_or_unknown(host_id: &str) -> String {
    format!("agent host {host_id} is not connected (lost: its agents keep their placement there until it links again)")
}

/// What a spawn told the controller about a session's fixed metadata.
pub(crate) struct SpawnedMeta {
    pub cwd: Option<PathBuf>,
    pub program: Option<String>,
    pub args: Vec<String>,
    pub label: String,
    pub label_source: LabelSource,
}

pub(crate) fn label_source_wire(l: LabelSource) -> &'static str {
    match l {
        LabelSource::Pty => "pty",
        LabelSource::Seed => "seed",
        LabelSource::Locked => "locked",
    }
}

pub(crate) fn label_source_from_wire(s: &str) -> LabelSource {
    match s {
        "seed" => LabelSource::Seed,
        "locked" => LabelSource::Locked,
        _ => LabelSource::Pty,
    }
}

struct Cached {
    alive: bool,
    /// W3: its host's link is down; its agent may still run there.
    lost: bool,
    label: String,
    label_source: LabelSource,
    ever_attached: bool,
}

/// A session placed on a remote host, as the controller sees it (the
/// remote half of `AgentSession`). Fixed metadata comes from its spawn;
/// liveness from `session.gone` (and link loss); everything else is asked
/// of the host.
pub struct RemoteSession {
    remote: &'static RemoteHost,
    pub agent_id: String,
    pub host_id: String,
    pub session_id: SessionId,
    pub epoch: PlacementEpoch,
    pub cwd: Option<PathBuf>,
    pub program: Option<String>,
    pub args: Vec<String>,
    cached: Mutex<Cached>,
    events: broadcast::Sender<AlacEvent>,
    labels: broadcast::Sender<String>,
    pins: broadcast::Sender<Option<(u16, u16)>>,
}

impl RemoteSession {
    fn new(
        remote: &'static RemoteHost,
        agent_id: &str,
        host_id: &str,
        epoch: PlacementEpoch,
        session_id: SessionId,
        meta: SpawnedMeta,
    ) -> Self {
        Self {
            remote,
            agent_id: agent_id.to_string(),
            host_id: host_id.to_string(),
            session_id,
            epoch,
            cwd: meta.cwd,
            program: meta.program,
            args: meta.args,
            cached: Mutex::new(Cached { alive: true, lost: false, label: meta.label, label_source: meta.label_source, ever_attached: false }),
            events: broadcast::channel(64).0,
            labels: broadcast::channel(16).0,
            pins: broadcast::channel(16).0,
        }
    }

    pub fn is_child_alive(&self) -> bool {
        self.cached.lock().alive
    }

    pub(crate) fn mark_exited(&self) {
        self.cached.lock().alive = false;
    }

    /// The child exited on the host: liveness off, and `ChildExit` to
    /// every events subscriber (code 0 when the host didn't say).
    fn exited(&self, code: Option<i32>) {
        self.mark_exited();
        let _ = self.events.send(AlacEvent::ChildExit(exit_status(code.unwrap_or(0))));
    }

    pub fn label(&self) -> String {
        self.cached.lock().label.clone()
    }

    pub fn label_source(&self) -> LabelSource {
        self.cached.lock().label_source
    }

    pub fn set_label(&self, label: String, lock: bool) -> String {
        let now = match self.remote.op(self, SessionOp::SetLabel { label: label.clone(), lock }) {
            Ok(v) => v.as_str().map(str::to_string).unwrap_or(label),
            Err(e) => {
                log_debug!("[hosts] set_label on {} failed: {e}", self.session_id);
                return self.label();
            }
        };
        {
            let mut c = self.cached.lock();
            c.label = now.clone();
            if lock {
                c.label_source = LabelSource::Locked;
            }
        }
        let _ = self.labels.send(now.clone());
        now
    }

    pub fn kill(&self) {
        if let Err(e) = self.remote.op(self, SessionOp::Kill) {
            log_debug!("[hosts] kill {} on host {} failed: {e}", self.session_id, self.host_id);
        }
    }

    /// The host's live state, refreshing the cached label.
    fn meta(&self) -> Option<SessionMeta> {
        match self.remote.op(self, SessionOp::Meta).and_then(|v| serde_json::from_value::<SessionMeta>(v).map_err(|e| e.to_string())) {
            Ok(m) => {
                let mut c = self.cached.lock();
                c.label = m.label.clone();
                c.label_source = label_source_from_wire(&m.label_source);
                c.ever_attached |= m.ever_attached;
                if !m.alive {
                    c.alive = false;
                }
                Some(m)
            }
            Err(e) => {
                log_debug!("[hosts] meta of {} on host {} failed: {e}", self.session_id, self.host_id);
                None
            }
        }
    }

    pub fn bracketed_paste_active(&self) -> bool {
        self.meta().is_some_and(|m| m.bracketed_paste)
    }

    pub fn subscriber_count(&self) -> usize {
        self.meta().map(|m| m.subscribers).unwrap_or(0)
    }

    pub fn ever_attached(&self) -> bool {
        self.meta();
        self.cached.lock().ever_attached
    }

    pub fn visible_text_rows(&self) -> Vec<String> {
        self.rows(SessionOp::VisibleRows)
    }

    fn rows(&self, op: SessionOp) -> Vec<String> {
        match self.remote.op(self, op).and_then(|v| serde_json::from_value::<Vec<String>>(v).map_err(|e| e.to_string())) {
            Ok(rows) => rows,
            Err(e) => {
                log_debug!("[hosts] screen read of {} on host {} failed: {e}", self.session_id, self.host_id);
                Vec::new()
            }
        }
    }

    fn dims(&self, op: SessionOp) -> (u16, u16) {
        self.remote
            .op(self, op)
            .ok()
            .and_then(|v| serde_json::from_value::<(u16, u16)>(v).ok())
            .unwrap_or((0, 0))
    }
}

#[cfg(unix)]
fn exit_status(code: i32) -> std::process::ExitStatus {
    use std::os::unix::process::ExitStatusExt;
    std::process::ExitStatus::from_raw((code & 0xff) << 8)
}

#[cfg(windows)]
fn exit_status(code: i32) -> std::process::ExitStatus {
    use std::os::windows::process::ExitStatusExt;
    std::process::ExitStatus::from_raw(code as u32)
}

fn remote_of(session: &AgentSession) -> &Arc<RemoteSession> {
    session.as_remote().expect("the controller's host only hands out remote sessions")
}

/// The controller's `AgentHost`: every agent on a remote host.
impl AgentHost for RemoteHost {
    fn kind(&self) -> &'static str {
        "remote"
    }

    /// Epochs are per placement here ([`AgentSession::epoch`]); this is
    /// the last one issued.
    fn epoch(&self) -> PlacementEpoch {
        PlacementEpoch(*self.epochs.last.lock())
    }

    /// The low-level spawn has no remote form: the workspace spawn and the
    /// v2 spawn routes place agents on hosts whole (`spawn.workspace`,
    /// `spawn`), with the hook token, cell socket and observer on the host.
    fn spawn(&self, _config: DaemonPtyConfig) -> io::Result<AgentSession> {
        Err(io::Error::new(io::ErrorKind::Unsupported, super::in_process::CONTROLLER_RUNS_NO_AGENTS))
    }

    fn place(&self, agent_id: &str, session: &AgentSession) -> Placement {
        let s = remote_of(session);
        let mut inner = self.inner.lock();
        inner.placements.insert(
            agent_id.to_string(),
            RemotePlacement {
                agent_id: agent_id.to_string(),
                host_id: s.host_id.clone(),
                session_id: Some(s.session_id.to_string()),
                epoch: s.epoch,
                hooks: 0,
            },
        );
        inner.sessions.insert(agent_id.to_string(), Arc::clone(s));
        Placement { agent_id: agent_id.to_string(), session_id: s.session_id, epoch: s.epoch }
    }

    /// Remove the placement and close it on the host (kill, no index
    /// clearing), as the in-process unplace does.
    fn unplace(&self, agent_id: &str) -> Option<AgentSession> {
        let (placement, session) = {
            let mut inner = self.inner.lock();
            (inner.placements.remove(agent_id), inner.sessions.remove(agent_id))
        };
        let placement = placement?;
        self.forget(agent_id, placement.epoch);
        let body = json!({"agent_name": agent_id, "force": true});
        let r = self.request_blocking(
            &placement.host_id,
            |req| Frame::Close { req, agent_id: agent_id.to_string(), epoch: placement.epoch.0, body },
            REPLY_DEADLINE,
        );
        if let Err(e) = r {
            log_debug!("[hosts] unplace {agent_id}: close on host {} failed: {e}", placement.host_id);
        }
        session.map(|s| {
            s.mark_exited();
            AgentSession::remote(s)
        })
    }

    fn find_agent(&self, agent_id: &str) -> Option<AgentSession> {
        self.session_for(agent_id).map(AgentSession::remote)
    }

    fn find_session(&self, session_id: &SessionId) -> Option<AgentSession> {
        let inner = self.inner.lock();
        inner.sessions.values().find(|s| &s.session_id == session_id).cloned().map(AgentSession::remote)
    }

    fn placements(&self) -> Vec<(String, AgentSession)> {
        let inner = self.inner.lock();
        inner.sessions.iter().map(|(a, s)| (a.clone(), AgentSession::remote(Arc::clone(s)))).collect()
    }

    /// Events, labels and pins from the controller's view of the session;
    /// the relayed grid stream is the host's viewer, so nothing registers.
    fn attach(&self, session: &AgentSession) -> Attachment {
        let s = remote_of(session);
        Attachment { events: s.events.subscribe(), labels: s.labels.subscribe(), pins: s.pins.subscribe(), _viewer: None }
    }

    fn detach(&self, _session: &AgentSession, _subscriber_id: u64) {}

    /// The grid pipeline runs on the host; a controller relays its frames
    /// (`hosts::hub::serve_grid`) and never reaches this.
    fn stream(&self, session: &AgentSession, _pane_id: &str, _wants_k1: bool) -> GridStream {
        let s = remote_of(session);
        panic!("grid stream of remote session {} requested on the controller (relayed by hosts::hub instead)", s.session_id)
    }

    fn events(&self, session: &AgentSession) -> broadcast::Receiver<AlacEvent> {
        remote_of(session).events.subscribe()
    }

    fn mark_exited(&self, session: &AgentSession) {
        remote_of(session).mark_exited();
    }

    fn read_screen(&self, session: &AgentSession, lines: usize) -> Vec<String> {
        remote_of(session).rows(SessionOp::Screen { lines })
    }

    fn write(&self, session: &AgentSession, bytes: Vec<u8>) -> io::Result<()> {
        let s = remote_of(session);
        let op = match String::from_utf8(bytes) {
            Ok(text) => SessionOp::Write { text: Some(text), b64: None },
            Err(e) => {
                use base64::Engine;
                SessionOp::Write { text: None, b64: Some(base64::engine::general_purpose::STANDARD.encode(e.as_bytes())) }
            }
        };
        self.op_send(s, op)
    }

    fn inject(&self, session: &AgentSession, steps: Vec<k2_core::inject_flow::InjectAction>) -> InjectResult {
        let s = remote_of(session);
        let steps = steps.iter().map(InjectStep::from_action).collect();
        let r = self.request_blocking(
            &s.host_id,
            |req| Frame::Inject { req, agent_id: s.agent_id.clone(), epoch: s.epoch.0, steps, msg_id: None },
            OP_DEADLINE,
        );
        match r {
            Ok(Frame::InjectResult { result: InjectOutcome::Delivered, .. }) => InjectResult::Delivered,
            Ok(other) => {
                log_debug!("[hosts] inject into {} on host {}: {other:?}", s.session_id, s.host_id);
                InjectResult::ChildGone
            }
            Err(e) => {
                log_debug!("[hosts] inject into {} on host {} failed: {e}", s.session_id, s.host_id);
                InjectResult::ChildGone
            }
        }
    }

    fn resize(&self, session: &AgentSession, cols: u16, rows: u16) {
        let s = remote_of(session);
        if let Err(e) = self.op_send(s, SessionOp::Resize { cols, rows }) {
            log_debug!("[hosts] resize {} failed: {e}", s.session_id);
        }
    }

    fn resize_now(&self, session: &AgentSession, cols: u16, rows: u16) -> (u16, u16) {
        remote_of(session).dims(SessionOp::ResizeNow { cols, rows })
    }

    fn size(&self, session: &AgentSession) -> (u16, u16) {
        remote_of(session).dims(SessionOp::Size)
    }

    fn claimed_size(&self, session: &AgentSession) -> (u16, u16) {
        remote_of(session).dims(SessionOp::ClaimedSize)
    }

    fn set_claimed_size(&self, session: &AgentSession, cols: u16, rows: u16) {
        let s = remote_of(session);
        if let Err(e) = self.op_send(s, SessionOp::SetClaimedSize { cols, rows }) {
            log_debug!("[hosts] set_claimed_size {} failed: {e}", s.session_id);
        }
    }

    fn pin(&self, session: &AgentSession, dims: Option<(u16, u16)>, set_by: Option<String>) {
        let s = remote_of(session);
        match self.op(s, SessionOp::Pin { dims, set_by }) {
            Ok(_) => {
                let _ = s.pins.send(dims);
            }
            Err(e) => log_debug!("[hosts] pin {} failed: {e}", s.session_id),
        }
    }

    fn set_label(&self, session: &AgentSession, label: String, lock: bool) -> String {
        remote_of(session).set_label(label, lock)
    }

    fn kill(&self, session: &AgentSession) {
        remote_of(session).kill();
    }

    /// Host hook envelopes are not yet replayed into this controller's
    /// hook plane (the activity store reads `hook_ingest` directly); hook
    /// frames arrive on [`RemoteHost::hook_events`]. A receiver that only
    /// closes.
    fn hook_events(&self) -> broadcast::Receiver<crate::hook_ingest::IngestEvent> {
        broadcast::channel(1).1
    }

    /// The agent's home lives on its host (FR6 moves it); not readable
    /// from the controller in this build.
    fn read_home(&self, _home: &AgentHome, rel: &Path) -> io::Result<Vec<u8>> {
        Err(io::Error::new(io::ErrorKind::Unsupported, format!("agent home on a remote host: {}", rel.display())))
    }

    fn write_home(&self, _home: &AgentHome, rel: &Path, _bytes: &[u8]) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::Unsupported, format!("agent home on a remote host: {}", rel.display())))
    }
}

/// The controller's one [`RemoteHost`].
pub fn global() -> &'static RemoteHost {
    static REMOTE: OnceLock<RemoteHost> = OnceLock::new();
    static RESTORED: std::sync::Once = std::sync::Once::new();
    let store = || {
        let path = k2_core::db::shared_path().ok_or_else(|| "the daemon database is not open".to_string())?;
        PlacementStore::open_file(&path)
    };
    let r = REMOTE.get_or_init(|| match store() {
        Ok(store) => RemoteHost::with_store(Arc::new(store), Some(&EpochCounter::default_path())),
        Err(e) => {
            log_debug!("[hosts] placement store unavailable ({e}); placements won't survive a restart");
            RemoteHost::with_epochs(EpochCounter::persisted(EpochCounter::default_path()))
        }
    });
    RESTORED.call_once(|| {
        r.restore();
    });
    r
}
