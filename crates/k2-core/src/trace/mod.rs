//! Trace events for the K2 lab (PRD "K2 Distributed Runtime", section 3,
//! rule 4: "The diagrams are the tests").
//!
//! Off unless `K2_TRACE_FILE` is set. It must be an absolute path the
//! daemon can append to; anything else is an error ([`boot_check`]), never
//! a silent "off". When it is set, every emission
//! point appends one JSON line to that file, in the lab's trace format
//! (`docs/prd/progress/lab.md`, "Trace format"):
//!
//! ```json
//! {"ts":"2026-11-14T09:12:03.123Z","event":"host.spawn","src":"k2-daemon",
//!  "agent_id":"a1b2","msg_id":null,"host":"local","session_id":"…",
//!  "epoch":null,"attrs":{"harness":"claude","pane":"…"}}
//! ```
//!
//! The lab's checker (`tests/lab/labkit/flows.py`) matches these lines
//! against the flows in `docs/prd/flows/*.json`.
//!
//! Cost when off: [`on`] is one atomic load (the setting is read once), and
//! [`emit`] takes a closure that is never called, so an emission point
//! builds nothing and allocates nothing. When on, the caller formats
//! nothing either: it stamps the time and hands the event to a bounded
//! queue with `try_send`, and a writer thread serializes and appends it. A
//! full queue drops the event (counted, reported as `trace.dropped`); the
//! caller, which may be a PTY or hook thread, never waits on the file.
//!
//! Correlation: `agent_id` is the spawn's `agent_name`. The HTTP spawn
//! entry opens a [`spawn_request`] scope on its thread; the PTY spawn under
//! it records pane (terminal session id) → agent, and every later event
//! that names the pane ([`Event::pane`]) carries that agent, including
//! hook events, whose envelope names only the pane.

pub mod pty;

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, OnceLock};

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::{Map, Value};

/// The setting: an absolute path. Unset or empty is off; anything else is
/// a [`ConfigError`].
pub const ENV_VAR: &str = "K2_TRACE_FILE";
/// `src` of every event this process writes.
pub const SRC: &str = "k2-daemon";
/// `host` in solo: the in-process agent host.
pub const HOST: &str = "local";
/// Events the writer may hold before emission points start dropping.
const QUEUE_CAP: usize = 16_384;

/// A `K2_TRACE_FILE` that is set but unusable. Never silently "off": the
/// lab would then report missing flows for a misleading reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    NotAbsolute(String),
    Open { path: String, error: String },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotAbsolute(v) => write!(f, "{ENV_VAR}={v:?} is not an absolute path"),
            Self::Open { path, error } => write!(f, "{ENV_VAR}={path:?} cannot be opened for append: {error}"),
        }
    }
}

impl std::error::Error for ConfigError {}

/// The trace file a `K2_TRACE_FILE` value names. Unset or empty is
/// `Ok(None)` (off); a relative path is an error.
pub fn config_from(value: Option<OsString>) -> Result<Option<PathBuf>, ConfigError> {
    let Some(value) = value else { return Ok(None) };
    let path = PathBuf::from(value);
    if path.as_os_str().is_empty() {
        return Ok(None);
    }
    if !path.is_absolute() {
        return Err(ConfigError::NotAbsolute(path.to_string_lossy().into_owned()));
    }
    Ok(Some(path))
}

fn global_state() -> &'static Result<Option<Sink>, ConfigError> {
    static SINK: OnceLock<Result<Option<Sink>, ConfigError>> = OnceLock::new();
    SINK.get_or_init(|| {
        let state = config_from(std::env::var_os(ENV_VAR)).and_then(|path| match path {
            None => Ok(None),
            Some(path) => Sink::open(&path).map(Some).map_err(|e| ConfigError::Open {
                path: path.to_string_lossy().into_owned(),
                error: e.to_string(),
            }),
        });
        if let Err(e) = &state {
            // Loud: stderr (the daemon log) and the debug log, once.
            eprintln!("[trace] ERROR: {e}; no trace will be written");
            crate::log_debug!("[trace] ERROR: {e}; no trace will be written");
        }
        state
    })
}

fn global() -> Option<&'static Sink> {
    match global_state() {
        Ok(Some(sink)) => Some(sink),
        _ => None,
    }
}

/// Boot check: returns the error for a set but unusable `K2_TRACE_FILE`
/// (relative, or not openable for append), so the daemon can refuse to
/// start. Unset is `Ok(())`. It opens and closes the file but starts no
/// thread (the daemon calls it before any thread exists); the sink itself
/// starts on the first emission.
pub fn boot_check() -> Result<(), ConfigError> {
    check_from(std::env::var_os(ENV_VAR))
}

/// [`boot_check`] on an explicit value.
pub fn check_from(value: Option<OsString>) -> Result<(), ConfigError> {
    let Some(path) = config_from(value)? else { return Ok(()) };
    OpenOptions::new().create(true).append(true).open(&path).map(drop).map_err(|e| ConfigError::Open {
        path: path.to_string_lossy().into_owned(),
        error: e.to_string(),
    })
}

/// True when this process writes a trace.
#[inline]
pub fn on() -> bool {
    global().is_some()
}

/// Emit the event `build` makes. `build` runs only when tracing is on.
#[inline]
pub fn emit(build: impl FnOnce() -> Event) {
    if let Some(sink) = global() {
        sink.send(build());
    }
}

/// One trace event, before it is stamped and written.
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub event: &'static str,
    pub agent_id: Option<String>,
    pub msg_id: Option<String>,
    pub session_id: Option<String>,
    pub attrs: Map<String, Value>,
}

impl Event {
    pub fn new(event: &'static str) -> Self {
        Self { event, agent_id: None, msg_id: None, session_id: None, attrs: Map::new() }
    }

    pub fn agent(mut self, agent_id: impl Into<String>) -> Self {
        self.agent_id = Some(agent_id.into());
        self
    }

    /// The provider's session id (Claude's `--session-id`), when known.
    pub fn session(mut self, session_id: Option<String>) -> Self {
        if session_id.is_some() {
            self.session_id = session_id;
        }
        self
    }

    pub fn msg(mut self, msg_id: impl Into<String>) -> Self {
        self.msg_id = Some(msg_id.into());
        self
    }

    /// The terminal session (`attrs.pane`). Fills `agent_id` and
    /// `session_id` from what the pane's spawn recorded, unless set.
    pub fn pane(mut self, pane: &str) -> Self {
        if let Some(c) = correlation(pane) {
            if self.agent_id.is_none() {
                self.agent_id = c.agent_id;
            }
            if self.session_id.is_none() {
                self.session_id = c.session_id;
            }
        }
        self.attrs.insert("pane".into(), Value::String(pane.to_string()));
        self
    }

    pub fn attr(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.attrs.insert(key.to_string(), value.into());
        self
    }
}

/// One line of the trace file. Field order is the lab's.
#[derive(Serialize)]
struct Line<'a> {
    ts: String,
    event: &'a str,
    src: &'a str,
    agent_id: Option<&'a str>,
    msg_id: Option<&'a str>,
    host: &'a str,
    session_id: Option<&'a str>,
    epoch: Option<u64>,
    attrs: &'a Map<String, Value>,
}

/// `2026-11-14T09:12:03.123Z`: RFC 3339, UTC, milliseconds.
pub fn format_ts(at: DateTime<Utc>) -> String {
    at.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

/// The JSON line (no newline) for `ev` stamped `at`.
pub fn format_line(ev: &Event, at: DateTime<Utc>) -> String {
    let line = Line {
        ts: format_ts(at),
        event: ev.event,
        src: SRC,
        agent_id: ev.agent_id.as_deref(),
        msg_id: ev.msg_id.as_deref(),
        host: HOST,
        session_id: ev.session_id.as_deref(),
        epoch: None,
        attrs: &ev.attrs,
    };
    serde_json::to_string(&line).expect("a trace line is plain JSON")
}

enum Msg {
    Event(Box<Event>, DateTime<Utc>),
    Flush(SyncSender<()>),
}

/// An open trace file and its writer thread.
pub struct Sink {
    tx: SyncSender<Msg>,
    dropped: Arc<AtomicU64>,
}

impl Sink {
    /// Open (create, append) `path` and start the writer thread.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        Self::open_with_capacity(path, QUEUE_CAP)
    }

    fn open_with_capacity(path: &Path, cap: usize) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let (tx, rx) = mpsc::sync_channel(cap);
        let dropped = Arc::new(AtomicU64::new(0));
        let counter = Arc::clone(&dropped);
        std::thread::Builder::new()
            .name("k2-trace-writer".into())
            .spawn(move || write_loop(file, rx, counter))?;
        Ok(Self { tx, dropped })
    }

    /// Stamp `ev` now and queue it. Never blocks: a full queue drops it.
    pub fn send(&self, ev: Event) {
        match self.tx.try_send(Msg::Event(Box::new(ev), Utc::now())) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Wait until every event queued before this call is in the file.
    pub fn flush(&self) {
        let (ack_tx, ack_rx) = mpsc::sync_channel(1);
        if self.tx.send(Msg::Flush(ack_tx)).is_ok() {
            let _ = ack_rx.recv();
        }
    }

    /// Events dropped because the queue was full.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// One `write` per line on an `O_APPEND` file, so lines from several
/// writers (threads, or the fake agent's process) never interleave.
fn write_loop(mut file: File, rx: Receiver<Msg>, dropped: Arc<AtomicU64>) {
    let mut reported = 0u64;
    for msg in rx {
        match msg {
            Msg::Event(ev, at) => {
                let now_dropped = dropped.load(Ordering::Relaxed);
                if now_dropped > reported {
                    let note = Event::new("trace.dropped").attr("count", now_dropped - reported);
                    write_line(&mut file, &note, at);
                    reported = now_dropped;
                }
                write_line(&mut file, &ev, at);
            }
            Msg::Flush(ack) => {
                let _ = file.flush();
                let _ = ack.send(());
            }
        }
    }
}

fn write_line(file: &mut File, ev: &Event, at: DateTime<Utc>) {
    let mut line = format_line(ev, at);
    line.push('\n');
    if let Err(e) = file.write_all(line.as_bytes()) {
        crate::log_debug!("[trace] write failed: {e}");
    }
}

// ── Correlation ────────────────────────────────────────────────────────

/// What a pane's spawn knew about it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Correlation {
    pub agent_id: Option<String>,
    pub session_id: Option<String>,
}

fn panes() -> &'static Mutex<HashMap<String, Correlation>> {
    static PANES: OnceLock<Mutex<HashMap<String, Correlation>>> = OnceLock::new();
    PANES.get_or_init(Default::default)
}

/// Record what `pane` belongs to (only while tracing is on).
pub fn correlate(pane: &str, c: Correlation) {
    if on() {
        panes().lock().insert(pane.to_string(), c);
    }
}

/// What `pane`'s spawn recorded, when tracing is on.
pub fn correlation(pane: &str) -> Option<Correlation> {
    if !on() {
        return None;
    }
    panes().lock().get(pane).cloned()
}

thread_local! {
    static SPAWN_AGENT: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// The agent a [`spawn_request`] scope on this thread is spawning.
pub fn current_spawn_agent() -> Option<String> {
    if !on() {
        return None;
    }
    SPAWN_AGENT.with(|a| a.borrow().clone())
}

/// Ends a [`spawn_request`] scope on drop.
#[must_use = "the scope ends when the guard drops"]
pub struct SpawnScope {
    active: bool,
}

impl Drop for SpawnScope {
    fn drop(&mut self) {
        if self.active {
            SPAWN_AGENT.with(|a| *a.borrow_mut() = None);
        }
    }
}

/// The HTTP spawn entry point: emits `api.spawn_request` and, until the
/// guard drops, tags PTY spawns on this thread with `agent_name`.
pub fn spawn_request(agent_name: &str, command: Option<&str>) -> SpawnScope {
    if !on() {
        return SpawnScope { active: false };
    }
    emit(|| {
        Event::new("api.spawn_request")
            .agent(agent_name)
            .attr("command", command.map(str::to_string))
            .attr("need", command.is_none())
    });
    SPAWN_AGENT.with(|a| *a.borrow_mut() = Some(agent_name.to_string()));
    SpawnScope { active: true }
}

/// The HTTP attach entry point (`route`: `grid`, `bytes`, …).
pub fn attach(pane: Option<&str>, route: &'static str) {
    if !on() {
        return;
    }
    if let Some(p) = pane {
        pty::note_attached(p);
    }
    emit(|| {
        let ev = Event::new("api.attach").attr("route", route);
        match pane {
            Some(p) => ev.pane(p),
            None => ev,
        }
    });
}

#[cfg(test)]
mod tests;
