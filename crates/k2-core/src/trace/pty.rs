//! Trace emission for `DaemonPtySession` (`terminal/daemon_pty.rs`). Each
//! function is one emission point there; all of them return at once when
//! tracing is off.
//!
//! In solo the in-process agent host and PTY host are the daemon, so the
//! flows' `host.*` and `ptyd.*` steps are emitted here, from `k2-daemon`.

use std::collections::HashSet;
use std::sync::OnceLock;

use parking_lot::Mutex;
use tokio::sync::broadcast;

use super::{correlate, current_spawn_agent, emit, on, Correlation, Event};
use crate::session::SessionId;
use crate::terminal::daemon_pty::DaemonPtyConfig;
use crate::terminal::AlacEvent;

fn attached() -> &'static Mutex<HashSet<String>> {
    static ATTACHED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    ATTACHED.get_or_init(Default::default)
}

/// Exit ordering per pane. The watcher (`ptyd.child_exit`) and the daemon's
/// exit observer (`mark_child_exited`, `host.agent_exit`) hear the same
/// broadcast on different threads; the session is marked dead *because*
/// the child exited, so `host.agent_exit` is written after `ptyd.child_exit`.
#[derive(Default)]
struct Exit {
    child_exit_written: HashSet<String>,
    dead_pending: HashSet<String>,
}

fn exits() -> &'static Mutex<Exit> {
    static EXITS: OnceLock<Mutex<Exit>> = OnceLock::new();
    EXITS.get_or_init(Default::default)
}

/// A client attached to `pane` (the HTTP attach entry point).
pub(super) fn note_attached(pane: &str) {
    attached().lock().insert(pane.to_string());
}

fn was_attached(pane: &str) -> bool {
    attached().lock().contains(pane)
}

/// `host.spawn`: the in-process host takes a spawn. Records the pane's
/// agent (from the HTTP spawn scope on this thread) and provider session.
pub fn host_spawn(cfg: &DaemonPtyConfig) {
    if !on() {
        return;
    }
    let pane = cfg.session_id.to_string();
    let program = cfg.program.as_deref().unwrap_or("");
    let args = cfg.durable_args.as_ref().unwrap_or(&cfg.args);
    let session_id = crate::workspace::provider_resume::session_id_from_spawn_argv(program, args);
    let resume = args.iter().any(|a| a == "--resume" || a == "-r");
    correlate(&pane, Correlation { agent_id: current_spawn_agent(), session_id: session_id.clone() });
    emit(|| {
        let harness = std::path::Path::new(program)
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        Event::new("host.spawn")
            .pane(&pane)
            .session(session_id)
            .attr("harness", harness)
            .attr("resume", resume)
            .attr("cols", cfg.cols)
            .attr("rows", cfg.rows)
    });
}

/// `ptyd.open_session`: the host asks for a terminal (`backend`:
/// `in-process` or `ptyd`). Emitted before the child is forked.
pub fn open_session(pane: &SessionId, cols: u16, rows: u16, backend: &'static str) {
    emit(|| {
        Event::new("ptyd.open_session")
            .pane(&pane.to_string())
            .attr("cols", cols)
            .attr("rows", rows)
            .attr("backend", backend)
    });
}

/// `pty.spawned`: the child exists (its pid).
pub fn spawned(pane: &SessionId, pid: Option<i32>) {
    emit(|| Event::new("pty.spawned").pane(&pane.to_string()).attr("pid", pid));
}

/// `pty.input`: bytes written to the child (the count, never the bytes).
pub fn input(pane: &SessionId, len: usize) {
    emit(|| Event::new("pty.input").pane(&pane.to_string()).attr("bytes", len));
}

/// `host.agent_exit`: the session is marked dead. Held until the watcher
/// has written the child's exit, if it has not yet.
pub fn marked_dead(pane: &SessionId) {
    if !on() {
        return;
    }
    let pane = pane.to_string();
    let mut ex = exits().lock();
    if ex.child_exit_written.remove(&pane) {
        emit(|| Event::new("host.agent_exit").pane(&pane));
    } else {
        ex.dead_pending.insert(pane);
    }
}

/// The watcher saw the child exit (`status`) or the stream close (`None`).
fn child_exited(pane: &str, status: Option<std::process::ExitStatus>) {
    let mut ex = exits().lock();
    if let Some(status) = status {
        emit(|| child_exit_event(pane, status));
    }
    if ex.dead_pending.remove(pane) {
        emit(|| Event::new("host.agent_exit").pane(pane));
    } else if status.is_some() {
        ex.child_exit_written.insert(pane.to_string());
    }
}

/// `pty.kill`: the daemon kills and reaps the child.
pub fn kill(pane: &SessionId, pid: Option<i32>) {
    emit(|| Event::new("pty.kill").pane(&pane.to_string()).attr("pid", pid));
}

/// Watch a session's event stream on a thread of its own (never the PTY
/// read loop): `pty.first_output` on its first output, `client.stream` on
/// the first output after a client attached, `ptyd.child_exit` on exit.
pub fn watch(pane: &SessionId, events: &broadcast::Sender<AlacEvent>) {
    if !on() {
        return;
    }
    let rx = events.subscribe();
    let pane = pane.to_string();
    let spawned = std::thread::Builder::new()
        .name("k2-trace-pty".into())
        .spawn(move || watch_loop(pane, rx));
    if let Err(e) = spawned {
        crate::log_debug!("[trace] cannot start the PTY watcher: {e}");
    }
}

fn watch_loop(pane: String, mut rx: broadcast::Receiver<AlacEvent>) {
    let mut first_output = false;
    let mut streamed = false;
    loop {
        match rx.blocking_recv() {
            Ok(AlacEvent::Wakeup) => {
                if !first_output {
                    first_output = true;
                    emit(|| Event::new("pty.first_output").pane(&pane));
                }
                if !streamed && was_attached(&pane) {
                    streamed = true;
                    emit(|| Event::new("client.stream").pane(&pane));
                }
            }
            Ok(AlacEvent::ChildExit(status)) => {
                child_exited(&pane, Some(status));
                attached().lock().remove(&pane);
                return;
            }
            Ok(_) => {}
            Err(broadcast::error::RecvError::Lagged(_)) => {}
            Err(broadcast::error::RecvError::Closed) => {
                child_exited(&pane, None);
                attached().lock().remove(&pane);
                return;
            }
        }
    }
}

/// `ptyd.child_exit` for `status`: `code` or `signal`, and `clean`
/// (exit code 0).
pub fn child_exit_event(pane: &str, status: std::process::ExitStatus) -> Event {
    #[cfg(unix)]
    let signal = std::os::unix::process::ExitStatusExt::signal(&status);
    #[cfg(not(unix))]
    let signal: Option<i32> = None;
    Event::new("ptyd.child_exit")
        .pane(pane)
        .attr("code", status.code())
        .attr("signal", signal)
        .attr("clean", status.success())
}
