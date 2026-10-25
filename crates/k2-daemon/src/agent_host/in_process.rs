//! The in-process [`AgentHost`]: agents run in this daemon's own PTYs.
//!
//! Every method is the exact call the controller made before P1, moved
//! here unchanged: `DaemonPtySession` for the terminal, `v2_session_map`
//! for placement, `grid_emitter` for the frame stream, `hook_ingest` for
//! hook events. Nothing here adds work on a hot path beyond one dynamic
//! dispatch.

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use k2_core::inject_flow::InjectAction;
use k2_core::session::SessionId;
use k2_core::terminal::{AlacEvent, DaemonPtyConfig, DaemonPtySession};
use tokio::sync::broadcast;

use super::{
    AgentHome, AgentHost, AgentSession, Attachment, GridStream, InjectResult, Placement, PlacementEpoch,
    IN_PROCESS_EPOCH,
};
use crate::v2_session_map;

/// Why [`InProcessHost::spawn`] refuses on a `K2_ROLE=controller` daemon.
pub(crate) const CONTROLLER_RUNS_NO_AGENTS: &str =
    "K2_ROLE=controller runs no agents locally; agents run on agent hosts (POST /cli/sessions/v2/spawn places them)";

/// Agents as child processes of this daemon (PRD topology "solo").
pub(crate) struct InProcessHost;

impl AgentHost for InProcessHost {
    fn kind(&self) -> &'static str {
        "in-process"
    }

    fn epoch(&self) -> PlacementEpoch {
        IN_PROCESS_EPOCH
    }

    fn spawn(&self, config: DaemonPtyConfig) -> io::Result<AgentSession> {
        // A controller never runs an agent itself (dist-link); every path
        // that would start one locally fails here with this message.
        if crate::runtime_role::current() == crate::runtime_role::Role::Controller {
            return Err(io::Error::new(io::ErrorKind::Unsupported, CONTROLLER_RUNS_NO_AGENTS));
        }
        DaemonPtySession::spawn(config).map(AgentSession::in_process)
    }

    fn place(&self, agent_id: &str, session: &AgentSession) -> Placement {
        v2_session_map::register(agent_id.to_string(), Arc::clone(session.pty()));
        Placement {
            agent_id: agent_id.to_string(),
            session_id: session.session_id(),
            epoch: IN_PROCESS_EPOCH,
        }
    }

    fn unplace(&self, agent_id: &str) -> Option<AgentSession> {
        v2_session_map::unregister(agent_id).map(AgentSession::in_process)
    }

    fn find_agent(&self, agent_id: &str) -> Option<AgentSession> {
        v2_session_map::lookup_by_agent_name(agent_id).map(AgentSession::in_process)
    }

    fn find_session(&self, session_id: &SessionId) -> Option<AgentSession> {
        v2_session_map::lookup_by_session_id(session_id).map(AgentSession::in_process)
    }

    fn placements(&self) -> Vec<(String, AgentSession)> {
        v2_session_map::snapshot()
            .into_iter()
            .map(|(name, pty)| (name, AgentSession::in_process(pty)))
            .collect()
    }

    fn attach(&self, session: &AgentSession) -> Attachment {
        let pty = session.pty();
        // Same order the grid WS always used: events first, then the
        // viewer registration, then labels and pins.
        let events = pty.subscribe_events();
        let viewer = pty.attach_viewer();
        let labels = pty.subscribe_labels();
        let pins = pty.subscribe_pins();
        Attachment {
            events,
            labels,
            pins,
            _viewer: Some(viewer),
        }
    }

    fn detach(&self, session: &AgentSession, subscriber_id: u64) {
        DaemonPtySession::detach_subscriber(session.pty(), subscriber_id);
    }

    fn stream(&self, session: &AgentSession, pane_id: &str, wants_k1: bool) -> GridStream {
        let (frames, emit_state, format_reg) =
            crate::grid_emitter::attach(session.pty(), pane_id, wants_k1);
        GridStream {
            frames,
            emit_state,
            format_reg,
        }
    }

    fn events(&self, session: &AgentSession) -> broadcast::Receiver<AlacEvent> {
        session.pty().subscribe_events()
    }

    fn mark_exited(&self, session: &AgentSession) {
        session.pty().mark_child_exited();
    }

    fn read_screen(&self, session: &AgentSession, lines: usize) -> Vec<String> {
        use k2_core::terminal::grid_snapshot::snapshot_term;

        let pty = session.pty();
        // Lock the Term briefly, take a snapshot, drop the lock fast. The
        // snapshot is owned, so the Term is not held while we render.
        let term_arc = pty.term();
        let snapshot = {
            let term = term_arc.lock();
            snapshot_term(&pty.session_id.to_string(), &*term, 0)
        };

        // Scrollback first (oldest at the top), then the live grid; each
        // row is its CellRun texts joined.
        let mut out: Vec<String> = Vec::with_capacity(snapshot.scrollback.len() + snapshot.grid.len());
        for row in &snapshot.scrollback {
            let text: String = row.iter().map(|run| run.text.as_str()).collect();
            out.push(text.trim_end().to_string());
        }
        for row in &snapshot.grid {
            let text: String = row.iter().map(|run| run.text.as_str()).collect();
            out.push(text.trim_end().to_string());
        }

        // alacritty pads the live grid to its full height; the padding is
        // noise to the caller.
        while out.last().map(String::is_empty).unwrap_or(false) {
            out.pop();
        }

        let start = out.len().saturating_sub(lines);
        out[start..].to_vec()
    }

    fn write(&self, session: &AgentSession, bytes: Vec<u8>) -> io::Result<()> {
        // Enqueues on the PTY's IO loop and returns; never touches the
        // master fd from this thread.
        session.pty().write(bytes);
        Ok(())
    }

    fn inject(&self, session: &AgentSession, steps: Vec<InjectAction>) -> InjectResult {
        for step in steps {
            match step {
                InjectAction::Write(bytes) => {
                    if self.write(session, bytes).is_err() {
                        return InjectResult::ChildGone;
                    }
                }
                InjectAction::Sleep(ms) => {
                    if ms > 0 {
                        std::thread::sleep(Duration::from_millis(ms));
                    }
                }
                InjectAction::CheckAlive => {
                    if !session.is_child_alive() {
                        return InjectResult::ChildGone;
                    }
                }
            }
        }
        InjectResult::Delivered
    }

    fn resize(&self, session: &AgentSession, cols: u16, rows: u16) {
        DaemonPtySession::request_resize(session.pty(), cols, rows);
    }

    fn resize_now(&self, session: &AgentSession, cols: u16, rows: u16) -> (u16, u16) {
        DaemonPtySession::apply_resize_now(session.pty(), cols, rows)
    }

    fn size(&self, session: &AgentSession) -> (u16, u16) {
        use k2_core::terminal::Dimensions;
        let term_mutex = session.pty().term();
        let term = term_mutex.lock();
        (term.columns() as u16, term.screen_lines() as u16)
    }

    fn claimed_size(&self, session: &AgentSession) -> (u16, u16) {
        use std::sync::atomic::Ordering;
        let pty = session.pty();
        (
            pty.active_cols.load(Ordering::Relaxed),
            pty.active_rows.load(Ordering::Relaxed),
        )
    }

    fn set_claimed_size(&self, session: &AgentSession, cols: u16, rows: u16) {
        use std::sync::atomic::Ordering;
        let pty = session.pty();
        pty.active_cols.store(cols, Ordering::Relaxed);
        pty.active_rows.store(rows, Ordering::Relaxed);
    }

    fn pin(&self, session: &AgentSession, dims: Option<(u16, u16)>, set_by: Option<String>) {
        DaemonPtySession::set_pinned(session.pty(), dims, set_by);
    }

    fn set_label(&self, session: &AgentSession, label: String, lock: bool) -> String {
        session.pty().set_label(label, lock)
    }

    fn kill(&self, session: &AgentSession) {
        session.pty().kill();
    }

    fn hook_events(&self) -> broadcast::Receiver<crate::hook_ingest::IngestEvent> {
        crate::hook_ingest::subscribe()
    }

    fn read_home(&self, home: &AgentHome, rel: &Path) -> io::Result<Vec<u8>> {
        std::fs::read(home.resolve(rel)?)
    }

    fn write_home(&self, home: &AgentHome, rel: &Path, bytes: &[u8]) -> io::Result<()> {
        let path = home.resolve(rel)?;
        let dir = path
            .parent()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "agent home path has no parent"))?;
        std::fs::create_dir_all(dir)?;
        let name = path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
        let tmp = dir.join(format!(".{name}.k2tmp-{}", uuid::Uuid::new_v4()));
        if let Err(e) = std::fs::write(&tmp, bytes) {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
        std::fs::rename(&tmp, &path).inspect_err(|_| {
            let _ = std::fs::remove_file(&tmp);
        })
    }
}
