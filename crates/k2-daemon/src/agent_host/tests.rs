//! The in-process host through the `AgentHost` trait, against a real PTY
//! running the `claude` shim (`AgentShim`: `exec cat`, so whatever is
//! written comes back on the screen).

use std::path::Path;
use std::time::{Duration, Instant};

use k2_core::inject_flow::InjectAction;
use k2_core::terminal::{AlacEvent, DaemonPtyConfig, LabelSource};
use k2_core::test_env::{self, AgentShim, TempHome};

use super::*;

/// One placed shim agent; unplaced (and so killed) on drop, even when an
/// assertion fails first.
struct Shim {
    agent_id: String,
    session: AgentSession,
}

impl Drop for Shim {
    fn drop(&mut self) {
        host().unplace(&self.agent_id);
        host().kill(&self.session);
    }
}

fn spawn_shim(tag: &str) -> Shim {
    let _ = k2_core::db::init_for_tests();
    let cwd = test_env::unique_temp_path(&format!("agent-host-{tag}"));
    std::fs::create_dir_all(&cwd).expect("create cwd");
    let config = DaemonPtyConfig {
        cols: 80,
        rows: 24,
        cwd: Some(cwd),
        program: Some("claude".to_string()),
        label: format!("agent-host {tag}"),
        label_source: LabelSource::Seed,
        ..DaemonPtyConfig::default()
    };
    let session = host().spawn(config).expect("spawn the claude shim");
    let agent_id = format!("agent-host-test-{tag}-{}", uuid::Uuid::new_v4());
    let placement = host().place(&agent_id, &session);
    assert_eq!(
        placement,
        Placement {
            agent_id: agent_id.clone(),
            session_id: session.session_id(),
            epoch: IN_PROCESS_EPOCH,
        }
    );
    Shim { agent_id, session }
}

/// Poll (bounded, fails loudly) until the screen shows `needle`.
fn wait_for_screen(session: &AgentSession, needle: &str) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let lines = host().read_screen(session, 200);
        if lines.iter().any(|l| l.contains(needle)) {
            return lines;
        }
        assert!(Instant::now() < deadline, "screen never showed {needle:?}; screen: {lines:?}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Poll (bounded, fails loudly) until the terminal is `want`.
fn wait_for_size(session: &AgentSession, want: (u16, u16)) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let got = host().size(session);
        if got == want {
            return;
        }
        assert!(Instant::now() < deadline, "terminal stayed {got:?}, wanted {want:?}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn the_daemon_host_is_the_in_process_host() {
    assert_eq!(host().kind(), "in-process");
    assert_eq!(host().epoch(), IN_PROCESS_EPOCH);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_places_a_shim_agent_findable_by_agent_and_session_id() {
    let _home = TempHome::new();
    let _agents = AgentShim::install();
    let shim = spawn_shim("spawn");
    let s = &shim.session;

    assert!(s.is_child_alive());
    assert!(s.child_pid().is_some());
    assert_eq!(s.epoch(), IN_PROCESS_EPOCH);
    assert_eq!(s.program(), Some("claude"));
    assert_eq!(s.label(), "agent-host spawn");

    let by_agent = host().find_agent(&shim.agent_id).expect("placed under its agent id");
    assert!(by_agent.same_session(s));
    let by_session = host().find_session(&s.session_id()).expect("findable by session id");
    assert!(by_session.same_session(s));
    let placed: Vec<_> = host()
        .placements()
        .into_iter()
        .filter(|(id, _)| id == &shim.agent_id)
        .collect();
    assert_eq!(placed.len(), 1);
    assert!(placed[0].1.same_session(s));

    // The legacy lookups are the same host.
    let live = crate::session_lookup::lookup_any(&shim.agent_id).expect("lookup_any");
    assert!(live.same_session(s));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attach_counts_a_viewer_until_dropped_and_stream_carries_output() {
    let _home = TempHome::new();
    let _agents = AgentShim::install();
    let shim = spawn_shim("attach");
    let s = &shim.session;

    assert_eq!(s.subscriber_count(), 0);
    assert!(!s.ever_attached());
    let attachment = host().attach(s);
    assert_eq!(s.subscriber_count(), 1);
    assert!(s.ever_attached());
    // An internal observer is not a viewer.
    let _observer = host().events(s);
    assert_eq!(s.subscriber_count(), 1);

    let mut stream = host().stream(s, &format!("kessel-{}", s.session_id()), false);
    let mut events = attachment.events;
    host().write(s, b"stream-check\n".to_vec()).expect("write");
    wait_for_screen(s, "stream-check");

    // The attachment saw the output wakeups, and the shared emitter sent
    // at least one encoded frame carrying the text.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut saw_event = false;
    let mut saw_frame = false;
    while !(saw_event && saw_frame) {
        assert!(Instant::now() < deadline, "event={saw_event} frame={saw_frame}");
        match events.try_recv() {
            Ok(_) => saw_event = true,
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => saw_event = true,
            Err(_) => {}
        }
        match stream.frames.try_recv() {
            Ok(frame) if frame.text.contains("stream-check") => saw_frame = true,
            Ok(_) | Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {}
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }

    // The registration is the attachment's last field; dropping the
    // remainder releases the viewer.
    drop(attachment._viewer);
    assert_eq!(s.subscriber_count(), 0);
    host().detach(s, 9_999_999);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_and_inject_reach_the_child() {
    let _home = TempHome::new();
    let _agents = AgentShim::install();
    let shim = spawn_shim("inject");
    let s = &shim.session;

    s.write(b"via-handle\n").expect("handle write");
    wait_for_screen(s, "via-handle");

    let steps = vec![
        InjectAction::Write(b"inject-".to_vec()),
        InjectAction::Sleep(0),
        InjectAction::CheckAlive,
        InjectAction::Write(b"done\n".to_vec()),
    ];
    assert_eq!(host().inject(s, steps), InjectResult::Delivered);
    wait_for_screen(s, "inject-done");

    host().kill(s);
    assert!(!s.is_child_alive());
    let steps = vec![InjectAction::CheckAlive, InjectAction::Write(b"never\n".to_vec())];
    assert_eq!(host().inject(s, steps), InjectResult::ChildGone);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resize_now_resize_claim_and_pin() {
    let _home = TempHome::new();
    let _agents = AgentShim::install();
    let shim = spawn_shim("resize");
    let s = &shim.session;

    assert_eq!(host().size(s), (80, 24));
    assert_eq!(host().resize_now(s, 100, 30), (100, 30));
    assert_eq!(host().size(s), (100, 30));

    // Debounced: lands after the window.
    host().resize(s, 120, 40);
    wait_for_size(s, (120, 40));
    // The handle shorthand is the same front door.
    s.resize(110, 35).expect("handle resize");
    wait_for_size(s, (110, 35));

    assert_eq!(host().claimed_size(s), (0, 0));
    host().set_claimed_size(s, 90, 28);
    assert_eq!(host().claimed_size(s), (90, 28));

    // A pin holds the terminal at its size against later resizes.
    host().pin(s, Some((70, 20)), Some("owner".to_string()));
    wait_for_size(s, (70, 20));
    assert_eq!(host().resize_now(s, 150, 50), (70, 20));
    host().pin(s, None, None);
    assert_eq!(host().resize_now(s, 150, 50), (150, 50));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_carry_the_child_exit_and_mark_exited_flips_liveness() {
    let _home = TempHome::new();
    let _agents = AgentShim::install();
    let shim = spawn_shim("exit");
    let s = &shim.session;
    // Let cat reach its read before the EOF.
    host().write(s, b"ready\n".to_vec()).expect("write");
    wait_for_screen(s, "ready");

    let weak = s.downgrade();
    let mut events = host().events(s);
    // Ctrl-D at the start of a line: cat reads EOF and exits 0.
    host().write(s, vec![0x04]).expect("write EOF");
    let code = loop {
        match tokio::time::timeout(Duration::from_secs(20), events.recv()).await {
            Ok(Ok(AlacEvent::ChildExit(status))) => break status.code(),
            Ok(Ok(_)) | Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {}
            other => panic!("no ChildExit on the events stream: {other:?}"),
        }
    };
    assert_eq!(code, Some(0));
    let upgraded = weak.upgrade().expect("still held by the placement");
    assert!(upgraded.same_session(s));
    host().mark_exited(&upgraded);
    assert!(!s.is_child_alive());

    // A weak handle never keeps a session alive on its own.
    host().unplace(&shim.agent_id);
    let lone = host()
        .spawn(DaemonPtyConfig {
            program: Some("claude".to_string()),
            ..DaemonPtyConfig::default()
        })
        .expect("spawn an unplaced shim");
    let lone_weak = lone.downgrade();
    host().kill(&lone);
    drop(lone);
    assert!(lone_weak.upgrade().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_ends_the_child_and_unplace_removes_the_placement() {
    let _home = TempHome::new();
    let _agents = AgentShim::install();
    let shim = spawn_shim("kill");
    let s = shim.session.clone();

    host().kill(&s);
    assert!(!s.is_child_alive());
    // Idempotent.
    host().kill(&s);

    let removed = host().unplace(&shim.agent_id).expect("was placed");
    assert!(removed.same_session(&s));
    assert!(host().find_agent(&shim.agent_id).is_none());
    assert!(host().find_session(&s.session_id()).is_none());
    assert!(host().unplace(&shim.agent_id).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_label_locks_against_pty_titles() {
    let _home = TempHome::new();
    let _agents = AgentShim::install();
    let shim = spawn_shim("label");
    let s = &shim.session;

    assert_eq!(host().set_label(s, "pinned name".to_string(), true), "pinned name");
    assert_eq!(s.label(), "pinned name");
    assert_eq!(s.label_source(), LabelSource::Locked);
    assert_eq!(s.pty().try_set_label_from_pty("Claude Code".to_string()), None);
    assert_eq!(s.label(), "pinned name");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hook_owner_check_resolves_panes_through_the_placement() {
    let _home = TempHome::new();
    let _agents = AgentShim::install();
    let shim = spawn_shim("hooks");
    let s = &shim.session;
    let pane = s.session_id().to_string();

    // The hook plane's owner check finds the pane only while it is placed.
    let facts = crate::hook_ingest::live_pane_facts(&pane).expect("placed pane is live");
    assert_eq!(facts.child_pid, s.child_pid());

    // The receiver is live on the hook plane. (This test publishes nothing:
    // hook_ingest's own tests assert on the first frame of the same bus.)
    let mut rx = host().hook_events();
    assert!(!matches!(
        rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Closed)
    ));

    host().unplace(&shim.agent_id);
    assert!(crate::hook_ingest::live_pane_facts(&pane).is_none());
}

#[test]
fn home_round_trips_files_and_refuses_paths_outside_it() {
    let root = test_env::unique_temp_path("agent-home");
    let home = AgentHome::new(&root);

    host()
        .write_home(&home, Path::new(".k2/MEMORY.md"), b"first")
        .expect("write creates parents");
    assert_eq!(host().read_home(&home, Path::new(".k2/MEMORY.md")).expect("read"), b"first");
    host()
        .write_home(&home, Path::new("./.k2/MEMORY.md"), b"second")
        .expect("overwrite");
    assert_eq!(host().read_home(&home, Path::new(".k2/MEMORY.md")).expect("read"), b"second");
    // No temp file is left next to the written one.
    let names: Vec<String> = std::fs::read_dir(root.join(".k2"))
        .expect("list")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names, vec!["MEMORY.md".to_string()]);

    for bad in ["../escape", "/etc/passwd", "a/../../b", "", "."] {
        let err = host()
            .write_home(&home, Path::new(bad), b"x")
            .expect_err(&format!("write {bad:?} must be refused"));
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput, "{bad:?}");
        let err = host()
            .read_home(&home, Path::new(bad))
            .expect_err(&format!("read {bad:?} must be refused"));
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput, "{bad:?}");
    }
    assert!(!root.parent().expect("parent").join("escape").exists());
    assert_eq!(
        host().read_home(&home, Path::new("missing")).expect_err("missing").kind(),
        std::io::ErrorKind::NotFound
    );
    std::fs::remove_dir_all(&root).expect("clean up");
}
