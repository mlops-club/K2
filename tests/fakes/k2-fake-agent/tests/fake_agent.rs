//! The fake agent as the daemon sees it: a child process with a HOME, a cwd
//! and input on stdin. Every run gets fresh temp dirs and a cleared env
//! (only what is passed here), so nothing reads the real `~/.claude`.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use k2_fake_agent::{project_slug, AGENT_CLIS, REPLY_MARKER};
use serde_json::Value;

const BIN: &str = env!("CARGO_BIN_EXE_k2-fake-agent");

struct Box_ {
    home: tempfile::TempDir,
    cwd: tempfile::TempDir,
}

impl Box_ {
    fn new() -> Self {
        Self { home: tempfile::tempdir().unwrap(), cwd: tempfile::tempdir().unwrap() }
    }
    fn home(&self) -> &Path {
        self.home.path()
    }
    fn project_dir(&self) -> PathBuf {
        self.home().join(".claude").join("projects").join(project_slug(self.cwd.path()))
    }
    fn memory(&self) -> PathBuf {
        self.project_dir().join("memory").join("MEMORY.md")
    }
    fn transcript(&self, id: &str) -> PathBuf {
        self.project_dir().join(format!("{id}.jsonl"))
    }
    fn cmd(&self, exe: &str, args: &[&str], extra_env: &[(&str, &str)]) -> Command {
        let mut c = Command::new(exe);
        c.args(args)
            .env_clear()
            .env("HOME", self.home())
            .env("PATH", "/usr/bin:/bin")
            .env("K2_FAKE_AGENT_TURN_MS", "1")
            .current_dir(self.cwd.path());
        for (k, v) in extra_env {
            c.env(k, v);
        }
        c
    }
    fn run_in(&self, exe: &str, args: &[&str], stdin: &str, extra_env: &[(&str, &str)]) -> Output {
        let mut child = self
            .cmd(exe, args, extra_env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn k2-fake-agent");
        child.stdin.take().unwrap().write_all(stdin.as_bytes()).unwrap();
        child.wait_with_output().unwrap()
    }
    fn run(&self, args: &[&str], stdin: &str) -> Output {
        self.run_in(BIN, args, stdin, &[])
    }
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn lines(p: &Path) -> Vec<Value> {
    fs::read_to_string(p)
        .unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// `k2_core::test_env::write_executable`, copied (this crate stays free of
/// k2-core): the bytes are written by a short-lived `/bin/sh` child, so no
/// write fd to the script ever lives in this multi-threaded process, and a
/// parallel test's fork can't make our exec fail with ETXTBSY.
fn write_executable(path: &Path, contents: &str) {
    let mut child = Command::new("/bin/sh")
        .args(["-c", "cat > \"$1\" && chmod 755 \"$1\"", "sh"])
        .arg(path)
        .stdin(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn /bin/sh to write {}: {e}", path.display()));
    child.stdin.take().unwrap().write_all(contents.as_bytes()).unwrap();
    let st = child.wait().unwrap();
    assert!(st.success(), "write {}: {st}", path.display());
}

const SID: &str = "11111111-2222-4333-8444-555555555555";

#[test]
fn version_is_claude_shaped_and_at_the_full_hook_set_floor() {
    let b = Box_::new();
    let o = b.run(&["--version"], "");
    assert!(o.status.success());
    assert_eq!(stdout(&o), "2.1.300 (Claude Code; k2-fake-agent 0.1.0)\n");
}

#[test]
fn agent_cli_names_match_the_spawn_guard() {
    let src = include_str!("../../../../crates/k2-core/src/terminal/agent_spawn_guard.rs");
    let start = src.find("pub const AGENT_CLIS: &[&str] = &[").expect("AGENT_CLIS in agent_spawn_guard.rs");
    let body = &src[start..];
    let body = &body[body.find('[').unwrap() + 1..];
    let body = &body[body.find('[').unwrap() + 1..body.find("];").unwrap()];
    let names: Vec<&str> = body.split(',').map(|s| s.trim().trim_matches('"')).filter(|s| !s.is_empty()).collect();
    assert_eq!(names, AGENT_CLIS);
}

#[test]
fn new_session_writes_transcript_and_memory_where_claude_does() {
    let b = Box_::new();
    let o = b.run(&["--session-id", SID], "hello there\r\nremember the build is green\n");
    assert!(o.status.success(), "{o:?}");
    let out = stdout(&o);
    assert!(out.starts_with(&format!("k2-fake-agent (as k2-fake-agent) · session {SID} · new\n")), "{out}");
    assert!(out.contains(&format!("{REPLY_MARKER}1: hello there\n")), "{out}");
    assert!(out.contains(&format!("{REPLY_MARKER}2: remember the build is green\n")), "{out}");

    let t = lines(&b.transcript(SID));
    let kinds: Vec<&str> = t.iter().map(|v| v["type"].as_str().unwrap()).collect();
    assert_eq!(kinds, ["user", "assistant", "user", "assistant"]);
    assert!(t.iter().all(|v| v["sessionId"] == SID));
    assert_eq!(t[0]["message"]["content"], "hello there");
    assert_eq!(t[0]["parentUuid"], Value::Null);
    assert_eq!(t[1]["parentUuid"], t[0]["uuid"]);
    assert_eq!(t[3]["message"]["content"][0]["text"], format!("{REPLY_MARKER}2: remember the build is green"));

    assert_eq!(fs::read_to_string(b.memory()).unwrap(), "# Memory\n\n- the build is green\n");
}

#[test]
fn resume_keeps_the_session_id_and_the_memory_bytes() {
    let b = Box_::new();
    assert!(b.run(&["--session-id", SID], "remember alpha\n").status.success());
    let memory_before = fs::read(b.memory()).unwrap();

    let o = b.run(&["--resume", SID], "what did I say?\n");
    assert!(o.status.success(), "{o:?}");
    let out = stdout(&o);
    assert!(out.starts_with(&format!("k2-fake-agent (as k2-fake-agent) · session {SID} · resumed\n")), "{out}");
    // Turn numbers continue from the transcript.
    assert!(out.contains(&format!("{REPLY_MARKER}2: what did I say?\n")), "{out}");
    assert_eq!(fs::read(b.memory()).unwrap(), memory_before, "I3: memory byte for byte");

    let t = lines(&b.transcript(SID));
    assert_eq!(t.len(), 4);
    assert!(t.iter().all(|v| v["sessionId"] == SID));
    assert_eq!(t[2]["parentUuid"], t[1]["uuid"], "the resumed turn chains onto the old one");
    // Only one transcript: no new session was minted.
    let jsonl = fs::read_dir(b.project_dir())
        .unwrap()
        .flatten()
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("jsonl"))
        .count();
    assert_eq!(jsonl, 1);
}

#[test]
fn resume_with_short_flag_and_continue_find_the_same_session() {
    let b = Box_::new();
    assert!(b.run(&["--session-id", SID], "one\n").status.success());
    let o = b.run(&["-r", SID, "--dangerously-skip-permissions"], "two\n");
    assert!(stdout(&o).contains(&format!("session {SID} · resumed")), "{o:?}");
    let o = b.run(&["--continue"], "three\n");
    assert!(stdout(&o).contains(&format!("session {SID} · resumed")), "{o:?}");
    assert_eq!(lines(&b.transcript(SID)).len(), 6);
}

#[test]
fn resume_of_an_unknown_session_fails_like_claude() {
    let b = Box_::new();
    let o = b.run(&["--resume", SID], "");
    assert_eq!(o.status.code(), Some(1));
    assert_eq!(stdout(&o), format!("No conversation found with session ID: {SID}\n"));
}

#[test]
fn a_session_id_in_use_is_refused() {
    let b = Box_::new();
    assert!(b.run(&["--session-id", SID], "x\n").status.success());
    let o = b.run(&["--session-id", SID], "");
    assert_eq!(o.status.code(), Some(1));
    assert_eq!(stdout(&o), format!("Error: Session ID {SID} is already in use.\n"));
}

#[test]
fn fork_session_copies_into_a_new_id_and_leaves_the_old_one() {
    let b = Box_::new();
    assert!(b.run(&["--session-id", SID], "x\n").status.success());
    let old = fs::read(b.transcript(SID)).unwrap();
    let new = "99999999-8888-4777-8666-555555555555";
    let o = b.run(&["--resume", SID, "--fork-session", "--session-id", new], "y\n");
    assert!(stdout(&o).contains(&format!("session {new} · resumed")), "{o:?}");
    assert_eq!(fs::read(b.transcript(SID)).unwrap(), old);
    let t = lines(&b.transcript(new));
    assert_eq!(t.len(), 4);
    assert!(t.iter().all(|v| v["sessionId"] == new));
}

#[test]
fn claude_config_dir_moves_the_project_dir() {
    let b = Box_::new();
    let cfg = b.home().join("alt-config");
    let o = b.run_in(BIN, &["--session-id", SID], "remember x\n", &[("CLAUDE_CONFIG_DIR", cfg.to_str().unwrap())]);
    assert!(o.status.success(), "{o:?}");
    let dir = cfg.join("projects").join(project_slug(b.cwd.path()));
    assert!(dir.join(format!("{SID}.jsonl")).is_file());
    assert!(dir.join("memory/MEMORY.md").is_file());
    assert!(!b.project_dir().exists());
}

#[test]
fn print_mode_answers_once_and_exits() {
    let b = Box_::new();
    let o = b.run(&["-p", "--session-id", SID, "summarise", "this"], "");
    assert!(o.status.success());
    assert_eq!(stdout(&o), format!("{REPLY_MARKER}1: summarise this\n"));
}

/// Hooks registered the way K2's installer writes them
/// (`agent_hooks::install::hook_command`) run as children of the agent with
/// Claude's payload on stdin and the cell's K2 env, and see the agent's pid
/// as `$PPID`.
#[test]
fn hooks_fire_like_claude_code_with_the_k2_env() {
    let b = Box_::new();
    let hooks_dir = b.home().join(".k2").join("hooks");
    fs::create_dir_all(&hooks_dir).unwrap();
    let log = b.home().join("hook.log");
    let script = hooks_dir.join("notify.sh");
    write_executable(
        &script,
        &format!(
            "#!/bin/sh\nbody=$(cat)\nprintf '%s|%s|%s|%s|%s|%s|%s|%s\\n' \"$1\" \"$K2_HOOK_AGENT_PID\" \"$K2_HOOK_SOURCE\" \
             \"$K2_HOOK_SOCK\" \"$K2_HOOK_TOKEN\" \"$K2_SESSION_ID\" \"$K2_CELL\" \"$body\" >> '{}'\n",
            log.display()
        ),
    );
    let p = format!("'{}'", script.display());
    let cmd = format!("[ -x {p} ] && K2_HOOK_AGENT_PID=$PPID K2_HOOK_SOURCE=claude {p} \"$@\" || true");
    let group = |c: &str| serde_json::json!([{ "hooks": [{ "type": "command", "command": c }] }]);
    let settings = serde_json::json!({ "hooks": {
        "SessionStart": group(&cmd),
        "UserPromptSubmit": group(&cmd),
        "Stop": group(&cmd),
        "SessionEnd": group(&cmd),
    }});
    fs::create_dir_all(b.home().join(".claude")).unwrap();
    fs::write(b.home().join(".claude/settings.json"), settings.to_string()).unwrap();

    let env = [
        ("K2_HOOK_SOCK", "/tmp/k2-cell.sock"),
        ("K2_HOOK_TOKEN", "k2sc_test"),
        ("K2_SESSION_ID", "sess-1"),
        ("K2_CELL", "canonical"),
    ];
    let mut child = b
        .cmd(BIN, &["--session-id", SID], &env)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let pid = child.id().to_string();
    child.stdin.take().unwrap().write_all(b"hi\n").unwrap();
    let o = child.wait_with_output().unwrap();
    assert!(o.status.success());

    let rows: Vec<Vec<String>> = fs::read_to_string(&log)
        .unwrap()
        .lines()
        .map(|l| l.splitn(8, '|').map(String::from).collect())
        .collect();
    let events: Vec<String> = rows
        .iter()
        .map(|r| serde_json::from_str::<Value>(&r[7]).unwrap()["hook_event_name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(events, ["SessionStart", "UserPromptSubmit", "Stop", "SessionEnd"]);
    for r in &rows {
        assert_eq!(r[0], "", "no positional arg");
        assert_eq!(r[1], pid, "$PPID in the hook command is the agent");
        assert_eq!(&r[2..7], ["claude", "/tmp/k2-cell.sock", "k2sc_test", "sess-1", "canonical"]);
        let body: Value = serde_json::from_str(&r[7]).unwrap();
        assert_eq!(body["session_id"], SID);
        assert_eq!(body["transcript_path"], b.transcript(SID).to_str().unwrap());
    }
    let start: Value = serde_json::from_str(&rows[0][7]).unwrap();
    assert_eq!(start["source"], "startup");
    let submit: Value = serde_json::from_str(&rows[1][7]).unwrap();
    assert_eq!(submit["prompt"], "hi");
}

#[test]
fn session_start_matcher_selects_startup_or_resume() {
    let b = Box_::new();
    let log = b.home().join("m.log");
    let rec = |tag: &str| format!("echo {tag} >> '{}'", log.display());
    let settings = serde_json::json!({ "hooks": { "SessionStart": [
        { "matcher": "startup", "hooks": [{ "type": "command", "command": rec("startup") }] },
        { "matcher": "resume", "hooks": [{ "type": "command", "command": rec("resume") }] },
    ]}});
    fs::create_dir_all(b.home().join(".claude")).unwrap();
    fs::write(b.home().join(".claude/settings.json"), settings.to_string()).unwrap();
    // A session with no turn is never saved (as with Claude Code), so the
    // first run has one.
    assert!(b.run(&["--session-id", SID], "x\n").status.success());
    assert!(b.run(&["--resume", SID], "").status.success());
    assert_eq!(fs::read_to_string(&log).unwrap(), "startup\nresume\n");
}

#[test]
fn install_shims_makes_every_agent_name_run_the_fake() {
    let b = Box_::new();
    let dir = b.home().join("shims");
    let o = b.run(&["--install-shims", dir.to_str().unwrap()], "");
    assert!(o.status.success(), "{o:?}");
    for name in AGENT_CLIS {
        assert!(dir.join(name).is_file(), "{name}");
    }
    let o = b.run_in(dir.join("claude").to_str().unwrap(), &["--session-id", SID], "ping\n", &[]);
    assert!(stdout(&o).starts_with(&format!("k2-fake-agent (as claude) · session {SID} · new\n")), "{o:?}");
    // Installing again replaces the links.
    assert!(b.run(&["--install-shims", dir.to_str().unwrap()], "").status.success());
}

#[test]
fn trace_events_use_the_lab_format() {
    let b = Box_::new();
    let trace = b.home().join("trace.jsonl");
    let o = b.run_in(BIN, &["--session-id", SID], "x\n", &[("K2_FAKE_AGENT_TRACE", trace.to_str().unwrap()), ("K2_PANE_ID", "pane-7")]);
    assert!(o.status.success());
    let t = lines(&trace);
    let events: Vec<&str> = t.iter().map(|v| v["event"].as_str().unwrap()).collect();
    assert_eq!(events, ["cli.exec", "cli.hook", "cli.turn.start", "cli.hook", "cli.turn.end", "cli.hook", "cli.hook", "cli.exit"]);
    for v in &t {
        assert_eq!(v["src"], "k2-fake-agent");
        assert_eq!(v["session_id"], SID);
        assert_eq!(v["pane"], "pane-7");
        assert!(v["ts"].as_str().unwrap().ends_with('Z'));
    }
    assert_eq!(t[1]["attrs"]["hook"], "SessionStart");
    assert_eq!(t[1]["attrs"]["registered"], 0);
}

/// `cli.hook` is traced before the hook command runs and `cli.hook.done`
/// (with the exit status) after: the hook's own side effect, written to the
/// same trace file, lands between them. Wave 2's flows rely on `cli.hook`
/// preceding whatever the daemon records on receiving the hook.
#[test]
fn cli_hook_is_traced_before_the_hook_runs_and_done_after() {
    let b = Box_::new();
    let trace = b.home().join("trace.jsonl");
    let mark = |tag: &str| {
        format!("printf '{{\"ts\":\"x\",\"event\":\"hook.ran\",\"attrs\":{{\"tag\":\"{tag}\"}}}}\\n' >> '{}'; exit 3", trace.display())
    };
    let settings = serde_json::json!({ "hooks": {
        "SessionStart": [{ "hooks": [{ "type": "command", "command": mark("start") }] }],
        "Stop": [{ "hooks": [{ "type": "command", "command": mark("stop") }] }],
    }});
    fs::create_dir_all(b.home().join(".claude")).unwrap();
    fs::write(b.home().join(".claude/settings.json"), settings.to_string()).unwrap();
    let o = b.run_in(BIN, &["--session-id", SID], "x\n", &[("K2_FAKE_AGENT_TRACE", trace.to_str().unwrap())]);
    assert!(o.status.success(), "{o:?}");
    let seq: Vec<String> = lines(&trace)
        .iter()
        .filter(|v| v["event"].as_str().unwrap().starts_with("cli.hook") || v["event"] == "hook.ran")
        .map(|v| {
            let a = &v["attrs"];
            match v["event"].as_str().unwrap() {
                "hook.ran" => format!("ran {}", a["tag"].as_str().unwrap()),
                "cli.hook.done" => format!("done {} exit={}", a["hook"].as_str().unwrap(), a["exit"]),
                e => format!("{e} {}", a["hook"].as_str().unwrap()),
            }
        })
        .collect();
    assert_eq!(
        seq,
        [
            "cli.hook SessionStart",
            "ran start",
            "done SessionStart exit=3",
            "cli.hook UserPromptSubmit", // registered: 0, nothing runs
            "cli.hook Stop",
            "ran stop",
            "done Stop exit=3",
            "cli.hook SessionEnd",
        ]
    );
}
