//! TEST ONLY: `k2-fake-agent`, a fake agent CLI for the K2 lab
//! (PRD "K2 Distributed Runtime", section 3, point 3).
//!
//! Grown from `k2_core::test_env::AgentShim` (`exec cat`). To the daemon it
//! looks like Claude Code:
//!
//! - it is spawned as `claude` (or any name in `AGENT_CLIS`) through a shim
//!   dir (`--install-shims <dir>` writes one symlink per name);
//! - it reads pasted input from its PTY, one line per prompt, and after a
//!   timed "turn" (`--turn-ms`, jittered deterministically from `--seed`)
//!   prints a recognisable reply: `[k2-fake-agent] reply #<n>: <prompt>`;
//! - it writes the transcript JSONL and the memory file where Claude Code
//!   does: `<config>/projects/<slug>/<session-id>.jsonl` and
//!   `<config>/projects/<slug>/memory/MEMORY.md`, with `<config>` =
//!   `$CLAUDE_CONFIG_DIR` or `$HOME/.claude` and `<slug>` the canonical cwd
//!   with every char other than `[A-Za-z0-9-]` turned into `-`
//!   (`k2_core::chat_history::claude_project_hash`);
//! - it runs the hook commands registered in Claude's settings files
//!   (SessionStart, UserPromptSubmit, Stop, SessionEnd) with Claude's JSON
//!   payload on stdin, through `/bin/sh -c` as a child of this process, so
//!   K2's `notify.sh` sees `K2_HOOK_SOCK`, `K2_HOOK_TOKEN`, `K2_PANE_ID` and
//!   `$PPID` exactly as it does under the real CLI;
//! - `--resume <id>` keeps the session id and never rewrites the memory
//!   file (invariant I3); only a prompt starting with `remember ` appends to
//!   it.
//!
//! With `K2_FAKE_AGENT_TRACE=<file>` it also appends its own trace events
//! (the lab trace format, `docs/prd/progress/lab.md`) to that file:
//! `cli.exec`, `cli.hook` (before the hook command runs), `cli.hook.done`
//! (after, with its exit status), `cli.turn.start`, `cli.turn.end`,
//! `cli.exit`.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

/// The version `--version` reports. At or above K2's full hook set floor
/// (`CLAUDE_FULL_SET_FLOOR`, 2.1.292), so the daemon installs every hook.
pub const CLAUDE_VERSION: &str = "2.1.300";

/// Every agent CLI name the K2 spawn guard knows
/// (`crates/k2-core/src/terminal/agent_spawn_guard.rs`, `AGENT_CLIS`).
/// A test checks this list against that file.
pub const AGENT_CLIS: &[&str] = &[
    "claude",
    "codex",
    "grok",
    "gemini",
    "cursor-agent",
    "cursor",
    "pi",
    "hermes",
    "opencode",
    "goose",
    "aider",
    "ollama",
    "copilot",
    "interpreter",
];

/// The marker every reply starts with.
pub const REPLY_MARKER: &str = "[k2-fake-agent] reply #";

pub const DEFAULT_TURN_MS: u64 = 300;

/// Parsed command line.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Args {
    pub version: bool,
    pub help: bool,
    pub install_shims: Option<PathBuf>,
    pub session_id: Option<String>,
    pub resume: Option<String>,
    pub continue_last: bool,
    pub fork_session: bool,
    pub print: bool,
    pub turn_ms: Option<u64>,
    pub seed: Option<u64>,
    pub settings: Option<String>,
    pub append_system_prompt: Option<String>,
    pub model: Option<String>,
    /// The positional prompt (a fire-once launch prompt, or `-p`'s prompt).
    pub prompt: Option<String>,
    /// Flags we accept and ignore, kept for the transcript.
    pub ignored: Vec<String>,
}

/// Claude Code flags that take a value. Anything else starting with `-` is
/// treated as a boolean flag and ignored.
const VALUE_FLAGS: &[&str] = &[
    "--session-id",
    "--resume",
    "-r",
    "--turn-ms",
    "--seed",
    "--settings",
    "--append-system-prompt",
    "--system-prompt",
    "--model",
    "--permission-mode",
    "--mcp-config",
    "--name",
    "--add-dir",
    "--allowedTools",
    "--allowed-tools",
    "--disallowedTools",
    "--disallowed-tools",
    "--output-format",
    "--input-format",
    "--fallback-model",
    "--install-shims",
];

pub fn parse_args(argv: &[String]) -> Result<Args, String> {
    let mut a = Args::default();
    let mut positional: Vec<String> = Vec::new();
    let mut i = 0;
    while i < argv.len() {
        let raw = &argv[i];
        i += 1;
        if raw == "--" {
            positional.extend(argv[i..].iter().cloned());
            break;
        }
        if !raw.starts_with('-') || raw == "-" {
            positional.push(raw.clone());
            continue;
        }
        let (flag, inline) = match raw.split_once('=') {
            Some((f, v)) if f.starts_with("--") => (f.to_string(), Some(v.to_string())),
            _ => (raw.clone(), None),
        };
        let mut value = || -> Result<String, String> {
            if let Some(v) = inline.clone() {
                return Ok(v);
            }
            let v = argv.get(i).cloned().ok_or_else(|| format!("error: option '{flag}' argument missing"))?;
            i += 1;
            Ok(v)
        };
        match flag.as_str() {
            "--version" | "-v" => a.version = true,
            "--help" | "-h" => a.help = true,
            "--continue" | "-c" => a.continue_last = true,
            "--fork-session" => a.fork_session = true,
            "--print" | "-p" => a.print = true,
            "--session-id" => a.session_id = Some(value()?),
            "--resume" | "-r" => a.resume = Some(value()?),
            "--turn-ms" => {
                let v = value()?;
                a.turn_ms = Some(v.parse().map_err(|_| format!("error: --turn-ms wants a number, got '{v}'"))?);
            }
            "--seed" => {
                let v = value()?;
                a.seed = Some(v.parse().map_err(|_| format!("error: --seed wants a number, got '{v}'"))?);
            }
            "--settings" => a.settings = Some(value()?),
            "--append-system-prompt" => a.append_system_prompt = Some(value()?),
            "--model" => a.model = Some(value()?),
            "--install-shims" => a.install_shims = Some(PathBuf::from(value()?)),
            f if VALUE_FLAGS.contains(&f) => {
                let v = value()?;
                a.ignored.push(format!("{f}={v}"));
            }
            f => a.ignored.push(f.to_string()),
        }
    }
    if !positional.is_empty() {
        a.prompt = Some(positional.join(" "));
    }
    Ok(a)
}

// ── Determinism ─────────────────────────────────────────────────────────

/// SplitMix64: one step, a pure function of its input.
pub fn splitmix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// How long turn `n` (1-based) takes: `turn_ms` scaled by a factor in
/// [0.5, 1.5] drawn from `(seed, n)`. Same seed, same durations.
pub fn turn_duration_ms(turn_ms: u64, seed: u64, n: u64) -> u64 {
    let pct = 50 + splitmix64(seed ^ n.wrapping_mul(0x2545_F491_4F6C_DD1D)) % 101;
    turn_ms * pct / 100
}

/// The reply line for turn `n`.
pub fn reply_text(n: u64, prompt: &str) -> String {
    format!("{REPLY_MARKER}{n}: {prompt}")
}

// ── Paths ───────────────────────────────────────────────────────────────

/// `k2_core::chat_history::claude_project_hash`: the canonical path with
/// every char other than ASCII alphanumerics and `-` replaced by `-`.
pub fn project_slug(cwd: &Path) -> String {
    let canonical = fs::canonicalize(cwd)
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| cwd.to_string_lossy().into_owned());
    canonical
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' { c } else { '-' })
        .collect()
}

/// Claude's config dir: `$CLAUDE_CONFIG_DIR`, else `$HOME/.claude`.
pub fn config_dir(env: &dyn Fn(&str) -> Option<String>) -> Result<PathBuf, String> {
    if let Some(d) = env("CLAUDE_CONFIG_DIR").filter(|d| !d.is_empty()) {
        return Ok(PathBuf::from(d));
    }
    let home = env("HOME").filter(|h| !h.is_empty()).ok_or("HOME is not set")?;
    Ok(PathBuf::from(home).join(".claude"))
}

/// Where one project's sessions and memory live.
#[derive(Debug, Clone)]
pub struct ProjectPaths {
    pub dir: PathBuf,
}

impl ProjectPaths {
    pub fn new(config: &Path, cwd: &Path) -> Self {
        Self { dir: config.join("projects").join(project_slug(cwd)) }
    }
    pub fn transcript(&self, session_id: &str) -> PathBuf {
        self.dir.join(format!("{session_id}.jsonl"))
    }
    pub fn memory(&self) -> PathBuf {
        self.dir.join("memory").join("MEMORY.md")
    }
    /// The newest transcript in the project dir (`--continue`).
    pub fn latest_session(&self) -> Option<String> {
        let mut best: Option<(SystemTime, String)> = None;
        for e in fs::read_dir(&self.dir).ok()?.flatten() {
            let p = e.path();
            if p.extension().and_then(|x| x.to_str()) != Some("jsonl") {
                continue;
            }
            let Some(stem) = p.file_stem().and_then(|s| s.to_str()) else { continue };
            let Ok(m) = e.metadata().and_then(|m| m.modified()) else { continue };
            if best.as_ref().map_or(true, |(t, _)| m > *t) {
                best = Some((m, stem.to_string()));
            }
        }
        best.map(|(_, s)| s)
    }
}

// ── Ids and time ────────────────────────────────────────────────────────

/// A random UUID v4 string (from `/dev/urandom`).
pub fn new_uuid() -> String {
    let mut b = [0u8; 16];
    let filled = fs::File::open("/dev/urandom").and_then(|mut f| io::Read::read_exact(&mut f, &mut b));
    if filled.is_err() {
        // No /dev/urandom: mix the clock through SplitMix64 instead.
        let t = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0);
        b[..8].copy_from_slice(&splitmix64(t).to_le_bytes());
        b[8..].copy_from_slice(&splitmix64(t ^ 0xA5A5).to_le_bytes());
    }
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32])
}

/// RFC 3339 UTC with milliseconds, e.g. `2026-10-24T09:12:03.123Z`.
pub fn rfc3339(t: SystemTime) -> String {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = d.as_secs() as i64;
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60,
        d.subsec_millis()
    )
}

fn now() -> String {
    rfc3339(SystemTime::now())
}

// ── Hooks ───────────────────────────────────────────────────────────────

/// One registered hook command.
#[derive(Debug, Clone, PartialEq)]
pub struct HookCmd {
    pub command: String,
    pub is_async: bool,
    pub timeout: Duration,
}

/// The hook commands for `event`, in the order Claude Code reads its
/// settings: user, project, project-local, then `--settings`. For
/// SessionStart a group's `matcher` (when set and not `*`) must equal the
/// start source (`startup` or `resume`).
pub fn hooks_for(settings_files: &[PathBuf], event: &str, source: Option<&str>) -> Vec<HookCmd> {
    let mut out = Vec::new();
    for f in settings_files {
        let Ok(bytes) = fs::read(f) else { continue };
        let Ok(v) = serde_json::from_slice::<Value>(&bytes) else { continue };
        let Some(groups) = v.get("hooks").and_then(|h| h.get(event)).and_then(Value::as_array) else {
            continue;
        };
        for g in groups {
            let matcher = g.get("matcher").and_then(Value::as_str).unwrap_or("");
            if let Some(src) = source {
                if !matcher.is_empty() && matcher != "*" && matcher != src {
                    continue;
                }
            }
            for h in g.get("hooks").and_then(Value::as_array).into_iter().flatten() {
                if h.get("type").and_then(Value::as_str) != Some("command") {
                    continue;
                }
                let Some(cmd) = h.get("command").and_then(Value::as_str) else { continue };
                out.push(HookCmd {
                    command: cmd.to_string(),
                    is_async: h.get("async").and_then(Value::as_bool).unwrap_or(false),
                    timeout: Duration::from_secs(h.get("timeout").and_then(Value::as_u64).unwrap_or(60)),
                });
            }
        }
    }
    out
}

/// Run one hook: `/bin/sh -c <command>`, payload on stdin, in `cwd`.
/// Returns the exit code (`None` for async hooks, a kill on timeout, or a
/// spawn failure).
pub fn run_hook(h: &HookCmd, payload: &Value, cwd: &Path) -> Option<i32> {
    let mut child = Command::new("/bin/sh")
        .arg("-c")
        .arg(&h.command)
        .current_dir(cwd)
        .env("CLAUDE_PROJECT_DIR", cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(payload.to_string().as_bytes());
    }
    if h.is_async {
        std::thread::spawn(move || {
            let _ = child.wait();
        });
        return None;
    }
    let deadline = Instant::now() + h.timeout;
    loop {
        match child.try_wait() {
            Ok(Some(st)) => return st.code(),
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(5)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    }
}

// ── The session ─────────────────────────────────────────────────────────

/// Everything a running fake agent needs from its environment.
pub struct Env {
    pub vars: BTreeMap<String, String>,
    pub cwd: PathBuf,
}

impl Env {
    pub fn from_process() -> io::Result<Self> {
        Ok(Self { vars: std::env::vars().collect(), cwd: std::env::current_dir()? })
    }
    fn get(&self, k: &str) -> Option<String> {
        self.vars.get(k).cloned()
    }
}

/// A started (new or resumed) session.
pub struct Session {
    pub id: String,
    pub resumed: bool,
    pub paths: ProjectPaths,
    pub cwd: PathBuf,
    pub turn_ms: u64,
    pub seed: u64,
    settings_files: Vec<PathBuf>,
    trace: Option<PathBuf>,
    pane: Option<String>,
    last_uuid: Option<String>,
    turns: u64,
}

impl Session {
    /// Resolve the session the args ask for (new, `--session-id`,
    /// `--resume`, `--continue`, `--fork-session`). Errors carry Claude
    /// Code's wording.
    pub fn open(args: &Args, env: &Env) -> Result<Self, String> {
        let config = config_dir(&|k| env.get(k))?;
        let paths = ProjectPaths::new(&config, &env.cwd);
        let resume_id = match (&args.resume, args.continue_last) {
            (Some(id), _) => Some(id.clone()),
            (None, true) => Some(paths.latest_session().ok_or("No conversation found to continue")?),
            (None, false) => None,
        };
        let (id, resumed) = match resume_id {
            Some(old) => {
                let t = paths.transcript(&old);
                if !t.is_file() {
                    return Err(format!("No conversation found with session ID: {old}"));
                }
                if args.fork_session {
                    let new = args.session_id.clone().unwrap_or_else(new_uuid);
                    fork_transcript(&t, &paths.transcript(&new), &new)?;
                    (new, true)
                } else {
                    (old, true)
                }
            }
            None => {
                let id = args.session_id.clone().unwrap_or_else(new_uuid);
                if paths.transcript(&id).exists() {
                    return Err(format!("Error: Session ID {id} is already in use."));
                }
                (id, false)
            }
        };
        fs::create_dir_all(&paths.dir).map_err(|e| format!("create {}: {e}", paths.dir.display()))?;
        let last_uuid = last_uuid_in(&paths.transcript(&id));
        let turns = count_turns(&paths.transcript(&id));
        let mut settings_files = vec![
            config.join("settings.json"),
            env.cwd.join(".claude").join("settings.json"),
            env.cwd.join(".claude").join("settings.local.json"),
        ];
        if let Some(s) = &args.settings {
            settings_files.push(PathBuf::from(s));
        }
        let turn_ms = args
            .turn_ms
            .or_else(|| env.get("K2_FAKE_AGENT_TURN_MS").and_then(|v| v.parse().ok()))
            .unwrap_or(DEFAULT_TURN_MS);
        let seed = args
            .seed
            .or_else(|| env.get("K2_FAKE_AGENT_SEED").and_then(|v| v.parse().ok()))
            .unwrap_or(0);
        Ok(Self {
            id,
            resumed,
            paths,
            cwd: env.cwd.clone(),
            turn_ms,
            seed,
            settings_files,
            trace: env.get("K2_FAKE_AGENT_TRACE").filter(|t| !t.is_empty()).map(PathBuf::from),
            pane: env.get("K2_PANE_ID").or_else(|| env.get("K2SO_PANE_ID")),
            last_uuid,
            turns,
        })
    }

    pub fn transcript(&self) -> PathBuf {
        self.paths.transcript(&self.id)
    }

    /// Append one lab trace event (when `K2_FAKE_AGENT_TRACE` is set).
    pub fn trace(&self, event: &str, attrs: Value) {
        let Some(path) = &self.trace else { return };
        let line = json!({
            "ts": now(),
            "event": event,
            "src": "k2-fake-agent",
            "session_id": self.id,
            "pane": self.pane,
            "pid": std::process::id(),
            "attrs": attrs,
        });
        let _ = append_line(path, &line.to_string());
    }

    fn payload(&self, event: &str, extra: Value) -> Value {
        let mut p = json!({
            "session_id": self.id,
            "transcript_path": self.transcript().to_string_lossy(),
            "cwd": self.cwd.to_string_lossy(),
            "hook_event_name": event,
            "permission_mode": "default",
        });
        if let (Some(obj), Value::Object(more)) = (p.as_object_mut(), extra) {
            obj.extend(more);
        }
        p
    }

    /// Fire every hook registered for `event`. Returns how many ran.
    ///
    /// Trace order matches a real CLI: `cli.hook` is written BEFORE the hook
    /// command starts (so it precedes anything the daemon logs on receiving
    /// it, e.g. `cell.*`), and `cli.hook.done` with the exit status after.
    pub fn fire(&self, event: &str, source: Option<&str>, extra: Value) -> usize {
        let hooks = hooks_for(&self.settings_files, event, source);
        let payload = self.payload(event, extra);
        for h in &hooks {
            self.trace("cli.hook", json!({ "hook": event, "async": h.is_async }));
            let code = run_hook(h, &payload, &self.cwd);
            self.trace("cli.hook.done", json!({ "hook": event, "exit": code, "async": h.is_async }));
        }
        if hooks.is_empty() {
            self.trace("cli.hook", json!({ "hook": event, "registered": 0 }));
        }
        hooks.len()
    }

    fn append_entry(&mut self, kind: &str, message: Value) -> Result<(), String> {
        let uuid = new_uuid();
        let line = json!({
            "parentUuid": self.last_uuid,
            "isSidechain": false,
            "userType": "external",
            "cwd": self.cwd.to_string_lossy(),
            "sessionId": self.id,
            "version": CLAUDE_VERSION,
            "type": kind,
            "message": message,
            "uuid": uuid,
            "timestamp": now(),
        });
        let path = self.transcript();
        append_line(&path, &line.to_string()).map_err(|e| format!("write {}: {e}", path.display()))?;
        self.last_uuid = Some(uuid);
        Ok(())
    }

    /// `remember <text>` appends `- <text>` to MEMORY.md (creating it with
    /// a `# Memory` header). Nothing else ever writes the memory file.
    fn remember(&self, prompt: &str) -> Result<(), String> {
        let Some(text) = prompt.strip_prefix("remember ") else { return Ok(()) };
        let path = self.paths.memory();
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
        }
        let fresh = !path.exists();
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| format!("open {}: {e}", path.display()))?;
        if fresh {
            f.write_all(b"# Memory\n\n").map_err(|e| e.to_string())?;
        }
        f.write_all(format!("- {}\n", text.trim()).as_bytes()).map_err(|e| e.to_string())
    }

    /// One turn: UserPromptSubmit, the timed "thinking", the reply on `out`,
    /// transcript entries, memory, Stop.
    pub fn turn(&mut self, prompt: &str, out: &mut dyn Write) -> Result<String, String> {
        self.turns += 1;
        let n = self.turns;
        self.trace("cli.turn.start", json!({ "turn": n }));
        self.fire("UserPromptSubmit", None, json!({ "prompt": prompt }));
        self.append_entry("user", json!({ "role": "user", "content": prompt }))?;
        let ms = turn_duration_ms(self.turn_ms, self.seed, n);
        std::thread::sleep(Duration::from_millis(ms));
        self.remember(prompt)?;
        let reply = reply_text(n, prompt);
        writeln!(out, "{reply}").map_err(|e| e.to_string())?;
        out.flush().map_err(|e| e.to_string())?;
        self.append_entry(
            "assistant",
            json!({
                "role": "assistant",
                "model": "k2-fake-agent",
                "content": [{ "type": "text", "text": reply }],
            }),
        )?;
        self.trace("cli.turn.end", json!({ "turn": n, "turn_ms": ms }));
        self.fire("Stop", None, json!({ "stop_hook_active": false }));
        Ok(reply)
    }
}

/// Append `line` and a newline to `path` (created if missing) with ONE
/// `write_all` on an O_APPEND file. `writeln!` on a `File` may issue several
/// writes, and the lab's trace file is shared by every agent of a host (and
/// a transcript by a CLI and its resumer): lines from concurrent writers
/// would then interleave and tear. One append write per line keeps each
/// line whole.
pub fn append_line(path: &Path, line: &str) -> io::Result<()> {
    let mut buf = Vec::with_capacity(line.len() + 1);
    buf.extend_from_slice(line.as_bytes());
    buf.push(b'\n');
    OpenOptions::new().create(true).append(true).open(path)?.write_all(&buf)
}

fn fork_transcript(from: &Path, to: &Path, new_id: &str) -> Result<(), String> {
    if to.exists() {
        return Err(format!("Error: Session ID {new_id} is already in use."));
    }
    let src = fs::read_to_string(from).map_err(|e| format!("read {}: {e}", from.display()))?;
    let mut out = String::new();
    for line in src.lines() {
        match serde_json::from_str::<Value>(line) {
            Ok(mut v) => {
                if let Some(o) = v.as_object_mut() {
                    o.insert("sessionId".into(), Value::String(new_id.to_string()));
                }
                out.push_str(&v.to_string());
            }
            Err(_) => out.push_str(line),
        }
        out.push('\n');
    }
    fs::write(to, out).map_err(|e| format!("write {}: {e}", to.display()))
}

fn transcript_lines(path: &Path) -> Vec<Value> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

fn last_uuid_in(path: &Path) -> Option<String> {
    transcript_lines(path).last().and_then(|v| v.get("uuid")).and_then(Value::as_str).map(String::from)
}

fn count_turns(path: &Path) -> u64 {
    transcript_lines(path).iter().filter(|v| v.get("type").and_then(Value::as_str) == Some("user")).count() as u64
}

// ── Shims ───────────────────────────────────────────────────────────────

/// Make `dir/<name>` a symlink to `exe` for every name in [`AGENT_CLIS`].
/// The daemon's spawn guard resolves agent names inside such a dir
/// (`K2_TEST_AGENT_SHIM_DIR`); it also works first on `PATH`.
pub fn install_shims(dir: &Path, exe: &Path) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    for name in AGENT_CLIS {
        let link = dir.join(name);
        if link.symlink_metadata().is_ok() {
            fs::remove_file(&link)?;
        }
        std::os::unix::fs::symlink(exe, &link)?;
    }
    Ok(())
}

// ── Entry point ─────────────────────────────────────────────────────────

pub const USAGE: &str = "\
k2-fake-agent: TEST ONLY fake of an agent CLI (claude-shaped) for the K2 lab.

Usage: claude [options] [prompt]
  --session-id <uuid>      start a new session with this id
  -r, --resume <id>        resume a session (same id, memory untouched)
  -c, --continue           resume the newest session in this directory
  --fork-session           with --resume: copy into a new session id
  -p, --print              answer the prompt once and exit
  --turn-ms <n>            base turn length (default 300; env K2_FAKE_AGENT_TURN_MS)
  --seed <n>               jitter seed (default 0; env K2_FAKE_AGENT_SEED)
  --settings <file>        extra settings file with hooks
  --install-shims <dir>    symlink every agent CLI name in <dir> to this binary
  -v, --version            print the version
Other Claude Code flags are accepted and ignored.
";

/// Run the fake agent. `name` is the basename it was invoked as.
pub fn run(name: &str, argv: &[String], env: &Env, input: &mut dyn BufRead, out: &mut dyn Write) -> i32 {
    let args = match parse_args(argv) {
        Ok(a) => a,
        Err(e) => {
            let _ = writeln!(out, "{e}");
            return 1;
        }
    };
    if args.version {
        let _ = writeln!(out, "{CLAUDE_VERSION} (Claude Code; k2-fake-agent {})", env!("CARGO_PKG_VERSION"));
        return 0;
    }
    if args.help {
        let _ = write!(out, "{USAGE}");
        return 0;
    }
    if let Some(dir) = &args.install_shims {
        let exe = match std::env::current_exe() {
            Ok(e) => e,
            Err(e) => {
                let _ = writeln!(out, "error: current_exe: {e}");
                return 1;
            }
        };
        return match install_shims(dir, &exe) {
            Ok(()) => 0,
            Err(e) => {
                let _ = writeln!(out, "error: install shims in {}: {e}", dir.display());
                1
            }
        };
    }
    let mut s = match Session::open(&args, env) {
        Ok(s) => s,
        Err(e) => {
            let _ = writeln!(out, "{e}");
            return 1;
        }
    };
    let source = if s.resumed { "resume" } else { "startup" };
    s.trace("cli.exec", json!({ "as": name, "argv": argv, "resumed": s.resumed }));
    s.fire("SessionStart", Some(source), json!({ "source": source }));
    if args.print {
        let prompt = args.prompt.clone().unwrap_or_default();
        let code = match s.turn(&prompt, out) {
            Ok(_) => 0,
            Err(e) => {
                let _ = writeln!(out, "error: {e}");
                1
            }
        };
        s.fire("SessionEnd", None, json!({ "reason": "other" }));
        s.trace("cli.exit", json!({ "code": code }));
        return code;
    }
    let _ = writeln!(
        out,
        "k2-fake-agent (as {name}) · session {} · {}",
        s.id,
        if s.resumed { "resumed" } else { "new" }
    );
    if let Some(p) = args.prompt.clone().filter(|p| !p.trim().is_empty()) {
        if let Err(e) = s.turn(p.trim(), out) {
            let _ = writeln!(out, "error: {e}");
            return 1;
        }
    }
    let mut code = 0;
    let mut reason = "other";
    loop {
        let _ = write!(out, "> ");
        let _ = out.flush();
        let mut line = String::new();
        match input.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => {
                let _ = writeln!(out, "error: read: {e}");
                code = 1;
                break;
            }
        }
        let prompt = line.trim_matches(|c| c == '\r' || c == '\n').trim();
        if prompt.is_empty() {
            continue;
        }
        if prompt == "/exit" {
            reason = "prompt_input_exit";
            break;
        }
        if let Err(e) = s.turn(prompt, out) {
            let _ = writeln!(out, "error: {e}");
            code = 1;
            break;
        }
    }
    s.fire("SessionEnd", None, json!({ "reason": reason }));
    s.trace("cli.exit", json!({ "code": code }));
    code
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn turn_durations_are_a_function_of_the_seed() {
        let one: Vec<u64> = (1..=20).map(|n| turn_duration_ms(1000, 7, n)).collect();
        let again: Vec<u64> = (1..=20).map(|n| turn_duration_ms(1000, 7, n)).collect();
        let other: Vec<u64> = (1..=20).map(|n| turn_duration_ms(1000, 8, n)).collect();
        assert_eq!(one, again);
        assert_ne!(one, other);
        assert!(one.iter().all(|&ms| (500..=1500).contains(&ms)), "{one:?}");
        assert_eq!(turn_duration_ms(0, 7, 3), 0);
    }

    #[test]
    fn parses_the_flags_k2_launches_claude_with() {
        let p = parse_args(&a(&[
            "--dangerously-skip-permissions",
            "--resume",
            "abc",
            "--append-system-prompt",
            "be brief",
            "--model=opus",
            "--permission-mode",
            "plan",
            "do the thing",
        ]))
        .unwrap();
        assert_eq!(p.resume.as_deref(), Some("abc"));
        assert_eq!(p.append_system_prompt.as_deref(), Some("be brief"));
        assert_eq!(p.model.as_deref(), Some("opus"));
        assert_eq!(p.prompt.as_deref(), Some("do the thing"));
        assert_eq!(p.ignored, ["--dangerously-skip-permissions", "--permission-mode=plan"]);
        assert_eq!(parse_args(&a(&["--session-id"])).unwrap_err(), "error: option '--session-id' argument missing");
        assert!(parse_args(&a(&["--turn-ms", "soon"])).is_err());
        assert_eq!(parse_args(&a(&["--", "-p", "x"])).unwrap().prompt.as_deref(), Some("-p x"));
    }

    #[test]
    fn slug_matches_claude_project_hash() {
        assert_eq!(project_slug(Path::new("/no/such/dir/My Repo.v2")), "-no-such-dir-My-Repo-v2");
    }

    #[test]
    fn rfc3339_formats_utc_with_millis() {
        let t = UNIX_EPOCH + Duration::from_millis(1_793_000_000_123);
        assert_eq!(rfc3339(t), "2026-10-26T07:33:20.123Z");
        assert_eq!(rfc3339(UNIX_EPOCH), "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn uuids_are_v4_shaped_and_distinct() {
        let (x, y) = (new_uuid(), new_uuid());
        assert_ne!(x, y);
        assert_eq!(x.len(), 36);
        assert_eq!(&x[14..15], "4");
        assert!(matches!(&x[19..20], "8" | "9" | "a" | "b"), "{x}");
    }
}
