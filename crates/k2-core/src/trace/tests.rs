//! The trace sink: format, off by default, whole lines under concurrency.

use super::*;
use crate::test_env::{self, EnvVar};
use chrono::TimeZone;

fn temp_file(tag: &str) -> PathBuf {
    let dir = test_env::unique_temp_path(tag);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir.join("trace.jsonl")
}

fn read_lines(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .expect("read trace")
        .lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("not a whole JSON line: {l:?}: {e}")))
        .collect()
}

#[test]
fn a_line_has_the_lab_format_in_order() {
    let at = Utc.with_ymd_and_hms(2026, 11, 14, 9, 12, 3).unwrap() + chrono::Duration::milliseconds(123);
    let ev = Event::new("host.spawn")
        .agent("a1b2")
        .session(Some("sess-1".into()))
        .attr("harness", "claude");
    assert_eq!(
        format_line(&ev, at),
        r#"{"ts":"2026-11-14T09:12:03.123Z","event":"host.spawn","src":"k2-daemon","agent_id":"a1b2","msg_id":null,"host":"local","session_id":"sess-1","epoch":null,"attrs":{"harness":"claude"}}"#
    );
}

#[test]
fn ts_always_has_milliseconds() {
    let at = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
    assert_eq!(format_ts(at), "2026-01-02T03:04:05.000Z");
}

#[test]
fn unset_or_empty_is_off() {
    assert_eq!(config_from(None), Ok(None));
    assert_eq!(config_from(Some(OsString::new())), Ok(None));
}

#[test]
fn a_relative_path_is_an_error_naming_the_value() {
    let err = config_from(Some(OsString::from("run/trace.jsonl"))).unwrap_err();
    assert_eq!(err, ConfigError::NotAbsolute("run/trace.jsonl".into()));
    assert_eq!(err.to_string(), r#"K2_TRACE_FILE="run/trace.jsonl" is not an absolute path"#);
}

#[test]
fn an_absolute_path_is_on() {
    assert_eq!(config_from(Some(OsString::from("/tmp/x/trace.jsonl"))), Ok(Some(PathBuf::from("/tmp/x/trace.jsonl"))));
}

#[test]
fn the_boot_check_opens_the_file_and_names_a_bad_value() {
    assert_eq!(check_from(None), Ok(()));
    assert!(matches!(check_from(Some(OsString::from("t.jsonl"))), Err(ConfigError::NotAbsolute(_))));
    let path = temp_file("trace-boot-check");
    assert_eq!(check_from(Some(path.clone().into_os_string())), Ok(()));
    assert!(path.exists(), "the boot check creates the trace file");
    let dir = path.parent().unwrap().to_path_buf();
    let err = check_from(Some(dir.clone().into_os_string())).unwrap_err();
    assert!(matches!(&err, ConfigError::Open { path, .. } if *path == dir.to_string_lossy()), "{err}");
    assert!(err.to_string().contains("cannot be opened for append"), "{err}");
}

#[test]
fn off_by_default_nothing_runs() {
    // The env lock holds K2_TRACE_FILE unset (no test sets it; the gate
    // runs with every K2_* cleared).
    let _unset = EnvVar::remove(ENV_VAR);
    assert!(!on());
    assert_eq!(boot_check(), Ok(()));
    let mut called = false;
    emit(|| {
        called = true;
        Event::new("never.built")
    });
    assert!(!called, "emit must not build an event when tracing is off");
    let scope = spawn_request("agent-x", Some("claude"));
    assert_eq!(current_spawn_agent(), None);
    drop(scope);
    assert_eq!(correlation("pane"), None);
}

#[test]
fn a_file_that_cannot_be_opened_is_an_error() {
    // A directory where the file must be (EISDIR), not a chmod.
    let dir = test_env::unique_temp_path("trace-dir");
    std::fs::create_dir_all(&dir).expect("create dir");
    assert!(Sink::open(&dir).is_err());
}

#[test]
fn a_sink_writes_whole_lines_and_appends() {
    let path = temp_file("trace-sink");
    std::fs::write(&path, "{\"ts\":\"x\",\"event\":\"earlier.line\"}\n").expect("seed");
    let sink = Sink::open(&path).expect("open");
    sink.send(Event::new("a.one").attr("n", 1));
    sink.send(Event::new("a.two").pane("p-1"));
    sink.flush();
    let lines = read_lines(&path);
    let names: Vec<&str> = lines.iter().map(|l| l["event"].as_str().unwrap()).collect();
    assert_eq!(names, ["earlier.line", "a.one", "a.two"]);
    assert_eq!(lines[1]["attrs"], serde_json::json!({"n": 1}));
    assert_eq!(lines[2]["attrs"]["pane"], "p-1");
    assert_eq!(sink.dropped(), 0);
}

#[test]
fn concurrent_writers_produce_whole_lines() {
    const THREADS: usize = 8;
    const PER: usize = 500;
    let path = temp_file("trace-concurrent");
    // Two sinks on one file stand in for two processes (daemon and fake
    // agent); four threads write through each.
    let sinks = [Arc::new(Sink::open(&path).expect("open")), Arc::new(Sink::open(&path).expect("open"))];
    let long = "x".repeat(3000);
    std::thread::scope(|s| {
        for t in 0..THREADS {
            let sink = Arc::clone(&sinks[t % 2]);
            let long = long.as_str();
            s.spawn(move || {
                for i in 0..PER {
                    sink.send(Event::new("load.line").attr("t", t).attr("i", i).attr("pad", long));
                }
            });
        }
    });
    for s in &sinks {
        s.flush();
    }
    let dropped: u64 = sinks.iter().map(|s| s.dropped()).sum();
    assert_eq!(dropped, 0, "the queue holds this load");
    let lines = read_lines(&path);
    assert_eq!(lines.len(), THREADS * PER);
    let mut seen = std::collections::HashSet::new();
    for l in &lines {
        assert_eq!(l["attrs"]["pad"].as_str().map(str::len), Some(3000));
        assert!(seen.insert((l["attrs"]["t"].as_u64().unwrap(), l["attrs"]["i"].as_u64().unwrap())));
    }
}

#[test]
fn a_full_queue_drops_and_reports_the_count() {
    let path = temp_file("trace-full");
    let sink = Sink::open_with_capacity(&path, 1).expect("open");
    // Fill well past the capacity faster than the writer drains it; at
    // least one send must have found the queue full.
    for i in 0..10_000 {
        sink.send(Event::new("burst.line").attr("i", i));
    }
    sink.flush();
    let dropped = sink.dropped();
    assert!(dropped > 0, "capacity 1 and 10000 sends must drop some");
    sink.send(Event::new("after.burst"));
    sink.flush();
    let lines = read_lines(&path);
    let written = lines.iter().filter(|l| l["event"] == "burst.line").count() as u64;
    assert_eq!(written + dropped, 10_000);
    let reported: u64 = lines
        .iter()
        .filter(|l| l["event"] == "trace.dropped")
        .map(|l| l["attrs"]["count"].as_u64().unwrap())
        .sum();
    assert_eq!(reported, dropped, "every drop is reported in the trace");
}

#[cfg(unix)]
#[test]
fn child_exit_event_says_signal_and_clean() {
    use std::os::unix::process::ExitStatusExt;
    let killed = pty::child_exit_event("p", std::process::ExitStatus::from_raw(9));
    assert_eq!(killed.attrs["signal"], 9);
    assert_eq!(killed.attrs["clean"], false);
    assert_eq!(killed.attrs["code"], Value::Null);
    let ok = pty::child_exit_event("p", std::process::ExitStatus::from_raw(0));
    assert_eq!(ok.attrs["code"], 0);
    assert_eq!(ok.attrs["clean"], true);
}
