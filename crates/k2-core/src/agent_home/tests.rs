//! T1 unit acceptance (`docs/prd/progress/p3-plan.md`, task T1): round
//! trip byte for byte, excludes, increments, the store's fence, corrupt
//! objects, a killed restore, the overlay. Two "hosts" are two homes and
//! two state dirs; the workspace path is the same for both (D2), so a move
//! deletes it first, as another machine wouldn't have it.

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use super::restore::{self, Kill, Outcome, Target};
use super::snapshot::{self, Options};
use super::store::{CommitError, Fence, InProcess, Store};
use super::*;
use crate::chat_history::claude_project_hash;

struct TempDir(PathBuf);
impl TempDir {
    fn new(prefix: &str) -> Self {
        static CTR: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = CTR.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
        let p = std::env::temp_dir().join(format!("{prefix}-{}-{nanos}-{n}", std::process::id()));
        fs::create_dir_all(&p).expect("create temp dir");
        // Canonical, so the slug is the one every reader computes.
        TempDir(fs::canonicalize(&p).expect("canonicalize temp dir"))
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn write(p: &Path, s: &str) {
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    fs::write(p, s).unwrap();
}

fn git(dir: &Path, home: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("HOME", home)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", home.join("no-gitconfig"))
        .env("GIT_AUTHOR_NAME", "lab")
        .env("GIT_AUTHOR_EMAIL", "lab@example.invalid")
        .env("GIT_COMMITTER_NAME", "lab")
        .env("GIT_COMMITTER_EMAIL", "lab@example.invalid")
        .output()
        .unwrap_or_else(|e| panic!("git {args:?}: {e} (git is needed for this test)"));
    assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// A lab of two hosts and a store.
struct Lab {
    _root: TempDir,
    project: PathBuf,
    a: Target,
    b: Target,
    store: Store,
}

fn lab() -> Lab {
    let root = TempDir::new("k2-agent-home");
    let project = root.0.join("work").join("notes");
    let target = |n: &str| Target {
        home: root.0.join(n).join("home"),
        state: root.0.join(n).join("state"),
        work_root: Some(root.0.join("work")),
    };
    let (a, b) = (target("host-a"), target("host-b"));
    for t in [&a, &b] {
        fs::create_dir_all(&t.home).unwrap();
    }
    fs::create_dir_all(&project).unwrap();
    let store = Store::open(&root.0.join("controller").join("homes")).unwrap();
    Lab { project, a, b, store, _root: root }
}

/// The agent's home on host `t`: a git repo with history, a dirty and an
/// untracked file, `.k2/`, an executable script, the Claude memory and a
/// transcript, a secret and a bulk dir.
fn seed(l: &Lab, t: &Target) {
    let p = &l.project;
    fs::create_dir_all(p).unwrap();
    git(p, &t.home, &["init", "-q", "-b", "main"]);
    write(&p.join("README.md"), "# notes\n");
    write(&p.join(".gitignore"), "node_modules/\n");
    write(&p.join("src/lib.rs"), "pub fn one() -> u32 { 1 }\n");
    git(p, &t.home, &["add", "-A"]);
    git(p, &t.home, &["commit", "-q", "-m", "first"]);
    write(&p.join("src/lib.rs"), "pub fn one() -> u32 { 2 }\n");
    git(p, &t.home, &["commit", "-q", "-am", "second"]);
    write(&p.join("src/lib.rs"), "pub fn one() -> u32 { 3 } // dirty\n");
    write(&p.join("notes/untracked.md"), "not added yet\n");
    write(&p.join(".k2/AGENT.md"), "# agent\n");
    write(&p.join("run.sh"), "#!/bin/sh\necho hi\n");
    fs::set_permissions(p.join("run.sh"), fs::Permissions::from_mode(0o755)).unwrap();
    write(&p.join(".env"), "API_KEY=sk-live-abcdefabcdefabcdefabcdef\n");
    write(&p.join("node_modules/dep/index.js"), "module.exports = 1\n");
    write(&p.join(".git/hooks/pre-commit"), "#!/bin/sh\nexit 0\n");
    let slug_dir = t.home.join(".claude/projects").join(claude_project_hash(&p.to_string_lossy()));
    write(&slug_dir.join("memory/MEMORY.md"), "- remember x\n");
    write(&slug_dir.join("1b4e28ba-2fa1-11d2-883f-0016d3cca427.jsonl"), "{\"type\":\"user\",\"text\":\"remember x\"}\n");
}

/// Every file under `dir`: sha256 and permission bits, skipping what
/// never travels.
fn tree(dir: &Path) -> BTreeMap<String, (String, u32)> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, (String, u32)>) {
        let Ok(rd) = fs::read_dir(dir) else { return };
        for e in rd {
            let p = e.unwrap().path();
            let rel = p.strip_prefix(root).unwrap().to_string_lossy().into_owned();
            let m = fs::symlink_metadata(&p).unwrap();
            if m.is_dir() {
                if !["node_modules"].contains(&rel.as_str()) {
                    walk(root, &p, out);
                }
            } else if !(rel.starts_with(".git/hooks/") && !rel.ends_with(".sample")) && !rel.ends_with(".lock") && rel != ".env" {
                out.insert(rel, (sha256_hex(&fs::read(&p).unwrap()), m.permissions().mode() & 0o777));
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(dir, dir, &mut out);
    out
}

fn open_fence() -> Box<dyn Fn(&Fence) -> Result<(), String>> {
    Box::new(|_| Ok(()))
}

fn remote<'a>(l: &'a Lab, host: &str) -> InProcess<'a> {
    InProcess { store: &l.store, host: host.into(), fence: open_fence() }
}

fn push(l: &Lab, t: &Target, host: &str, epoch: u64) -> snapshot::Pushed {
    let parent = restore::held(&t.state, "notes").map(|h| h.seq);
    let p = snapshot::push(&remote(l, host), "notes", epoch, host, &l.project.to_string_lossy(), parent, &Options::new(&t.home))
        .unwrap_or_else(|e| panic!("push from {host}: {e}"));
    restore::mark(&t.state, "notes", p.seq, &p.manifest_sha, &p.manifest.encode()).unwrap();
    p
}

fn slug_dir(l: &Lab, t: &Target) -> PathBuf {
    t.home.join(".claude/projects").join(claude_project_hash(&l.project.to_string_lossy()))
}

#[test]
fn a_home_round_trips_byte_for_byte_to_another_host() {
    let l = lab();
    seed(&l, &l.a);
    let before = tree(&l.project);
    let memory = fs::read(slug_dir(&l, &l.a).join("memory/MEMORY.md")).unwrap();
    let transcript = fs::read(slug_dir(&l, &l.a).join("1b4e28ba-2fa1-11d2-883f-0016d3cca427.jsonl")).unwrap();
    let pushed = push(&l, &l.a, "host-a", 7);
    assert_eq!(pushed.seq, 1);
    assert!(before.contains_key(".git/HEAD") && pushed.manifest.entries.iter().any(|e| e.path == ".git/HEAD"), "git travels as files (T1-a)");

    // Host B is another machine: no workspace, its own home.
    fs::remove_dir_all(&l.project).unwrap();
    let out = restore::restore(&remote(&l, "host-b"), "notes", &l.b).unwrap();
    assert!(matches!(out, Outcome::Restored { seq: 1, kept: 0, .. }), "{out:?}");

    assert_eq!(tree(&l.project), before, "every workspace file, git's included, with its bits");
    assert_eq!(fs::metadata(l.project.join("run.sh")).unwrap().permissions().mode() & 0o777, 0o755);
    assert_eq!(fs::read(slug_dir(&l, &l.b).join("memory/MEMORY.md")).unwrap(), memory);
    assert_eq!(fs::read(slug_dir(&l, &l.b).join("1b4e28ba-2fa1-11d2-883f-0016d3cca427.jsonl")).unwrap(), transcript);
    // git agrees: same history, same dirty and untracked state.
    let log = git(&l.project, &l.b.home, &["log", "--format=%H %s"]);
    assert_eq!(log.lines().map(|l| l.split_once(' ').unwrap().1).collect::<Vec<_>>(), ["second", "first"]);
    let status = git(&l.project, &l.b.home, &["status", "--porcelain"]);
    assert!(status.contains(" M src/lib.rs") && status.contains("?? notes/"), "{status}");
    assert_eq!(restore::held(&l.b.state, "notes").map(|h| h.seq), Some(1));

    // Again: host B already holds it.
    assert_eq!(restore::restore(&remote(&l, "host-b"), "notes", &l.b).unwrap(), Outcome::Current { seq: 1 });
}

#[test]
fn secrets_bulk_dirs_and_git_hooks_stay_behind() {
    let l = lab();
    seed(&l, &l.a);
    let pushed = push(&l, &l.a, "host-a", 1);
    let paths: Vec<&str> = pushed.manifest.entries.iter().map(|e| e.path.as_str()).collect();
    for gone in [".env", "node_modules/dep/index.js", ".git/hooks/pre-commit"] {
        assert!(!paths.contains(&gone), "{gone} must not travel: {paths:?}");
    }
    assert!(pushed.manifest.scrubbed.contains(&".env".to_string()), "{:?}", pushed.manifest.scrubbed);
    assert!(paths.iter().any(|p| p.ends_with(".sample")), "git's sample hooks are inert and travel");
}

#[test]
fn a_second_snapshot_sends_only_what_changed() {
    let l = lab();
    seed(&l, &l.a);
    let first = push(&l, &l.a, "host-a", 1);
    assert_eq!(first.sent_objects, first.manifest.entries.iter().map(|e| &e.sha256).collect::<std::collections::HashSet<_>>().len());
    write(&l.project.join("README.md"), "# notes, edited\n");
    write(&slug_dir(&l, &l.a).join("memory/MEMORY.md"), "- remember x\n- remember y\n");
    let second = push(&l, &l.a, "host-a", 1);
    assert_eq!((second.seq, second.sent_objects), (2, 2), "two files changed, two objects sent");
    assert_eq!(second.manifest.parent_seq, Some(1));
    let third = push(&l, &l.a, "host-a", 1);
    assert_eq!((third.seq, third.sent_objects, third.sent_bytes), (3, 0, 0), "nothing changed, nothing sent");
}

/// T2: a snapshot taken again reads only the files whose stamp changed,
/// and a file written just before it was hashed is read again next time.
#[test]
fn a_hash_cache_reads_only_the_files_that_changed() {
    let l = lab();
    seed(&l, &l.a);
    let snap = |cache: &std::sync::Arc<snapshot::HashCache>| {
        let mut opts = Options::new(&l.a.home);
        opts.cache = Some(std::sync::Arc::clone(cache));
        let parent = restore::held(&l.a.state, "notes").map(|h| h.seq);
        let p = snapshot::push(&remote(&l, "host-a"), "notes", 1, "host-a", &l.project.to_string_lossy(), parent, &opts)
            .unwrap_or_else(|e| panic!("push: {e}"));
        restore::mark(&l.a.state, "notes", p.seq, &p.manifest_sha, &p.manifest.encode()).unwrap();
        p
    };
    let trusting = std::sync::Arc::new(snapshot::HashCache::with_racy(std::time::Duration::ZERO));
    let first = snap(&trusting);
    assert_eq!(first.hashed, first.files, "a cold cache reads every file");
    assert_eq!(trusting.len(), first.files);
    let again = snap(&trusting);
    assert_eq!((again.hashed, again.sent_objects), (0, 0), "nothing changed: no file read, nothing sent");
    assert_eq!(again.manifest.entries, first.manifest.entries, "the cached hashes are the files' hashes");
    // The same size, new bytes: the stamp (ctime) still changes.
    let memory = slug_dir(&l, &l.a).join("memory/MEMORY.md");
    write(&memory, "- remember z\n");
    let changed = snap(&trusting);
    assert_eq!((changed.hashed, changed.sent_objects), (1, 1), "only MEMORY.md is read again and sent");
    let mem = changed.manifest.entries.iter().find(|e| e.path.ends_with("MEMORY.md")).expect("MEMORY.md in the manifest");
    assert_eq!(mem.sha256, sha256_hex(b"- remember z\n"));

    // Files written within the racy window of their hashing are never
    // trusted: every one is read again.
    let wary = std::sync::Arc::new(snapshot::HashCache::with_racy(std::time::Duration::from_secs(3600)));
    let a = snap(&wary);
    let b = snap(&wary);
    assert_eq!((a.hashed, b.hashed, wary.len()), (a.files, b.files, 0));
}

#[test]
fn the_store_takes_a_commit_only_from_the_writer_its_fence_admits() {
    let l = lab();
    seed(&l, &l.a);
    push(&l, &l.a, "host-a", 5);
    // A fence like the controller's: epoch no lower than the newest, same host.
    let fence = |f: &Fence| match f.newest {
        Some(n) if f.epoch < n.epoch => Err(format!("stale epoch {} (newest snapshot at {})", f.epoch, n.epoch)),
        _ => Ok(()),
    };
    let stale = InProcess { store: &l.store, host: "host-b".into(), fence: Box::new(fence) };
    let opts = Options::new(&l.a.home);
    let err = snapshot::push(&stale, "notes", 4, "host-b", &l.project.to_string_lossy(), None, &opts).unwrap_err();
    assert!(err.contains("refused: stale epoch 4"), "{err}");
    assert_eq!(l.store.latest("notes").unwrap().map(|x| (x.seq, x.epoch)), Some((1, 5)), "nothing taken");

    // A manifest whose objects aren't stored, or that names another agent.
    let m = Manifest {
        version: MANIFEST_VERSION,
        agent: "notes".into(),
        epoch: 6,
        host: "host-a".into(),
        created_ms: 0,
        project_path: l.project.to_string_lossy().into_owned(),
        work_rel: None,
        slug: "s".into(),
        parent_seq: None,
        entries: vec![Entry { class: Class::Workspace, path: "x".into(), sha256: "0".repeat(64), size: 1, mode: None }],
        scrubbed: vec![],
    };
    let r = l.store.commit("notes", "host-a", 6, &m.encode(), &|_| Ok(()));
    assert!(matches!(&r, Err(CommitError::Invalid(e)) if e.contains("not in the store")), "{r:?}");
    let r = l.store.commit("other", "host-a", 6, &m.encode(), &|_| Ok(()));
    assert!(matches!(&r, Err(CommitError::Invalid(e)) if e.contains("for agent notes")), "{r:?}");
    let mut bad = m.clone();
    bad.entries[0].path = "../escape".into();
    let r = l.store.commit("notes", "host-a", 6, &serde_json::to_vec(&bad).unwrap(), &|_| Ok(()));
    assert!(matches!(&r, Err(CommitError::Invalid(e)) if e.contains("unsafe path")), "{r:?}");
}

#[test]
fn a_corrupt_object_is_refused_and_changes_nothing() {
    let l = lab();
    assert!(l.store.put(&sha256_hex(b"abc"), b"abd").unwrap_err().contains("hash to"));
    seed(&l, &l.a);
    push(&l, &l.a, "host-a", 1);
    fs::remove_dir_all(&l.project).unwrap();
    // A store (or a wire) that flips a byte of one object.
    struct Flip<'a>(InProcess<'a>);
    impl Remote for Flip<'_> {
        fn missing(&self, s: &[String]) -> Result<Vec<String>, String> {
            self.0.missing(s)
        }
        fn put_object(&self, s: &str, b: &[u8]) -> Result<(), String> {
            self.0.put_object(s, b)
        }
        fn get_object(&self, s: &str) -> Result<Vec<u8>, String> {
            let mut b = self.0.get_object(s)?;
            if b.starts_with(b"# notes") {
                b[0] ^= 1;
            }
            Ok(b)
        }
        fn commit(&self, a: &str, e: u64, m: &[u8]) -> Result<Committed, String> {
            self.0.commit(a, e, m)
        }
        fn latest(&self, a: &str) -> Result<Option<Latest>, String> {
            self.0.latest(a)
        }
    }
    let err = restore::restore(&Flip(remote(&l, "host-b")), "notes", &l.b).unwrap_err();
    assert!(err.contains("doesn't match its hash"), "{err}");
    let left = tree(&l.project);
    assert!(left.is_empty(), "nothing restored, no staged file left: {left:?}");
    assert_eq!(restore::held(&l.b.state, "notes"), None);
    // And an object corrupted on the store's disk is refused there.
    let sha = sha256_hex(b"# notes\n");
    let path = l._root.0.join("controller/homes/objects").join(&sha[..2]).join(&sha[2..]);
    fs::write(&path, b"# nites\n").unwrap();
    assert!(l.store.get(&sha).unwrap_err().contains("corrupt"));
}

#[test]
fn a_restore_killed_before_its_commit_leaves_the_old_tree_and_one_killed_after_finishes() {
    let l = lab();
    seed(&l, &l.a);
    push(&l, &l.a, "host-a", 1);
    // Host B holds snapshot 1.
    fs::remove_dir_all(&l.project).unwrap();
    restore::restore(&remote(&l, "host-b"), "notes", &l.b).unwrap();
    let old = tree(&l.project);
    // Host A (another machine: its own copy of the workspace) moves on to snapshot 2.
    let b_copy = l._root.0.join("b-copy");
    fs::rename(&l.project, &b_copy).unwrap();
    seed(&l, &l.a);
    write(&l.project.join("README.md"), "# notes, on host A\n");
    write(&l.project.join("new.md"), "new on A\n");
    push(&l, &l.a, "host-a", 2);
    let new = tree(&l.project);
    fs::remove_dir_all(&l.project).unwrap();
    fs::rename(&b_copy, &l.project).unwrap();
    assert_eq!(tree(&l.project), old);

    // Killed after staging: the tree is the old one (staged files aside), and recover removes those.
    let err = restore::restore_until(&remote(&l, "host-b"), "notes", &l.b, Some(Kill::AfterStaging)).unwrap_err();
    assert!(err.contains("killed"), "{err}");
    let mid = tree(&l.project);
    assert!(mid.keys().any(|k| k.contains(STAGED_PREFIX)), "staged next to their targets");
    assert_eq!(mid.into_iter().filter(|(k, _)| !k.contains(STAGED_PREFIX)).collect::<BTreeMap<_, _>>(), old);
    assert!(restore::recover(&l.b.state, "notes").unwrap());
    assert_eq!(tree(&l.project), old, "rolled back: exactly the old tree");
    assert_eq!(restore::held(&l.b.state, "notes").map(|h| h.seq), Some(1));

    // Killed after the commit: the next restore finishes it first.
    restore::restore_until(&remote(&l, "host-b"), "notes", &l.b, Some(Kill::AfterCommit)).unwrap_err();
    assert_eq!(restore::restore(&remote(&l, "host-b"), "notes", &l.b).unwrap(), Outcome::Current { seq: 2 });
    assert_eq!(tree(&l.project), new, "rolled forward: exactly snapshot 2");
}

#[test]
fn a_restore_is_an_overlay_that_deletes_only_what_a_snapshot_put_there() {
    let l = lab();
    seed(&l, &l.a);
    write(&l.project.join("old.md"), "will be deleted on A\n");
    push(&l, &l.a, "host-a", 1);
    fs::remove_dir_all(&l.project).unwrap();
    restore::restore(&remote(&l, "host-b"), "notes", &l.b).unwrap();
    // Host B's own files, which never travel: they must survive.
    write(&l.project.join("node_modules/local/index.js"), "local\n");
    write(&l.project.join(".env"), "LOCAL=1\n");
    let b_copy = l._root.0.join("b-copy");
    fs::rename(&l.project, &b_copy).unwrap();
    seed(&l, &l.a);
    write(&l.project.join("old.md"), "will be deleted on A\n");
    push(&l, &l.a, "host-a", 1);
    fs::remove_file(l.project.join("old.md")).unwrap();
    push(&l, &l.a, "host-a", 1);
    fs::remove_dir_all(&l.project).unwrap();
    fs::rename(&b_copy, &l.project).unwrap();

    let out = restore::restore(&remote(&l, "host-b"), "notes", &l.b).unwrap();
    // (Snapshot 3's repo is a new one, so snapshot 1's git objects go too.)
    assert!(matches!(out, Outcome::Restored { seq: 3, deleted, .. } if deleted >= 1), "{out:?}");
    assert!(!l.project.join("old.md").exists(), "deleted: snapshot 1 put it there, snapshot 3 lacks it");
    assert_eq!(fs::read_to_string(l.project.join("node_modules/local/index.js")).unwrap(), "local\n");
    assert_eq!(fs::read_to_string(l.project.join(".env")).unwrap(), "LOCAL=1\n");
}

#[test]
fn a_restore_never_writes_through_a_symlink() {
    let l = lab();
    seed(&l, &l.a);
    push(&l, &l.a, "host-a", 1);
    fs::remove_dir_all(&l.project).unwrap();
    fs::create_dir_all(&l.project).unwrap();
    let elsewhere = l._root.0.join("elsewhere");
    fs::create_dir_all(&elsewhere).unwrap();
    std::os::unix::fs::symlink(&elsewhere, l.project.join("src")).unwrap();
    let err = restore::restore(&remote(&l, "host-b"), "notes", &l.b).unwrap_err();
    assert!(err.contains("symlink"), "{err}");
    assert_eq!(fs::read_dir(&elsewhere).unwrap().count(), 0, "nothing written through it");
}

#[test]
fn a_home_over_its_cap_is_refused_with_the_setting_named() {
    let l = lab();
    seed(&l, &l.a);
    let mut opts = Options::new(&l.a.home);
    opts.max_bytes = 10;
    let err = snapshot::push(&remote(&l, "host-a"), "notes", 1, "host-a", &l.project.to_string_lossy(), None, &opts).unwrap_err();
    assert!(err.contains("home too large") && err.contains(MAX_BYTES_VAR), "{err}");
    assert_eq!(l.store.latest("notes").unwrap(), None);
}

#[test]
fn a_host_with_its_own_work_root_gets_the_workspace_at_the_same_place_under_it() {
    // Separate disks: host A's workspaces under work-a, host B's under work-b.
    let l = lab();
    let (root_a, root_b) = (l._root.0.join("work-a"), l._root.0.join("work-b"));
    let project_a = root_a.join("team").join("notes");
    fs::create_dir_all(&project_a).unwrap();
    write(&project_a.join("README.md"), "# notes\n");
    let pa = project_a.to_string_lossy().into_owned();
    let slug_a = l.a.home.join(".claude/projects").join(claude_project_hash(&pa));
    write(&slug_a.join("memory/MEMORY.md"), "- remember x\n");
    write(&slug_a.join("s1.jsonl"), &format!("{{\"cwd\":\"{pa}\",\"text\":\"hi\"}}\n"));
    let mut opts = Options::new(&l.a.home);
    opts.work_root = Some(root_a.clone());
    let pushed = snapshot::push(&remote(&l, "host-a"), "notes", 1, "host-a", &pa, None, &opts).unwrap();
    assert_eq!(pushed.manifest.work_rel.as_deref(), Some("team/notes"));

    let b = Target { home: l.b.home.clone(), state: l.b.state.clone(), work_root: Some(root_b.clone()) };
    let out = restore::restore(&remote(&l, "host-b"), "notes", &b).unwrap();
    assert!(matches!(out, Outcome::Restored { fetched, .. } if fetched == 3), "every object fetched: {out:?}");
    let project_b = root_b.join("team").join("notes");
    let pb = project_b.to_string_lossy().into_owned();
    assert_eq!(fs::read_to_string(project_b.join("README.md")).unwrap(), "# notes\n");
    let slug_b = l.b.home.join(".claude/projects").join(claude_project_hash(&pb));
    assert_eq!(fs::read_to_string(slug_b.join("memory/MEMORY.md")).unwrap(), "- remember x\n");
    assert_eq!(
        fs::read_to_string(slug_b.join("s1.jsonl")).unwrap(),
        format!("{{\"cwd\":\"{pb}\",\"text\":\"hi\"}}\n"),
        "the transcript names host B's workspace"
    );
    assert_eq!(restore::local_project(&b, "notes"), Some((project_b, Some("team/notes".into()))));
}

#[test]
fn a_workspace_outside_the_work_root_is_never_restored() {
    let l = lab();
    seed(&l, &l.a);
    push(&l, &l.a, "host-a", 1);
    fs::remove_dir_all(&l.project).unwrap();
    let mut b = l.b.clone();
    b.work_root = Some(l._root.0.join("elsewhere"));
    let err = restore::restore(&remote(&l, "host-b"), "notes", &b).unwrap_err();
    assert!(err.contains("not under this host's work root"), "{err}");
    assert!(!l.project.exists(), "nothing created");
}

#[test]
fn a_manifest_with_an_unsafe_path_never_decodes() {
    for p in ["../x", "/etc/passwd", "a/../../b", "", "a/./b"] {
        let m = Manifest {
            version: MANIFEST_VERSION,
            agent: "a".into(),
            epoch: 1,
            host: "h".into(),
            created_ms: 0,
            project_path: "/w/a".into(),
            work_rel: None,
            slug: "s".into(),
            parent_seq: None,
            entries: vec![Entry { class: Class::Workspace, path: p.into(), sha256: "0".repeat(64), size: 0, mode: None }],
            scrubbed: vec![],
        };
        assert!(Manifest::decode(&m.encode()).is_err(), "{p:?} must be refused");
    }
}
