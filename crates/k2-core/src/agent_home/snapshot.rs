//! Take a snapshot on a host and push it: [`push`].

use std::collections::{BTreeMap, HashMap};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use super::{file_mode, now_ms, sha256_hex, Class, Entry, Manifest, Remote, MAX_OBJECT_BYTES, STAGED_PREFIX};
use crate::clone::{inventory, CloneOptions};

/// Limits and where the provider files are.
#[derive(Debug, Clone)]
pub struct Options {
    /// The host's home: `~/.claude/projects/…` and the other providers.
    pub home: PathBuf,
    /// The whole home's cap (`K2_HOME_MAX_BYTES`, D12).
    pub max_bytes: u64,
    /// One file's cap.
    pub max_object: u64,
    /// The host's work root: the manifest records the workspace's path
    /// relative to it, so another host can put it under its own.
    pub work_root: Option<PathBuf>,
    /// The hashes of files already read, so a snapshot taken again (every
    /// turn, T2) reads only the files that changed.
    pub cache: Option<Arc<HashCache>>,
}

impl Options {
    pub fn new(home: &Path) -> Options {
        Options {
            home: home.to_path_buf(),
            max_bytes: super::DEFAULT_MAX_BYTES,
            max_object: MAX_OBJECT_BYTES,
            work_root: None,
            cache: None,
        }
    }
}

/// A file as it was when it was hashed. Any write moves `ctime`, which
/// nothing can set back, so an equal stamp means the same bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Stamp {
    size: u64,
    mtime_ns: i128,
    ctime_ns: i128,
    ino: u64,
    dev: u64,
}

impl Stamp {
    fn of(m: &std::fs::Metadata) -> Stamp {
        let ns = |s: i64, n: i64| s as i128 * 1_000_000_000 + n as i128;
        Stamp {
            size: m.len(),
            mtime_ns: ns(m.mtime(), m.mtime_nsec()),
            ctime_ns: ns(m.ctime(), m.ctime_nsec()),
            ino: m.ino(),
            dev: m.dev(),
        }
    }
}

/// A file written this close to its hashing may be written again within
/// the same clock tick: it is not cached (git's "racy" files).
pub const RACY: Duration = Duration::from_secs(2);

/// sha256 by path and [`Stamp`], one per host process.
pub struct HashCache {
    entries: Mutex<HashMap<PathBuf, (Stamp, String)>>,
    racy: Duration,
}

impl std::fmt::Debug for HashCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashCache").field("files", &self.len()).field("racy", &self.racy).finish()
    }
}

impl Default for HashCache {
    fn default() -> Self {
        HashCache::with_racy(RACY)
    }
}

impl HashCache {
    /// A cache that trusts files last written more than `racy` before
    /// they were hashed.
    pub fn with_racy(racy: Duration) -> Self {
        HashCache { entries: Mutex::default(), racy }
    }

    fn get(&self, path: &Path, stamp: &Stamp) -> Option<String> {
        self.entries.lock().get(path).filter(|(s, _)| s == stamp).map(|(_, sha)| sha.clone())
    }

    fn put(&self, path: &Path, stamp: Stamp, sha: &str, hashed_at: SystemTime) {
        let at = hashed_at.duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as i128).unwrap_or(0);
        let racy = self.racy.as_nanos() as i128;
        if stamp.mtime_ns + racy < at && stamp.ctime_ns + racy < at {
            self.entries.lock().insert(path.to_path_buf(), (stamp, sha.to_string()));
        } else {
            self.entries.lock().remove(path);
        }
    }

    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// What a push did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pushed {
    pub seq: u64,
    pub manifest_sha: String,
    pub manifest: Manifest,
    pub files: usize,
    pub bytes: u64,
    /// Objects the store lacked, and their bytes: all of them the first
    /// time, only the changed files after.
    pub sent_objects: usize,
    pub sent_bytes: u64,
    /// Files read to hash them (the rest came from [`Options::cache`]).
    pub hashed: usize,
}

/// Does a workspace file travel? Not git's hooks (code another host would
/// run) nor its lock files (T1-a), nor a restore's staged files.
fn travels(class: Class, rel: &str) -> bool {
    let name = rel.rsplit('/').next().unwrap_or(rel);
    if name.starts_with(STAGED_PREFIX) {
        return false;
    }
    if class != Class::Workspace {
        return true;
    }
    let parts: Vec<&str> = rel.split('/').collect();
    let Some(git) = parts.iter().position(|p| *p == ".git") else { return true };
    if name.ends_with(".lock") {
        return false;
    }
    !(parts.get(git + 1) == Some(&"hooks") && !name.ends_with(".sample"))
}

/// What [`collect`] found: the resolved project path, its slug, every file
/// with its source path (sorted), and the files left out as secrets.
pub type Collected = (String, String, Vec<(Entry, PathBuf)>, Vec<String>);

/// The files of `project_path`'s home.
pub fn collect(project_path: &str, opts: &Options) -> Result<Collected, String> {
    let inv = inventory(
        project_path,
        CloneOptions { include_all_history: true, carry_secrets: false, home_override: Some(opts.home.clone()) },
    )?;
    let mut total = 0u64;
    let mut out = Vec::new();
    for e in inv.entries {
        if !travels(e.class, &e.rel_path) {
            continue;
        }
        let meta = std::fs::metadata(&e.abs_path).map_err(|err| format!("{}: {err}", e.abs_path.display()))?;
        if meta.len() > opts.max_object {
            return Err(format!(
                "home too large: {} is {} bytes (one file may be {} at most); the agent stays on this host",
                e.rel_path,
                meta.len(),
                opts.max_object
            ));
        }
        total += meta.len();
        let entry = Entry { class: e.class, path: e.rel_path, sha256: String::new(), size: meta.len(), mode: file_mode(&meta) };
        out.push((entry, e.abs_path));
    }
    if total > opts.max_bytes {
        return Err(format!(
            "home too large: {total} bytes after excludes ({} at most, {}); the agent stays on this host",
            opts.max_bytes,
            super::MAX_BYTES_VAR
        ));
    }
    out.sort_by(|a, b| (class_order(a.0.class), &a.0.path).cmp(&(class_order(b.0.class), &b.0.path)));
    Ok((inv.project_path, inv.slug, out, inv.scrubbed_secrets))
}

fn class_order(c: Class) -> u8 {
    match c {
        Class::Workspace => 0,
        Class::Memory => 1,
        Class::Session => 2,
        Class::Provider => 3,
    }
}

/// Snapshot `project_path`'s home and push it to `remote` as `agent`'s
/// next snapshot from `host` at placement `epoch`.
pub fn push(
    remote: &dyn Remote,
    agent: &str,
    epoch: u64,
    host: &str,
    project_path: &str,
    parent_seq: Option<u64>,
    opts: &Options,
) -> Result<Pushed, String> {
    let (project, slug, mut files, scrubbed) = collect(project_path, opts)?;
    // Hash every file whose stamp changed; one source path per distinct
    // content.
    let mut by_sha: BTreeMap<String, PathBuf> = BTreeMap::new();
    let mut hashed = 0usize;
    for (entry, abs) in files.iter_mut() {
        let stamp = std::fs::metadata(&*abs).map(|m| Stamp::of(&m)).ok();
        let cached = opts.cache.as_ref().zip(stamp.as_ref()).and_then(|(c, s)| c.get(abs, s));
        match cached {
            Some(sha) => {
                entry.sha256 = sha;
                entry.size = stamp.as_ref().map_or(entry.size, |s| s.size);
            }
            None => {
                let at = SystemTime::now();
                let bytes = std::fs::read(&*abs).map_err(|e| format!("read {}: {e}", abs.display()))?;
                entry.sha256 = sha256_hex(&bytes);
                entry.size = bytes.len() as u64;
                hashed += 1;
                if let (Some(c), Some(s)) = (&opts.cache, stamp) {
                    if s.size == entry.size {
                        c.put(abs, s, &entry.sha256, at);
                    }
                }
            }
        }
        by_sha.entry(entry.sha256.clone()).or_insert_with(|| abs.clone());
    }
    let shas: Vec<String> = by_sha.keys().cloned().collect();
    let missing = remote.missing(&shas)?;
    let mut sent_bytes = 0u64;
    for sha in &missing {
        let abs = by_sha.get(sha).ok_or_else(|| format!("the store asked for {sha}, which this home doesn't have"))?;
        let bytes = std::fs::read(abs).map_err(|e| format!("read {}: {e}", abs.display()))?;
        if sha256_hex(&bytes) != *sha {
            return Err(format!("{} changed while its snapshot was taken; try again", abs.display()));
        }
        remote.put_object(sha, &bytes)?;
        sent_bytes += bytes.len() as u64;
    }
    let work_rel = opts
        .work_root
        .as_ref()
        .and_then(|root| Path::new(&project).strip_prefix(root).ok())
        .map(|r| r.to_string_lossy().into_owned())
        .filter(|r| !r.is_empty());
    let manifest = Manifest {
        version: super::MANIFEST_VERSION,
        agent: agent.to_string(),
        epoch,
        host: host.to_string(),
        created_ms: now_ms(),
        project_path: project,
        work_rel,
        slug,
        parent_seq,
        entries: files.into_iter().map(|(e, _)| e).collect(),
        scrubbed,
    };
    let encoded = manifest.encode();
    let committed = remote.commit(agent, epoch, &encoded)?;
    Ok(Pushed {
        seq: committed.seq,
        manifest_sha: committed.manifest_sha,
        files: manifest.entries.len(),
        bytes: manifest.bytes(),
        manifest,
        sent_objects: missing.len(),
        sent_bytes,
        hashed,
    })
}

#[cfg(test)]
mod travel_tests {
    use super::*;

    #[test]
    fn git_hooks_and_locks_stay_behind() {
        assert!(travels(Class::Workspace, ".git/objects/ab/cdef"));
        assert!(travels(Class::Workspace, ".git/refs/heads/main"));
        assert!(travels(Class::Workspace, ".git/hooks/pre-commit.sample"));
        assert!(!travels(Class::Workspace, ".git/hooks/pre-commit"));
        assert!(!travels(Class::Workspace, ".git/index.lock"));
        assert!(!travels(Class::Workspace, "sub/.git/hooks/post-checkout"));
        assert!(travels(Class::Workspace, "hooks/pre-commit"), "a workspace's own hooks dir is a file like any other");
        assert!(!travels(Class::Workspace, &format!("src/{STAGED_PREFIX}3-a.rs")));
        assert!(travels(Class::Memory, "MEMORY.md"));
    }
}
