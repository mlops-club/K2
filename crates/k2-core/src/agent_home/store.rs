//! The controller's home store: objects by sha256 under `objects/`, and
//! the snapshots of each agent in `index.db`, its own SQLite file (never
//! the shared `k2.db`; T1-g).

use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};

use super::{is_sha, now_ms, sha256_hex, Committed, Latest, Manifest, Remote};

/// What a commit's writer must be (I4), checked by the caller's fence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fence<'a> {
    pub agent: &'a str,
    pub host: &'a str,
    pub epoch: u64,
    /// The agent's newest snapshot so far.
    pub newest: Option<&'a Latest>,
}

/// Why a commit was not taken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitError {
    /// Not this writer (I4): HTTP 409.
    Refused(String),
    /// A bad manifest, or objects it names are missing: HTTP 422.
    Invalid(String),
    Internal(String),
}

impl std::fmt::Display for CommitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommitError::Refused(m) => write!(f, "refused: {m}"),
            CommitError::Invalid(m) => write!(f, "invalid: {m}"),
            CommitError::Internal(m) => write!(f, "internal: {m}"),
        }
    }
}

pub struct Store {
    objects: PathBuf,
    db: Mutex<Connection>,
}

impl Store {
    /// The store in `root` (`<K2_HOME>/homes` on a controller), created
    /// when missing.
    pub fn open(root: &Path) -> Result<Store, String> {
        let objects = root.join("objects");
        std::fs::create_dir_all(&objects).map_err(|e| format!("create {}: {e}", objects.display()))?;
        let path = root.join("index.db");
        let c = Connection::open(&path).map_err(|e| format!("open {}: {e}", path.display()))?;
        c.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS snapshots (
                 agent TEXT NOT NULL,
                 seq INTEGER NOT NULL,
                 epoch INTEGER NOT NULL,
                 host TEXT NOT NULL,
                 manifest_sha TEXT NOT NULL,
                 created_ms INTEGER NOT NULL,
                 files INTEGER NOT NULL,
                 bytes INTEGER NOT NULL,
                 PRIMARY KEY (agent, seq)
             );",
        )
        .map_err(|e| format!("{}: {e}", path.display()))?;
        Ok(Store { objects, db: Mutex::new(c) })
    }

    fn object_path(&self, sha: &str) -> PathBuf {
        self.objects.join(&sha[..2]).join(&sha[2..])
    }

    pub fn has(&self, sha: &str) -> bool {
        is_sha(sha) && self.object_path(sha).is_file()
    }

    pub fn missing(&self, shas: &[String]) -> Vec<String> {
        let mut out: Vec<String> = shas.iter().filter(|s| !self.has(s)).cloned().collect();
        out.sort();
        out.dedup();
        out
    }

    /// Store `bytes` as `sha`, after checking the hash. `Ok(false)` when
    /// it was already there.
    pub fn put(&self, sha: &str, bytes: &[u8]) -> Result<bool, String> {
        if !is_sha(sha) {
            return Err(format!("bad object name {sha:?}"));
        }
        let got = sha256_hex(bytes);
        if got != sha {
            return Err(format!("object {sha}: the bytes hash to {got}"));
        }
        let path = self.object_path(sha);
        if path.is_file() {
            return Ok(false);
        }
        let dir = path.parent().expect("an object has a parent dir");
        std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
        let tmp = dir.join(format!(".{}.{}.tmp", &sha[2..], std::process::id()));
        std::fs::write(&tmp, bytes).map_err(|e| format!("write {}: {e}", tmp.display()))?;
        std::fs::rename(&tmp, &path).map_err(|e| format!("rename {}: {e}", path.display()))?;
        Ok(true)
    }

    /// The object's bytes, checked against its name.
    pub fn get(&self, sha: &str) -> Result<Vec<u8>, String> {
        if !is_sha(sha) {
            return Err(format!("bad object name {sha:?}"));
        }
        let bytes = std::fs::read(self.object_path(sha)).map_err(|e| format!("object {sha}: {e}"))?;
        let got = sha256_hex(&bytes);
        if got != sha {
            return Err(format!("object {sha} is corrupt in the store (hashes to {got})"));
        }
        Ok(bytes)
    }

    /// Take `manifest` as `agent`'s next snapshot from `host` at `epoch`,
    /// if `fence` admits that writer and every object it names is stored.
    pub fn commit(
        &self,
        agent: &str,
        host: &str,
        epoch: u64,
        manifest: &[u8],
        fence: &dyn Fn(&Fence) -> Result<(), String>,
    ) -> Result<Committed, CommitError> {
        let m = Manifest::decode(manifest).map_err(CommitError::Invalid)?;
        if m.agent != agent || m.epoch != epoch || m.host != host {
            return Err(CommitError::Invalid(format!(
                "the manifest is for agent {} epoch {} from {}, the commit for agent {agent} epoch {epoch} from {host}",
                m.agent, m.epoch, m.host
            )));
        }
        let shas: Vec<String> = m.entries.iter().map(|e| e.sha256.clone()).collect();
        let missing = self.missing(&shas);
        if !missing.is_empty() {
            return Err(CommitError::Invalid(format!("{} object(s) not in the store, first {}", missing.len(), missing[0])));
        }
        let db = self.db.lock();
        let newest = latest_in(&db, agent).map_err(CommitError::Internal)?;
        fence(&Fence { agent, host, epoch, newest: newest.as_ref() }).map_err(CommitError::Refused)?;
        let manifest_sha = sha256_hex(manifest);
        self.put(&manifest_sha, manifest).map_err(CommitError::Internal)?;
        let seq = newest.as_ref().map_or(1, |l| l.seq + 1);
        db.execute(
            "INSERT INTO snapshots (agent, seq, epoch, host, manifest_sha, created_ms, files, bytes)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![agent, seq as i64, epoch as i64, host, manifest_sha, now_ms(), m.entries.len() as i64, m.bytes() as i64],
        )
        .map_err(|e| CommitError::Internal(format!("index: {e}")))?;
        Ok(Committed { seq, manifest_sha })
    }

    pub fn latest(&self, agent: &str) -> Result<Option<Latest>, String> {
        latest_in(&self.db.lock(), agent)
    }

    /// Every agent's newest snapshot (status).
    pub fn all_latest(&self) -> Result<Vec<(String, Latest)>, String> {
        let db = self.db.lock();
        let mut q = db
            .prepare(
                "SELECT agent, seq, epoch, host, manifest_sha, created_ms, files, bytes FROM snapshots s
                 WHERE seq = (SELECT MAX(seq) FROM snapshots WHERE agent = s.agent) ORDER BY agent",
            )
            .map_err(|e| format!("index: {e}"))?;
        let rows = q
            .query_map([], |r| Ok((r.get::<_, String>(0)?, row_latest(r, 1)?)))
            .map_err(|e| format!("index: {e}"))?;
        rows.collect::<Result<Vec<_>, _>>().map_err(|e| format!("index: {e}"))
    }
}

fn row_latest(r: &rusqlite::Row, at: usize) -> rusqlite::Result<Latest> {
    Ok(Latest {
        seq: r.get::<_, i64>(at)? as u64,
        epoch: r.get::<_, i64>(at + 1)? as u64,
        host: r.get(at + 2)?,
        manifest_sha: r.get(at + 3)?,
        created_ms: r.get(at + 4)?,
        files: r.get::<_, i64>(at + 5)? as u64,
        bytes: r.get::<_, i64>(at + 6)? as u64,
    })
}

fn latest_in(db: &Connection, agent: &str) -> Result<Option<Latest>, String> {
    db.query_row(
        "SELECT seq, epoch, host, manifest_sha, created_ms, files, bytes FROM snapshots
         WHERE agent = ?1 ORDER BY seq DESC LIMIT 1",
        params![agent],
        |r| row_latest(r, 0),
    )
    .optional()
    .map_err(|e| format!("index: {e}"))
}

/// The store as one host's [`Remote`], in process, with the caller's
/// fence (tests; a controller's own use).
pub struct InProcess<'a> {
    pub store: &'a Store,
    pub host: String,
    pub fence: Box<dyn Fn(&Fence) -> Result<(), String> + 'a>,
}

impl Remote for InProcess<'_> {
    fn missing(&self, shas: &[String]) -> Result<Vec<String>, String> {
        Ok(self.store.missing(shas))
    }
    fn put_object(&self, sha: &str, bytes: &[u8]) -> Result<(), String> {
        self.store.put(sha, bytes).map(|_| ())
    }
    fn get_object(&self, sha: &str) -> Result<Vec<u8>, String> {
        self.store.get(sha)
    }
    fn commit(&self, agent: &str, epoch: u64, manifest: &[u8]) -> Result<Committed, String> {
        self.store.commit(agent, &self.host, epoch, manifest, &*self.fence).map_err(|e| e.to_string())
    }
    fn latest(&self, agent: &str) -> Result<Option<Latest>, String> {
        self.store.latest(agent)
    }
}
