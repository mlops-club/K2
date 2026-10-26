//! An agent's portable home (PRD "K2 Distributed Runtime" P3, task T1,
//! FR6; design in `docs/prd/progress/p3-plan.md`, "T1 in detail").
//!
//! A home is the agent's workspace (its git repo, `.git` included, and
//! `.k2/`), its Claude memory and transcripts, and other providers' session
//! files: what `clone::inventory` gathers, with its excludes and secret
//! scrub. A **snapshot** is a [`Manifest`] of those files, each stored once
//! by its sha256 in the controller's [`store::Store`].
//!
//! - [`snapshot::push`]: a host sends the objects the store lacks, then
//!   commits the manifest at the agent's placement epoch. The store assigns
//!   the snapshot's `seq` and refuses a writer that isn't the agent's host
//!   (I4: [`store::Fence`]).
//! - [`restore::restore`]: the next host fetches the newest snapshot's
//!   objects whose bytes it lacks, stages them next to their targets,
//!   commits a journal, then renames them into place. A restore killed
//!   before the commit leaves the old tree; after it, [`restore::recover`]
//!   finishes it.
//!
//! Paths are the same on every host relative to its work root (D2, as the
//! coordinator settled it for T1): a manifest records the workspace's path
//! under the snapshotting host's `K2_WORK_ROOT` (`work_rel`), and a restore
//! puts it at the same place under its own. With the same root everywhere
//! (production) that is the same absolute path. Provider files go under the
//! host's own home; transcripts get their embedded workspace path rewritten
//! when it differs.

pub mod restore;
pub mod snapshot;
pub mod store;

#[cfg(test)]
mod tests;

use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub use crate::clone::DestinationClass as Class;

pub const MANIFEST_VERSION: u32 = 1;
/// `K2_HOME_MAX_BYTES`: a home over this (after excludes) isn't snapshot;
/// the agent stays where it is (D12).
pub const MAX_BYTES_VAR: &str = "K2_HOME_MAX_BYTES";
pub const DEFAULT_MAX_BYTES: u64 = 2 << 30;
/// One file over this fails the snapshot the same way.
pub const MAX_OBJECT_BYTES: u64 = 256 << 20;
/// Prefix of a restore's staged files, next to their targets.
pub const STAGED_PREFIX: &str = ".k2-restore-";

/// One file of a home.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub class: Class,
    /// Relative to the class's root: the workspace for `workspace`;
    /// `<slug>/memory/` for `memory`; `~/.claude/projects/` for `session`
    /// (`<slug>/<id>.jsonl`); `providers/` for `provider` (`codex/…`).
    pub path: String,
    pub sha256: String,
    pub size: u64,
    /// Permission bits, without setuid, setgid and sticky.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<u32>,
}

/// A snapshot: every file of the home at one moment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub agent: String,
    /// The placement epoch it was taken at.
    pub epoch: u64,
    pub host: String,
    pub created_ms: i64,
    /// The workspace path on the host that took it.
    pub project_path: String,
    /// The same path relative to that host's work root (`K2_WORK_ROOT`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_rel: Option<String>,
    /// `claude_project_hash(project_path)` where it was taken.
    pub slug: String,
    /// The snapshot this host last held, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_seq: Option<u64>,
    pub entries: Vec<Entry>,
    /// Files left out as secrets (clone's classifier).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scrubbed: Vec<String>,
}

impl Manifest {
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a manifest always serializes")
    }

    /// Parse and [`Self::check`].
    pub fn decode(bytes: &[u8]) -> Result<Manifest, String> {
        let m: Manifest = serde_json::from_slice(bytes).map_err(|e| format!("manifest: {e}"))?;
        m.check()?;
        Ok(m)
    }

    /// Every path relative and inside its root, every hash well formed,
    /// the project path absolute.
    pub fn check(&self) -> Result<(), String> {
        if self.version != MANIFEST_VERSION {
            return Err(format!("manifest version {} (this build reads {MANIFEST_VERSION})", self.version));
        }
        if !Path::new(&self.project_path).is_absolute() {
            return Err(format!("manifest project_path {:?} is not absolute", self.project_path));
        }
        if let Some(rel) = &self.work_rel {
            safe_rel(rel)?;
        }
        for e in &self.entries {
            safe_rel(&e.path)?;
            if !is_sha(&e.sha256) {
                return Err(format!("manifest entry {}: bad sha256 {:?}", e.path, e.sha256));
            }
        }
        Ok(())
    }

    /// Total bytes of its files.
    pub fn bytes(&self) -> u64 {
        self.entries.iter().map(|e| e.size).sum()
    }
}

/// What a commit answers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Committed {
    pub seq: u64,
    pub manifest_sha: String,
}

/// The newest snapshot of an agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Latest {
    pub seq: u64,
    pub epoch: u64,
    pub host: String,
    pub manifest_sha: String,
    pub created_ms: i64,
    pub files: u64,
    pub bytes: u64,
}

/// The controller's store as a host sees it: over HTTP in production
/// (`k2-daemon` `hosts::home`), in process in tests ([`store::InProcess`]).
pub trait Remote {
    /// Which of `shas` the store lacks.
    fn missing(&self, shas: &[String]) -> Result<Vec<String>, String>;
    fn put_object(&self, sha: &str, bytes: &[u8]) -> Result<(), String>;
    fn get_object(&self, sha: &str) -> Result<Vec<u8>, String>;
    /// Commit `manifest` (its objects already stored) as `agent`'s next
    /// snapshot, written at placement `epoch`.
    fn commit(&self, agent: &str, epoch: u64, manifest: &[u8]) -> Result<Committed, String>;
    fn latest(&self, agent: &str) -> Result<Option<Latest>, String>;
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let d = Sha256::digest(bytes);
    d.iter().map(|b| format!("{b:02x}")).collect()
}

/// 64 lowercase hex digits.
pub fn is_sha(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// `rel` as a path under some root: not empty, not absolute, only plain
/// components (no `..`, no `.`), no NUL.
pub fn safe_rel(rel: &str) -> Result<PathBuf, String> {
    let p = Path::new(rel);
    let plain = rel.split('/').all(|c| !c.is_empty() && c != "." && c != "..");
    if !plain || rel.contains('\0') || !p.components().all(|c| matches!(c, Component::Normal(_))) {
        return Err(format!("unsafe path in a home: {rel:?}"));
    }
    Ok(p.to_path_buf())
}

/// A file name that is safe on every host for `key` (an agent id may hold
/// any character).
pub fn file_key(key: &str) -> String {
    sha256_hex(key.as_bytes())[..32].to_string()
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Permission bits to keep: rwx for user, group, other.
pub(crate) fn file_mode(meta: &std::fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt;
    Some(meta.permissions().mode() & 0o777)
}
