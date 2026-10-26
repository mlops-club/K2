//! Restore an agent's newest snapshot on a host: [`restore`].
//!
//! An overlay (T1-f): files a snapshot holds are written, files an earlier
//! snapshot put here and the new one lacks are deleted, anything else
//! (excluded dirs, `.env`) is left alone. Every new file is first staged
//! next to its target, then a journal is committed, then the staged files
//! are renamed into place. A restore killed before the commit leaves the
//! old tree (the next [`recover`] removes the staged files); one killed
//! after it is finished by the next [`recover`].
//!
//! `state` is the host's own record (`<K2_HOME>/homes` on a host): which
//! snapshot it holds for each agent (`agents/`), the manifests it held
//! (`manifests/`), and the journals of restores in progress (`journal/`).

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::{file_key, safe_rel, sha256_hex, Class, Entry, Manifest, Remote, STAGED_PREFIX};
use crate::chat_history::claude_project_hash;

/// Where a host keeps its homes.
#[derive(Debug, Clone)]
pub struct Target {
    /// The host's home: `~/.claude/projects/…` and the other providers.
    pub home: PathBuf,
    /// The host's record (`<K2_HOME>/homes`).
    pub state: PathBuf,
    /// Only a workspace under this root is restored (`K2_WORK_ROOT`, D2);
    /// `None` = anywhere (tests).
    pub work_root: Option<PathBuf>,
}

/// The snapshot a host holds for an agent: the one it last pushed or
/// restored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Held {
    pub agent: String,
    pub seq: u64,
    pub manifest_sha: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The agent has no snapshot yet.
    NoSnapshot,
    /// This host already holds the newest one.
    Current { seq: u64 },
    Restored { seq: u64, fetched: usize, fetched_bytes: u64, kept: usize, deleted: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Staging,
    Commit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Move {
    staged: PathBuf,
    dest: PathBuf,
    mode: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Journal {
    phase: Phase,
    held: Held,
    moves: Vec<Move>,
    /// Files already right but with other permission bits.
    chmods: Vec<(PathBuf, u32)>,
    deletes: Vec<PathBuf>,
}

/// Where a test stops a restore, as a kill would.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kill {
    AfterStaging,
    AfterCommit,
}

fn held_path(state: &Path, agent: &str) -> PathBuf {
    state.join("agents").join(format!("{}.json", file_key(agent)))
}

fn journal_path(state: &Path, agent: &str) -> PathBuf {
    state.join("journal").join(format!("{}.json", file_key(agent)))
}

fn manifest_path(state: &Path, sha: &str) -> PathBuf {
    state.join("manifests").join(format!("{sha}.json"))
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let dir = path.parent().ok_or_else(|| format!("{} has no parent", path.display()))?;
    std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    let name = path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    let tmp = dir.join(format!(".{name}.{}.tmp", std::process::id()));
    std::fs::write(&tmp, bytes).map_err(|e| format!("write {}: {e}", tmp.display()))?;
    std::fs::File::open(&tmp).and_then(|f| f.sync_all()).map_err(|e| format!("sync {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("rename {}: {e}", path.display()))
}

/// The snapshot this host holds for `agent`.
pub fn held(state: &Path, agent: &str) -> Option<Held> {
    let bytes = std::fs::read(held_path(state, agent)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Record that this host holds `agent`'s snapshot `seq` (after a push:
/// the files here are that snapshot).
pub fn mark(state: &Path, agent: &str, seq: u64, manifest_sha: &str, manifest: &[u8]) -> Result<(), String> {
    write_atomic(&manifest_path(state, manifest_sha), manifest)?;
    let h = Held { agent: agent.to_string(), seq, manifest_sha: manifest_sha.to_string() };
    write_atomic(&held_path(state, agent), &serde_json::to_vec(&h).expect("held serializes"))
}

/// Where `m`'s workspace goes on this host: the same path under this
/// host's work root, or the manifest's own path without one.
pub fn project_for(m: &Manifest, t: &Target) -> PathBuf {
    match (&t.work_root, &m.work_rel) {
        (Some(root), Some(rel)) => root.join(rel),
        _ => PathBuf::from(&m.project_path),
    }
}

/// The snapshot this host holds for `agent`: where its workspace is here,
/// and its path relative to the work root.
pub fn local_project(t: &Target, agent: &str) -> Option<(PathBuf, Option<String>)> {
    let h = held(&t.state, agent)?;
    let m = cached_manifest(&t.state, &h.manifest_sha)?;
    Some((project_for(&m, t), m.work_rel.clone()))
}

/// Does `e`'s content embed the workspace path (transcripts)?
fn embeds_path(e: &Entry) -> bool {
    match e.class {
        Class::Session => true,
        Class::Provider => e.path.ends_with(".jsonl") || e.path.ends_with(".json"),
        _ => false,
    }
}

fn cached_manifest(state: &Path, sha: &str) -> Option<Manifest> {
    let bytes = std::fs::read(manifest_path(state, sha)).ok()?;
    (sha256_hex(&bytes) == sha).then(|| Manifest::decode(&bytes).ok()).flatten()
}

/// Refuse a target reached through a symlink below `root` (a snapshot
/// must never write outside its roots).
fn no_symlink_below(root: &Path, dest: &Path) -> Result<(), String> {
    let rel = dest.strip_prefix(root).map_err(|_| format!("{} is outside {}", dest.display(), root.display()))?;
    let mut p = root.to_path_buf();
    for c in rel.components() {
        p.push(c);
        if let Ok(m) = std::fs::symlink_metadata(&p) {
            if m.file_type().is_symlink() {
                return Err(format!("{} is a symlink; a home is never restored through one", p.display()));
            }
        }
    }
    Ok(())
}

/// Where `e` goes on this host, or `None` for what this build doesn't
/// restore (Hermes's `state.db` merge, T1-h).
fn dest_of(m: &Manifest, e: &Entry, home: &Path, project: &Path, slug: &str) -> Result<Option<(PathBuf, PathBuf)>, String> {
    let rel = safe_rel(&e.path)?;
    let projects = home.join(".claude").join("projects");
    let out = match e.class {
        Class::Workspace => (project.to_path_buf(), project.join(&rel)),
        Class::Memory => (projects.clone(), projects.join(slug).join("memory").join(&rel)),
        Class::Session => {
            let mut comps = rel.components();
            let first = comps.next().map(|c| c.as_os_str().to_string_lossy().into_owned()).unwrap_or_default();
            let renamed = if first == m.slug {
                slug.to_string()
            } else if let Some(branch) = first.strip_prefix(&format!("{}-", m.slug)) {
                format!("{slug}-{branch}")
            } else {
                first
            };
            (projects.clone(), projects.join(renamed).join(comps.as_path()))
        }
        Class::Provider => {
            let p = project.to_string_lossy();
            match crate::clone::providers::reroot_provider(&rel, home, &m.project_path, &p) {
                Some(crate::clone::providers::ProviderDest::File(out)) => (home.to_path_buf(), out),
                _ => return Ok(None),
            }
        }
    };
    if !out.1.starts_with(&out.0) {
        return Err(format!("{} lands outside {}", e.path, out.0.display()));
    }
    no_symlink_below(&out.0, &out.1)?;
    Ok(Some(out))
}

fn staged_for(dest: &Path, seq: u64) -> PathBuf {
    let name = dest.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    dest.with_file_name(format!("{STAGED_PREFIX}{seq}-{name}"))
}

fn file_sha(p: &Path) -> Option<String> {
    let m = std::fs::symlink_metadata(p).ok()?;
    if !m.file_type().is_file() {
        return None;
    }
    std::fs::read(p).ok().map(|b| sha256_hex(&b))
}

fn set_mode(p: &Path, mode: u32) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(p, std::fs::Permissions::from_mode(mode & 0o777)).map_err(|e| format!("chmod {}: {e}", p.display()))
}

/// Finish what a journal says: rename, chmod, delete, record. Each step
/// can run twice.
fn apply(state: &Path, j: &Journal) -> Result<(), String> {
    for mv in &j.moves {
        if mv.staged.exists() {
            if let Some(dir) = mv.dest.parent() {
                std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
            }
            std::fs::rename(&mv.staged, &mv.dest).map_err(|e| format!("rename into {}: {e}", mv.dest.display()))?;
        }
    }
    for (p, mode) in &j.chmods {
        set_mode(p, *mode)?;
    }
    for p in &j.deletes {
        match std::fs::remove_file(p) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(format!("delete {}: {e}", p.display())),
        }
    }
    write_atomic(&held_path(state, &j.held.agent), &serde_json::to_vec(&j.held).expect("held serializes"))?;
    let _ = std::fs::remove_file(journal_path(state, &j.held.agent));
    Ok(())
}

/// Finish or undo a restore of `agent` that was killed: before its commit,
/// remove its staged files (the old tree stays); after, finish it.
/// `Ok(true)` when there was one.
pub fn recover(state: &Path, agent: &str) -> Result<bool, String> {
    let path = journal_path(state, agent);
    let Ok(bytes) = std::fs::read(&path) else { return Ok(false) };
    let j: Journal = serde_json::from_slice(&bytes).map_err(|e| format!("journal {}: {e}", path.display()))?;
    match j.phase {
        Phase::Staging => {
            for mv in &j.moves {
                let _ = std::fs::remove_file(&mv.staged);
            }
            std::fs::remove_file(&path).map_err(|e| format!("remove {}: {e}", path.display()))?;
        }
        Phase::Commit => apply(state, &j)?,
    }
    Ok(true)
}

/// Bring `agent`'s home on this host to its newest snapshot.
pub fn restore(remote: &dyn Remote, agent: &str, t: &Target) -> Result<Outcome, String> {
    restore_until(remote, agent, t, None)
}

pub(crate) fn restore_until(remote: &dyn Remote, agent: &str, t: &Target, kill: Option<Kill>) -> Result<Outcome, String> {
    recover(&t.state, agent)?;
    let Some(latest) = remote.latest(agent)? else { return Ok(Outcome::NoSnapshot) };
    let before = held(&t.state, agent);
    if before.as_ref().is_some_and(|h| h.manifest_sha == latest.manifest_sha) {
        return Ok(Outcome::Current { seq: latest.seq });
    }
    let manifest_bytes = remote.get_object(&latest.manifest_sha)?;
    if sha256_hex(&manifest_bytes) != latest.manifest_sha {
        return Err(format!("snapshot {} of {agent}: its manifest doesn't match its hash", latest.seq));
    }
    let m = Manifest::decode(&manifest_bytes)?;
    if m.agent != agent {
        return Err(format!("snapshot {} is agent {}'s, not {agent}'s", latest.seq, m.agent));
    }
    let project = project_for(&m, t);
    if let Some(root) = &t.work_root {
        if !project.starts_with(root) || project == *root {
            return Err(format!("{agent}'s workspace {} is not under this host's work root {}", project.display(), root.display()));
        }
    }
    // The workspace dir first, so its slug is the one everything else
    // computes (`claude_project_hash` canonicalizes an existing path).
    std::fs::create_dir_all(&project).map_err(|e| format!("create {}: {e}", project.display()))?;
    let slug = claude_project_hash(&project.to_string_lossy());
    // Transcripts name the workspace; on another path they must name ours.
    let (from, to) = (m.project_path.as_bytes().to_vec(), project.to_string_lossy().as_bytes().to_vec());
    let rewrite = |e: &Entry, bytes: &mut Vec<u8>| {
        if embeds_path(e) && from != to && !from.is_empty() {
            if let Some(b) = crate::clone::unpack::replace_bytes(bytes, &from, &to) {
                *bytes = b;
            }
        }
    };

    let mut moves = Vec::new();
    let mut chmods = Vec::new();
    let mut wanted: Vec<(&Entry, PathBuf)> = Vec::new();
    let mut new_dests = HashSet::new();
    let mut kept = 0;
    for e in &m.entries {
        let Some((_, dest)) = dest_of(&m, e, &t.home, &project, &slug)? else { continue };
        new_dests.insert(dest.clone());
        if !(embeds_path(e) && from != to) && file_sha(&dest).as_deref() == Some(e.sha256.as_str()) {
            kept += 1;
            if let (Some(mode), Ok(meta)) = (e.mode, std::fs::metadata(&dest)) {
                if super::file_mode(&meta) != Some(mode) {
                    chmods.push((dest, mode));
                }
            }
            continue;
        }
        moves.push(Move { staged: staged_for(&dest, latest.seq), dest: dest.clone(), mode: e.mode });
        wanted.push((e, dest));
    }
    // What the snapshot this host held put here, and the new one lacks.
    let mut deletes = Vec::new();
    if let Some(old) = before.as_ref().and_then(|h| cached_manifest(&t.state, &h.manifest_sha)) {
        let old_project = project_for(&old, t);
        for e in &old.entries {
            if let Ok(Some((_, dest))) = dest_of(&old, e, &t.home, &old_project, &slug) {
                if !new_dests.contains(&dest) && file_sha(&dest).as_deref() == Some(e.sha256.as_str()) {
                    deletes.push(dest);
                }
            }
        }
    }
    let held_now = Held { agent: agent.to_string(), seq: latest.seq, manifest_sha: latest.manifest_sha.clone() };
    let mut j = Journal { phase: Phase::Staging, held: held_now, moves, chmods, deletes };
    let jpath = journal_path(&t.state, agent);
    write_atomic(&jpath, &serde_json::to_vec(&j).expect("journal serializes"))?;
    write_atomic(&manifest_path(&t.state, &latest.manifest_sha), &manifest_bytes)?;

    // Stage: fetch each object once, check it, write it next to its target.
    let mut fetched = 0;
    let mut fetched_bytes = 0u64;
    let mut first_staged: HashMap<&str, PathBuf> = HashMap::new();
    let staged: Result<(), String> = (|| {
        for ((e, _), mv) in wanted.iter().zip(&j.moves) {
            if let Some(dir) = mv.staged.parent() {
                std::fs::create_dir_all(dir).map_err(|err| format!("create {}: {err}", dir.display()))?;
            }
            if let Some(src) = first_staged.get(e.sha256.as_str()).filter(|_| !embeds_path(e)) {
                std::fs::copy(src, &mv.staged).map_err(|err| format!("copy to {}: {err}", mv.staged.display()))?;
            } else {
                let mut bytes = remote.get_object(&e.sha256)?;
                if sha256_hex(&bytes) != e.sha256 {
                    return Err(format!("object {} for {} doesn't match its hash; nothing was changed", e.sha256, e.path));
                }
                let len = bytes.len() as u64;
                rewrite(e, &mut bytes);
                std::fs::write(&mv.staged, &bytes).map_err(|err| format!("write {}: {err}", mv.staged.display()))?;
                fetched += 1;
                fetched_bytes += len;
                if !embeds_path(e) {
                    first_staged.insert(e.sha256.as_str(), mv.staged.clone());
                }
            }
            if let Some(mode) = mv.mode {
                set_mode(&mv.staged, mode)?;
            }
        }
        Ok(())
    })();
    if kill == Some(Kill::AfterStaging) {
        return Err("killed after staging (test)".into());
    }
    if let Err(e) = staged {
        recover(&t.state, agent)?;
        return Err(e);
    }
    j.phase = Phase::Commit;
    write_atomic(&jpath, &serde_json::to_vec(&j).expect("journal serializes"))?;
    if kill == Some(Kill::AfterCommit) {
        return Err("killed after the commit (test)".into());
    }
    apply(&t.state, &j)?;
    Ok(Outcome::Restored { seq: latest.seq, fetched, fetched_bytes, kept, deleted: j.deletes.len() })
}
