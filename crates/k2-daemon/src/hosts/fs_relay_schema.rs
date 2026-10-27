//! fs-relay R6: one request schema per relayed route. Each names every key
//! its handler reads, what each is (a path, a path relative to a base it
//! names, a file name, or not a path), and the paths the handler derives
//! from them (a worktree's folder, an upload's file, a copy's target, the
//! repository a git path opens). A host serves a relayed request only as
//! its schema says: an undeclared key is refused, every path and derived
//! path is confined to the work root, and the request its daemon gets is
//! rebuilt from the confined values alone.

use std::path::{Component, Path, PathBuf};

use serde_json::{Map, Value};

use super::{has_dot_dot, resolve};

/// What a request key is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// An absolute path, or a list of them.
    Abs,
    /// A path in the work tree of the repository the base key's path
    /// opens (the handler joins it to that work tree), or a list of them.
    RepoRel(&'static str),
    /// One plain file name, in the folder the base key names.
    NameIn(&'static str),
    /// One plain file name, beside the path the base key names.
    NameBeside(&'static str),
    /// resolve-links' candidates, each checked on its own (`split_links`).
    Candidates,
    /// Not a path: text, a flag, a number, a ref, an id.
    Plain,
}

/// A path a handler makes from its request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Derived {
    /// The repository each path key opens: its git dir, common dir and
    /// work tree.
    Repo,
    /// `<projectPath>/.worktrees`, `.worktrees/<branch>` and
    /// `<projectPath>/.gitignore` (create-worktree).
    Worktree,
    /// `<path>/.git` (init).
    GitDir,
    /// The file an upload lands at (its collision-free name in `dir`) and
    /// a chunk's `.part`.
    Upload,
    /// Each copy's target, and every file under each source (followed
    /// the way the copy follows it).
    CopyTree,
    /// A forced checkout's work tree (abort, a fast-forward merge, discard,
    /// resolve): no link in it to a folder outside the work root (or a
    /// dangling one), which the checkout would write or delete through.
    CheckoutTree,
}

#[derive(Debug)]
pub struct Schema {
    pub method: &'static str,
    pub route: &'static str,
    pub keys: &'static [(&'static str, Kind)],
    pub derived: &'static [Derived],
}

impl Schema {
    pub fn kind(&self, key: &str) -> Option<Kind> {
        self.keys.iter().find(|(k, _)| *k == key).map(|(_, kind)| *kind)
    }
}

use Derived as D;
use Kind::*;

const fn get(route: &'static str, keys: &'static [(&'static str, Kind)], derived: &'static [Derived]) -> Schema {
    Schema { method: "GET", route, keys, derived }
}

const fn post(route: &'static str, keys: &'static [(&'static str, Kind)], derived: &'static [Derived]) -> Schema {
    Schema { method: "POST", route, keys, derived }
}

/// Every relayed route, exactly. Not the git notes (they live in the
/// controller's database and are checked by author), not `open-changes`
/// (an event on the controller's bus).
pub const SCHEMAS: &[Schema] = &[
    get("/cli/fs/info", &[], &[]),
    get("/cli/fs/read-dir", &[("path", Abs), ("show_hidden", Plain)], &[]),
    get("/cli/fs/read-file", &[("path", Abs)], &[]),
    get("/cli/fs/read-binary", &[("path", Abs)], &[]),
    get("/cli/fs/read-range", &[("path", Abs), ("offset", Plain), ("len", Plain)], &[]),
    get("/cli/fs/stat", &[("path", Abs)], &[]),
    get("/cli/fs/zip-list", &[("path", Abs)], &[]),
    get("/cli/git/branches", &[("path", Abs)], &[D::Repo]),
    get("/cli/git/changes", &[("path", Abs)], &[D::Repo]),
    get("/cli/git/diff-between", &[("path", Abs), ("base_branch", Plain), ("head_branch", Plain)], &[D::Repo]),
    get(
        "/cli/git/diff-file",
        &[
            ("path", Abs),
            ("file_path", RepoRel("path")),
            ("old_path", RepoRel("path")),
            ("sides", Plain),
            ("mode", Plain),
            ("staged", Plain),
            ("base", Plain),
        ],
        &[D::Repo],
    ),
    get("/cli/git/diff-summary", &[("path", Abs)], &[D::Repo]),
    get("/cli/git/file-at-ref", &[("path", Abs), ("file_path", RepoRel("path")), ("git_ref", Plain)], &[D::Repo]),
    get("/cli/git/info", &[("path", Abs)], &[D::Repo]),
    get("/cli/git/merge-status", &[("path", Abs)], &[D::Repo]),
    get("/cli/git/snapshot", &[("path", Abs), ("base", Plain)], &[D::Repo]),
    get("/cli/git/worktrees", &[("path", Abs)], &[D::Repo]),
    post("/cli/fs/create", &[("path", Abs), ("is_directory", Plain)], &[]),
    post("/cli/fs/delete", &[("paths", Abs), ("permanent", Plain)], &[]),
    post("/cli/fs/move", &[("sources", Abs), ("destination", Abs)], &[D::CopyTree]),
    post("/cli/fs/rename", &[("old_path", Abs), ("new_name", NameBeside("old_path"))], &[]),
    post("/cli/fs/copy", &[("sources", Abs), ("destination", Abs)], &[D::CopyTree]),
    post("/cli/fs/duplicate", &[("path", Abs)], &[D::CopyTree]),
    post("/cli/fs/write-file", &[("path", Abs), ("content", Plain)], &[]),
    post("/cli/fs/upload-binary", &[("dir", Abs), ("filename", NameIn("dir")), ("base64", Plain)], &[D::Upload]),
    post(
        "/cli/fs/upload-chunk",
        &[
            ("upload_id", Plain),
            ("dir", Abs),
            ("filename", NameIn("dir")),
            ("offset", Plain),
            ("base64", Plain),
            ("is_last", Plain),
            ("total_bytes", Plain),
        ],
        &[D::Upload],
    ),
    post("/cli/fs/search-tree", &[("root", Abs), ("query", Plain), ("show_hidden", Plain), ("max_results", Plain)], &[]),
    post("/cli/fs/resolve-links", &[("workspace", Abs), ("paths", Candidates)], &[]),
    post("/cli/git/abort-merge", &[("path", Abs)], &[D::Repo, D::CheckoutTree]),
    post("/cli/git/commit", &[("path", Abs), ("message", Plain)], &[D::Repo]),
    post(
        "/cli/git/create-worktree",
        &[("projectPath", Abs), ("branch", Plain), ("projectId", Plain), ("existingBranch", Plain)],
        &[D::Repo, D::Worktree],
    ),
    post("/cli/git/delete-branch", &[("path", Abs), ("branch", Plain)], &[D::Repo]),
    post("/cli/git/discard", &[("path", Abs), ("files", RepoRel("path")), ("includeStaged", Plain)], &[D::Repo, D::CheckoutTree]),
    post("/cli/git/init", &[("path", Abs)], &[D::GitDir]),
    post("/cli/git/merge-branch", &[("path", Abs), ("branch", Plain)], &[D::Repo, D::CheckoutTree]),
    post("/cli/git/prune-worktrees", &[("projectPath", Abs), ("projectId", Plain)], &[D::Repo]),
    post(
        "/cli/git/remove-worktree",
        &[("projectPath", Abs), ("worktreePath", Abs), ("workspaceId", Plain), ("force", Plain)],
        &[D::Repo],
    ),
    post("/cli/git/reopen-worktree", &[("projectPath", Abs), ("worktreePath", Abs), ("branch", Plain)], &[D::Repo]),
    post("/cli/git/resolve", &[("path", Abs), ("filePath", RepoRel("path")), ("resolution", Plain)], &[D::Repo, D::CheckoutTree]),
    post("/cli/git/stage", &[("path", Abs), ("filePath", RepoRel("path"))], &[D::Repo]),
    post("/cli/git/stage-all", &[("path", Abs)], &[D::Repo]),
    post("/cli/git/unstage", &[("path", Abs), ("filePath", RepoRel("path"))], &[D::Repo]),
];

/// The schema of `method path`, when the relay carries it. Exact routes.
pub fn schema(method: &str, path: &str) -> Option<&'static Schema> {
    SCHEMAS.iter().find(|s| s.method.eq_ignore_ascii_case(method) && s.route == path)
}

// ── host ───────────────────────────────────────────────────────────────

/// `p` after every symlink, strictly under `root` (a workspace, never the
/// root itself).
fn inside(root: &Path, what: &str, p: &Path) -> Result<PathBuf, String> {
    let r = resolve(p).ok_or_else(|| format!("{what} can't be resolved"))?;
    if r == root || !r.starts_with(root) {
        return Err(format!("{what} is not in a workspace under this host's work root"));
    }
    Ok(r)
}

/// An absolute path's confined value: its folder with every symlink
/// resolved (under the work root, spelled from the request's own spelling
/// of the root), its last name as given (a link there is the handler's to
/// act on: delete it, rename it), the whole of it under the work root.
fn confined_path(root: &Path, key: &str, v: &str) -> Result<String, String> {
    if !v.starts_with('/') {
        return Err(format!("{key} is relative; a relayed request names absolute paths"));
    }
    if has_dot_dot(v) {
        return Err(format!("{key} has '..'"));
    }
    let p = Path::new(v);
    inside(root, key, p)?;
    let (Some(parent), Some(name)) = (p.parent(), p.file_name()) else {
        return Err(format!("{key} is not in a workspace under this host's work root"));
    };
    let rp = resolve(parent).ok_or_else(|| format!("{key} can't be resolved"))?;
    let rel = rp.strip_prefix(root).map_err(|_| format!("{key} is not in a workspace under this host's work root"))?;
    // The request's spelling of the root (the controller maps answers back
    // from it), else the root's own.
    let mut prefix = PathBuf::new();
    let mut spelled = None;
    for c in p.components() {
        prefix.push(c);
        if resolve(&prefix).as_deref() == Some(root) {
            spelled = Some(prefix.clone());
            break;
        }
    }
    let out = spelled.unwrap_or_else(|| root.to_path_buf()).join(rel).join(name);
    out.to_str().map(str::to_string).ok_or_else(|| format!("{key} is not UTF-8"))
}

/// One plain file name: a single normal component.
fn plain_name(key: &str, v: &str) -> Result<(), String> {
    let mut c = Path::new(v).components();
    match (c.next(), c.next()) {
        (Some(Component::Normal(_)), None) if !v.contains('/') => Ok(()),
        _ => Err(format!("{key} is not one plain name")),
    }
}

/// The work tree a repo-relative key joins to: the one the repository at
/// `base` has (as the handler opens it), else `base` itself.
fn repo_base(base: &str) -> PathBuf {
    match k2_core::git::repo_dirs(base) {
        Ok((_, _, Some(work))) => work,
        _ => PathBuf::from(base),
    }
}

/// A repo-relative value: under the work tree it joins (and, absolute,
/// under the work root).
fn confined_repo_rel(root: &Path, key: &str, v: &str, work: &Path) -> Result<(), String> {
    if has_dot_dot(v) {
        return Err(format!("{key} has '..'"));
    }
    if v.starts_with('/') {
        return inside(root, key, Path::new(v)).map(|_| ());
    }
    if v.starts_with('~') {
        return Err(format!("{key} is not in the repository"));
    }
    inside(root, key, &work.join(v)).map(|_| ())
}

/// The strings a value holds: itself, or a list of them.
fn strings<'a>(key: &str, v: &'a Value) -> Result<Vec<&'a str>, String> {
    match v {
        Value::String(s) => Ok(vec![s.as_str()]),
        Value::Array(a) => a.iter().map(|x| x.as_str().ok_or_else(|| format!("{key} is not a list of strings"))).collect(),
        _ => Err(format!("{key} is not a path")),
    }
}

/// A request as a host's daemon gets it: the query's pairs and the body.
#[derive(Debug)]
pub struct Confined {
    pub query: Vec<(String, String)>,
    pub body: Value,
}

/// Why a host refuses a relayed request.
#[derive(Debug, PartialEq, Eq)]
pub enum Refusal {
    /// A key the route's schema doesn't declare (or one given twice).
    Undeclared(String),
    /// A path, or a path its handler derives, outside the work root.
    Outside(String),
    /// Refused with its own code: `link_outside_worktree` (a checkout's
    /// folder link leaving the work tree), `walk_budget` (the checks cost
    /// more than a request may).
    Coded(&'static str, String),
}

/// R8 C: what a request's checks may cost: steps (a link resolved one
/// component at a time, a folder entry walked) and time.
pub const WALK_STEPS: usize = 500_000;
pub const WALK_TIME: std::time::Duration = std::time::Duration::from_secs(5);

/// The request `s` describes, confined to `root`: every declared path (a
/// path, a repo-relative path, a name in a folder) and every path its
/// handler derives under the work root, rebuilt from the confined values.
pub fn confine(root: &Path, s: &Schema, query: &[(String, String)], body: &Value) -> Result<Confined, Refusal> {
    confine_within(root, s, query, body, WALK_STEPS, WALK_TIME)
}

/// [`confine`] within a walk budget of `steps` and `time` (R8 C): past
/// either, `walk_budget`. Blocking: a host runs it off its link's task.
pub fn confine_within(
    root: &Path,
    s: &Schema,
    query: &[(String, String)],
    body: &Value,
    steps: usize,
    time: std::time::Duration,
) -> Result<Confined, Refusal> {
    super::budget::start(steps, time);
    let r = confine_unbudgeted(root, s, query, body);
    if super::budget::end() {
        return Err(Refusal::Coded("walk_budget", format!("checking {}'s paths took more than a relayed request may", s.route)));
    }
    r
}

fn confine_unbudgeted(root: &Path, s: &Schema, query: &[(String, String)], body: &Value) -> Result<Confined, Refusal> {
    let out = Refusal::Outside;
    // The request's values, by key: the query's and a flat JSON object's.
    let mut values: Vec<(String, Value)> = Vec::new();
    for (k, v) in query {
        if values.iter().any(|(x, _)| x == k) {
            return Err(Refusal::Undeclared(format!("{k} is given twice")));
        }
        values.push((k.clone(), Value::String(v.clone())));
    }
    match body {
        Value::Null => {}
        Value::Object(o) if s.method == "POST" => {
            for (k, v) in o {
                if values.iter().any(|(x, _)| x == k) {
                    return Err(Refusal::Undeclared(format!("{k} is given twice")));
                }
                values.push((k.clone(), v.clone()));
            }
        }
        _ => return Err(Refusal::Undeclared("a relayed body is one JSON object (POST)".into())),
    }
    let given = |key: &str| values.iter().find(|(k, _)| k == key).map(|(_, v)| v);
    // Declared keys first: a bad path is refused as one.
    let mut confined: Vec<(String, Value)> = Vec::new();
    for (k, v) in &values {
        let Some(kind) = s.kind(k) else { continue };
        let c = match kind {
            Abs => match v {
                Value::String(x) => Value::String(confined_path(root, k, x).map_err(out)?),
                Value::Array(a) => Value::Array(
                    a.iter()
                        .map(|x| x.as_str().ok_or_else(|| out(format!("{k} is not a list of paths"))).and_then(|x| confined_path(root, k, x).map_err(out)))
                        .map(|r| r.map(Value::String))
                        .collect::<Result<_, _>>()?,
                ),
                _ => return Err(out(format!("{k} is not a path"))),
            },
            RepoRel(base) => {
                let b = given(base).and_then(Value::as_str).ok_or_else(|| out(format!("{k} is relative to {base}, which the request doesn't give")))?;
                let b = confined_path(root, base, b).map_err(out)?;
                let work = repo_base(&b);
                for x in strings(k, v).map_err(out)? {
                    confined_repo_rel(root, k, x, &work).map_err(out)?;
                }
                v.clone()
            }
            NameIn(base) | NameBeside(base) => {
                let name = v.as_str().ok_or_else(|| out(format!("{k} is not a name")))?;
                plain_name(k, name).map_err(out)?;
                let b = given(base).and_then(Value::as_str).ok_or_else(|| out(format!("{k} goes with {base}, which the request doesn't give")))?;
                let b = PathBuf::from(confined_path(root, base, b).map_err(out)?);
                let dir = if matches!(kind, NameIn(_)) { b } else { b.parent().map(Path::to_path_buf).unwrap_or(b) };
                inside(root, k, &dir.join(name)).map_err(out)?;
                v.clone()
            }
            // Checked one by one by `split_links` before this.
            Candidates => {
                strings(k, v).map_err(out)?;
                v.clone()
            }
            Plain => {
                if v.is_array() || v.is_object() {
                    return Err(out(format!("{k} is not a plain value")));
                }
                v.clone()
            }
        };
        confined.push((k.clone(), c));
    }
    if let Some((k, _)) = values.iter().find(|(k, _)| s.kind(k).is_none()) {
        return Err(Refusal::Undeclared(format!("{} doesn't take {k}", s.route)));
    }
    for d in s.derived {
        if *d == Derived::CheckoutTree {
            let p = confined.iter().find(|(k, _)| k == "path").and_then(|(_, v)| v.as_str()).ok_or_else(|| out("a git request names its repository's path".into()))?;
            checkout_tree_inside(root, Path::new(p))?;
        } else {
            derived(root, s, *d, &confined).map_err(out)?;
        }
    }
    // The daemon gets the confined values, in the request's own shape.
    if s.method == "GET" {
        let query = confined.into_iter().map(|(k, v)| (k, v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string()))).collect();
        Ok(Confined { query, body: Value::Null })
    } else {
        let query_keys: Vec<&String> = query.iter().map(|(k, _)| k).collect();
        let (q, b): (Vec<_>, Vec<_>) = confined.into_iter().partition(|(k, _)| query_keys.contains(&k));
        let query = q.into_iter().map(|(k, v)| (k, v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string()))).collect();
        Ok(Confined { query, body: Value::Object(b.into_iter().collect::<Map<_, _>>()) })
    }
}

/// The derived paths `d` names for this request, under the work root.
fn derived(root: &Path, s: &Schema, d: Derived, confined: &[(String, Value)]) -> Result<(), String> {
    let value = |key: &str| confined.iter().find(|(k, _)| k == key).map(|(_, v)| v);
    let path_of = |k: &str| value(k).and_then(Value::as_str).map(PathBuf::from);
    let no_link = |p: &Path| -> Result<(), String> {
        if std::fs::symlink_metadata(p).is_ok_and(|m| m.file_type().is_symlink()) {
            return Err(format!("{} is a symlink", p.display()));
        }
        inside(root, &p.display().to_string(), p).map(|_| ())
    };
    match d {
        Derived::Repo => {
            let paths: Vec<PathBuf> = s.keys.iter().filter(|(_, k)| *k == Abs).filter_map(|(k, _)| path_of(k)).collect();
            super::git_precheck(root, &paths)
        }
        Derived::GitDir => {
            let p = path_of("path").ok_or("a git request names its repository's path")?;
            no_link(&p.join(".git"))
        }
        Derived::Worktree => {
            let project = path_of("projectPath").ok_or("create-worktree names its projectPath")?;
            let branch = value("branch").and_then(Value::as_str).ok_or("create-worktree names its branch")?;
            let (dir, wt) = k2_core::git::worktree_place(&project.to_string_lossy(), branch)?;
            for p in [dir, wt, project.join(".gitignore")] {
                no_link(&p)?;
            }
            Ok(())
        }
        Derived::Upload => {
            let dir = path_of("dir").ok_or("an upload names its dir")?;
            let name = value("filename").and_then(Value::as_str).ok_or("an upload names its filename")?;
            // The handler makes `dir`, then writes in its real folder.
            let real = resolve(&dir).ok_or("dir can't be resolved")?;
            let target = k2_core::fs_commands::collision_free_path(&real, &k2_core::fs_commands::sanitize_filename(name));
            inside(root, "the upload's file", &target)?;
            if s.route == "/cli/fs/upload-chunk" {
                let id = value("upload_id").and_then(Value::as_str).unwrap_or("");
                let part = real.join(format!(".k2-upload-{}.part", k2_core::fs_commands::sanitize_upload_id(id)));
                no_link(&part)?;
            }
            Ok(())
        }
        // Its own codes: `confine` runs it ([`checkout_tree_inside`]).
        Derived::CheckoutTree => Ok(()),
        Derived::CopyTree => {
            let (sources, dest): (Vec<PathBuf>, PathBuf) = if s.route == "/cli/fs/duplicate" {
                let p = path_of("path").ok_or("duplicate names its path")?;
                let parent = p.parent().map(Path::to_path_buf).ok_or("duplicate's path has no folder")?;
                (vec![p], parent)
            } else {
                let srcs = value("sources").and_then(Value::as_array).ok_or("a copy names its sources")?;
                (srcs.iter().filter_map(Value::as_str).map(PathBuf::from).collect(), path_of("destination").ok_or("a copy names its destination")?)
            };
            for src in &sources {
                let target = k2_core::fs_commands::disambiguate_path(&k2_core::fs_commands::resolve_destination(src, &dest));
                inside(root, "the copy's target", &target)?;
                tree_inside(root, src)?;
            }
            Ok(())
        }
    }
}

/// F1 (rounds 7, 8): a forced checkout (abort, a fast-forward merge,
/// discard, resolve) deletes and writes through a folder link in its work
/// tree. The work tree of the repository at `path` is walked (every
/// folder but the repository's own git dir, by path: B1) without following
/// a link; a link to a folder, or a dangling one, whose target is outside
/// the work tree is refused (`link_outside_worktree`, B2). A link to a file
/// is the checkout's to replace.
fn checkout_tree_inside(root: &Path, path: &Path) -> Result<(), Refusal> {
    let out = Refusal::Outside;
    let (git_dir, work) = match k2_core::git::repo_dirs(&path.to_string_lossy()) {
        Ok((g, _, Some(w))) => (resolve(&g), w),
        _ => (None, path.to_path_buf()),
    };
    let work = resolve(&work).ok_or_else(|| out(format!("{} can't be resolved", work.display())))?;
    if !work.starts_with(root) {
        return Err(out(format!("{} is not under this host's work root", work.display())));
    }
    let mut todo = vec![work.clone()];
    let mut n = 0usize;
    while let Some(d) = todo.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else { continue };
        for e in rd.filter_map(Result::ok) {
            n += 1;
            if n > TREE_LIMIT || !super::budget::charge() {
                return Err(Refusal::Coded("walk_budget", format!("{} is too big to check for a relayed checkout", work.display())));
            }
            let Ok(ft) = e.file_type() else { continue };
            let p = e.path();
            if ft.is_symlink() {
                let r = resolve(&p);
                let file = r.as_ref().is_some_and(|r| r.is_file());
                let leaves = r.as_ref().is_none_or(|r| !r.starts_with(&work));
                if leaves && !file {
                    return Err(Refusal::Coded(
                        "link_outside_worktree",
                        format!("{} links to a folder outside the repository's work tree; a checkout would go through it", p.display()),
                    ));
                }
            } else if ft.is_dir() && git_dir.as_deref() != Some(p.as_path()) {
                todo.push(p);
            }
        }
    }
    Ok(())
}

/// The most entries a copy's source may hold for the host to check it.
const TREE_LIMIT: usize = 200_000;

/// Every entry under `src`, followed as a copy follows it (a folder link
/// is copied as its folder, a file link as its file), is under the work
/// root.
fn tree_inside(root: &Path, src: &Path) -> Result<(), String> {
    let mut todo = vec![src.to_path_buf()];
    let mut seen = std::collections::HashSet::new();
    let mut n = 0usize;
    while let Some(p) = todo.pop() {
        n += 1;
        if n > TREE_LIMIT || !super::budget::charge() {
            return Err(format!("{} holds over {TREE_LIMIT} entries; too many to check for a relayed copy", src.display()));
        }
        let real = inside(root, &p.display().to_string(), &p)?;
        if real.is_dir() && seen.insert(real.clone()) {
            if let Ok(rd) = std::fs::read_dir(&real) {
                todo.extend(rd.filter_map(Result::ok).map(|e| e.path()));
            }
        }
    }
    Ok(())
}
