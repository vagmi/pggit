use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use git2::{Odb, Oid, Repository};
use tempfile::TempDir;

use super::error::HttpError;
use super::state::HttpState;
use crate::error::{PgGitError, Result as PgResult};

pub(crate) const REPO_DIR_NAME: &str = "repo";

/// A per-request bare git repository materialized on disk from PG.
///
/// Drop reaps the tempdir.
pub(crate) struct Workdir {
    _tempdir: TempDir,
    git_dir: PathBuf,
}

/// Snapshot of object set + ref-name → OID map at a point in time.
pub(crate) struct Snapshot {
    objects: HashSet<Oid>,
    refs: HashMap<String, Oid>,
}

impl Workdir {
    /// Create a tempdir, init it as a bare repo, and replay every ref's
    /// reachable closure from PG into it.
    pub(crate) async fn prepare(state: &HttpState, repo_id: i32) -> Result<Self, HttpError> {
        let store = Arc::clone(&state.store);
        let tempdir_root = state.opts.tempdir_root.clone();

        let workdir = tokio::task::spawn_blocking(move || -> Result<Workdir, HttpError> {
            let tempdir = match tempdir_root {
                Some(root) => TempDir::new_in(root)?,
                None => TempDir::new()?,
            };
            let git_dir = tempdir.path().join(REPO_DIR_NAME);
            tracing::debug!(repo_id, git_dir = %git_dir.display(), "workdir: tempdir created");

            std::fs::create_dir(&git_dir)?;
            Repository::init_bare(&git_dir).map_err(PgGitError::from)?;
            tracing::debug!(repo_id, "workdir: bare repo initialized");

            let pg = store.open_repository(repo_id)?;
            let local = Repository::open(&git_dir).map_err(PgGitError::from)?;
            // git http-backend refuses receive-pack unless this is set on the
            // repo's own config; the env doesn't override it.
            local
                .config()
                .map_err(PgGitError::from)?
                .set_bool("http.receivepack", true)
                .map_err(PgGitError::from)?;
            let n_refs = replay_all_refs(&local, &pg)?;
            tracing::info!(repo_id, n_refs, "workdir: prepared");

            Ok(Workdir {
                _tempdir: tempdir,
                git_dir,
            })
        })
        .await
        .map_err(|e| HttpError::Internal(format!("workdir prepare join: {e}")))??;

        Ok(workdir)
    }

    pub(crate) fn git_dir(&self) -> &Path {
        &self.git_dir
    }

    /// Capture the current object set and refs in the workdir.
    pub(crate) async fn snapshot(&self) -> Result<Snapshot, HttpError> {
        let git_dir = self.git_dir.clone();
        tokio::task::spawn_blocking(move || -> Result<Snapshot, HttpError> {
            let local = Repository::open(&git_dir).map_err(PgGitError::from)?;
            Ok(snapshot_repo(&local)?)
        })
        .await
        .map_err(|e| HttpError::Internal(format!("snapshot join: {e}")))?
    }

    /// After `git receive-pack` has run inside the workdir, copy any new
    /// objects to PG and apply ref deltas. Reuses the existing PG ODB/RefDB
    /// backends, which take per-key advisory locks for ref writes and
    /// content-addressed dedupe for object writes.
    pub(crate) async fn apply_changes(
        &self,
        state: &HttpState,
        repo_id: i32,
        before: Snapshot,
    ) -> Result<(), HttpError> {
        let store = Arc::clone(&state.store);
        let git_dir = self.git_dir.clone();

        tokio::task::spawn_blocking(move || -> Result<(), HttpError> {
            let local = Repository::open(&git_dir).map_err(PgGitError::from)?;
            let pg = store.open_repository(repo_id)?;
            let after = snapshot_repo(&local)?;

            copy_new_objects(&local, &pg, &before.objects)?;
            apply_ref_deltas(&pg, &before.refs, &after.refs)?;
            Ok(())
        })
        .await
        .map_err(|e| HttpError::Internal(format!("apply_changes join: {e}")))?
    }
}

fn snapshot_repo(repo: &Repository) -> PgResult<Snapshot> {
    let mut objects = HashSet::new();
    let odb = repo.odb()?;
    odb.foreach(|oid| {
        objects.insert(*oid);
        true
    })?;

    let mut refs = HashMap::new();
    for r in repo.references()? {
        let r = r?;
        let Some(name) = r.name() else { continue };
        let resolved = r.resolve().ok();
        if let Some(oid) = resolved.as_ref().and_then(|r| r.target()) {
            refs.insert(name.to_string(), oid);
        }
    }
    Ok(Snapshot { objects, refs })
}

/// Copy every reachable object from each ref in `pg` into `local`.
/// Returns the number of refs replayed.
fn replay_all_refs(local: &Repository, pg: &Repository) -> PgResult<usize> {
    let local_odb = local.odb()?;
    let pg_odb = pg.odb()?;

    let refs: Vec<(String, Oid)> = pg
        .references()?
        .filter_map(|r| {
            let r = r.ok()?;
            let name = r.name()?.to_string();
            // Skip HEAD and any non-refs/ entries; HEAD is mirrored separately
            // as a symbolic ref below.
            if !name.starts_with("refs/") {
                return None;
            }
            let oid = r.resolve().ok()?.target()?;
            Some((name, oid))
        })
        .collect();

    let n = refs.len();
    for (name, oid) in &refs {
        tracing::trace!(name = %name, oid = %oid, "replay ref");
        copy_commit_closure(&pg_odb, &local_odb, pg, *oid)?;
        local.reference(name, *oid, true, "pggit smart-http snapshot")?;
    }

    // HEAD: if PG has a symbolic HEAD, mirror it.
    if let Ok(head) = pg.find_reference("HEAD") {
        if let Some(target) = head.symbolic_target() {
            let _ = local.reference_symbolic("HEAD", target, true, "pggit smart-http snapshot");
        }
    }

    Ok(n)
}

fn copy_commit_closure(
    src_odb: &Odb,
    dst_odb: &Odb,
    src_repo: &Repository,
    root: Oid,
) -> PgResult<()> {
    let mut walker = src_repo.revwalk()?;
    if walker.push(root).is_err() {
        // Not a commit — peel (e.g. annotated tag), copy the tag object,
        // and walk from the peeled commit.
        let obj = src_repo.find_object(root, None)?;
        let peeled = obj.peel(git2::ObjectType::Commit)?;
        copy_object_if_absent(src_odb, dst_odb, root)?;
        walker.push(peeled.id())?;
    }

    for cid in walker {
        let cid = cid?;
        if dst_odb.exists(cid) {
            continue;
        }
        copy_object(src_odb, dst_odb, cid)?;
        let commit = src_repo.find_commit(cid)?;
        copy_tree_closure(src_odb, dst_odb, src_repo, commit.tree_id())?;
    }
    Ok(())
}

fn copy_tree_closure(
    src_odb: &Odb,
    dst_odb: &Odb,
    src_repo: &Repository,
    tid: Oid,
) -> PgResult<()> {
    if dst_odb.exists(tid) {
        return Ok(());
    }
    copy_object(src_odb, dst_odb, tid)?;
    let tree = src_repo.find_tree(tid)?;
    for entry in tree.iter() {
        let oid = entry.id();
        match entry.kind() {
            Some(git2::ObjectType::Blob) => copy_object_if_absent(src_odb, dst_odb, oid)?,
            Some(git2::ObjectType::Tree) => copy_tree_closure(src_odb, dst_odb, src_repo, oid)?,
            _ => {}
        }
    }
    Ok(())
}

fn copy_object_if_absent(src: &Odb, dst: &Odb, oid: Oid) -> PgResult<()> {
    if dst.exists(oid) {
        return Ok(());
    }
    copy_object(src, dst, oid)
}

fn copy_object(src: &Odb, dst: &Odb, oid: Oid) -> PgResult<()> {
    let obj = src.read(oid)?;
    let written = dst.write(obj.kind(), obj.data())?;
    if written != oid {
        return Err(PgGitError::Other(format!(
            "object oid mismatch on copy: src={oid} dst={written}"
        )));
    }
    Ok(())
}

fn copy_new_objects(
    local: &Repository,
    pg: &Repository,
    before_objects: &HashSet<Oid>,
) -> PgResult<()> {
    let local_odb = local.odb()?;
    let pg_odb = pg.odb()?;
    let mut new_oids: Vec<Oid> = Vec::new();
    local_odb.foreach(|oid| {
        if !before_objects.contains(oid) {
            new_oids.push(*oid);
        }
        true
    })?;

    let count = new_oids.len();
    for oid in new_oids {
        if pg_odb.exists(oid) {
            continue;
        }
        let obj = local_odb.read(oid)?;
        let written = pg_odb.write(obj.kind(), obj.data())?;
        if written != oid {
            return Err(PgGitError::Other(format!(
                "object oid mismatch on reimport: local={oid} pg={written}"
            )));
        }
    }
    let _ = pg;
    tracing::debug!(reimported = count, "reimport: objects copied to PG");
    Ok(())
}

fn apply_ref_deltas(
    pg: &Repository,
    before: &HashMap<String, Oid>,
    after: &HashMap<String, Oid>,
) -> PgResult<()> {
    let mut creates_or_updates = 0usize;
    let mut deletes = 0usize;

    // Skip refs that the local bare repo creates for its own bookkeeping
    // (we never want to write these back to PG).
    let is_internal = |name: &str| name == "HEAD" || name.starts_with("refs/remotes/");

    // Deletes: in `before` but not in `after`.
    for name in before.keys() {
        if is_internal(name) {
            continue;
        }
        if !after.contains_key(name) {
            if let Ok(mut r) = pg.find_reference(name) {
                r.delete()?;
                deletes += 1;
            }
        }
    }

    // Creates / updates.
    for (name, oid) in after {
        if is_internal(name) {
            continue;
        }
        if before.get(name) == Some(oid) {
            continue;
        }
        pg.reference(name, *oid, true, "pggit smart-http push")?;
        creates_or_updates += 1;
    }

    tracing::debug!(creates_or_updates, deletes, "reimport: refs applied");
    Ok(())
}

