use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use git2::{Oid, Repository};

use crate::error::{PgGitError, Result};

/// Materialize a commit's tree to a local directory.
///
/// Creates the directory if it doesn't exist. Writes all blobs with
/// correct paths. Also initializes a `.git` directory with a single
/// commit so `git log` / `git diff` work on the checkout.
pub fn checkout_to(repo: &Repository, oid: Oid, dest: &Path) -> Result<()> {
    let commit = repo.find_commit(oid)?;
    let tree = commit.tree()?;

    fs::create_dir_all(dest)?;
    write_tree(repo, &tree, dest)?;

    // Initialize a real git repo at dest so CLI tools work
    init_local_git(dest, repo, &commit)?;

    Ok(())
}

/// Recursively write a git tree to a filesystem directory.
fn write_tree(repo: &Repository, tree: &git2::Tree, dir: &Path) -> Result<()> {
    for entry in tree.iter() {
        let name = entry.name().unwrap_or("");
        let path = dir.join(name);

        match entry.kind() {
            Some(git2::ObjectType::Blob) => {
                let blob = repo.find_blob(entry.id())?;
                fs::write(&path, blob.content())?;

                // Set executable bit if filemode indicates it
                if entry.filemode() == 0o100755 {
                    let mut perms = fs::metadata(&path)?.permissions();
                    perms.set_mode(0o755);
                    fs::set_permissions(&path, perms)?;
                }
            }
            Some(git2::ObjectType::Tree) => {
                fs::create_dir_all(&path)?;
                let subtree = repo.find_tree(entry.id())?;
                write_tree(repo, &subtree, &path)?;
            }
            _ => {
                // Skip submodules, etc.
            }
        }
    }
    Ok(())
}

/// Initialize a local git repo at dest with the commit replayed so
/// `git log` and `git status` work.
fn init_local_git(dest: &Path, pg_repo: &Repository, commit: &git2::Commit) -> Result<()> {
    let local = git2::Repository::init(dest)?;

    // Replay all objects needed for this commit into the local repo
    replay_commit(&local, pg_repo, commit)?;

    // Point HEAD at the commit
    local.set_head_detached(commit.id())?;

    // Reset the index to match the tree
    let obj = local.find_object(commit.id(), None)?;
    local.reset(&obj, git2::ResetType::Mixed, None)?;

    Ok(())
}

/// Recursively copy a commit and all its reachable objects from the
/// PG-backed repo into a local repo.
fn replay_commit(
    local: &git2::Repository,
    pg_repo: &Repository,
    commit: &git2::Commit,
) -> Result<()> {
    // Copy tree objects
    let tree = commit.tree()?;
    replay_tree(local, pg_repo, &tree)?;

    // Copy parent commits (recursively)
    for i in 0..commit.parent_count() {
        let parent = commit.parent(i)?;
        // Only replay if not already in local
        if local.find_commit(parent.id()).is_err() {
            replay_commit(local, pg_repo, &parent)?;
        }
    }

    // Now create the commit in local repo
    if local.find_commit(commit.id()).is_ok() {
        return Ok(());
    }

    let local_tree = local.find_tree(tree.id())?;

    let parents: Vec<git2::Commit> = (0..commit.parent_count())
        .map(|i| local.find_commit(commit.parent_id(i).unwrap()).unwrap())
        .collect();
    let parent_refs: Vec<&git2::Commit> = parents.iter().collect();

    let author = commit.author();
    let committer = commit.committer();
    let message = commit.message().unwrap_or("");

    let oid = local.commit(
        None,
        &author,
        &committer,
        message,
        &local_tree,
        &parent_refs,
    )?;

    // Verify OID matches (should be identical since content is the same)
    if oid != commit.id() {
        return Err(PgGitError::Other(format!(
            "commit OID mismatch: local={} pg={}",
            oid,
            commit.id()
        )));
    }

    Ok(())
}

/// Copy a tree and all its blobs into the local repo.
fn replay_tree(
    local: &git2::Repository,
    pg_repo: &Repository,
    tree: &git2::Tree,
) -> Result<()> {
    let mut tb = local.treebuilder(None)?;

    for entry in tree.iter() {
        let name = entry.name().unwrap_or("");
        match entry.kind() {
            Some(git2::ObjectType::Blob) => {
                // Copy blob content
                if local.find_blob(entry.id()).is_err() {
                    let blob = pg_repo.find_blob(entry.id())?;
                    let local_oid = local.blob(blob.content())?;
                    assert_eq!(local_oid, entry.id());
                }
                tb.insert(name, entry.id(), entry.filemode())?;
            }
            Some(git2::ObjectType::Tree) => {
                let subtree = pg_repo.find_tree(entry.id())?;
                replay_tree(local, pg_repo, &subtree)?;
                tb.insert(name, entry.id(), entry.filemode())?;
            }
            _ => {
                tb.insert(name, entry.id(), entry.filemode())?;
            }
        }
    }

    let oid = tb.write()?;
    assert_eq!(oid, tree.id());
    Ok(())
}

impl From<std::io::Error> for PgGitError {
    fn from(e: std::io::Error) -> Self {
        PgGitError::Other(format!("io error: {}", e))
    }
}
