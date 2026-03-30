use std::path::PathBuf;
use std::sync::Arc;

use git2::Oid;

use crate::error::{PgGitError, Result};
use crate::porcelain::{checkout, diff, tree};
use crate::porcelain::diff::DiffSummary;
use crate::store::PgGitStore;

/// A high-level async handle to a git repository stored in PostgreSQL.
///
/// All operations are async and internally dispatch git2 calls to
/// blocking threads.
pub struct PgRepository {
    store: Arc<PgGitStore>,
    repo_id: i32,
    name: String,
}

/// Represents a commit in the log.
pub struct LogEntry {
    pub oid: Oid,
    pub message: String,
    pub author_name: String,
    pub author_email: String,
    pub time: i64,
    pub parent_ids: Vec<Oid>,
}

impl PgRepository {
    pub(crate) fn new(store: Arc<PgGitStore>, repo_id: i32, name: String) -> Self {
        Self {
            store,
            repo_id,
            name,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn repo_id(&self) -> i32 {
        self.repo_id
    }

    /// Open the underlying git2::Repository on a blocking thread and run a closure.
    async fn with_repo<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&git2::Repository) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let store = Arc::clone(&self.store);
        let repo_id = self.repo_id;
        tokio::task::spawn_blocking(move || {
            let repo = store.open_repository(repo_id)?;
            f(&repo)
        })
        .await
        .map_err(|e| PgGitError::Other(format!("spawn_blocking failed: {}", e)))?
    }

    /// Write files and create a commit on a branch.
    ///
    /// If the branch exists, the new commit's parent is the current tip and
    /// the tree is built by layering `files` on top of the parent's tree.
    /// If the branch doesn't exist, a root commit is created.
    ///
    /// Returns the new commit OID.
    pub async fn commit(
        &self,
        branch: &str,
        files: &[(&str, &[u8])],
        message: &str,
        author_name: &str,
        author_email: &str,
    ) -> Result<Oid> {
        let branch = branch.to_string();
        let message = message.to_string();
        let author_name = author_name.to_string();
        let author_email = author_email.to_string();
        // Clone file data so it's owned and 'static
        let files: Vec<(String, Vec<u8>)> = files
            .iter()
            .map(|(p, c)| (p.to_string(), c.to_vec()))
            .collect();

        self.with_repo(move |repo| {
            let refname = if branch.starts_with("refs/") {
                branch.clone()
            } else {
                format!("refs/heads/{}", branch)
            };

            let sig = git2::Signature::now(&author_name, &author_email)?;

            // Check if the branch already exists
            let parent_commit = repo.find_reference(&refname).ok().and_then(|r| {
                let oid = r.target()?;
                repo.find_commit(oid).ok()
            });

            let file_refs: Vec<(&str, &[u8])> =
                files.iter().map(|(p, c)| (p.as_str(), c.as_slice())).collect();

            let tree_oid = if let Some(ref parent) = parent_commit {
                tree::build_tree_update(repo, parent.tree_id(), &file_refs)?
            } else {
                tree::build_tree(repo, &file_refs)?
            };

            let tree = repo.find_tree(tree_oid)?;

            let parents: Vec<&git2::Commit> = match parent_commit {
                Some(ref c) => vec![c],
                None => vec![],
            };

            let oid = repo.commit(
                Some(&refname),
                &sig,
                &sig,
                &message,
                &tree,
                &parents,
            )?;

            Ok(oid)
        })
        .await
    }

    /// Read a file's content at a given ref (branch name or full refname).
    /// Returns `None` if the file doesn't exist in that tree.
    pub async fn read_file(&self, refname: &str, path: &str) -> Result<Option<Vec<u8>>> {
        let refname = if refname.starts_with("refs/") {
            refname.to_string()
        } else {
            format!("refs/heads/{}", refname)
        };
        let path = path.to_string();

        self.with_repo(move |repo| {
            let reference = repo.find_reference(&refname)?;
            let commit = reference.peel_to_commit()?;
            let tree = commit.tree()?;

            match tree.get_path(std::path::Path::new(&path)) {
                Ok(entry) => {
                    let obj = entry.to_object(repo)?;
                    let blob = obj
                        .as_blob()
                        .ok_or_else(|| PgGitError::Other(format!("{} is not a file", path)))?;
                    Ok(Some(blob.content().to_vec()))
                }
                Err(e) if e.code() == git2::ErrorCode::NotFound => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
        .await
    }

    /// List files at a given ref. Returns paths relative to repo root.
    pub async fn list_files(&self, refname: &str) -> Result<Vec<String>> {
        let refname = if refname.starts_with("refs/") {
            refname.to_string()
        } else {
            format!("refs/heads/{}", refname)
        };

        self.with_repo(move |repo| {
            let reference = repo.find_reference(&refname)?;
            let commit = reference.peel_to_commit()?;
            let tree = commit.tree()?;

            let mut paths = Vec::new();
            tree.walk(git2::TreeWalkMode::PreOrder, |dir, entry| {
                if entry.kind() == Some(git2::ObjectType::Blob) {
                    let name = entry.name().unwrap_or("");
                    if dir.is_empty() {
                        paths.push(name.to_string());
                    } else {
                        paths.push(format!("{}{}", dir, name));
                    }
                }
                git2::TreeWalkResult::Ok
            })?;

            Ok(paths)
        })
        .await
    }

    /// Get commit log for a branch, newest first.
    pub async fn log(&self, refname: &str, max_count: usize) -> Result<Vec<LogEntry>> {
        let refname = if refname.starts_with("refs/") {
            refname.to_string()
        } else {
            format!("refs/heads/{}", refname)
        };

        self.with_repo(move |repo| {
            let reference = repo.find_reference(&refname)?;
            let oid = reference
                .target()
                .ok_or_else(|| PgGitError::Other("symbolic ref".into()))?;

            let mut revwalk = repo.revwalk()?;
            revwalk.push(oid)?;
            revwalk.set_sorting(git2::Sort::TIME)?;

            let mut entries = Vec::new();
            for oid_result in revwalk {
                if entries.len() >= max_count {
                    break;
                }
                let oid = oid_result?;
                let commit = repo.find_commit(oid)?;
                let parent_ids: Vec<Oid> = (0..commit.parent_count())
                    .map(|i| commit.parent_id(i).unwrap())
                    .collect();

                entries.push(LogEntry {
                    oid,
                    message: commit.message().unwrap_or("").to_string(),
                    author_name: commit.author().name().unwrap_or("").to_string(),
                    author_email: commit.author().email().unwrap_or("").to_string(),
                    time: commit.time().seconds(),
                    parent_ids,
                });
            }

            Ok(entries)
        })
        .await
    }

    /// Compute a diff between two commits (by OID).
    pub async fn diff(&self, old_oid: Oid, new_oid: Oid) -> Result<DiffSummary> {
        self.with_repo(move |repo| diff::diff_commits(repo, old_oid, new_oid))
            .await
    }

    /// Compute a diff for an initial commit (against empty tree).
    pub async fn diff_initial(&self, oid: Oid) -> Result<DiffSummary> {
        self.with_repo(move |repo| diff::diff_initial_commit(repo, oid))
            .await
    }

    /// Diff between the tips of two branches (or refnames).
    pub async fn diff_refs(&self, old_ref: &str, new_ref: &str) -> Result<DiffSummary> {
        let old_ref = normalize_ref(old_ref);
        let new_ref = normalize_ref(new_ref);

        self.with_repo(move |repo| {
            let old_oid = repo
                .find_reference(&old_ref)?
                .peel_to_commit()?
                .id();
            let new_oid = repo
                .find_reference(&new_ref)?
                .peel_to_commit()?
                .id();
            diff::diff_commits(repo, old_oid, new_oid)
        })
        .await
    }

    /// Checkout the repository at a given ref to a local directory.
    ///
    /// The directory will contain all files from the commit's tree,
    /// plus a `.git` directory so standard git CLI tools work.
    pub async fn checkout(&self, refname: &str, dest: &str) -> Result<()> {
        let refname = normalize_ref(refname);
        let dest = PathBuf::from(dest);

        self.with_repo(move |repo| {
            let reference = repo.find_reference(&refname)?;
            let oid = reference
                .target()
                .ok_or_else(|| PgGitError::Other("symbolic ref".into()))?;
            checkout::checkout_to(repo, oid, &dest)
        })
        .await
    }
}

fn normalize_ref(refname: &str) -> String {
    if refname.starts_with("refs/") {
        refname.to_string()
    } else {
        format!("refs/heads/{}", refname)
    }
}
