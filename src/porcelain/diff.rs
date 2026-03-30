use git2::{Oid, Repository};

use crate::error::Result;

/// Status of a file in a diff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffStatus {
    Added,
    Deleted,
    Modified,
    Renamed,
    Copied,
    TypeChange,
}

/// A single hunk in a diff.
#[derive(Debug, Clone)]
pub struct DiffHunk {
    pub header: String,
    pub lines: Vec<DiffLine>,
}

/// A line within a diff hunk.
#[derive(Debug, Clone)]
pub struct DiffLine {
    pub origin: char, // '+', '-', ' '
    pub content: String,
}

/// A file changed in a diff.
#[derive(Debug, Clone)]
pub struct DiffFile {
    pub old_path: Option<String>,
    pub new_path: Option<String>,
    pub status: DiffStatus,
    pub hunks: Vec<DiffHunk>,
}

/// Summary statistics of a diff.
#[derive(Debug, Clone, Default)]
pub struct DiffStats {
    pub files_changed: usize,
    pub insertions: usize,
    pub deletions: usize,
}

/// Complete diff result between two commits.
#[derive(Debug, Clone)]
pub struct DiffSummary {
    pub files: Vec<DiffFile>,
    pub stats: DiffStats,
}

/// Compute a diff between two commits in the repository.
pub fn diff_commits(repo: &Repository, old_oid: Oid, new_oid: Oid) -> Result<DiffSummary> {
    let old_commit = repo.find_commit(old_oid)?;
    let new_commit = repo.find_commit(new_oid)?;
    let old_tree = old_commit.tree()?;
    let new_tree = new_commit.tree()?;

    let diff = repo.diff_tree_to_tree(Some(&old_tree), Some(&new_tree), None)?;
    build_summary(&diff)
}

/// Compute a diff showing everything in a commit (against empty tree).
pub fn diff_initial_commit(repo: &Repository, oid: Oid) -> Result<DiffSummary> {
    let commit = repo.find_commit(oid)?;
    let tree = commit.tree()?;

    let diff = repo.diff_tree_to_tree(None, Some(&tree), None)?;
    build_summary(&diff)
}

fn build_summary(diff: &git2::Diff) -> Result<DiffSummary> {
    let mut files: Vec<DiffFile> = Vec::new();
    let mut total_insertions = 0usize;
    let mut total_deletions = 0usize;

    for delta_idx in 0..diff.deltas().len() {
        let delta = diff.get_delta(delta_idx).unwrap();
        let status = match delta.status() {
            git2::Delta::Added => DiffStatus::Added,
            git2::Delta::Deleted => DiffStatus::Deleted,
            git2::Delta::Modified => DiffStatus::Modified,
            git2::Delta::Renamed => DiffStatus::Renamed,
            git2::Delta::Copied => DiffStatus::Copied,
            git2::Delta::Typechange => DiffStatus::TypeChange,
            _ => DiffStatus::Modified,
        };

        let old_path = delta.old_file().path().map(|p| p.to_string_lossy().into_owned());
        let new_path = delta.new_file().path().map(|p| p.to_string_lossy().into_owned());

        let mut hunks = Vec::new();
        let patch = git2::Patch::from_diff(diff, delta_idx)?;
        if let Some(patch) = patch {
            for hunk_idx in 0..patch.num_hunks() {
                let (hunk, _) = patch.hunk(hunk_idx)?;
                let header = std::str::from_utf8(hunk.header())
                    .unwrap_or("")
                    .trim_end()
                    .to_string();

                let mut lines = Vec::new();
                let num_lines = patch.num_lines_in_hunk(hunk_idx)?;
                for line_idx in 0..num_lines {
                    let line = patch.line_in_hunk(hunk_idx, line_idx)?;
                    let origin = line.origin();
                    match origin {
                        '+' | '-' | ' ' => {
                            let content = std::str::from_utf8(line.content())
                                .unwrap_or("")
                                .to_string();
                            if origin == '+' {
                                total_insertions += 1;
                            } else if origin == '-' {
                                total_deletions += 1;
                            }
                            lines.push(DiffLine { origin, content });
                        }
                        _ => {}
                    }
                }

                hunks.push(DiffHunk { header, lines });
            }
        }

        files.push(DiffFile {
            old_path,
            new_path,
            status,
            hunks,
        });
    }

    Ok(DiffSummary {
        stats: DiffStats {
            files_changed: files.len(),
            insertions: total_insertions,
            deletions: total_deletions,
        },
        files,
    })
}

impl DiffSummary {
    /// Format as a unified diff string.
    pub fn to_patch(&self) -> String {
        let mut out = String::new();
        for file in &self.files {
            let old = file.old_path.as_deref().unwrap_or("/dev/null");
            let new = file.new_path.as_deref().unwrap_or("/dev/null");

            match file.status {
                DiffStatus::Added => {
                    out.push_str(&format!("--- /dev/null\n+++ b/{}\n", new));
                }
                DiffStatus::Deleted => {
                    out.push_str(&format!("--- a/{}\n+++ /dev/null\n", old));
                }
                _ => {
                    out.push_str(&format!("--- a/{}\n+++ b/{}\n", old, new));
                }
            }

            for hunk in &file.hunks {
                if !hunk.header.is_empty() {
                    out.push_str(&hunk.header);
                    out.push('\n');
                }
                for line in &hunk.lines {
                    out.push(line.origin);
                    out.push_str(&line.content);
                    if !line.content.ends_with('\n') {
                        out.push('\n');
                    }
                }
            }
        }
        out
    }

    /// One-line summary like "2 files changed, 5 insertions(+), 3 deletions(-)"
    pub fn stat_line(&self) -> String {
        format!(
            "{} file(s) changed, {} insertion(s)(+), {} deletion(s)(-)",
            self.stats.files_changed, self.stats.insertions, self.stats.deletions
        )
    }
}

impl std::fmt::Display for DiffStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Added => write!(f, "added"),
            Self::Deleted => write!(f, "deleted"),
            Self::Modified => write!(f, "modified"),
            Self::Renamed => write!(f, "renamed"),
            Self::Copied => write!(f, "copied"),
            Self::TypeChange => write!(f, "typechange"),
        }
    }
}
