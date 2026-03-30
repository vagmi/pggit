use std::collections::BTreeMap;

use git2::{Oid, Repository};

use crate::error::Result;

/// Build a nested git tree from a flat list of `(path, content)` pairs.
///
/// Handles subdirectories automatically. For example:
/// ```text
/// [("src/main.rs", b"..."), ("src/lib.rs", b"..."), ("README.md", b"...")]
/// ```
/// produces a root tree with entries `src/` (subtree) and `README.md` (blob),
/// where the `src/` subtree contains `main.rs` and `lib.rs`.
pub fn build_tree(repo: &Repository, files: &[(&str, &[u8])]) -> Result<Oid> {
    // Organize files into a nested directory structure.
    let mut root = DirNode::new();
    for &(path, content) in files {
        root.insert(path, content);
    }
    root.write(repo)
}

/// Build a tree that layers new files on top of an existing tree.
/// Files in `files` override entries at the same path. Existing entries
/// not mentioned in `files` are preserved.
pub fn build_tree_update(
    repo: &Repository,
    base: Oid,
    files: &[(&str, &[u8])],
) -> Result<Oid> {
    let base_tree = repo.find_tree(base)?;
    let mut root = DirNode::new();

    // Seed with existing tree entries
    populate_from_tree(repo, &base_tree, &mut root)?;

    // Overlay new files
    for &(path, content) in files {
        root.insert(path, content);
    }

    root.write(repo)
}

/// Recursively populate a DirNode from an existing git tree.
fn populate_from_tree<'a>(
    repo: &'a Repository,
    tree: &git2::Tree<'a>,
    node: &mut DirNode,
) -> Result<()> {
    for entry in tree.iter() {
        let name = entry.name().unwrap_or("");
        match entry.kind() {
            Some(git2::ObjectType::Tree) => {
                let subtree = repo.find_tree(entry.id())?;
                let child = node.children.entry(name.to_string()).or_insert_with(DirNode::new);
                populate_from_tree(repo, &subtree, child)?;
            }
            Some(git2::ObjectType::Blob) => {
                let blob = repo.find_blob(entry.id())?;
                node.files.insert(
                    name.to_string(),
                    FileEntry {
                        content: Content::Existing(entry.id()),
                        mode: entry.filemode(),
                        _blob_data: Some(blob.content().to_vec()),
                    },
                );
            }
            _ => {
                // Preserve other entries (e.g. submodules) by OID
                node.files.insert(
                    name.to_string(),
                    FileEntry {
                        content: Content::Existing(entry.id()),
                        mode: entry.filemode(),
                        _blob_data: None,
                    },
                );
            }
        }
    }
    Ok(())
}

enum Content {
    New(Vec<u8>),
    Existing(Oid),
}

struct FileEntry {
    content: Content,
    mode: i32,
    _blob_data: Option<Vec<u8>>,
}

struct DirNode {
    files: BTreeMap<String, FileEntry>,
    children: BTreeMap<String, DirNode>,
}

impl DirNode {
    fn new() -> Self {
        Self {
            files: BTreeMap::new(),
            children: BTreeMap::new(),
        }
    }

    /// Insert a file at the given path, creating intermediate directories.
    fn insert(&mut self, path: &str, content: &[u8]) {
        if let Some(slash_pos) = path.find('/') {
            let (dir, rest) = path.split_at(slash_pos);
            let rest = &rest[1..]; // skip the '/'
            let child = self
                .children
                .entry(dir.to_string())
                .or_insert_with(DirNode::new);
            child.insert(rest, content);
        } else {
            self.files.insert(
                path.to_string(),
                FileEntry {
                    content: Content::New(content.to_vec()),
                    mode: 0o100644,
                    _blob_data: None,
                },
            );
        }
    }

    /// Write this directory node (and all children) as git tree objects.
    fn write(&self, repo: &Repository) -> Result<Oid> {
        let mut tb = repo.treebuilder(None)?;

        for (name, entry) in &self.files {
            let oid = match &entry.content {
                Content::New(data) => repo.blob(data)?,
                Content::Existing(oid) => *oid,
            };
            tb.insert(name, oid, entry.mode)?;
        }

        for (name, child) in &self.children {
            let child_oid = child.write(repo)?;
            tb.insert(name, child_oid, 0o040000)?;
        }

        let oid = tb.write()?;
        Ok(oid)
    }
}
