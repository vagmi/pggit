use std::process::Command;
use std::sync::Arc;

use pggit::{PgGitStore, PgRepository};

/// Get a connected store, panicking with a helpful message if DATABASE_URL is unset.
async fn store() -> Arc<PgGitStore> {
    let url = std::env::var("DATABASE_URL").expect(
        "DATABASE_URL must be set to run integration tests.\n\
         Example: DATABASE_URL=postgresql://postgres@localhost:5432/pggit_test cargo test",
    );
    let store = PgGitStore::connect(&url).await.unwrap();
    store.migrate().await.unwrap();
    store
}

/// Create a repo with a unique name to avoid collisions between tests.
async fn create_repo(store: &Arc<PgGitStore>, test_name: &str) -> PgRepository {
    let name = format!("test-{}-{}", test_name, std::process::id());
    store.get_or_create_repository(&name).await.unwrap()
}

/// Run a git command in a directory, returning stdout.
fn git(dir: &std::path::Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .expect("failed to run git");
    assert!(
        output.status.success(),
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

// ============================================================
// Object roundtrip tests
// ============================================================

#[tokio::test]
async fn blob_roundtrip() {
    let store = store().await;
    let repo_id = store.create_repository(&format!("blob-rt-{}", std::process::id())).await.unwrap();

    let result = tokio::task::spawn_blocking(move || {
        let repo = store.open_repository(repo_id).unwrap();
        let content = b"Hello, integration test!\n";
        let oid = repo.blob(content).unwrap();
        let blob = repo.find_blob(oid).unwrap();
        assert_eq!(blob.content(), content);
        assert_eq!(blob.size(), content.len());
        oid
    })
    .await
    .unwrap();

    // OID should be a valid 40-char hex
    assert_eq!(result.to_string().len(), 40);
}

#[tokio::test]
async fn tree_roundtrip() {
    let store = store().await;
    let repo_id = store.create_repository(&format!("tree-rt-{}", std::process::id())).await.unwrap();

    tokio::task::spawn_blocking(move || {
        let repo = store.open_repository(repo_id).unwrap();

        let b1 = repo.blob(b"file one").unwrap();
        let b2 = repo.blob(b"file two").unwrap();

        let mut tb = repo.treebuilder(None).unwrap();
        tb.insert("a.txt", b1, 0o100644).unwrap();
        tb.insert("b.txt", b2, 0o100644).unwrap();
        let tree_oid = tb.write().unwrap();

        let tree = repo.find_tree(tree_oid).unwrap();
        assert_eq!(tree.len(), 2);

        let entry_a = tree.get_name("a.txt").unwrap();
        assert_eq!(entry_a.id(), b1);

        let entry_b = tree.get_name("b.txt").unwrap();
        assert_eq!(entry_b.id(), b2);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn commit_with_parents() {
    let store = store().await;
    let repo_id = store.create_repository(&format!("commit-parents-{}", std::process::id())).await.unwrap();

    tokio::task::spawn_blocking(move || {
        let repo = store.open_repository(repo_id).unwrap();
        let sig = git2::Signature::now("Test", "test@test.com").unwrap();

        // First commit
        let b1 = repo.blob(b"v1").unwrap();
        let mut tb = repo.treebuilder(None).unwrap();
        tb.insert("file.txt", b1, 0o100644).unwrap();
        let t1 = repo.find_tree(tb.write().unwrap()).unwrap();
        let c1 = repo.commit(Some("refs/heads/main"), &sig, &sig, "first", &t1, &[]).unwrap();

        // Second commit
        let b2 = repo.blob(b"v2").unwrap();
        let mut tb2 = repo.treebuilder(None).unwrap();
        tb2.insert("file.txt", b2, 0o100644).unwrap();
        let t2 = repo.find_tree(tb2.write().unwrap()).unwrap();
        let parent = repo.find_commit(c1).unwrap();
        let c2 = repo.commit(Some("refs/heads/main"), &sig, &sig, "second", &t2, &[&parent]).unwrap();

        // Verify
        let commit = repo.find_commit(c2).unwrap();
        assert_eq!(commit.parent_count(), 1);
        assert_eq!(commit.parent_id(0).unwrap(), c1);
        assert_eq!(commit.message().unwrap(), "second");

        // Ref should point to c2
        let r = repo.find_reference("refs/heads/main").unwrap();
        assert_eq!(r.target().unwrap(), c2);
    })
    .await
    .unwrap();
}

// ============================================================
// Ref management tests
// ============================================================

#[tokio::test]
async fn ref_create_and_iterate() {
    let store = store().await;
    let repo_id = store.create_repository(&format!("ref-iter-{}", std::process::id())).await.unwrap();

    tokio::task::spawn_blocking(move || {
        let repo = store.open_repository(repo_id).unwrap();
        let sig = git2::Signature::now("Test", "test@test.com").unwrap();

        let blob = repo.blob(b"data").unwrap();
        let mut tb = repo.treebuilder(None).unwrap();
        tb.insert("f.txt", blob, 0o100644).unwrap();
        let tree = repo.find_tree(tb.write().unwrap()).unwrap();

        // Create commits on two branches
        let c1 = repo.commit(Some("refs/heads/main"), &sig, &sig, "on main", &tree, &[]).unwrap();
        let c2 = repo.commit(Some("refs/heads/dev"), &sig, &sig, "on dev", &tree, &[]).unwrap();

        // Iterate all refs
        let refs: Vec<String> = repo
            .references()
            .unwrap()
            .filter_map(|r| r.ok())
            .filter_map(|r| r.name().map(String::from))
            .collect();

        assert!(refs.contains(&"refs/heads/main".to_string()));
        assert!(refs.contains(&"refs/heads/dev".to_string()));

        // Glob iteration
        let head_refs: Vec<String> = repo
            .references_glob("refs/heads/*")
            .unwrap()
            .filter_map(|r| r.ok())
            .filter_map(|r| r.name().map(String::from))
            .collect();
        assert_eq!(head_refs.len(), 2);

        // Verify targets
        assert_eq!(repo.find_reference("refs/heads/main").unwrap().target().unwrap(), c1);
        assert_eq!(repo.find_reference("refs/heads/dev").unwrap().target().unwrap(), c2);
    })
    .await
    .unwrap();
}

// ============================================================
// Porcelain API tests
// ============================================================

#[tokio::test]
async fn porcelain_commit_and_read() {
    let store = store().await;
    let repo = create_repo(&store, "porcelain-cr").await;

    let oid = repo
        .commit(
            "main",
            &[
                ("src/main.rs", b"fn main() {}" as &[u8]),
                ("README.md", b"# Hello"),
            ],
            "init",
            "Alice",
            "alice@test.com",
        )
        .await
        .unwrap();

    // Read files back
    let main_rs = repo.read_file("main", "src/main.rs").await.unwrap();
    assert_eq!(main_rs.unwrap(), b"fn main() {}");

    let readme = repo.read_file("main", "README.md").await.unwrap();
    assert_eq!(readme.unwrap(), b"# Hello");

    // Non-existent file
    let missing = repo.read_file("main", "nope.txt").await.unwrap();
    assert!(missing.is_none());

    // List files
    let files = repo.list_files("main").await.unwrap();
    assert_eq!(files, vec!["README.md", "src/main.rs"]);

    // Log
    let log = repo.log("main", 10).await.unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].oid, oid);
    assert_eq!(log[0].message.trim(), "init");
    assert_eq!(log[0].author_name, "Alice");
}

#[tokio::test]
async fn porcelain_incremental_commit() {
    let store = store().await;
    let repo = create_repo(&store, "porcelain-inc").await;

    // First commit
    repo.commit(
        "main",
        &[("a.txt", b"aaa" as &[u8]), ("b.txt", b"bbb")],
        "first",
        "A",
        "a@t.com",
    )
    .await
    .unwrap();

    // Second commit: update a.txt, add c.txt — b.txt should survive
    repo.commit(
        "main",
        &[("a.txt", b"AAA" as &[u8]), ("c.txt", b"ccc")],
        "second",
        "B",
        "b@t.com",
    )
    .await
    .unwrap();

    let files = repo.list_files("main").await.unwrap();
    assert_eq!(files, vec!["a.txt", "b.txt", "c.txt"]);

    assert_eq!(repo.read_file("main", "a.txt").await.unwrap().unwrap(), b"AAA");
    assert_eq!(repo.read_file("main", "b.txt").await.unwrap().unwrap(), b"bbb");
    assert_eq!(repo.read_file("main", "c.txt").await.unwrap().unwrap(), b"ccc");

    let log = repo.log("main", 10).await.unwrap();
    assert_eq!(log.len(), 2);
    assert_eq!(log[0].message.trim(), "second");
    assert_eq!(log[1].message.trim(), "first");
}

#[tokio::test]
async fn porcelain_nested_directories() {
    let store = store().await;
    let repo = create_repo(&store, "porcelain-nested").await;

    repo.commit(
        "main",
        &[
            ("src/a/b/c.txt", b"deep" as &[u8]),
            ("src/a/d.txt", b"less deep"),
            ("top.txt", b"top"),
        ],
        "nested",
        "A",
        "a@t.com",
    )
    .await
    .unwrap();

    let files = repo.list_files("main").await.unwrap();
    assert_eq!(
        files,
        vec!["src/a/b/c.txt", "src/a/d.txt", "top.txt"]
    );

    assert_eq!(
        repo.read_file("main", "src/a/b/c.txt").await.unwrap().unwrap(),
        b"deep"
    );
}

// ============================================================
// Diff tests
// ============================================================

#[tokio::test]
async fn diff_between_commits() {
    let store = store().await;
    let repo = create_repo(&store, "diff-commits").await;

    let oid1 = repo
        .commit(
            "main",
            &[("f.txt", b"line1\n" as &[u8])],
            "c1",
            "A",
            "a@t.com",
        )
        .await
        .unwrap();

    let oid2 = repo
        .commit(
            "main",
            &[("f.txt", b"line1\nline2\n" as &[u8]), ("g.txt", b"new\n")],
            "c2",
            "A",
            "a@t.com",
        )
        .await
        .unwrap();

    let diff = repo.diff(oid1, oid2).await.unwrap();
    assert_eq!(diff.stats.files_changed, 2);
    assert_eq!(diff.stats.insertions, 2); // "line2\n" + "new\n"
    assert_eq!(diff.stats.deletions, 0);

    let statuses: Vec<_> = diff.files.iter().map(|f| f.status).collect();
    assert!(statuses.contains(&pggit::DiffStatus::Modified));
    assert!(statuses.contains(&pggit::DiffStatus::Added));

    // Patch should contain the diff
    let patch = diff.to_patch();
    assert!(patch.contains("+line2"));
    assert!(patch.contains("+new"));
}

#[tokio::test]
async fn diff_initial_commit() {
    let store = store().await;
    let repo = create_repo(&store, "diff-init").await;

    let oid = repo
        .commit(
            "main",
            &[("a.txt", b"hello\n" as &[u8]), ("b.txt", b"world\n")],
            "init",
            "A",
            "a@t.com",
        )
        .await
        .unwrap();

    let diff = repo.diff_initial(oid).await.unwrap();
    assert_eq!(diff.stats.files_changed, 2);
    assert_eq!(diff.stats.insertions, 2);
    assert_eq!(diff.stats.deletions, 0);
}

// ============================================================
// Checkout + git CLI validation tests
// ============================================================

#[tokio::test]
async fn checkout_produces_valid_git_repo() {
    let store = store().await;
    let repo = create_repo(&store, "checkout-valid").await;

    repo.commit(
        "main",
        &[
            ("src/main.rs", b"fn main() { println!(\"hi\"); }\n" as &[u8]),
            ("README.md", b"# Test\n"),
        ],
        "Initial commit",
        "Alice",
        "alice@test.com",
    )
    .await
    .unwrap();

    repo.commit(
        "main",
        &[("src/main.rs", b"fn main() { println!(\"hello\"); }\n" as &[u8])],
        "Update main",
        "Bob",
        "bob@test.com",
    )
    .await
    .unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("checkout");
    repo.checkout("main", dir.to_str().unwrap()).await.unwrap();

    // git log should show both commits
    let log = git(&dir, &["log", "--oneline"]);
    assert!(log.contains("Update main"));
    assert!(log.contains("Initial commit"));

    // git status should be clean
    let status = git(&dir, &["status", "--porcelain"]);
    assert!(status.trim().is_empty(), "git status not clean: {}", status);

    // File content should match
    let content = std::fs::read_to_string(dir.join("src/main.rs")).unwrap();
    assert!(content.contains("hello"));

    // README should still be present (from first commit, preserved by second)
    assert!(dir.join("README.md").exists());
}

#[tokio::test]
async fn checkout_git_cat_file_matches() {
    let store = store().await;
    let repo = create_repo(&store, "checkout-cat").await;

    let data = b"Exact content check\n";
    repo.commit(
            "main",
            &[("test.txt", data as &[u8])],
            "test",
            "T",
            "t@t.com",
        )
        .await
        .unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("co");
    repo.checkout("main", dir.to_str().unwrap()).await.unwrap();

    // Use git cat-file to read the blob from the local git repo
    let blob_hash = git(&dir, &["hash-object", "test.txt"]);
    let blob_content = git(&dir, &["cat-file", "-p", blob_hash.trim()]);
    assert_eq!(blob_content.as_bytes(), data);

    // Verify commit exists in local repo
    let show = git(&dir, &["show", "--stat", "HEAD"]);
    assert!(show.contains("test.txt"));
}

#[tokio::test]
async fn checkout_git_diff_matches_our_diff() {
    let store = store().await;
    let repo = create_repo(&store, "checkout-gdiff").await;

    let oid1 = repo
        .commit(
            "main",
            &[("f.txt", b"old\n" as &[u8])],
            "v1",
            "A",
            "a@t.com",
        )
        .await
        .unwrap();

    let oid2 = repo
        .commit(
            "main",
            &[("f.txt", b"new\n" as &[u8])],
            "v2",
            "A",
            "a@t.com",
        )
        .await
        .unwrap();

    // Our diff
    let our_diff = repo.diff(oid1, oid2).await.unwrap();
    assert_eq!(our_diff.stats.files_changed, 1);
    assert_eq!(our_diff.stats.insertions, 1);
    assert_eq!(our_diff.stats.deletions, 1);

    // Checkout and use git diff
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("co");
    repo.checkout("main", dir.to_str().unwrap()).await.unwrap();

    let git_diff = git(&dir, &["diff", "HEAD~1", "HEAD", "--stat"]);
    assert!(git_diff.contains("f.txt"));
    assert!(git_diff.contains("1 insertion"));
    assert!(git_diff.contains("1 deletion"));
}

// ============================================================
// Concurrent access test
// ============================================================

#[tokio::test]
async fn concurrent_repos_no_interference() {
    let store = store().await;

    let mut handles = Vec::new();
    for i in 0..5 {
        let store = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            let name = format!("concurrent-{}-{}", i, std::process::id());
            let repo = store.get_or_create_repository(&name).await.unwrap();

            for j in 0..3 {
                let content = format!("repo {} commit {}\n", i, j);
                repo.commit(
                    "main",
                    &[("data.txt", content.as_bytes())],
                    &format!("c{}", j),
                    "Bot",
                    "bot@t.com",
                )
                .await
                .unwrap();
            }

            let log = repo.log("main", 100).await.unwrap();
            assert_eq!(log.len(), 3, "repo {} has wrong commit count", i);

            let content = repo.read_file("main", "data.txt").await.unwrap().unwrap();
            let expected = format!("repo {} commit 2\n", i);
            assert_eq!(content, expected.as_bytes(), "repo {} wrong content", i);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}
