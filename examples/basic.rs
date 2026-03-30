use std::sync::Arc;

use pggit::PgGitStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "postgresql://postgres@localhost:17266/postgres".to_string());

    println!("Connecting to {}", database_url);
    let store = PgGitStore::connect(&database_url).await?;

    println!("Running migrations...");
    store.migrate().await?;

    println!("Getting or creating repository 'test-repo2'...");
    let repo_id = match store.get_repository_id("test-repo2").await {
        Ok(id) => {
            println!("Found existing repository with id={}", id);
            id
        }
        Err(_) => {
            let id = store.create_repository("test-repo2").await?;
            println!("Created repository with id={}", id);
            id
        }
    };

    let store_clone = Arc::clone(&store);
    tokio::task::spawn_blocking(move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let repo = store_clone.open_repository(repo_id)?;

        // === Phase 1: Objects ===
        println!("\n--- Phase 1: Objects ---");

        let blob_oid = repo.blob(b"Hello from pggit!\n")?;
        println!("Blob OID: {}", blob_oid);

        let blob = repo.find_blob(blob_oid)?;
        assert_eq!(blob.content(), b"Hello from pggit!\n");
        println!("Blob roundtrip OK");

        let mut tb = repo.treebuilder(None)?;
        tb.insert("hello.txt", blob_oid, 0o100644)?;
        let tree_oid = tb.write()?;
        let tree = repo.find_tree(tree_oid)?;
        println!("Tree OID: {} ({} entries)", tree_oid, tree.len());

        // === Phase 2: Refs + Commits ===
        println!("\n--- Phase 2: Refs + Commits ---");

        let sig = git2::Signature::now("Test User", "test@example.com")?;

        // First commit - updates refs/heads/main
        let commit_oid = repo.commit(
            Some("refs/heads/main"),
            &sig,
            &sig,
            "Initial commit",
            &tree,
            &[],
        )?;
        println!("Commit 1 OID: {}", commit_oid);

        // Verify the ref was created
        let main_ref = repo.find_reference("refs/heads/main")?;
        println!(
            "refs/heads/main -> {}",
            main_ref.target().unwrap()
        );
        assert_eq!(main_ref.target().unwrap(), commit_oid);

        // Second commit (child of first)
        let blob2_oid = repo.blob(b"Updated content\n")?;
        let mut tb2 = repo.treebuilder(None)?;
        tb2.insert("hello.txt", blob2_oid, 0o100644)?;
        tb2.insert("readme.md", repo.blob(b"# Test\n")?, 0o100644)?;
        let tree2_oid = tb2.write()?;
        let tree2 = repo.find_tree(tree2_oid)?;

        let parent = repo.find_commit(commit_oid)?;
        let commit2_oid = repo.commit(
            Some("refs/heads/main"),
            &sig,
            &sig,
            "Add readme",
            &tree2,
            &[&parent],
        )?;
        println!("Commit 2 OID: {}", commit2_oid);

        // Verify ref was updated
        let main_ref = repo.find_reference("refs/heads/main")?;
        assert_eq!(main_ref.target().unwrap(), commit2_oid);
        println!("refs/heads/main updated -> {}", commit2_oid);

        // Read commit and verify parent
        let commit2 = repo.find_commit(commit2_oid)?;
        assert_eq!(commit2.parent_id(0)?, commit_oid);
        println!("Commit 2 parent: {} (correct)", commit_oid);

        // Iterate refs
        println!("\nAll refs:");
        for r in repo.references()? {
            let r = r?;
            if let Some(name) = r.name() {
                if let Some(oid) = r.target() {
                    println!("  {} -> {}", name, oid);
                } else if let Some(target) = r.symbolic_target() {
                    println!("  {} -> {} (symbolic)", name, target);
                }
            }
        }

        println!("\nAll Phase 2 operations successful!");
        Ok(())
    })
    .await?
    .map_err(|e| -> Box<dyn std::error::Error> { e })?;

    // Verify data in PG
    println!("\n--- Database state ---");
    let objects: Vec<(i32, i16, i32)> = sqlx::query_as(
        "SELECT repo_id, type, size FROM objects WHERE repo_id=$1 ORDER BY type",
    )
    .bind(repo_id)
    .fetch_all(store.pool())
    .await?;
    println!("Objects: {} total", objects.len());
    for (_, t, s) in &objects {
        let tname = match t {
            1 => "commit",
            2 => "tree",
            3 => "blob",
            _ => "?",
        };
        println!("  {} (size={})", tname, s);
    }

    let refs: Vec<(String, Option<Vec<u8>>, Option<String>)> = sqlx::query_as(
        "SELECT name, oid, symbolic FROM refs WHERE repo_id=$1",
    )
    .bind(repo_id)
    .fetch_all(store.pool())
    .await?;
    println!("Refs: {} total", refs.len());
    for (name, oid, sym) in &refs {
        if let Some(oid) = oid {
            let hex: String = oid.iter().map(|b| format!("{:02x}", b)).collect();
            println!("  {} -> {}", name, hex);
        } else if let Some(sym) = sym {
            println!("  {} -> {} (symbolic)", name, sym);
        }
    }

    Ok(())
}
