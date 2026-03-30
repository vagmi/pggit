use std::sync::Arc;

use pggit::PgGitStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");

    let store = PgGitStore::connect(&database_url).await?;
    store.migrate().await?;

    let num_repos = 10;
    let commits_per_repo = 5;

    println!(
        "Testing concurrent access: {} repos x {} commits each",
        num_repos, commits_per_repo
    );

    // Spawn tasks that each create a separate repo and make multiple commits
    let mut handles = Vec::new();
    for i in 0..num_repos {
        let store = Arc::clone(&store);
        let handle = tokio::spawn(async move {
            let repo_name = format!("concurrent-test-{}", i);
            let repo = store.get_or_create_repository(&repo_name).await?;

            for j in 0..commits_per_repo {
                let content = format!("// Repo {} commit {}\nfn main() {{}}\n", i, j);
                let readme = format!("# Repo {}\n\nCommit {}\n", i, j);
                repo.commit(
                    "main",
                    &[
                        ("src/main.rs", content.as_bytes()),
                        ("README.md", readme.as_bytes()),
                    ],
                    &format!("Commit {} of repo {}", j, i),
                    "Bot",
                    "bot@example.com",
                )
                .await?;
            }

            // Verify the log has all commits
            let log = repo.log("main", 100).await?;
            assert_eq!(
                log.len(),
                commits_per_repo,
                "repo {} should have {} commits, got {}",
                i,
                commits_per_repo,
                log.len()
            );

            // Verify latest file content
            let content = repo.read_file("main", "src/main.rs").await?;
            let expected = format!(
                "// Repo {} commit {}\nfn main() {{}}\n",
                i,
                commits_per_repo - 1
            );
            assert_eq!(
                content.as_deref(),
                Some(expected.as_bytes()),
                "repo {} content mismatch",
                i
            );

            println!("  repo {} OK ({} commits)", i, commits_per_repo);
            Ok::<_, pggit::PgGitError>(())
        });
        handles.push(handle);
    }

    // Wait for all tasks
    let mut errors = 0;
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                eprintln!("  repo {} FAILED: {}", i, e);
                errors += 1;
            }
            Err(e) => {
                eprintln!("  repo {} PANIC: {}", i, e);
                errors += 1;
            }
        }
    }

    if errors > 0 {
        eprintln!("\n{} repos failed!", errors);
        std::process::exit(1);
    }

    // Verify total objects in DB
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM repositories WHERE name LIKE 'concurrent-test-%'")
        .fetch_one(store.pool())
        .await?;
    println!("\nTotal concurrent-test repos: {}", count.0);

    let obj_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM objects")
        .fetch_one(store.pool())
        .await?;
    println!("Total objects in DB: {}", obj_count.0);

    println!("\nAll concurrent operations successful!");
    Ok(())
}
