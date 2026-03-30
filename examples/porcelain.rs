use pggit::PgGitStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");

    let store = PgGitStore::connect(&database_url).await?;
    store.migrate().await?;

    // Get or create a repo
    let repo = store.get_or_create_repository("porcelain-demo").await?;
    println!("Repository: {} (id={})", repo.name(), repo.repo_id());

    // First commit: create some files
    println!("\n--- Commit 1: Initial files ---");
    let oid1 = repo
        .commit(
            "main",
            &[
                ("src/main.rs", b"fn main() {\n    println!(\"Hello\");\n}\n"),
                ("src/lib.rs", b"pub fn add(a: i32, b: i32) -> i32 {\n    a + b\n}\n"),
                ("README.md", b"# My Project\n\nA demo project.\n"),
                ("Cargo.toml", b"[package]\nname = \"demo\"\nversion = \"0.1.0\"\n"),
            ],
            "Initial commit",
            "Alice",
            "alice@example.com",
        )
        .await?;
    println!("Commit: {}", oid1);

    // List files
    let files = repo.list_files("main").await?;
    println!("Files: {:?}", files);

    // Read a file
    let content = repo.read_file("main", "src/main.rs").await?;
    println!(
        "src/main.rs:\n{}",
        std::str::from_utf8(&content.unwrap()).unwrap()
    );

    // Second commit: modify and add files
    println!("--- Commit 2: Update files ---");
    let oid2 = repo
        .commit(
            "main",
            &[
                (
                    "src/main.rs",
                    b"fn main() {\n    println!(\"Hello, pggit!\");\n}\n",
                ),
                ("src/utils.rs", b"pub fn greet(name: &str) -> String {\n    format!(\"Hello, {}!\", name)\n}\n"),
            ],
            "Update main and add utils",
            "Bob",
            "bob@example.com",
        )
        .await?;
    println!("Commit: {}", oid2);

    // List files - should have all 5 now
    let files = repo.list_files("main").await?;
    println!("Files: {:?}", files);

    // Read the updated file
    let content = repo.read_file("main", "src/main.rs").await?;
    println!(
        "src/main.rs (updated):\n{}",
        std::str::from_utf8(&content.unwrap()).unwrap()
    );

    // Original files should still be there
    let readme = repo.read_file("main", "README.md").await?;
    assert!(readme.is_some(), "README.md should still exist");
    println!("README.md still present: OK");

    // Log
    println!("\n--- Commit log ---");
    let log = repo.log("main", 10).await?;
    for entry in &log {
        println!(
            "  {} {} <{}> - {}",
            &entry.oid.to_string()[..8],
            entry.author_name,
            entry.author_email,
            entry.message.trim()
        );
    }
    assert_eq!(log.len(), 2);
    assert_eq!(log[0].parent_ids.len(), 1);
    assert_eq!(log[1].parent_ids.len(), 0);

    // Non-existent file returns None
    let missing = repo.read_file("main", "does-not-exist.txt").await?;
    assert!(missing.is_none());
    println!("\nMissing file returns None: OK");

    println!("\nAll porcelain operations successful!");
    Ok(())
}
