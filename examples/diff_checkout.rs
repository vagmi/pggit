use pggit::PgGitStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "postgresql://postgres@localhost:17266/postgres".to_string());

    let store = PgGitStore::connect(&database_url).await?;
    store.migrate().await?;

    let repo = store.get_or_create_repository("diff-checkout-demo").await?;
    println!("Repository: {}", repo.name());

    // Commit 1
    let oid1 = repo
        .commit(
            "main",
            &[
                ("src/main.rs", b"fn main() {\n    println!(\"Hello\");\n}\n"),
                ("src/lib.rs", b"pub fn add(a: i32, b: i32) -> i32 {\n    a + b\n}\n"),
                ("README.md", b"# Demo\n"),
            ],
            "Initial commit",
            "Alice",
            "alice@example.com",
        )
        .await?;
    println!("Commit 1: {}", oid1);

    // Commit 2: modify a file, add a new one, delete one
    let oid2 = repo
        .commit(
            "main",
            &[
                ("src/main.rs", b"fn main() {\n    println!(\"Hello, world!\");\n    greet();\n}\n"),
                ("src/greet.rs", b"pub fn greet() {\n    println!(\"Hi!\");\n}\n"),
                ("README.md", b"# Demo\n\nA project with greeting support.\n"),
            ],
            "Add greeting support",
            "Bob",
            "bob@example.com",
        )
        .await?;
    println!("Commit 2: {}", oid2);

    // === Diff ===
    println!("\n--- Diff between commit 1 and 2 ---");
    let diff = repo.diff(oid1, oid2).await?;
    println!("{}", diff.stat_line());
    for file in &diff.files {
        let path = file.new_path.as_deref().or(file.old_path.as_deref()).unwrap_or("?");
        println!("  {} {}", file.status, path);
    }
    println!("\nPatch:\n{}", diff.to_patch());

    // === Diff initial commit ===
    println!("--- Diff of initial commit ---");
    let diff_init = repo.diff_initial(oid1).await?;
    println!("{}", diff_init.stat_line());

    // === Checkout ===
    let tmp = tempfile::tempdir()?;
    let checkout_path = tmp.path().join("checkout");
    println!(
        "\n--- Checkout to {} ---",
        checkout_path.display()
    );
    repo.checkout("main", checkout_path.to_str().unwrap()).await?;

    // Verify files exist
    println!("Files on disk:");
    for entry in walkdir(&checkout_path, "") {
        println!("  {}", entry);
    }

    // Verify content
    let main_rs = std::fs::read_to_string(checkout_path.join("src/main.rs"))?;
    assert!(main_rs.contains("greet()"));
    println!("\nsrc/main.rs content verified OK");

    // Verify git log works on the checkout
    let output = std::process::Command::new("git")
        .args(["log", "--oneline"])
        .current_dir(&checkout_path)
        .output()?;
    let log_output = String::from_utf8_lossy(&output.stdout);
    println!("\ngit log --oneline:\n{}", log_output);
    assert!(log_output.contains("Add greeting support"));
    assert!(log_output.contains("Initial commit"));

    // Verify git diff is clean
    let output = std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .current_dir(&checkout_path)
        .output()?;
    let status = String::from_utf8_lossy(&output.stdout);
    println!("git status: {:?}", status.trim());

    println!("\nAll diff + checkout operations successful!");
    Ok(())
}

/// Simple recursive directory walker, skipping .git
fn walkdir(dir: &std::path::Path, prefix: &str) -> Vec<String> {
    let mut result = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        let mut entries: Vec<_> = entries.filter_map(|e| e.ok()).collect();
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name == ".git" {
                continue;
            }
            let path = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", prefix, name)
            };
            if entry.file_type().map_or(false, |t| t.is_dir()) {
                result.extend(walkdir(&entry.path(), &path));
            } else {
                result.push(path);
            }
        }
    }
    result
}
