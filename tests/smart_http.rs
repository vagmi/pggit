#![cfg(feature = "smart-http")]

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use pggit::{PgGitStore, PgRepository};

async fn store() -> Arc<PgGitStore> {
    init_tracing();
    let url = std::env::var("DATABASE_URL").expect(
        "DATABASE_URL must be set: postgresql://postgres@localhost:5432/pggit_test",
    );
    let store = PgGitStore::connect(&url).await.unwrap();
    store.migrate().await.unwrap();
    store
}

fn init_tracing() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "info,pggit=debug".into()),
            )
            .with_test_writer()
            .try_init();
    });
}

async fn create_repo(store: &Arc<PgGitStore>, test_name: &str) -> PgRepository {
    let name = format!("smart-http-{}-{}", test_name, std::process::id());
    store.get_or_create_repository(&name).await.unwrap()
}

/// Bind on 127.0.0.1:0, spawn axum::serve, return the bound port and a
/// shutdown handle. Drop the handle to stop the server.
async fn spawn_server(store: Arc<PgGitStore>) -> (u16, tokio::task::JoinHandle<()>) {
    let git_router = pggit::http::router(pggit::http::HttpState::new(store));
    let app: Router = Router::new().nest("/git", git_router);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    // Give the listener a moment to be ready.
    tokio::time::sleep(Duration::from_millis(50)).await;
    (port, handle)
}

fn git(dir: &std::path::Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .expect("failed to run git");
    assert!(
        output.status.success(),
        "git {} failed: {}\nstdout: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout),
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clone_via_smart_http() {
    let store = store().await;
    let repo = create_repo(&store, "clone").await;

    repo.commit(
        "main",
        &[
            ("README.md", b"# Hello smart-http" as &[u8]),
            ("src/main.rs", b"fn main() {}\n"),
        ],
        "init",
        "Alice",
        "alice@test.com",
    )
    .await
    .unwrap();

    let (port, _server) = spawn_server(Arc::clone(&store)).await;

    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("clone");
    let url = format!("http://127.0.0.1:{port}/git/{}", repo.name());
    git(tmp.path(), &["clone", &url, dest.to_str().unwrap()]);

    let readme = std::fs::read_to_string(dest.join("README.md")).unwrap();
    assert_eq!(readme, "# Hello smart-http");
    let main_rs = std::fs::read_to_string(dest.join("src/main.rs")).unwrap();
    assert_eq!(main_rs, "fn main() {}\n");

    let log = git(&dest, &["log", "--oneline"]);
    assert!(log.contains("init"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_via_smart_http_lands_in_pg() {
    let store = store().await;
    let repo = create_repo(&store, "push").await;

    repo.commit(
        "main",
        &[("a.txt", b"original\n" as &[u8])],
        "first",
        "Alice",
        "alice@test.com",
    )
    .await
    .unwrap();

    let (port, _server) = spawn_server(Arc::clone(&store)).await;

    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("clone");
    let url = format!("http://127.0.0.1:{port}/git/{}", repo.name());

    git(tmp.path(), &["clone", &url, dest.to_str().unwrap()]);
    git(&dest, &["config", "user.email", "bob@test.com"]);
    git(&dest, &["config", "user.name", "Bob"]);
    git(&dest, &["config", "http.receivepack", "true"]);

    std::fs::write(dest.join("a.txt"), b"changed\n").unwrap();
    std::fs::write(dest.join("b.txt"), b"brand new\n").unwrap();
    git(&dest, &["add", "."]);
    git(&dest, &["commit", "-m", "second"]);
    git(&dest, &["push", "origin", "main"]);

    // Verify the push made it back to PG.
    let log = repo.log("main", 10).await.unwrap();
    assert_eq!(log.len(), 2, "expected 2 commits in PG after push");
    assert_eq!(log[0].message.trim(), "second");
    assert_eq!(log[0].author_name, "Bob");

    let new_a = repo.read_file("main", "a.txt").await.unwrap().unwrap();
    assert_eq!(new_a, b"changed\n");
    let new_b = repo.read_file("main", "b.txt").await.unwrap().unwrap();
    assert_eq!(new_b, b"brand new\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clone_unknown_repo_returns_404() {
    let store = store().await;
    let (port, _server) = spawn_server(store).await;

    let tmp = tempfile::tempdir().unwrap();
    let url = format!("http://127.0.0.1:{port}/git/does-not-exist");
    let output = Command::new("git")
        .args(["clone", &url, "out"])
        .current_dir(tmp.path())
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .expect("failed to run git");
    assert!(!output.status.success(), "clone of missing repo should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("404") || stderr.contains("not found"),
        "expected 404 in stderr, got: {stderr}"
    );
}
