use std::path::PathBuf;
use std::sync::Arc;

use crate::store::PgGitStore;

/// Tunables for the smart-HTTP server.
#[derive(Clone, Debug)]
pub struct HttpOptions {
    /// Path to the `git` binary. Defaults to `git` (resolved on `$PATH`).
    pub git_binary: PathBuf,
    /// Whether to allow `git push` (`git-receive-pack`). Default: true.
    pub allow_push: bool,
    /// Optional override for where per-request tempdirs are created.
    /// `None` uses the system temp dir.
    pub tempdir_root: Option<PathBuf>,
}

impl Default for HttpOptions {
    fn default() -> Self {
        Self {
            git_binary: PathBuf::from("git"),
            allow_push: true,
            tempdir_root: None,
        }
    }
}

/// State shared across smart-HTTP handlers.
#[derive(Clone)]
pub struct HttpState {
    pub(crate) store: Arc<PgGitStore>,
    pub(crate) opts: Arc<HttpOptions>,
    /// Process-local mutex serializing `git-receive-pack` flows so the
    /// snapshot/CGI/reimport cycle is atomic per process. Multi-replica
    /// deployments need a stronger lock (PG advisory).
    pub(crate) push_lock: Arc<tokio::sync::Mutex<()>>,
}

impl HttpState {
    pub fn new(store: Arc<PgGitStore>) -> Self {
        Self {
            store,
            opts: Arc::new(HttpOptions::default()),
            push_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub fn with_options(store: Arc<PgGitStore>, opts: HttpOptions) -> Self {
        Self {
            store,
            opts: Arc::new(opts),
            push_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
}
