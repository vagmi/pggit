use std::sync::Arc;

use sqlx::PgPool;

use crate::backend;
use crate::db::{queries, schema};
use crate::error::Result;
use crate::porcelain::PgRepository;

/// A dedicated multi-thread tokio runtime for FFI callbacks.
/// The key insight: `Handle::block_on()` runs the future on the CALLING thread,
/// but uses the runtime's I/O driver and timer. So:
/// - The future runs on the `spawn_blocking` thread (no Send needed)
/// - I/O (Postgres queries) is driven by the dedicated runtime's workers
/// - No coupling to the caller's runtime lifecycle
pub(crate) struct FfiRuntime {
    handle: tokio::runtime::Handle,
    runtime: Option<tokio::runtime::Runtime>,
}

impl FfiRuntime {
    fn new() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("pggit-io")
            .build()
            .expect("Failed to create pggit internal runtime");
        let handle = runtime.handle().clone();
        Self {
            handle,
            runtime: Some(runtime),
        }
    }
}

impl Drop for FfiRuntime {
    fn drop(&mut self) {
        if let Some(rt) = self.runtime.take() {
            // Shut down on a separate thread to avoid
            // "Cannot drop a runtime in a context where blocking is not allowed"
            let _ = std::thread::spawn(move || drop(rt)).join();
        }
    }
}

/// Shared state used by ODB/RefDB backends to access PostgreSQL.
/// Stored behind an Arc so callbacks can safely reference it.
///
/// Uses a dedicated tokio runtime so FFI callbacks can run async SQL
/// queries without depending on (or deadlocking with) the caller's runtime.
pub struct PgGitStore {
    pub(crate) pool: PgPool,
    pub(crate) ffi_rt: FfiRuntime,
}

impl PgGitStore {
    /// Run an async future, blocking the current thread until it completes.
    /// The future runs on the current thread but uses the store's dedicated
    /// runtime for I/O. Safe to call from FFI callbacks.
    ///
    /// Unlike tokio's own Handle::block_on, this uses our dedicated runtime
    /// so it won't deadlock with the caller's runtime.
    pub(crate) fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        self.ffi_rt.handle.block_on(f)
    }

    /// Connect to PostgreSQL and return a store.
    pub async fn connect(database_url: &str) -> Result<Arc<Self>> {
        let pool = PgPool::connect(database_url).await?;
        Ok(Arc::new(Self {
            ffi_rt: FfiRuntime::new(),
            pool,
        }))
    }

    /// Create a store from an existing pool.
    pub fn from_pool(pool: PgPool) -> Arc<Self> {
        Arc::new(Self {
            ffi_rt: FfiRuntime::new(),
            pool,
        })
    }

    /// Run schema migrations to create tables.
    pub async fn migrate(&self) -> Result<()> {
        schema::migrate(&self.pool).await
    }

    /// Create a new repository and return its id.
    pub async fn create_repository(&self, name: &str) -> Result<i32> {
        queries::create_repository(&self.pool, name).await
    }

    /// Get a repository id by name.
    pub async fn get_repository_id(&self, name: &str) -> Result<i32> {
        queries::get_repository_id(&self.pool, name).await
    }

    /// Get a reference to the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Open a git2::Repository backed by PostgreSQL for the given repo_id.
    /// All git2 operations on this repository will read/write to the database.
    ///
    /// Note: git2 calls must happen on a blocking thread (e.g. `spawn_blocking`).
    /// For an async-friendly API, use [`repository()`](Self::repository) instead.
    pub fn open_repository(self: &Arc<Self>, repo_id: i32) -> Result<git2::Repository> {
        backend::repo::open_pg_repo(self, repo_id)
    }

    /// Get a high-level async repository handle by name.
    pub async fn repository(self: &Arc<Self>, name: &str) -> Result<PgRepository> {
        let repo_id = self.get_repository_id(name).await?;
        Ok(PgRepository::new(Arc::clone(self), repo_id, name.to_string()))
    }

    /// Get or create a repository, returning a high-level async handle.
    pub async fn get_or_create_repository(self: &Arc<Self>, name: &str) -> Result<PgRepository> {
        let repo_id = match self.get_repository_id(name).await {
            Ok(id) => id,
            Err(_) => self.create_repository(name).await?,
        };
        Ok(PgRepository::new(Arc::clone(self), repo_id, name.to_string()))
    }
}
