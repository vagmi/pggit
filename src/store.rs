use std::sync::Arc;

use sqlx::PgPool;
use tokio::runtime::Handle;

use crate::backend;
use crate::db::{queries, schema};
use crate::error::Result;
use crate::porcelain::PgRepository;

/// Shared state used by ODB/RefDB backends to access PostgreSQL.
/// Stored behind an Arc so callbacks can safely reference it.
pub struct PgGitStore {
    pub(crate) pool: PgPool,
    pub(crate) rt_handle: Handle,
}

impl PgGitStore {
    /// Connect to PostgreSQL and return a store.
    pub async fn connect(database_url: &str) -> Result<Arc<Self>> {
        let pool = PgPool::connect(database_url).await?;
        Ok(Arc::new(Self {
            pool,
            rt_handle: Handle::current(),
        }))
    }

    /// Create a store from an existing pool.
    pub fn from_pool(pool: PgPool) -> Arc<Self> {
        Arc::new(Self {
            pool,
            rt_handle: Handle::current(),
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
