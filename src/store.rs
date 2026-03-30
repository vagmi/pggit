use std::sync::Arc;

use sqlx::PgPool;
use tokio::runtime::Handle;

use crate::backend;
use crate::db::{queries, schema};
use crate::error::Result;

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

    /// Open a git2::Repository backed by PostgreSQL for the given repo_id.
    /// All git2 operations on this repository will read/write to the database.
    pub fn open_repository(self: &Arc<Self>, repo_id: i32) -> Result<git2::Repository> {
        backend::repo::open_pg_repo(self, repo_id)
    }
}
