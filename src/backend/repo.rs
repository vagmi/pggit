use std::ptr;
use std::sync::Arc;

use libgit2_sys as raw;

use crate::backend::odb::PostgresOdbBackend;
use crate::error::{PgGitError, Result};
use crate::store::PgGitStore;

/// Create a `git2::Repository` backed by PostgreSQL for the given repo.
///
/// The returned repository has a custom ODB backend that reads/writes
/// objects to the `objects` table in PostgreSQL.
pub fn open_pg_repo(store: &Arc<PgGitStore>, repo_id: i32) -> Result<git2::Repository> {
    unsafe {
        // Ensure libgit2 is initialized
        raw::git_libgit2_init();

        // Create a new empty repository (no filesystem backing)
        let mut repo_ptr: *mut raw::git_repository = ptr::null_mut();
        check_lg2(raw::git_repository_new(&mut repo_ptr))?;

        // Create a new ODB
        let mut odb_ptr: *mut raw::git_odb = ptr::null_mut();
        let rc = raw::git_odb_new(&mut odb_ptr);
        if rc < 0 {
            raw::git_repository_free(repo_ptr);
            return Err(last_git_error());
        }

        // Create the ODB backend and add it
        let odb_backend = Box::into_raw(Box::new(PostgresOdbBackend::new(store, repo_id)));
        let rc = raw::git_odb_add_backend(odb_ptr, &mut (*odb_backend).parent, 1);
        if rc < 0 {
            drop(Box::from_raw(odb_backend));
            raw::git_odb_free(odb_ptr);
            raw::git_repository_free(repo_ptr);
            return Err(last_git_error());
        }

        // Attach ODB to repository
        let rc = raw::git_repository_set_odb(repo_ptr, odb_ptr);
        // Release our reference; the repo now owns it
        raw::git_odb_free(odb_ptr);
        if rc < 0 {
            raw::git_repository_free(repo_ptr);
            return Err(last_git_error());
        }

        // Safety: git2::Repository is a #[repr(Rust)] struct with a single
        // `*mut git_repository` field. We use transmute to construct it
        // because `Binding::from_raw` is not publicly accessible.
        Ok(std::mem::transmute::<*mut raw::git_repository, git2::Repository>(repo_ptr))
    }
}

fn check_lg2(rc: i32) -> Result<()> {
    if rc < 0 {
        Err(last_git_error())
    } else {
        Ok(())
    }
}

fn last_git_error() -> PgGitError {
    PgGitError::Git(git2::Error::last_error(-1))
}
