use std::ptr;
use std::sync::{Arc, Once};

use libgit2_sys as raw;

use crate::backend::odb::PostgresOdbBackend;
use crate::backend::refdb::PostgresRefdbBackend;
use crate::error::{PgGitError, Result};
use crate::store::PgGitStore;

static LIBGIT2_INIT: Once = Once::new();

/// Create a `git2::Repository` backed by PostgreSQL for the given repo.
///
/// The returned repository has custom ODB and RefDB backends that
/// read/write objects and refs to PostgreSQL.
pub fn open_pg_repo(store: &Arc<PgGitStore>, repo_id: i32) -> Result<git2::Repository> {
    unsafe {
        // Ensure libgit2 is initialized exactly once (thread-safe)
        LIBGIT2_INIT.call_once(|| {
            raw::git_libgit2_init();
        });

        // Create a new empty repository (no filesystem backing)
        let mut repo_ptr: *mut raw::git_repository = ptr::null_mut();
        check_lg2(raw::git_repository_new(&mut repo_ptr))?;

        // --- ODB ---
        let mut odb_ptr: *mut raw::git_odb = ptr::null_mut();
        let rc = raw::git_odb_new(&mut odb_ptr);
        if rc < 0 {
            raw::git_repository_free(repo_ptr);
            return Err(last_git_error());
        }

        let odb_backend = Box::into_raw(Box::new(PostgresOdbBackend::new(store, repo_id)));
        let rc = raw::git_odb_add_backend(odb_ptr, &mut (*odb_backend).parent, 1);
        if rc < 0 {
            drop(Box::from_raw(odb_backend));
            raw::git_odb_free(odb_ptr);
            raw::git_repository_free(repo_ptr);
            return Err(last_git_error());
        }

        let rc = raw::git_repository_set_odb(repo_ptr, odb_ptr);
        raw::git_odb_free(odb_ptr); // repo owns it now
        if rc < 0 {
            raw::git_repository_free(repo_ptr);
            return Err(last_git_error());
        }

        // --- RefDB ---
        let mut refdb_ptr: *mut raw::git_refdb = ptr::null_mut();
        let rc = raw::git_refdb_new(&mut refdb_ptr, repo_ptr);
        if rc < 0 {
            raw::git_repository_free(repo_ptr);
            return Err(last_git_error());
        }

        let refdb_backend = Box::into_raw(Box::new(PostgresRefdbBackend::new(store, repo_id)));
        let rc = raw::git_refdb_set_backend(refdb_ptr, &mut (*refdb_backend).parent);
        if rc < 0 {
            drop(Box::from_raw(refdb_backend));
            raw::git_refdb_free(refdb_ptr);
            raw::git_repository_free(repo_ptr);
            return Err(last_git_error());
        }

        let rc = raw::git_repository_set_refdb(repo_ptr, refdb_ptr);
        raw::git_refdb_free(refdb_ptr); // repo owns it now
        if rc < 0 {
            raw::git_repository_free(repo_ptr);
            return Err(last_git_error());
        }

        // Safety: git2::Repository is a struct with a single `*mut git_repository` field.
        // We use transmute because Binding::from_raw is not publicly accessible.
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
