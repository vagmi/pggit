use std::os::raw::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::Arc;

use libgit2_sys as raw;

use crate::db::queries;
use crate::store::PgGitStore;

const OID_SIZE: usize = raw::GIT_OID_RAWSZ;
const OID_HEXSIZE: usize = raw::GIT_OID_HEXSZ;

/// Wrap a callback body in catch_unwind to prevent panics unwinding through C.
fn catch_panic(f: impl FnOnce() -> i32 + std::panic::UnwindSafe) -> i32 {
    catch_unwind(f).unwrap_or(-1)
}

/// Custom ODB backend that stores git objects in PostgreSQL.
///
/// Memory layout: starts with `git_odb_backend` so we can cast between them.
/// The `store` Arc is kept alive for the lifetime of this backend.
#[repr(C)]
pub(crate) struct PostgresOdbBackend {
    pub parent: raw::git_odb_backend,
    store: *const PgGitStore,
    _prevent_drop: *const Arc<PgGitStore>,
    repo_id: i32,
}

unsafe impl Send for PostgresOdbBackend {}
unsafe impl Sync for PostgresOdbBackend {}

impl PostgresOdbBackend {
    pub fn new(store: &Arc<PgGitStore>, repo_id: i32) -> Self {
        let arc_clone = store.clone();
        let store_ptr = Arc::as_ptr(store);
        let arc_box = Box::new(arc_clone);
        let arc_ptr = Box::into_raw(arc_box);

        let mut parent: raw::git_odb_backend = unsafe { std::mem::zeroed() };
        parent.version = raw::GIT_ODB_BACKEND_VERSION;
        parent.read = Some(pg_odb_read);
        parent.read_header = Some(pg_odb_read_header);
        parent.read_prefix = Some(pg_odb_read_prefix);
        parent.write = Some(pg_odb_write);
        parent.exists = Some(pg_odb_exists);
        parent.exists_prefix = Some(pg_odb_exists_prefix);
        parent.foreach = Some(pg_odb_foreach);
        parent.free = Some(pg_odb_free);

        Self {
            parent,
            store: store_ptr,
            _prevent_drop: arc_ptr as *const _,
            repo_id,
        }
    }

    fn store(&self) -> &PgGitStore {
        unsafe { &*self.store }
    }
}

unsafe fn get_backend<'a>(backend: *mut raw::git_odb_backend) -> &'a PostgresOdbBackend {
    unsafe { &*(backend as *const PostgresOdbBackend) }
}

extern "C" fn pg_odb_read(
    data_p: *mut *mut c_void,
    len_p: *mut usize,
    type_p: *mut raw::git_object_t,
    backend: *mut raw::git_odb_backend,
    oid: *const raw::git_oid,
) -> i32 {
    catch_panic(AssertUnwindSafe(|| {
        let pg = unsafe { get_backend(backend) };
        let store = pg.store();
        let oid_bytes = unsafe { &(&(*oid).id)[..OID_SIZE] };

        let result =
            store
                .rt_handle
                .block_on(queries::read_object(&store.pool, pg.repo_id, oid_bytes));

        match result {
            Ok(row) => unsafe {
                let buf = raw::git_odb_backend_data_alloc(backend, row.content.len());
                if buf.is_null() {
                    return -1;
                }
                ptr::copy_nonoverlapping(
                    row.content.as_ptr(),
                    buf as *mut u8,
                    row.content.len(),
                );
                *data_p = buf;
                *len_p = row.size as usize;
                *type_p = row.object_type as raw::git_object_t;
                0
            },
            Err(crate::error::PgGitError::NotFound(_)) => raw::GIT_ENOTFOUND,
            Err(_) => -1,
        }
    }))
}

extern "C" fn pg_odb_read_header(
    len_p: *mut usize,
    type_p: *mut raw::git_object_t,
    backend: *mut raw::git_odb_backend,
    oid: *const raw::git_oid,
) -> i32 {
    catch_panic(AssertUnwindSafe(|| {
        let pg = unsafe { get_backend(backend) };
        let store = pg.store();
        let oid_bytes = unsafe { &(&(*oid).id)[..OID_SIZE] };

        let result = store.rt_handle.block_on(queries::read_object_header(
            &store.pool,
            pg.repo_id,
            oid_bytes,
        ));

        match result {
            Ok((obj_type, size)) => unsafe {
                *len_p = size as usize;
                *type_p = obj_type as raw::git_object_t;
                0
            },
            Err(crate::error::PgGitError::NotFound(_)) => raw::GIT_ENOTFOUND,
            Err(_) => -1,
        }
    }))
}

extern "C" fn pg_odb_read_prefix(
    out_oid: *mut raw::git_oid,
    data_p: *mut *mut c_void,
    len_p: *mut usize,
    type_p: *mut raw::git_object_t,
    backend: *mut raw::git_odb_backend,
    short_oid: *const raw::git_oid,
    prefix_len: usize,
) -> i32 {
    catch_panic(AssertUnwindSafe(|| {
        if prefix_len == OID_HEXSIZE {
            let ret = pg_odb_read(data_p, len_p, type_p, backend, short_oid);
            if ret == 0 {
                unsafe { (*out_oid).id = (*short_oid).id };
            }
            return ret;
        }

        let pg = unsafe { get_backend(backend) };
        let store = pg.store();
        let byte_len = ((prefix_len + 1) / 2) as i32;
        let prefix_bytes = unsafe { &(&(*short_oid).id)[..byte_len as usize] };

        let result = store.rt_handle.block_on(queries::read_object_prefix(
            &store.pool,
            pg.repo_id,
            prefix_bytes,
            byte_len,
        ));

        match result {
            Ok(row) => unsafe {
                ptr::copy_nonoverlapping(row.oid.as_ptr(), (*out_oid).id.as_mut_ptr(), OID_SIZE);
                let buf = raw::git_odb_backend_data_alloc(backend, row.content.len());
                if buf.is_null() {
                    return -1;
                }
                ptr::copy_nonoverlapping(
                    row.content.as_ptr(),
                    buf as *mut u8,
                    row.content.len(),
                );
                *data_p = buf;
                *len_p = row.size as usize;
                *type_p = row.object_type as raw::git_object_t;
                0
            },
            Err(crate::error::PgGitError::NotFound(_)) => raw::GIT_ENOTFOUND,
            Err(crate::error::PgGitError::Ambiguous(_)) => raw::GIT_EAMBIGUOUS,
            Err(_) => -1,
        }
    }))
}

extern "C" fn pg_odb_write(
    backend: *mut raw::git_odb_backend,
    oid: *const raw::git_oid,
    data: *const c_void,
    len: usize,
    obj_type: raw::git_object_t,
) -> i32 {
    catch_panic(AssertUnwindSafe(|| {
        let pg = unsafe { get_backend(backend) };
        let store = pg.store();
        let oid_bytes = unsafe { &(&(*oid).id)[..OID_SIZE] };
        let content = unsafe { std::slice::from_raw_parts(data as *const u8, len) };

        let result = store.rt_handle.block_on(queries::write_object(
            &store.pool,
            pg.repo_id,
            oid_bytes,
            obj_type as i16,
            len as i32,
            content,
        ));

        match result {
            Ok(()) => 0,
            Err(_) => -1,
        }
    }))
}

extern "C" fn pg_odb_exists(backend: *mut raw::git_odb_backend, oid: *const raw::git_oid) -> i32 {
    catch_panic(AssertUnwindSafe(|| {
        let pg = unsafe { get_backend(backend) };
        let store = pg.store();
        let oid_bytes = unsafe { &(&(*oid).id)[..OID_SIZE] };

        let result = store
            .rt_handle
            .block_on(queries::object_exists(&store.pool, pg.repo_id, oid_bytes));

        match result {
            Ok(true) => 1,
            Ok(false) => 0,
            Err(_) => 0,
        }
    }))
}

extern "C" fn pg_odb_exists_prefix(
    out_oid: *mut raw::git_oid,
    backend: *mut raw::git_odb_backend,
    short_oid: *const raw::git_oid,
    prefix_len: usize,
) -> i32 {
    catch_panic(AssertUnwindSafe(|| {
        if prefix_len == OID_HEXSIZE {
            if pg_odb_exists(backend, short_oid) == 0 {
                return raw::GIT_ENOTFOUND;
            }
            unsafe { (*out_oid).id = (*short_oid).id };
            return 0;
        }

        let pg = unsafe { get_backend(backend) };
        let store = pg.store();
        let byte_len = ((prefix_len + 1) / 2) as i32;
        let prefix_bytes = unsafe { &(&(*short_oid).id)[..byte_len as usize] };

        let result = store.rt_handle.block_on(queries::object_exists_prefix(
            &store.pool,
            pg.repo_id,
            prefix_bytes,
            byte_len,
        ));

        match result {
            Ok(full_oid) => unsafe {
                ptr::copy_nonoverlapping(
                    full_oid.as_ptr(),
                    (*out_oid).id.as_mut_ptr(),
                    OID_SIZE,
                );
                0
            },
            Err(crate::error::PgGitError::NotFound(_)) => raw::GIT_ENOTFOUND,
            Err(crate::error::PgGitError::Ambiguous(_)) => raw::GIT_EAMBIGUOUS,
            Err(_) => -1,
        }
    }))
}

extern "C" fn pg_odb_foreach(
    backend: *mut raw::git_odb_backend,
    cb: raw::git_odb_foreach_cb,
    payload: *mut c_void,
) -> i32 {
    catch_panic(AssertUnwindSafe(|| {
        let cb = match cb {
            Some(f) => f,
            None => return 0,
        };

        let pg = unsafe { get_backend(backend) };
        let store = pg.store();

        let result = store
            .rt_handle
            .block_on(queries::all_oids(&store.pool, pg.repo_id));

        match result {
            Ok(oids) => {
                for oid_bytes in &oids {
                    let mut oid: raw::git_oid = unsafe { std::mem::zeroed() };
                    let copy_len = oid_bytes.len().min(OID_SIZE);
                    oid.id[..copy_len].copy_from_slice(&oid_bytes[..copy_len]);

                    let ret = cb(&oid, payload);
                    if ret != 0 {
                        return ret;
                    }
                }
                0
            }
            Err(_) => -1,
        }
    }))
}

extern "C" fn pg_odb_free(backend: *mut raw::git_odb_backend) {
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        let pg = backend as *mut PostgresOdbBackend;
        let arc_ptr = (*pg)._prevent_drop as *mut Arc<PgGitStore>;
        if !arc_ptr.is_null() {
            drop(Box::from_raw(arc_ptr));
        }
        drop(Box::from_raw(pg));
    }));
}
