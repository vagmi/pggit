use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::sync::Arc;

use libgit2_sys as raw;

use crate::db::queries;
use crate::store::PgGitStore;

const OID_SIZE: usize = raw::GIT_OID_RAWSZ;

// These functions are in libgit2 but not bound by libgit2-sys.
// They are the official way to construct git_reference objects for custom backends.
unsafe extern "C" {
    fn git_reference__alloc(
        name: *const c_char,
        oid: *const raw::git_oid,
        peel: *const raw::git_oid,
    ) -> *mut raw::git_reference;

    fn git_reference__alloc_symbolic(
        name: *const c_char,
        target: *const c_char,
    ) -> *mut raw::git_reference;
}

/// Custom RefDB backend that stores refs in PostgreSQL.
#[repr(C)]
pub(crate) struct PostgresRefdbBackend {
    pub parent: raw::git_refdb_backend,
    store: *const PgGitStore,
    _prevent_drop: *const Arc<PgGitStore>,
    repo_id: i32,
}

unsafe impl Send for PostgresRefdbBackend {}
unsafe impl Sync for PostgresRefdbBackend {}

impl PostgresRefdbBackend {
    pub fn new(store: &Arc<PgGitStore>, repo_id: i32) -> Self {
        let arc_clone = store.clone();
        let store_ptr = Arc::as_ptr(store);
        let arc_box = Box::new(arc_clone);
        let arc_ptr = Box::into_raw(arc_box);

        let mut parent: raw::git_refdb_backend = unsafe { std::mem::zeroed() };
        parent.version = raw::GIT_REFDB_BACKEND_VERSION;
        parent.exists = Some(pg_refdb_exists);
        parent.lookup = Some(pg_refdb_lookup);
        parent.iterator = Some(pg_refdb_iterator);
        parent.write = Some(pg_refdb_write);
        parent.rename = Some(pg_refdb_rename);
        parent.del = Some(pg_refdb_del);
        parent.compress = None;
        parent.has_log = Some(pg_refdb_has_log);
        parent.ensure_log = Some(pg_refdb_ensure_log);
        parent.free = Some(pg_refdb_free);
        parent.reflog_read = Some(pg_refdb_reflog_read);
        parent.reflog_write = Some(pg_refdb_reflog_write);
        parent.reflog_rename = Some(pg_refdb_reflog_rename);
        parent.reflog_delete = Some(pg_refdb_reflog_delete);
        parent.lock = Some(pg_refdb_lock);
        parent.unlock = Some(pg_refdb_unlock);

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

unsafe fn get_backend<'a>(backend: *mut raw::git_refdb_backend) -> &'a PostgresRefdbBackend {
    unsafe { &*(backend as *const PostgresRefdbBackend) }
}

/// FNV-1a hash of repo_id + refname, used as advisory lock key.
fn hash_refname(repo_id: i32, refname: &str) -> i64 {
    let mut h: u64 = 14695981039346656037;
    let repo_bytes = repo_id.to_be_bytes();
    for &b in &repo_bytes {
        h ^= b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    for &b in refname.as_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    h as i64
}

/// Build a `git_reference*` from a RefRow.
unsafe fn ref_from_row(
    name: &str,
    oid: Option<&[u8]>,
    symbolic: Option<&str>,
) -> *mut raw::git_reference {
    let c_name = CString::new(name).unwrap();

    if let Some(oid_bytes) = oid {
        let mut git_oid: raw::git_oid = unsafe { std::mem::zeroed() };
        let copy_len = oid_bytes.len().min(OID_SIZE);
        git_oid.id[..copy_len].copy_from_slice(&oid_bytes[..copy_len]);
        unsafe { git_reference__alloc(c_name.as_ptr(), &git_oid, ptr::null()) }
    } else if let Some(target) = symbolic {
        let c_target = CString::new(target).unwrap();
        unsafe { git_reference__alloc_symbolic(c_name.as_ptr(), c_target.as_ptr()) }
    } else {
        ptr::null_mut()
    }
}

// ---- Iterator ----

/// Our custom iterator. Must match the memory layout of `git_reference_iterator`.
#[repr(C)]
struct PostgresRefdbIterator {
    // Fields matching git_reference_iterator layout:
    db: *mut raw::git_refdb,
    next: Option<
        extern "C" fn(*mut *mut raw::git_reference, *mut raw::git_reference_iterator) -> c_int,
    >,
    next_name: Option<
        extern "C" fn(*mut *const c_char, *mut raw::git_reference_iterator) -> c_int,
    >,
    free: Option<extern "C" fn(*mut raw::git_reference_iterator)>,
    // Our extra fields:
    refs: Vec<queries::RefRow>,
    /// Owned CStrings for next_name to return pointers into.
    names: Vec<CString>,
    current: usize,
}

extern "C" fn pg_refdb_iter_next(
    out: *mut *mut raw::git_reference,
    iter: *mut raw::git_reference_iterator,
) -> c_int {
    let iter = unsafe { &mut *(iter as *mut PostgresRefdbIterator) };

    if iter.current >= iter.refs.len() {
        return raw::GIT_ITEROVER;
    }

    let row = &iter.refs[iter.current];
    let ref_ptr =
        unsafe { ref_from_row(&row.name, row.oid.as_deref(), row.symbolic.as_deref()) };
    if ref_ptr.is_null() {
        return -1;
    }

    unsafe { *out = ref_ptr };
    iter.current += 1;
    0
}

extern "C" fn pg_refdb_iter_next_name(
    out: *mut *const c_char,
    iter: *mut raw::git_reference_iterator,
) -> c_int {
    let iter = unsafe { &mut *(iter as *mut PostgresRefdbIterator) };

    if iter.current >= iter.names.len() {
        return raw::GIT_ITEROVER;
    }

    unsafe { *out = iter.names[iter.current].as_ptr() };
    iter.current += 1;
    0
}

extern "C" fn pg_refdb_iter_free(iter: *mut raw::git_reference_iterator) {
    unsafe {
        drop(Box::from_raw(iter as *mut PostgresRefdbIterator));
    }
}

// ---- Callbacks ----

extern "C" fn pg_refdb_exists(
    exists: *mut c_int,
    backend: *mut raw::git_refdb_backend,
    ref_name: *const c_char,
) -> c_int {
    let pg = unsafe { get_backend(backend) };
    let store = pg.store();
    let name = unsafe { CStr::from_ptr(ref_name) }.to_str().unwrap_or("");

    let result = store
        .rt_handle
        .block_on(queries::ref_exists(&store.pool, pg.repo_id, name));

    match result {
        Ok(found) => {
            unsafe { *exists = if found { 1 } else { 0 } };
            0
        }
        Err(_) => -1,
    }
}

extern "C" fn pg_refdb_lookup(
    out: *mut *mut raw::git_reference,
    backend: *mut raw::git_refdb_backend,
    ref_name: *const c_char,
) -> c_int {
    let pg = unsafe { get_backend(backend) };
    let store = pg.store();
    let name = unsafe { CStr::from_ptr(ref_name) }.to_str().unwrap_or("");

    let result = store
        .rt_handle
        .block_on(queries::read_ref(&store.pool, pg.repo_id, name));

    match result {
        Ok(row) => {
            let ref_ptr =
                unsafe { ref_from_row(name, row.oid.as_deref(), row.symbolic.as_deref()) };
            if ref_ptr.is_null() {
                return -1;
            }
            unsafe { *out = ref_ptr };
            0
        }
        Err(crate::error::PgGitError::NotFound(_)) => raw::GIT_ENOTFOUND,
        Err(_) => -1,
    }
}

extern "C" fn pg_refdb_iterator(
    out: *mut *mut raw::git_reference_iterator,
    backend: *mut raw::git_refdb_backend,
    glob: *const c_char,
) -> c_int {
    let pg = unsafe { get_backend(backend) };
    let store = pg.store();

    // Convert glob pattern: replace * with % for SQL LIKE
    let like_pattern = if !glob.is_null() {
        let g = unsafe { CStr::from_ptr(glob) }.to_str().unwrap_or("");
        if g.is_empty() {
            None
        } else {
            Some(g.replace('*', "%"))
        }
    } else {
        None
    };

    let result = store.rt_handle.block_on(queries::list_refs(
        &store.pool,
        pg.repo_id,
        like_pattern.as_deref(),
    ));

    match result {
        Ok(refs) => {
            let names: Vec<CString> = refs
                .iter()
                .map(|r| CString::new(r.name.as_str()).unwrap())
                .collect();

            let iter = Box::new(PostgresRefdbIterator {
                db: ptr::null_mut(),
                next: Some(pg_refdb_iter_next),
                next_name: Some(pg_refdb_iter_next_name),
                free: Some(pg_refdb_iter_free),
                refs,
                names,
                current: 0,
            });

            unsafe { *out = Box::into_raw(iter) as *mut raw::git_reference_iterator };
            0
        }
        Err(_) => -1,
    }
}

extern "C" fn pg_refdb_write(
    backend: *mut raw::git_refdb_backend,
    ref_: *const raw::git_reference,
    force: c_int,
    who: *const raw::git_signature,
    message: *const c_char,
    old: *const raw::git_oid,
    old_target: *const c_char,
) -> c_int {
    let pg = unsafe { get_backend(backend) };
    let store = pg.store();

    let ref_name = unsafe { CStr::from_ptr(raw::git_reference_name(ref_)) }
        .to_str()
        .unwrap_or("");
    let ref_type = unsafe { raw::git_reference_type(ref_) };

    // For simplicity in Phase 2, we do the upsert directly.
    // CAS checking (non-force mode) would require a transaction.
    // The C code uses BEGIN/SELECT FOR UPDATE/UPSERT/COMMIT.
    // We'll implement a simpler version that handles the common cases.

    let result = store.rt_handle.block_on(async {
        if force == 0 {
            // Check CAS constraints
            let existing = queries::read_ref(&store.pool, pg.repo_id, ref_name).await;

            if !old.is_null() || !old_target.is_null() {
                // Ref must exist for update
                let row = match existing {
                    Ok(row) => row,
                    Err(crate::error::PgGitError::NotFound(_)) => {
                        return Err(crate::error::PgGitError::NotFound(
                            format!("reference {} does not exist for update", ref_name),
                        ));
                    }
                    Err(e) => return Err(e),
                };

                if !old.is_null() {
                    // Check current OID matches expected
                    let old_oid = unsafe { &(&(*old).id)[..OID_SIZE] };
                    match &row.oid {
                        Some(cur_oid) if cur_oid.as_slice() == old_oid => {}
                        _ => {
                            return Err(crate::error::PgGitError::Other(
                                format!("reference {} value has changed", ref_name),
                            ));
                        }
                    }
                }

                if !old_target.is_null() {
                    let expected = unsafe { CStr::from_ptr(old_target) }
                        .to_str()
                        .unwrap_or("");
                    match &row.symbolic {
                        Some(cur) if cur == expected => {}
                        _ => {
                            return Err(crate::error::PgGitError::Other(
                                format!("reference {} symbolic target has changed", ref_name),
                            ));
                        }
                    }
                }
            } else {
                // Neither old nor old_target: ref must NOT exist
                if existing.is_ok() {
                    return Err(crate::error::PgGitError::Other(
                        format!("reference {} already exists", ref_name),
                    ));
                }
            }
        }

        // Upsert the ref
        if ref_type == raw::GIT_REFERENCE_DIRECT {
            let oid = unsafe { raw::git_reference_target(ref_) };
            let oid_bytes = unsafe { &(&(*oid).id)[..OID_SIZE] };
            queries::upsert_direct_ref(&store.pool, pg.repo_id, ref_name, oid_bytes).await?;
        } else {
            let target = unsafe { CStr::from_ptr(raw::git_reference_symbolic_target(ref_)) }
                .to_str()
                .unwrap_or("");
            queries::upsert_symbolic_ref(&store.pool, pg.repo_id, ref_name, target).await?;
        }

        // Write reflog entry if signature provided
        if !who.is_null() {
            let sig = unsafe { &*who };
            let sig_name = unsafe { CStr::from_ptr(sig.name) }.to_str().unwrap_or("");
            let sig_email = unsafe { CStr::from_ptr(sig.email) }.to_str().unwrap_or("");
            let committer = format!("{} <{}>", sig_name, sig_email);
            let offset = sig.when.offset;
            let tz = format!(
                "{}{:02}{:02}",
                if offset >= 0 { '+' } else { '-' },
                offset.unsigned_abs() / 60,
                offset.unsigned_abs() % 60,
            );
            let msg = if !message.is_null() {
                Some(unsafe { CStr::from_ptr(message) }.to_str().unwrap_or(""))
            } else {
                None
            };

            let old_oid = if !old.is_null() {
                let bytes = unsafe { &(&(*old).id)[..OID_SIZE] };
                if bytes.iter().all(|&b| b == 0) {
                    None
                } else {
                    Some(bytes as &[u8])
                }
            } else {
                None
            };

            let new_oid = if ref_type == raw::GIT_REFERENCE_DIRECT {
                let oid = unsafe { raw::git_reference_target(ref_) };
                let bytes = unsafe { &(&(*oid).id)[..OID_SIZE] };
                if bytes.iter().all(|&b| b == 0) {
                    None
                } else {
                    Some(bytes as &[u8])
                }
            } else {
                None
            };

            queries::write_reflog_entry(
                &store.pool,
                pg.repo_id,
                ref_name,
                old_oid,
                new_oid,
                &committer,
                sig.when.time,
                &tz,
                msg,
            )
            .await?;
        }

        Ok(())
    });

    match result {
        Ok(()) => 0,
        Err(crate::error::PgGitError::NotFound(_)) => raw::GIT_ENOTFOUND,
        Err(_) => -1,
    }
}

extern "C" fn pg_refdb_rename(
    out: *mut *mut raw::git_reference,
    backend: *mut raw::git_refdb_backend,
    old_name: *const c_char,
    new_name: *const c_char,
    force: c_int,
    _who: *const raw::git_signature,
    _message: *const c_char,
) -> c_int {
    let pg = unsafe { get_backend(backend) };
    let store = pg.store();

    let old = unsafe { CStr::from_ptr(old_name) }.to_str().unwrap_or("");
    let new = unsafe { CStr::from_ptr(new_name) }.to_str().unwrap_or("");

    let result = store.rt_handle.block_on(async {
        if force == 0 {
            if queries::ref_exists(&store.pool, pg.repo_id, new).await? {
                return Err(crate::error::PgGitError::Other(
                    format!("reference {} already exists", new),
                ));
            }
        } else {
            let _ = queries::delete_ref(&store.pool, pg.repo_id, new).await;
        }

        let renamed = queries::rename_ref(&store.pool, pg.repo_id, old, new).await?;
        if !renamed {
            return Err(crate::error::PgGitError::NotFound(
                format!("reference {} not found", old),
            ));
        }

        let _ = queries::rename_reflog(&store.pool, pg.repo_id, old, new).await;

        queries::read_ref(&store.pool, pg.repo_id, new).await
    });

    match result {
        Ok(row) => {
            let ref_ptr =
                unsafe { ref_from_row(new, row.oid.as_deref(), row.symbolic.as_deref()) };
            if ref_ptr.is_null() {
                return -1;
            }
            unsafe { *out = ref_ptr };
            0
        }
        Err(crate::error::PgGitError::NotFound(_)) => raw::GIT_ENOTFOUND,
        Err(_) => -1,
    }
}

extern "C" fn pg_refdb_del(
    backend: *mut raw::git_refdb_backend,
    ref_name: *const c_char,
    _old_id: *const raw::git_oid,
    _old_target: *const c_char,
) -> c_int {
    let pg = unsafe { get_backend(backend) };
    let store = pg.store();
    let name = unsafe { CStr::from_ptr(ref_name) }.to_str().unwrap_or("");

    let result = store.rt_handle.block_on(async {
        queries::delete_ref(&store.pool, pg.repo_id, name).await?;
        queries::delete_reflog(&store.pool, pg.repo_id, name).await?;
        Ok::<(), crate::error::PgGitError>(())
    });

    match result {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

extern "C" fn pg_refdb_has_log(
    backend: *mut raw::git_refdb_backend,
    refname: *const c_char,
) -> c_int {
    let pg = unsafe { get_backend(backend) };
    let store = pg.store();
    let name = unsafe { CStr::from_ptr(refname) }.to_str().unwrap_or("");

    let result = store
        .rt_handle
        .block_on(queries::has_reflog(&store.pool, pg.repo_id, name));

    match result {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(_) => -1,
    }
}

extern "C" fn pg_refdb_ensure_log(
    _backend: *mut raw::git_refdb_backend,
    _refname: *const c_char,
) -> c_int {
    // Reflog entries are written as part of ref updates. Nothing to pre-create.
    0
}

extern "C" fn pg_refdb_reflog_read(
    _out: *mut *mut raw::git_reflog,
    _backend: *mut raw::git_refdb_backend,
    _name: *const c_char,
) -> c_int {
    // Cannot construct a git_reflog through the public API.
    raw::GIT_ENOTFOUND
}

extern "C" fn pg_refdb_reflog_write(
    _backend: *mut raw::git_refdb_backend,
    _reflog: *mut raw::git_reflog,
) -> c_int {
    // Reflog entries are written in pg_refdb_write. This is a no-op.
    0
}

extern "C" fn pg_refdb_reflog_rename(
    backend: *mut raw::git_refdb_backend,
    old_name: *const c_char,
    new_name: *const c_char,
) -> c_int {
    let pg = unsafe { get_backend(backend) };
    let store = pg.store();
    let old = unsafe { CStr::from_ptr(old_name) }.to_str().unwrap_or("");
    let new = unsafe { CStr::from_ptr(new_name) }.to_str().unwrap_or("");

    let result = store
        .rt_handle
        .block_on(queries::rename_reflog(&store.pool, pg.repo_id, old, new));

    match result {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

extern "C" fn pg_refdb_reflog_delete(
    backend: *mut raw::git_refdb_backend,
    name: *const c_char,
) -> c_int {
    let pg = unsafe { get_backend(backend) };
    let store = pg.store();
    let name = unsafe { CStr::from_ptr(name) }.to_str().unwrap_or("");

    let result = store
        .rt_handle
        .block_on(queries::delete_reflog(&store.pool, pg.repo_id, name));

    match result {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

/// Lock payload for advisory locking.
struct PgRefLock {
    lock_key: i64,
    _refname: String,
}

extern "C" fn pg_refdb_lock(
    payload_out: *mut *mut c_void,
    backend: *mut raw::git_refdb_backend,
    refname: *const c_char,
) -> c_int {
    let pg = unsafe { get_backend(backend) };
    let store = pg.store();
    let name = unsafe { CStr::from_ptr(refname) }.to_str().unwrap_or("");

    let lock_key = hash_refname(pg.repo_id, name);

    let result = store
        .rt_handle
        .block_on(queries::advisory_lock(&store.pool, lock_key));

    match result {
        Ok(()) => {
            let lock = Box::new(PgRefLock {
                lock_key,
                _refname: name.to_string(),
            });
            unsafe { *payload_out = Box::into_raw(lock) as *mut c_void };
            0
        }
        Err(_) => -1,
    }
}

extern "C" fn pg_refdb_unlock(
    backend: *mut raw::git_refdb_backend,
    payload: *mut c_void,
    success: c_int,
    update_reflog: c_int,
    ref_: *const raw::git_reference,
    sig: *const raw::git_signature,
    message: *const c_char,
) -> c_int {
    let pg = unsafe { get_backend(backend) };
    let store = pg.store();
    let lock = unsafe { Box::from_raw(payload as *mut PgRefLock) };

    let error = if success == 1 {
        // Write/update the ref
        let ref_name = unsafe { CStr::from_ptr(raw::git_reference_name(ref_)) }
            .to_str()
            .unwrap_or("");
        let ref_type = unsafe { raw::git_reference_type(ref_) };

        store.rt_handle.block_on(async {
            if ref_type == raw::GIT_REFERENCE_DIRECT {
                let oid = unsafe { raw::git_reference_target(ref_) };
                let oid_bytes = unsafe { &(&(*oid).id)[..OID_SIZE] };
                queries::upsert_direct_ref(&store.pool, pg.repo_id, ref_name, oid_bytes)
                    .await?;
            } else {
                let target =
                    unsafe { CStr::from_ptr(raw::git_reference_symbolic_target(ref_)) }
                        .to_str()
                        .unwrap_or("");
                queries::upsert_symbolic_ref(&store.pool, pg.repo_id, ref_name, target)
                    .await?;
            }

            if update_reflog != 0 && !sig.is_null() {
                let s = unsafe { &*sig };
                let s_name = unsafe { CStr::from_ptr(s.name) }.to_str().unwrap_or("");
                let s_email = unsafe { CStr::from_ptr(s.email) }.to_str().unwrap_or("");
                let committer = format!("{} <{}>", s_name, s_email);
                let offset = s.when.offset;
                let tz = format!(
                    "{}{:02}{:02}",
                    if offset >= 0 { '+' } else { '-' },
                    offset.unsigned_abs() / 60,
                    offset.unsigned_abs() % 60,
                );
                let msg = if !message.is_null() {
                    Some(unsafe { CStr::from_ptr(message) }.to_str().unwrap_or(""))
                } else {
                    None
                };

                let new_oid = if ref_type == raw::GIT_REFERENCE_DIRECT {
                    let oid = unsafe { raw::git_reference_target(ref_) };
                    Some(unsafe { &(&(*oid).id)[..OID_SIZE] } as &[u8])
                } else {
                    None
                };

                queries::write_reflog_entry(
                    &store.pool,
                    pg.repo_id,
                    ref_name,
                    None,
                    new_oid,
                    &committer,
                    s.when.time,
                    &tz,
                    msg,
                )
                .await?;
            }

            Ok::<(), crate::error::PgGitError>(())
        })
    } else if success == 2 {
        // Delete the ref
        let ref_name = unsafe { CStr::from_ptr(raw::git_reference_name(ref_)) }
            .to_str()
            .unwrap_or("");
        store.rt_handle.block_on(async {
            queries::delete_ref(&store.pool, pg.repo_id, ref_name).await?;
            queries::delete_reflog(&store.pool, pg.repo_id, ref_name).await?;
            Ok::<(), crate::error::PgGitError>(())
        })
    } else {
        // success == 0: discard
        Ok(())
    };

    // Release the advisory lock
    let _ = store
        .rt_handle
        .block_on(queries::advisory_unlock(&store.pool, lock.lock_key));

    match error {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

extern "C" fn pg_refdb_free(backend: *mut raw::git_refdb_backend) {
    unsafe {
        let pg = backend as *mut PostgresRefdbBackend;
        let arc_ptr = (*pg)._prevent_drop as *mut Arc<PgGitStore>;
        if !arc_ptr.is_null() {
            drop(Box::from_raw(arc_ptr));
        }
        drop(Box::from_raw(pg));
    }
}
