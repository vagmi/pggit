use sqlx::PgPool;

use crate::error::{PgGitError, Result};

/// Row returned from object reads.
pub struct ObjectRow {
    pub oid: Vec<u8>,
    pub object_type: i16,
    pub size: i32,
    pub content: Vec<u8>,
}

/// Write a git object. Uses ON CONFLICT DO NOTHING for idempotency.
pub async fn write_object(
    pool: &PgPool,
    repo_id: i32,
    oid: &[u8],
    object_type: i16,
    size: i32,
    content: &[u8],
) -> Result<()> {
    sqlx::query(
        "INSERT INTO objects (repo_id, oid, type, size, content) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (repo_id, oid) DO NOTHING",
    )
    .bind(repo_id)
    .bind(oid)
    .bind(object_type)
    .bind(size)
    .bind(content)
    .execute(pool)
    .await?;
    Ok(())
}

/// Read an object by exact OID.
pub async fn read_object(pool: &PgPool, repo_id: i32, oid: &[u8]) -> Result<ObjectRow> {
    let row: Option<(i16, i32, Vec<u8>)> = sqlx::query_as(
        "SELECT type, size, content FROM objects WHERE repo_id=$1 AND oid=$2",
    )
    .bind(repo_id)
    .bind(oid)
    .fetch_optional(pool)
    .await?;

    match row {
        Some((object_type, size, content)) => Ok(ObjectRow {
            oid: oid.to_vec(),
            object_type,
            size,
            content,
        }),
        None => Err(PgGitError::NotFound(format!(
            "object {} not found",
            hex::encode(oid)
        ))),
    }
}

/// Read an object header (type + size) without fetching content.
pub async fn read_object_header(
    pool: &PgPool,
    repo_id: i32,
    oid: &[u8],
) -> Result<(i16, i32)> {
    let row: Option<(i16, i32)> =
        sqlx::query_as("SELECT type, size FROM objects WHERE repo_id=$1 AND oid=$2")
            .bind(repo_id)
            .bind(oid)
            .fetch_optional(pool)
            .await?;

    row.ok_or_else(|| PgGitError::NotFound(format!("object {} not found", hex::encode(oid))))
}

/// Check if an object exists by exact OID.
pub async fn object_exists(pool: &PgPool, repo_id: i32, oid: &[u8]) -> Result<bool> {
    let row: Option<(i32,)> =
        sqlx::query_as("SELECT 1 FROM objects WHERE repo_id=$1 AND oid=$2")
            .bind(repo_id)
            .bind(oid)
            .fetch_optional(pool)
            .await?;
    Ok(row.is_some())
}

/// Read an object by OID prefix. Returns error if ambiguous.
pub async fn read_object_prefix(
    pool: &PgPool,
    repo_id: i32,
    prefix: &[u8],
    prefix_byte_len: i32,
) -> Result<ObjectRow> {
    let rows: Vec<(Vec<u8>, i16, i32, Vec<u8>)> = sqlx::query_as(
        "SELECT oid, type, size, content FROM objects \
         WHERE repo_id=$1 AND substring(oid from 1 for $2) = $3",
    )
    .bind(repo_id)
    .bind(prefix_byte_len)
    .bind(&prefix[..prefix_byte_len as usize])
    .fetch_all(pool)
    .await?;

    match rows.len() {
        0 => Err(PgGitError::NotFound("object not found by prefix".into())),
        1 => {
            let (oid, object_type, size, content) = rows.into_iter().next().unwrap();
            Ok(ObjectRow {
                oid,
                object_type,
                size,
                content,
            })
        }
        _ => Err(PgGitError::Ambiguous("multiple objects match prefix".into())),
    }
}

/// Check if an object exists by OID prefix. Returns the full OID if unique.
pub async fn object_exists_prefix(
    pool: &PgPool,
    repo_id: i32,
    prefix: &[u8],
    prefix_byte_len: i32,
) -> Result<Vec<u8>> {
    let rows: Vec<(Vec<u8>,)> = sqlx::query_as(
        "SELECT oid FROM objects \
         WHERE repo_id=$1 AND substring(oid from 1 for $2) = $3",
    )
    .bind(repo_id)
    .bind(prefix_byte_len)
    .bind(&prefix[..prefix_byte_len as usize])
    .fetch_all(pool)
    .await?;

    match rows.len() {
        0 => Err(PgGitError::NotFound("object not found by prefix".into())),
        1 => Ok(rows.into_iter().next().unwrap().0),
        _ => Err(PgGitError::Ambiguous("multiple objects match prefix".into())),
    }
}

/// Get all OIDs in a repository (for foreach).
pub async fn all_oids(pool: &PgPool, repo_id: i32) -> Result<Vec<Vec<u8>>> {
    let rows: Vec<(Vec<u8>,)> =
        sqlx::query_as("SELECT oid FROM objects WHERE repo_id=$1")
            .bind(repo_id)
            .fetch_all(pool)
            .await?;
    Ok(rows.into_iter().map(|(oid,)| oid).collect())
}

/// Create a repository and return its id.
pub async fn create_repository(pool: &PgPool, name: &str) -> Result<i32> {
    let row: (i32,) = sqlx::query_as(
        "INSERT INTO repositories (name) VALUES ($1) RETURNING id",
    )
    .bind(name)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

/// Get a repository id by name.
pub async fn get_repository_id(pool: &PgPool, name: &str) -> Result<i32> {
    let row: Option<(i32,)> =
        sqlx::query_as("SELECT id FROM repositories WHERE name=$1")
            .bind(name)
            .fetch_optional(pool)
            .await?;
    row.map(|(id,)| id)
        .ok_or_else(|| PgGitError::NotFound(format!("repository '{}' not found", name)))
}

// ---- Ref queries ----

/// A row from the refs table.
pub struct RefRow {
    pub name: String,
    pub oid: Option<Vec<u8>>,
    pub symbolic: Option<String>,
}

/// Check if a ref exists.
pub async fn ref_exists(pool: &PgPool, repo_id: i32, name: &str) -> Result<bool> {
    let row: Option<(i32,)> =
        sqlx::query_as("SELECT 1 FROM refs WHERE repo_id=$1 AND name=$2")
            .bind(repo_id)
            .bind(name)
            .fetch_optional(pool)
            .await?;
    Ok(row.is_some())
}

/// Lookup a ref by name.
pub async fn read_ref(pool: &PgPool, repo_id: i32, name: &str) -> Result<RefRow> {
    let row: Option<(String, Option<Vec<u8>>, Option<String>)> = sqlx::query_as(
        "SELECT name, oid, symbolic FROM refs WHERE repo_id=$1 AND name=$2",
    )
    .bind(repo_id)
    .bind(name)
    .fetch_optional(pool)
    .await?;

    match row {
        Some((name, oid, symbolic)) => Ok(RefRow {
            name,
            oid,
            symbolic,
        }),
        None => Err(PgGitError::NotFound(format!("ref '{}' not found", name))),
    }
}

/// List refs, optionally filtered by a SQL LIKE pattern.
pub async fn list_refs(
    pool: &PgPool,
    repo_id: i32,
    like_pattern: Option<&str>,
) -> Result<Vec<RefRow>> {
    let rows: Vec<(String, Option<Vec<u8>>, Option<String>)> = match like_pattern {
        Some(pattern) => {
            sqlx::query_as(
                "SELECT name, oid, symbolic FROM refs \
                 WHERE repo_id=$1 AND name LIKE $2 ORDER BY name",
            )
            .bind(repo_id)
            .bind(pattern)
            .fetch_all(pool)
            .await?
        }
        None => {
            sqlx::query_as(
                "SELECT name, oid, symbolic FROM refs \
                 WHERE repo_id=$1 ORDER BY name",
            )
            .bind(repo_id)
            .fetch_all(pool)
            .await?
        }
    };

    Ok(rows
        .into_iter()
        .map(|(name, oid, symbolic)| RefRow {
            name,
            oid,
            symbolic,
        })
        .collect())
}

/// Upsert a direct ref (oid-based).
pub async fn upsert_direct_ref(
    pool: &PgPool,
    repo_id: i32,
    name: &str,
    oid: &[u8],
) -> Result<()> {
    sqlx::query(
        "INSERT INTO refs (repo_id, name, oid, symbolic) \
         VALUES ($1, $2, $3, NULL) \
         ON CONFLICT (repo_id, name) DO UPDATE \
         SET oid = EXCLUDED.oid, symbolic = NULL",
    )
    .bind(repo_id)
    .bind(name)
    .bind(oid)
    .execute(pool)
    .await?;
    Ok(())
}

/// Upsert a symbolic ref.
pub async fn upsert_symbolic_ref(
    pool: &PgPool,
    repo_id: i32,
    name: &str,
    target: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO refs (repo_id, name, oid, symbolic) \
         VALUES ($1, $2, NULL, $3) \
         ON CONFLICT (repo_id, name) DO UPDATE \
         SET oid = NULL, symbolic = EXCLUDED.symbolic",
    )
    .bind(repo_id)
    .bind(name)
    .bind(target)
    .execute(pool)
    .await?;
    Ok(())
}

/// Delete a ref.
pub async fn delete_ref(pool: &PgPool, repo_id: i32, name: &str) -> Result<()> {
    sqlx::query("DELETE FROM refs WHERE repo_id=$1 AND name=$2")
        .bind(repo_id)
        .bind(name)
        .execute(pool)
        .await?;
    Ok(())
}

/// Rename a ref. Returns true if a row was actually updated.
pub async fn rename_ref(
    pool: &PgPool,
    repo_id: i32,
    old_name: &str,
    new_name: &str,
) -> Result<bool> {
    let result =
        sqlx::query("UPDATE refs SET name=$1 WHERE repo_id=$2 AND name=$3")
            .bind(new_name)
            .bind(repo_id)
            .bind(old_name)
            .execute(pool)
            .await?;
    Ok(result.rows_affected() > 0)
}

// ---- Reflog queries ----

/// Write a reflog entry.
pub async fn write_reflog_entry(
    pool: &PgPool,
    repo_id: i32,
    ref_name: &str,
    old_oid: Option<&[u8]>,
    new_oid: Option<&[u8]>,
    committer: &str,
    timestamp_s: i64,
    tz_offset: &str,
    message: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO reflog (repo_id, ref_name, old_oid, new_oid, \
         committer, timestamp_s, tz_offset, message) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(repo_id)
    .bind(ref_name)
    .bind(old_oid)
    .bind(new_oid)
    .bind(committer)
    .bind(timestamp_s)
    .bind(tz_offset)
    .bind(message)
    .execute(pool)
    .await?;
    Ok(())
}

/// Check if any reflog entries exist for a ref.
pub async fn has_reflog(pool: &PgPool, repo_id: i32, ref_name: &str) -> Result<bool> {
    let row: Option<(i32,)> = sqlx::query_as(
        "SELECT 1 FROM reflog WHERE repo_id=$1 AND ref_name=$2 LIMIT 1",
    )
    .bind(repo_id)
    .bind(ref_name)
    .fetch_optional(pool)
    .await?;
    Ok(row.is_some())
}

/// Rename reflog entries.
pub async fn rename_reflog(
    pool: &PgPool,
    repo_id: i32,
    old_name: &str,
    new_name: &str,
) -> Result<()> {
    sqlx::query(
        "UPDATE reflog SET ref_name=$1 WHERE repo_id=$2 AND ref_name=$3",
    )
    .bind(new_name)
    .bind(repo_id)
    .bind(old_name)
    .execute(pool)
    .await?;
    Ok(())
}

/// Delete reflog entries for a ref.
pub async fn delete_reflog(pool: &PgPool, repo_id: i32, ref_name: &str) -> Result<()> {
    sqlx::query("DELETE FROM reflog WHERE repo_id=$1 AND ref_name=$2")
        .bind(repo_id)
        .bind(ref_name)
        .execute(pool)
        .await?;
    Ok(())
}

/// Acquire a PostgreSQL advisory lock scoped to a ref name.
pub async fn advisory_lock(pool: &PgPool, lock_key: i64) -> Result<()> {
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(lock_key)
        .execute(pool)
        .await?;
    Ok(())
}

/// Release a PostgreSQL advisory lock.
pub async fn advisory_unlock(pool: &PgPool, lock_key: i64) -> Result<()> {
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(lock_key)
        .execute(pool)
        .await?;
    Ok(())
}

// We use hex crate for nice error messages. Since it's a dev convenience
// and sqlx already depends on it transitively, this is fine.
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }
}
