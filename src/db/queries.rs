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

// We use hex crate for nice error messages. Since it's a dev convenience
// and sqlx already depends on it transitively, this is fine.
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }
}
