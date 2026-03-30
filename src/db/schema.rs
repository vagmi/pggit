use sqlx::PgPool;

use crate::error::Result;

const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS repositories (
    id          serial PRIMARY KEY,
    name        text NOT NULL UNIQUE,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS objects (
    repo_id     integer NOT NULL REFERENCES repositories(id),
    oid         bytea NOT NULL,
    type        smallint NOT NULL,
    size        integer NOT NULL,
    content     bytea NOT NULL,
    PRIMARY KEY (repo_id, oid)
);
CREATE INDEX IF NOT EXISTS idx_objects_oid ON objects (oid);

CREATE TABLE IF NOT EXISTS refs (
    repo_id     integer NOT NULL REFERENCES repositories(id),
    name        text NOT NULL,
    oid         bytea,
    symbolic    text,
    PRIMARY KEY (repo_id, name),
    CHECK ((oid IS NOT NULL) != (symbolic IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS reflog (
    id          bigserial PRIMARY KEY,
    repo_id     integer NOT NULL REFERENCES repositories(id),
    ref_name    text NOT NULL,
    old_oid     bytea,
    new_oid     bytea,
    committer   text NOT NULL,
    timestamp_s bigint NOT NULL,
    tz_offset   text NOT NULL,
    message     text,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_reflog_ref ON reflog (repo_id, ref_name, id);
"#;

pub async fn migrate(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql(SCHEMA_SQL).execute(pool).await?;
    Ok(())
}
