use thiserror::Error;

#[derive(Error, Debug)]
pub enum PgGitError {
    #[error("git error: {0}")]
    Git(#[from] git2::Error),

    #[error("database error: {0}")]
    Sql(#[from] sqlx::Error),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("ambiguous: {0}")]
    Ambiguous(String),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, PgGitError>;
