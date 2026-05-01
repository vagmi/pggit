use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::error::PgGitError;

/// Errors surfaced to HTTP responses.
#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error("repository not found: {0}")]
    RepoNotFound(String),

    #[error("push is disabled")]
    PushDisabled,

    #[error("invalid request: {0}")]
    BadRequest(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error(transparent)]
    PgGit(#[from] PgGitError),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl From<git2::Error> for HttpError {
    fn from(e: git2::Error) -> Self {
        HttpError::PgGit(e.into())
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let (status, msg) = match &self {
            HttpError::RepoNotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            HttpError::PushDisabled => (StatusCode::FORBIDDEN, self.to_string()),
            HttpError::BadRequest(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            HttpError::Internal(_) | HttpError::PgGit(_) | HttpError::Io(_) => {
                tracing::error!(error = %self, "smart-http internal error");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string())
            }
        };
        (status, msg).into_response()
    }
}
