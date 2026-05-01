use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::Response;
use serde::Deserialize;

use super::error::HttpError;
use super::state::HttpState;

#[derive(Debug, Deserialize)]
pub(crate) struct InfoRefsQuery {
    pub service: Option<String>,
}

#[tracing::instrument(skip(state, _headers), fields(repo = %repo, service = ?q.service))]
pub(crate) async fn info_refs(
    State(state): State<HttpState>,
    Path(repo): Path<String>,
    Query(q): Query<InfoRefsQuery>,
    _headers: HeaderMap,
) -> Result<Response, HttpError> {
    let service = match q.service.as_deref() {
        Some(s @ ("git-upload-pack" | "git-receive-pack")) => s.to_string(),
        Some(other) => return Err(HttpError::BadRequest(format!("unknown service {other}"))),
        None => return Err(HttpError::BadRequest("missing ?service=".into())),
    };

    if service == "git-receive-pack" && !state.opts.allow_push {
        return Err(HttpError::PushDisabled);
    }

    let _repo_id = resolve_repo(&state, &repo).await?;

    // TODO: workdir checkout + CGI dispatch.
    Err(HttpError::Internal(format!(
        "info_refs not yet implemented (service={service})"
    )))
}

#[tracing::instrument(skip(state, _headers, _body), fields(repo = %repo))]
pub(crate) async fn upload_pack(
    State(state): State<HttpState>,
    Path(repo): Path<String>,
    _headers: HeaderMap,
    _body: Body,
) -> Result<Response, HttpError> {
    let _repo_id = resolve_repo(&state, &repo).await?;
    Err(HttpError::Internal("upload_pack not yet implemented".into()))
}

#[tracing::instrument(skip(state, _headers, _body), fields(repo = %repo))]
pub(crate) async fn receive_pack(
    State(state): State<HttpState>,
    Path(repo): Path<String>,
    _headers: HeaderMap,
    _body: Body,
) -> Result<Response, HttpError> {
    if !state.opts.allow_push {
        return Err(HttpError::PushDisabled);
    }
    let _repo_id = resolve_repo(&state, &repo).await?;
    Err(HttpError::Internal("receive_pack not yet implemented".into()))
}

async fn resolve_repo(state: &HttpState, name: &str) -> Result<i32, HttpError> {
    match state.store.lookup_repository(name).await? {
        Some(id) => Ok(id),
        None => Err(HttpError::RepoNotFound(name.to_string())),
    }
}
