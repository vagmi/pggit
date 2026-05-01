use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::Response;
use serde::Deserialize;

use super::cgi::{self, CgiCall};
use super::error::HttpError;
use super::state::HttpState;
use super::workdir::{self, Workdir};

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

    tracing::info!(repo = %repo, %service, "smart-http: info/refs");

    let repo_id = resolve_repo(&state, &repo).await?;
    let workdir = Workdir::prepare(&state, repo_id).await?;

    let path_info = format!("/{}/info/refs", workdir::REPO_DIR_NAME);
    let qs = format!("service={service}");
    let call = CgiCall {
        git_binary: &state.opts.git_binary,
        git_dir: workdir.git_dir(),
        path_info: &path_info,
        query_string: &qs,
        method: "GET",
        content_type: None,
        remote_addr: None,
        content_length: None,
    };

    cgi::run_buffered(call, Body::empty()).await
}

#[tracing::instrument(skip(state, headers, body), fields(repo = %repo))]
pub(crate) async fn upload_pack(
    State(state): State<HttpState>,
    Path(repo): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, HttpError> {
    tracing::info!(repo = %repo, "smart-http: upload-pack");

    let repo_id = resolve_repo(&state, &repo).await?;
    let workdir = Workdir::prepare(&state, repo_id).await?;

    let path_info = format!("/{}/git-upload-pack", workdir::REPO_DIR_NAME);
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let content_length = headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    // Keep the workdir alive until the streaming body finishes — otherwise
    // the tempdir gets reaped while git http-backend is still using it.
    let keep_alive: cgi::KeepAlive = Box::new(workdir);
    // Borrow git_dir back out of the boxed workdir.
    let workdir_ref: &Workdir = keep_alive.downcast_ref().expect("just boxed Workdir");
    let git_dir = workdir_ref.git_dir().to_path_buf();

    let call = CgiCall {
        git_binary: &state.opts.git_binary,
        git_dir: &git_dir,
        path_info: &path_info,
        query_string: "",
        method: "POST",
        content_type: content_type.as_deref(),
        remote_addr: None,
        content_length,
    };

    cgi::run_streaming(call, body, Some(keep_alive)).await
}

#[tracing::instrument(skip(state, headers, body), fields(repo = %repo))]
pub(crate) async fn receive_pack(
    State(state): State<HttpState>,
    Path(repo): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, HttpError> {
    tracing::info!(repo = %repo, "smart-http: receive-pack");

    if !state.opts.allow_push {
        return Err(HttpError::PushDisabled);
    }
    let repo_id = resolve_repo(&state, &repo).await?;

    // Serialize all pushes within a process. (k8s single-replica assumption;
    // multi-replica setups need PG advisory locks instead.)
    let _push_guard = state.push_lock.lock().await;

    let workdir = Workdir::prepare(&state, repo_id).await?;
    let before = workdir.snapshot().await?;
    tracing::debug!(repo = %repo, "snapshot taken before receive-pack");

    let path_info = format!("/{}/git-receive-pack", workdir::REPO_DIR_NAME);
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let content_length = headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    let call = CgiCall {
        git_binary: &state.opts.git_binary,
        git_dir: workdir.git_dir(),
        path_info: &path_info,
        query_string: "",
        method: "POST",
        content_type: content_type.as_deref(),
        remote_addr: None,
        content_length,
    };

    let resp = cgi::run_buffered(call, body).await?;
    tracing::debug!(repo = %repo, "receive-pack CGI complete; reimporting");

    workdir.apply_changes(&state, repo_id, before).await?;
    tracing::info!(repo = %repo, "receive-pack: reimport done");

    Ok(resp)
}

async fn resolve_repo(state: &HttpState, name: &str) -> Result<i32, HttpError> {
    match state.store.lookup_repository(name).await? {
        Some(id) => Ok(id),
        None => Err(HttpError::RepoNotFound(name.to_string())),
    }
}
