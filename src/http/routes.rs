use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::Response;
use serde::Deserialize;

use std::sync::Arc;

use super::error::HttpError;
use super::proto::{self, Service};
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
        Some("git-upload-pack") => Service::UploadPack,
        Some("git-receive-pack") => Service::ReceivePack,
        Some(other) => return Err(HttpError::BadRequest(format!("unknown service {other}"))),
        None => return Err(HttpError::BadRequest("missing ?service=".into())),
    };

    if service == Service::ReceivePack && !state.opts.allow_push {
        return Err(HttpError::PushDisabled);
    }

    tracing::info!(repo = %repo, service = service.name(), "smart-http: info/refs");

    let repo_id = resolve_repo(&state, &repo).await?;

    let store = Arc::clone(&state.store);
    let bytes = tokio::task::spawn_blocking(move || proto::advert::build(&store, repo_id, service))
        .await
        .map_err(|e| HttpError::Internal(format!("advert join: {e}")))??;

    let resp = Response::builder()
        .status(axum::http::StatusCode::OK)
        .header(
            axum::http::header::CONTENT_TYPE,
            proto::advert::content_type(service),
        )
        .header(axum::http::header::CACHE_CONTROL, "no-cache")
        .body(Body::from(bytes))
        .map_err(|e| HttpError::Internal(format!("response build: {e}")))?;
    Ok(resp)
}

/// Upper bound on the in-memory request body buffer for upload/receive
/// pack POSTs. 256 MiB is generous for the coding-agent use case and
/// refuses absurd payloads.
const MAX_REQUEST_BODY: usize = 256 * 1024 * 1024;

#[tracing::instrument(skip(state, _headers, body), fields(repo = %repo))]
pub(crate) async fn upload_pack(
    State(state): State<HttpState>,
    Path(repo): Path<String>,
    _headers: HeaderMap,
    body: Body,
) -> Result<Response, HttpError> {
    tracing::info!(repo = %repo, "smart-http: upload-pack");

    let repo_id = resolve_repo(&state, &repo).await?;

    let body_bytes = axum::body::to_bytes(body, MAX_REQUEST_BODY)
        .await
        .map_err(|e| HttpError::BadRequest(format!("read body: {e}")))?;

    let store = Arc::clone(&state.store);
    let (sync_writer, async_reader) = tokio::io::duplex(64 * 1024);
    let writer_bridge = tokio_util::io::SyncIoBridge::new(sync_writer);

    tokio::task::spawn_blocking(move || {
        let request = std::io::Cursor::new(body_bytes);
        if let Err(e) = proto::upload_pack::run(store, repo_id, request, writer_bridge) {
            tracing::error!(error = %e, "upload-pack: protocol failure");
        }
    });

    let body = Body::from_stream(tokio_util::io::ReaderStream::new(async_reader));
    Response::builder()
        .status(axum::http::StatusCode::OK)
        .header(
            axum::http::header::CONTENT_TYPE,
            "application/x-git-upload-pack-result",
        )
        .header(axum::http::header::CACHE_CONTROL, "no-cache")
        .body(body)
        .map_err(|e| HttpError::Internal(format!("response build: {e}")))
}

#[tracing::instrument(skip(state, _headers, body), fields(repo = %repo))]
pub(crate) async fn receive_pack(
    State(state): State<HttpState>,
    Path(repo): Path<String>,
    _headers: HeaderMap,
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

    let body_bytes = axum::body::to_bytes(body, MAX_REQUEST_BODY)
        .await
        .map_err(|e| HttpError::BadRequest(format!("read body: {e}")))?;

    let store = Arc::clone(&state.store);
    let report_bytes: Vec<u8> = tokio::task::spawn_blocking(move || -> Result<Vec<u8>, HttpError> {
        let mut response = Vec::with_capacity(256);
        proto::receive_pack::run(
            store,
            repo_id,
            std::io::Cursor::new(body_bytes),
            &mut response,
        )?;
        Ok(response)
    })
    .await
    .map_err(|e| HttpError::Internal(format!("receive-pack join: {e}")))??;

    tracing::info!(repo = %repo, "receive-pack: complete");

    Response::builder()
        .status(axum::http::StatusCode::OK)
        .header(
            axum::http::header::CONTENT_TYPE,
            "application/x-git-receive-pack-result",
        )
        .header(axum::http::header::CACHE_CONTROL, "no-cache")
        .body(Body::from(report_bytes))
        .map_err(|e| HttpError::Internal(format!("response build: {e}")))
}

async fn resolve_repo(state: &HttpState, name: &str) -> Result<i32, HttpError> {
    match state.store.lookup_repository(name).await? {
        Some(id) => Ok(id),
        None => Err(HttpError::RepoNotFound(name.to_string())),
    }
}
