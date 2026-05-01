//! Minimal smart-HTTP server with bearer-token auth as a Tower layer.
//!
//! Run:
//!   PGGIT_DATABASE_URL=postgres://... \
//!   PGGIT_BEARER_TOKEN=hunter2 \
//!     cargo run --example smart_http_server --features smart-http
//!
//! Then:
//!   git -c http.extraHeader='Authorization: Bearer hunter2' \
//!       clone http://127.0.0.1:8080/git/<repo-name>

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::Response;
use pggit::PgGitStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,pggit=debug".into()),
        )
        .init();

    let db_url = std::env::var("PGGIT_DATABASE_URL")?;
    let token = std::env::var("PGGIT_BEARER_TOKEN")?;

    let store: Arc<PgGitStore> = PgGitStore::connect(&db_url).await?;
    store.migrate().await?;

    let git_router = pggit::http::router(pggit::http::HttpState::new(store));

    let app: Router = Router::new()
        .nest("/git", git_router)
        .layer(middleware::from_fn_with_state(token, require_bearer));

    let addr: SocketAddr = "0.0.0.0:8080".parse()?;
    tracing::info!(%addr, "smart-http server listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn require_bearer(
    axum::extract::State(expected): axum::extract::State<String>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let presented = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match presented {
        Some(t) if t == expected => Ok(next.run(req).await),
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}
