//! Smart HTTP server for pggit repositories.
//!
//! Returns an [`axum::Router`] that speaks the git smart HTTP protocol
//! (`info/refs`, `git-upload-pack`, `git-receive-pack`). Internally it
//! materializes each request to a temp checkout and shells out to
//! `git http-backend` as CGI.
//!
//! No auth is included. The router assumes that any request that reaches it
//! is authorized; compose authentication as a [`tower::Layer`] in your app.

mod cgi;
mod error;
mod routes;
mod state;
mod workdir;

pub use error::HttpError;
pub use state::{HttpOptions, HttpState};

use axum::{Router, routing};

/// Build the smart-HTTP router. Nest it under whatever prefix you like.
///
/// ```ignore
/// let state = pggit::http::HttpState::new(store);
/// let app = axum::Router::new()
///     .nest("/git", pggit::http::router(state).layer(my_auth_layer));
/// ```
pub fn router(state: HttpState) -> Router {
    Router::new()
        .route("/{repo}/info/refs", routing::get(routes::info_refs))
        .route("/{repo}/git-upload-pack", routing::post(routes::upload_pack))
        .route("/{repo}/git-receive-pack", routing::post(routes::receive_pack))
        .with_state(state)
}
