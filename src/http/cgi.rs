//! CGI driver for `git http-backend`.
//!
//! Spawns the binary with a smart-HTTP-shaped environment, streams the request
//! body to its stdin, parses the CGI status/header block from its stdout, and
//! returns the remaining stdout as a streaming axum response body.
//!
//! Filled in next pass.
