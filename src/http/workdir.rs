//! Per-request temp checkout + post-push reimport helpers.
//!
//! - `prepare(state, repo_id)` -> `TempDir` containing a fresh checkout of
//!   the repo (via existing `porcelain::checkout`).
//! - `reimport(state, repo_id, tempdir, snapshot)` -> after `git receive-pack`
//!   runs, walks new loose objects + new pack files and applies them through
//!   the existing PG ODB queries, then diffs `.git/refs` to apply ref updates
//!   via the existing PG RefDB queries. Wrapped in the existing PG advisory
//!   lock for the repo.
//!
//! Filled in next pass.
