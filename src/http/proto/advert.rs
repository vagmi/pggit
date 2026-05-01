//! Smart-HTTP `info/refs` advertisement.
//!
//! Emits a pkt-line stream of the form:
//!
//! ```text
//! # service=git-{upload,receive}-pack\n     (pkt-line)
//! 0000                                       (flush)
//! <oid> <ref>\0<capabilities>\n              (first ref)
//! <oid> <ref>\n                              (subsequent refs)
//! 0000                                       (flush)
//! ```
//!
//! When the repo has no refs, a sentinel line of the form
//! `0000000000000000000000000000000000000000 capabilities^{}\0<caps>\n`
//! is emitted instead so the client still sees the capability list.

use std::io::Write;
use std::sync::Arc;

use crate::error::PgGitError;
use crate::http::error::HttpError;
use crate::store::PgGitStore;

use super::Service;

/// Capability lines for each service. Conservative MVP set — we don't yet
/// support shallow, partial-clone filters, or protocol v2.
const UPLOAD_PACK_CAPS: &str =
    "multi_ack_detailed multi_ack side-band-64k thin-pack ofs-delta no-progress include-tag agent=pggit/0.1";
const RECEIVE_PACK_CAPS: &str =
    "report-status delete-refs ofs-delta atomic agent=pggit/0.1";
const ZERO_OID: &str = "0000000000000000000000000000000000000000";

/// Generate the advertisement bytes for `service`, reading refs from PG.
///
/// Runs git2 calls on the calling thread; intended to be invoked from
/// inside `tokio::task::spawn_blocking`.
pub(crate) fn build(
    store: &Arc<PgGitStore>,
    repo_id: i32,
    service: Service,
) -> Result<Vec<u8>, HttpError> {
    let mut out = Vec::with_capacity(4096);

    // # service=...\n line, length-prefixed.
    write_pktline(&mut out, format!("# service={}\n", service.name()).as_bytes())?;
    // Flush after the service announcement.
    out.extend_from_slice(b"0000");

    let pg = store.open_repository(repo_id).map_err(HttpError::PgGit)?;
    let caps = match service {
        Service::UploadPack => UPLOAD_PACK_CAPS,
        Service::ReceivePack => RECEIVE_PACK_CAPS,
    };

    let refs = collect_refs(&pg)?;
    if refs.is_empty() {
        // No refs to advertise — emit the capabilities^{} sentinel.
        let line = format!("{ZERO_OID} capabilities^{{}}\0{caps}\n");
        write_pktline(&mut out, line.as_bytes())?;
    } else {
        // First ref carries the capability list (NUL-separated). Subsequent
        // refs are plain `<oid> <name>\n`.
        let mut first = true;
        for (name, oid) in &refs {
            let line = if first {
                first = false;
                format!("{oid} {name}\0{caps}\n")
            } else {
                format!("{oid} {name}\n")
            };
            write_pktline(&mut out, line.as_bytes())?;
        }
    }

    // Final flush.
    out.extend_from_slice(b"0000");
    Ok(out)
}

/// Read all `refs/*` entries (peeled to direct OIDs) plus, if HEAD is
/// symbolic and points at a known ref, prepend HEAD as a synthetic entry so
/// the client knows the default branch.
fn collect_refs(pg: &git2::Repository) -> Result<Vec<(String, String)>, HttpError> {
    let mut out: Vec<(String, String)> = Vec::new();

    // Resolve HEAD first so we can advertise it.
    if let Ok(head) = pg.find_reference("HEAD") {
        if let Ok(resolved) = head.resolve() {
            if let Some(oid) = resolved.target() {
                out.push(("HEAD".to_string(), oid.to_string()));
            }
        }
    }

    let iter = pg.references().map_err(PgGitError::from)?;
    for r in iter {
        let r = r.map_err(PgGitError::from)?;
        let Some(name) = r.name() else { continue };
        if !name.starts_with("refs/") {
            continue;
        }
        let resolved = r.resolve().map_err(PgGitError::from)?;
        let Some(oid) = resolved.target() else {
            continue;
        };
        out.push((name.to_string(), oid.to_string()));
    }

    // git http-backend orders refs alphabetically except HEAD goes first.
    let head = if !out.is_empty() && out[0].0 == "HEAD" {
        Some(out.remove(0))
    } else {
        None
    };
    out.sort_by(|a, b| a.0.cmp(&b.0));
    if let Some(head) = head {
        out.insert(0, head);
    }

    // Convert to (name, oid_hex) — already done above. Reshape to the right
    // tuple order: ref output expects (name, oid).
    Ok(out
        .into_iter()
        .map(|(name, oid)| (name, oid))
        .collect())
}

/// Length-prefix `payload` and append it to `out`. Length includes the 4-byte
/// length prefix itself.
fn write_pktline(out: &mut Vec<u8>, payload: &[u8]) -> Result<(), HttpError> {
    let total = payload.len() + 4;
    if total > 65520 {
        return Err(HttpError::Internal(format!("pktline too long: {total}")));
    }
    write!(out, "{:04x}", total).map_err(|e| HttpError::Internal(e.to_string()))?;
    out.extend_from_slice(payload);
    Ok(())
}

/// Content-type returned with the info/refs response.
pub(crate) fn content_type(service: Service) -> &'static str {
    match service {
        Service::UploadPack => "application/x-git-upload-pack-advertisement",
        Service::ReceivePack => "application/x-git-receive-pack-advertisement",
    }
}
