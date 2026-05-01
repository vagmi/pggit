//! `git-upload-pack` server-side. The client sends a stateless-rpc request:
//!
//! ```text
//! 0032want <oid>[ capabilities...]\n
//! 0032want <oid>\n
//! ...
//! 0000                                  (flush — end of wants)
//! 0032have <oid>\n  *                  (zero or more haves)
//! 0009done\n                            (sentinel; the client always sends it
//!                                        last in stateless-rpc, even with no
//!                                        haves, in which case it's the only
//!                                        line after the flush)
//! ```
//!
//! Our minimal response is `NAK` followed by the pack data, optionally
//! sideband-multiplexed if the client advertised `side-band-64k`.

use std::collections::HashSet;
use std::io::{Read, Write};
use std::sync::Arc;

use git2::Oid;
use gix_packetline::{blocking_io::StreamingPeekableIter, PacketLineRef};

use crate::http::error::HttpError;
use crate::store::PgGitStore;

use super::sideband::SidebandWriter;

#[derive(Debug, Default)]
struct Request {
    wants: Vec<Oid>,
    haves: Vec<Oid>,
    capabilities: Vec<String>,
}

impl Request {
    fn supports_sideband_64k(&self) -> bool {
        self.capabilities.iter().any(|c| c == "side-band-64k")
    }
}

/// Run an upload-pack exchange. `request` is the client's POST body
/// (buffered into memory by the caller), `response` is the body writer that
/// the caller has bridged to a streaming axum response.
pub(crate) fn run<R: Read, W: Write>(
    store: Arc<PgGitStore>,
    repo_id: i32,
    request: R,
    mut response: W,
) -> Result<(), HttpError> {
    let req = parse_request(request)?;
    if req.wants.is_empty() {
        // Nothing to do. Emit the protocol-required NAK and a flush so the
        // client doesn't hang.
        write_pktline(&mut response, b"NAK\n")?;
        return Ok(());
    }

    tracing::debug!(
        wants = req.wants.len(),
        haves = req.haves.len(),
        sideband = req.supports_sideband_64k(),
        "upload-pack: parsed request",
    );

    // We acknowledge with a single NAK — for a stateless RPC fetch this is
    // the simplest valid negotiation result, and clients accept it.
    write_pktline(&mut response, b"NAK\n")?;

    // Build the pack synchronously. Writing to a Vec keeps the libgit2 calls
    // simple; the surrounding spawn_blocking + duplex still streams the
    // bytes out to the client as we hand them off below.
    let pack_bytes = build_pack(&store, repo_id, &req)?;
    tracing::debug!(pack_bytes = pack_bytes.len(), "upload-pack: pack built");

    if req.supports_sideband_64k() {
        let mut sb = SidebandWriter::new(&mut response);
        sb.write_all(&pack_bytes)
            .map_err(|e| HttpError::Internal(format!("sideband write: {e}")))?;
        drop(sb);
        // Final pkt-line flush ends the response.
        response
            .write_all(b"0000")
            .map_err(|e| HttpError::Internal(format!("flush write: {e}")))?;
    } else {
        // No sideband — write pack bytes raw, no flush.
        response
            .write_all(&pack_bytes)
            .map_err(|e| HttpError::Internal(format!("pack write: {e}")))?;
    }

    Ok(())
}

fn parse_request<R: Read>(reader: R) -> Result<Request, HttpError> {
    let mut iter = StreamingPeekableIter::new(reader, &[PacketLineRef::Flush], false);
    let mut req = Request::default();
    let mut saw_first_want = false;
    let mut after_wants_flush = false;

    loop {
        match iter.read_line() {
            Some(Ok(Ok(line))) => {
                let bytes = match line.as_slice() {
                    Some(s) => s,
                    None => continue, // delimiter / response-end (shouldn't happen here)
                };
                let text = std::str::from_utf8(bytes)
                    .map_err(|_| HttpError::BadRequest("non-UTF8 pkt-line".into()))?
                    .trim_end_matches(['\r', '\n']);

                if !after_wants_flush {
                    if let Some(rest) = text.strip_prefix("want ") {
                        let (oid_str, caps) = match rest.split_once(' ') {
                            Some((oid, caps)) => (oid, Some(caps)),
                            None => (rest, None),
                        };
                        let oid = parse_oid(oid_str)?;
                        req.wants.push(oid);
                        if !saw_first_want {
                            saw_first_want = true;
                            if let Some(caps) = caps {
                                req.capabilities =
                                    caps.split(' ').map(str::to_string).collect();
                            }
                        }
                        continue;
                    }
                    return Err(HttpError::BadRequest(format!(
                        "unexpected pre-flush line: {text:?}"
                    )));
                }

                if let Some(rest) = text.strip_prefix("have ") {
                    let oid = parse_oid(rest)?;
                    req.haves.push(oid);
                } else if text == "done" {
                    break;
                } else if text.is_empty() {
                    // ignore stray empty lines
                } else {
                    // unknown — log and ignore so we don't choke on
                    // capability extensions we haven't taught about.
                    tracing::trace!(line = %text, "upload-pack: ignoring unknown line");
                }
            }
            Some(Ok(Err(e))) => {
                return Err(HttpError::BadRequest(format!("pkt-line decode: {e}")));
            }
            Some(Err(e)) => {
                return Err(HttpError::BadRequest(format!("pkt-line io: {e}")));
            }
            None => {
                // The iterator stopped on a flush. Reset it so we can keep
                // reading the next section (haves/done).
                if !after_wants_flush {
                    after_wants_flush = true;
                    iter.reset();
                    continue;
                }
                break;
            }
        }
    }

    Ok(req)
}

fn parse_oid(s: &str) -> Result<Oid, HttpError> {
    Oid::from_str(s).map_err(|e| HttpError::BadRequest(format!("bad oid {s:?}: {e}")))
}

fn build_pack(
    store: &Arc<PgGitStore>,
    repo_id: i32,
    req: &Request,
) -> Result<Vec<u8>, HttpError> {
    let pg = store.open_repository(repo_id)?;

    let mut walker = pg.revwalk()?;
    let mut want_set: HashSet<Oid> = HashSet::new();
    for want in &req.wants {
        // Tolerate wants that name objects we don't have — clients sometimes
        // ask for refs we already advertised but raced against deletion.
        if pg.find_object(*want, None).is_err() {
            return Err(HttpError::BadRequest(format!(
                "want {want} not found in repo"
            )));
        }
        walker.push(*want)?;
        want_set.insert(*want);
    }
    for have in &req.haves {
        // Hide failures are non-fatal: the peer may have history we don't.
        let _ = walker.hide(*have);
    }

    let mut pb = pg.packbuilder()?;
    pb.insert_walk(&mut walker)?;

    let mut buf = git2::Buf::new();
    pb.write_buf(&mut buf)?;
    Ok(buf.to_vec())
}

fn write_pktline<W: Write>(w: &mut W, payload: &[u8]) -> Result<(), HttpError> {
    let total = payload.len() + 4;
    if total > 65520 {
        return Err(HttpError::Internal(format!("pktline too long: {total}")));
    }
    write!(w, "{:04x}", total).map_err(|e| HttpError::Internal(e.to_string()))?;
    w.write_all(payload)
        .map_err(|e| HttpError::Internal(e.to_string()))?;
    Ok(())
}

