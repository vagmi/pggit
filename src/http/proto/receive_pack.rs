//! `git-receive-pack` server-side. The client POSTs:
//!
//! ```text
//! 00aa<old> <new> <ref>\0<capabilities>\n   (first command, caps after NUL)
//! 0050<old> <new> <ref>\n                    (subsequent commands)
//! 0000                                       (flush — end of commands)
//! <pack data>                                (zero-length only when all
//!                                             commands are deletes)
//! ```
//!
//! We respond with report-status pkt-lines: a single `unpack ok|error`
//! line, then per-ref `ok <ref>` or `ng <ref> <reason>`, then flush.

use std::io::{Read, Write};
use std::sync::Arc;

use git2::Oid;
use gix_packetline::{blocking_io::StreamingPeekableIter, PacketLineRef};
use tempfile::TempDir;

use crate::http::error::HttpError;
use crate::store::PgGitStore;

const ZERO_OID: &str = "0000000000000000000000000000000000000000";

#[derive(Debug)]
struct Command {
    old: Oid,
    new: Oid,
    refname: String,
}

impl Command {
    fn is_delete(&self) -> bool {
        self.new.is_zero()
    }

    fn is_create(&self) -> bool {
        self.old.is_zero()
    }
}

#[derive(Debug, Default)]
struct Request {
    commands: Vec<Command>,
    capabilities: Vec<String>,
}

impl Request {
    fn wants_report_status(&self) -> bool {
        self.capabilities.iter().any(|c| c == "report-status")
    }
}

pub(crate) fn run<R: Read, W: Write>(
    store: Arc<PgGitStore>,
    repo_id: i32,
    request: R,
    mut response: W,
) -> Result<(), HttpError> {
    let (req, mut request_reader) = parse_commands(request)?;
    tracing::debug!(
        commands = req.commands.len(),
        capabilities = ?req.capabilities,
        "receive-pack: parsed commands",
    );

    if req.commands.is_empty() {
        // No work — but the protocol still expects us to send a report.
        if req.wants_report_status() {
            write_report(&mut response, "ok", &[])?;
        }
        return Ok(());
    }

    // 1. Receive the pack (if any).
    let pack_outcome = if req.commands.iter().all(Command::is_delete) {
        // No pack expected — clients may legitimately send no body bytes.
        Ok(())
    } else {
        receive_pack(&store, repo_id, &mut request_reader)
    };

    // 2. Apply ref updates. Even if the pack failed we still respond with
    //    the unpack-error line and ng lines for each ref.
    let mut per_ref: Vec<(String, Result<(), String>)> = Vec::with_capacity(req.commands.len());
    let unpack_status = match &pack_outcome {
        Ok(()) => "ok".to_string(),
        Err(e) => {
            tracing::warn!(error = %e, "receive-pack: unpack failed");
            format!("error {e}")
        }
    };

    if pack_outcome.is_ok() {
        let pg = store.open_repository(repo_id).map_err(HttpError::PgGit)?;
        for cmd in &req.commands {
            let outcome = apply_command(&pg, cmd);
            per_ref.push((cmd.refname.clone(), outcome));
        }
    } else {
        for cmd in &req.commands {
            per_ref.push((
                cmd.refname.clone(),
                Err("pack rejected".to_string()),
            ));
        }
    }

    if req.wants_report_status() {
        write_report(&mut response, &unpack_status, &per_ref)?;
    }

    pack_outcome.map_err(HttpError::Internal)
}

/// Parse pkt-line ref-update commands until the flush, then return a Reader
/// positioned at the start of the pack data.
fn parse_commands<R: Read>(reader: R) -> Result<(Request, R), HttpError> {
    let mut iter = StreamingPeekableIter::new(reader, &[PacketLineRef::Flush], false);
    let mut req = Request::default();
    let mut saw_first = false;

    loop {
        match iter.read_line() {
            Some(Ok(Ok(line))) => {
                let bytes = match line.as_slice() {
                    Some(s) => s,
                    None => continue,
                };
                let raw = bytes.strip_suffix(b"\n").unwrap_or(bytes);
                let raw = raw.strip_suffix(b"\r").unwrap_or(raw);

                // First command may have NUL-separated capabilities appended.
                let (cmd_bytes, caps_bytes) = match raw.iter().position(|&b| b == 0) {
                    Some(i) => (&raw[..i], Some(&raw[i + 1..])),
                    None => (raw, None),
                };

                let cmd = parse_command(cmd_bytes)?;
                if !saw_first {
                    saw_first = true;
                    if let Some(caps) = caps_bytes {
                        let caps_str = std::str::from_utf8(caps)
                            .map_err(|_| HttpError::BadRequest("non-UTF8 caps".into()))?;
                        req.capabilities = caps_str.split(' ').map(str::to_string).collect();
                    }
                }
                req.commands.push(cmd);
            }
            Some(Ok(Err(e))) => {
                return Err(HttpError::BadRequest(format!("pkt-line decode: {e}")));
            }
            Some(Err(e)) => {
                return Err(HttpError::BadRequest(format!("pkt-line io: {e}")));
            }
            None => break, // hit flush
        }
    }

    let raw = iter.into_inner();
    Ok((req, raw))
}

fn parse_command(bytes: &[u8]) -> Result<Command, HttpError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| HttpError::BadRequest("non-UTF8 command".into()))?;
    // Format: "<old> <new> <ref>"
    let mut parts = text.splitn(3, ' ');
    let old = parts
        .next()
        .ok_or_else(|| HttpError::BadRequest(format!("malformed cmd: {text}")))?;
    let new = parts
        .next()
        .ok_or_else(|| HttpError::BadRequest(format!("malformed cmd: {text}")))?;
    let refname = parts
        .next()
        .ok_or_else(|| HttpError::BadRequest(format!("malformed cmd: {text}")))?;
    let old = parse_oid_or_zero(old)?;
    let new = parse_oid_or_zero(new)?;
    Ok(Command {
        old,
        new,
        refname: refname.to_string(),
    })
}

fn parse_oid_or_zero(s: &str) -> Result<Oid, HttpError> {
    // Oid::from_str("0000...") returns the zero OID.
    if s == ZERO_OID {
        Oid::from_str(ZERO_OID).map_err(|e| HttpError::BadRequest(format!("{e}")))
    } else {
        Oid::from_str(s).map_err(|e| HttpError::BadRequest(format!("bad oid {s:?}: {e}")))
    }
}

/// Receive a pack and copy its objects into PG.
///
/// libgit2's `Indexer` writes the pack to a file (and validates it), but
/// can't write objects directly into our custom PG ODB — that backend
/// doesn't expose a `writepack` callback. So we route the pack through a
/// temp directory: indexer writes `pack-<sha>.pack` + `.idx`, we open it as
/// a local bare repo, then walk every OID and copy to PG via the regular
/// `Odb::write` path that our backend does support.
fn receive_pack<R: Read>(
    store: &Arc<PgGitStore>,
    repo_id: i32,
    reader: &mut R,
) -> Result<(), String> {
    let pg = store
        .open_repository(repo_id)
        .map_err(|e| format!("open repo: {e}"))?;

    let tempdir = TempDir::new().map_err(|e| format!("tempdir: {e}"))?;
    let local = git2::Repository::init_bare(tempdir.path())
        .map_err(|e| format!("init bare: {e}"))?;
    let pack_dir = tempdir.path().join("objects").join("pack");

    let pg_odb = pg.odb().map_err(|e| format!("odb: {e}"))?;
    {
        let mut indexer = git2::Indexer::new(Some(&pg_odb), &pack_dir, 0, true)
            .map_err(|e| format!("indexer: {e}"))?;
        std::io::copy(reader, &mut indexer).map_err(|e| format!("pack body: {e}"))?;
        let pack_name = indexer.commit().map_err(|e| format!("pack commit: {e}"))?;
        tracing::debug!(pack = pack_name, "receive-pack: indexer committed");
    }

    let local_odb = local.odb().map_err(|e| format!("local odb: {e}"))?;
    local_odb.refresh().map_err(|e| format!("odb refresh: {e}"))?;

    let mut copied = 0usize;
    local_odb
        .foreach(|oid| {
            if pg_odb.exists(*oid) {
                return true;
            }
            match local_odb.read(*oid) {
                Ok(obj) => match pg_odb.write(obj.kind(), obj.data()) {
                    Ok(written) if written == *oid => {
                        copied += 1;
                        true
                    }
                    Ok(written) => {
                        tracing::error!(
                            local = %oid,
                            pg = %written,
                            "receive-pack: oid mismatch on copy",
                        );
                        false
                    }
                    Err(e) => {
                        tracing::error!(error = %e, oid = %oid, "receive-pack: pg write failed");
                        false
                    }
                },
                Err(e) => {
                    tracing::error!(error = %e, oid = %oid, "receive-pack: local read failed");
                    false
                }
            }
        })
        .map_err(|e| format!("foreach: {e}"))?;
    tracing::debug!(copied, "receive-pack: objects written to PG");
    Ok(())
}

fn apply_command(pg: &git2::Repository, cmd: &Command) -> Result<(), String> {
    if cmd.is_delete() {
        match pg.find_reference(&cmd.refname) {
            Ok(mut r) => {
                let current = r.target();
                if current != Some(cmd.old) {
                    return Err(format!(
                        "old oid mismatch (have {:?}, expected {})",
                        current, cmd.old
                    ));
                }
                r.delete().map_err(|e| format!("delete: {e}"))?;
                Ok(())
            }
            Err(_) => Err("ref does not exist".into()),
        }
    } else if cmd.is_create() {
        if pg.find_reference(&cmd.refname).is_ok() {
            return Err("ref already exists".into());
        }
        pg.reference(&cmd.refname, cmd.new, false, "pggit smart-http push")
            .map_err(|e| format!("create: {e}"))?;
        Ok(())
    } else {
        // Update: CAS on old.
        let r = pg
            .find_reference(&cmd.refname)
            .map_err(|_| "ref does not exist".to_string())?;
        let current = r.target();
        if current != Some(cmd.old) {
            return Err(format!(
                "old oid mismatch (have {:?}, expected {})",
                current, cmd.old
            ));
        }
        // Verify the new commit reachable from local repo (it should be — we
        // just wrote the pack).
        if pg.find_object(cmd.new, None).is_err() {
            return Err("new oid not in repo".into());
        }
        pg.reference(&cmd.refname, cmd.new, true, "pggit smart-http push")
            .map_err(|e| format!("update: {e}"))?;
        Ok(())
    }
}

fn write_report<W: Write>(
    w: &mut W,
    unpack_status: &str,
    per_ref: &[(String, Result<(), String>)],
) -> Result<(), HttpError> {
    write_pktline(w, format!("unpack {unpack_status}\n").as_bytes())?;
    for (refname, result) in per_ref {
        let line = match result {
            Ok(()) => format!("ok {refname}\n"),
            Err(reason) => format!("ng {refname} {reason}\n"),
        };
        write_pktline(w, line.as_bytes())?;
    }
    w.write_all(b"0000")
        .map_err(|e| HttpError::Internal(format!("flush write: {e}")))?;
    Ok(())
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

