use std::path::{Path, PathBuf};
use std::process::Stdio;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Response, StatusCode};
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdout, Command};
use tokio_util::io::ReaderStream;

use super::error::HttpError;

/// Inputs for a single CGI invocation of `git http-backend`.
pub(crate) struct CgiCall<'a> {
    pub git_binary: &'a Path,
    pub git_dir: &'a Path,
    pub path_info: &'a str,
    pub query_string: &'a str,
    pub method: &'a str,
    pub content_type: Option<&'a str>,
    pub remote_addr: Option<&'a str>,
    pub content_length: Option<u64>,
}

struct CgiHead {
    status: StatusCode,
    headers: HeaderMap,
}

const HEADER_LIMIT: usize = 64 * 1024;

/// Drop guard handed to a streaming response body. Whatever is wrapped here
/// (tempdir, workdir, ...) is kept alive until the response stream completes.
pub(crate) type KeepAlive = Box<dyn std::any::Any + Send + Sync>;

/// Run the CGI process, returning the response with a streaming body.
/// Used for `git-upload-pack`. `keep_alive` is dropped after the response
/// body finishes streaming.
pub(crate) async fn run_streaming(
    call: CgiCall<'_>,
    body: Body,
    keep_alive: Option<KeepAlive>,
) -> Result<Response<Body>, HttpError> {
    let (child, mut stdout) = spawn_and_pipe_body(&call, body).await?;

    // Read until we've consumed the CGI header block, then stream the rest.
    let mut prologue = Vec::with_capacity(2048);
    let mut tmp = [0u8; 1024];
    let body_start = loop {
        let n = stdout.read(&mut tmp).await?;
        if n == 0 {
            return Err(HttpError::Internal(
                "git http-backend closed before sending headers".into(),
            ));
        }
        prologue.extend_from_slice(&tmp[..n]);
        if let Some(off) = find_header_end(&prologue) {
            break off;
        }
        if prologue.len() > HEADER_LIMIT {
            return Err(HttpError::Internal("CGI header block too large".into()));
        }
    };

    let head = parse_cgi_head(&prologue[..body_start])?;
    let leftover = Bytes::from(prologue.split_off(body_start));

    let stream = ChainStream::new(leftover, ReaderStream::new(stdout), child, keep_alive);
    let body = Body::from_stream(stream);

    build_response(head, body)
}

/// Run the CGI process, buffering its entire stdout. Used where the response
/// body is small or where we need to know the child exited cleanly before
/// proceeding (`info/refs`, `git-receive-pack`).
pub(crate) async fn run_buffered(call: CgiCall<'_>, body: Body) -> Result<Response<Body>, HttpError> {
    let (mut child, mut stdout) = spawn_and_pipe_body(&call, body).await?;

    let mut buf = Vec::new();
    stdout.read_to_end(&mut buf).await?;
    let status = child.wait().await?;
    if !status.success() {
        return Err(HttpError::Internal(format!(
            "git http-backend exited with status {status}"
        )));
    }

    let body_start = find_header_end(&buf)
        .ok_or_else(|| HttpError::Internal("CGI output had no header terminator".into()))?;
    let head = parse_cgi_head(&buf[..body_start])?;
    let body_bytes = Bytes::from(buf.split_off(body_start));

    build_response(head, Body::from(body_bytes))
}

/// Spawn `git http-backend` and start streaming the request body to stdin.
async fn spawn_and_pipe_body(
    call: &CgiCall<'_>,
    body: Body,
) -> Result<(Child, ChildStdout), HttpError> {
    let git_dir = absolutize(call.git_dir)?;
    let project_root = git_dir
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| git_dir.clone());

    // git http-backend's PATH_INFO is interpreted relative to GIT_PROJECT_ROOT,
    // so we set project root = parent dir, and the path info already includes
    // the repo name as the first segment.

    let mut cmd = Command::new(call.git_binary);
    cmd.arg("http-backend")
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("GIT_PROJECT_ROOT", &project_root)
        .env("GIT_HTTP_EXPORT_ALL", "1")
        .env("PATH_INFO", call.path_info)
        .env("QUERY_STRING", call.query_string)
        .env("REQUEST_METHOD", call.method)
        .env("GATEWAY_INTERFACE", "CGI/1.1")
        .env("SERVER_PROTOCOL", "HTTP/1.1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    if let Some(ct) = call.content_type {
        cmd.env("CONTENT_TYPE", ct);
    }
    if let Some(len) = call.content_length {
        cmd.env("CONTENT_LENGTH", len.to_string());
    }
    if let Some(addr) = call.remote_addr {
        cmd.env("REMOTE_ADDR", addr);
    }

    tracing::debug!(
        binary = ?call.git_binary,
        path_info = call.path_info,
        query = call.query_string,
        method = call.method,
        project_root = %project_root.display(),
        "cgi: spawning git http-backend",
    );

    let mut child = cmd.spawn().map_err(|e| {
        HttpError::Internal(format!("failed to spawn {:?}: {e}", call.git_binary))
    })?;

    let stdin = child.stdin.take().expect("stdin piped");
    let stdout = child.stdout.take().expect("stdout piped");
    let stderr = child.stderr.take().expect("stderr piped");

    // Forward request body -> child stdin.
    tokio::spawn(async move {
        use futures::StreamExt;
        let mut stdin = stdin;
        let mut stream = body.into_data_stream();
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    if let Err(e) = stdin.write_all(&bytes).await {
                        tracing::debug!(error = %e, "stdin write failed (client likely disconnected)");
                        return;
                    }
                }
                Err(e) => {
                    tracing::debug!(error = %e, "request body read error");
                    return;
                }
            }
        }
        let _ = stdin.shutdown().await;
    });

    // Drain stderr to tracing.
    tokio::spawn(async move {
        let mut s = stderr;
        let mut buf = Vec::with_capacity(1024);
        if s.read_to_end(&mut buf).await.is_ok() && !buf.is_empty() {
            let msg = String::from_utf8_lossy(&buf);
            for line in msg.lines() {
                tracing::warn!(target: "pggit::http::cgi", "git http-backend: {line}");
            }
        }
    });

    Ok((child, stdout))
}

/// Find the byte offset of the first byte after the CGI header terminator
/// (handles both `\r\n\r\n` and `\n\n`).
fn find_header_end(buf: &[u8]) -> Option<usize> {
    for i in 0..buf.len() {
        if buf[i..].starts_with(b"\r\n\r\n") {
            return Some(i + 4);
        }
        if buf[i..].starts_with(b"\n\n") {
            return Some(i + 2);
        }
    }
    None
}

fn parse_cgi_head(prologue: &[u8]) -> Result<CgiHead, HttpError> {
    let text = std::str::from_utf8(prologue)
        .map_err(|_| HttpError::Internal("CGI headers contained non-UTF8 bytes".into()))?;

    let mut status = StatusCode::OK;
    let mut headers = HeaderMap::new();

    for raw_line in text.split('\n') {
        let line = raw_line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err(HttpError::Internal(format!(
                "malformed CGI header line: {line:?}"
            )));
        };
        let name = name.trim();
        let value = value.trim();

        if name.eq_ignore_ascii_case("Status") {
            let code = value
                .split_whitespace()
                .next()
                .and_then(|s| s.parse::<u16>().ok())
                .ok_or_else(|| HttpError::Internal(format!("bad CGI Status: {value}")))?;
            status = StatusCode::from_u16(code)
                .map_err(|_| HttpError::Internal(format!("bad status code: {code}")))?;
            continue;
        }

        let header_name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| HttpError::Internal(format!("bad CGI header name {name}: {e}")))?;
        let header_value = HeaderValue::from_str(value)
            .map_err(|e| HttpError::Internal(format!("bad CGI header value: {e}")))?;
        headers.append(header_name, header_value);
    }

    Ok(CgiHead { status, headers })
}

fn build_response(head: CgiHead, body: Body) -> Result<Response<Body>, HttpError> {
    let mut builder = Response::builder().status(head.status);
    for (name, value) in head.headers.iter() {
        builder = builder.header(name, value);
    }
    builder
        .body(body)
        .map_err(|e| HttpError::Internal(format!("response build: {e}")))
}

fn absolutize(p: &Path) -> Result<PathBuf, HttpError> {
    if p.is_absolute() {
        Ok(p.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(p))
    }
}

/// Stream that emits a leading buffer of bytes, then the contents of an inner
/// stream, and reaps a child process when exhausted. Holds an optional
/// drop-guard (e.g. a `Workdir`) so backing resources outlive the response.
struct ChainStream {
    leftover: Option<Bytes>,
    inner: ReaderStream<ChildStdout>,
    child: Option<Child>,
    _keep_alive: Option<KeepAlive>,
}

impl ChainStream {
    fn new(
        leftover: Bytes,
        inner: ReaderStream<ChildStdout>,
        child: Child,
        keep_alive: Option<KeepAlive>,
    ) -> Self {
        let leftover = if leftover.is_empty() { None } else { Some(leftover) };
        Self {
            leftover,
            inner,
            child: Some(child),
            _keep_alive: keep_alive,
        }
    }
}

impl futures::Stream for ChainStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if let Some(b) = self.leftover.take() {
            return std::task::Poll::Ready(Some(Ok(b)));
        }
        let inner = std::pin::Pin::new(&mut self.inner);
        let r = inner.poll_next(cx);
        if let std::task::Poll::Ready(None) = &r {
            // Drop child to reap (kill_on_drop ensures cleanup if still alive).
            if let Some(child) = self.child.take() {
                tokio::spawn(async move {
                    let mut child = child;
                    if let Ok(status) = child.wait().await {
                        if !status.success() {
                            tracing::warn!(?status, "git http-backend exited non-zero");
                        }
                    }
                });
            }
        }
        r
    }
}
