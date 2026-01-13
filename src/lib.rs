//! Fast, idempotent, multi-part downloads with a simple API.
//!
//! ripget prioritizes speed by downloading large files in parallel with HTTP
//! range requests. It also supports idempotent resumes using baked-in marker
//! bytes that are extremely unlikely to appear in real data.
//!
//! # Example
//! ```no_run
//! # #[tokio::main]
//! # async fn main() -> Result<(), ripget::RipgetError> {
//! let report = ripget::download_url(
//!     "https://example.com/large.bin",
//!     "large.bin",
//!     Some(16),
//!     None,
//! ).await?;
//! println!("downloaded {} bytes to {:?}", report.bytes, report.path);
//! # Ok(())
//! # }
//! ```

use std::cmp;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures_util::TryStreamExt;
use memchr::memmem;
use reqwest::header::{ACCEPT_ENCODING, CONTENT_RANGE, HeaderMap, HeaderValue, RANGE};
use reqwest::{Client, StatusCode};
use tokio::fs::OpenOptions;
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::task::JoinSet;
use tokio_util::io::StreamReader;

/// Default number of parallel ranges used by ripget.
pub const DEFAULT_THREADS: usize = 16;

/// Fixed read buffer size used for streaming data.
pub const BUFFER_SIZE: usize = 8 * 1024 * 1024;

const MARKER_A: u128 = 0x0b933fa4efd1205406d7cce948d07562;
const MARKER_B: u128 = 0xc229c46eda461eaf9e68fa9da89a29ba;

const fn marker_bytes() -> [u8; 32] {
    let a = MARKER_A.to_be_bytes();
    let b = MARKER_B.to_be_bytes();
    let mut out = [0u8; 32];
    let mut i = 0;
    while i < 16 {
        out[i] = a[i];
        out[i + 16] = b[i];
        i += 1;
    }
    out
}

const MARKER_BYTES: [u8; 32] = marker_bytes();
const MARKER_LEN: usize = 32;

/// Result type for ripget operations.
pub type Result<T> = std::result::Result<T, RipgetError>;

/// Error type for ripget operations.
#[derive(Debug, thiserror::Error)]
pub enum RipgetError {
    #[error("invalid thread count: {0}")]
    InvalidThreadCount(usize),
    #[error("missing Content-Range header for {0}")]
    ContentRangeMissing(String),
    #[error("invalid Content-Range header for {0}")]
    InvalidContentRange(String),
    #[error("range requests are not supported by {0}")]
    RangeNotSupported(String),
    #[error("unexpected HTTP status {status} for {url}")]
    HttpStatus { status: StatusCode, url: String },
    #[error("file size {found} does not match expected {expected}")]
    FileSizeMismatch { expected: u64, found: u64 },
    #[error("unexpected end of stream after {got} bytes, expected {expected}")]
    UnexpectedEof { expected: u64, got: u64 },
    #[error("task failed: {0}")]
    JoinError(String),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
}

/// Information about a completed download.
#[derive(Debug, Clone)]
pub struct DownloadReport {
    /// URL if the source was HTTP.
    pub url: Option<String>,
    /// Destination path on disk.
    pub path: PathBuf,
    /// Total bytes written.
    pub bytes: u64,
    /// Number of parallel ranges used.
    pub threads: usize,
}

/// Reports download progress for integrations like CLI progress bars.
pub trait ProgressReporter: Send + Sync {
    /// Initializes the total expected bytes.
    fn init(&self, total: u64);
    /// Adds downloaded bytes to the progress total.
    fn add(&self, delta: u64);
}

/// Shared progress reporter handle.
pub type Progress = Arc<dyn ProgressReporter>;

#[derive(Debug, Clone, Copy)]
struct Range {
    start: u64,
    end: u64,
}

#[derive(Debug, Clone, Copy)]
struct RemoteMetadata {
    len: u64,
}

/// Download a URL to a file path using parallel range requests.
///
/// * `threads` defaults to 16 when `None`.
/// * `user_agent` defaults to `ripget/<version>` when `None`.
///
/// Use [`download_url_with_progress`] to receive progress callbacks.
pub async fn download_url(
    url: impl AsRef<str>,
    dest: impl AsRef<Path>,
    threads: Option<usize>,
    user_agent: Option<&str>,
) -> Result<DownloadReport> {
    download_url_with_progress(url, dest, threads, user_agent, None).await
}

/// Download a URL to a file path using parallel range requests with progress.
///
/// * `threads` defaults to 16 when `None`.
/// * `user_agent` defaults to `ripget/<version>` when `None`.
pub async fn download_url_with_progress(
    url: impl AsRef<str>,
    dest: impl AsRef<Path>,
    threads: Option<usize>,
    user_agent: Option<&str>,
    progress: Option<Progress>,
) -> Result<DownloadReport> {
    let url = url.as_ref();
    let dest = dest.as_ref().to_path_buf();
    let requested_threads = normalize_threads(threads)?;
    let client = build_client(user_agent)?;
    let metadata = fetch_metadata(&client, url).await?;
    progress_init(&progress, metadata.len);
    if metadata.len == 0 {
        prepare_file(&dest, 0).await?;
        return Ok(DownloadReport {
            url: Some(url.to_string()),
            path: dest,
            bytes: 0,
            threads: 0,
        });
    }

    let preexisting = tokio::fs::metadata(&dest).await.is_ok();
    prepare_file(&dest, metadata.len).await?;

    let threads = clamp_threads(requested_threads, metadata.len);
    let ranges = split_ranges(metadata.len, threads);

    let mut join_set = JoinSet::new();
    for range in ranges {
        let client = client.clone();
        let url = url.to_string();
        let dest = dest.clone();
        let progress = progress.clone();
        join_set.spawn(async move {
            download_range(&client, &url, &dest, range, preexisting, progress).await
        });
    }

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(inner) => inner?,
            Err(err) => return Err(RipgetError::JoinError(err.to_string())),
        }
    }

    Ok(DownloadReport {
        url: Some(url.to_string()),
        path: dest,
        bytes: metadata.len,
        threads,
    })
}

/// Copy an async reader into a file path while preserving idempotent markers.
///
/// This uses a single range and requires the expected length up front.
///
/// Use [`download_reader_with_progress`] to receive progress callbacks.
pub async fn download_reader<R>(
    reader: R,
    dest: impl AsRef<Path>,
    expected_len: u64,
) -> Result<DownloadReport>
where
    R: AsyncRead + Unpin,
{
    download_reader_with_progress(reader, dest, expected_len, None).await
}

/// Copy an async reader into a file path while preserving idempotent markers.
///
/// This uses a single range and requires the expected length up front.
pub async fn download_reader_with_progress<R>(
    mut reader: R,
    dest: impl AsRef<Path>,
    expected_len: u64,
    progress: Option<Progress>,
) -> Result<DownloadReport>
where
    R: AsyncRead + Unpin,
{
    let dest = dest.as_ref().to_path_buf();
    progress_init(&progress, expected_len);
    if expected_len == 0 {
        prepare_file(&dest, 0).await?;
        return Ok(DownloadReport {
            url: None,
            path: dest,
            bytes: 0,
            threads: 1,
        });
    }

    let preexisting = tokio::fs::metadata(&dest).await.is_ok();
    prepare_file(&dest, expected_len).await?;

    let range = Range {
        start: 0,
        end: expected_len - 1,
    };
    let resume = resume_offset(&dest, range, preexisting).await?;
    let Some(offset) = resume else {
        progress_add(&progress, expected_len);
        return Ok(DownloadReport {
            url: None,
            path: dest,
            bytes: expected_len,
            threads: 1,
        });
    };

    progress_add(&progress, offset.saturating_sub(range.start));
    skip_bytes(&mut reader, offset).await?;
    write_range_from_reader(&mut reader, &dest, range, offset, &progress).await?;

    Ok(DownloadReport {
        url: None,
        path: dest,
        bytes: expected_len,
        threads: 1,
    })
}

fn normalize_threads(threads: Option<usize>) -> Result<usize> {
    let threads = threads.unwrap_or(DEFAULT_THREADS);
    if threads == 0 {
        return Err(RipgetError::InvalidThreadCount(threads));
    }
    Ok(threads)
}

fn clamp_threads(threads: usize, total_len: u64) -> usize {
    let total = cmp::max(1, total_len) as usize;
    cmp::min(threads, total)
}

fn split_ranges(total_len: u64, threads: usize) -> Vec<Range> {
    if total_len == 0 {
        return Vec::new();
    }
    let threads = clamp_threads(threads, total_len);
    let base = total_len / threads as u64;
    let remainder = total_len % threads as u64;

    let mut ranges = Vec::with_capacity(threads);
    let mut start = 0u64;
    for idx in 0..threads {
        let mut size = base;
        if (idx as u64) < remainder {
            size += 1;
        }
        let end = start + size - 1;
        ranges.push(Range { start, end });
        start = end + 1;
    }
    ranges
}

fn default_user_agent() -> String {
    format!("ripget/{}", env!("CARGO_PKG_VERSION"))
}

fn build_client(user_agent: Option<&str>) -> Result<Client> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
    let agent = user_agent
        .map(str::to_string)
        .unwrap_or_else(default_user_agent);
    Ok(Client::builder()
        .default_headers(headers)
        .user_agent(agent)
        .build()?)
}

async fn fetch_metadata(client: &Client, url: &str) -> Result<RemoteMetadata> {
    let response = client.get(url).header(RANGE, "bytes=0-0").send().await?;

    if response.status() != StatusCode::PARTIAL_CONTENT {
        return Err(RipgetError::RangeNotSupported(url.to_string()));
    }

    let content_range = response
        .headers()
        .get(CONTENT_RANGE)
        .ok_or_else(|| RipgetError::ContentRangeMissing(url.to_string()))?;
    let total_len = parse_content_range_total(content_range, url)?;

    Ok(RemoteMetadata { len: total_len })
}

fn parse_content_range_total(value: &HeaderValue, url: &str) -> Result<u64> {
    let value = value
        .to_str()
        .map_err(|_| RipgetError::InvalidContentRange(url.to_string()))?;
    let mut parts = value.split('/');
    let _range = parts
        .next()
        .ok_or_else(|| RipgetError::InvalidContentRange(url.to_string()))?;
    let total = parts
        .next()
        .ok_or_else(|| RipgetError::InvalidContentRange(url.to_string()))?;
    if parts.next().is_some() || total == "*" {
        return Err(RipgetError::InvalidContentRange(url.to_string()));
    }
    total
        .parse::<u64>()
        .map_err(|_| RipgetError::InvalidContentRange(url.to_string()))
}

async fn prepare_file(path: &Path, size: u64) -> Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)
        .await?;
    let len = file.metadata().await?.len();
    if len > size {
        return Err(RipgetError::FileSizeMismatch {
            expected: size,
            found: len,
        });
    }
    if len < size {
        file.set_len(size).await?;
    }
    Ok(())
}

async fn download_range(
    client: &Client,
    url: &str,
    path: &Path,
    range: Range,
    preexisting: bool,
    progress: Option<Progress>,
) -> Result<()> {
    let range_len = range.end - range.start + 1;
    let resume = resume_offset(path, range, preexisting).await?;
    let Some(offset) = resume else {
        progress_add(&progress, range_len);
        return Ok(());
    };
    progress_add(&progress, offset.saturating_sub(range.start));
    if offset > range.end {
        return Ok(());
    }

    let response = client
        .get(url)
        .header(RANGE, format!("bytes={}-{}", offset, range.end))
        .send()
        .await?;

    if response.status() != StatusCode::PARTIAL_CONTENT {
        return Err(RipgetError::HttpStatus {
            status: response.status(),
            url: url.to_string(),
        });
    }

    let stream = response
        .bytes_stream()
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err));
    let mut reader = StreamReader::new(stream);
    write_range_from_reader(&mut reader, path, range, offset, &progress).await
}

async fn resume_offset(path: &Path, range: Range, preexisting: bool) -> Result<Option<u64>> {
    if !preexisting {
        return Ok(Some(range.start));
    }
    Ok(find_last_marker(path, range.start, range.end).await?)
}

async fn find_last_marker(path: &Path, start: u64, end: u64) -> Result<Option<u64>> {
    let mut file = OpenOptions::new().read(true).open(path).await?;
    file.seek(SeekFrom::Start(start)).await?;

    let mut offset = start;
    let mut carry: Vec<u8> = Vec::new();
    let mut last_found: Option<u64> = None;
    let finder = memmem::Finder::new(&MARKER_BYTES);

    let mut buf = vec![0u8; BUFFER_SIZE];
    while offset <= end {
        let remaining = (end - offset + 1) as usize;
        let read_len = cmp::min(remaining, BUFFER_SIZE);
        let n = file.read(&mut buf[..read_len]).await?;
        if n == 0 {
            break;
        }

        let mut window = Vec::with_capacity(carry.len() + n);
        window.extend_from_slice(&carry);
        window.extend_from_slice(&buf[..n]);

        for pos in finder.find_iter(&window) {
            let absolute = offset.saturating_sub(carry.len() as u64) + pos as u64;
            last_found = Some(absolute);
        }

        let keep = MARKER_LEN.saturating_sub(1);
        if keep > 0 {
            if window.len() >= keep {
                carry.clear();
                carry.extend_from_slice(&window[window.len() - keep..]);
            } else {
                carry = window;
            }
        }

        offset += n as u64;
    }

    Ok(last_found)
}

async fn write_range_from_reader<R: AsyncRead + Unpin>(
    reader: &mut R,
    path: &Path,
    range: Range,
    mut offset: u64,
    progress: &Option<Progress>,
) -> Result<()> {
    let expected = range.end - offset + 1;
    let mut remaining = expected;
    let mut file = OpenOptions::new().read(true).write(true).open(path).await?;
    let mut buf = vec![0u8; BUFFER_SIZE];

    while remaining > 0 {
        let read_len = cmp::min(remaining as usize, BUFFER_SIZE);
        let n = reader.read(&mut buf[..read_len]).await?;
        if n == 0 {
            break;
        }

        file.seek(SeekFrom::Start(offset)).await?;
        file.write_all(&buf[..n]).await?;
        progress_add(progress, n as u64);
        offset += n as u64;
        remaining -= n as u64;

        if remaining > 0 && offset + MARKER_LEN as u64 - 1 <= range.end {
            file.seek(SeekFrom::Start(offset)).await?;
            file.write_all(&MARKER_BYTES).await?;
        }
    }

    if remaining != 0 {
        let got = expected - remaining;
        return Err(RipgetError::UnexpectedEof { expected, got });
    }
    Ok(())
}

async fn skip_bytes<R: AsyncRead + Unpin>(reader: &mut R, mut to_skip: u64) -> Result<()> {
    if to_skip == 0 {
        return Ok(());
    }
    let mut buf = vec![0u8; cmp::min(BUFFER_SIZE, 64 * 1024)];
    let expected = to_skip;
    while to_skip > 0 {
        let read_len = cmp::min(to_skip as usize, buf.len());
        let n = reader.read(&mut buf[..read_len]).await?;
        if n == 0 {
            let got = expected - to_skip;
            return Err(RipgetError::UnexpectedEof { expected, got });
        }
        to_skip -= n as u64;
    }
    Ok(())
}

fn progress_init(progress: &Option<Progress>, total: u64) {
    if let Some(progress) = progress {
        progress.init(total);
    }
}

fn progress_add(progress: &Option<Progress>, delta: u64) {
    if delta == 0 {
        return;
    }
    if let Some(progress) = progress {
        progress.add(delta);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, RANGE};
    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Method, Request, Response, Server, StatusCode};
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;
    use tokio::sync::oneshot;

    fn handle_request(req: Request<Body>, data: Arc<Vec<u8>>) -> Response<Body> {
        match *req.method() {
            Method::HEAD => Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_LENGTH, data.len().to_string())
                .header(ACCEPT_RANGES, "bytes")
                .body(Body::empty())
                .unwrap(),
            Method::GET => {
                if let Some(range) = req.headers().get(RANGE) {
                    if let Ok(range_str) = range.to_str() {
                        if let Some((start, end)) = parse_range_header(range_str, data.len()) {
                            let body = data[start..=end].to_vec();
                            return Response::builder()
                                .status(StatusCode::PARTIAL_CONTENT)
                                .header(CONTENT_LENGTH, body.len().to_string())
                                .header(
                                    CONTENT_RANGE,
                                    format!("bytes {}-{}/{}", start, end, data.len()),
                                )
                                .header(ACCEPT_RANGES, "bytes")
                                .body(Body::from(body))
                                .unwrap();
                        }
                    }
                }
                Response::builder()
                    .status(StatusCode::OK)
                    .header(CONTENT_LENGTH, data.len().to_string())
                    .header(ACCEPT_RANGES, "bytes")
                    .body(Body::from(data.as_slice().to_vec()))
                    .unwrap()
            }
            _ => Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(Body::empty())
                .unwrap(),
        }
    }

    fn parse_range_header(value: &str, len: usize) -> Option<(usize, usize)> {
        let value = value.strip_prefix("bytes=")?;
        let mut parts = value.splitn(2, '-');
        let start = parts.next()?.parse::<usize>().ok()?;
        let end = parts.next()?.parse::<usize>().ok()?;
        if start >= len {
            return None;
        }
        let end = cmp::min(end, len - 1);
        if start > end {
            return None;
        }
        Some((start, end))
    }

    async fn spawn_range_server(data: Arc<Vec<u8>>) -> (SocketAddr, oneshot::Sender<()>) {
        let make_svc = make_service_fn(move |_| {
            let data = data.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |req| {
                    let data = data.clone();
                    async move { Ok::<_, Infallible>(handle_request(req, data)) }
                }))
            }
        });

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = Server::from_tcp(listener).unwrap().serve(make_svc);
        let (tx, rx) = oneshot::channel();
        let graceful = server.with_graceful_shutdown(async {
            let _ = rx.await;
        });
        tokio::spawn(graceful);
        (addr, tx)
    }

    #[tokio::test]
    async fn find_last_marker_returns_last_offset() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("marker.bin");
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .await?;
        file.set_len(256).await?;
        file.seek(SeekFrom::Start(10)).await?;
        file.write_all(&MARKER_BYTES).await?;
        file.seek(SeekFrom::Start(64)).await?;
        file.write_all(&MARKER_BYTES).await?;

        let found = find_last_marker(&path, 0, 255).await?;
        assert_eq!(found, Some(64));
        Ok(())
    }

    #[tokio::test]
    async fn download_url_completes() -> Result<()> {
        let data: Vec<u8> = (0..(1024 * 1024 * 2)).map(|i| (i % 251) as u8).collect();
        let (addr, shutdown) = spawn_range_server(Arc::new(data.clone())).await;

        let dir = tempdir()?;
        let path = dir.path().join("file.bin");
        let url = format!("http://{}/file.bin", addr);

        let report = download_url(&url, &path, Some(4), None).await?;
        assert_eq!(report.bytes as usize, data.len());

        let downloaded = tokio::fs::read(&path).await?;
        assert_eq!(downloaded, data);
        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn download_url_resumes_from_marker() -> Result<()> {
        let data: Vec<u8> = (0..(1024 * 1024 * 3)).map(|i| (i % 193) as u8).collect();
        let (addr, shutdown) = spawn_range_server(Arc::new(data.clone())).await;

        let dir = tempdir()?;
        let path = dir.path().join("resume.bin");
        let url = format!("http://{}/resume.bin", addr);

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .await?;
        file.set_len(data.len() as u64).await?;
        let resume_at = 1024 * 512;
        file.seek(SeekFrom::Start(0)).await?;
        file.write_all(&data[..resume_at]).await?;
        file.seek(SeekFrom::Start(resume_at as u64)).await?;
        file.write_all(&MARKER_BYTES).await?;
        drop(file);

        download_url(&url, &path, Some(1), None).await?;

        let downloaded = tokio::fs::read(&path).await?;
        assert_eq!(downloaded, data);
        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn download_reader_completes() -> Result<()> {
        let data = b"hello from a reader".to_vec();
        let (mut tx, rx) = tokio::io::duplex(64);
        let data_clone = data.clone();
        tokio::spawn(async move {
            let _ = tx.write_all(&data_clone).await;
        });

        let dir = tempdir()?;
        let path = dir.path().join("reader.bin");
        download_reader(rx, &path, data.len() as u64).await?;

        let downloaded = tokio::fs::read(&path).await?;
        assert_eq!(downloaded, data);
        Ok(())
    }

    struct TestProgress {
        total: Mutex<Option<u64>>,
        seen: Mutex<u64>,
    }

    impl ProgressReporter for TestProgress {
        fn init(&self, total: u64) {
            *self.total.lock().unwrap() = Some(total);
        }

        fn add(&self, delta: u64) {
            let mut seen = self.seen.lock().unwrap();
            *seen += delta;
        }
    }

    #[tokio::test]
    async fn download_reader_reports_progress() -> Result<()> {
        let data = b"progress bytes".to_vec();
        let (mut tx, rx) = tokio::io::duplex(64);
        let data_clone = data.clone();
        tokio::spawn(async move {
            let _ = tx.write_all(&data_clone).await;
        });

        let progress = Arc::new(TestProgress {
            total: Mutex::new(None),
            seen: Mutex::new(0),
        });

        let dir = tempdir()?;
        let path = dir.path().join("progress.bin");
        download_reader_with_progress(rx, &path, data.len() as u64, Some(progress.clone())).await?;

        assert_eq!(*progress.total.lock().unwrap(), Some(data.len() as u64));
        assert_eq!(*progress.seen.lock().unwrap(), data.len() as u64);
        Ok(())
    }
}
