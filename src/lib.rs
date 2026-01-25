//! Fast, multi-part downloads with a simple API.
//!
//! ripget prioritizes speed by downloading large files in parallel with HTTP
//! range requests.
//!
//! # Example
//! ```no_run
//! # #[tokio::main]
//! # async fn main() -> Result<(), ripget::RipgetError> {
//! let report = ripget::download_url("https://example.com/large.bin", "large.bin", None, None)
//!     .await?;
//! println!("downloaded {} bytes to {:?}", report.bytes, report.path);
//! # Ok(())
//! # }
//! ```

use std::cell::UnsafeCell;
use std::cmp;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::TryStreamExt;
use reqwest::header::{ACCEPT_ENCODING, CONTENT_RANGE, HeaderMap, HeaderValue, RANGE, RETRY_AFTER};
use reqwest::{Client, StatusCode};
use tokio::fs::OpenOptions;
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use tokio_util::io::StreamReader;

/// Default number of parallel ranges used by ripget.
pub const DEFAULT_THREADS: usize = 10;

/// Fixed read buffer size used for streaming data.
pub const BUFFER_SIZE: usize = 16 * 1024 * 1024;

const READ_IDLE_TIMEOUT: Duration = Duration::from_secs(15);
const RETRY_BASE_DELAY_MS: u64 = 1_000;
const RETRY_MAX_DELAY_MS: u64 = 30_000;
const RETRY_MAX_EXPONENT: usize = 10;

/// Result type for ripget operations.
pub type Result<T> = std::result::Result<T, RipgetError>;

/// Error type for ripget operations.
#[derive(Debug, thiserror::Error)]
pub enum RipgetError {
    #[error("invalid thread count: {0}")]
    InvalidThreadCount(usize),
    #[error("invalid buffer size: {0}")]
    InvalidBufferSize(usize),
    #[error("invalid window size: {0}")]
    InvalidWindowSize(u64),
    #[error("missing Content-Range header for {0}")]
    ContentRangeMissing(String),
    #[error("invalid Content-Range header for {0}")]
    InvalidContentRange(String),
    #[error("range requests are not supported by {0}")]
    RangeNotSupported(String),
    #[error("unexpected HTTP status {status} for {url}")]
    HttpStatus { status: StatusCode, url: String },
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
    /// Updates the active thread count.
    fn set_threads(&self, _threads: usize) {}
}

/// Shared progress reporter handle.
pub type Progress = Arc<dyn ProgressReporter>;

/// Configuration for URL downloads.
#[derive(Clone, Default)]
pub struct DownloadOptions {
    /// Override the number of parallel ranges.
    pub threads: Option<usize>,
    /// Override the HTTP user agent.
    pub user_agent: Option<String>,
    /// Optional progress reporter.
    pub progress: Option<Progress>,
    /// Override the read buffer size.
    pub buffer_size: Option<usize>,
}

impl DownloadOptions {
    /// Create a new options struct with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the number of parallel ranges.
    pub fn threads(mut self, threads: usize) -> Self {
        self.threads = Some(threads);
        self
    }

    /// Override the HTTP user agent.
    pub fn user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = Some(user_agent.into());
        self
    }

    /// Supply a progress reporter.
    pub fn progress(mut self, progress: Progress) -> Self {
        self.progress = Some(progress);
        self
    }

    /// Override the read buffer size.
    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = Some(buffer_size);
        self
    }
}

/// Configuration for windowed URL downloads.
#[derive(Clone)]
pub struct WindowedDownloadOptions {
    /// Size of the hot/cold window in bytes.
    pub window_size: u64,
    /// Override the number of parallel ranges.
    pub threads: Option<usize>,
    /// Override the HTTP user agent.
    pub user_agent: Option<String>,
    /// Optional progress reporter.
    pub progress: Option<Progress>,
    /// Override the read buffer size.
    pub buffer_size: Option<usize>,
}

impl WindowedDownloadOptions {
    /// Create a new options struct with the required window size.
    pub fn new(window_size: u64) -> Self {
        Self {
            window_size,
            threads: None,
            user_agent: None,
            progress: None,
            buffer_size: None,
        }
    }

    /// Override the number of parallel ranges.
    pub fn threads(mut self, threads: usize) -> Self {
        self.threads = Some(threads);
        self
    }

    /// Override the HTTP user agent.
    pub fn user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = Some(user_agent.into());
        self
    }

    /// Supply a progress reporter.
    pub fn progress(mut self, progress: Progress) -> Self {
        self.progress = Some(progress);
        self
    }

    /// Override the read buffer size.
    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = Some(buffer_size);
        self
    }
}

/// Information about a completed windowed stream download.
#[derive(Debug, Clone)]
pub struct StreamReport {
    /// URL that was streamed.
    pub url: String,
    /// Total bytes read.
    pub bytes: u64,
    /// Number of parallel ranges used.
    pub threads: usize,
}

/// Reader for a windowed URL download.
pub struct WindowedDownload {
    state: Arc<WindowState>,
    result: oneshot::Receiver<Result<StreamReport>>,
    expected_len: u64,
    threads: usize,
    next_seq: u64,
    current_idx: Option<usize>,
    current_offset: usize,
    current_len: usize,
    read_total: u64,
    wait: Option<Pin<Box<dyn std::future::Future<Output = ()> + Send>>>,
}

impl WindowedDownload {
    /// Total bytes expected from the stream.
    pub fn expected_len(&self) -> u64 {
        self.expected_len
    }

    /// Number of parallel ranges used.
    pub fn threads(&self) -> usize {
        self.threads
    }

    /// Wait for the background download to complete and return its report.
    pub async fn finish(self) -> Result<StreamReport> {
        match self.result.await {
            Ok(result) => result,
            Err(err) => Err(RipgetError::JoinError(err.to_string())),
        }
    }
}

impl AsyncRead for WindowedDownload {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        loop {
            if this.read_total >= this.expected_len {
                return Poll::Ready(Ok(()));
            }

            if let Some(idx) = this.current_idx {
                let remaining_in_buffer = this.current_len.saturating_sub(this.current_offset);
                if remaining_in_buffer == 0 {
                    this.state.states[idx].store(BUFFER_EMPTY, Ordering::Release);
                    this.state.notify.notify_waiters();
                    this.current_idx = None;
                    continue;
                }

                let to_copy = cmp::min(buf.remaining(), remaining_in_buffer);
                let end = this.current_offset + to_copy;
                let ptr = unsafe { this.state.buffers[idx].read_ptr(this.current_offset, end) };
                let chunk = unsafe { std::slice::from_raw_parts(ptr, to_copy) };
                buf.put_slice(chunk);
                this.current_offset = end;
                this.read_total += to_copy as u64;
                return Poll::Ready(Ok(()));
            }

            if this.wait.is_none() {
                let notify = this.state.notify.clone();
                this.wait = Some(Box::pin(async move {
                    notify.notified().await;
                }));
            }

            if let Some((idx, len)) = try_acquire_ready_buffer(this) {
                this.current_idx = Some(idx);
                this.current_offset = 0;
                this.current_len = len;
                this.wait = None;
                continue;
            }

            if this.state.done.load(Ordering::Acquire) {
                this.wait = None;
                return Poll::Ready(Ok(()));
            }

            if let Some(wait) = &mut this.wait {
                match wait.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        this.wait = None;
                        continue;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
    }
}

fn try_acquire_ready_buffer(download: &mut WindowedDownload) -> Option<(usize, usize)> {
    for idx in 0..download.state.buffers.len() {
        let state = download.state.states[idx].load(Ordering::Acquire);
        if state != BUFFER_READY {
            continue;
        }
        let seq = download.state.seqs[idx].load(Ordering::Acquire);
        if seq != download.next_seq {
            continue;
        }
        if download.state.states[idx]
            .compare_exchange(
                BUFFER_READY,
                BUFFER_READING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            continue;
        }
        let len = download.state.lens[idx].load(Ordering::Acquire) as usize;
        download.next_seq = download.next_seq.saturating_add(1);
        return Some((idx, len));
    }
    None
}

#[derive(Debug, Clone, Copy)]
struct Range {
    start: u64,
    end: u64,
}

#[derive(Debug, Clone, Copy)]
struct RemoteMetadata {
    len: u64,
}

const BUFFER_EMPTY: u8 = 0;
const BUFFER_WRITING: u8 = 1;
const BUFFER_READY: u8 = 2;
const BUFFER_READING: u8 = 3;

struct SharedCell<T> {
    inner: UnsafeCell<T>,
}

unsafe impl<T: Send> Send for SharedCell<T> {}
unsafe impl<T: Send> Sync for SharedCell<T> {}

impl<T> SharedCell<T> {
    fn new(value: T) -> Self {
        Self {
            inner: UnsafeCell::new(value),
        }
    }

    fn get(&self) -> *mut T {
        self.inner.get()
    }
}

struct SharedBuffer {
    data: Arc<SharedCell<Vec<u8>>>,
    len: usize,
}

impl Clone for SharedBuffer {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            len: self.len,
        }
    }
}

impl SharedBuffer {
    fn new(len: usize) -> Self {
        let data = vec![0; len];
        Self {
            data: Arc::new(SharedCell::new(data)),
            len,
        }
    }

    unsafe fn write_ptr(&self, start: usize, end: usize) -> *mut u8 {
        debug_assert!(start <= end);
        debug_assert!(end <= self.len);
        // SAFETY: Caller guarantees no overlapping mutable ranges. The buffer is
        // pre-sized and never reallocated while in use.
        unsafe { (*self.data.get()).as_mut_ptr().add(start) }
    }

    unsafe fn read_ptr(&self, start: usize, end: usize) -> *const u8 {
        debug_assert!(start <= end);
        debug_assert!(end <= self.len);
        // SAFETY: Reads are only performed after the buffer range is fully
        // written, and no other task mutates this range concurrently.
        unsafe { (*self.data.get()).as_ptr().add(start) }
    }
}

struct WindowState {
    buffers: [SharedBuffer; 2],
    states: [AtomicU8; 2],
    lens: [AtomicU64; 2],
    seqs: [AtomicU64; 2],
    notify: Arc<Notify>,
    done: AtomicBool,
}

impl WindowState {
    fn new(buffer_len: usize) -> Self {
        Self {
            buffers: [SharedBuffer::new(buffer_len), SharedBuffer::new(buffer_len)],
            states: [AtomicU8::new(BUFFER_EMPTY), AtomicU8::new(BUFFER_EMPTY)],
            lens: [AtomicU64::new(0), AtomicU64::new(0)],
            seqs: [AtomicU64::new(0), AtomicU64::new(0)],
            notify: Arc::new(Notify::new()),
            done: AtomicBool::new(false),
        }
    }
}

/// Download a URL to a file path using parallel range requests.
///
/// * `threads` defaults to 10 when `None`.
/// * `user_agent` defaults to `ripget/<version>` when `None`.
/// * Retries network failures with exponential backoff; 404/500 errors are fatal.
/// * Range reads that stall for 15 seconds reconnect automatically.
/// * Buffer size defaults to 16MB.
/// * Existing files at `dest` are truncated and overwritten.
///
/// Use [`download_url_with_progress`] to receive progress callbacks.
pub async fn download_url(
    url: impl AsRef<str>,
    dest: impl AsRef<Path>,
    threads: Option<usize>,
    user_agent: Option<&str>,
) -> Result<DownloadReport> {
    download_url_with_progress(url, dest, threads, user_agent, None, None).await
}

/// Download a URL to a file path using parallel range requests with options.
///
/// This is a convenience wrapper around [`download_url_with_progress`] that
/// accepts owned option values like user agents.
pub async fn download_url_with_options(
    url: impl AsRef<str>,
    dest: impl AsRef<Path>,
    options: DownloadOptions,
) -> Result<DownloadReport> {
    download_url_with_progress(
        url,
        dest,
        options.threads,
        options.user_agent.as_deref(),
        options.progress,
        options.buffer_size,
    )
    .await
}

/// Download a URL as a sequential reader using a hot/cold window.
///
/// This streams the response using two in-memory buffers sized at
/// `window_size / 2`. While one buffer is streamed to the reader, the next
/// buffer is downloaded with the configured parallel range requests.
/// The reader consumes directly from the cold buffer; once drained it waits
/// for the hot buffer to complete and then swaps without extra copies.
///
/// * `window_size` must be at least 2 bytes; buffers are sized at
///   `window_size / 2` (integer division).
/// * `threads` defaults to 10 when `None`.
/// * `user_agent` defaults to `ripget/<version>` when `None`.
/// * `buffer_size` defaults to 16MB when `None`.
///
/// The returned [`WindowedDownload`] implements [`AsyncRead`]. Call
/// [`WindowedDownload::finish`] to observe the final download status.
pub async fn download_url_windowed(
    url: impl AsRef<str>,
    options: WindowedDownloadOptions,
) -> Result<WindowedDownload> {
    let url = url.as_ref().to_string();
    let buffer_len = normalize_window_size(options.window_size)?;
    let requested_threads = normalize_threads(options.threads)?;
    let buffer_size = normalize_buffer_size(options.buffer_size)?;
    let client = build_client(options.user_agent.as_deref())?;
    let metadata = fetch_metadata(&client, &url).await?;
    progress_init(&options.progress, metadata.len);

    let threads = if metadata.len == 0 {
        0
    } else {
        clamp_threads(requested_threads, metadata.len)
    };
    progress_set_threads(&options.progress, threads);
    let (result_tx, result_rx) = oneshot::channel();
    let state = Arc::new(WindowState::new(buffer_len));

    if metadata.len == 0 {
        let _ = result_tx.send(Ok(StreamReport {
            url,
            bytes: 0,
            threads: 0,
        }));
        return Ok(WindowedDownload {
            state,
            result: result_rx,
            expected_len: 0,
            threads: 0,
            next_seq: 0,
            current_idx: None,
            current_offset: 0,
            current_len: 0,
            read_total: 0,
            wait: None,
        });
    }

    let progress = options.progress.clone();
    let state_task = state.clone();

    tokio::spawn(async move {
        let result = run_windowed_download(
            client,
            url,
            metadata.len,
            buffer_len,
            threads,
            progress,
            buffer_size,
            state_task,
        )
        .await;
        let _ = result_tx.send(result);
    });

    Ok(WindowedDownload {
        state,
        result: result_rx,
        expected_len: metadata.len,
        threads,
        next_seq: 0,
        current_idx: None,
        current_offset: 0,
        current_len: 0,
        read_total: 0,
        wait: None,
    })
}

/// Download a URL to a file path using parallel range requests with progress.
///
/// * `threads` defaults to 10 when `None`.
/// * `user_agent` defaults to `ripget/<version>` when `None`.
/// * Retries network failures with exponential backoff; 404/500 errors are fatal.
/// * Range reads that stall for 15 seconds reconnect automatically.
/// * `buffer_size` defaults to 16MB when `None`.
/// * Existing files at `dest` are truncated and overwritten.
pub async fn download_url_with_progress(
    url: impl AsRef<str>,
    dest: impl AsRef<Path>,
    threads: Option<usize>,
    user_agent: Option<&str>,
    progress: Option<Progress>,
    buffer_size: Option<usize>,
) -> Result<DownloadReport> {
    let url = url.as_ref();
    let dest = dest.as_ref().to_path_buf();
    let requested_threads = normalize_threads(threads)?;
    let buffer_size = normalize_buffer_size(buffer_size)?;
    let client = build_client(user_agent)?;
    let metadata = fetch_metadata(&client, url).await?;
    progress_init(&progress, metadata.len);
    if metadata.len == 0 {
        prepare_file(&dest, 0).await?;
        progress_set_threads(&progress, 0);
        return Ok(DownloadReport {
            url: Some(url.to_string()),
            path: dest,
            bytes: 0,
            threads: 0,
        });
    }

    prepare_file(&dest, metadata.len).await?;

    let threads = clamp_threads(requested_threads, metadata.len);
    progress_set_threads(&progress, threads);
    let ranges = split_ranges(metadata.len, threads);

    let mut join_set = JoinSet::new();
    for range in ranges {
        let client = client.clone();
        let url = url.to_string();
        let dest = dest.clone();
        let progress = progress.clone();
        let allow_full_body = threads == 1;
        join_set.spawn(async move {
            download_range(
                &client,
                &url,
                &dest,
                range,
                progress,
                buffer_size,
                allow_full_body,
            )
            .await
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

/// Copy an async reader into a file path.
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
    download_reader_with_progress(reader, dest, expected_len, None, None).await
}

/// Copy an async reader into a file path.
///
/// This uses a single range and requires the expected length up front.
///
/// * `buffer_size` defaults to 16MB when `None`.
/// * Existing files at `dest` are truncated and overwritten.
pub async fn download_reader_with_progress<R>(
    mut reader: R,
    dest: impl AsRef<Path>,
    expected_len: u64,
    progress: Option<Progress>,
    buffer_size: Option<usize>,
) -> Result<DownloadReport>
where
    R: AsyncRead + Unpin,
{
    let dest = dest.as_ref().to_path_buf();
    let buffer_size = normalize_buffer_size(buffer_size)?;
    progress_init(&progress, expected_len);
    progress_set_threads(&progress, 1);
    if expected_len == 0 {
        prepare_file(&dest, 0).await?;
        return Ok(DownloadReport {
            url: None,
            path: dest,
            bytes: 0,
            threads: 1,
        });
    }

    prepare_file(&dest, expected_len).await?;

    let range = Range {
        start: 0,
        end: expected_len - 1,
    };
    let mut offset = range.start;
    write_range_from_reader(
        &mut reader,
        &dest,
        range,
        &mut offset,
        &progress,
        buffer_size,
        None,
    )
    .await?;

    Ok(DownloadReport {
        url: None,
        path: dest,
        bytes: expected_len,
        threads: 1,
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_windowed_download(
    client: Client,
    url: String,
    total_len: u64,
    buffer_len: usize,
    threads: usize,
    progress: Option<Progress>,
    buffer_size: usize,
    state: Arc<WindowState>,
) -> Result<StreamReport> {
    let download_result = async {
        let mut offset = 0u64;
        let mut idx = 0usize;
        let mut seq = 0u64;
        while offset < total_len {
            loop {
                let notified = state.notify.notified();
                if state.states[idx]
                    .compare_exchange(
                        BUFFER_EMPTY,
                        BUFFER_WRITING,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    break;
                }
                notified.await;
            }

            let remaining = total_len - offset;
            let chunk_len = remaining.min(buffer_len as u64);
            let buffer = state.buffers[idx].clone();
            let allow_full_body = offset == 0 && chunk_len == total_len;

            download_window_in_memory(
                &client,
                &url,
                buffer,
                offset,
                chunk_len,
                threads,
                progress.clone(),
                buffer_size,
                allow_full_body,
            )
            .await?;

            state.lens[idx].store(chunk_len, Ordering::Release);
            state.seqs[idx].store(seq, Ordering::Release);
            state.states[idx].store(BUFFER_READY, Ordering::Release);
            state.notify.notify_waiters();

            offset += chunk_len;
            idx = (idx + 1) % state.buffers.len();
            seq = seq.saturating_add(1);
        }
        Ok::<(), RipgetError>(())
    }
    .await;

    state.done.store(true, Ordering::Release);
    state.notify.notify_waiters();

    download_result?;

    Ok(StreamReport {
        url,
        bytes: total_len,
        threads,
    })
}

fn normalize_threads(threads: Option<usize>) -> Result<usize> {
    let threads = threads.unwrap_or(DEFAULT_THREADS);
    if threads == 0 {
        return Err(RipgetError::InvalidThreadCount(threads));
    }
    Ok(threads)
}

fn normalize_buffer_size(buffer_size: Option<usize>) -> Result<usize> {
    let buffer_size = buffer_size.unwrap_or(BUFFER_SIZE);
    if buffer_size == 0 {
        return Err(RipgetError::InvalidBufferSize(buffer_size));
    }
    Ok(buffer_size)
}

fn normalize_window_size(window_size: u64) -> Result<usize> {
    if window_size < 2 {
        return Err(RipgetError::InvalidWindowSize(window_size));
    }
    let half = window_size / 2;
    if half == 0 {
        return Err(RipgetError::InvalidWindowSize(window_size));
    }
    let half = usize::try_from(half).map_err(|_| RipgetError::InvalidWindowSize(window_size))?;
    if half == 0 {
        return Err(RipgetError::InvalidWindowSize(window_size));
    }
    Ok(half)
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
    let mut attempt = 0usize;
    loop {
        let response = match client.get(url).header(RANGE, "bytes=0-0").send().await {
            Ok(resp) => resp,
            Err(err) => {
                if !is_retryable_reqwest_error(&err) {
                    return Err(RipgetError::Http(err));
                }
                sleep_with_backoff(attempt, None).await;
                attempt = attempt.saturating_add(1);
                continue;
            }
        };

        let status = response.status();
        if is_fatal_status(status) {
            return Err(RipgetError::HttpStatus {
                status,
                url: url.to_string(),
            });
        }
        if status != StatusCode::PARTIAL_CONTENT {
            let retry_after = retry_after_delay(response.headers());
            sleep_with_backoff(attempt, retry_after).await;
            attempt = attempt.saturating_add(1);
            continue;
        }

        let content_range = response
            .headers()
            .get(CONTENT_RANGE)
            .ok_or_else(|| RipgetError::ContentRangeMissing(url.to_string()))?;
        let total_len = parse_content_range_total(content_range, url)?;

        return Ok(RemoteMetadata { len: total_len });
    }
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
        .truncate(true)
        .read(true)
        .write(true)
        .open(path)
        .await?;
    file.set_len(size).await?;
    Ok(())
}

async fn download_range(
    client: &Client,
    url: &str,
    path: &Path,
    range: Range,
    progress: Option<Progress>,
    buffer_size: usize,
    allow_full_body: bool,
) -> Result<()> {
    let mut offset = range.start;

    let mut attempt = 0usize;
    loop {
        if offset > range.end {
            return Ok(());
        }

        let response = match client
            .get(url)
            .header(RANGE, format!("bytes={}-{}", offset, range.end))
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                if !is_retryable_reqwest_error(&err) {
                    return Err(RipgetError::Http(err));
                }
                sleep_with_backoff(attempt, None).await;
                attempt = attempt.saturating_add(1);
                continue;
            }
        };

        let status = response.status();
        if is_fatal_status(status) {
            return Err(RipgetError::HttpStatus {
                status,
                url: url.to_string(),
            });
        }
        if status != StatusCode::PARTIAL_CONTENT {
            if status == StatusCode::OK
                && allow_full_body
                && offset == range.start
                && range.start == 0
            {
                // Server ignored the range header for the full-file request.
            } else {
                let retry_after = retry_after_delay(response.headers());
                sleep_with_backoff(attempt, retry_after).await;
                attempt = attempt.saturating_add(1);
                continue;
            }
        }

        let stream = response.bytes_stream().map_err(io::Error::other);
        let mut reader = StreamReader::new(stream);
        let start_offset = offset;
        match write_range_from_reader(
            &mut reader,
            path,
            range,
            &mut offset,
            &progress,
            buffer_size,
            Some(READ_IDLE_TIMEOUT),
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(err) if is_retryable_error(&err) => {
                if offset > start_offset {
                    attempt = 0;
                }
                sleep_with_backoff(attempt, None).await;
                attempt = attempt.saturating_add(1);
                continue;
            }
            Err(err) => return Err(err),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn download_window_in_memory(
    client: &Client,
    url: &str,
    buffer: SharedBuffer,
    window_start: u64,
    window_len: u64,
    threads: usize,
    progress: Option<Progress>,
    buffer_size: usize,
    allow_full_body: bool,
) -> Result<()> {
    if window_len == 0 {
        return Ok(());
    }

    let threads = clamp_threads(threads, window_len);
    let ranges = split_ranges(window_len, threads);
    let mut join_set = JoinSet::new();
    for range in ranges {
        let client = client.clone();
        let url = url.to_string();
        let buffer = buffer.clone();
        let progress = progress.clone();
        let request_range = Range {
            start: window_start + range.start,
            end: window_start + range.end,
        };
        join_set.spawn(async move {
            download_range_window_to_buffer(
                &client,
                &url,
                &buffer,
                range,
                request_range,
                progress,
                buffer_size,
                allow_full_body,
            )
            .await
        });
    }

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(inner) => inner?,
            Err(err) => return Err(RipgetError::JoinError(err.to_string())),
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn download_range_window_to_buffer(
    client: &Client,
    url: &str,
    buffer: &SharedBuffer,
    file_range: Range,
    request_range: Range,
    progress: Option<Progress>,
    buffer_size: usize,
    allow_full_body: bool,
) -> Result<()> {
    let mut offset = file_range.start;
    let mut attempt = 0usize;
    loop {
        if offset > file_range.end {
            return Ok(());
        }

        let request_start = request_range.start + (offset - file_range.start);
        let response = match client
            .get(url)
            .header(
                RANGE,
                format!("bytes={}-{}", request_start, request_range.end),
            )
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                if !is_retryable_reqwest_error(&err) {
                    return Err(RipgetError::Http(err));
                }
                sleep_with_backoff(attempt, None).await;
                attempt = attempt.saturating_add(1);
                continue;
            }
        };

        let status = response.status();
        if is_fatal_status(status) {
            return Err(RipgetError::HttpStatus {
                status,
                url: url.to_string(),
            });
        }
        if status != StatusCode::PARTIAL_CONTENT {
            if status == StatusCode::OK
                && allow_full_body
                && request_range.start == 0
                && offset == file_range.start
                && file_range.start == 0
            {
                // Server ignored the range header for the full-file request.
            } else {
                let retry_after = retry_after_delay(response.headers());
                sleep_with_backoff(attempt, retry_after).await;
                attempt = attempt.saturating_add(1);
                continue;
            }
        }

        let stream = response.bytes_stream().map_err(io::Error::other);
        let mut reader = StreamReader::new(stream);
        let start_offset = offset;
        match write_range_from_reader_to_buffer(
            &mut reader,
            buffer,
            file_range,
            &mut offset,
            &progress,
            buffer_size,
            Some(READ_IDLE_TIMEOUT),
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(err) if is_retryable_error(&err) => {
                if offset > start_offset {
                    attempt = 0;
                }
                sleep_with_backoff(attempt, None).await;
                attempt = attempt.saturating_add(1);
                continue;
            }
            Err(err) => return Err(err),
        }
    }
}

async fn write_range_from_reader<R: AsyncRead + Unpin>(
    reader: &mut R,
    path: &Path,
    range: Range,
    offset: &mut u64,
    progress: &Option<Progress>,
    buffer_size: usize,
    idle_timeout: Option<Duration>,
) -> Result<()> {
    let expected = range.end - *offset + 1;
    let mut remaining = expected;
    let mut file = OpenOptions::new().read(true).write(true).open(path).await?;
    let mut buf = vec![0u8; buffer_size];

    while remaining > 0 {
        let read_len = cmp::min(remaining as usize, buffer_size);
        let n = read_fully_with_timeout(reader, &mut buf[..read_len], idle_timeout).await?;
        if n == 0 {
            break;
        }

        file.seek(SeekFrom::Start(*offset)).await?;
        file.write_all(&buf[..n]).await?;
        progress_add(progress, n as u64);
        *offset += n as u64;
        remaining -= n as u64;
    }

    if remaining != 0 {
        let got = expected - remaining;
        return Err(RipgetError::UnexpectedEof { expected, got });
    }
    Ok(())
}

async fn write_range_from_reader_to_buffer<R: AsyncRead + Unpin>(
    reader: &mut R,
    buffer: &SharedBuffer,
    range: Range,
    offset: &mut u64,
    progress: &Option<Progress>,
    buffer_size: usize,
    idle_timeout: Option<Duration>,
) -> Result<()> {
    let expected = range.end - *offset + 1;
    let mut remaining = expected;

    while remaining > 0 {
        let read_len = cmp::min(remaining as usize, buffer_size);
        let start = *offset as usize;
        let end = start + read_len;
        let ptr = unsafe { buffer.write_ptr(start, end) };
        let slice = unsafe { std::slice::from_raw_parts_mut(ptr, read_len) };
        let n = read_fully_with_timeout(reader, slice, idle_timeout).await?;
        if n == 0 {
            break;
        }
        progress_add(progress, n as u64);
        *offset += n as u64;
        remaining -= n as u64;
    }

    if remaining != 0 {
        let got = expected - remaining;
        return Err(RipgetError::UnexpectedEof { expected, got });
    }
    Ok(())
}

async fn read_with_timeout<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
    idle_timeout: Option<Duration>,
) -> io::Result<usize> {
    match idle_timeout {
        Some(duration) => match timeout(duration, reader.read(buf)).await {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(io::ErrorKind::TimedOut, "read timed out")),
        },
        None => reader.read(buf).await,
    }
}

async fn read_fully_with_timeout<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
    idle_timeout: Option<Duration>,
) -> io::Result<usize> {
    let mut read_total = 0usize;
    while read_total < buf.len() {
        let n = read_with_timeout(reader, &mut buf[read_total..], idle_timeout).await?;
        if n == 0 {
            break;
        }
        read_total += n;
    }
    Ok(read_total)
}

fn progress_init(progress: &Option<Progress>, total: u64) {
    if let Some(progress) = progress {
        progress.init(total);
    }
}

fn progress_set_threads(progress: &Option<Progress>, threads: usize) {
    if let Some(progress) = progress {
        progress.set_threads(threads);
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

fn is_fatal_status(status: StatusCode) -> bool {
    status == StatusCode::NOT_FOUND || status == StatusCode::INTERNAL_SERVER_ERROR
}

fn retry_after_delay(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?;
    let seconds = value.parse::<u64>().ok()?;
    Some(Duration::from_secs(seconds))
}

fn backoff_delay(attempt: usize) -> Duration {
    let exp = attempt.min(RETRY_MAX_EXPONENT);
    let factor = 1u64.checked_shl(exp as u32).unwrap_or(u64::MAX);
    let delay = RETRY_BASE_DELAY_MS.saturating_mul(factor);
    Duration::from_millis(cmp::min(delay, RETRY_MAX_DELAY_MS))
}

async fn sleep_with_backoff(attempt: usize, retry_after: Option<Duration>) {
    let backoff = backoff_delay(attempt);
    let delay = retry_after
        .map(|value| value.max(backoff))
        .unwrap_or(backoff);
    sleep(delay).await;
}

fn is_retryable_error(err: &RipgetError) -> bool {
    match err {
        RipgetError::UnexpectedEof { .. } => true,
        RipgetError::Io(err) => matches!(
            err.kind(),
            io::ErrorKind::TimedOut
                | io::ErrorKind::Interrupted
                | io::ErrorKind::WouldBlock
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::BrokenPipe
                | io::ErrorKind::NotConnected
                | io::ErrorKind::Other
        ),
        RipgetError::Http(_) => true,
        _ => false,
    }
}

fn is_retryable_reqwest_error(err: &reqwest::Error) -> bool {
    !err.is_builder()
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, RANGE, USER_AGENT};
    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Method, Request, Response, Server, StatusCode};
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::oneshot;
    use tokio::time::{Duration, sleep};

    fn handle_request(req: Request<Body>, data: Arc<Vec<u8>>) -> Response<Body> {
        match *req.method() {
            Method::HEAD => Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_LENGTH, data.len().to_string())
                .header(ACCEPT_RANGES, "bytes")
                .body(Body::empty())
                .unwrap(),
            Method::GET => {
                if let Some(range) = req.headers().get(RANGE)
                    && let Ok(range_str) = range.to_str()
                    && let Some((start, end)) = parse_range_header(range_str, data.len())
                {
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
    async fn download_url_sets_user_agent() -> Result<()> {
        let data: Vec<u8> = (0..(1024 * 128)).map(|i| (i % 251) as u8).collect();
        let data = Arc::new(data);
        let expected = Arc::new("ripget-test/1.0".to_string());
        let mismatch = Arc::new(AtomicBool::new(false));
        let seen = Arc::new(AtomicUsize::new(0));

        let data_for_svc = data.clone();
        let expected_for_svc = expected.clone();
        let mismatch_for_svc = mismatch.clone();
        let seen_for_svc = seen.clone();

        let make_svc = make_service_fn(move |_| {
            let data = data_for_svc.clone();
            let expected = expected_for_svc.clone();
            let mismatch = mismatch_for_svc.clone();
            let seen = seen_for_svc.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |req| {
                    let data = data.clone();
                    let expected = expected.clone();
                    let mismatch = mismatch.clone();
                    let seen = seen.clone();
                    async move {
                        let ua = req.headers().get(USER_AGENT).and_then(|v| v.to_str().ok());
                        if ua != Some(expected.as_str()) {
                            mismatch.store(true, Ordering::Relaxed);
                        }
                        seen.fetch_add(1, Ordering::Relaxed);
                        Ok::<_, Infallible>(handle_request(req, data))
                    }
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

        let dir = tempdir()?;
        let path = dir.path().join("ua.bin");
        let url = format!("http://{}/ua.bin", addr);

        let options = DownloadOptions::new().user_agent(expected.as_str());
        let report = download_url_with_options(&url, &path, options).await?;
        assert_eq!(report.bytes as usize, data.len());

        assert!(!mismatch.load(Ordering::Relaxed));
        assert!(seen.load(Ordering::Relaxed) > 0);

        let _ = tx.send(());
        Ok(())
    }

    #[tokio::test]
    async fn download_url_windowed_streams() -> Result<()> {
        let data: Vec<u8> = (0..(256 * 1024)).map(|i| (i % 251) as u8).collect();
        let (addr, shutdown) = spawn_range_server(Arc::new(data.clone())).await;

        let url = format!("http://{}/file.bin", addr);
        let options = WindowedDownloadOptions::new(64 * 1024).threads(4);
        let mut download = download_url_windowed(&url, options).await?;
        let mut received = Vec::new();
        download.read_to_end(&mut received).await?;
        let report = download.finish().await?;

        assert_eq!(received, data);
        assert_eq!(report.bytes as usize, data.len());

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn download_url_windowed_reports_progress() -> Result<()> {
        let data: Vec<u8> = (0..(128 * 1024)).map(|i| (i % 251) as u8).collect();
        let (addr, shutdown) = spawn_range_server(Arc::new(data.clone())).await;

        let progress = Arc::new(TestProgress {
            total: Mutex::new(None),
            seen: Mutex::new(0),
        });

        let url = format!("http://{}/file.bin", addr);
        let options = WindowedDownloadOptions::new(64 * 1024)
            .threads(4)
            .progress(progress.clone());
        let mut download = download_url_windowed(&url, options).await?;
        let mut received = Vec::new();
        download.read_to_end(&mut received).await?;
        let report = download.finish().await?;

        assert_eq!(received, data);
        assert_eq!(report.bytes as usize, data.len());
        assert_eq!(*progress.total.lock().unwrap(), Some(data.len() as u64));
        assert_eq!(*progress.seen.lock().unwrap(), data.len() as u64);

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn download_url_windowed_is_sequential() -> Result<()> {
        let data: Vec<u8> = (0..(192 * 1024 + 123)).map(|i| (i % 251) as u8).collect();
        let (addr, shutdown) = spawn_range_server(Arc::new(data.clone())).await;

        let url = format!("http://{}/file.bin", addr);
        let options = WindowedDownloadOptions::new(64 * 1024).threads(4);
        let mut download = download_url_windowed(&url, options).await?;

        let mut offset = 0usize;
        let mut buf = vec![0u8; 1537];
        loop {
            let n = download.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            let expected = &data[offset..offset + n];
            assert_eq!(&buf[..n], expected);
            offset += n;
        }
        assert_eq!(offset, data.len());
        let report = download.finish().await?;
        assert_eq!(report.bytes as usize, data.len());

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn download_url_windowed_many_swaps_is_sequential() -> Result<()> {
        let data: Vec<u8> = (0..(64 * 1024 + 333)).map(|i| (i % 251) as u8).collect();
        let (addr, shutdown) = spawn_range_server(Arc::new(data.clone())).await;

        let url = format!("http://{}/file.bin", addr);
        let options = WindowedDownloadOptions::new(8 * 1024).threads(4);
        let mut download = download_url_windowed(&url, options).await?;

        let mut offset = 0usize;
        let mut buf = vec![0u8; 1025];
        loop {
            let n = download.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            let expected = &data[offset..offset + n];
            assert_eq!(&buf[..n], expected);
            offset += n;
        }
        assert_eq!(offset, data.len());
        let report = download.finish().await?;
        assert_eq!(report.bytes as usize, data.len());

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn download_url_windowed_slow_reader_is_sequential() -> Result<()> {
        let data: Vec<u8> = (0..(128 * 1024 + 7)).map(|i| (i % 251) as u8).collect();
        let (addr, shutdown) = spawn_range_server(Arc::new(data.clone())).await;

        let url = format!("http://{}/file.bin", addr);
        let options = WindowedDownloadOptions::new(64 * 1024).threads(4);
        let mut download = download_url_windowed(&url, options).await?;

        let mut offset = 0usize;
        let mut buf = vec![0u8; 512];
        loop {
            let n = download.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            let expected = &data[offset..offset + n];
            assert_eq!(&buf[..n], expected);
            offset += n;
            sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(offset, data.len());
        let report = download.finish().await?;
        assert_eq!(report.bytes as usize, data.len());

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn download_url_windowed_rejects_small_window() {
        let options = WindowedDownloadOptions::new(1);
        let result = download_url_windowed("http://example.com/file.bin", options).await;
        assert!(matches!(result, Err(RipgetError::InvalidWindowSize(1))));
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
        download_reader_with_progress(rx, &path, data.len() as u64, Some(progress.clone()), None)
            .await?;

        assert_eq!(*progress.total.lock().unwrap(), Some(data.len() as u64));
        assert_eq!(*progress.seen.lock().unwrap(), data.len() as u64);
        Ok(())
    }
}
