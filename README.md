# ripget

[![Crates.io](https://img.shields.io/crates/v/ripget.svg)](https://crates.io/crates/ripget)
[![Docs.rs](https://img.shields.io/docsrs/ripget.svg)](https://docs.rs/ripget)
[![CI](https://github.com/sam0x17/ripget/actions/workflows/ci.yaml/badge.svg)](https://github.com/sam0x17/ripget/actions/workflows/ci.yaml)

ripget is a fast downloader that uses parallel HTTP range requests to pull large files as
quickly as possible. The default configuration uses 10 parallel ranges and 16MB buffers,
similar in spirit to aria2c.

## Features
- Download files as fast as possible using HTTP multiplexing
- Overwrites existing output files by default
- Interactive CLI progress bar in terminals
- Automatic retry with exponential backoff for network throttling or disconnects
- Per-range idle timeout reconnects after 15 seconds without data
- Configurable parallelism with simple overrides
- Windowed streaming mode for sequential consumers
- Async library API powered by tokio and reqwest

## Install
```
cargo install ripget
```

## CLI usage
Download to the current directory using the URL filename:
```
ripget "https://example.com/assets/large.bin"
```

When run in an interactive terminal, ripget shows a progress bar on stderr. Use `--silent` to
disable the progress bar. Use `--threads` or `RIPGET_THREADS` to override the default thread
count.

Override the buffer size:
```
ripget --cache-size 8mb "https://example.com/assets/large.bin"
```

Override the output name:
```
ripget "https://example.com/assets/large.bin" my_file.blob
```

### Environment overrides
- `RIPGET_THREADS`: override the default parallel range count
- `RIPGET_USER_AGENT`: override the HTTP user agent
- `RIPGET_CACHE_SIZE`: override the read buffer size (e.g. `8mb`)

### CLI options
- `--threads <N>`: override the default parallel range count
- `--user-agent <UA>`: override the HTTP user agent
- `--silent`: disable the progress bar
- `--cache-size <SIZE>`: override the read buffer size (e.g. `8mb`)

## Library usage
```
use ripget::download_url;

# #[tokio::main]
# async fn main() -> Result<(), ripget::RipgetError> {
let report = download_url(
    "https://example.com/assets/large.bin",
    "large.bin",
    None,
    None,
).await?;
println!("downloaded {} bytes", report.bytes);
# Ok(())
# }
```

Override the user agent programmatically:
```
let options = ripget::DownloadOptions::new()
    .user_agent(format!("my-app/{}", env!("CARGO_PKG_VERSION")));
let report = ripget::download_url_with_options(
    "https://example.com/assets/large.bin",
    "large.bin",
    options,
).await?;
println!("downloaded {} bytes", report.bytes);
```

Windowed streaming (double-buffered range download):
```
# #[tokio::main]
# async fn main() -> Result<(), ripget::RipgetError> {
let options = ripget::WindowedDownloadOptions::new(10 * 1024 * 1024)
    .threads(8);
let mut stream = ripget::download_url_windowed(
    "https://example.com/assets/large.bin",
    options,
).await?;
let mut file = tokio::fs::File::create("large.bin").await?;
tokio::io::copy(&mut stream, &mut file).await?;
let report = stream.finish().await?;
println!("streamed {} bytes", report.bytes);
# Ok(())
# }
```
Windowed streaming uses two in-memory buffers sized at `window_size / 2` (total resident memory
~= `window_size`, plus HTTP read buffers). The reader consumes directly from the current cold
buffer; when it drains, it waits for the hot buffer to finish and then swaps without extra
copies. If the stream is dropped or `finish()` is called early, the background download is
cancelled and `finish()` returns a report for the bytes read.

For async readers with a known length:
```
use tokio::io::{self, AsyncWriteExt};

# #[tokio::main]
# async fn main() -> Result<(), ripget::RipgetError> {
let data = b"hello from a stream".to_vec();
let (mut tx, rx) = io::duplex(64);
tokio::spawn(async move {
    let _ = tx.write_all(&data).await;
});
let report = ripget::download_reader(rx, "out.bin", data.len() as u64).await?;
println!("downloaded {} bytes", report.bytes);
# Ok(())
# }
```

## Retry behavior
ripget retries network failures and most HTTP statuses with exponential backoff to handle
throttling or transient outages. Only 404 and 500 responses are treated as fatal. Each range
reconnects if no data arrives for 15 seconds. If the server does not support range requests,
ripget logs a warning and falls back to a single-threaded download.

## Limitations
- The server should report the full size. When range requests are unsupported, ripget falls
  back to a single-threaded download.

## License
Licensed under either of:
- Apache License, Version 2.0 (`LICENSE-APACHE`)
- MIT license (`LICENSE-MIT`)
