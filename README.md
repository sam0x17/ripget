# ripget

ripget is a fast, idempotent downloader that uses parallel HTTP range requests
to pull large files as quickly as possible. The default configuration uses
16 parallel ranges and 8MB buffers, similar in spirit to aria2c.

## Features
- Parallel range downloads with a preallocated file target
- Idempotent resumes using baked-in u128 marker bytes
- Interactive CLI progress bar in terminals
- Sensible defaults with simple overrides
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

When run in an interactive terminal, ripget shows a progress bar on stderr.

Override the output name:
```
ripget "https://example.com/assets/large.bin" my_file.blob
```

### Environment overrides
- `RIPGET_THREADS`: override the default parallel range count
- `RIPGET_USER_AGENT`: override the HTTP user agent

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

## Idempotent resume model
ripget bakes two random u128 values into the binary and treats their byte
sequence as a marker. Each write appends the marker at the end of the chunk,
and the next write overwrites it. When resuming, ripget scans each range for
the last marker and starts from that offset. If no marker is found for a
preexisting file, the range is assumed complete.

## Limitations
- The server must support HTTP range requests and report the full size.
- Very small files (smaller than the marker length) have limited resume value.
