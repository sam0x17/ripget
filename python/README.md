# ripget

Python bindings for [ripget](https://github.com/sam0x17/ripget) - a fast multi-part downloader with S3/R2 support.

## Installation

```bash
uv pip install ripget
# or
pip install ripget
```

Or build from source:

```bash
cd python
uv venv
source .venv/bin/activate
uv pip install maturin
maturin develop
```

## Usage

### Simple Download

```python
import asyncio
from ripget import download

async def main():
    report = await download("https://example.com/large.bin", "output.bin")
    print(f"Downloaded {report.bytes} bytes using {report.threads} threads")

asyncio.run(main())
```

### Download from R2/S3 with Credentials

```python
import asyncio
from ripget import download, S3Credentials

async def main():
    creds = S3Credentials(
        access_key_id="your-access-key",
        secret_access_key="your-secret-key",
        endpoint="https://account.r2.cloudflarestorage.com",
    )

    # Download using bucket/key path
    report = await download(
        "my-bucket/path/to/file.bin",
        "output.bin",
        credentials=creds,
        threads=10,  # optional, default is 10
    )
    print(f"Downloaded {report.bytes} bytes")

asyncio.run(main())
```

## API Reference

### `S3Credentials`

Credentials for S3-compatible storage (R2, S3, MinIO, etc.)

```python
S3Credentials(
    access_key_id: str,
    secret_access_key: str,
    endpoint: str,
    region: str | None = None,  # defaults to "auto" for R2
)
```

### `download()`

```python
async def download(
    url: str,
    dest: str,
    credentials: S3Credentials | None = None,
    threads: int | None = None,  # default: 10
    buffer_size: int | None = None,  # default: 16MB
) -> DownloadReport
```

### `DownloadReport`

Result of a completed download with the following attributes:

- `url: str | None` - URL that was downloaded
- `path: str` - Destination path on disk
- `bytes: int` - Total bytes written
- `threads: int` - Number of parallel threads used

## License

MIT OR Apache-2.0

