"""Python bindings for ripget - fast multi-part downloader with S3/R2 support."""

from typing import Optional

class S3Credentials:
    """Credentials for S3-compatible storage (R2, S3, MinIO, etc.)

    Example:
        ```python
        creds = S3Credentials(
            access_key_id="...",
            secret_access_key="...",
            endpoint="https://account.r2.cloudflarestorage.com",
        )
        ```
    """

    access_key_id: str
    secret_access_key: str
    endpoint: str
    region: str

    def __init__(
        self,
        access_key_id: str,
        secret_access_key: str,
        endpoint: str,
        region: Optional[str] = None,
    ) -> None:
        """Create new S3 credentials.

        Args:
            access_key_id: AWS/S3 access key ID.
            secret_access_key: AWS/S3 secret access key.
            endpoint: S3-compatible endpoint URL (e.g., https://account.r2.cloudflarestorage.com).
            region: AWS region. Defaults to "auto" for R2.
        """
        ...

class DownloadReport:
    """Result of a completed download."""

    url: Optional[str]
    """URL that was downloaded (if HTTP source)."""

    path: str
    """Destination path on disk."""

    bytes: int
    """Total bytes written."""

    threads: int
    """Number of parallel threads used."""

async def download(
    url: str,
    dest: str,
    credentials: Optional[S3Credentials] = None,
    threads: Optional[int] = None,
    buffer_size: Optional[int] = None,
) -> DownloadReport:
    """Download a file from a URL with optional S3/R2 credentials.

    Args:
        url: The URL or bucket/key path to download.
        dest: Destination file path.
        credentials: Optional S3Credentials for authenticated downloads.
        threads: Number of parallel download threads (default: 10).
        buffer_size: Read buffer size in bytes (default: 16MB).

    Returns:
        DownloadReport with download statistics.

    Example:
        ```python
        import asyncio
        from ripget import download, S3Credentials

        async def main():
            # Simple public URL download
            report = await download("https://example.com/file.bin", "output.bin")
            print(f"Downloaded {report.bytes} bytes")

            # With S3/R2 credentials
            creds = S3Credentials(
                access_key_id="...",
                secret_access_key="...",
                endpoint="https://account.r2.cloudflarestorage.com",
            )
            report = await download("bucket/key", "output.bin", credentials=creds)

        asyncio.run(main())
        ```
    """
    ...

