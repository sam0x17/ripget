//! Python bindings for ripget - fast multi-part downloader with S3/R2 support.

use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;

/// Credentials for S3-compatible storage (R2, S3, MinIO, etc.)
///
/// Example:
///     ```python
///     creds = S3Credentials(
///         access_key_id="...",
///         secret_access_key="...",
///         endpoint="https://account.r2.cloudflarestorage.com",
///     )
///     ```
#[pyclass(name = "S3Credentials")]
#[derive(Clone)]
pub struct PyS3Credentials {
    inner: ripget::S3Credentials,
}

#[pymethods]
impl PyS3Credentials {
    #[new]
    #[pyo3(signature = (access_key_id, secret_access_key, endpoint, region=None))]
    fn new(
        access_key_id: String,
        secret_access_key: String,
        endpoint: String,
        region: Option<String>,
    ) -> Self {
        let inner = match region {
            Some(r) => {
                ripget::S3Credentials::with_region(access_key_id, secret_access_key, endpoint, r)
            }
            None => ripget::S3Credentials::new(access_key_id, secret_access_key, endpoint),
        };
        Self { inner }
    }

    #[getter]
    fn access_key_id(&self) -> &str {
        &self.inner.access_key_id
    }

    #[getter]
    fn secret_access_key(&self) -> &str {
        &self.inner.secret_access_key
    }

    #[getter]
    fn endpoint(&self) -> &str {
        &self.inner.endpoint
    }

    #[getter]
    fn region(&self) -> &str {
        &self.inner.region
    }

    fn __repr__(&self) -> String {
        let key_preview = if self.inner.access_key_id.len() > 4 {
            &self.inner.access_key_id[..4]
        } else {
            &self.inner.access_key_id
        };
        format!(
            "S3Credentials(access_key_id='{}...', endpoint='{}')",
            key_preview, self.inner.endpoint
        )
    }
}

/// Result of a completed download.
#[pyclass(name = "DownloadReport")]
#[derive(Clone)]
pub struct PyDownloadReport {
    /// URL that was downloaded (if HTTP source).
    #[pyo3(get)]
    pub url: Option<String>,
    /// Destination path on disk.
    #[pyo3(get)]
    pub path: String,
    /// Total bytes written.
    #[pyo3(get)]
    pub bytes: u64,
    /// Number of parallel threads used.
    #[pyo3(get)]
    pub threads: usize,
}

#[pymethods]
impl PyDownloadReport {
    fn __repr__(&self) -> String {
        format!(
            "DownloadReport(path='{}', bytes={}, threads={})",
            self.path, self.bytes, self.threads
        )
    }
}

impl From<ripget::DownloadReport> for PyDownloadReport {
    fn from(report: ripget::DownloadReport) -> Self {
        Self {
            url: report.url,
            path: report.path.to_string_lossy().to_string(),
            bytes: report.bytes,
            threads: report.threads,
        }
    }
}

/// Convert ripget errors to Python exceptions.
fn to_py_err(err: ripget::RipgetError) -> PyErr {
    match &err {
        ripget::RipgetError::InvalidThreadCount(_) | ripget::RipgetError::InvalidBufferSize(_) => {
            PyValueError::new_err(err.to_string())
        }
        ripget::RipgetError::Io(_) => PyIOError::new_err(err.to_string()),
        _ => PyRuntimeError::new_err(err.to_string()),
    }
}

/// Download a file from a URL with optional S3/R2 credentials.
///
/// Args:
///     url: The URL or bucket/key path to download.
///     dest: Destination file path.
///     credentials: Optional S3Credentials for authenticated downloads.
///     threads: Number of parallel download threads (default: 10).
///     buffer_size: Read buffer size in bytes (default: 16MB).
///
/// Returns:
///     DownloadReport with download statistics.
///
/// Example:
///     ```python
///     import asyncio
///     from ripget import download, S3Credentials
///
///     async def main():
///         # Simple public URL download
///         report = await download("https://example.com/file.bin", "output.bin")
///         print(f"Downloaded {report.bytes} bytes")
///
///         # With S3/R2 credentials
///         creds = S3Credentials(
///             access_key_id="...",
///             secret_access_key="...",
///             endpoint="https://account.r2.cloudflarestorage.com",
///         )
///         report = await download("bucket/key", "output.bin", credentials=creds)
///
///     asyncio.run(main())
///     ```
#[pyfunction]
#[pyo3(signature = (url, dest, credentials=None, threads=None, buffer_size=None))]
fn download<'py>(
    py: Python<'py>,
    url: String,
    dest: String,
    credentials: Option<PyS3Credentials>,
    threads: Option<usize>,
    buffer_size: Option<usize>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let report = match credentials {
            Some(creds) => {
                ripget::download_url_with_s3_auth(&url, &dest, &creds.inner, threads, None, buffer_size)
                    .await
                    .map_err(to_py_err)?
            }
            None => {
                ripget::download_url_with_progress(&url, &dest, threads, None, None, buffer_size)
                    .await
                    .map_err(to_py_err)?
            }
        };
        Ok(PyDownloadReport::from(report))
    })
}

/// Python module for ripget.
#[pymodule(name = "ripget")]
fn ripget_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyS3Credentials>()?;
    m.add_class::<PyDownloadReport>()?;
    m.add_function(wrap_pyfunction!(download, m)?)?;
    Ok(())
}
