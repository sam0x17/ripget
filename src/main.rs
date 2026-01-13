use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::{env, io};

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use std::io::IsTerminal;

#[derive(Debug, Parser)]
#[command(
    name = "ripget",
    version,
    about = "Fast, idempotent, multi-part downloader"
)]
struct Args {
    /// URL to download.
    #[arg(value_name = "URL")]
    url: String,

    /// Optional output filename. Defaults to the URL basename.
    #[arg(value_name = "OUTPUT")]
    output: Option<PathBuf>,

    /// Override the number of parallel ranges.
    #[arg(long)]
    threads: Option<usize>,

    /// Override the HTTP user agent.
    #[arg(long = "user-agent")]
    user_agent: Option<String>,

    /// Disable the interactive progress bar.
    #[arg(long)]
    silent: bool,

    /// Override the read buffer size, e.g. 8mb or 16777216.
    #[arg(long = "cache-size", value_name = "SIZE")]
    cache_size: Option<String>,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("ripget: {err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let output = match args.output {
        Some(path) => path,
        None => default_output_path(&args.url)?,
    };
    let threads = match args.threads {
        Some(value) => Some(value),
        None => env_threads()?,
    };
    let user_agent = match args.user_agent {
        Some(value) => Some(value),
        None => env::var("RIPGET_USER_AGENT").ok(),
    };
    let cache_size = match args.cache_size {
        Some(value) => Some(parse_cache_size(&value)?),
        None => env_cache_size()?,
    };

    let progress_handle = if !args.silent && io::stderr().is_terminal() {
        let bar = ProgressBar::new(0);
        let style = ProgressStyle::with_template(
            "{spinner:.green} {msg} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, ETA {eta})",
        )?
        .progress_chars("=>-");
        bar.set_style(style);
        bar.set_message(output.display().to_string());
        bar.enable_steady_tick(Duration::from_millis(120));
        Some(Arc::new(CliProgress { bar }))
    } else {
        None
    };
    let progress = progress_handle
        .as_ref()
        .map(|handle| handle.clone() as ripget::Progress);

    ripget::download_url_with_progress(
        &args.url,
        &output,
        threads,
        user_agent.as_deref(),
        progress,
        cache_size,
    )
    .await?;
    if let Some(handle) = progress_handle {
        handle.finish("done");
    }
    Ok(())
}

fn default_output_path(url: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let parsed = reqwest::Url::parse(url)?;
    let name = parsed
        .path_segments()
        .and_then(|segments| segments.filter(|s| !s.is_empty()).next_back())
        .unwrap_or("download");
    Ok(PathBuf::from(name))
}

fn env_threads() -> Result<Option<usize>, Box<dyn std::error::Error>> {
    match env::var("RIPGET_THREADS") {
        Ok(value) => {
            let parsed = value.parse::<usize>().map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid RIPGET_THREADS value: {value} ({err})"),
                )
            })?;
            Ok(Some(parsed))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(Box::new(err)),
    }
}

struct CliProgress {
    bar: ProgressBar,
}

impl CliProgress {
    fn finish(&self, message: &'static str) {
        self.bar.finish_with_message(message);
    }
}

fn env_cache_size() -> Result<Option<usize>, Box<dyn std::error::Error>> {
    match env::var("RIPGET_CACHE_SIZE") {
        Ok(value) => Ok(Some(parse_cache_size(&value)?)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(Box::new(err)),
    }
}

fn parse_cache_size(value: &str) -> Result<usize, Box<dyn std::error::Error>> {
    let value = value.trim();
    if value.is_empty() {
        return Err(Box::new(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cache size must not be empty",
        )));
    }
    let lower = value.to_ascii_lowercase();
    let mut split = 0usize;
    for (idx, ch) in lower.char_indices() {
        if ch.is_ascii_digit() {
            split = idx + ch.len_utf8();
        } else {
            break;
        }
    }
    if split == 0 {
        return Err(Box::new(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid cache size: {value}"),
        )));
    }
    let (num_str, suffix) = lower.split_at(split);
    let suffix = suffix.trim();
    let number: u64 = num_str.parse().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid cache size: {value} ({err})"),
        )
    })?;
    let multiplier = match suffix {
        "" | "b" => 1u64,
        "k" | "kb" => 1024u64,
        "m" | "mb" => 1024u64 * 1024,
        "g" | "gb" => 1024u64 * 1024 * 1024,
        _ => {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid cache size suffix: {value}"),
            )));
        }
    };
    let bytes = number
        .checked_mul(multiplier)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "cache size overflow"))?;
    let bytes = usize::try_from(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "cache size too large"))?;
    Ok(bytes)
}

impl ripget::ProgressReporter for CliProgress {
    fn init(&self, total: u64) {
        self.bar.set_length(total);
    }

    fn add(&self, delta: u64) {
        self.bar.inc(delta);
    }
}

#[cfg(test)]
mod tests {
    use super::parse_cache_size;

    #[test]
    fn parse_cache_size_values() {
        assert_eq!(parse_cache_size("8mb").unwrap(), 8 * 1024 * 1024);
        assert_eq!(parse_cache_size("16m").unwrap(), 16 * 1024 * 1024);
        assert_eq!(parse_cache_size("1gb").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_cache_size("4096").unwrap(), 4096);
        assert_eq!(parse_cache_size("2KB").unwrap(), 2048);
    }

    #[test]
    fn parse_cache_size_rejects_invalid_suffix() {
        assert!(parse_cache_size("12xb").is_err());
    }
}
