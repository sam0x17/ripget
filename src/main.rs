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

    let progress_handle = if io::stderr().is_terminal() {
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
        .and_then(|segments| segments.filter(|s| !s.is_empty()).last())
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

impl ripget::ProgressReporter for CliProgress {
    fn init(&self, total: u64) {
        self.bar.set_length(total);
    }

    fn add(&self, delta: u64) {
        self.bar.inc(delta);
    }
}
