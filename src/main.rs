mod state;
mod watcher;

use clap::Parser;
use std::io;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "tngl", about = "Monitor a folder and print tngl tree changes")]
struct Cli {
    #[arg(long, value_name = "PATH", help = "Folder to monitor")]
    folder: PathBuf,

    #[arg(
        long,
        value_name = "NODE_ID",
        default_value = "local",
        help = "Origin used in local logical versions"
    )]
    origin: String,

    #[arg(
        long,
        value_name = "MILLIS",
        default_value = "500",
        help = "Debounce delay for filesystem events, or scan interval with --poll"
    )]
    interval_ms: u64,

    #[arg(long, help = "Use periodic polling instead of filesystem events")]
    poll: bool,
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .without_time()
        .init();

    if let Err(err) = run() {
        tracing::error!("{err}");
        std::process::exit(1);
    }
}

fn run() -> io::Result<()> {
    let cli = Cli::parse();
    watcher::run(cli.folder, cli.origin, cli.interval_ms, cli.poll)
}
