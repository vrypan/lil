//! Rolling log file writer. Caps the log at `MAX_LOG_BYTES`, then renames it
//! to `<name>.1` (overwriting any previous backup) and starts a fresh file.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;

const MAX_LOG_BYTES: u64 = 10 * 1024 * 1024; // 10 MB

pub struct RollingWriter {
    path: PathBuf,
    file: File,
    written: u64,
}

impl RollingWriter {
    fn open(path: PathBuf) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let written = file.metadata()?.len();
        Ok(Self {
            path,
            file,
            written,
        })
    }

    fn rotate(&mut self) -> io::Result<()> {
        let name = self
            .path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let backup = self.path.with_file_name(format!("{name}.1"));
        let _ = fs::rename(&self.path, &backup);
        self.file = File::create(&self.path)?;
        self.written = 0;
        Ok(())
    }
}

impl Write for RollingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.written >= MAX_LOG_BYTES {
            if let Err(err) = self.rotate() {
                let _ = writeln!(self.file, "[log rotation failed: {err}]");
            }
        }
        let n = self.file.write(buf)?;
        self.written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// `MakeWriter` impl: clones the `Arc` on each call so every log event gets
/// its own short-lived lock guard, which is then written to and dropped.
#[derive(Clone)]
struct LogWriter(std::sync::Arc<std::sync::Mutex<RollingWriter>>);

impl io::Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().expect("log writer poisoned").write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().expect("log writer poisoned").flush()
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogWriter {
    type Writer = LogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Initialise `tracing_subscriber` to write to a rolling log file.
/// Must be called at most once.
pub fn init_file_logging(path: PathBuf) -> io::Result<()> {
    let writer = LogWriter(std::sync::Arc::new(std::sync::Mutex::new(
        RollingWriter::open(path)?,
    )));
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_level(true)
        .with_target(false)
        .with_ansi(false)
        .with_writer(writer)
        .init();
    Ok(())
}
