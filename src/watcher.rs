//! Filesystem event monitoring. Wraps the `notify` crate with debouncing,
//! a polling fallback, and filtering of access-only and state-dir events.

use notify::{
    Config as NotifyConfig, Event, RecommendedWatcher, RecursiveMode, Watcher as _,
    event::{AccessKind, AccessMode, EventKind},
};
use std::io;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;

const IGNORED_DIR_NAMES: &[&str] = &[".lil"];

pub fn spawn(
    root: PathBuf,
    debounce_ms: u64,
    poll: bool,
    tx: UnboundedSender<Vec<PathBuf>>,
) -> io::Result<thread::JoinHandle<()>> {
    if poll {
        Ok(thread::spawn(move || run_polling(debounce_ms, tx)))
    } else {
        spawn_watcher(root, debounce_ms, tx)
    }
}

fn spawn_watcher(
    root: PathBuf,
    debounce_ms: u64,
    tx: UnboundedSender<Vec<PathBuf>>,
) -> io::Result<thread::JoinHandle<()>> {
    let (watch_tx, watch_rx) = mpsc::channel();
    let mut watcher = RecommendedWatcher::new(
        move |result| {
            let _ = watch_tx.send(result);
        },
        NotifyConfig::default(),
    )
    .map_err(io::Error::other)?;

    watcher
        .watch(&root, RecursiveMode::Recursive)
        .map_err(io::Error::other)?;

    let debounce = Duration::from_millis(debounce_ms.max(20));
    Ok(thread::spawn(move || {
        let _watcher = watcher;
        while let Ok(result) = watch_rx.recv() {
            match result {
                Ok(event) => {
                    tracing::debug!("filesystem event: {:?}", event.kind);
                    let mut paths = if is_relevant_event(&event) {
                        event.paths
                    } else {
                        Vec::new()
                    };
                    drain_debounce_window(&watch_rx, debounce, &mut paths);
                    paths.retain(|path| !is_ignored_event_path(path));
                    paths.sort();
                    paths.dedup();
                    if !paths.is_empty() {
                        let _ = tx.send(paths);
                    }
                }
                Err(err) => tracing::warn!("filesystem watch error: {err}"),
            }
        }
    }))
}

fn drain_debounce_window(
    rx: &mpsc::Receiver<notify::Result<Event>>,
    debounce: Duration,
    paths: &mut Vec<PathBuf>,
) {
    let deadline = Instant::now() + debounce;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }

        match rx.recv_timeout(remaining) {
            Ok(Ok(event)) => {
                tracing::debug!("filesystem event: {:?}", event.kind);
                if is_relevant_event(&event) {
                    paths.extend(event.paths);
                }
            }
            Ok(Err(err)) => tracing::warn!("filesystem watch error: {err}"),
            Err(mpsc::RecvTimeoutError::Timeout) => return,
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
        }
    }
}

fn is_relevant_event(event: &Event) -> bool {
    !matches!(
        event.kind,
        EventKind::Access(
            AccessKind::Any
                | AccessKind::Read
                | AccessKind::Open(_)
                | AccessKind::Close(AccessMode::Read)
                | AccessKind::Close(AccessMode::Execute)
                | AccessKind::Close(AccessMode::Other)
                | AccessKind::Other
        )
    )
}

fn is_ignored_event_path(path: &std::path::Path) -> bool {
    path.components().any(|component| {
        let name = component.as_os_str().to_string_lossy();
        IGNORED_DIR_NAMES.contains(&name.as_ref())
    })
}

fn run_polling(interval_ms: u64, tx: UnboundedSender<Vec<PathBuf>>) {
    let interval = Duration::from_millis(interval_ms.max(50));
    loop {
        thread::sleep(interval);
        if tx.send(Vec::new()).is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn ignores_state_dir_events() {
        assert!(is_ignored_event_path(Path::new(
            "/tmp/root/.lil/entries.bin"
        )));
        assert!(is_ignored_event_path(Path::new(".lil/recv-file")));
        assert!(!is_ignored_event_path(Path::new("/tmp/root/file.txt")));
    }

    #[test]
    fn ignores_read_access_events() {
        let read = Event::new(EventKind::Access(AccessKind::Close(AccessMode::Read)));
        let write = Event::new(EventKind::Access(AccessKind::Close(AccessMode::Write)));
        let modify = Event::new(EventKind::Modify(notify::event::ModifyKind::Data(
            notify::event::DataChange::Any,
        )));

        assert!(!is_relevant_event(&read));
        assert!(is_relevant_event(&write));
        assert!(is_relevant_event(&modify));
    }
}
