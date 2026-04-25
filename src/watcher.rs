use crate::state::{Change, FolderState, hex};
use notify::{Config as NotifyConfig, Event, RecommendedWatcher, RecursiveMode, Watcher as _};
use std::io;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

pub fn run(folder: PathBuf, origin: String, interval_ms: u64, poll: bool) -> io::Result<()> {
    std::fs::create_dir_all(&folder)?;

    let state = FolderState::new(folder, origin)?;
    print_start(&state, poll, interval_ms);

    if poll {
        run_polling(state, interval_ms)
    } else {
        run_watching(state, interval_ms)
    }
}

fn run_watching(mut state: FolderState, debounce_ms: u64) -> io::Result<()> {
    let (tx, rx) = mpsc::channel();
    let mut watcher = RecommendedWatcher::new(
        move |result| {
            let _ = tx.send(result);
        },
        NotifyConfig::default(),
    )
    .map_err(io::Error::other)?;

    watcher
        .watch(state.root(), RecursiveMode::Recursive)
        .map_err(io::Error::other)?;

    let debounce = Duration::from_millis(debounce_ms.max(20));
    loop {
        match rx.recv() {
            Ok(Ok(event)) => {
                tracing::debug!("filesystem event: {:?}", event.kind);
                drain_debounce_window(&rx, debounce);
                rescan_and_print(&mut state);
            }
            Ok(Err(err)) => tracing::warn!("filesystem watch error: {err}"),
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "filesystem watcher disconnected",
                ));
            }
        }
    }
}

fn drain_debounce_window(rx: &mpsc::Receiver<notify::Result<Event>>, debounce: Duration) {
    let deadline = Instant::now() + debounce;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }

        match rx.recv_timeout(remaining) {
            Ok(Ok(event)) => tracing::debug!("filesystem event: {:?}", event.kind),
            Ok(Err(err)) => tracing::warn!("filesystem watch error: {err}"),
            Err(mpsc::RecvTimeoutError::Timeout) => return,
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
        }
    }
}

fn run_polling(mut state: FolderState, interval_ms: u64) -> io::Result<()> {
    let interval = Duration::from_millis(interval_ms.max(50));
    loop {
        thread::sleep(interval);
        rescan_and_print(&mut state);
    }
}

fn rescan_and_print(state: &mut FolderState) {
    let before_state = state.root_hash();
    let before_live = state.live_root_hash();
    match state.rescan() {
        Ok(changes) => print_changes(before_state, before_live, state, &changes),
        Err(err) => tracing::warn!("scan failed: {err}"),
    }
}

fn print_start(state: &FolderState, poll: bool, interval_ms: u64) {
    tracing::info!("watching {}", state.root().display());
    if poll {
        tracing::info!("mode poll interval={}ms", interval_ms.max(50));
    } else {
        tracing::info!("mode fs-events debounce={}ms", interval_ms.max(20));
    }
    tracing::info!("state root {}", hex(state.root_hash()));
    tracing::info!("live root {}", hex(state.live_root_hash()));
    tracing::info!(
        "nodes state={} live={}",
        state.tree().nodes.len(),
        state.live_tree().nodes.len()
    );
}

fn print_changes(
    before_state: [u8; 32],
    before_live: [u8; 32],
    state: &FolderState,
    changes: &[Change],
) {
    if changes.is_empty() {
        return;
    }

    for change in changes {
        tracing::info!(
            "{} {} v{}:{}",
            change.verb(),
            change.path,
            change.new.version.lamport,
            change.new.version.origin
        );
    }

    tracing::info!(
        "state root {} -> {} nodes={}",
        hex(before_state),
        hex(state.root_hash()),
        state.tree().nodes.len()
    );
    tracing::info!(
        "live root {} -> {} nodes={}",
        hex(before_live),
        hex(state.live_root_hash()),
        state.live_tree().nodes.len()
    );
}
