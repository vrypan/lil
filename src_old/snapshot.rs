use crate::cache::read_cache_file;
use notify::event::{CreateKind, ModifyKind, RemoveKind};
use notify::{Event, EventKind};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering as CmpOrdering;
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub type Blake3Hash = [u8; 32];
pub const TOMBSTONE_HASH: Blake3Hash = [0; 32];
pub type HashMapById = HashMap<String, SnapshotEntry>;

/// Name of the state directory placed inside the sync folder.
const STATE_DIR: &str = ".tngl";
const IGNORE_FILE_NAME: &str = ".notngl";
const IGNORED_FILE_NAMES: &[&str] = &[".DS_Store", "Thumbs.db", "Desktop.ini"];
const IGNORED_FILE_PREFIXES: &[&str] = &["._"];
const IGNORED_DIR_NAMES: &[&str] = &[
    ".Spotlight-V100",
    ".Trashes",
    ".fseventsd",
    "$RECYCLE.BIN",
    "lost+found",
];

#[derive(Clone, Debug)]
struct IgnorePattern {
    pattern: String,
    negated: bool,
    directory_only: bool,
    anchored: bool,
    has_slash: bool,
}

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    Serialize,
    Deserialize,
)]
pub struct SnapshotEntry {
    pub hash: Blake3Hash,
    pub lamport: u64,
    pub changed_at_ms: u64,
    pub origin: String,
    /// Actual filesystem mtime at the time this entry was last observed locally.
    /// Used at startup to skip rehashing files whose mtime is unchanged.
    /// Never sent over the wire; not included in bucket hashes.
    pub mtime_ms: u64,
}

/// Wire-format entry: metadata only, no file contents.
///
/// File content is streamed separately over the QUIC bidirectional stream;
/// see `FileHeader` in `protocol.rs`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplicatedEntry {
    /// Relative path from the sync root, using forward slashes.
    pub id: String,
    pub hash: Blake3Hash,
    pub lamport: u64,
    pub changed_at_ms: u64,
    pub origin: String,
}

pub fn reconcile_startup_state<F>(
    sync_dir: &Path,
    cache_path: &Path,
    local_origin: &str,
    mut emit: F,
) -> io::Result<HashMapById>
where
    F: FnMut(&str, &SnapshotEntry),
{
    let cached = read_cache_file(cache_path, 8)?;
    // Pass cache so build_snapshot_map can skip hashing files whose mtime is unchanged.
    let mut current = build_snapshot_map(sync_dir, local_origin, cached.as_ref())?;
    if let Some(cached) = cached {
        let mut next_lamport = cached
            .values()
            .map(|entry| entry.lamport)
            .max()
            .unwrap_or(0);
        let tombstone_ts = unix_timestamp_ms()?;
        for (id, current_entry) in &mut current {
            if let Some(old_entry) = cached.get(id) {
                if old_entry.hash == current_entry.hash {
                    // Preserve lamport, changed_at_ms, origin from cache.
                    // Keep the fresh mtime_ms from the scan.
                    let fresh_mtime = current_entry.mtime_ms;
                    *current_entry = old_entry.clone();
                    current_entry.mtime_ms = fresh_mtime;
                    continue;
                }
            }
            next_lamport = next_lamport.saturating_add(1);
            current_entry.lamport = next_lamport;
            current_entry.origin = local_origin.to_string();
        }
        for (id, old_entry) in &cached {
            if !current.contains_key(id) && old_entry.hash != TOMBSTONE_HASH {
                current.insert(
                    id.clone(),
                    SnapshotEntry {
                        hash: TOMBSTONE_HASH,
                        lamport: next_lamport.saturating_add(1),
                        changed_at_ms: tombstone_ts,
                        origin: local_origin.to_string(),
                        mtime_ms: tombstone_ts,
                    },
                );
                next_lamport = next_lamport.saturating_add(1);
            }
        }
        emit_differences(&cached, &current, &mut emit);
    }
    Ok(current)
}

pub fn canonical_path(path: PathBuf) -> PathBuf {
    fs::canonicalize(&path).unwrap_or_else(|_| canonicalize_missing_path(&path).unwrap_or(path))
}

fn canonicalize_missing_path(path: &Path) -> Option<PathBuf> {
    let parent = path.parent()?;
    let canonical_parent = fs::canonicalize(parent)
        .ok()
        .or_else(|| canonicalize_missing_path(parent))?;
    let name = path.file_name()?;
    Some(canonical_parent.join(name))
}

/// Scan `sync_dir` recursively and build the entry map.
///
/// When `cache` is provided, files whose mtime matches the cached `mtime_ms`
/// are returned directly from the cache (no rehashing). This makes startup fast
/// for large folders where most files are unchanged.
pub fn build_snapshot_map(
    sync_dir: &Path,
    local_origin: &str,
    cache: Option<&HashMapById>,
) -> io::Result<HashMapById> {
    let sync_dir = canonical_path(sync_dir.to_path_buf());
    let mut entries = HashMap::new();
    collect_files_recursive(&sync_dir, &sync_dir, local_origin, cache, &mut entries)?;
    Ok(entries)
}

fn collect_files_recursive(
    sync_dir: &Path,
    dir: &Path,
    local_origin: &str,
    cache: Option<&HashMapById>,
    entries: &mut HashMapById,
) -> io::Result<()> {
    let read_dir = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    for entry in read_dir {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name() else {
            continue;
        };
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            if should_ignore_path(sync_dir, &path) {
                continue;
            }
            if is_ignored_dir_name(name) || name == OsStr::new(STATE_DIR) {
                continue;
            }
            collect_files_recursive(sync_dir, &path, local_origin, cache, entries)?;
        } else if file_type.is_file() {
            if is_ignored_file_name(name) {
                continue;
            }
            let Some(id) = sync_id_from_path(sync_dir, &path) else {
                continue;
            };
            let mtime_ms = file_changed_at_ms(&path)?;

            // If mtime matches the cache, skip reading the file entirely.
            if let Some(cached) = cache.and_then(|c| c.get(&id)) {
                if cached.mtime_ms == mtime_ms && cached.hash != TOMBSTONE_HASH {
                    let mut entry = cached.clone();
                    entry.mtime_ms = mtime_ms;
                    entries.insert(id, entry);
                    continue;
                }
            }

            if let Some(hash) = hash_file_if_present(&path)? {
                entries.insert(
                    id,
                    SnapshotEntry {
                        hash,
                        lamport: 0,
                        changed_at_ms: mtime_ms,
                        origin: local_origin.to_string(),
                        mtime_ms,
                    },
                );
            }
        }
    }
    Ok(())
}

pub fn file_changed_at_ms(path: &Path) -> io::Result<u64> {
    match fs::metadata(path).and_then(|meta| meta.modified()) {
        Ok(modified) => Ok(system_time_to_ms(modified)?),
        Err(err) if err.kind() == io::ErrorKind::NotFound => unix_timestamp_ms(),
        Err(err) => Err(err),
    }
}

pub fn system_time_to_ms(value: SystemTime) -> io::Result<u64> {
    let duration = value.duration_since(UNIX_EPOCH).map_err(io::Error::other)?;
    Ok(duration.as_millis() as u64)
}

fn should_process_event_kind(kind: &EventKind) -> bool {
    matches!(
        kind,
        EventKind::Any
            | EventKind::Other
            | EventKind::Create(CreateKind::Any | CreateKind::File | CreateKind::Folder)
            | EventKind::Modify(
                ModifyKind::Any
                    | ModifyKind::Data(_)
                    | ModifyKind::Metadata(_)
                    | ModifyKind::Name(_)
            )
            | EventKind::Remove(RemoveKind::Any | RemoveKind::File | RemoveKind::Folder)
    )
}

pub fn should_rescan_event(sync_dir: &Path, event: &Event) -> bool {
    let sync_dir_c = canonical_path(sync_dir.to_path_buf());
    let state_dir = sync_dir_c.join(STATE_DIR);
    for path in &event.paths {
        let path = canonical_path(path.clone());
        if path.starts_with(&state_dir) {
            continue;
        }
        if path.file_name() == Some(OsStr::new(IGNORE_FILE_NAME)) {
            return true;
        }
        if should_ignore_path(sync_dir, &path) {
            continue;
        }
        if path == sync_dir_c {
            return true;
        }
        if !path.starts_with(&sync_dir_c) {
            continue;
        }
        if fs::metadata(&path).map(|m| m.is_dir()).unwrap_or(false) {
            return true;
        }
        if matches!(event.kind, EventKind::Remove(RemoveKind::Folder)) {
            return true;
        }
    }
    false
}

fn unique_sync_paths(sync_dir: &Path, paths: &[PathBuf]) -> Vec<PathBuf> {
    let sync_dir_c = canonical_path(sync_dir.to_path_buf());
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for path in paths {
        let path = canonical_path(path.clone());
        if !path.starts_with(&sync_dir_c) {
            continue;
        }
        if should_ignore_path(sync_dir, &path) {
            continue;
        }
        if seen.insert(path.clone()) {
            out.push(path);
        }
    }
    out
}

/// Returns the relative path from `sync_dir` as a forward-slash string.
/// Returns `None` for paths outside `sync_dir`, the `sync_dir` itself,
/// or anything under the `.tngl` state directory.
fn sync_id_from_path(sync_dir: &Path, path: &Path) -> Option<String> {
    let path = canonical_path(path.to_path_buf());
    if should_ignore_path(sync_dir, &path) {
        return None;
    }
    let sync_dir_c = canonical_path(sync_dir.to_path_buf());
    let rel = path.strip_prefix(&sync_dir_c).ok()?;
    if rel.as_os_str().is_empty() {
        return None;
    }
    let id = rel
        .components()
        .filter_map(|c| c.as_os_str().to_str())
        .collect::<Vec<_>>()
        .join("/");
    if id.is_empty() { None } else { Some(id) }
}

/// Hash a file using a streaming blake3 hasher (no full-file buffering).
fn hash_file_if_present(path: &Path) -> io::Result<Option<Blake3Hash>> {
    match fs::File::open(path) {
        Ok(mut file) => {
            let mut hasher = blake3::Hasher::new();
            io::copy(&mut file, &mut hasher)?;
            Ok(Some(*hasher.finalize().as_bytes()))
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

/// Hash a file and return its mtime. Returns `None` if the file does not exist.
fn read_local_file_snapshot(path: &Path) -> io::Result<Option<(Blake3Hash, u64)>> {
    match fs::File::open(path) {
        Ok(mut file) => {
            let mut hasher = blake3::Hasher::new();
            io::copy(&mut file, &mut hasher)?;
            let hash = *hasher.finalize().as_bytes();
            let mtime_ms = file_changed_at_ms(path)?;
            Ok(Some((hash, mtime_ms)))
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

pub fn process_local_event(
    sync_dir: &Path,
    lamport: &mut u64,
    entries: &mut HashMapById,
    suppressed: &mut HashMap<String, SnapshotEntry>,
    local_origin: &str,
    event: Event,
) -> io::Result<Vec<ReplicatedEntry>> {
    if !should_process_event_kind(&event.kind) {
        return Ok(Vec::new());
    }
    if should_rescan_event(sync_dir, &event) {
        return rescan_local_state(sync_dir, lamport, entries, suppressed, local_origin);
    }
    let mut out = Vec::new();
    for path in unique_sync_paths(sync_dir, &event.paths) {
        let Some(id) = sync_id_from_path(sync_dir, &path) else {
            continue;
        };
        // Directory removals can surface as generic rename/remove events after
        // the path is already gone, so metadata no longer tells us it was a
        // directory. If we still track descendants under this path, fall back
        // to a full rescan to emit tombstones for the files in that subtree
        // instead of creating a bogus tombstone for the directory itself.
        if !path.exists() && has_tracked_descendants(entries, &id) {
            return rescan_local_state(sync_dir, lamport, entries, suppressed, local_origin);
        }
        let local_state = read_local_file_snapshot(&path)?;
        let observed_hash = local_state
            .as_ref()
            .map(|(hash, _)| *hash)
            .unwrap_or(TOMBSTONE_HASH);
        if let Some(expected) = suppressed.get(&id).cloned() {
            if expected.hash == observed_hash {
                entries.insert(id.clone(), expected);
                suppressed.remove(&id);
                continue;
            }
        }
        if entries.get(&id).map(|entry| entry.hash) == Some(observed_hash) {
            continue;
        }
        *lamport = lamport.saturating_add(1);
        let observed_mtime_ms = local_state
            .as_ref()
            .map(|(_, mtime_ms)| *mtime_ms)
            .unwrap_or(unix_timestamp_ms()?);
        let previous_changed_at_ms = entries
            .get(&id)
            .map(|entry| entry.changed_at_ms)
            .unwrap_or(0);
        let entry = SnapshotEntry {
            hash: observed_hash,
            lamport: *lamport,
            changed_at_ms: observed_mtime_ms.max(previous_changed_at_ms.saturating_add(1)),
            origin: local_origin.to_string(),
            mtime_ms: observed_mtime_ms,
        };
        let replicated = ReplicatedEntry {
            id: id.clone(),
            hash: entry.hash,
            lamport: entry.lamport,
            changed_at_ms: entry.changed_at_ms,
            origin: entry.origin.clone(),
        };
        entries.insert(id, entry);
        out.push(replicated);
    }
    Ok(out)
}

fn has_tracked_descendants(entries: &HashMapById, id: &str) -> bool {
    let prefix = format!("{id}/");
    entries.keys().any(|existing| existing.starts_with(&prefix))
}

pub fn rescan_local_state(
    sync_dir: &Path,
    lamport: &mut u64,
    entries: &mut HashMapById,
    suppressed: &mut HashMap<String, SnapshotEntry>,
    local_origin: &str,
) -> io::Result<Vec<ReplicatedEntry>> {
    // Pass current entries as cache so unchanged files skip rehashing.
    let scanned = build_snapshot_map(sync_dir, local_origin, Some(entries))?;
    let all_ids: HashSet<String> = entries.keys().chain(scanned.keys()).cloned().collect();

    let mut updates = Vec::new();
    for id in all_ids {
        let observed_hash = scanned.get(&id).map(|e| e.hash).unwrap_or(TOMBSTONE_HASH);
        if let Some(expected) = suppressed.get(&id).cloned() {
            if expected.hash == observed_hash {
                entries.insert(id.clone(), expected);
                suppressed.remove(&id);
                continue;
            }
        }
        if entries.get(&id).map(|e| e.hash) == Some(observed_hash) {
            continue;
        }
        *lamport = lamport.saturating_add(1);
        let previous_changed_at_ms = entries.get(&id).map(|e| e.changed_at_ms).unwrap_or(0);
        let entry = if let Some(scanned_entry) = scanned.get(&id) {
            SnapshotEntry {
                hash: scanned_entry.hash,
                lamport: *lamport,
                changed_at_ms: scanned_entry
                    .mtime_ms
                    .max(previous_changed_at_ms.saturating_add(1)),
                origin: local_origin.to_string(),
                mtime_ms: scanned_entry.mtime_ms,
            }
        } else {
            let now = unix_timestamp_ms()?;
            SnapshotEntry {
                hash: TOMBSTONE_HASH,
                lamport: *lamport,
                changed_at_ms: now.max(previous_changed_at_ms.saturating_add(1)),
                origin: local_origin.to_string(),
                mtime_ms: now,
            }
        };
        let replicated = ReplicatedEntry {
            id: id.clone(),
            hash: entry.hash,
            lamport: entry.lamport,
            changed_at_ms: entry.changed_at_ms,
            origin: entry.origin.clone(),
        };
        entries.insert(id, entry);
        updates.push(replicated);
    }
    Ok(updates)
}

/// Build the list of files to send for a `GetFiles` response.
///
/// Returns `(ReplicatedEntry, Option<PathBuf>)` pairs: `None` path for tombstones,
/// `Some(path)` pointing to the actual file on disk for live entries.
///
/// If a live entry's file is missing at send time, a tombstone is emitted instead
/// and the local snapshot is updated accordingly.
pub fn collect_file_send_list(
    sync_dir: &Path,
    entries: &mut HashMapById,
    local_origin: &str,
    lamport: &mut u64,
    ids: &[String],
) -> io::Result<Vec<(ReplicatedEntry, Option<PathBuf>)>> {
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        if should_ignore_id(sync_dir, id) {
            continue;
        }
        let Some(entry) = entries.get(id) else {
            continue;
        };
        if entry.hash == TOMBSTONE_HASH {
            out.push((
                ReplicatedEntry {
                    id: id.clone(),
                    hash: TOMBSTONE_HASH,
                    lamport: entry.lamport,
                    changed_at_ms: entry.changed_at_ms,
                    origin: entry.origin.clone(),
                },
                None,
            ));
        } else {
            let path = sync_dir.join(id);
            if path.exists() {
                out.push((
                    ReplicatedEntry {
                        id: id.clone(),
                        hash: entry.hash,
                        lamport: entry.lamport,
                        changed_at_ms: entry.changed_at_ms,
                        origin: entry.origin.clone(),
                    },
                    Some(path),
                ));
            } else {
                // File disappeared since we last saw it; send a tombstone instead.
                *lamport = lamport.saturating_add(1);
                let now = unix_timestamp_ms()?;
                let tombstone = SnapshotEntry {
                    hash: TOMBSTONE_HASH,
                    lamport: *lamport,
                    changed_at_ms: now,
                    origin: local_origin.to_string(),
                    mtime_ms: now,
                };
                entries.insert(id.clone(), tombstone.clone());
                out.push((
                    ReplicatedEntry {
                        id: id.clone(),
                        hash: TOMBSTONE_HASH,
                        lamport: tombstone.lamport,
                        changed_at_ms: tombstone.changed_at_ms,
                        origin: tombstone.origin.clone(),
                    },
                    None,
                ));
            }
        }
    }
    Ok(out)
}

pub fn apply_remote_entries<F>(
    sync_dir: &Path,
    lamport: &mut u64,
    entries: &mut HashMapById,
    suppressed: &mut HashMap<String, SnapshotEntry>,
    local_origin: &str,
    remote_entries: Vec<(ReplicatedEntry, Option<PathBuf>)>,
    mut emit: F,
) -> io::Result<Vec<ReplicatedEntry>>
where
    F: FnMut(&str, &SnapshotEntry),
{
    let mut accepted = Vec::new();
    let mut ignore_file_changed = false;
    for (entry, temp_path) in remote_entries {
        if should_ignore_id(sync_dir, &entry.id) {
            if let Some(ref path) = temp_path {
                let _ = fs::remove_file(path);
            }
            continue;
        }
        if entry.origin == local_origin {
            // Drop temp file for entries we originated ourselves.
            if let Some(ref path) = temp_path {
                let _ = fs::remove_file(path);
            }
            continue;
        }
        let remote_snapshot = snapshot_from_wire(&entry);
        let current = entries.get(&entry.id);
        if !should_accept_remote(current, &remote_snapshot) {
            // CRDT says local wins; discard temp file.
            if let Some(ref path) = temp_path {
                let _ = fs::remove_file(path);
            }
            continue;
        }
        if let Err(err) = apply_remote_entry_to_disk(sync_dir, &entry, temp_path.as_deref()) {
            // Log and skip — a single bad entry must not crash the daemon.
            tracing::warn!(target: "tngl", id = entry.id, "skipping entry: {err}");
            if let Some(ref path) = temp_path {
                let _ = fs::remove_file(path);
            }
            continue;
        }

        // Read back the actual mtime after writing so future startup scans can
        // skip rehashing this file when its mtime is unchanged.
        let mtime_ms = if entry.hash != TOMBSTONE_HASH {
            file_changed_at_ms(&sync_dir.join(&entry.id))
                .unwrap_or_else(|_| remote_snapshot.changed_at_ms)
        } else {
            unix_timestamp_ms().unwrap_or(remote_snapshot.changed_at_ms)
        };
        let stored = SnapshotEntry {
            mtime_ms,
            ..remote_snapshot.clone()
        };

        *lamport = (*lamport).max(stored.lamport);
        entries.insert(entry.id.clone(), stored.clone());
        suppressed.insert(entry.id.clone(), stored);
        emit(&entry.id, entries.get(&entry.id).unwrap());
        if entry.id == IGNORE_FILE_NAME {
            ignore_file_changed = true;
        }
        accepted.push(entry);
    }
    if ignore_file_changed {
        let updates = rescan_local_state(sync_dir, lamport, entries, suppressed, local_origin)?;
        for update in &updates {
            emit(&update.id, entries.get(&update.id).unwrap());
        }
        accepted.extend(updates);
    }
    Ok(accepted)
}

pub fn should_accept_remote(current: Option<&SnapshotEntry>, remote: &SnapshotEntry) -> bool {
    match current {
        None => true,
        Some(current) => compare_snapshot(remote, current) == CmpOrdering::Greater,
    }
}

pub fn compare_snapshot(left: &SnapshotEntry, right: &SnapshotEntry) -> CmpOrdering {
    left.lamport
        .cmp(&right.lamport)
        .then_with(|| left.changed_at_ms.cmp(&right.changed_at_ms))
        .then_with(|| left.origin.cmp(&right.origin))
        .then_with(|| left.hash.cmp(&right.hash))
}

/// Write a remote entry to disk.
///
/// For tombstones (`entry.hash == TOMBSTONE_HASH`), removes the file.
/// For live entries, atomically renames `temp_path` to the final location.
/// The content was already hash-verified by `read_stream_to_temp` in protocol.rs.
pub fn apply_remote_entry_to_disk(
    sync_dir: &Path,
    entry: &ReplicatedEntry,
    temp_path: Option<&Path>,
) -> io::Result<()> {
    if entry.id.split('/').any(|component| component == "..") {
        return Err(io::Error::other(format!(
            "rejected unsafe entry id: {}",
            entry.id
        )));
    }
    let path = sync_dir.join(&entry.id);
    if entry.hash == TOMBSTONE_HASH {
        let deleted = match fs::remove_file(&path) {
            Ok(()) => true,
            Err(err) if err.kind() == io::ErrorKind::NotFound => false,
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::PermissionDenied | io::ErrorKind::IsADirectory
                ) =>
            {
                match fs::remove_dir_all(&path) {
                    Ok(()) => true,
                    Err(dir_err) if dir_err.kind() == io::ErrorKind::NotFound => false,
                    Err(dir_err) => return Err(dir_err),
                }
            }
            Err(err) => return Err(err),
        };
        if deleted {
            prune_empty_parent_dirs(sync_dir, &path)?;
        }
        Ok(())
    } else {
        let temp = temp_path
            .ok_or_else(|| io::Error::other("missing temp file for non-tombstone remote entry"))?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        // Prefer atomic rename. On macOS, rename(2) can return EPERM when the
        // destination exists and has certain attributes (e.g. set by iCloud
        // Drive, Finder tags, or the immutable/append-only flags). In that
        // case remove the existing file first and retry.
        match fs::rename(temp, &path) {
            Ok(()) => Ok(()),
            Err(rename_err) if rename_err.raw_os_error() == Some(libc::EPERM) => {
                let _ = fs::remove_file(&path); // best-effort; ignore if absent
                fs::rename(temp, &path)
            }
            Err(err) => Err(err),
        }
    }
}

fn prune_empty_parent_dirs(sync_dir: &Path, path: &Path) -> io::Result<()> {
    let sync_dir = canonical_path(sync_dir.to_path_buf());
    let mut current = path.parent().map(|dir| canonical_path(dir.to_path_buf()));
    while let Some(dir) = current {
        if dir == sync_dir {
            break;
        }
        match fs::remove_dir(&dir) {
            Ok(()) => {
                current = dir
                    .parent()
                    .map(|parent| canonical_path(parent.to_path_buf()));
            }
            Err(err) if matches!(err.kind(), io::ErrorKind::NotFound) => {
                current = dir
                    .parent()
                    .map(|parent| canonical_path(parent.to_path_buf()));
            }
            Err(err) if matches!(err.kind(), io::ErrorKind::DirectoryNotEmpty) => break,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

/// Build a SnapshotEntry from a wire entry. `mtime_ms` is set to zero since
/// the entry has not been written to the local filesystem yet.
pub fn snapshot_from_wire(entry: &ReplicatedEntry) -> SnapshotEntry {
    SnapshotEntry {
        hash: entry.hash,
        lamport: entry.lamport,
        changed_at_ms: entry.changed_at_ms,
        origin: entry.origin.clone(),
        mtime_ms: 0,
    }
}

fn should_ignore_id(sync_dir: &Path, id: &str) -> bool {
    if id == IGNORE_FILE_NAME {
        return false;
    }
    Path::new(id)
        .components()
        .any(|component| should_ignore_component(component.as_os_str()))
        || matches_ignore_patterns(&load_ignore_patterns(sync_dir).unwrap_or_default(), id)
}

fn should_ignore_path(sync_dir: &Path, path: &Path) -> bool {
    let sync_dir_c = canonical_path(sync_dir.to_path_buf());
    let Ok(rel) = path.strip_prefix(&sync_dir_c) else {
        return true;
    };
    let path_id = rel
        .components()
        .filter_map(|c| c.as_os_str().to_str())
        .collect::<Vec<_>>()
        .join("/");
    should_ignore_id(sync_dir, &path_id)
}

fn should_ignore_component(name: &OsStr) -> bool {
    name == OsStr::new(STATE_DIR) || is_ignored_file_name(name) || is_ignored_dir_name(name)
}

fn is_ignored_file_name(name: &OsStr) -> bool {
    IGNORED_FILE_NAMES
        .iter()
        .any(|candidate| name == OsStr::new(candidate))
        || IGNORED_FILE_PREFIXES
            .iter()
            .any(|prefix| name.to_str().is_some_and(|value| value.starts_with(prefix)))
}

fn is_ignored_dir_name(name: &OsStr) -> bool {
    IGNORED_DIR_NAMES
        .iter()
        .any(|candidate| name == OsStr::new(candidate))
}

fn load_ignore_patterns(sync_dir: &Path) -> io::Result<Vec<IgnorePattern>> {
    let path = canonical_path(sync_dir.to_path_buf()).join(IGNORE_FILE_NAME);
    match fs::read_to_string(path) {
        Ok(contents) => Ok(parse_ignore_patterns(&contents)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(err),
    }
}

fn parse_ignore_patterns(contents: &str) -> Vec<IgnorePattern> {
    let mut patterns = Vec::new();
    for raw_line in contents.lines() {
        let mut line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let negated = line.starts_with('!');
        if negated {
            line = &line[1..];
        }
        let directory_only = line.ends_with('/');
        if directory_only {
            line = line.trim_end_matches('/');
        }
        let anchored = line.starts_with('/');
        if anchored {
            line = &line[1..];
        }
        if line.is_empty() {
            continue;
        }
        patterns.push(IgnorePattern {
            pattern: line.to_string(),
            negated,
            directory_only,
            anchored,
            has_slash: line.contains('/'),
        });
    }
    patterns
}

fn matches_ignore_patterns(patterns: &[IgnorePattern], id: &str) -> bool {
    let mut ignored = false;
    for pattern in patterns {
        if matches_ignore_pattern(pattern, id) {
            ignored = !pattern.negated;
        }
    }
    ignored
}

fn matches_ignore_pattern(pattern: &IgnorePattern, id: &str) -> bool {
    let targets = if pattern.directory_only {
        directory_candidates(id)
    } else {
        vec![id.to_string()]
    };

    for target in targets {
        if pattern.anchored {
            if glob_match(&pattern.pattern, &target) {
                return true;
            }
            continue;
        }

        if pattern.has_slash {
            for suffix in path_suffix_candidates(&target) {
                if glob_match(&pattern.pattern, suffix) {
                    return true;
                }
            }
        } else {
            for component in target.split('/') {
                if glob_match(&pattern.pattern, component) {
                    return true;
                }
            }
        }
    }
    false
}

fn directory_candidates(id: &str) -> Vec<String> {
    let parts: Vec<&str> = id.split('/').collect();
    if parts.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(parts.len());
    for i in 0..parts.len() {
        out.push(parts[..=i].join("/"));
    }
    out
}

fn path_suffix_candidates(id: &str) -> Vec<&str> {
    let mut suffixes = Vec::new();
    let mut start = 0usize;
    suffixes.push(id);
    while let Some(pos) = id[start..].find('/') {
        start += pos + 1;
        suffixes.push(&id[start..]);
    }
    suffixes
}

fn glob_match(pattern: &str, text: &str) -> bool {
    glob_match_bytes(pattern.as_bytes(), text.as_bytes())
}

fn glob_match_bytes(pattern: &[u8], text: &[u8]) -> bool {
    if pattern.is_empty() {
        return text.is_empty();
    }
    if pattern.starts_with(b"**") {
        let rest = &pattern[2..];
        if glob_match_bytes(rest, text) {
            return true;
        }
        return !text.is_empty() && glob_match_bytes(pattern, &text[1..]);
    }
    match pattern[0] {
        b'*' => {
            let rest = &pattern[1..];
            if glob_match_bytes(rest, text) {
                return true;
            }
            !text.is_empty() && text[0] != b'/' && glob_match_bytes(pattern, &text[1..])
        }
        b'?' => !text.is_empty() && text[0] != b'/' && glob_match_bytes(&pattern[1..], &text[1..]),
        ch => !text.is_empty() && ch == text[0] && glob_match_bytes(&pattern[1..], &text[1..]),
    }
}

fn emit_differences<F>(old: &HashMapById, new: &HashMapById, emit: &mut F)
where
    F: FnMut(&str, &SnapshotEntry),
{
    let all_ids: HashSet<&String> = old.keys().chain(new.keys()).collect();
    for id in all_ids {
        let old_hash = old.get(id).map(|e| e.hash);
        let new_entry = new.get(id);
        if old_hash != new_entry.map(|e| e.hash) {
            if let Some(new_entry) = new_entry {
                emit(id, new_entry);
            }
        }
    }
}

pub fn unix_timestamp_ms() -> io::Result<u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?;
    Ok(now.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::write_cache_file;
    use tempfile::TempDir;

    fn temp_sync_dir() -> TempDir {
        tempfile::Builder::new()
            .prefix("tngl-sync")
            .tempdir()
            .unwrap()
    }

    fn hex_hash(hash: Blake3Hash) -> String {
        hash.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn dummy_entry(hash: Blake3Hash, lamport: u64) -> SnapshotEntry {
        SnapshotEntry {
            hash,
            lamport,
            changed_at_ms: lamport,
            origin: "peer-a".into(),
            mtime_ms: lamport,
        }
    }

    #[test]
    fn build_snapshot_map_uses_mtime_for_scan() {
        let dir = temp_sync_dir();
        let filename = "hello.txt";
        fs::write(dir.path().join(filename), b"hello\n").unwrap();
        let entries = build_snapshot_map(dir.path(), "peer-a", None).unwrap();
        let entry = entries.get(filename).unwrap();
        assert_eq!(entry.hash, *blake3::hash(b"hello\n").as_bytes());
        assert_eq!(entry.lamport, 0);
        assert!(entry.changed_at_ms > 0);
        assert!(entry.mtime_ms > 0);
        assert_eq!(entry.origin, "peer-a");
    }

    #[test]
    fn build_snapshot_map_skips_hashing_when_mtime_matches_cache() {
        let dir = temp_sync_dir();
        let filename = "data.bin";
        fs::write(dir.path().join(filename), b"original").unwrap();

        // Prime a cache entry with a sentinel hash and matching mtime.
        let mtime = file_changed_at_ms(&dir.path().join(filename)).unwrap();
        let sentinel_hash = [0xAB; 32];
        let cache: HashMapById = HashMap::from([(
            filename.to_string(),
            SnapshotEntry {
                hash: sentinel_hash,
                lamport: 5,
                changed_at_ms: 42,
                origin: "peer-b".into(),
                mtime_ms: mtime,
            },
        )]);

        // build_snapshot_map should return the cached entry without re-reading the file.
        let entries = build_snapshot_map(dir.path(), "peer-a", Some(&cache)).unwrap();
        let entry = entries.get(filename).unwrap();
        // Returns cached values, not freshly computed ones.
        assert_eq!(entry.hash, sentinel_hash);
        assert_eq!(entry.lamport, 5);
        assert_eq!(entry.changed_at_ms, 42);
        assert_eq!(entry.origin, "peer-b");
    }

    #[test]
    fn build_snapshot_map_rehashes_when_mtime_differs() {
        let dir = temp_sync_dir();
        let filename = "data.bin";
        fs::write(dir.path().join(filename), b"changed").unwrap();

        let cache: HashMapById = HashMap::from([(
            filename.to_string(),
            SnapshotEntry {
                hash: [0xAB; 32],
                lamport: 5,
                changed_at_ms: 42,
                origin: "peer-b".into(),
                mtime_ms: 1, // stale mtime
            },
        )]);

        let entries = build_snapshot_map(dir.path(), "peer-a", Some(&cache)).unwrap();
        let entry = entries.get(filename).unwrap();
        // Should have freshly computed hash, not the sentinel.
        assert_eq!(entry.hash, *blake3::hash(b"changed").as_bytes());
        assert_ne!(entry.hash, [0xAB; 32]);
    }

    #[test]
    fn build_snapshot_map_recurses_into_subdirectories() {
        let dir = temp_sync_dir();
        fs::create_dir_all(dir.path().join("sub")).unwrap();
        fs::write(dir.path().join("sub/file.txt"), b"nested\n").unwrap();
        let entries = build_snapshot_map(dir.path(), "peer-a", None).unwrap();
        assert!(entries.contains_key("sub/file.txt"), "{entries:?}");
    }

    #[test]
    fn build_snapshot_map_excludes_state_dir() {
        let dir = temp_sync_dir();
        fs::create_dir_all(dir.path().join(".tngl")).unwrap();
        fs::write(dir.path().join(".tngl/iroh.key"), b"secret").unwrap();
        let entries = build_snapshot_map(dir.path(), "peer-a", None).unwrap();
        assert!(entries.is_empty(), "{entries:?}");
    }

    #[test]
    fn build_snapshot_map_excludes_ds_store() {
        let dir = temp_sync_dir();
        fs::write(dir.path().join(".DS_Store"), b"metadata").unwrap();
        fs::write(dir.path().join("file1.txt"), b"content").unwrap();
        let entries = build_snapshot_map(dir.path(), "peer-a", None).unwrap();
        assert!(!entries.contains_key(".DS_Store"));
        assert!(entries.contains_key("file1.txt"));
    }

    #[test]
    fn build_snapshot_map_excludes_windows_and_appledouble_metadata_files() {
        let dir = temp_sync_dir();
        fs::write(dir.path().join("Thumbs.db"), b"metadata").unwrap();
        fs::write(dir.path().join("Desktop.ini"), b"metadata").unwrap();
        fs::write(dir.path().join("._file1.txt"), b"metadata").unwrap();
        fs::write(dir.path().join(".directory"), b"[Desktop Entry]").unwrap();
        fs::write(dir.path().join("file1.txt"), b"content").unwrap();
        let entries = build_snapshot_map(dir.path(), "peer-a", None).unwrap();
        assert!(!entries.contains_key("Thumbs.db"));
        assert!(!entries.contains_key("Desktop.ini"));
        assert!(!entries.contains_key("._file1.txt"));
        assert!(entries.contains_key(".directory"));
        assert!(entries.contains_key("file1.txt"));
    }

    #[test]
    fn build_snapshot_map_excludes_os_metadata_directories() {
        let dir = temp_sync_dir();
        fs::create_dir_all(dir.path().join(".Spotlight-V100")).unwrap();
        fs::write(dir.path().join(".Spotlight-V100/store.db"), b"metadata").unwrap();
        fs::create_dir_all(dir.path().join("$RECYCLE.BIN")).unwrap();
        fs::write(dir.path().join("$RECYCLE.BIN/entry.txt"), b"metadata").unwrap();
        fs::write(dir.path().join("file1.txt"), b"content").unwrap();
        let entries = build_snapshot_map(dir.path(), "peer-a", None).unwrap();
        assert!(!entries.keys().any(|id| id.starts_with(".Spotlight-V100/")));
        assert!(!entries.keys().any(|id| id.starts_with("$RECYCLE.BIN/")));
        assert!(entries.contains_key("file1.txt"));
    }

    #[test]
    fn build_snapshot_map_respects_notngl() {
        let dir = temp_sync_dir();
        fs::write(
            dir.path().join(".notngl"),
            "ignored.txt\nlogs/\n*.tmp\n/sub/exact.txt\n",
        )
        .unwrap();
        fs::write(dir.path().join("ignored.txt"), b"nope").unwrap();
        fs::write(dir.path().join("keep.txt"), b"yes").unwrap();
        fs::write(dir.path().join("note.tmp"), b"tmp").unwrap();
        fs::create_dir_all(dir.path().join("logs")).unwrap();
        fs::write(dir.path().join("logs/a.txt"), b"log").unwrap();
        fs::create_dir_all(dir.path().join("sub")).unwrap();
        fs::write(dir.path().join("sub/exact.txt"), b"exact").unwrap();
        fs::write(dir.path().join("sub/keep.txt"), b"keep").unwrap();

        let entries = build_snapshot_map(dir.path(), "peer-a", None).unwrap();
        assert!(entries.contains_key(".notngl"));
        assert!(entries.contains_key("keep.txt"));
        assert!(entries.contains_key("sub/keep.txt"));
        assert!(!entries.contains_key("ignored.txt"));
        assert!(!entries.contains_key("note.tmp"));
        assert!(!entries.contains_key("logs/a.txt"));
        assert!(!entries.contains_key("sub/exact.txt"));
    }

    #[test]
    fn startup_reconcile_creates_tombstone_for_removed_cached_entry() {
        let dir = temp_sync_dir();
        fs::create_dir_all(dir.path().join(".tngl")).unwrap();
        let cache_path = dir.path().join(".tngl/daemon.cache");
        let removed = "gone.txt".to_string();
        write_cache_file(
            &cache_path,
            &HashMap::from([(
                removed.clone(),
                dummy_entry(*blake3::hash(b"old\n").as_bytes(), 7),
            )]),
            8,
        )
        .unwrap();
        let mut lines = Vec::new();
        let entries = reconcile_startup_state(dir.path(), &cache_path, "peer-a", |id, entry| {
            lines.push(format!(
                "{} {} {}",
                entry.changed_at_ms,
                id,
                hex_hash(entry.hash)
            ));
        })
        .unwrap();
        let entry = entries.get(&removed).unwrap();
        assert_eq!(entry.hash, TOMBSTONE_HASH);
        assert!(entry.changed_at_ms > 0);
        assert!(entry.lamport > 0);
        assert_eq!(entry.origin, "peer-a");
        assert_eq!(lines.len(), 1);
    }

    #[test]
    fn startup_reconcile_tombstones_cached_entry_now_ignored() {
        let dir = temp_sync_dir();
        fs::create_dir_all(dir.path().join(".tngl")).unwrap();
        fs::write(dir.path().join(".notngl"), "ignored.txt\n").unwrap();
        let cache_path = dir.path().join(".tngl/daemon.cache");
        let ignored = "ignored.txt".to_string();
        write_cache_file(
            &cache_path,
            &HashMap::from([(
                ignored.clone(),
                dummy_entry(*blake3::hash(b"old\n").as_bytes(), 7),
            )]),
            8,
        )
        .unwrap();

        let entries =
            reconcile_startup_state(dir.path(), &cache_path, "peer-a", |_, _| {}).unwrap();
        let entry = entries.get(&ignored).unwrap();
        assert_eq!(entry.hash, TOMBSTONE_HASH);
        assert_eq!(entry.origin, "peer-a");
    }

    #[test]
    fn local_event_suppresses_remote_apply_echo() {
        let dir = temp_sync_dir();
        let id = "file.txt".to_string();
        fs::write(dir.path().join(&id), b"remote\n").unwrap();
        let remote = SnapshotEntry {
            hash: *blake3::hash(b"remote\n").as_bytes(),
            lamport: 4,
            changed_at_ms: 42,
            origin: "peer-b".into(),
            mtime_ms: 42,
        };
        let mut entries = HashMap::from([(id.clone(), remote.clone())]);
        let mut suppressed = HashMap::from([(id.clone(), remote.clone())]);
        let event = Event {
            kind: EventKind::Modify(ModifyKind::Any),
            paths: vec![dir.path().join(&id)],
            attrs: Default::default(),
        };
        let mut lamport = 4;
        let updates = process_local_event(
            dir.path(),
            &mut lamport,
            &mut entries,
            &mut suppressed,
            "peer-a",
            event,
        )
        .unwrap();
        assert!(updates.is_empty());
        assert!(suppressed.is_empty());
        assert_eq!(entries.get(&id), Some(&remote));
    }

    #[test]
    fn directory_event_rescans_new_files() {
        let dir = temp_sync_dir();
        let id = "newfile.txt".to_string();
        fs::write(dir.path().join(&id), b"kind=test\n").unwrap();
        let event = Event {
            kind: EventKind::Create(CreateKind::Any),
            paths: vec![dir.path().to_path_buf()],
            attrs: Default::default(),
        };
        let mut lamport = 0;
        let mut entries = HashMap::new();
        let mut suppressed = HashMap::new();
        let updates = process_local_event(
            dir.path(),
            &mut lamport,
            &mut entries,
            &mut suppressed,
            "peer-a",
            event,
        )
        .unwrap();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].id, id);
        assert_eq!(entries.len(), 1);
        assert!(lamport > 0);
    }

    #[test]
    fn ignore_file_change_triggers_rescan() {
        let dir = temp_sync_dir();
        let id = "keep.txt".to_string();
        fs::write(dir.path().join(&id), b"keep\n").unwrap();
        let mut entries = build_snapshot_map(dir.path(), "peer-a", None).unwrap();
        let mut lamport = entries
            .values()
            .map(|entry| entry.lamport)
            .max()
            .unwrap_or(0);
        let mut suppressed = HashMap::new();

        fs::write(dir.path().join(".notngl"), "keep.txt\n").unwrap();
        let event = Event {
            kind: EventKind::Modify(ModifyKind::Any),
            paths: vec![dir.path().join(".notngl")],
            attrs: Default::default(),
        };
        let updates = process_local_event(
            dir.path(),
            &mut lamport,
            &mut entries,
            &mut suppressed,
            "peer-a",
            event,
        )
        .unwrap();

        assert!(updates.iter().any(|entry| entry.id == ".notngl"));
        assert!(
            updates
                .iter()
                .any(|entry| entry.id == "keep.txt" && entry.hash == TOMBSTONE_HASH)
        );
    }

    #[test]
    fn creating_file_in_ignored_directory_is_ignored() {
        let dir = temp_sync_dir();
        fs::write(dir.path().join(".notngl"), "files/\nfiles2/\n").unwrap();
        fs::create_dir_all(dir.path().join("files2")).unwrap();
        fs::write(dir.path().join("files2/file1.txt"), b"ignored\n").unwrap();

        let mut lamport = 0;
        let mut entries = HashMap::new();
        let mut suppressed = HashMap::new();
        let event = Event {
            kind: EventKind::Create(CreateKind::Any),
            paths: vec![dir.path().join("files2")],
            attrs: Default::default(),
        };
        let updates = process_local_event(
            dir.path(),
            &mut lamport,
            &mut entries,
            &mut suppressed,
            "peer-a",
            event,
        )
        .unwrap();

        assert!(updates.is_empty());
        assert!(!entries.contains_key("files2/file1.txt"));
        assert_eq!(lamport, 0);
    }

    #[test]
    fn ds_store_event_is_ignored() {
        let dir = temp_sync_dir();
        let id = ".DS_Store".to_string();
        fs::write(dir.path().join(&id), b"metadata").unwrap();
        let event = Event {
            kind: EventKind::Create(CreateKind::File),
            paths: vec![dir.path().join(&id)],
            attrs: Default::default(),
        };
        let mut lamport = 0;
        let mut entries = HashMap::new();
        let mut suppressed = HashMap::new();
        let updates = process_local_event(
            dir.path(),
            &mut lamport,
            &mut entries,
            &mut suppressed,
            "peer-a",
            event,
        )
        .unwrap();
        assert!(updates.is_empty());
        assert!(entries.is_empty());
        assert_eq!(lamport, 0);
    }

    #[test]
    fn windows_metadata_event_is_ignored() {
        let dir = temp_sync_dir();
        let id = "Thumbs.db".to_string();
        fs::write(dir.path().join(&id), b"metadata").unwrap();
        let event = Event {
            kind: EventKind::Create(CreateKind::File),
            paths: vec![dir.path().join(&id)],
            attrs: Default::default(),
        };
        let mut lamport = 0;
        let mut entries = HashMap::new();
        let mut suppressed = HashMap::new();
        let updates = process_local_event(
            dir.path(),
            &mut lamport,
            &mut entries,
            &mut suppressed,
            "peer-a",
            event,
        )
        .unwrap();
        assert!(updates.is_empty());
        assert!(entries.is_empty());
        assert_eq!(lamport, 0);
    }

    #[test]
    fn apply_remote_allows_ellipsis_in_filename() {
        let dir = temp_sync_dir();
        let tmp = dir.path().join("test-tmp");
        let content = b"data";
        fs::write(&tmp, content).unwrap();
        let filename = "files/file1... [id1].bin";
        let entry = ReplicatedEntry {
            id: filename.to_string(),
            hash: *blake3::hash(content).as_bytes(),
            lamport: 1,
            changed_at_ms: 1,
            origin: "peer-b".into(),
        };
        apply_remote_entry_to_disk(dir.path(), &entry, Some(&tmp)).unwrap();
        assert!(dir.path().join(filename).exists());
    }

    #[test]
    fn apply_remote_rejects_path_traversal() {
        let dir = temp_sync_dir();
        let tmp = dir.path().join("test-tmp");
        fs::write(&tmp, b"bad").unwrap();
        let entry = ReplicatedEntry {
            id: "../outside.txt".to_string(),
            hash: *blake3::hash(b"bad").as_bytes(),
            lamport: 1,
            changed_at_ms: 1,
            origin: "peer-b".into(),
        };
        assert!(apply_remote_entry_to_disk(dir.path(), &entry, Some(&tmp)).is_err());
    }

    #[test]
    fn apply_remote_creates_parent_directories() {
        let dir = temp_sync_dir();
        // Write content to a temp file as streaming would do.
        let tmp = dir.path().join("test-tmp");
        fs::write(&tmp, b"hello\n").unwrap();
        let entry = ReplicatedEntry {
            id: "sub/dir/file.txt".to_string(),
            hash: *blake3::hash(b"hello\n").as_bytes(),
            lamport: 1,
            changed_at_ms: 1,
            origin: "peer-b".into(),
        };
        apply_remote_entry_to_disk(dir.path(), &entry, Some(&tmp)).unwrap();
        assert_eq!(
            fs::read(dir.path().join("sub/dir/file.txt")).unwrap(),
            b"hello\n"
        );
    }

    #[test]
    fn tombstone_removes_directory_tree() {
        let dir = temp_sync_dir();
        fs::create_dir_all(dir.path().join("files/dir1/nested")).unwrap();
        fs::write(dir.path().join("files/dir1/nested/file1.txt"), b"hello\n").unwrap();
        let entry = ReplicatedEntry {
            id: "files/dir1".to_string(),
            hash: TOMBSTONE_HASH,
            lamport: 2,
            changed_at_ms: 2,
            origin: "peer-b".into(),
        };
        apply_remote_entry_to_disk(dir.path(), &entry, None).unwrap();
        assert!(!dir.path().join("files/dir1").exists());
    }

    #[test]
    fn tombstone_prunes_empty_parent_directories() {
        let dir = temp_sync_dir();
        fs::create_dir_all(dir.path().join("files/dir1/nested")).unwrap();
        fs::write(dir.path().join("files/dir1/nested/file1.txt"), b"hello\n").unwrap();
        let entry = ReplicatedEntry {
            id: "files/dir1/nested/file1.txt".to_string(),
            hash: TOMBSTONE_HASH,
            lamport: 2,
            changed_at_ms: 2,
            origin: "peer-b".into(),
        };
        apply_remote_entry_to_disk(dir.path(), &entry, None).unwrap();
        assert!(!dir.path().join("files/dir1/nested/file1.txt").exists());
        assert!(!dir.path().join("files/dir1/nested").exists());
        assert!(!dir.path().join("files/dir1").exists());
        assert!(!dir.path().join("files").exists());
    }

    #[test]
    fn removed_directory_event_rescans_descendants() {
        let dir = temp_sync_dir();
        fs::create_dir_all(dir.path().join("files/dir1")).unwrap();
        fs::write(dir.path().join("files/dir1/file1.txt"), b"a").unwrap();
        fs::write(dir.path().join("files/dir1/file2.txt"), b"b").unwrap();

        let mut entries = build_snapshot_map(dir.path(), "peer-a", None).unwrap();
        let mut lamport = entries
            .values()
            .map(|entry| entry.lamport)
            .max()
            .unwrap_or(0);
        let mut suppressed = HashMap::new();

        fs::remove_dir_all(dir.path().join("files/dir1")).unwrap();
        let event = Event {
            kind: EventKind::Modify(ModifyKind::Name(notify::event::RenameMode::Any)),
            paths: vec![dir.path().join("files/dir1")],
            attrs: Default::default(),
        };

        let updates = process_local_event(
            dir.path(),
            &mut lamport,
            &mut entries,
            &mut suppressed,
            "peer-a",
            event,
        )
        .unwrap();

        assert_eq!(updates.len(), 2);
        assert!(updates.iter().all(|entry| entry.hash == TOMBSTONE_HASH));
        assert!(
            updates
                .iter()
                .any(|entry| entry.id == "files/dir1/file1.txt")
        );
        assert!(
            updates
                .iter()
                .any(|entry| entry.id == "files/dir1/file2.txt")
        );
        assert!(!entries.contains_key("files/dir1"));
    }

    #[test]
    fn remote_newer_snapshot_wins_by_timestamp() {
        let current = dummy_entry(*blake3::hash(b"old\n").as_bytes(), 1);
        let remote = dummy_entry(*blake3::hash(b"new\n").as_bytes(), 2);
        assert!(should_accept_remote(Some(&current), &remote));
    }

    #[test]
    fn equal_lamport_uses_changed_at_ms_before_origin_and_hash() {
        let older = SnapshotEntry {
            hash: [9; 32],
            lamport: 5,
            changed_at_ms: 10,
            origin: "peer-z".into(),
            mtime_ms: 10,
        };
        let newer = SnapshotEntry {
            hash: [1; 32],
            lamport: 5,
            changed_at_ms: 11,
            origin: "peer-a".into(),
            mtime_ms: 11,
        };
        assert_eq!(compare_snapshot(&newer, &older), CmpOrdering::Greater);
    }

    #[test]
    fn equal_lamport_and_changed_at_uses_origin_then_hash() {
        let low_origin = SnapshotEntry {
            hash: [1; 32],
            lamport: 5,
            changed_at_ms: 10,
            origin: "peer-a".into(),
            mtime_ms: 10,
        };
        let high_origin = SnapshotEntry {
            hash: [1; 32],
            lamport: 5,
            changed_at_ms: 10,
            origin: "peer-b".into(),
            mtime_ms: 10,
        };
        assert_eq!(
            compare_snapshot(&high_origin, &low_origin),
            CmpOrdering::Greater
        );
    }
}
