use crate::entries::{Entry, EntryKind, placeholder_version, validate_symlink_target};
use crate::ignore::{IgnorePattern, load_ignore_patterns, should_ignore};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

const FILE_STABLE_AGE: Duration = Duration::from_secs(1);
pub(crate) const FILE_STABILITY_PAUSE: Duration = Duration::from_millis(250);
pub(crate) const FILE_STABILITY_ATTEMPTS: usize = 8;

pub(crate) struct ScanResult {
    pub(crate) entries: BTreeMap<String, Entry>,
    pub(crate) unstable: BTreeSet<String>,
}

pub(crate) struct ObservedFile {
    pub(crate) content_hash: [u8; 32],
    pub(crate) size: u64,
    pub(crate) mode: Option<u32>,
}

pub(crate) fn scan_folder(root: &Path) -> io::Result<ScanResult> {
    let mut entries = BTreeMap::new();
    let mut unstable = BTreeSet::new();
    let ignore_patterns = load_ignore_patterns(root)?;
    scan_dir(root, root, &ignore_patterns, &mut entries, &mut unstable)?;
    Ok(ScanResult { entries, unstable })
}

pub(crate) fn scan_dir(
    root: &Path,
    dir: &Path,
    ignore_patterns: &[IgnorePattern],
    entries: &mut BTreeMap<String, Entry>,
    unstable: &mut BTreeSet<String>,
) -> io::Result<()> {
    let mut children = Vec::new();
    for child in fs::read_dir(dir)? {
        let child = child?;
        children.push(child.path());
    }
    children.sort();

    for path in children {
        let relative = relative_path(root, &path)?;
        if should_ignore(&relative, ignore_patterns) {
            continue;
        }

        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            if let Some(target) = read_supported_symlink(&path)? {
                entries.insert(
                    relative.clone(),
                    Entry {
                        path: relative,
                        kind: EntryKind::Symlink,
                        content_hash: None,
                        symlink_target: Some(target),
                        size: 0,
                        mode: None,
                        version: placeholder_version(),
                    },
                );
            }
            continue;
        }

        let file_mode = mode(&metadata);
        if metadata.is_dir() {
            entries.insert(
                relative.clone(),
                Entry {
                    path: relative,
                    kind: EntryKind::Dir,
                    content_hash: None,
                    symlink_target: None,
                    size: 0,
                    mode: file_mode,
                    version: placeholder_version(),
                },
            );
            scan_dir(root, &path, ignore_patterns, entries, unstable)?;
        } else if metadata.is_file() {
            let Some(observed) = observe_file_when_stable(&path, metadata, false)? else {
                tracing::debug!("file still changing; skipping {relative}");
                unstable.insert(relative);
                continue;
            };
            entries.insert(
                relative.clone(),
                Entry {
                    path: relative,
                    kind: EntryKind::File,
                    content_hash: Some(observed.content_hash),
                    symlink_target: None,
                    size: observed.size,
                    mode: observed.mode,
                    version: placeholder_version(),
                },
            );
        }
    }

    Ok(())
}

pub(crate) fn read_supported_symlink(path: &Path) -> io::Result<Option<String>> {
    let target = fs::read_link(path)?;
    let Some(target) = target.to_str().map(|s| s.to_string()) else {
        return Ok(None);
    };
    if validate_symlink_target(&target).is_err() {
        return Ok(None);
    }
    Ok(Some(target))
}

pub(crate) fn observe_file_when_stable(
    path: &Path,
    mut before: fs::Metadata,
    wait_for_recent: bool,
) -> io::Result<Option<ObservedFile>> {
    for attempt in 0..FILE_STABILITY_ATTEMPTS {
        if wait_for_recent && is_recently_modified(&before) {
            std::thread::sleep(FILE_STABILITY_PAUSE);
            let after_pause = fs::symlink_metadata(path)?;
            if !after_pause.is_file() {
                return Ok(None);
            }
            before = after_pause;
            continue;
        }

        let content_hash = hash_file(path)?;
        let after_hash = fs::symlink_metadata(path)?;
        if !after_hash.is_file() {
            return Ok(None);
        }
        if same_file_observation(&before, &after_hash) {
            return Ok(Some(ObservedFile {
                content_hash,
                size: after_hash.len(),
                mode: mode(&after_hash),
            }));
        }

        before = after_hash;
        if attempt + 1 < FILE_STABILITY_ATTEMPTS {
            std::thread::sleep(FILE_STABILITY_PAUSE);
        }
    }
    Ok(None)
}

fn hash_file(path: &Path) -> io::Result<[u8; 32]> {
    let mut file = fs::File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(*hasher.finalize().as_bytes())
}

fn same_file_observation(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.len() == right.len()
        && left.modified().ok() == right.modified().ok()
        && mode(left) == mode(right)
}

fn is_recently_modified(metadata: &fs::Metadata) -> bool {
    let Ok(modified) = metadata.modified() else {
        return false;
    };
    SystemTime::now()
        .duration_since(modified)
        .map(|age| age < FILE_STABLE_AGE)
        .unwrap_or(false)
}

#[cfg(unix)]
pub(crate) fn mode(metadata: &fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt;
    Some(metadata.permissions().mode())
}

#[cfg(not(unix))]
pub(crate) fn mode(_metadata: &fs::Metadata) -> Option<u32> {
    None
}

pub(crate) fn relative_path(root: &Path, path: &Path) -> io::Result<String> {
    let relative = path
        .strip_prefix(root)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    Ok(relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/"))
}

pub(crate) fn normalize_event_path(path: &Path) -> Option<PathBuf> {
    if path.parent().is_none() {
        return fs::canonicalize(path).ok();
    }
    path.parent()
        .and_then(|parent| fs::canonicalize(parent).ok())
        .map(|parent| parent.join(path.file_name().unwrap_or_default()))
}
