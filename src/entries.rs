use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io;
use std::path::Path;

pub type GcWatermark = BTreeMap<String, u64>;

#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EntryKind {
    File,
    Dir,
    Tombstone,
    Symlink,
}

impl EntryKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Dir => "dir",
            Self::Tombstone => "tombstone",
            Self::Symlink => "symlink",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Version {
    pub lamport: u64,
    pub origin: String,
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        self.lamport
            .cmp(&other.lamport)
            .then_with(|| self.origin.cmp(&other.origin))
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Entry {
    pub path: String,
    pub kind: EntryKind,
    pub content_hash: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symlink_target: Option<String>,
    pub size: u64,
    pub mode: Option<u32>,
    pub version: Version,
}

#[derive(Debug, Clone)]
pub struct Change {
    pub path: String,
    pub old: Option<Entry>,
    pub new: Entry,
}

impl Change {
    pub fn verb(&self) -> &'static str {
        match (&self.old, self.new.kind) {
            (None, EntryKind::File) => "file new",
            (None, EntryKind::Dir) => "dir new",
            (None, EntryKind::Tombstone) => "tombstone new",
            (Some(_), EntryKind::Tombstone) => "delete",
            (Some(old), EntryKind::File) if old.kind == EntryKind::File => "file update",
            (Some(old), EntryKind::Dir) if old.kind == EntryKind::Dir => "dir update",
            (Some(old), EntryKind::Symlink) if old.kind == EntryKind::Symlink => "symlink update",
            (Some(_), EntryKind::File) => "file replace",
            (Some(_), EntryKind::Dir) => "dir replace",
            (None, EntryKind::Symlink) => "symlink new",
            (Some(_), EntryKind::Symlink) => "symlink replace",
        }
    }
}

pub fn tombstone_entry(path: &str, old: &Entry, version: Version) -> Entry {
    Entry {
        path: path.to_string(),
        kind: EntryKind::Tombstone,
        content_hash: None,
        symlink_target: None,
        size: 0,
        mode: old.mode,
        version,
    }
}

pub fn placeholder_version() -> Version {
    Version {
        lamport: 0,
        origin: String::new(),
    }
}

pub fn same_observed_state(left: &Entry, right: &Entry) -> bool {
    left.path == right.path
        && left.kind == right.kind
        && left.content_hash == right.content_hash
        && left.symlink_target == right.symlink_target
        && left.size == right.size
        && left.mode == right.mode
}

pub fn hex(hash: [u8; 32]) -> String {
    hash.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn validate_symlink_target(target: &str) -> io::Result<()> {
    if target.is_empty() || target.contains('\0') || target.contains('\\') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid symlink target {target:?}"),
        ));
    }
    let path = Path::new(target);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::Prefix(_)
                    | std::path::Component::RootDir
                    | std::path::Component::CurDir
                    | std::path::Component::ParentDir
            )
        })
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid symlink target {target:?}"),
        ));
    }
    Ok(())
}
