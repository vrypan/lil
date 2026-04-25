use crate::snapshot::{Blake3Hash, HashMapById, SnapshotEntry};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Hash of a file leaf node in the sync tree.
///
/// Includes lamport so a version bump — even with identical content — produces
/// a different hash. This ensures replication events are detected even when
/// file content did not change but origin or clock did.
pub fn leaf_hash(entry: &SnapshotEntry) -> Blake3Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&entry.hash);
    hasher.update(&entry.lamport.to_le_bytes());
    hasher.update(&entry.changed_at_ms.to_le_bytes());
    hasher.update(entry.origin.as_bytes());
    *hasher.finalize().as_bytes()
}

/// Hash of a directory node in the sync tree.
///
/// Computed from all immediate children sorted lexicographically by name.
/// Children may be file leaves or subdirectory nodes.
pub fn dir_hash(children: &BTreeMap<String, Blake3Hash>) -> Blake3Hash {
    let mut hasher = blake3::Hasher::new();
    for (name, hash) in children {
        hasher.update(name.as_bytes());
        hasher.update(&[0]); // name/hash separator
        hasher.update(hash);
    }
    *hasher.finalize().as_bytes()
}

/// Root hash of the sync tree.
///
/// Describes the complete folder state. Two nodes with the same root hash
/// are fully synchronized. Returns all-zeros for an empty tree.
#[allow(dead_code)] // used when SyncState gossip is published
pub fn root_hash(entries: &HashMapById) -> Blake3Hash {
    subtree_hash(entries, "")
}

fn subtree_hash(entries: &HashMapById, path_prefix: &str) -> Blake3Hash {
    let nodes = children_of(entries, path_prefix);
    if nodes.is_empty() {
        return [0u8; 32];
    }
    let child_hashes: BTreeMap<String, Blake3Hash> =
        nodes.into_iter().map(|n| (n.name, n.hash)).collect();
    dir_hash(&child_hashes)
}

/// One node returned by `get_nodes`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TreeNodeInfo {
    pub name: String,
    pub hash: Blake3Hash,
    pub is_dir: bool,
}

/// Returns the immediate children of the node at `path_prefix`.
///
/// Used by the `GetNodes` RPC. The caller compares each child hash against its
/// own tree: for differing directory nodes it recurses; for differing file
/// leaves it collects the path for `GetFiles`. An empty `path_prefix` returns
/// the children of the root.
pub fn get_nodes(entries: &HashMapById, path_prefix: &str) -> Vec<TreeNodeInfo> {
    children_of(entries, path_prefix)
}

fn children_of(entries: &HashMapById, path_prefix: &str) -> Vec<TreeNodeInfo> {
    let prefix_slash = if path_prefix.is_empty() {
        String::new()
    } else {
        format!("{path_prefix}/")
    };

    // Collect unique direct children: name → is_dir.
    // A name is a directory if any entry has it as an interior path component.
    let mut child_names: BTreeMap<String, bool> = BTreeMap::new();
    for id in entries.keys() {
        let rest = if prefix_slash.is_empty() {
            id.as_str()
        } else if let Some(r) = id.strip_prefix(&prefix_slash) {
            r
        } else {
            continue;
        };
        if let Some(slash_pos) = rest.find('/') {
            // Interior component — directory child; overwrite any file entry.
            child_names.insert(rest[..slash_pos].to_string(), true);
        } else {
            // Direct file child — only record as file if not already seen as dir.
            child_names.entry(rest.to_string()).or_insert(false);
        }
    }

    child_names
        .into_iter()
        .map(|(name, is_dir)| {
            let child_path = if prefix_slash.is_empty() {
                name.clone()
            } else {
                format!("{path_prefix}/{name}")
            };
            let hash = if is_dir {
                subtree_hash(entries, &child_path)
            } else if let Some(entry) = entries.get(&child_path) {
                leaf_hash(entry)
            } else {
                [0u8; 32]
            };
            TreeNodeInfo { name, hash, is_dir }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::SnapshotEntry;
    use std::collections::HashMap;

    fn entry(hash_byte: u8, lamport: u64) -> SnapshotEntry {
        SnapshotEntry {
            hash: [hash_byte; 32],
            lamport,
            changed_at_ms: lamport,
            origin: "node-a".into(),
            mtime_ms: lamport,
        }
    }

    #[test]
    fn root_hash_is_zero_for_empty_tree() {
        let entries: HashMapById = HashMap::new();
        assert_eq!(root_hash(&entries), [0u8; 32]);
    }

    #[test]
    fn root_hash_changes_when_entry_changes() {
        let mut entries: HashMapById = HashMap::from([("foo.txt".into(), entry(1, 1))]);
        let h1 = root_hash(&entries);
        entries.insert("foo.txt".into(), entry(2, 2));
        let h2 = root_hash(&entries);
        assert_ne!(h1, h2);
    }

    #[test]
    fn root_hash_changes_when_lamport_changes_even_with_same_content() {
        let mut entries: HashMapById = HashMap::from([("foo.txt".into(), entry(1, 1))]);
        let h1 = root_hash(&entries);
        // Same content hash, different lamport.
        entries.insert(
            "foo.txt".into(),
            SnapshotEntry {
                lamport: 2,
                ..entry(1, 1)
            },
        );
        let h2 = root_hash(&entries);
        assert_ne!(h1, h2);
    }

    #[test]
    fn get_nodes_root_returns_direct_children() {
        let entries: HashMapById = HashMap::from([
            ("README.md".into(), entry(1, 1)),
            ("src/main.rs".into(), entry(2, 1)),
            ("src/lib.rs".into(), entry(3, 1)),
        ]);
        let mut nodes = get_nodes(&entries, "");
        nodes.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].name, "README.md");
        assert!(!nodes[0].is_dir);
        assert_eq!(nodes[1].name, "src");
        assert!(nodes[1].is_dir);
    }

    #[test]
    fn get_nodes_subdir_returns_files_in_that_dir() {
        let entries: HashMapById = HashMap::from([
            ("src/main.rs".into(), entry(1, 1)),
            ("src/lib.rs".into(), entry(2, 1)),
            ("docs/guide.md".into(), entry(3, 1)),
        ]);
        let mut nodes = get_nodes(&entries, "src");
        nodes.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].name, "lib.rs");
        assert!(!nodes[0].is_dir);
        assert_eq!(nodes[1].name, "main.rs");
        assert!(!nodes[1].is_dir);
    }

    #[test]
    fn editing_file_changes_parent_dir_hash_and_root() {
        let entries1: HashMapById = HashMap::from([
            ("src/foo/bar.rs".into(), entry(1, 1)),
            ("src/foo/baz.rs".into(), entry(2, 1)),
            ("README.md".into(), entry(3, 1)),
        ]);
        let mut entries2 = entries1.clone();
        entries2.insert("src/foo/bar.rs".into(), entry(99, 5));

        // Root changed.
        assert_ne!(root_hash(&entries1), root_hash(&entries2));

        // src/foo changed.
        let foo1 = subtree_hash(&entries1, "src/foo");
        let foo2 = subtree_hash(&entries2, "src/foo");
        assert_ne!(foo1, foo2);

        // README.md leaf hash unchanged.
        let readme_hash = |entries: &HashMapById| leaf_hash(entries.get("README.md").unwrap());
        assert_eq!(readme_hash(&entries1), readme_hash(&entries2));
    }
}
