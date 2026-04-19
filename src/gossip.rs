use serde::{Deserialize, Serialize};

/// The status of a member entry in the membership ledger.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MemberStatus {
    Active,
    Removed,
}

/// One entry in a `MemberList` message.
///
/// Each entry carries the lamport at which this status was recorded. The merge
/// rule is uniform: apply if `entry.lamport > local lamport for that id`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberEntry {
    pub id: String,
    pub status: MemberStatus,
    pub lamport: u64,
}

/// A gossip message published to the group topic.
///
/// All messages carry an `origin` field (the publishing node ID) and use a
/// `kind` tag for serde dispatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum GossipMessage {
    /// Published on every local file write or deletion. No debouncing.
    ///
    /// Receivers call `should_accept_remote` before doing any network IO;
    /// only then do they fetch via `GetFiles`.
    FileChanged {
        origin: String,
        /// Relative path from sync root, forward slashes.
        id: String,
        /// blake3 of file content. All-zeros = tombstone (deletion).
        hash: [u8; 32],
        lamport: u64,
        changed_at_ms: u64,
    },
    /// Published on startup / topic rejoin. Announces the node's current
    /// sync tree state so peers can decide whether a sync is needed.
    SyncState {
        origin: String,
        /// Sync tree root hash. Peers compare against their own root hash.
        root_hash: [u8; 32],
        /// Current max lamport; fallback check when root hash not yet tracked.
        lamport: u64,
    },
    /// Published after a join or removal and periodically by every node.
    ///
    /// Carries the sender's complete membership ledger (active + removed).
    /// A single message conveys full group membership state. The merge rule
    /// per entry: apply if `entry.lamport > locally recorded lamport for id`.
    MemberList {
        origin: String,
        members: Vec<MemberEntry>,
    },
}

#[allow(dead_code)]
impl GossipMessage {
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("GossipMessage serialization is infallible")
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        serde_json::from_slice(bytes).ok()
    }

    pub fn origin(&self) -> &str {
        match self {
            GossipMessage::FileChanged { origin, .. } => origin,
            GossipMessage::SyncState { origin, .. } => origin,
            GossipMessage::MemberList { origin, .. } => origin,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_changed_roundtrips() {
        let msg = GossipMessage::FileChanged {
            origin: "node-abc".into(),
            id: "src/foo/bar.rs".into(),
            hash: [1u8; 32],
            lamport: 42,
            changed_at_ms: 1776330951115,
        };
        let bytes = msg.to_bytes();
        let parsed = GossipMessage::from_bytes(&bytes).unwrap();
        assert!(matches!(
            parsed,
            GossipMessage::FileChanged { lamport: 42, .. }
        ));
    }

    #[test]
    fn member_list_roundtrips() {
        let msg = GossipMessage::MemberList {
            origin: "node-x".into(),
            members: vec![
                MemberEntry {
                    id: "node-a".into(),
                    status: MemberStatus::Active,
                    lamport: 10,
                },
                MemberEntry {
                    id: "node-b".into(),
                    status: MemberStatus::Removed,
                    lamport: 15,
                },
            ],
        };
        let bytes = msg.to_bytes();
        let parsed = GossipMessage::from_bytes(&bytes).unwrap();
        if let GossipMessage::MemberList { members, .. } = parsed {
            assert_eq!(members.len(), 2);
            assert_eq!(members[1].status, MemberStatus::Removed);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn sync_state_roundtrips() {
        let msg = GossipMessage::SyncState {
            origin: "node-y".into(),
            root_hash: [0xAB; 32],
            lamport: 99,
        };
        let bytes = msg.to_bytes();
        let parsed = GossipMessage::from_bytes(&bytes).unwrap();
        assert!(matches!(
            parsed,
            GossipMessage::SyncState { lamport: 99, .. }
        ));
    }
}
