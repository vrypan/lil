use crate::gossip::{MemberEntry, MemberStatus};
use iroh::PublicKey;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::Path;

/// The on-disk format of peers.json.
///
/// Holds everything needed to restart and rejoin the group: the gossip
/// topic and the full membership ledger.
#[derive(Serialize, Deserialize, Clone, Default)]
pub(crate) struct PeersFile {
    /// Gossip topic ID (hex). Absent until the node has joined a group.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<String>,
    /// Full membership ledger.
    #[serde(default)]
    pub members: Vec<MemberEntry>,
    /// Legacy active-peer list kept only for migration from older peers.json files.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub peers: Vec<String>,
}

impl PeersFile {
    pub fn peer_ids(&self) -> Vec<PublicKey> {
        self.member_entries()
            .into_iter()
            .filter(|entry| entry.status == MemberStatus::Active)
            .filter_map(|entry| entry.id.parse().ok())
            .collect()
    }

    pub fn member_entries(&self) -> Vec<MemberEntry> {
        if !self.members.is_empty() {
            return self.members.clone();
        }
        self.peers
            .iter()
            .map(|id| MemberEntry {
                id: id.clone(),
                status: MemberStatus::Active,
                lamport: 0,
            })
            .collect()
    }
}

pub(crate) fn load_peers(path: &Path) -> io::Result<PeersFile> {
    match fs::read_to_string(path) {
        Ok(s) => serde_json::from_str(&s).map_err(io::Error::other),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(PeersFile::default()),
        Err(e) => Err(e),
    }
}

fn save_peers_file(path: &Path, pf: &PeersFile) -> io::Result<()> {
    let json = serde_json::to_string_pretty(pf).map_err(io::Error::other)?;
    fs::write(path, json)
}

pub(crate) fn set_topic_id(path: &Path, topic_id: &str) -> io::Result<()> {
    let mut pf = load_peers(path)?;
    if pf.topic_id.as_deref() == Some(topic_id) {
        return Ok(());
    }
    pf.topic_id = Some(topic_id.to_string());
    save_peers_file(path, &pf)
}

pub(crate) fn add_peer(path: &Path, id: &PublicKey) -> io::Result<()> {
    let pf = load_peers(path)?;
    let id_str = id.to_string();
    let mut members = pf.member_entries();
    let next_lamport = members.iter().map(|entry| entry.lamport).max().unwrap_or(0) + 1;
    if let Some(existing) = members.iter_mut().find(|entry| entry.id == id_str) {
        if existing.status == MemberStatus::Active {
            return Ok(());
        }
        existing.status = MemberStatus::Active;
        existing.lamport = next_lamport;
    } else {
        members.push(MemberEntry {
            id: id_str,
            status: MemberStatus::Active,
            lamport: next_lamport,
        });
    }
    save_group_state(path, pf.topic_id.as_deref(), &members)
}

#[allow(dead_code)]
pub(crate) fn remove_peer(path: &Path, id: &PublicKey) -> io::Result<()> {
    let pf = load_peers(path)?;
    let id_str = id.to_string();
    let mut members = pf.member_entries();
    let next_lamport = members.iter().map(|entry| entry.lamport).max().unwrap_or(0) + 1;
    if let Some(existing) = members.iter_mut().find(|entry| entry.id == id_str) {
        existing.status = MemberStatus::Removed;
        existing.lamport = next_lamport;
    } else {
        members.push(MemberEntry {
            id: id_str,
            status: MemberStatus::Removed,
            lamport: next_lamport,
        });
    }
    save_group_state(path, pf.topic_id.as_deref(), &members)
}

/// Write the complete group state (topic + full member list) in one operation.
///
/// Used by the join flow: after `JoinAccepted` is received, the joiner writes
/// the full peers.json it was handed by the inviter.
pub(crate) fn save_group_state(
    path: &Path,
    topic_id: Option<&str>,
    members: &[MemberEntry],
) -> io::Result<()> {
    let mut members = members.to_vec();
    members.sort_by(|left, right| left.id.cmp(&right.id));
    let pf = PeersFile {
        members,
        topic_id: topic_id.map(str::to_string),
        peers: Vec::new(),
    };
    save_peers_file(path, &pf)
}
