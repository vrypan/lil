use iroh::PublicKey;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::Path;

/// The on-disk format of peers.json.
///
/// Holds everything needed to restart and rejoin the group: the gossip
/// topic and the complete list of known peer node IDs (excluding self).
#[derive(Serialize, Deserialize, Clone, Default)]
pub(crate) struct PeersFile {
    /// Gossip topic ID (hex). Absent until the node has joined a group.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<String>,
    /// All known group members (excluding self), as node ID strings.
    #[serde(default)]
    pub peers: Vec<String>,
}

impl PeersFile {
    pub fn peer_ids(&self) -> Vec<PublicKey> {
        self.peers.iter().filter_map(|p| p.parse().ok()).collect()
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

pub(crate) fn add_peer(path: &Path, id: &PublicKey) -> io::Result<()> {
    let mut pf = load_peers(path)?;
    let id_str = id.to_string();
    if !pf.peers.contains(&id_str) {
        pf.peers.push(id_str);
        save_peers_file(path, &pf)?;
    }
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn remove_peer(path: &Path, id: &PublicKey) -> io::Result<()> {
    let mut pf = load_peers(path)?;
    let id_str = id.to_string();
    pf.peers.retain(|p| p != &id_str);
    save_peers_file(path, &pf)
}

/// Write the complete group state (topic + full member list) in one operation.
///
/// Used by the join flow: after `JoinAccepted` is received, the joiner writes
/// the full peers.json it was handed by the inviter.
pub(crate) fn save_group_state(
    path: &Path,
    topic_id: Option<&str>,
    members: &[String],
) -> io::Result<()> {
    let pf = PeersFile {
        topic_id: topic_id.map(str::to_string),
        peers: members.to_vec(),
    };
    save_peers_file(path, &pf)
}
