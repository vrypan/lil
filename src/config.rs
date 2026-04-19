use iroh::{EndpointAddr, PublicKey};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub sync_dir: PathBuf,
    pub cache_path: PathBuf,
    pub key_path: PathBuf,
    pub peers_path: PathBuf,
    pub invites_path: PathBuf,
    pub peer_ids: Vec<PublicKey>,
    pub peers: Vec<EndpointAddr>,
    pub allow_peers: Vec<PublicKey>,
    pub show_id: bool,
    pub rescan: bool,
    pub invite_expire_secs: u64,
}
