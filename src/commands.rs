//! One-shot CLI subcommand implementations: `invite`, `peers`, `remove`,
//! `join`, `dump-state`, and the per-folder daemon lock.

use crate::cli::{encode_ticket, parse_join_ticket};
use crate::discovery::AddressBook;
use crate::group::{GroupState, add_invite, generate_secret, now_ms};
use crate::identity::Identity;
use crate::rpc::RpcClient;
use crate::state::{Entry, EntryKind, hex, load_stored_entries};
use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;

pub const KEY_FILE: &str = "private.key";
pub const PEERS_FILE: &str = "peers.json";
pub const INVITES_FILE: &str = "invites.json";

pub fn create_invite(state_dir: &Path, expire_secs: u64) -> io::Result<()> {
    let identity = Identity::load_or_create(&state_dir.join(KEY_FILE))?;
    let node_id = identity.node_id();
    let peers_path = state_dir.join(PEERS_FILE);
    let _group = GroupState::load_or_init(peers_path, node_id)?;
    let secret_bytes = generate_secret()?;
    let secret_hex = hex(secret_bytes);
    let expires_at = now_ms()? + expire_secs.saturating_mul(1000);
    add_invite(&state_dir.join(INVITES_FILE), &secret_hex, expires_at)?;
    println!("{}", encode_ticket(node_id, &secret_bytes));
    tracing::info!("invite expires in {}s", expire_secs);
    Ok(())
}

pub fn peers_cmd(state_dir: &Path) -> io::Result<()> {
    let identity = Identity::load_or_create(&state_dir.join(KEY_FILE))?;
    let node_id = identity.node_id();
    let peers_path = state_dir.join(PEERS_FILE);
    let group = GroupState::load_or_init(peers_path, node_id)?;
    for member in group.members() {
        let marker = if member.id == node_id.to_string() {
            " [self]"
        } else {
            ""
        };
        println!("{:?} {}{}", member.status, member.id, marker);
    }
    Ok(())
}

pub fn dump_state_cmd(folder: &Path, prefix: Option<&str>) -> io::Result<()> {
    let entries = load_stored_entries(folder)?;
    let prefix = prefix
        .map(|p| p.trim_matches('/'))
        .filter(|p| !p.is_empty());
    for entry in entries
        .values()
        .filter(|entry| entry_matches_prefix(entry, prefix))
    {
        println!("{}", entry_dump_json(entry)?);
    }
    Ok(())
}

fn entry_matches_prefix(entry: &Entry, prefix: Option<&str>) -> bool {
    let Some(prefix) = prefix else {
        return true;
    };
    entry.path == prefix
        || entry
            .path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}

fn entry_dump_json(entry: &Entry) -> io::Result<String> {
    let hash = entry.content_hash.map(hex);
    serde_json::to_string(&serde_json::json!({
        "path": entry.path,
        "kind": entry_kind_name(entry.kind),
        "hash": hash,
        "target": entry.symlink_target,
        "size": entry.size,
        "mode": entry.mode,
        "lamport": entry.version.lamport,
        "origin": entry.version.origin,
    }))
    .map_err(io::Error::other)
}

fn entry_kind_name(kind: EntryKind) -> &'static str {
    match kind {
        EntryKind::File => "file",
        EntryKind::Dir => "dir",
        EntryKind::Tombstone => "tombstone",
        EntryKind::Symlink => "symlink",
    }
}

pub fn remove_peer_cmd(state_dir: &Path, target: &str) -> io::Result<()> {
    let identity = Identity::load_or_create(&state_dir.join(KEY_FILE))?;
    let node_id = identity.node_id();
    let peers_path = state_dir.join(PEERS_FILE);
    let mut group = GroupState::load_or_init(peers_path, node_id)?;
    match group.remove_peer(target)? {
        Some(id) => {
            println!("removed {id}");
            println!("restart the daemon to broadcast the removal to other peers");
        }
        None => {
            eprintln!("no peer found matching {target:?}");
            std::process::exit(1);
        }
    }
    Ok(())
}

pub async fn join_group(
    identity: Arc<Identity>,
    address_book: AddressBook,
    peers_path: &Path,
    ticket: &str,
) -> io::Result<()> {
    let ticket = parse_join_ticket(ticket)?;
    let rpc_client = RpcClient::new(Arc::clone(&identity), address_book);
    let members = rpc_client
        .join_group(ticket.issuer, ticket.secret, identity.node_id())
        .await?;
    GroupState::replace(peers_path, members)?;
    tracing::info!("joined group via {}", ticket.issuer);
    Ok(())
}

#[cfg(unix)]
pub fn acquire_daemon_lock(state_dir: &Path) -> io::Result<fs::File> {
    use std::os::fd::AsRawFd;

    let lock_path = state_dir.join("daemon.lock");
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)?;
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock {
            return Err(io::Error::other(
                "another lil instance is already running on this folder",
            ));
        }
        return Err(err);
    }
    Ok(file)
}

#[cfg(not(unix))]
pub fn acquire_daemon_lock(state_dir: &Path) -> io::Result<fs::File> {
    let lock_path = state_dir.join("daemon.lock");
    fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(lock_path)
        .map_err(|err| {
            if err.kind() == io::ErrorKind::AlreadyExists {
                io::Error::other("another lil instance is already running on this folder")
            } else {
                err
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_key_is_reused_from_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let key_path = tmp.path().join(KEY_FILE);

        let first = Identity::load_or_create(&key_path).unwrap();
        let second = Identity::load_or_create(&key_path).unwrap();

        assert_eq!(first.node_id(), second.node_id());
        assert_eq!(fs::read(key_path).unwrap().len(), 32);
    }

    #[test]
    fn daemon_lock_rejects_second_holder() {
        let tmp = tempfile::tempdir().unwrap();

        let first = acquire_daemon_lock(tmp.path()).unwrap();
        let second = acquire_daemon_lock(tmp.path()).unwrap_err();

        assert_eq!(
            second.to_string(),
            "another lil instance is already running on this folder"
        );
        drop(first);
        let _third = acquire_daemon_lock(tmp.path()).unwrap();
    }
}
