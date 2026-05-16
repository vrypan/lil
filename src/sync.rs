use crate::identity::NodeId;
use crate::message::TreeHint;
use crate::rpc::RpcClient;
use crate::state::{Change, EntryKind, FolderState, TreeNode, entry_hash, hex, tree_node_hash};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::{Notify, RwLock, Semaphore};
use tokio::task::JoinSet;

const MAX_CONCURRENT_FETCHES: usize = 8;

pub struct RemoteRoot {
    pub state_root: [u8; 32],
    pub live_root: [u8; 32],
    pub lamport: u64,
    pub hint: Option<TreeHint>,
}

#[derive(Clone, Default)]
pub struct DownloadCoordinator {
    active: Arc<Mutex<HashMap<[u8; 32], Arc<DownloadSlot>>>>,
}

struct DownloadSlot {
    state: Mutex<DownloadState>,
    notify: Notify,
}

#[derive(Default)]
struct DownloadState {
    waiters: usize,
    claimed: bool,
    result: Option<Result<PathBuf, String>>,
}

impl DownloadCoordinator {
    fn acquire(&self, content_hash: [u8; 32]) -> (Arc<DownloadSlot>, bool) {
        let mut active = self.active.lock().expect("download coordinator poisoned");
        if let Some(slot) = active.get(&content_hash) {
            slot.state.lock().expect("download slot poisoned").waiters += 1;
            return (Arc::clone(slot), false);
        }

        let slot = Arc::new(DownloadSlot {
            state: Mutex::new(DownloadState {
                waiters: 1,
                claimed: false,
                result: None,
            }),
            notify: Notify::new(),
        });
        active.insert(content_hash, Arc::clone(&slot));
        (slot, true)
    }

    fn claim(slot: &Arc<DownloadSlot>) -> bool {
        let mut state = slot.state.lock().expect("download slot poisoned");
        if state.claimed {
            false
        } else {
            state.claimed = true;
            true
        }
    }

    fn release(&self, content_hash: [u8; 32], slot: &Arc<DownloadSlot>) {
        let mut state = slot.state.lock().expect("download slot poisoned");
        state.waiters = state.waiters.saturating_sub(1);
        if state.waiters != 0 {
            return;
        }
        drop(state);

        let mut active = self.active.lock().expect("download coordinator poisoned");
        if active
            .get(&content_hash)
            .is_some_and(|active_slot| Arc::ptr_eq(active_slot, slot))
        {
            active.remove(&content_hash);
        }
    }
}

pub async fn reconcile_with_peer(
    rpc: RpcClient,
    state: Arc<RwLock<FolderState>>,
    downloads: DownloadCoordinator,
    peer: NodeId,
) -> io::Result<Vec<Change>> {
    let (remote_root, remote_live_root, remote_lamport) = rpc.get_root(peer).await?;
    reconcile_with_advertised_root(
        rpc,
        state,
        peer,
        RemoteRoot {
            state_root: remote_root,
            live_root: remote_live_root,
            lamport: remote_lamport,
            hint: None,
        },
        downloads,
    )
    .await
}

pub async fn reconcile_with_advertised_root(
    rpc: RpcClient,
    state: Arc<RwLock<FolderState>>,
    peer: NodeId,
    remote: RemoteRoot,
    downloads: DownloadCoordinator,
) -> io::Result<Vec<Change>> {
    tracing::info!(
        "sync peer={} remote_lamport={} state_root={} live_root={}",
        peer,
        remote.lamport,
        hex(remote.state_root),
        hex(remote.live_root)
    );

    let local_root = {
        let state = state.read().await;
        state.root_hash()
    };
    if local_root == remote.state_root {
        return Ok(Vec::new());
    }

    let hinted_nodes = remote.hint.map(HintedNodes::from).unwrap_or_default();
    let ctx = ReconcileContext {
        rpc: &rpc,
        state: &state,
        peer,
        hinted_nodes: &hinted_nodes,
        downloads: &downloads,
    };
    let mut changes = Vec::new();
    let mut stack = vec![("".to_string(), remote.state_root)];
    while let Some((prefix, remote_hash)) = stack.pop() {
        reconcile_node(&ctx, &prefix, remote_hash, &mut changes, &mut stack).await?;
    }

    // Post-reconciliation sweep: for every tombstone we accepted, re-run
    // tombstone_descendants so any entries that arrived concurrently (from
    // another reconciliation or watcher event) are cleaned up too.
    let tombstoned: Vec<PathBuf> = {
        let s = state.read().await;
        changes
            .iter()
            .filter(|c| c.new.kind == EntryKind::Tombstone)
            .map(|c| s.root().join(&c.path))
            .collect()
    };
    if !tombstoned.is_empty() {
        let _ = state.write().await.apply_paths(tombstoned);
    }

    if !changes.is_empty() {
        state.write().await.save_entries();
    }

    Ok(changes)
}

struct ReconcileContext<'a> {
    rpc: &'a RpcClient,
    state: &'a Arc<RwLock<FolderState>>,
    peer: NodeId,
    hinted_nodes: &'a HintedNodes,
    downloads: &'a DownloadCoordinator,
}

async fn reconcile_node(
    ctx: &ReconcileContext<'_>,
    prefix: &str,
    expected_remote_hash: [u8; 32],
    changes: &mut Vec<Change>,
    stack: &mut Vec<(String, [u8; 32])>,
) -> io::Result<()> {
    let local_node = {
        let state = ctx.state.read().await;
        state.node(prefix)
    };
    if local_node
        .as_ref()
        .is_some_and(|node| node.hash == expected_remote_hash)
    {
        return Ok(());
    }

    let remote_node = match ctx.hinted_nodes.get(prefix, expected_remote_hash) {
        Some(node) => node,
        None => ctx.rpc.get_node(ctx.peer, prefix).await?.ok_or_else(|| {
            io::Error::other(format!(
                "peer {} did not return node for prefix {prefix:?}",
                ctx.peer
            ))
        })?,
    };
    if remote_node.hash != expected_remote_hash {
        return Err(io::Error::other(format!(
            "peer {} returned stale node {}: expected {}, got {}",
            ctx.peer,
            remote_node.prefix,
            hex(expected_remote_hash),
            hex(remote_node.hash)
        )));
    }

    reconcile_entries(ctx, prefix, local_node.as_ref(), &remote_node, changes).await?;
    queue_changed_children(prefix, local_node.as_ref(), &remote_node, stack);
    Ok(())
}

#[derive(Default)]
struct HintedNodes {
    nodes: BTreeMap<String, TreeNode>,
}

impl HintedNodes {
    fn get(&self, prefix: &str, expected_hash: [u8; 32]) -> Option<TreeNode> {
        self.nodes
            .get(&normalize_prefix(prefix))
            .filter(|node| node.hash == expected_hash)
            .cloned()
    }
}

impl From<TreeHint> for HintedNodes {
    fn from(hint: TreeHint) -> Self {
        let nodes = hint
            .nodes
            .into_iter()
            .filter(|node| tree_node_hash(node) == node.hash)
            .map(|node| (normalize_prefix(&node.prefix), node))
            .collect();
        Self { nodes }
    }
}

async fn reconcile_entries(
    ctx: &ReconcileContext<'_>,
    prefix: &str,
    local_node: Option<&TreeNode>,
    remote_node: &TreeNode,
    changes: &mut Vec<Change>,
) -> io::Result<()> {
    // Phase 1: fetch metadata for all changed entries.
    let mut to_apply: Vec<crate::state::Entry> = Vec::new();
    for (name, remote_hash) in &remote_node.entries {
        if local_node
            .and_then(|node| node.entries.get(name))
            .is_some_and(|local_hash| local_hash == remote_hash)
        {
            continue;
        }
        let path = join_path(prefix, name);
        let Some(remote_entry) = ctx.rpc.get_entry(ctx.peer, &path).await? else {
            return Err(io::Error::other(format!(
                "peer {} did not return entry {path}",
                ctx.peer
            )));
        };
        if entry_hash(&remote_entry) != *remote_hash {
            return Err(io::Error::other(format!(
                "peer {} returned stale entry {path}: expected {}, got {}",
                ctx.peer,
                hex(*remote_hash),
                hex(entry_hash(&remote_entry))
            )));
        }
        if !ctx.state.read().await.should_accept_remote(&remote_entry) {
            continue;
        }
        to_apply.push(remote_entry);
    }

    // Phase 2: apply non-file entries immediately; download file entries in parallel.
    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_FETCHES));
    let mut join_set: JoinSet<io::Result<(crate::state::Entry, Option<PathBuf>)>> = JoinSet::new();

    for entry in to_apply {
        if entry.kind == EntryKind::File {
            let content_hash = entry.content_hash.ok_or_else(|| {
                io::Error::other(format!("remote file {} has no content hash", entry.path))
            })?;
            let rpc = ctx.rpc.clone();
            let state = Arc::clone(ctx.state);
            let downloads = ctx.downloads.clone();
            let peer = ctx.peer;
            let sem = Arc::clone(&sem);
            let expected_size = entry.size;
            join_set.spawn(async move {
                let _permit = sem.acquire().await.map_err(io::Error::other)?;
                let tmp_path = fetch_object_once(
                    downloads,
                    rpc,
                    state,
                    peer,
                    content_hash,
                    expected_size,
                    &entry,
                )
                .await?;
                Ok((entry, tmp_path))
            });
        } else {
            let mut state = ctx.state.write().await;
            if let Some(change) = state.apply_remote_entry(entry, None)? {
                log_applied(&change);
                changes.push(change);
            }
        }
    }

    // Phase 3: apply file entries as their downloads complete.
    while let Some(result) = join_set.join_next().await {
        let (entry, tmp_path) = result.map_err(io::Error::other)??;
        let Some(tmp_path) = tmp_path else {
            continue;
        };
        let mut state = ctx.state.write().await;
        if let Some(change) = state.apply_remote_entry(entry, Some(&tmp_path))? {
            log_applied(&change);
            changes.push(change);
        }
    }

    Ok(())
}

async fn fetch_object_once(
    downloads: DownloadCoordinator,
    rpc: RpcClient,
    state: Arc<RwLock<FolderState>>,
    peer: NodeId,
    content_hash: [u8; 32],
    expected_size: u64,
    entry: &crate::state::Entry,
) -> io::Result<Option<PathBuf>> {
    let (slot, leader) = downloads.acquire(content_hash);

    if leader {
        let shared_tmp_path = state.read().await.tmp_recv_path(entry);
        tracing::debug!(
            "download leader peer={} object={} path={}",
            peer,
            hex(content_hash),
            entry.path
        );
        let result = rpc
            .get_object_to_file(peer, content_hash, expected_size, &shared_tmp_path)
            .await
            .map(|_| shared_tmp_path)
            .map_err(|err| err.to_string());
        {
            let mut state = slot.state.lock().expect("download slot poisoned");
            state.result = Some(result);
        }
        slot.notify.notify_waiters();
    } else {
        tracing::debug!(
            "download waiting for in-flight object={} path={}",
            hex(content_hash),
            entry.path
        );
    }

    let shared_tmp_path = loop {
        let result = {
            slot.state
                .lock()
                .expect("download slot poisoned")
                .result
                .clone()
        };
        match result {
            Some(Ok(path)) => break path,
            Some(Err(err)) => {
                downloads.release(content_hash, &slot);
                return Err(io::Error::other(err));
            }
            None => slot.notify.notified().await,
        }
    };

    let claimed = DownloadCoordinator::claim(&slot);
    downloads.release(content_hash, &slot);
    if claimed {
        Ok(Some(shared_tmp_path))
    } else {
        tracing::debug!(
            "download joined in-flight object={} path={} already claimed",
            hex(content_hash),
            entry.path
        );
        Ok(None)
    }
}

fn log_applied(change: &Change) {
    tracing::info!(
        "sync applied {} {} v{}:{}",
        change.verb(),
        change.path,
        change.new.version.lamport,
        change.new.version.origin
    );
}

fn queue_changed_children(
    prefix: &str,
    local_node: Option<&TreeNode>,
    remote_node: &TreeNode,
    stack: &mut Vec<(String, [u8; 32])>,
) {
    let child_names: BTreeSet<String> = remote_node.children.keys().cloned().collect();
    for name in child_names {
        let remote_hash = remote_node
            .children
            .get(&name)
            .copied()
            .expect("name came from remote children");
        if local_node
            .and_then(|node| node.children.get(&name))
            .is_some_and(|local_hash| *local_hash == remote_hash)
        {
            continue;
        }
        let child_prefix = join_path(prefix, &name);
        stack.push((child_prefix, remote_hash));
    }
}

fn join_path(prefix: &str, name: &str) -> String {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}/{name}")
    }
}

fn normalize_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn download_coordinator_allows_one_claim_per_object() {
        let coordinator = DownloadCoordinator::default();
        let hash = [7; 32];
        let (first, first_leader) = coordinator.acquire(hash);
        let (second, second_leader) = coordinator.acquire(hash);

        assert!(first_leader);
        assert!(!second_leader);
        assert!(Arc::ptr_eq(&first, &second));
        assert!(DownloadCoordinator::claim(&first));
        assert!(!DownloadCoordinator::claim(&second));

        coordinator.release(hash, &first);
        coordinator.release(hash, &second);
        assert!(coordinator.active.lock().unwrap().is_empty());
    }
}
