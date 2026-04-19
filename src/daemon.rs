use crate::cache::write_cache_file;
use crate::config::Config;
use crate::diagnostics::emit_snapshot;
use crate::gossip::{GossipMessage, MemberEntry, MemberStatus};
use crate::invite::{add_invite, generate_token, now_ms};
use crate::peers::{load_peers, save_group_state};
use crate::snapshot::{
    HashMapById, ReplicatedEntry, SnapshotEntry,
    apply_remote_entries, canonical_path, collect_file_send_list, process_local_event,
    reconcile_startup_state, snapshot_from_wire,
};
use crate::sync_tree::{TreeNodeInfo, get_nodes};
use crate::transport::{broadcast_entries, handle_incoming, send_join_request, sync_peer};
use iroh::{Endpoint, EndpointAddr, PublicKey, SecretKey, Watcher};
use notify::{Config as NotifyConfig, Event, RecommendedWatcher, RecursiveMode, Watcher as _};
use signal_hook::consts::signal::SIGTERM;
use signal_hook::flag;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{RwLock, mpsc, oneshot};

pub(crate) const ALPN: &[u8] = b"tngl/folder/1";
const DAEMON_CACHE_VERSION: u32 = 8;
const RESYNC_INTERVAL: Duration = Duration::from_secs(30);

pub(crate) enum Command {
    LocalFs(Event),
    ApplyRemote {
        peer: PublicKey,
        entries: Vec<(ReplicatedEntry, Option<PathBuf>)>,
        respond_to: Option<oneshot::Sender<()>>,
    },
    GetLocalNodes {
        path_prefix: String,
        respond_to: oneshot::Sender<Vec<TreeNodeInfo>>,
    },
    CollectFileSendList {
        ids: Vec<String>,
        respond_to: oneshot::Sender<io::Result<Vec<(ReplicatedEntry, Option<PathBuf>)>>>,
    },
    SyncPeer(EndpointAddr),
    AddPeer {
        id: PublicKey,
    },
}

struct State {
    sync_dir: std::path::PathBuf,
    cache_path: std::path::PathBuf,
    endpoint: Endpoint,
    local_origin: String,
    peers: Vec<EndpointAddr>,
    sync_peers: Arc<tokio::sync::Mutex<Vec<EndpointAddr>>>,
    lamport: u64,
    entries: HashMapById,
    suppressed: HashMap<String, SnapshotEntry>,
}

pub fn run(config: Config) -> io::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(io::Error::other)?;
    runtime.block_on(run_async(config))
}

async fn run_async(config: Config) -> io::Result<()> {
    let secret_key = load_or_create_secret_key(&config.key_path)?;
    let endpoint = Endpoint::builder()
        .alpns(vec![ALPN.to_vec()])
        .secret_key(secret_key.clone())
        .bind()
        .await
        .map_err(io::Error::other)?;

    if config.show_id {
        println!("{}", endpoint.id());
        return Ok(());
    }

    let sync_dir = canonical_path(config.sync_dir);
    let cache_path = config.cache_path;
    let tmp_dir: Arc<Path> = sync_dir.join(".tngl").into();
    print_node_info(&endpoint)?;

    // Load peers from peers.json.
    let saved_peers_file = load_peers(&config.peers_path)?;
    let mut peers: Vec<EndpointAddr> = Vec::new();

    // CLI-supplied peers.
    for p in &config.peers {
        peers.push(p.clone());
    }
    for peer_id in &config.peer_ids {
        peers.push(EndpointAddr::new(*peer_id));
    }
    // Persisted peers from peers.json.
    for id in saved_peers_file.peer_ids() {
        peers.push(EndpointAddr::new(id));
    }

    let mut allowlist: HashSet<PublicKey> = config.allow_peers.into_iter().collect();
    for peer_id in &config.peer_ids {
        allowlist.insert(*peer_id);
    }
    for peer in &peers {
        allowlist.insert(peer.id);
    }
    let local_origin = endpoint.id().to_string();
    let terminated = Arc::new(AtomicBool::new(false));
    flag::register(SIGTERM, Arc::clone(&terminated)).map_err(io::Error::other)?;

    if config.rescan {
        match fs::remove_file(&cache_path) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
    }

    let entries =
        reconcile_startup_state(&sync_dir, &cache_path, &local_origin, emit_snapshot)?;
    write_cache_file(&cache_path, &entries, DAEMON_CACHE_VERSION)?;

    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

    // Filesystem watcher.
    let watch_tx = cmd_tx.clone();
    let mut watcher = RecommendedWatcher::new(
        move |result| match result {
            Ok(event) => {
                let _ = watch_tx.send(Command::LocalFs(event));
            }
            Err(err) => eprintln!("watch error: {err}"),
        },
        NotifyConfig::default(),
    )
    .map_err(io::Error::other)?;
    watcher
        .watch(&sync_dir, RecursiveMode::Recursive)
        .map_err(io::Error::other)?;

    // Incoming connection acceptor.
    let accept_tx = cmd_tx.clone();
    let accept_allowlist = Arc::new(RwLock::new(allowlist));
    let accept_endpoint = endpoint.clone();
    let accept_invites_path: Arc<Path> = config.invites_path.clone().into();
    let accept_peers_path: Arc<Path> = config.peers_path.clone().into();
    let accept_local_id = endpoint.id();
    let accept_tmp_dir = Arc::clone(&tmp_dir);
    tokio::spawn(async move {
        loop {
            let Some(connecting) = accept_endpoint.accept().await else {
                break;
            };
            let accept_tx = accept_tx.clone();
            let accept_allowlist = Arc::clone(&accept_allowlist);
            let invites_path = Arc::clone(&accept_invites_path);
            let peers_path = Arc::clone(&accept_peers_path);
            let tmp_dir = Arc::clone(&accept_tmp_dir);
            tokio::spawn(async move {
                if let Err(err) = handle_incoming(
                    connecting,
                    accept_tx,
                    accept_allowlist,
                    invites_path,
                    peers_path,
                    accept_local_id,
                    tmp_dir,
                )
                .await
                {
                    eprintln!("incoming sync error: {err}");
                }
            });
        }
    });

    // Periodic resync task.
    let sync_peers = Arc::new(tokio::sync::Mutex::new(peers.clone()));
    let sync_peers_task = Arc::clone(&sync_peers);
    let sync_tx = cmd_tx.clone();
    tokio::spawn(async move {
        {
            let peers = sync_peers_task.lock().await;
            for peer in peers.iter() {
                let _ = sync_tx.send(Command::SyncPeer(peer.clone()));
            }
        }
        let mut interval = tokio::time::interval(RESYNC_INTERVAL);
        interval.tick().await;
        loop {
            interval.tick().await;
            let peers = sync_peers_task.lock().await.clone();
            for peer in &peers {
                let _ = sync_tx.send(Command::SyncPeer(peer.clone()));
            }
        }
    });

    let mut state = State {
        sync_dir,
        cache_path,
        endpoint,
        local_origin,
        peers,
        sync_peers,
        lamport: entries
            .values()
            .map(|entry| entry.lamport)
            .max()
            .unwrap_or(0),
        entries,
        suppressed: HashMap::new(),
    };

    while !terminated.load(Ordering::Relaxed) {
        let Some(command) = cmd_rx.recv().await else {
            break;
        };
        match command {
            Command::LocalFs(event) => {
                let updates = process_local_event(
                    &state.sync_dir,
                    &mut state.lamport,
                    &mut state.entries,
                    &mut state.suppressed,
                    &state.local_origin,
                    event,
                )?;
                if updates.is_empty() {
                    continue;
                }
                for update in &updates {
                    emit_snapshot(&update.id, &snapshot_from_wire(update));
                    // Publish FileChanged gossip announcement (when gossip is
                    // integrated, send over the topic here).
                    let _msg = GossipMessage::FileChanged {
                        origin: state.local_origin.clone(),
                        id: update.id.clone(),
                        hash: update.hash,
                        lamport: update.lamport,
                        changed_at_ms: update.changed_at_ms,
                    };
                    // TODO(gossip): topic.broadcast(_msg.to_bytes()).await;
                }
                write_cache_file(&state.cache_path, &state.entries, DAEMON_CACHE_VERSION)?;
                broadcast_entries(&state.endpoint, &state.peers, &updates, None, &state.sync_dir)
                    .await;
            }
            Command::ApplyRemote {
                peer,
                entries,
                respond_to,
            } => {
                let updates = apply_remote_entries(
                    &state.sync_dir,
                    &mut state.lamport,
                    &mut state.entries,
                    &mut state.suppressed,
                    &state.local_origin,
                    entries,
                    emit_snapshot,
                )?;
                if !updates.is_empty() {
                    write_cache_file(&state.cache_path, &state.entries, DAEMON_CACHE_VERSION)?;
                    broadcast_entries(
                        &state.endpoint,
                        &state.peers,
                        &updates,
                        Some(peer),
                        &state.sync_dir,
                    )
                    .await;
                }
                if let Some(reply) = respond_to {
                    let _ = reply.send(());
                }
            }
            Command::GetLocalNodes {
                path_prefix,
                respond_to,
            } => {
                let nodes = get_nodes(&state.entries, &path_prefix);
                let _ = respond_to.send(nodes);
            }
            Command::CollectFileSendList { ids, respond_to } => {
                let result = collect_file_send_list(
                    &state.sync_dir,
                    &mut state.entries,
                    &state.local_origin,
                    &mut state.lamport,
                    &ids,
                );
                let _ = respond_to.send(result);
            }
            Command::AddPeer { id } => {
                let peer_addr = EndpointAddr::new(id);
                state.peers.push(peer_addr.clone());
                state.sync_peers.lock().await.push(peer_addr.clone());
                let _ = cmd_tx.send(Command::SyncPeer(peer_addr));

                // Publish updated MemberList gossip (when gossip is integrated).
                let _msg = build_member_list_msg(&state.local_origin, &state.peers);
                // TODO(gossip): topic.broadcast(_msg.to_bytes()).await;
            }
            Command::SyncPeer(peer) => {
                let tx = cmd_tx.clone();
                let endpoint = state.endpoint.clone();
                let sync_dir = state.sync_dir.clone();
                tokio::spawn(async move {
                    let tmp_dir = sync_dir.join(".tngl");
                    if let Err(err) = sync_peer(endpoint, peer, tx, tmp_dir).await {
                        eprintln!("peer sync error: {err}");
                    }
                });
            }
        }
    }

    write_cache_file(&state.cache_path, &state.entries, DAEMON_CACHE_VERSION)?;
    Ok(())
}

fn build_member_list_msg(local_origin: &str, peers: &[EndpointAddr]) -> GossipMessage {
    let mut members: Vec<MemberEntry> = peers
        .iter()
        .map(|p| MemberEntry {
            id: p.id.to_string(),
            status: MemberStatus::Active,
            lamport: 0,
        })
        .collect();
    members.push(MemberEntry {
        id: local_origin.to_string(),
        status: MemberStatus::Active,
        lamport: 0,
    });
    GossipMessage::MemberList {
        origin: local_origin.to_string(),
        members,
    }
}

pub fn run_invite(config: Config) -> io::Result<()> {
    let secret_key = load_or_create_secret_key(&config.key_path)?;
    let node_id = secret_key.public();
    let token = generate_token()?;
    let expires_at = now_ms()? + config.invite_expire_secs * 1000;
    add_invite(&config.invites_path, &token, expires_at)?;
    println!("{node_id}:{token}");
    eprintln!(
        "tngl: invite valid for {} seconds",
        config.invite_expire_secs
    );
    Ok(())
}

pub fn run_join(config: Config, ticket: &str) -> io::Result<()> {
    let (host_id_str, token) = ticket
        .split_once(':')
        .ok_or_else(|| io::Error::other("invalid ticket: expected <node_id>:<token>"))?;
    let host_id: PublicKey = host_id_str
        .parse()
        .map_err(|_| io::Error::other("invalid node id in ticket"))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(io::Error::other)?;
    runtime.block_on(async {
        let secret_key = load_or_create_secret_key(&config.key_path)?;
        let endpoint = Endpoint::builder()
            .alpns(vec![ALPN.to_vec()])
            .secret_key(secret_key)
            .bind()
            .await
            .map_err(io::Error::other)?;

        let (topic_id, mut members) = send_join_request(&endpoint, host_id, token).await?;

        // The inviter includes itself in the members list it sends back;
        // ensure self is not included.
        let self_id = endpoint.id().to_string();
        members.retain(|m| m != &self_id);

        save_group_state(&config.peers_path, topic_id.as_deref(), &members)?;
        eprintln!("tngl: joined successfully");
        Ok(())
    })
}

fn load_or_create_secret_key(path: &Path) -> io::Result<SecretKey> {
    if let Ok(bytes) = fs::read(path) {
        let array: [u8; 32] = bytes
            .try_into()
            .map_err(|_| io::Error::other("invalid iroh key file length"))?;
        return Ok(SecretKey::from_bytes(&array));
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut bytes = [0u8; 32];
    fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    fs::write(path, bytes)?;
    Ok(SecretKey::from_bytes(&bytes))
}

fn print_node_info(endpoint: &Endpoint) -> io::Result<()> {
    let addr = endpoint.watch_addr().get();
    let addr_json = serde_json::to_string(&addr).map_err(io::Error::other)?;
    eprintln!("tngl node-id {}", endpoint.id());
    eprintln!("tngl peer {addr_json}");

    // Publish SyncState gossip on startup (when gossip is integrated).
    // TODO(gossip): topic.broadcast(SyncState { origin, root_hash, lamport }.to_bytes()).await;

    Ok(())
}
