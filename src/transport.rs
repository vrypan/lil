use crate::daemon::Command;
use crate::diagnostics::{sync_io_error, sync_trace, sync_trace_request, sync_trace_response};
use crate::invite::consume_invite;
use crate::peers::add_peer;
use crate::protocol::{
    FileHeader, RequestMessage, ResponseMessage, assert_eof, close_send, read_frame,
    read_stream_to_temp, write_file_to_stream, write_frame,
};
use crate::snapshot::{ReplicatedEntry, TOMBSTONE_HASH};
use crate::sync_tree::TreeNodeInfo;
use iroh::endpoint::ConnectionError;
use iroh::{Endpoint, EndpointAddr, PublicKey};
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{RwLock, mpsc, oneshot};

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

fn is_routine_connection_close(err: &ConnectionError) -> bool {
    matches!(
        err,
        ConnectionError::ApplicationClosed(_) | ConnectionError::LocallyClosed
    )
}

fn next_request_id() -> u64 {
    NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
}

// ─── daemon channel helpers ──────────────────────────────────────────────────

async fn get_local_nodes(
    path_prefix: &str,
    tx: &mpsc::UnboundedSender<Command>,
) -> io::Result<Vec<TreeNodeInfo>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(Command::GetLocalNodes {
        path_prefix: path_prefix.to_string(),
        respond_to: reply_tx,
    })
    .map_err(|_| io::Error::other("GetLocalNodes channel closed"))?;
    reply_rx
        .await
        .map_err(|_| io::Error::other("GetLocalNodes dropped"))
}

async fn collect_file_send_list(
    ids: &[String],
    peer_id: PublicKey,
    tx: &mpsc::UnboundedSender<Command>,
) -> io::Result<Vec<(ReplicatedEntry, Option<PathBuf>)>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(Command::CollectFileSendList {
        ids: ids.to_vec(),
        respond_to: reply_tx,
    })
    .map_err(|_| {
        io::Error::other(format!(
            "CollectFileSendList for peer {peer_id} channel closed"
        ))
    })?;
    reply_rx
        .await
        .map_err(|_| {
            io::Error::other(format!(
                "CollectFileSendList for peer {peer_id} dropped"
            ))
        })?
}

// ─── streaming helpers ───────────────────────────────────────────────────────

/// Send a slice of (entry, optional path) pairs as streaming FileHeader frames.
///
/// Writes the frames and content bytes but does **not** close the send stream.
async fn stream_send_list(
    send: &mut iroh::endpoint::SendStream,
    send_list: &[(ReplicatedEntry, Option<PathBuf>)],
    context: &str,
) -> io::Result<()> {
    for (entry, path_opt) in send_list {
        let size = if entry.hash != TOMBSTONE_HASH {
            if let Some(path) = path_opt {
                tokio::fs::metadata(path).await.map(|m| m.len()).unwrap_or(0)
            } else {
                0
            }
        } else {
            0
        };
        let header = FileHeader {
            id: entry.id.clone(),
            hash: entry.hash,
            lamport: entry.lamport,
            changed_at_ms: entry.changed_at_ms,
            origin: entry.origin.clone(),
            size,
        };
        write_frame(send, &header)
            .await
            .map_err(|err| sync_io_error(format!("{context}: write FileHeader for {}", entry.id), err))?;
        if size > 0 {
            if let Some(path) = path_opt {
                write_file_to_stream(send, path)
                    .await
                    .map_err(|err| sync_io_error(format!("{context}: stream content for {}", entry.id), err))?;
            }
        }
    }
    Ok(())
}

/// Receive `count` FileHeader frames + content from `recv`, writing content to
/// temp files under `tmp_dir`. Returns `(ReplicatedEntry, Option<PathBuf>)` pairs.
async fn stream_recv_list(
    recv: &mut iroh::endpoint::RecvStream,
    count: usize,
    tmp_dir: &Path,
    request_id: u64,
    context: &str,
) -> io::Result<Vec<(ReplicatedEntry, Option<PathBuf>)>> {
    let mut result = Vec::with_capacity(count);
    for i in 0..count {
        let header: FileHeader = read_frame(recv)
            .await
            .map_err(|err| sync_io_error(format!("{context}: read FileHeader #{i}"), err))?;
        let temp_path = if header.size > 0 {
            let tmp = tmp_dir.join(format!("recv-{request_id}-{i}"));
            if let Err(err) =
                read_stream_to_temp(recv, header.size, &header.hash, &tmp).await
            {
                // Clean up all temp files written so far on error.
                for (_, p) in &result {
                    if let Some(p) = p {
                        let _ = std::fs::remove_file(p);
                    }
                }
                let _ = std::fs::remove_file(&tmp);
                return Err(sync_io_error(
                    format!("{context}: stream content for {}", header.id),
                    err,
                ));
            }
            Some(tmp)
        } else {
            None
        };
        result.push((ReplicatedEntry::from(&header), temp_path));
    }
    Ok(result)
}

// ─── incoming connection handler ─────────────────────────────────────────────

pub(crate) async fn handle_incoming(
    incoming: iroh::endpoint::Incoming,
    tx: mpsc::UnboundedSender<Command>,
    allowlist: Arc<RwLock<HashSet<PublicKey>>>,
    invites_path: Arc<Path>,
    peers_path: Arc<Path>,
    local_id: PublicKey,
    tmp_dir: Arc<Path>,
) -> io::Result<()> {
    // Failures here (timeout, authentication error, QUIC protocol errors) come
    // from nodes that can't complete a QUIC handshake with us — random internet
    // probes, nodes with wrong certificates, etc. Treat all of them as routine.
    let connection = match incoming.await {
        Ok(conn) => conn,
        Err(err) => {
            sync_trace(format!("incoming connection rejected at handshake: {err}"));
            return Ok(());
        }
    };
    let peer = connection.remote_id();
    sync_trace(format!("incoming connection from {peer}"));

    loop {
        let (mut send, mut recv) = match connection.accept_bi().await {
            Ok(streams) => streams,
            Err(err) => {
                if is_routine_connection_close(&err) {
                    sync_trace(format!("incoming connection from {peer} closed: {err}"));
                    return Ok(());
                }
                return Err(sync_io_error(
                    format!("incoming peer {peer}: accept bi stream"),
                    err,
                ));
            }
        };
        // read_frame does not assert EOF — more data may follow for PushEntries.
        let request: RequestMessage = read_frame(&mut recv)
            .await
            .map_err(|err| sync_io_error(format!("incoming peer {peer}: read request"), err))?;
        sync_trace_request("recv", peer, &request);

        // Join requests are handled before the allowlist check — the token is the gate.
        if let RequestMessage::JoinRequest { token, joiner_id } = request {
            sync_trace(format!("incoming join request from {peer}"));
            let accepted = consume_invite(&invites_path, &token)?;
            if accepted {
                let joiner_key: PublicKey = joiner_id
                    .parse()
                    .map_err(|_| io::Error::other("invalid joiner_id in join request"))?;
                add_peer(&peers_path, &joiner_key)?;
                allowlist.write().await.insert(joiner_key);
                let _ = tx.send(Command::AddPeer { id: joiner_key });
                // Build the full member list: all known peers plus self.
                let pf = crate::peers::load_peers(&peers_path)?;
                let mut members = pf.peers.clone();
                let self_id_str = local_id.to_string();
                if !members.contains(&self_id_str) {
                    members.push(self_id_str);
                }
                let response = ResponseMessage::JoinAccepted {
                    topic_id: pf.topic_id.clone(),
                    members,
                };
                sync_trace_response("send", peer, &response);
                write_frame(&mut send, &response)
                    .await
                    .map_err(|err| sync_io_error(format!("incoming peer {peer}: write join accepted"), err))?;
                close_send(&mut send)
                    .await
                    .map_err(|err| sync_io_error(format!("incoming peer {peer}: close send (join accepted)"), err))?;
                eprintln!("tngl: joined by {joiner_id}");
            } else {
                let response = ResponseMessage::JoinRejected {
                    reason: "invalid or expired token".into(),
                };
                sync_trace_response("send", peer, &response);
                write_frame(&mut send, &response)
                    .await
                    .map_err(|err| sync_io_error(format!("incoming peer {peer}: write join rejected"), err))?;
                close_send(&mut send)
                    .await
                    .map_err(|err| sync_io_error(format!("incoming peer {peer}: close send (join rejected)"), err))?;
                sync_trace(format!("join request from {peer}: invalid or expired token"));
            }
            continue;
        }

        // All other requests require the peer to be allowlisted.
        if !allowlist.read().await.contains(&peer) {
            sync_trace(format!("rejecting non-allowlisted peer {peer}"));
            continue;
        }

        match request {
            RequestMessage::GetNodes {
                request_id,
                path_prefix,
            } => {
                assert_eof(&mut recv).await.map_err(|err| {
                    sync_io_error(format!("incoming peer {peer}: trailing bytes after GetNodes"), err)
                })?;
                let nodes = get_local_nodes(&path_prefix, &tx).await?;
                let response = ResponseMessage::Nodes { request_id, nodes };
                sync_trace_response("send", peer, &response);
                write_frame(&mut send, &response)
                    .await
                    .map_err(|err| sync_io_error(format!("incoming peer {peer}: write nodes #{request_id}"), err))?;
                close_send(&mut send)
                    .await
                    .map_err(|err| sync_io_error(format!("incoming peer {peer}: close send (nodes #{request_id})"), err))?;
            }
            RequestMessage::GetFiles { request_id, ids } => {
                assert_eof(&mut recv).await.map_err(|err| {
                    sync_io_error(format!("incoming peer {peer}: trailing bytes after GetFiles"), err)
                })?;
                let send_list = collect_file_send_list(&ids, peer, &tx).await?;
                let count = send_list.len();
                let begin = ResponseMessage::FilesBegin { request_id, count };
                sync_trace_response("send", peer, &begin);
                write_frame(&mut send, &begin)
                    .await
                    .map_err(|err| sync_io_error(format!("incoming peer {peer}: write FilesBegin #{request_id}"), err))?;
                let ctx = format!("incoming peer {peer}: GetFiles #{request_id}");
                stream_send_list(&mut send, &send_list, &ctx).await?;
                close_send(&mut send)
                    .await
                    .map_err(|err| sync_io_error(format!("incoming peer {peer}: close send (files #{request_id})"), err))?;
            }
            RequestMessage::PushEntries {
                request_id,
                count,
            } => {
                let ctx = format!("incoming peer {peer}: PushEntries #{request_id}");
                let received =
                    stream_recv_list(&mut recv, count, &tmp_dir, request_id, &ctx).await?;
                assert_eof(&mut recv).await.map_err(|err| {
                    sync_io_error(format!("incoming peer {peer}: trailing bytes after PushEntries"), err)
                })?;
                let (reply_tx, reply_rx) = oneshot::channel();
                let _ = tx.send(Command::ApplyRemote {
                    peer,
                    entries: received,
                    respond_to: Some(reply_tx),
                });
                reply_rx.await.unwrap_or(());
                let response = ResponseMessage::Ack { request_id };
                sync_trace_response("send", peer, &response);
                write_frame(&mut send, &response)
                    .await
                    .map_err(|err| sync_io_error(format!("incoming peer {peer}: write ack #{request_id}"), err))?;
                close_send(&mut send)
                    .await
                    .map_err(|err| sync_io_error(format!("incoming peer {peer}: close send (ack #{request_id})"), err))?;
            }
            RequestMessage::JoinRequest { .. } => {
                unreachable!("JoinRequest handled above");
            }
        }
    }
}

// ─── outbound helpers ────────────────────────────────────────────────────────

/// One-frame request, one-frame response (used for GetNodes).
async fn round_trip(
    endpoint: &Endpoint,
    peer: &EndpointAddr,
    request: &RequestMessage,
) -> io::Result<ResponseMessage> {
    let peer_id = peer.id;
    let connection = endpoint
        .connect(peer.clone(), crate::daemon::ALPN)
        .await
        .map_err(|err| sync_io_error(format!("peer {peer_id}: connect"), err))?;
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|err| sync_io_error(format!("peer {peer_id}: open bi stream"), err))?;

    sync_trace_request("send", peer_id, request);
    write_frame(&mut send, request)
        .await
        .map_err(|err| sync_io_error(format!("peer {peer_id}: write request"), err))?;
    close_send(&mut send)
        .await
        .map_err(|err| sync_io_error(format!("peer {peer_id}: close send"), err))?;

    let response = read_frame(&mut recv)
        .await
        .map_err(|err| sync_io_error(format!("peer {peer_id}: read response"), err))?;
    assert_eof(&mut recv)
        .await
        .map_err(|err| sync_io_error(format!("peer {peer_id}: assert EOF after response"), err))?;
    sync_trace_response("recv", peer_id, &response);
    Ok(response)
}

async fn request_nodes(
    endpoint: &Endpoint,
    peer: &EndpointAddr,
    path_prefix: &str,
) -> io::Result<Vec<TreeNodeInfo>> {
    let request_id = next_request_id();
    let response = round_trip(
        endpoint,
        peer,
        &RequestMessage::GetNodes {
            request_id,
            path_prefix: path_prefix.to_string(),
        },
    )
    .await?;
    match response {
        ResponseMessage::Nodes {
            request_id: rid,
            nodes,
        } if rid == request_id => Ok(nodes),
        ResponseMessage::Error {
            request_id: rid,
            message,
        } if rid == request_id => Err(io::Error::other(message)),
        _ => Err(io::Error::other(format!(
            "peer {}: unexpected nodes response for #{request_id}",
            peer.id
        ))),
    }
}

/// Fetch files from a peer using streaming. Writes content to temp files under
/// `tmp_dir` and returns `(entry, Option<temp_path>)` pairs.
async fn request_files(
    endpoint: &Endpoint,
    peer: &EndpointAddr,
    ids: Vec<String>,
    tmp_dir: &Path,
) -> io::Result<Vec<(ReplicatedEntry, Option<PathBuf>)>> {
    let peer_id = peer.id;
    let request_id = next_request_id();

    let connection = endpoint
        .connect(peer.clone(), crate::daemon::ALPN)
        .await
        .map_err(|err| sync_io_error(format!("peer {peer_id}: connect for GetFiles"), err))?;
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|err| sync_io_error(format!("peer {peer_id}: open bi stream for GetFiles"), err))?;

    let req = RequestMessage::GetFiles { request_id, ids };
    sync_trace_request("send", peer_id, &req);
    write_frame(&mut send, &req)
        .await
        .map_err(|err| sync_io_error(format!("peer {peer_id}: write GetFiles #{request_id}"), err))?;
    close_send(&mut send)
        .await
        .map_err(|err| sync_io_error(format!("peer {peer_id}: close send (GetFiles #{request_id})"), err))?;

    let resp: ResponseMessage = read_frame(&mut recv)
        .await
        .map_err(|err| sync_io_error(format!("peer {peer_id}: read FilesBegin #{request_id}"), err))?;
    sync_trace_response("recv", peer_id, &resp);
    let count = match resp {
        ResponseMessage::FilesBegin {
            request_id: rid,
            count,
        } if rid == request_id => count,
        ResponseMessage::Error {
            request_id: rid,
            message,
        } if rid == request_id => return Err(io::Error::other(message)),
        _ => {
            return Err(io::Error::other(format!(
                "peer {peer_id}: unexpected response to GetFiles #{request_id}"
            )));
        }
    };

    let ctx = format!("peer {peer_id}: GetFiles #{request_id}");
    let result = stream_recv_list(&mut recv, count, tmp_dir, request_id, &ctx).await?;
    assert_eof(&mut recv)
        .await
        .map_err(|err| sync_io_error(format!("peer {peer_id}: assert EOF after GetFiles"), err))?;
    Ok(result)
}

// ─── tree-based sync walk ────────────────────────────────────────────────────

/// Recursively walk the sync tree, comparing remote nodes against local state.
/// Recurses into differing directory nodes; collects differing file paths for
/// a single `GetFiles` call at each level.
pub(crate) async fn sync_peer(
    endpoint: Endpoint,
    peer: EndpointAddr,
    tx: mpsc::UnboundedSender<Command>,
    tmp_dir: PathBuf,
) -> io::Result<()> {
    sync_tree_prefix(&endpoint, &peer, &tx, "", &tmp_dir).await
}

fn sync_tree_prefix<'a>(
    endpoint: &'a Endpoint,
    peer: &'a EndpointAddr,
    tx: &'a mpsc::UnboundedSender<Command>,
    path_prefix: &'a str,
    tmp_dir: &'a Path,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let remote_nodes = request_nodes(endpoint, peer, path_prefix).await?;
        if remote_nodes.is_empty() {
            return Ok(());
        }

        let local_nodes = get_local_nodes(path_prefix, tx).await?;
        let local_map: HashMap<String, &TreeNodeInfo> =
            local_nodes.iter().map(|n| (n.name.clone(), n)).collect();

        let mut files_to_fetch: Vec<String> = Vec::new();

        for node in &remote_nodes {
            let in_sync = local_map
                .get(&node.name)
                .map(|local| local.hash == node.hash)
                .unwrap_or(false);
            if in_sync {
                continue;
            }
            let child_path = if path_prefix.is_empty() {
                node.name.clone()
            } else {
                format!("{path_prefix}/{}", node.name)
            };
            if node.is_dir {
                sync_tree_prefix(endpoint, peer, tx, &child_path, tmp_dir).await?;
            } else {
                files_to_fetch.push(child_path);
            }
        }

        if !files_to_fetch.is_empty() {
            let entries = request_files(endpoint, peer, files_to_fetch, tmp_dir).await?;
            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = tx.send(Command::ApplyRemote {
                peer: peer.id,
                entries,
                respond_to: Some(reply_tx),
            });
            reply_rx.await.unwrap_or(());
        }

        Ok(())
    })
}

// ─── join ────────────────────────────────────────────────────────────────────

/// Connect to `host_id`, present a join token, and return the group state
/// (`topic_id` and member list) on success.
pub(crate) async fn send_join_request(
    endpoint: &Endpoint,
    host_id: PublicKey,
    token: &str,
) -> io::Result<(Option<String>, Vec<String>)> {
    let joiner_id = endpoint.id().to_string();
    let peer_addr = EndpointAddr::new(host_id);
    let connection = endpoint
        .connect(peer_addr, crate::daemon::ALPN)
        .await
        .map_err(|err| sync_io_error("join: connect to host", err))?;
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|err| sync_io_error("join: open bi stream", err))?;

    let request = RequestMessage::JoinRequest {
        token: token.to_string(),
        joiner_id,
    };
    sync_trace_request("send", host_id, &request);
    write_frame(&mut send, &request)
        .await
        .map_err(|err| sync_io_error("join: write join request", err))?;
    close_send(&mut send)
        .await
        .map_err(|err| sync_io_error("join: close send", err))?;

    let response: ResponseMessage = read_frame(&mut recv)
        .await
        .map_err(|err| sync_io_error("join: read join response", err))?;
    assert_eof(&mut recv)
        .await
        .map_err(|err| sync_io_error("join: assert EOF after join response", err))?;
    sync_trace_response("recv", host_id, &response);

    match response {
        ResponseMessage::JoinAccepted { topic_id, members } => Ok((topic_id, members)),
        ResponseMessage::JoinRejected { reason } => {
            Err(io::Error::other(format!("host rejected join: {reason}")))
        }
        _ => Err(io::Error::other(
            "unexpected response from host during join",
        )),
    }
}

// ─── live push ───────────────────────────────────────────────────────────────

async fn send_live_event(
    endpoint: Endpoint,
    peer: EndpointAddr,
    entry: ReplicatedEntry,
    sync_dir: PathBuf,
) -> io::Result<()> {
    let peer_id = peer.id;
    let entry_id = entry.id.clone();
    let request_id = next_request_id();

    let connection = endpoint
        .connect(peer.clone(), crate::daemon::ALPN)
        .await
        .map_err(|err| sync_io_error(format!("peer {peer_id}: connect for push {entry_id}"), err))?;
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|err| sync_io_error(format!("peer {peer_id}: open bi stream for push {entry_id}"), err))?;

    // Send PushEntries header with count=1.
    let req = RequestMessage::PushEntries { request_id, count: 1 };
    sync_trace_request("send", peer_id, &req);
    write_frame(&mut send, &req)
        .await
        .map_err(|err| sync_io_error(format!("peer {peer_id}: write PushEntries for {entry_id}"), err))?;

    // Stream the single file entry.
    let send_list: Vec<(ReplicatedEntry, Option<PathBuf>)> = if entry.hash != TOMBSTONE_HASH {
        let path = sync_dir.join(&entry.id);
        vec![(entry, Some(path))]
    } else {
        vec![(entry, None)]
    };
    let ctx = format!("peer {peer_id}: PushEntries #{request_id}");
    stream_send_list(&mut send, &send_list, &ctx).await?;
    close_send(&mut send)
        .await
        .map_err(|err| sync_io_error(format!("peer {peer_id}: close send (push {entry_id})"), err))?;

    // Read Ack.
    let response: ResponseMessage = read_frame(&mut recv)
        .await
        .map_err(|err| sync_io_error(format!("peer {peer_id}: read ack for {entry_id}"), err))?;
    assert_eof(&mut recv)
        .await
        .map_err(|err| sync_io_error(format!("peer {peer_id}: assert EOF after ack for {entry_id}"), err))?;
    match response {
        ResponseMessage::Ack { request_id: rid } if rid == request_id => Ok(()),
        ResponseMessage::Error {
            request_id: rid,
            message,
        } if rid == request_id => Err(io::Error::other(message)),
        _ => Err(io::Error::other(format!(
            "peer {peer_id}: unexpected ack for live event {entry_id}"
        ))),
    }
}

pub(crate) async fn broadcast_entries(
    endpoint: &Endpoint,
    peers: &[EndpointAddr],
    entries: &[ReplicatedEntry],
    skip_peer: Option<PublicKey>,
    sync_dir: &Path,
) {
    for peer in peers {
        if skip_peer == Some(peer.id) {
            continue;
        }
        let endpoint = endpoint.clone();
        let peer = peer.clone();
        let entries = entries.to_vec();
        let sync_dir = sync_dir.to_path_buf();
        tokio::spawn(async move {
            for entry in entries {
                if let Err(err) =
                    send_live_event(endpoint.clone(), peer.clone(), entry, sync_dir.clone()).await
                {
                    eprintln!("live event send error: {err}");
                }
            }
        });
    }
}
