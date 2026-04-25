use crate::gossip::GossipMessage;
use crate::protocol::{RequestMessage, ResponseMessage};
use crate::snapshot::SnapshotEntry;
use std::io;

pub(crate) fn sync_io_error(context: impl AsRef<str>, err: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("{}: {}", context.as_ref(), err))
}

pub(crate) fn emit_snapshot(id: &str, entry: &SnapshotEntry) {
    tracing::debug!(target: "tngl", id, hash = %hex4(&entry.hash), lamport = entry.lamport, "file changed");
}

pub(crate) fn summarize_request(request: &RequestMessage) -> String {
    match request {
        RequestMessage::GetNodes {
            request_id,
            path_prefix,
        } => format!("GetNodes #{request_id} prefix={path_prefix:?}"),
        RequestMessage::GetFiles { request_id, ids } => {
            format!("GetFiles #{request_id} count={}", ids.len())
        }
        RequestMessage::JoinRequest { joiner_id, .. } => {
            format!("JoinRequest joiner={joiner_id}")
        }
    }
}

pub(crate) fn summarize_response(response: &ResponseMessage) -> String {
    match response {
        ResponseMessage::Nodes { request_id, nodes } => {
            format!("Nodes #{request_id} count={}", nodes.len())
        }
        ResponseMessage::FilesBegin { request_id, count } => {
            format!("FilesBegin #{request_id} count={count}")
        }
        ResponseMessage::Error {
            request_id,
            message,
        } => format!("Error #{request_id} message={message:?}"),
        ResponseMessage::JoinAccepted { members, .. } => {
            format!("JoinAccepted members={}", members.len())
        }
        ResponseMessage::JoinRejected { reason } => {
            format!("JoinRejected reason={reason:?}")
        }
    }
}

pub(crate) fn summarize_gossip(message: &GossipMessage) -> String {
    match message {
        GossipMessage::FileChanged {
            origin,
            id,
            lamport,
            ..
        } => format!("FileChanged id={id:?} lamport={lamport} origin={origin}"),
        GossipMessage::SyncState {
            origin,
            root_hash,
            lamport,
        } => format!(
            "SyncState root={} lamport={lamport} origin={origin}",
            hex4(root_hash)
        ),
        GossipMessage::MemberList { origin, members } => {
            format!("MemberList members={} origin={origin}", members.len())
        }
    }
}

fn hex4(hash: &[u8; 32]) -> String {
    hash[..4].iter().map(|byte| format!("{byte:02x}")).collect()
}
