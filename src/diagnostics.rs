use crate::protocol::{RequestMessage, ResponseMessage};
use crate::snapshot::SnapshotEntry;
use std::io;

pub(crate) fn sync_debug_enabled() -> bool {
    std::env::var_os("TNGL_DEBUG_SYNC").is_some() || std::env::var_os("STASHD_DEBUG_SYNC").is_some()
}

pub(crate) fn sync_trace(message: impl AsRef<str>) {
    if sync_debug_enabled() {
        eprintln!("tngl sync: {}", message.as_ref());
    }
}

pub(crate) fn sync_trace_request(
    direction: &str,
    peer: impl std::fmt::Display,
    request: &RequestMessage,
) {
    sync_trace(format!(
        "{direction} {peer}: {}",
        summarize_request(request)
    ));
}

pub(crate) fn sync_trace_response(
    direction: &str,
    peer: impl std::fmt::Display,
    response: &ResponseMessage,
) {
    sync_trace(format!(
        "{direction} {peer}: {}",
        summarize_response(response)
    ));
}

pub(crate) fn sync_io_error(context: impl AsRef<str>, err: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("{}: {}", context.as_ref(), err))
}

pub(crate) fn emit_snapshot(id: &str, entry: &SnapshotEntry) {
    eprintln!("{} {} {}", entry.changed_at_ms, id, hex_hash(entry.hash));
}

fn hex_hash(hash: [u8; 32]) -> String {
    hash.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn summarize_request(request: &RequestMessage) -> String {
    match request {
        RequestMessage::GetNodes {
            request_id,
            path_prefix,
        } => format!("GetNodes request_id={request_id} prefix={path_prefix:?}"),
        RequestMessage::GetFiles { request_id, ids } => {
            format!("GetFiles request_id={request_id} ids={}", ids.len())
        }
        RequestMessage::JoinRequest { joiner_id, .. } => {
            format!("JoinRequest joiner_id={joiner_id}")
        }
    }
}

fn summarize_response(response: &ResponseMessage) -> String {
    match response {
        ResponseMessage::Nodes { request_id, nodes } => {
            format!("Nodes request_id={request_id} nodes={}", nodes.len())
        }
        ResponseMessage::FilesBegin { request_id, count } => {
            format!("FilesBegin request_id={request_id} count={count}")
        }
        ResponseMessage::Error {
            request_id,
            message,
        } => format!("Error request_id={request_id} message={message}"),
        ResponseMessage::JoinAccepted { members, .. } => {
            format!("JoinAccepted members={}", members.len())
        }
        ResponseMessage::JoinRejected { reason } => format!("JoinRejected reason={reason}"),
    }
}
