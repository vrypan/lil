use crate::group::MemberEntry;
use crate::message::GossipMessage;
use crate::state::{Entry, TreeNode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum RequestMessage {
    Join {
        request_id: u64,
        secret: String,
        joiner_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    },
    GetRoot {
        request_id: u64,
    },
    GetNode {
        request_id: u64,
        prefix: String,
    },
    GetEntry {
        request_id: u64,
        path: String,
    },
    GetObject {
        request_id: u64,
        content_hash: [u8; 32],
    },
    Announce {
        request_id: u64,
        message: GossipMessage,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ResponseMessage {
    JoinAccepted {
        request_id: u64,
        members: Vec<MemberEntry>,
    },
    Ok {
        request_id: u64,
    },
    JoinRejected {
        request_id: u64,
        reason: String,
    },
    Root {
        request_id: u64,
        state_root: [u8; 32],
        live_root: [u8; 32],
        lamport: u64,
    },
    Node {
        request_id: u64,
        node: Option<TreeNode>,
    },
    Entry {
        request_id: u64,
        entry: Option<Entry>,
    },
    ObjectHeader {
        request_id: u64,
        size: u64,
    },
    Error {
        request_id: u64,
        message: String,
    },
}
