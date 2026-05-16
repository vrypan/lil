//! RPC message enums: `RequestMessage` and `ResponseMessage`. Defines the
//! wire protocol between RPC client and server (join, tree queries, file
//! transfer, peer list, gossip announce).

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
    },
    GetRoot {
        request_id: u64,
    },
    GetPeers {
        request_id: u64,
    },
    GetNode {
        request_id: u64,
        prefix: String,
    },
    GetEntries {
        request_id: u64,
        prefix: String,
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
    Peers {
        request_id: u64,
        members: Vec<MemberEntry>,
    },
    Node {
        request_id: u64,
        node: Option<TreeNode>,
    },
    Entries {
        request_id: u64,
        entries: Vec<Entry>,
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
