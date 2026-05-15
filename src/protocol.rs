use crate::group::MemberEntry;
use crate::state::{Entry, TreeNode};
use serde::{Deserialize, Serialize};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

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
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ResponseMessage {
    JoinAccepted {
        request_id: u64,
        topic_id: String,
        members: Vec<MemberEntry>,
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

pub async fn write_frame<T>(send: &mut iroh::endpoint::SendStream, value: &T) -> io::Result<()>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    send.write_u32(bytes.len() as u32).await?;
    send.write_all(&bytes).await?;
    Ok(())
}

pub async fn read_frame<T>(recv: &mut iroh::endpoint::RecvStream) -> io::Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let len = recv.read_u32().await? as usize;
    validate_frame_len(len)?;
    let mut bytes = vec![0; len];
    recv.read_exact(&mut bytes)
        .await
        .map_err(io::Error::other)?;
    serde_json::from_slice(&bytes).map_err(io::Error::other)
}

pub async fn close_send(send: &mut iroh::endpoint::SendStream) -> io::Result<()> {
    send.finish().map_err(io::Error::other)?;
    match send.stopped().await.map_err(io::Error::other)? {
        None => Ok(()),
        Some(code) => Err(io::Error::other(format!(
            "stream stopped by peer with code {code}"
        ))),
    }
}

fn validate_frame_len(len: usize) -> io::Result<()> {
    if len > MAX_FRAME_BYTES {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame length {len} exceeds maximum {MAX_FRAME_BYTES}"),
        ))
    } else {
        Ok(())
    }
}

pub async fn assert_eof(recv: &mut iroh::endpoint::RecvStream) -> io::Result<()> {
    let trailing = recv.read_to_end(1).await.map_err(io::Error::other)?;
    if trailing.is_empty() {
        Ok(())
    } else {
        Err(io::Error::other("unexpected trailing bytes after frame"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_frame_at_size_limit() {
        validate_frame_len(MAX_FRAME_BYTES).unwrap();
    }

    #[test]
    fn rejects_oversized_frame_before_allocation() {
        let err = validate_frame_len(MAX_FRAME_BYTES + 1).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
