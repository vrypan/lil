use crate::snapshot::{Blake3Hash, ReplicatedEntry};
use crate::sync_tree::TreeNodeInfo;
use serde::{Deserialize, Serialize};
use std::io;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Serialize, Deserialize)]
pub enum RequestMessage {
    /// Walk the sync tree: returns immediate children of the node at `path_prefix`.
    /// An empty `path_prefix` returns the root's children.
    GetNodes {
        request_id: u64,
        path_prefix: String,
    },
    /// Fetch full file entries for the given paths.
    /// Response is `FilesBegin { count }` followed by `count` FileHeader frames,
    /// each optionally followed by `header.size` raw content bytes.
    GetFiles {
        request_id: u64,
        ids: Vec<String>,
    },
    /// Live push: `count` FileHeader frames + content bytes follow this frame
    /// before the send stream is closed. Response is `Ack`.
    PushEntries {
        request_id: u64,
        count: usize,
    },
    /// Join request: joiner presents a one-time token to an existing member.
    JoinRequest {
        token: String,
        joiner_id: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ResponseMessage {
    Nodes {
        request_id: u64,
        nodes: Vec<TreeNodeInfo>,
    },
    /// Header for a streaming file response. `count` FileHeader frames + content
    /// bytes follow before the send stream is closed.
    FilesBegin {
        request_id: u64,
        count: usize,
    },
    Ack {
        request_id: u64,
    },
    Error {
        request_id: u64,
        message: String,
    },
    /// Sent by the inviter on a successful join. Contains the topic and full
    /// member list so the new node can subscribe and sync immediately.
    JoinAccepted {
        topic_id: Option<String>,
        members: Vec<String>,
    },
    JoinRejected {
        reason: String,
    },
}

/// Per-file metadata frame sent before the raw content bytes.
///
/// Sent by both the GetFiles responder and the PushEntries initiator.
/// `size == 0` means this is a tombstone; no content bytes follow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileHeader {
    pub id: String,
    pub hash: Blake3Hash,
    pub lamport: u64,
    pub changed_at_ms: u64,
    pub origin: String,
    /// Byte length of the raw content that follows. Zero for tombstones.
    pub size: u64,
}

impl From<&FileHeader> for ReplicatedEntry {
    fn from(h: &FileHeader) -> Self {
        ReplicatedEntry {
            id: h.id.clone(),
            hash: h.hash,
            lamport: h.lamport,
            changed_at_ms: h.changed_at_ms,
            origin: h.origin.clone(),
        }
    }
}

// ─── frame I/O ───────────────────────────────────────────────────────────────

/// Write a length-prefixed JSON frame. Does **not** close the send stream;
/// call `close_send` when all frames have been written.
pub async fn write_frame<T>(send: &mut iroh::endpoint::SendStream, value: &T) -> io::Result<()>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    send.write_u32(bytes.len() as u32).await?;
    send.write_all(&bytes).await?;
    Ok(())
}

/// Close the send stream and wait for the peer to acknowledge it.
pub async fn close_send(send: &mut iroh::endpoint::SendStream) -> io::Result<()> {
    send.finish().map_err(io::Error::other)?;
    match send.stopped().await.map_err(io::Error::other)? {
        None => Ok(()),
        Some(code) => Err(io::Error::other(format!(
            "stream stopped by peer with code {code}"
        ))),
    }
}

/// Read a length-prefixed JSON frame. Does **not** assert end-of-stream;
/// call `assert_eof` after the last expected frame.
pub async fn read_frame<T>(recv: &mut iroh::endpoint::RecvStream) -> io::Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let len = recv.read_u32().await? as usize;
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf).await.map_err(io::Error::other)?;
    serde_json::from_slice(&buf).map_err(io::Error::other)
}

/// Assert that the receive stream has no more data (expected end-of-stream).
pub async fn assert_eof(recv: &mut iroh::endpoint::RecvStream) -> io::Result<()> {
    let trailing = recv.read_to_end(1).await.map_err(io::Error::other)?;
    if !trailing.is_empty() {
        return Err(io::Error::other("unexpected trailing bytes after frame"));
    }
    Ok(())
}

// ─── streaming file I/O ──────────────────────────────────────────────────────

/// Stream a file from `path` to the send stream in chunks.
///
/// Returns the number of bytes written. The stream is **not** closed after.
pub async fn write_file_to_stream(
    send: &mut iroh::endpoint::SendStream,
    path: &Path,
) -> io::Result<u64> {
    let mut file = tokio::fs::File::open(path).await?;
    let bytes = tokio::io::copy(&mut file, send).await?;
    Ok(bytes)
}

/// Read exactly `size` bytes from the receive stream into a temp file,
/// verifying the blake3 hash of the content as it arrives.
///
/// The caller is responsible for removing `dest` on error.
pub async fn read_stream_to_temp(
    recv: &mut iroh::endpoint::RecvStream,
    size: u64,
    expected_hash: &[u8; 32],
    dest: &Path,
) -> io::Result<()> {
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut file = tokio::fs::File::create(dest).await?;
    let mut hasher = blake3::Hasher::new();
    let mut remaining = size;
    let mut buf = vec![0u8; 65536];
    while remaining > 0 {
        let want = (buf.len() as u64).min(remaining) as usize;
        recv.read_exact(&mut buf[..want])
            .await
            .map_err(io::Error::other)?;
        hasher.update(&buf[..want]);
        file.write_all(&buf[..want]).await?;
        remaining -= want as u64;
    }
    let computed = *hasher.finalize().as_bytes();
    if &computed != expected_hash {
        return Err(io::Error::other(format!(
            "file content hash mismatch for stream (expected {}, got {})",
            hex(&expected_hash[..4]),
            hex(&computed[..4])
        )));
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
