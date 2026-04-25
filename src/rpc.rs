use crate::protocol::{
    RequestMessage, ResponseMessage, assert_eof, close_send, read_frame, write_frame,
};
use crate::state::{Entry, FolderState, TreeNode, hex};
use iroh::endpoint::{Connection, ConnectionError};
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh::{Endpoint, PublicKey};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, RwLock};

pub const ALPN: &[u8] = b"tngl/rpc/1";

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

fn next_request_id() -> u64 {
    NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Clone)]
pub struct FolderRpc {
    state: Arc<RwLock<FolderState>>,
}

impl FolderRpc {
    pub fn new(state: Arc<RwLock<FolderState>>) -> Self {
        Self { state }
    }
}

#[derive(Debug, Clone)]
pub struct RpcClient {
    endpoint: Endpoint,
    connections: Arc<Mutex<HashMap<PublicKey, Connection>>>,
}

impl RpcClient {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn get_root(&self, peer: PublicKey) -> io::Result<([u8; 32], [u8; 32], u64)> {
        let request_id = next_request_id();
        match self
            .round_trip(peer, RequestMessage::GetRoot { request_id })
            .await?
        {
            ResponseMessage::Root {
                request_id: actual,
                state_root,
                live_root,
                lamport,
            } if actual == request_id => Ok((state_root, live_root, lamport)),
            ResponseMessage::Error {
                request_id: actual,
                message,
            } if actual == request_id => Err(io::Error::other(message)),
            response => Err(io::Error::other(format!(
                "unexpected GetRoot response from {peer}: {response:?}"
            ))),
        }
    }

    pub async fn get_node(&self, peer: PublicKey, prefix: &str) -> io::Result<Option<TreeNode>> {
        let request_id = next_request_id();
        let request = RequestMessage::GetNode {
            request_id,
            prefix: prefix.to_string(),
        };
        match self.round_trip(peer, request).await? {
            ResponseMessage::Node {
                request_id: actual,
                node,
            } if actual == request_id => Ok(node),
            ResponseMessage::Error {
                request_id: actual,
                message,
            } if actual == request_id => Err(io::Error::other(message)),
            response => Err(io::Error::other(format!(
                "unexpected GetNode response from {peer}: {response:?}"
            ))),
        }
    }

    pub async fn get_entry(&self, peer: PublicKey, path: &str) -> io::Result<Option<Entry>> {
        let request_id = next_request_id();
        let request = RequestMessage::GetEntry {
            request_id,
            path: path.to_string(),
        };
        match self.round_trip(peer, request).await? {
            ResponseMessage::Entry {
                request_id: actual,
                entry,
            } if actual == request_id => Ok(entry),
            ResponseMessage::Error {
                request_id: actual,
                message,
            } if actual == request_id => Err(io::Error::other(message)),
            response => Err(io::Error::other(format!(
                "unexpected GetEntry response from {peer}: {response:?}"
            ))),
        }
    }

    pub async fn get_object(&self, peer: PublicKey, content_hash: [u8; 32]) -> io::Result<Vec<u8>> {
        let request_id = next_request_id();
        let request = RequestMessage::GetObject {
            request_id,
            content_hash,
        };
        match self.round_trip(peer, request).await? {
            ResponseMessage::Object {
                request_id: actual,
                bytes,
            } if actual == request_id => Ok(bytes),
            ResponseMessage::Error {
                request_id: actual,
                message,
            } if actual == request_id => Err(io::Error::other(message)),
            response => Err(io::Error::other(format!(
                "unexpected GetObject response from {peer}: {response:?}"
            ))),
        }
    }

    async fn round_trip(
        &self,
        peer: PublicKey,
        request: RequestMessage,
    ) -> io::Result<ResponseMessage> {
        let connection = self.connection(peer).await?;
        match round_trip_on_connection(&connection, peer, &request).await {
            Ok(response) => Ok(response),
            Err(first_err) => {
                self.drop_connection(peer).await;
                tracing::debug!(target: "tngl::rpc", %peer, "retrying RPC after connection error: {first_err}");
                let connection = self.connection(peer).await?;
                round_trip_on_connection(&connection, peer, &request).await
            }
        }
    }

    async fn connection(&self, peer: PublicKey) -> io::Result<Connection> {
        let mut connections = self.connections.lock().await;
        if let Some(connection) = connections.get(&peer) {
            if connection.close_reason().is_none() {
                return Ok(connection.clone());
            }
            connections.remove(&peer);
        }

        let connection = self
            .endpoint
            .connect(peer, ALPN)
            .await
            .map_err(|err| io::Error::other(format!("connect to {peer}: {err}")))?;
        connections.insert(peer, connection.clone());
        Ok(connection)
    }

    async fn drop_connection(&self, peer: PublicKey) {
        self.connections.lock().await.remove(&peer);
    }
}

impl ProtocolHandler for FolderRpc {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        handle_connection(connection, Arc::clone(&self.state))
            .await
            .map_err(AcceptError::from_err)
    }
}

async fn handle_connection(
    connection: Connection,
    state: Arc<RwLock<FolderState>>,
) -> io::Result<()> {
    let peer = connection.remote_id();
    tracing::debug!(target: "tngl::rpc", %peer, "incoming connection");

    loop {
        let (mut send, mut recv) = match connection.accept_bi().await {
            Ok(streams) => streams,
            Err(err) if is_routine_connection_close(&err) => {
                tracing::debug!(target: "tngl::rpc", %peer, "connection closed: {err}");
                return Ok(());
            }
            Err(err) => return Err(io::Error::other(format!("accept bi stream: {err}"))),
        };

        let request: RequestMessage = read_frame(&mut recv)
            .await
            .map_err(|err| io::Error::other(format!("read request from {peer}: {err}")))?;
        tracing::debug!(target: "tngl::rpc", %peer, "recv {:?}", request);
        assert_eof(&mut recv)
            .await
            .map_err(|err| io::Error::other(format!("trailing bytes from {peer}: {err}")))?;

        let response = handle_request(request, &state).await;
        tracing::debug!(target: "tngl::rpc", %peer, "send {:?}", response);
        write_frame(&mut send, &response)
            .await
            .map_err(|err| io::Error::other(format!("write response to {peer}: {err}")))?;
        close_send(&mut send)
            .await
            .map_err(|err| io::Error::other(format!("close response stream to {peer}: {err}")))?;
    }
}

async fn handle_request(
    request: RequestMessage,
    state: &Arc<RwLock<FolderState>>,
) -> ResponseMessage {
    match request {
        RequestMessage::GetRoot { request_id } => {
            let state = state.read().await;
            ResponseMessage::Root {
                request_id,
                state_root: state.root_hash(),
                live_root: state.live_root_hash(),
                lamport: state.lamport(),
            }
        }
        RequestMessage::GetNode { request_id, prefix } => {
            let state = state.read().await;
            ResponseMessage::Node {
                request_id,
                node: state.node(&prefix),
            }
        }
        RequestMessage::GetEntry { request_id, path } => {
            let state = state.read().await;
            ResponseMessage::Entry {
                request_id,
                entry: state.entry(&path),
            }
        }
        RequestMessage::GetObject {
            request_id,
            content_hash,
        } => {
            let path = {
                let state = state.read().await;
                state.object_path(content_hash)
            };
            match path {
                Some(path) => match tokio::fs::read(&path).await {
                    Ok(bytes) => ResponseMessage::Object { request_id, bytes },
                    Err(err) => ResponseMessage::Error {
                        request_id,
                        message: format!("read object {} failed: {err}", hex(content_hash)),
                    },
                },
                None => ResponseMessage::Error {
                    request_id,
                    message: format!("object {} not found", hex(content_hash)),
                },
            }
        }
    }
}

async fn round_trip_on_connection(
    connection: &Connection,
    peer: PublicKey,
    request: &RequestMessage,
) -> io::Result<ResponseMessage> {
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|err| io::Error::other(format!("open stream to {peer}: {err}")))?;

    write_frame(&mut send, &request)
        .await
        .map_err(|err| io::Error::other(format!("write request to {peer}: {err}")))?;
    close_send(&mut send)
        .await
        .map_err(|err| io::Error::other(format!("close request stream to {peer}: {err}")))?;

    let response = read_frame(&mut recv)
        .await
        .map_err(|err| io::Error::other(format!("read response from {peer}: {err}")))?;
    assert_eof(&mut recv)
        .await
        .map_err(|err| io::Error::other(format!("trailing response from {peer}: {err}")))?;
    Ok(response)
}

fn is_routine_connection_close(err: &ConnectionError) -> bool {
    matches!(
        err,
        ConnectionError::ApplicationClosed(_) | ConnectionError::LocallyClosed
    )
}
