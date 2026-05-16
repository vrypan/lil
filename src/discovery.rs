//! mDNS-based peer discovery: advertises this node's RPC port on the local
//! network and maintains an address book mapping `NodeId` to `SocketAddr`.

use crate::identity::NodeId;
use mdns_sd::{ScopedIp, ServiceDaemon, ServiceEvent, ServiceInfo, UnregisterStatus};
use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

pub const SERVICE_TYPE: &str = "_lilsync._tcp.local.";

pub type AddressBook = Arc<RwLock<HashMap<NodeId, SocketAddr>>>;

pub fn new_address_book() -> AddressBook {
    Arc::new(RwLock::new(HashMap::new()))
}

pub struct MdnsHandle {
    daemon: ServiceDaemon,
    fullname: Option<String>,
}

impl MdnsHandle {
    pub async fn announce_removal(&self) {
        let Some(fullname) = self.fullname.clone() else {
            return;
        };
        let daemon = self.daemon.clone();
        let log_fullname = fullname.clone();
        let result = tokio::task::spawn_blocking(move || {
            let receiver = daemon.unregister(&fullname).map_err(io::Error::other)?;
            receiver
                .recv_timeout(Duration::from_secs(1))
                .map_err(io::Error::other)
        })
        .await
        .map_err(io::Error::other);

        match result {
            Ok(Ok(UnregisterStatus::OK)) => {
                tracing::info!("mdns announced removal {log_fullname}");
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            Ok(Ok(UnregisterStatus::NotFound)) => {
                tracing::debug!("mdns service already removed {log_fullname}");
            }
            Ok(Err(err)) | Err(err) => {
                tracing::warn!("mdns removal announcement failed for {log_fullname}: {err}");
            }
        }
    }
}

pub fn spawn(local_id: NodeId, port: u16, address_book: AddressBook) -> io::Result<MdnsHandle> {
    let mdns = ServiceDaemon::new().map_err(io::Error::other)?;
    let instance = format!("lilsync-{}", &local_id.to_string()[..12]);
    let host = format!("{instance}.local.");
    let mut props = HashMap::new();
    props.insert("id".to_string(), local_id.to_string());
    let service = ServiceInfo::new(SERVICE_TYPE, &instance, &host, "", port, props)
        .map_err(io::Error::other)?
        .enable_addr_auto();
    let fullname = service.get_fullname().to_string();
    mdns.register(service).map_err(io::Error::other)?;

    spawn_browser_task(&mdns, local_id, address_book)?;

    Ok(MdnsHandle {
        daemon: mdns,
        fullname: Some(fullname),
    })
}

pub fn spawn_browser(local_id: NodeId, address_book: AddressBook) -> io::Result<MdnsHandle> {
    let mdns = ServiceDaemon::new().map_err(io::Error::other)?;
    spawn_browser_task(&mdns, local_id, address_book)?;
    Ok(MdnsHandle {
        daemon: mdns,
        fullname: None,
    })
}

fn spawn_browser_task(
    mdns: &ServiceDaemon,
    local_id: NodeId,
    address_book: AddressBook,
) -> io::Result<()> {
    let receiver = mdns.browse(SERVICE_TYPE).map_err(io::Error::other)?;
    tokio::spawn(async move {
        let mut services = HashMap::<String, NodeId>::new();
        while let Ok(event) = receiver.recv_async().await {
            match event {
                ServiceEvent::ServiceResolved(info) => {
                    let Some(id) = info.get_property_val_str("id").and_then(|s| s.parse().ok())
                    else {
                        continue;
                    };
                    if id == local_id {
                        continue;
                    }
                    let Some(addr) = best_addr(&info.addresses, info.port) else {
                        continue;
                    };
                    services.insert(info.get_fullname().to_string(), id);
                    tracing::debug!("mdns resolved {id} at {addr}");
                    address_book.write().await.insert(id, addr);
                }
                ServiceEvent::ServiceRemoved(_, fullname) => {
                    if let Some(id) = services.remove(&fullname) {
                        address_book.write().await.remove(&id);
                        tracing::info!("mdns removed {id}");
                    } else {
                        tracing::debug!("mdns service removed {fullname}");
                    }
                }
                other => {
                    tracing::trace!("mdns event {other:?}");
                }
            }
        }
    });

    Ok(())
}

fn best_addr(addresses: &HashSet<ScopedIp>, port: u16) -> Option<SocketAddr> {
    addresses
        .iter()
        .find(|addr| addr.is_ipv4())
        .or_else(|| addresses.iter().next())
        .map(|addr| SocketAddr::new(addr.to_ip_addr(), port))
}
