mod cache;
mod config;
mod daemon;
mod diagnostics;
mod gossip;
mod invite;
mod peers;
mod protocol;
mod snapshot;
mod sync_tree;
mod transport;

use clap::Parser;
use config::Config;
use daemon::{run, run_invite, run_join};
use iroh::{EndpointAddr, PublicKey};
use std::io;
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(name = "tngl", about = "Sync a folder between nodes over iroh")]
struct Cli {
    #[arg(
        long,
        value_name = "PATH",
        help = "Folder to sync (state stored in <PATH>/.tngl/)"
    )]
    folder: PathBuf,

    #[arg(
        long = "peer-id",
        value_name = "NODE_ID",
        action = clap::ArgAction::Append,
        help = "Peer node ID to sync with using iroh discovery"
    )]
    peer_ids: Vec<PublicKey>,

    #[arg(
        long = "peer",
        value_name = "ENDPOINT_ADDR_JSON",
        value_parser = parse_endpoint_addr,
        action = clap::ArgAction::Append,
        help = "Static peer EndpointAddr encoded as JSON (advanced)"
    )]
    peers: Vec<EndpointAddr>,

    #[arg(
        long = "allow-peer",
        value_name = "NODE_ID",
        action = clap::ArgAction::Append,
        help = "Additional allowlisted peer node IDs"
    )]
    allow_peers: Vec<PublicKey>,

    #[arg(long = "show-id", help = "Print the node ID then exit")]
    show_id: bool,

    #[arg(
        long = "rescan",
        help = "Ignore the startup cache and rebuild folder state from disk"
    )]
    rescan: bool,

    #[arg(
        long = "invite",
        help = "Generate an invite ticket and exit (the running daemon will accept it)"
    )]
    invite: bool,

    #[arg(
        long = "expire",
        value_name = "SECONDS",
        default_value = "3600",
        help = "Invite token expiry in seconds (used with --invite)"
    )]
    expire: u64,

    #[arg(
        long = "join",
        value_name = "TICKET",
        help = "Join a peer using an invite ticket (<node_id>:<token>), then exit"
    )]
    join: Option<String>,
}

fn main() {
    if let Err(err) = run_cli() {
        eprintln!("tngl: {err}");
        std::process::exit(1);
    }
}

fn run_cli() -> io::Result<()> {
    let cli = Cli::parse();
    let sync_dir = cli.folder;
    let state_dir = sync_dir.join(".tngl");
    std::fs::create_dir_all(&sync_dir)?;
    std::fs::create_dir_all(&state_dir)?;

    let config = Config {
        sync_dir,
        cache_path: state_dir.join("daemon.cache"),
        key_path: state_dir.join("iroh.key"),
        peers_path: state_dir.join("peers.json"),
        invites_path: state_dir.join("pending_invites.json"),
        peer_ids: cli.peer_ids,
        peers: cli.peers,
        allow_peers: cli.allow_peers,
        show_id: cli.show_id,
        rescan: cli.rescan,
        invite_expire_secs: cli.expire,
    };

    if cli.invite {
        return run_invite(config);
    }

    if let Some(ticket) = cli.join {
        run_join(config.clone(), &ticket)?;
        // Fall through to start the daemon after joining.
    }

    run(config)
}

fn parse_endpoint_addr(input: &str) -> Result<EndpointAddr, String> {
    serde_json::from_str(input).map_err(|err| format!("invalid peer JSON: {err}"))
}
