//! Entry point. Parses CLI arguments, initialises logging, and dispatches to
//! subcommand handlers or the sync daemon.

mod cli;
mod commands;
mod daemon;
mod discovery;
mod entries;
mod group;
mod identity;
mod ignore;
mod message;
mod protocol;
mod rpc;
mod scan;
mod state;
mod sync;
mod transport;
mod tree;
mod ui;
mod watcher;

use crate::cli::{Cli, Command};
use crate::commands::{
    KEY_FILE, PEERS_FILE, create_invite, dump_state_cmd, join_group, peers_cmd, remove_peer_cmd,
};
use crate::daemon::run_sync;
use clap::Parser;
use std::fs;
use std::io;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let default_filter = if cli.status_mode() { "warn" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter)),
        )
        .with_level(true)
        .with_target(false)
        .init();

    if let Err(err) = run(cli).await {
        tracing::error!("{err}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> io::Result<()> {
    match cli.command {
        Command::Invite {
            folder,
            expire_secs,
        } => {
            fs::create_dir_all(&folder)?;
            let state_dir = folder.join(".lil");
            fs::create_dir_all(&state_dir)?;
            return create_invite(&state_dir, expire_secs);
        }
        Command::Remove { folder, target } => {
            fs::create_dir_all(&folder)?;
            let state_dir = folder.join(".lil");
            fs::create_dir_all(&state_dir)?;
            return remove_peer_cmd(&state_dir, &target);
        }
        Command::Peers { folder } => {
            fs::create_dir_all(&folder)?;
            let state_dir = folder.join(".lil");
            fs::create_dir_all(&state_dir)?;
            return peers_cmd(&state_dir);
        }
        Command::DumpState { folder, prefix } => {
            return dump_state_cmd(&folder, prefix.as_deref());
        }
        Command::Join {
            folder,
            ticket,
            name,
            exit,
            status,
        } => {
            fs::create_dir_all(&folder)?;
            let state_dir = folder.join(".lil");
            fs::create_dir_all(&state_dir)?;
            let identity = Arc::new(crate::identity::Identity::load_or_create(
                &state_dir.join(KEY_FILE),
            )?);
            let address_book = discovery::new_address_book();
            let _mdns = discovery::spawn_browser(identity.node_id(), Arc::clone(&address_book))?;
            let peers_path = state_dir.join(PEERS_FILE);
            join_group(
                Arc::clone(&identity),
                address_book,
                &peers_path,
                &ticket,
                name.clone(),
            )
            .await?;
            if exit {
                return Ok(());
            }
            run_sync(folder, name, false, 500, 10, status).await?;
        }
        Command::Sync {
            folder,
            name,
            poll,
            interval_ms,
            announce_interval_secs,
            status,
        } => {
            run_sync(
                folder,
                name,
                poll,
                interval_ms,
                announce_interval_secs,
                status,
            )
            .await?
        }
    }
    Ok(())
}
