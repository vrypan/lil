use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, Read};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Serialize, Deserialize)]
pub(crate) struct PendingInvite {
    pub token: String,   // 64 hex chars (32 bytes)
    pub expires_at: u64, // unix timestamp ms
}

pub(crate) fn now_ms() -> io::Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .map_err(io::Error::other)
}

pub(crate) fn generate_token() -> io::Result<String> {
    let mut bytes = [0u8; 32];
    fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

fn load_invites(path: &Path) -> io::Result<Vec<PendingInvite>> {
    match fs::read_to_string(path) {
        Ok(s) => serde_json::from_str(&s).map_err(io::Error::other),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(vec![]),
        Err(e) => Err(e),
    }
}

fn save_invites(path: &Path, invites: &[PendingInvite]) -> io::Result<()> {
    let json = serde_json::to_string_pretty(invites).map_err(io::Error::other)?;
    fs::write(path, json)
}

pub(crate) fn add_invite(path: &Path, token: &str, expires_at: u64) -> io::Result<()> {
    let mut invites = load_invites(path)?;
    let now = now_ms()?;
    invites.retain(|inv| inv.expires_at > now); // purge expired
    invites.push(PendingInvite {
        token: token.to_string(),
        expires_at,
    });
    save_invites(path, &invites)
}

/// Checks whether `token` is valid and not expired, consumes it if so.
/// Returns `true` if the token was valid and has been consumed.
pub(crate) fn consume_invite(path: &Path, token: &str) -> io::Result<bool> {
    let mut invites = load_invites(path)?;
    let now = now_ms()?;
    let mut found = false;
    invites.retain(|inv| {
        if inv.expires_at <= now {
            return false; // purge expired regardless
        }
        if inv.token == token {
            found = true;
            return false; // consume
        }
        true
    });
    if found {
        save_invites(path, &invites)?;
    }
    Ok(found)
}
