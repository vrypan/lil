# Security Assessment

`lil` is designed for syncing folders between a small group of trusted nodes on
the same LAN, such as your own devices. This document describes the threat model
and known limitations.

## Threat Model

`lil` is **not** designed to defend against a compromised group member, a
compromised machine, or a sophisticated adversary with physical access to any
node. It is designed to be safe against outsiders who have only LAN network
access.

## Attack Surface

### File content — strong protection

An outsider cannot read your files passively.

Peers discover each other with mDNS, then communicate over TCP encrypted with
Noise. Each side proves its Ed25519 node identity inside the encrypted
handshake transcript. File transfer RPCs are gated by the membership ledger in
`peers.json`; a non-member can connect, but its requests are rejected before
any file data is sent.

The realistic paths to file content are:

- **Invite token interception**: tokens are single-use and time-limited, but if
  transmitted insecurely an attacker on the LAN could use one before the
  intended recipient. Share invite tokens over an already-secure channel.
- **Key file theft**: stealing `.lil/private.key` from a compromised machine
  allows impersonating that node. This requires physical or OS-level access.

### File metadata — LAN-visible announcements

Announcements are delivered by direct LAN fanout to active peers. Announcement
payloads include node IDs, state roots, Lamport clocks, and bounded tree hints
for filesystem changes. They are encrypted in transit, but any current group
member can see them.

Removed members stop receiving new announcements once remaining peers have
applied the removal. A removed member that still has a valid old key cannot
download file content from peers that have the removal in their local member
ledger.

### Discovery metadata — LAN-visible service records

mDNS advertises that a node is running the `lilsync` service, its TCP port, and
its node ID. Anyone on the LAN can observe those service records. There is no
relay metadata exposure because `lil` does not use relays.

### Denial of service — lightly mitigated

The TCP listener accepts inbound connections from the LAN. Handshakes and RPCs
have timeouts, and non-member requests are rejected, but there is no global rate
limit or connection cap. A targeted LAN flood could still exhaust file
descriptors or CPU.

## Summary

| Threat | Risk | Status |
|---|---|---|
| Passive network sniffing | Low | Noise encryption after mDNS discovery |
| Unauthorized file download | Very low | Membership allowlist on all file RPCs |
| Invite token interception | Low–medium | Single-use, time-limited; depends on how it is shared |
| Key file theft (`private.key`) | Low | Requires physical or OS-level access |
| LAN service discovery metadata | Low | mDNS exposes node ID and port locally |
| Removed-member metadata access | Low | Stops after peers learn the removal |
| DoS via connection flood | Low–medium | Per-operation timeouts; no rate limit |

## Files to Protect

| File | Secret | Consequence if leaked |
|---|---|---|
| `.lil/private.key` | Node identity key | Attacker can impersonate this node |
| `.lil/peers.json` | Member node IDs and status ledger | Attacker learns group membership |
| Invite tokens | Printed to stdout at generation time | Attacker can join the group if used before the intended recipient |
