# lil Design

## Goal

`lil` syncs a folder between a small, trusted group of nodes on the same LAN.
Every node keeps a full copy of the folder and a full membership ledger.

There is no central node, relay server, NAT traversal, or remote discovery.
Discovery is local mDNS. Data transfer and announcements use direct
Noise-encrypted TCP RPCs.

## Model

`peers.json` is the source of truth for group membership. A node is trusted only
if it appears as active in the local membership ledger.

- mDNS maps active node IDs to LAN socket addresses.
- direct RPCs are the source of truth for sync data.
- announcements are a hint and repair trigger, not an authoritative write.
- every active node fans announcements out to every other active node it can
  currently discover.

## Non-Goals

- Sync across NAT or the public internet.
- Trust announcement payloads as authoritative writes.
- Implement decentralized membership discovery beyond mDNS plus the explicit
  join flow.

## Identity And Transport

Each node stores a 32-byte Ed25519 private key in `.lil/private.key`. The
Ed25519 public key is the node ID.

Peers establish a Noise `NN` session over TCP and then exchange signed identity
payloads inside the encrypted session. The Ed25519 signature binds each node ID
to the Noise handshake transcript, so the encrypted connection is associated
with the same identity used by the membership ledger.

## Discovery

Running sync daemons advertise `_lilsync._tcp.local.` over mDNS with a TXT
record containing their node ID. Browsers ignore their own ID and keep an
in-memory address book from node ID to socket address.

Discovery is intentionally LAN-only. If a peer is not visible over mDNS, RPCs to
that peer time out instead of falling back to a relay.

## Sync Tree

The sync tree is a path-based directory-mirroring Merkle tree. It provides:

- **A single root hash** describing the complete folder state.
- **Locality**: a change to `src/foo/bar.rs` recomputes only the file leaf, the
  `src/foo/` node, the `src/` node, and the root.

### File Leaves

```
leaf_hash = blake3(content_hash ++ lamport_le64 ++ changed_at_ms_le64 ++ origin_bytes)
```

- `content_hash`: BLAKE3 of file content; `TOMBSTONE_HASH` for deletions.
- `lamport_le64`: 8-byte little-endian Lamport clock.
- `changed_at_ms_le64`: 8-byte little-endian wall-clock timestamp.
- `origin_bytes`: UTF-8 node ID of the originator.

Deleted files remain as tombstone leaves until garbage collection can prove the
active peers have converged on the deletion.

### Directory Nodes

```
dir_hash = blake3("name1\0" ++ hash1 ++ "name2\0" ++ hash2 ++ ...)
```

Children are sorted lexicographically by name.

## RPCs

All RPCs run over a fresh Noise-encrypted TCP connection.

| RPC | Purpose |
|---|---|
| `Join` | Consume an invite token and return the full member ledger |
| `GetRoot` | Read a peer's state root, live root, and Lamport clock |
| `GetNode` | Read one Merkle tree node by path prefix |
| `GetEntry` | Read one replicated entry by path |
| `GetObject` | Stream file content by content hash |
| `Announce` | Deliver a sync-state, filesystem-changed, or peers announcement |

File content is streamed in encrypted chunks. The receiver verifies the BLAKE3
hash before installing a downloaded object.

## Announcements

Announcements are sent by LAN fanout RPC to active peers. They carry an
`origin` node ID and are accepted only if the origin is active in the local
member ledger.

| Message | Published by | When |
|---|---|---|
| `FilesystemChanged` | any node | after local filesystem changes |
| `SyncState` | any node | on startup and periodically |
| `Peers` | any node | after join/removal and periodically |

Receivers use announcements as hints:

- a different root schedules a Merkle reconciliation against the origin.
- a filesystem hint can reduce the tree walk to changed prefixes.
- a peers announcement merges newer member ledger entries.

## Membership

### Invite Ticket

An existing member creates an invite token and stores it in `.lil/invites.json`.
The ticket encodes the issuer node ID and the token secret. Tokens are
single-use and expire.

### Join Flow

1. Joiner discovers the issuer by node ID over mDNS.
2. Joiner connects to the issuer and sends `Join`.
3. Issuer consumes the invite token.
4. Issuer adds the joiner to its member ledger.
5. Issuer returns the full member ledger.
6. Joiner writes that ledger to `.lil/peers.json`.
7. If not started with `--exit`, the joiner starts its sync daemon and begins
   advertising itself over mDNS.

### Member Removal

Any node can mark another member as removed. Removed entries remain in the
ledger with their Lamport values so stale announcements cannot resurrect old
members. Remaining peers apply the removal when they receive a newer `Peers`
announcement or reconnect and reconcile.

## Tombstone Garbage Collection

Deletes are retained as tombstones until active peers have reported matching
roots for the deletion. Each node also persists a GC watermark in
`.lil/gc-watermark.bin` so old versions cannot be reintroduced after restart.

When active peers converge on the same root, tombstones covered by the
converged watermark can be pruned and the new state is announced.

## State Files

All state lives in `<sync-folder>/.lil/`:

| File | Contents |
|---|---|
| `private.key` | 32-byte Ed25519 private key |
| `daemon.lock` | exclusive sync process lock |
| `peers.json` | full membership ledger |
| `invites.json` | outstanding invite tokens with expiry |
| `entries.bin` | persisted replicated entry index |
| `lamport` | persisted Lamport clock |
| `gc-watermark.bin` | persisted tombstone GC watermark |

### `peers.json` Schema

```json
{
  "members": [
    { "id": "<node-id>", "status": "active", "lamport": 42, "name": "macbook" },
    { "id": "<node-id>", "status": "removed", "lamport": 105, "name": null }
  ]
}
```

Older files may contain a `topic_id` field from the previous transport. It is
ignored and omitted on the next save.

## Risks

### LAN-Only Availability

Peers cannot sync unless they are on the same LAN and visible over mDNS. This
is an intentional trade-off after removing relay and NAT traversal support.

### Authorization Drift

Member lists are persisted independently and may diverge while a node is
offline. Periodic `Peers` announcements repair this when nodes are online
together.

### Announcement Fanout

Every announcement is sent directly to every active peer. This is simple and
works for small groups, but large groups or high write volume can produce many
short TCP connections.

### Backward Compatibility

Existing data and private keys are reused, but peer IDs are now stored as raw
Ed25519 public-key hex strings. Very old `peers.json` files that only contain
transport-specific node ID encodings may need to be rejoined.
