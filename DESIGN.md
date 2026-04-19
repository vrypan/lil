# tngl Design

## Goal

`tngl` syncs a folder between a small, trusted group of nodes — for example,
devices owned by the same person.

`iroh-gossip` is used for low-latency change announcements. Direct
`iroh` request/response RPCs are the authoritative data path. Gossip is a
dissemination layer, not a replication engine.

## Model

Every node stores the complete member list. There is no partial membership
view and no need for membership discovery beyond the initial join handshake.

`peers.json` holds everything needed to restart and rejoin the group: the
gossip topic ID and the full peer list. It is the single source of truth for
both sync peers and group membership.

- `iroh-gossip` carries announcements only; no payload is trusted as an
  authoritative write
- direct `iroh` RPCs (`GetNodes` / `GetFiles`) are the source of truth for
  all data transfer
- every node knows every other node

## Non-Goals

- Replace exact state sync with gossip
- Trust gossiped payloads as authoritative writes
- Implement decentralized membership discovery

## Sync Tree

### Structure

The sync tree is a path-based directory-mirroring Merkle tree (similar to git
tree objects). It provides:

- **A single root hash** describing the complete folder state — O(1) sync
  check between two nodes.
- **Locality**: a change to `src/foo/bar.rs` recomputes only the file leaf,
  the `src/foo/` node, the `src/` node, and the root. Files elsewhere are
  untouched.

### File Leaves

```
leaf_hash = blake3(content_hash ++ lamport_le64 ++ changed_at_ms_le64 ++ origin_bytes)
```

- `content_hash`: blake3 of file content; `TOMBSTONE_HASH` (`[0u8; 32]`) for deletions
- `lamport_le64`: 8-byte little-endian lamport clock
- `changed_at_ms_le64`: 8-byte little-endian wall-clock timestamp
- `origin_bytes`: UTF-8 node ID of the originator

The lamport is included so a version bump — even with identical content —
produces a different leaf hash.

Deleted files (tombstones) remain as leaf nodes so removals and their lamport
values propagate correctly during repair.

### Directory Nodes

```
dir_hash = blake3("name1\0" ++ hash1 ++ "name2\0" ++ hash2 ++ ...)
```

Children (file leaves and subdirectory nodes) are sorted lexicographically by
name. `\0` separates each name from its hash.

### Root Node

The root is the directory node for the sync root. Two nodes with the same root
hash are fully synchronized.

### Example

```
root
├── README.md  (leaf)
└── src/       (dir)
    ├── lib.rs (leaf)
    └── foo/   (dir)
        ├── bar.rs (leaf)
        └── baz.rs (leaf)
```

Editing `src/foo/bar.rs` recomputes: leaf → `src/foo/` → `src/` → root.
`README.md`, `src/lib.rs`, and `src/foo/baz.rs` are untouched.

## Sync RPCs

### `GetNodes(path_prefix)`

Returns the immediate children of the node at `path_prefix`:

```json
[
  { "name": "foo",    "hash": "<blake3-hex>", "is_dir": true  },
  { "name": "bar.rs", "hash": "<blake3-hex>", "is_dir": false }
]
```

An empty `path_prefix` returns the root's children. The caller compares each
child hash against its own tree: for differing directory nodes it recurses; for
differing file leaves it collects the path for `GetFiles`.

This walk is O(changed subtrees) — if two nodes differ only in `src/foo/`, the
caller recurses into `src/` then `src/foo/` and stops.

### `GetFiles(paths)`

Returns streamed file metadata and content for each requested path.

The response begins with:

```json
{ "request_id": 7, "count": 1 }
```

Then, for each file, the sender writes a `FileHeader` frame:

```json
{
  "id": "src/foo/bar.rs",
  "hash": "<blake3-hex>",
  "lamport": 42,
  "changed_at_ms": 1776330951115,
  "origin": "<node-id>",
  "size": 12345
}
```

If `size > 0`, the sender streams exactly `size` raw content bytes immediately
after that header. If `size == 0`, the entry is a tombstone and no content
bytes follow.

Used both for targeted fetches after a `FileChanged` announcement and as the
final step of a repair walk. `ReplicatedEntry` is metadata-only; file content
is streamed separately on the QUIC stream.

### `PushEntries`

Live push of changed entries directly to peers. The request frame carries only
the number of entries; each entry is then sent as a `FileHeader` plus optional
raw content bytes, using the same streaming format as `GetFiles`. Used
alongside gossip announcements for immediate delivery without waiting for a
pull.

## Change Paths

### Live change

1. Local filesystem event
2. Update local state and sync tree
3. Publish `FileChanged` to gossip topic
4. Receiving peers: `should_accept_remote` check, then `GetFiles([id])` from origin
5. Also: direct `PushEntries` to each peer for immediate delivery

### Repair (startup / reconnect)

1. Subscribe to gossip topic
2. Publish `SyncState` (current root hash + lamport)
3. Peers with a different root hash initiate a `GetNodes` tree walk
4. Walk recurses into differing subtrees; collects differing files for `GetFiles`

## Gossip Messages

All messages carry `"origin"` (publishing node ID) and a `"kind"` tag.

### `FileChanged`

Published on every local file write or deletion. No debouncing.

```json
{
  "kind": "file-changed",
  "origin": "<node-id>",
  "id": "src/foo/bar.rs",
  "hash": "<blake3-hex>",
  "lamport": 42,
  "changed_at_ms": 1776330951115
}
```

`hash` and `lamport` are enough for `should_accept_remote` before any network IO.

### `SyncState`

Published on startup or topic rejoin.

```json
{
  "kind": "sync-state",
  "origin": "<node-id>",
  "root_hash": "<blake3-hex>",
  "lamport": 284
}
```

A receiver with a different `root_hash` schedules a full tree sync against `origin`.

### `MemberList`

Published after join or removal, and periodically by every node. Carries the
complete membership ledger — both active and removed members — so a single
message conveys the full group state.

```json
{
  "kind": "member-list",
  "origin": "<node-id>",
  "members": [
    { "id": "<node-id>", "status": "active",  "lamport": 42  },
    { "id": "<node-id>", "status": "removed", "lamport": 105 }
  ]
}
```

The `members` list includes the publisher itself.

Merge rule: for each entry, apply if `entry.lamport > local recorded lamport
for that id`. This handles additions and removals identically. A stale
broadcast cannot undo a more recent status change.

A node that sees itself listed as `"removed"` unsubscribes from the topic and
stops publishing.

## Gossip Receiver Behavior

```
receive gossip message
│
├─ sender not in member list → drop
│   (exception: MemberList from a bootstrap peer before local list is populated)
│
├─ FileChanged
│   ├─ should_accept_remote(local_entry, announced)? → no: drop
│   └─ yes: GetFiles([id]) from origin
│             └─ RPC fails: fall back to full tree sync against origin
│
├─ SyncState
│   ├─ local root hash == announced root_hash? → drop
│   └─ differs: schedule full tree sync against origin
│
└─ MemberList
    └─ for each entry { id, status, lamport }:
          entry.lamport <= local recorded lamport for id? → skip
          else: update local record with { status, lamport }
                status == "active"?  → add to peer list; schedule sync if new
                status == "removed"? → remove from peer list
                                       id == self? → unsubscribe, stop publishing
```

## Authorization

- Accept sync RPCs only from nodes in the local member list.
- Treat gossip announcements as actionable only from nodes in the local member
  list.
- Gossip does not create trust. Hearing from a node over gossip is not enough
  to add it to the member list.

## State Files

All state lives in `<sync-folder>/.tngl/`:

| File | Contents |
|---|---|
| `iroh.key` | 32-byte ed25519 secret key |
| `daemon.cache` | rkyv-serialized entry map (mtime cache for fast startup) |
| `peers.json` | group state: `topic_id` + peer list |
| `pending_invites.json` | outstanding invite tokens with expiry |

### `peers.json` schema

```json
{
  "topic_id": "<iroh-gossip topic hex>",
  "peers": ["<node-id>", "<node-id>"]
}
```

- `topic_id`: absent until the node has joined a group
- `peers`: all known group members, **excluding self**

`MemberList` gossip keeps `peers.json` consistent across nodes. On any
membership change a `MemberList` is broadcast; receivers update their own
`peers.json` accordingly.

## Membership

### Invite ticket

Issued by an existing member. Encoded as `<node-id>:<32-byte-hex-token>`.

### Join RPC

`JoinRequest` (joiner → inviter):
```json
{ "token": "<hex>", "joiner_id": "<node-id>" }
```

`JoinAccepted` (inviter → joiner):
```json
{ "topic_id": "<hex-or-null>", "members": ["<node-id>", ...] }
```

`members` is the complete group including the inviter itself. The joiner strips
its own ID and saves the rest as peers.

`JoinRejected` (inviter → joiner):
```json
{ "reason": "invalid or expired token" }
```

Tokens are single-use. `topic_id` and `members` come from the inviter, not the
ticket, so they are always fresh.

### Join flow

1. Joiner connects to `host_id`, sends `JoinRequest`
2. Inviter validates token (consumes it)
3. Inviter adds joiner to its member list
4. Inviter sends `JoinAccepted` with `topic_id` and full member list (including self)
5. Inviter publishes updated `MemberList` to the topic
6. Joiner strips self from members, writes `peers.json`
7. Joiner subscribes to topic, publishes `SyncState`
8. Joiner starts pull sync from inviter

### Member removal

Any node can remove any other node (access control to be refined). The removing
node marks the member as `"removed"` in its ledger, saves `peers.json`, and
broadcasts an updated `MemberList`. All peers apply the same merge rule.

## Startup Optimization

`daemon.cache` stores each entry's `mtime_ms` alongside its hash. On startup,
files whose filesystem mtime matches the cached value are returned directly
without re-hashing. Only files with changed mtime are rehashed.

## Risks

### Bootstrap fragility

If a node's only known peer disappears, reconnecting to the group is difficult.
Mitigation: store several bootstrap peers; future: bootstrap from topic.

### Authorization drift

Member lists are persisted independently and may diverge if a node is offline
during a membership change. Periodic `MemberList` broadcasts repair this on
reconnect.

### Announcement storms

High write volume generates many `FileChanged` messages. Receivers deduplicate
cheaply via `should_accept_remote` before any I/O. `iroh-gossip` handles
transport-level deduplication.

## Summary

### Gossip messages

| Message | Published by | When |
|---|---|---|
| `FileChanged` | any node | on every local write or deletion |
| `SyncState` | any node | on startup / topic rejoin |
| `MemberList` | any node | after join/removal and periodically |

### Sync RPCs

| RPC | Arguments | Returns |
|---|---|---|
| `GetNodes` | `path_prefix: String` | `[{name, hash, is_dir}]` |
| `GetFiles` | `ids: Vec<String>` | `FilesBegin { count }`, then `count` streamed `FileHeader` + content payloads |
| `PushEntries` | `count: usize`, then `count` streamed `FileHeader` + content payloads | `Ack` |

### Key decisions

- **Small trusted group**: every node stores the complete peer list
- **`peers.json` is restart state**: holds `topic_id` + peer list; no separate group config
- **`MemberList` is the full ledger**: active + removed members with per-entry lamports; uniform merge rule; no separate join/remove messages
- **`MemberList` includes self**: the publisher includes its own entry so receivers have a complete picture
- **`JoinAccepted.members` includes inviter**: joiner strips self and saves the rest
- **Topic = group identity**: no separate group name
- **Minimal ticket**: `<node-id>:<token>` only; group state comes from `JoinAccepted`
- **No debouncing**: every write produces one `FileChanged`; deduplication at receiver
- **Gossip for announcements only**: all data transfer over direct RPC
- **Path-based Merkle tree**: O(1) root hash comparison; O(depth) updates per file change
- **`GetNodes` walk is the repair path**: recurses only into differing subtrees
- **Leaf hash includes lamport**: version changes detected even when content is identical
- **Tombstones in the tree**: deleted files remain as leaves so removals propagate during repair
- **mtime cache**: startup skips rehashing files whose mtime is unchanged
