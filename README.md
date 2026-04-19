# tngl

`tngl` syncs a folder between a small, trusted group of nodes over `iroh`.

It is designed for direct peer-to-peer sync between your own machines or other
trusted peers. Each node keeps a full local copy of the synced folder.

## Build

Debug build:

```bash
cargo build
```

Release build:

```bash
cargo build --release
```

The binary name is `tngl`.

## Basic Usage

Start a node for a folder:

```bash
./target/release/tngl --folder /path/to/folder
```

On startup, `tngl` prints:

- the local node ID
- a JSON `--peer` value you can use on another node

Example:

```bash
tngl node-id <NODE_ID>
tngl peer {"id":"<NODE_ID>","addrs":[...]}
```

State is stored inside the synced folder under:

```text
<folder>/.tngl/
```

This contains:

- `iroh.key`: node identity key
- `peers.json`: known peers
- `pending_invites.json`: invite tokens
- `daemon.cache`: startup cache

## Two-Node Setup

### Option 1: Static Peer

Start node A:

```bash
./target/release/tngl --folder /tmp/node-a
```

Copy the printed `peer` JSON from node A and start node B with it:

```bash
./target/release/tngl \
  --folder /tmp/node-b \
  --peer '{"id":"<NODE_ID>","addrs":[...]}'
```

You can also connect by node ID only:

```bash
./target/release/tngl --folder /tmp/node-b --peer-id <NODE_ID>
```

### Option 2: Invite / Join

Start the daemon on node A:

```bash
./target/release/tngl --folder /tmp/node-a
```

Generate an invite on node A:

```bash
./target/release/tngl --folder /tmp/node-a --invite
```

This prints a ticket of the form:

```text
<NODE_ID>:<TOKEN>
```

Join from node B:

```bash
./target/release/tngl --folder /tmp/node-b --join '<NODE_ID>:<TOKEN>'
```

After joining, the daemon starts normally and persists the peer list in
`/tmp/node-b/.tngl/peers.json`.

## Useful Flags

Print the local node ID and exit:

```bash
./target/release/tngl --folder /tmp/node-a --show-id
```

Force a full startup rescan by dropping the cache before startup:

```bash
./target/release/tngl --folder /tmp/node-a --rescan
```

Set a custom invite expiration in seconds:

```bash
./target/release/tngl --folder /tmp/node-a --invite --expire 600
```

Remove a peer from the membership ledger and broadcast the update:

```bash
./target/release/tngl --folder /tmp/node-a --remove-peer <NODE_ID>
```

## Ignore Rules

You can ignore paths by creating a file named:

```text
.notngl
```

inside the synced folder.

Example:

```text
files/
*.tmp
build/
!build/keep.txt
```

Supported rule features are intentionally gitignore-like:

- blank lines
- `#` comments
- `!` negation
- `*`, `?`, and `**`
- leading `/` for root-anchored patterns
- trailing `/` for directory patterns

When `.notngl` changes, `tngl` rescans the folder so newly ignored paths become
tombstones and newly unignored paths can reappear.

## Notes

- `.tngl/` is always ignored by sync.
- live replication uses gossip announcements plus direct `GetFiles` pulls; file
  content is not pushed proactively to every peer.
- Some OS metadata files are also ignored automatically, such as `.DS_Store`,
  `Thumbs.db`, `Desktop.ini`, `._*`, `.Spotlight-V100`, and `$RECYCLE.BIN`.
- Empty directories are not tracked as first-class state. If the last file in a
  directory is deleted, the empty directory may be removed on peers.

## Debugging

Enable sync trace logging with:

```bash
TNGL_DEBUG_SYNC=1 ./target/release/tngl --folder /tmp/node-a
```
