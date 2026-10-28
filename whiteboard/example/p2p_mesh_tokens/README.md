# Sunya P2P full-database sync example

This is a separate Go project under `whiteboard/example/p2p_mesh_tokens`. It
does not modify or import the existing token examples. Every running node has
its own LevelDB database and persistent libp2p identity. The project keeps the
`CompareTokenValues` function in `internal/compare/compare.go`; it reads both
records from the node's local database.

## What this example does

- Dials configured peer multiaddresses and retries after a peer disconnects.
- Exchanges a full database snapshot when a connection is established or
  restored.
- Sends new records to connected peers and forwards accepted updates through
  the connected graph.
- Applies a deterministic last-write-wins rule to records with the same key.
- Enables libp2p's experimental hole-punching support.

## Important internet connectivity limitation

This project has **no bootstrap, rendezvous, or relay server**. Peers must
exchange a full peer multiaddress out of band and pass it with `-peers`.
Hole-punching is best-effort: libp2p must first have a path to coordinate the
attempt, and some NAT/firewall/CGNAT combinations cannot establish a direct
connection. Therefore this example cannot guarantee that machines behind
separate home routers will connect over the public internet.

For peers on different routers, at least one of the following is generally
needed:

1. A peer with a reachable public address and a firewall/router rule forwarding
   the P2P TCP port.
2. A separately operated rendezvous/bootstrap and Circuit Relay v2 peer that
   helps peers find and reach one another. It can forward encrypted traffic
   without storing this project's database.
3. A VPN/overlay network that gives peers routable addresses.

The second option requires an always-reachable helper host (which need not
store token records). No helper infrastructure and no publicly reachable peer
means no reliable automatic discovery or guaranteed connection. The project
still attempts direct connections to manually configured addresses.

## Run two nodes on one computer

From this directory, fetch dependencies and run tests:

```sh
go mod tidy
go test ./...
```

In Terminal 1, start the first node:

```sh
go run ./cmd/token-node \
  -http :8083 \
  -p2p /ip4/127.0.0.1/tcp/4002 \
  -announce /ip4/127.0.0.1/tcp/4002 \
  -db ./data/node1
```

Copy its complete printed dial address, including `/p2p/PEER_ID`. Then start
Terminal 2 with a different HTTP port, P2P port, database directory, and
identity file:

```sh
go run ./cmd/token-node \
  -http :8084 \
  -p2p /ip4/127.0.0.1/tcp/4003 \
  -announce /ip4/127.0.0.1/tcp/4003 \
  -db ./data/node2 \
  -identity ./data/node2.peer.key \
  -peers '/ip4/127.0.0.1/tcp/4002/p2p/PASTE_FIRST_PEER_ID'
```

Replace `PASTE_FIRST_PEER_ID` with the ID from Terminal 1. Visit
`http://localhost:8083` and `http://localhost:8084`; each page reports
connected peers. Save a record through either page, then look it up or compare
it on the other node after synchronization.

## Connect peers across the internet

Every peer that accepts inbound connections needs a dialable address. If a
router has a public IPv4 address and supports port forwarding:

1. Start the node listening on all interfaces, for example
   `-p2p /ip4/0.0.0.0/tcp/4002`.
2. Forward external TCP port `4002` on the router to the machine's TCP port
   `4002`; allow the port in the machine firewall.
3. Start the node with its public IP or DNS name in `-announce`, for example
   `-announce /ip4/198.51.100.20/tcp/4002`. Do not use the example IP literally.
4. Give another operator the full address printed by the node, including
   `/p2p/PEER_ID`.
5. They start their own node with a unique database/identity and that address
   in `-peers`.

If you cannot port-forward or the ISP uses CGNAT, direct connections and
hole-punch attempts may fail. In that case, use a VPN/overlay or add a
reachable Circuit Relay v2 peer in a future version. This example does not
configure a relay or discovery service.

The `-peers` flag accepts comma-separated full multiaddresses. A peer can use
an already-connected node as its seed; records propagate over active links.
Configured seeds are retried automatically while the process runs.

## Data and identity persistence

Keep both the LevelDB directory and peer identity key between restarts. The
identity key is private and determines the peer ID in its dial address. By
default the identity path is the database path plus `.peer.key`; `-identity`
overrides that path. Do not copy one peer's key to another node. If the key is
lost or replaced, that node gets a different peer ID and other peers need its
new dial address.

When nodes are disconnected, each saves changes locally. A configured peer
retries its seed address; after reconnecting, both nodes exchange snapshots.
Connected peers receive every token record, so this is full replication, not
selective sharing. Each peer can read the records in its local database.

## Conflict and security limitations

Records carry a UTC update timestamp and origin peer ID. The latest timestamp
wins; ties are broken by peer ID, then record contents. Clock skew can affect
which write wins. This mechanism provides deterministic replication behavior,
not blockchain consensus.

Libp2p encrypts peer connections, but this demo has no peer authorization and
accepts replication from any peer that can connect. Its HTTP UI also has no
authentication. Do not expose it to untrusted networks or use sensitive
records. Before production, add authenticated peer membership, application
authorization, privacy controls, limits, monitoring, backup/recovery, and a
reviewed conflict policy.
