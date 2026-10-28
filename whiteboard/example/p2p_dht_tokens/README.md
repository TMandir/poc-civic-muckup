# Sunya P2P tokens with DHT discovery and relay fallback

This is a standalone Go example in `whiteboard/example/p2p_dht_tokens`. It
does not modify the other examples. Each peer stores its own LevelDB database.
`CompareTokenValues` is preserved in `internal/compare/compare.go` and compares
records in that peer's local database.

## What the network does

1. A peer connects to configured public Kademlia DHT bootstrap peers.
2. It advertises a shared `-network` tag using the DHT routing-discovery
   provider-record mechanism.
3. Peers with the same tag discover one another and attempt direct connections.
4. Libp2p hole punching can try to make a direct connection through compatible
   NATs.
5. If direct connectivity fails and a configured Circuit Relay v2 peer accepts
   a reservation, AutoRelay can advertise a relay address and carry encrypted
   peer traffic.
6. Once connected, peers exchange full LevelDB snapshots and forward newer
   records to the connected peer graph.

The DHT stores peer-discovery provider records, **not token records**. Relay
peers forward encrypted network traffic; **they do not store the database**.
Every application peer stores its own full copy of records it receives.

## Required public infrastructure

DHT and relay are separate functions:

- **DHT bootstrap peers** provide the initial route into the Kademlia DHT.
  Bootstrap addresses must be reachable and speak the compatible libp2p/IPFS
  DHT protocol.
- **Relay peers** must run Circuit Relay v2 hop service and permit client
  reservations. A normal libp2p peer or DHT bootstrap is not necessarily a
  relay.

If `-bootstrap` is omitted, the libp2p DHT library's built-in public bootstrap
peer list is used. Operators can override it with current addresses using
`-bootstrap`. Relay fallback is optional and requires one or more valid relay
addresses through `-relays`; the library's DHT bootstrap peers are not
necessarily relays. Public endpoints, policies, and availability can change.
Without working bootstrap peers, DHT discovery cannot start. Without a usable
relay, hole punching is best-effort and some router/CGNAT combinations will
remain unable to connect.

Every node must use a unique group tag and share it with its intended
participants out of band. The tag is public metadata, not an access-control
mechanism: anyone who learns it and can use the DHT can discover advertising
peer IDs. This demo has no peer authorization; connected peers receive the
entire replicated database. Use test data only.

## Requirements

- Go 1.23 or later (libp2p DHT dependency requirement).
- Public DHT bootstrap connectivity. The libp2p built-in list is used by
  default; `-bootstrap` overrides it.
- For relay fallback, a reachable Circuit Relay v2 peer multiaddress that
  accepts reservations.
- A unique persistent database path and identity-key path for each local node.

From this directory, download dependencies and run tests:

```sh
go mod tidy
go test ./...
```

## Run peers on one computer

For a local test, the nodes use the built-in public DHT bootstrap list for
discovery and connect to each other over loopback. Outbound internet access
must be available. No relay is needed for two processes on the same computer.
Pick a unique network tag and use the exact same value on both nodes.

Terminal 1:

```sh
go run ./cmd/token-node \
  -http 127.0.0.1:8083 \
  -p2p /ip4/127.0.0.1/tcp/4002 \
  -announce /ip4/127.0.0.1/tcp/4002 \
  -db ./data/node1 \
  -network my-unique-demo-group
```

Copy the printed Peer ID only to confirm process identity; peer discovery will
be through the DHT using the shared tag.

Terminal 2:

```sh
go run ./cmd/token-node \
  -http 127.0.0.1:8084 \
  -p2p /ip4/127.0.0.1/tcp/4003 \
  -announce /ip4/127.0.0.1/tcp/4003 \
  -db ./data/node2 \
  -network my-unique-demo-group
```

The `-relays` argument is optional for this same-computer test. Add actual
operator-provided relay addresses only when testing relay fallback. Both nodes
must use exactly the same unique `-network` tag.

Open `http://localhost:8083` and `http://localhost:8084`. Save distinct records
in either UI, wait for a peer connection, then look up or compare the records
on the other peer.

## Join over the public internet

1. Use the built-in libp2p DHT bootstrap list or obtain current reachable
   addresses from an operator of compatible public DHT peers. Supply custom
   addresses with `-bootstrap`; otherwise the built-in list is used.
2. Obtain one or more Circuit Relay v2 addresses from relay operators if you
   need fallback when direct connections fail.
3. Run each local node with its own `-db` and `-identity` files, the common
   `-network` tag, and the same bootstrap/relay address lists.
4. Allow the node to connect to bootstrap and relay peers outbound. Most home
   routers allow outbound connections without port forwarding.
5. Check the node logs and browser UI. Discovery and relay reservations can
   take time; logs identify bootstrap, DHT, peer-dial, and sync errors.

Multiaddresses must be complete, for example
`/ip4/203.0.113.10/tcp/4001/p2p/12D3KooW...`; do not use documentation IPs
literally. Addresses obtained from an operator should be copied exactly.
Community bootstrap/relay endpoints may go offline, limit use, or change
policies. Configure multiple independently operated endpoints where possible.

### Example using operator-provided addresses

```sh
go run ./cmd/token-node \
  -http 127.0.0.1:8083 \
  -p2p /ip4/0.0.0.0/tcp/4002 \
  -db ./data/alice \
  -identity ./data/alice.peer.key \
  -bootstrap '/ip4/BOOTSTRAP_IP/tcp/BOOTSTRAP_PORT/p2p/BOOTSTRAP_PEER_ID' \
  -relays '/ip4/RELAY_IP/tcp/RELAY_PORT/p2p/RELAY_PEER_ID' \
  -network my-unique-private-test-network
```

`-announce` is optional and should be used only for a genuine externally
dialable local address. Do not announce `0.0.0.0`, loopback, or a private LAN
address to internet peers. AutoRelay can append addresses from successful
relay reservations.

The HTTP UI binds to localhost by default. Keep it there unless you add
authentication and intentionally need remote browser access.

## Restart and persistence

Keep the LevelDB directory and peer identity key. A peer retains the same ID
after restart and re-advertises itself in DHT. It discovers peers again and
exchanges snapshots when connections return. If the identity file is lost,
the peer ID changes. A disconnected peer stores changes locally and syncs
after it reconnects to a discovered peer.

Stop with Ctrl+C for graceful shutdown. `go run` starts the compiled process
and Go cleans up generated build artifacts; data and identity files remain.

## Engineering and security notes

- Records use deterministic last-write-wins ordering by update timestamp,
  origin peer ID, then record contents. Clock skew can affect the winner; this
  is replication behavior, not blockchain consensus.
- DHT discovery is public and the tag is not a secret.
- Libp2p encrypts peer connections, but encryption does not authorize peers.
  This example accepts replication messages from peers that can connect.
- The HTTP UI has no authentication. Do not expose it to untrusted networks.
- Production work should add authenticated membership, write permissions,
  quotas, monitoring, robust snapshot pagination/streaming, backpressure,
  backup/recovery, and a reviewed conflict policy.
- A single-node or isolated DHT is not a public discovery network. Public
  internet operation depends on independently operated, reachable DHT
  bootstrap peers and (for relay fallback) working Circuit Relay v2 servers.
