# Sunya P2P user-token example

This is a standalone Go example. It does not import or modify the existing
`user_tokens` example. Each process owns a separate LevelDB database and a
persistent libp2p identity. The HTML comparison reads only the local database;
records become available there after peer replication.

## Requirements

- Go 1.22 or later.
- A TCP port that is reachable by peers. For peers on different networks, the
  node operator may need to allow the port in the host firewall and forward it
  from the router to the node.
- A reachable IP address or DNS hostname for at least one peer. This example
  uses configured peer addresses; it does not include a public peer directory
  or automatic discovery.

## Start the first (seed) node

From this directory, fetch dependencies and run the test suite:

```sh
go mod tidy
go test ./...
```

Choose a TCP port, for example `4002`. Start the node with a wildcard listen
address so it can accept connections on its network interfaces:

```sh
go run . \
  -http :8083 \
  -p2p /ip4/0.0.0.0/tcp/4002 \
  -announce /ip4/SEED_REACHABLE_IP/tcp/4002 \
  -db ./data/seed
```

Replace `SEED_REACHABLE_IP` with an address the joining machine can reach:

- For two processes on the same machine, use `127.0.0.1`.
- For machines on the same LAN, use the seed machine's LAN address, such as
  `192.168.1.20`.
- Across the internet, use a public IP or DNS name and configure the router to
  forward TCP port `4002` to the seed machine. Allow that port through the
  machine's firewall.

The `-announce` address tells other peers where to dial this node. It must be
the reachable host and port, without a trailing `/p2p/...` component. If no
`-announce` flag is supplied, the app prints the addresses it can infer from
the listening host; wildcard addresses such as `0.0.0.0` are not dialable from
another machine.

At startup, copy the complete address printed under **P2P addresses**. It will
look like:

```text
/ip4/SEED_REACHABLE_IP/tcp/4002/p2p/12D3KooW...
```

Keep the process running. The web UI is at `http://localhost:8083` on the seed
machine. The `-http` port serves the UI; the `-p2p` TCP port is used for peer
connections. Do not confuse the two.

## Join an existing peer network

On the new machine, run the same project with a different HTTP port, P2P port,
database directory, and persistent identity file. Pass the seed's entire
multiaddress—including its `/p2p/PEER_ID` suffix—to `-peers`:

```sh
go run . \
  -http :8083 \
  -p2p /ip4/0.0.0.0/tcp/4002 \
  -announce /ip4/NEW_PEER_REACHABLE_IP/tcp/4002 \
  -db ./data/node-alice \
  -identity ./data/node-alice.peer.key \
  -peers '/ip4/SEED_REACHABLE_IP/tcp/4002/p2p/SEED_PEER_ID'
```

Replace both placeholders with the actual seed address and peer ID copied
from its startup output. Use a unique `-db`, `-identity`, HTTP port, and P2P
port for every process running on the same machine. If the new peer is behind
a firewall or NAT and cannot accept inbound connections, it can still dial the
reachable seed; omit `-announce` unless the new node has a usable inbound
address.

Once connected, the peers exchange their existing records and forward new
records. The new peer's startup output prints its own peer ID and dial address.
You can use that address as `-peers` for another peer joining later. The
existing seed does not need to be restarted or reconfigured to accept the new
peer.

For multiple seed peers, pass a comma-separated list:

```sh
-peers '/ip4/SEED1_IP/tcp/4002/p2p/SEED1_ID,/ip4/SEED2_IP/tcp/4002/p2p/SEED2_ID'
```

Verify a connection by checking the **Connected peers** section in the node's
web UI. Save records on either node, wait for replication, then look up or
compare those keys on the other node. `GET /token?frontendTokenKey=KEY` also
returns a record from the local database as JSON.

## Persistence and restart

Each node's LevelDB directory and identity key must be retained between
restarts. The identity key determines the peer ID in configured multiaddresses.
The default identity path is derived from the database directory; `-identity`
overrides it. Back up the key securely and do not share it between different
nodes. If you delete or replace it, that node gets a different peer ID and
other nodes must be configured with its new printed address.

When a configured peer becomes unavailable, the node retries its seed
addresses. When it reconnects, peers send their local database snapshots to
each other. Records saved while nodes were disconnected can therefore
replicate after they rejoin, assuming both still have their database and
identity files.

## Replication behavior

Every record carries a UTC update timestamp and its originating peer ID.
Conflicting writes are resolved by the later timestamp; equal timestamps are
ordered by origin peer ID and then by serialized record contents. This
deterministic last-write-wins policy helps connected peers converge, but a
wall-clock change or skew can affect which write wins. This is a demo
replication protocol, not blockchain consensus.

## Troubleshooting

- **Peer ID mismatch:** Use the complete address copied from the currently
  running seed. Confirm its identity file has not changed.
- **Connection refused or timeout:** Confirm the seed is running, both nodes
  use the correct TCP port, and the firewall/router allows inbound TCP traffic.
- **Address contains `0.0.0.0`:** Set `-announce` to an IP/DNS name reachable
  from the joining peer. Do not use `0.0.0.0` as an announced address.
- **No connected peers in the UI:** Check the startup logs for `connect to
  peer` errors and verify the full `-peers` address includes `/p2p/PEER_ID`.
- **Keys do not appear on the other node:** Check the connected-peer status,
  wait for synchronization, and verify that both nodes are running this same
  replication protocol version.
- **Several nodes use the same host:** Give every process distinct ports,
  database directories, and identity files.

## Security and production TODOs

- Add peer authorization/trust management. Libp2p encrypts peer connections,
  but this example accepts replication messages from any peer that can connect.
- Add authentication and authorization for the HTTP UI/API.
- Review input quotas, rate limits, privacy requirements, and operational
  monitoring before exposing nodes to untrusted networks.
- Define a robust clock/conflict policy if timestamp-based ordering is not
  appropriate.
- Add peer discovery and NAT traversal for deployments that cannot use direct
  multiaddresses.
- Add database backups, migrations, retention rules, and production deployment
  configuration.

Do not expose this demo on an untrusted network with real tokens or sensitive
information.
