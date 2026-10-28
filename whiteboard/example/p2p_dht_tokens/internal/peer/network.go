// Package peer combines Kademlia DHT discovery with encrypted record
// replication, NAT hole punching, and optional Circuit Relay v2 fallback.
package peer

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	libcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	routingdiscovery "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	ma "github.com/multiformats/go-multiaddr"
	"sunya-p2p-dht-tokens/internal/database"
)

const (
	replicationProtocol  = protocol.ID("/sunya/dht-token-sync/1.0.0")
	maxMessageBytes      = 1 << 20
	peerOperationTimeout = 12 * time.Second
	discoveryInterval    = 30 * time.Second
)

// replicationMessage carries one record through an encrypted libp2p stream.
type replicationMessage struct {
	Record database.Record `json:"record"`
}

// replicationResponse acknowledges a peer request or reports a validation
// error without exposing an internal database failure to the remote peer.
type replicationResponse struct {
	Applied bool   `json:"applied"`
	Error   string `json:"error,omitempty"`
}

// Network owns the libp2p host, Kademlia DHT, peer discovery, and sync loops.
type Network struct {
	host      host.Host
	dht       *dht.IpfsDHT
	discovery *routingdiscovery.RoutingDiscovery
	store     *database.Store
	bootstrap []peer.AddrInfo
	relays    []peer.AddrInfo
	namespace string
	ctx       context.Context
	cancel    context.CancelFunc

	mu     sync.Mutex
	synced map[peer.ID]bool
	peers  map[peer.ID]peer.AddrInfo
	wg     sync.WaitGroup
	notify *network.NotifyBundle
}

// Config contains the network settings supplied by the node operator.
type Config struct {
	ListenAddress        string
	AnnounceAddress      []string
	BootstrapPeers       []string
	RelayPeers           []string
	DiscoveryTag         string
	IdentityPath         string
	DHTMode              string
	AllowPrivateDHTPeers bool
}

// New starts a peer using configured public DHT bootstrap peers and optional
// Circuit Relay v2 candidates. Bootstrap and relay peers are helpers only:
// this node's database remains local and is never stored by them.
func New(parent context.Context, config Config, store *database.Store) (*Network, error) {
	if parent == nil || store == nil {
		return nil, errors.New("parent context and local database are required")
	}
	config.DiscoveryTag = strings.TrimSpace(config.DiscoveryTag)
	if config.DiscoveryTag == "" {
		return nil, errors.New("DHT discovery tag is required")
	}
	bootstrap, err := parsePeerInfos(config.BootstrapPeers, "DHT bootstrap")
	if err != nil {
		return nil, err
	}
	if len(bootstrap) == 0 {
		bootstrap = dht.GetDefaultBootstrapPeerAddrInfos()
	}
	relays, err := parsePeerInfos(config.RelayPeers, "Circuit Relay")
	if err != nil {
		return nil, err
	}
	listenAddress, err := ma.NewMultiaddr(config.ListenAddress)
	if err != nil {
		return nil, fmt.Errorf("parse P2P listen address: %w", err)
	}
	announced, err := parseAnnounceAddresses(config.AnnounceAddress)
	if err != nil {
		return nil, err
	}
	identity, err := loadOrCreateIdentity(config.IdentityPath)
	if err != nil {
		return nil, fmt.Errorf("load persistent peer identity: %w", err)
	}

	options := []libp2p.Option{
		libp2p.Identity(identity),
		libp2p.ListenAddrs(listenAddress),
		libp2p.EnableHolePunching(),
	}
	// Force private reachability because this example targets nodes behind
	// home routers; AutoRelay then attempts reservations with configured relays.
	if len(relays) > 0 {
		options = append(options,
			libp2p.ForceReachabilityPrivate(),
			libp2p.EnableAutoRelayWithStaticRelays(relays),
		)
	}
	if len(announced) > 0 {
		options = append(options, libp2p.AddrsFactory(func([]ma.Multiaddr) []ma.Multiaddr {
			return append([]ma.Multiaddr(nil), announced...)
		}))
	}
	node, err := libp2p.New(options...)
	if err != nil {
		return nil, fmt.Errorf("create libp2p host: %w", err)
	}

	ctx, cancel := context.WithCancel(parent)
	mode, err := parseDHTMode(config.DHTMode)
	if err != nil {
		cancel()
		_ = node.Close()
		return nil, err
	}
	dhtOptions := []dht.Option{
		dht.BootstrapPeers(bootstrap...),
		dht.Mode(mode),
	}
	if config.AllowPrivateDHTPeers {
		// Local integration tests can use loopback DHT peers. Do not enable this
		// for a public DHT because private nodes should not enter its routing table.
		dhtOptions = append(dhtOptions, dht.RoutingTableFilter(func(interface{}, peer.ID) bool {
			return true
		}), dht.QueryFilter(dht.PrivateQueryFilter))
	}
	kad, err := dht.New(ctx, node, dhtOptions...)
	if err != nil {
		cancel()
		_ = node.Close()
		return nil, fmt.Errorf("create Kademlia DHT: %w", err)
	}
	routing := routingdiscovery.NewRoutingDiscovery(kad)
	n := &Network{
		host: node, dht: kad, discovery: routing, store: store,
		bootstrap: bootstrap, relays: relays, namespace: config.DiscoveryTag,
		ctx: ctx, cancel: cancel, synced: make(map[peer.ID]bool),
		peers: make(map[peer.ID]peer.AddrInfo),
	}
	n.notify = &network.NotifyBundle{
		DisconnectedF: func(_ network.Network, connection network.Conn) {
			n.mu.Lock()
			delete(n.synced, connection.RemotePeer())
			n.mu.Unlock()
		},
	}
	node.Network().Notify(n.notify)
	node.SetStreamHandler(replicationProtocol, n.handleReplication)
	if err := kad.Bootstrap(ctx); err != nil {
		n.Close()
		return nil, fmt.Errorf("bootstrap Kademlia DHT: %w", err)
	}
	n.wg.Add(3)
	go n.discoveryLoop()
	go n.advertisementLoop()
	go n.connectionSyncLoop()
	return n, nil
}

// ID returns this node's persistent libp2p peer identity.
func (n *Network) ID() peer.ID {
	return n.host.ID()
}

// Addresses returns announced or observed addresses for sharing with peers.
func (n *Network) Addresses() []string {
	addresses := n.host.Addrs()
	result := make([]string, 0, len(addresses))
	for _, address := range addresses {
		result = append(result, address.Encapsulate(ma.StringCast("/p2p/"+n.ID().String())).String())
	}
	return result
}

// ConnectedPeers returns application peers discovered under this network tag,
// excluding DHT bootstrap and relay helper connections.
func (n *Network) ConnectedPeers() []string {
	connected := n.connectedApplicationPeers()
	result := make([]string, 0, len(connected))
	for _, id := range connected {
		result = append(result, id.String())
	}
	return result
}

// Broadcast sends a newly saved record to all currently connected peers.
func (n *Network) Broadcast(ctx context.Context, record database.Record) error {
	peers := n.connectedApplicationPeers()
	if len(peers) == 0 {
		return errors.New("no discovered application peers connected; record is local and will sync after discovery")
	}
	var failures []string
	for _, id := range peers {
		if err := n.sendRecord(ctx, id, record); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", id, err))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("record saved locally, but sync failed for %s", strings.Join(failures, "; "))
	}
	return nil
}

// Close stops discovery, DHT services, peer connections, and the host.
func (n *Network) Close() error {
	if n == nil {
		return nil
	}
	n.cancel()
	n.host.RemoveStreamHandler(replicationProtocol)
	n.host.Network().StopNotify(n.notify)
	dhtErr := n.dht.Close()
	hostErr := n.host.Close()
	n.wg.Wait()
	return errors.Join(dhtErr, hostErr)
}

// discoveryLoop connects to DHT bootstrap peers, advertises this peer under a
// network-specific tag, and discovers other peers that advertise the same tag.
func (n *Network) discoveryLoop() {
	defer n.wg.Done()
	n.connectHelpers()
	n.refreshRoutingTable()
	ticker := time.NewTicker(discoveryInterval)
	defer ticker.Stop()
	for {
		n.findAndConnectPeers()
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.connectHelpers()
		}
	}
}

// refreshRoutingTable queries known DHT bootstrap peers so this node has
// routing-table entries before it publishes its discovery record.
func (n *Network) refreshRoutingTable() {
	refreshResults := n.dht.RefreshRoutingTable()
	for {
		select {
		case <-n.ctx.Done():
			return
		case err, open := <-refreshResults:
			if !open {
				return
			}
			if err != nil {
				log.Printf("refresh DHT routing table: %v", err)
			}
		}
	}
}

// connectHelpers maintains outbound connections to configured DHT and relay
// peers. These nodes assist networking and do not hold the token database.
func (n *Network) connectHelpers() {
	helpers := append(append([]peer.AddrInfo(nil), n.bootstrap...), n.relays...)
	for _, helper := range helpers {
		if n.host.Network().Connectedness(helper.ID) == network.Connected {
			continue
		}
		ctx, cancel := context.WithTimeout(n.ctx, peerOperationTimeout)
		if err := n.host.Connect(ctx, helper); err != nil && n.ctx.Err() == nil {
			log.Printf("connect to helper %s: %v", helper.ID, err)
		}
		cancel()
	}
}

// advertisementLoop publishes and renews DHT provider records. Renewal keeps
// the peer discoverable after provider records expire.
func (n *Network) advertisementLoop() {
	defer n.wg.Done()
	for n.ctx.Err() == nil {
		ctx, cancel := context.WithTimeout(n.ctx, peerOperationTimeout)
		ttl, err := n.discovery.Advertise(ctx, n.namespace)
		cancel()
		if err != nil {
			if n.ctx.Err() != nil {
				return
			}
			log.Printf("advertise peer in DHT namespace %q: %v", n.namespace, err)
			if !waitContext(n.ctx, 5*time.Second) {
				return
			}
			continue
		}
		renewAfter := ttl / 2
		if renewAfter <= 0 {
			renewAfter = time.Hour
		}
		if !waitContext(n.ctx, renewAfter) {
			return
		}
	}
}

// waitContext waits for a duration unless the peer is shutting down.
func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// findAndConnectPeers discovers nodes advertising this namespace and dials
// them using direct addresses or relay addresses learned through libp2p.
func (n *Network) findAndConnectPeers() {
	ctx, cancel := context.WithTimeout(n.ctx, peerOperationTimeout)
	defer cancel()
	found, err := n.discovery.FindPeers(ctx, n.namespace, discovery.Limit(100))
	if err != nil {
		if n.ctx.Err() == nil {
			log.Printf("find peers in DHT namespace %q: %v", n.namespace, err)
		}
		return
	}
	for info := range found {
		if info.ID == n.ID() {
			continue
		}
		n.mu.Lock()
		n.peers[info.ID] = info
		n.mu.Unlock()
		if n.host.Network().Connectedness(info.ID) == network.Connected {
			continue
		}
		if err := n.host.Connect(ctx, info); err != nil && n.ctx.Err() == nil {
			log.Printf("connect to discovered peer %s: %v", info.ID, err)
		}
	}
}

// connectionSyncLoop finds new connections and exchanges full local snapshots.
func (n *Network) connectionSyncLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		n.syncNewConnections()
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// syncNewConnections detects reconnects and sends this node's local snapshot.
func (n *Network) syncNewConnections() {
	connected := n.connectedApplicationPeers()
	current := make(map[peer.ID]struct{}, len(connected))
	for _, id := range connected {
		current[id] = struct{}{}
	}
	n.mu.Lock()
	for id := range n.synced {
		if _, ok := current[id]; !ok {
			delete(n.synced, id)
		}
	}
	n.mu.Unlock()

	for _, id := range connected {
		n.mu.Lock()
		alreadySynced := n.synced[id]
		if !alreadySynced {
			n.synced[id] = true
		}
		n.mu.Unlock()
		if !alreadySynced {
			if err := n.syncPeer(id); err != nil && n.ctx.Err() == nil {
				log.Printf("initial record sync with %s: %v", id, err)
				n.mu.Lock()
				delete(n.synced, id)
				n.mu.Unlock()
			}
		}
	}
}

// syncPeer sends all local records to one peer after a connection is created.
func (n *Network) syncPeer(id peer.ID) error {
	records, err := n.store.List()
	if err != nil {
		return err
	}
	for _, record := range records {
		ctx, cancel := context.WithTimeout(n.ctx, peerOperationTimeout)
		err := n.sendRecord(ctx, id, record)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

// handleReplication validates one incoming record, applies it if newer, then
// forwards accepted updates through the connected peer graph.
func (n *Network) handleReplication(stream network.Stream) {
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(peerOperationTimeout))
	var incoming replicationMessage
	decoder := json.NewDecoder(io.LimitReader(stream, maxMessageBytes))
	if err := decoder.Decode(&incoming); err != nil {
		_ = json.NewEncoder(stream).Encode(replicationResponse{Error: "invalid record message"})
		return
	}
	applied, err := n.store.ApplyIfNewer(incoming.Record)
	if err != nil {
		_ = json.NewEncoder(stream).Encode(replicationResponse{Error: "record validation or storage failed"})
		return
	}
	if err := json.NewEncoder(stream).Encode(replicationResponse{Applied: applied}); err != nil {
		return
	}
	if applied {
		n.forwardRecord(stream.Conn().RemotePeer(), incoming.Record)
	}
}

// sendRecord sends one record and waits for the remote peer's acknowledgement.
func (n *Network) sendRecord(ctx context.Context, id peer.ID, record database.Record) error {
	stream, err := n.host.NewStream(ctx, id, replicationProtocol)
	if err != nil {
		return fmt.Errorf("open replication stream: %w", err)
	}
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(peerOperationTimeout))
	if err := json.NewEncoder(stream).Encode(replicationMessage{Record: record}); err != nil {
		return fmt.Errorf("send record: %w", err)
	}
	var result replicationResponse
	if err := json.NewDecoder(io.LimitReader(stream, maxMessageBytes)).Decode(&result); err != nil {
		return fmt.Errorf("read peer response: %w", err)
	}
	if result.Error != "" {
		return errors.New(result.Error)
	}
	return nil
}

// forwardRecord relays a new version to connected peers except its sender.
// Version checks make this gossip forwarding idempotent and stop loops.
func (n *Network) forwardRecord(source peer.ID, record database.Record) {
	for _, id := range n.connectedApplicationPeers() {
		if id == source {
			continue
		}
		ctx, cancel := context.WithTimeout(n.ctx, peerOperationTimeout)
		if err := n.sendRecord(ctx, id, record); err != nil && n.ctx.Err() == nil {
			log.Printf("forward record to peer %s: %v", id, err)
		}
		cancel()
	}
}

// connectedApplicationPeers excludes DHT bootstrap and relay helper
// connections, which do not advertise the application's synchronization tag.
func (n *Network) connectedApplicationPeers() []peer.ID {
	connected := n.host.Network().Peers()
	n.mu.Lock()
	defer n.mu.Unlock()
	applicationPeers := make([]peer.ID, 0, len(connected))
	for _, id := range connected {
		if _, discovered := n.peers[id]; discovered {
			applicationPeers = append(applicationPeers, id)
		}
	}
	return applicationPeers
}

// parsePeerInfos parses comma-separated full multiaddresses ending in /p2p/ID.
func parsePeerInfos(rawAddresses []string, label string) ([]peer.AddrInfo, error) {
	infos := make([]peer.AddrInfo, 0, len(rawAddresses))
	seen := make(map[peer.ID]struct{})
	for _, raw := range rawAddresses {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		address, err := ma.NewMultiaddr(raw)
		if err != nil {
			return nil, fmt.Errorf("parse %s address %q: %w", label, raw, err)
		}
		info, err := peer.AddrInfoFromP2pAddr(address)
		if err != nil {
			return nil, fmt.Errorf("%s address %q must include /p2p/peer-id: %w", label, raw, err)
		}
		if _, exists := seen[info.ID]; exists {
			continue
		}
		seen[info.ID] = struct{}{}
		infos = append(infos, *info)
	}
	return infos, nil
}

// parseDHTMode selects whether this peer serves DHT requests or uses the DHT
// only as a client. Auto is appropriate for most nodes behind residential NAT.
func parseDHTMode(raw string) (dht.ModeOpt, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "auto":
		return dht.ModeAuto, nil
	case "client":
		return dht.ModeClient, nil
	case "server":
		return dht.ModeServer, nil
	default:
		return 0, fmt.Errorf("invalid DHT mode %q: choose auto, client, or server", raw)
	}
}

// parseAnnounceAddresses validates optional dialable addresses that do not
// include a peer ID. AutoRelay can append relay circuit addresses to them.
func parseAnnounceAddresses(rawAddresses []string) ([]ma.Multiaddr, error) {
	addresses := make([]ma.Multiaddr, 0, len(rawAddresses))
	for _, raw := range rawAddresses {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		address, err := ma.NewMultiaddr(raw)
		if err != nil {
			return nil, fmt.Errorf("parse announced address %q: %w", raw, err)
		}
		if _, err := peer.AddrInfoFromP2pAddr(address); err == nil {
			return nil, fmt.Errorf("announced address %q must not include /p2p/peer-id", raw)
		}
		addresses = append(addresses, address)
	}
	return addresses, nil
}

// loadOrCreateIdentity creates or loads a private key so peer IDs remain stable.
func loadOrCreateIdentity(path string) (libcrypto.PrivKey, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("identity file path is required")
	}
	encoded, err := os.ReadFile(path)
	if err == nil {
		identity, err := libcrypto.UnmarshalPrivateKey(encoded)
		if err != nil {
			return nil, fmt.Errorf("decode identity key: %w", err)
		}
		return identity, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read identity key: %w", err)
	}
	identity, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate identity key: %w", err)
	}
	encoded, err = libcrypto.MarshalPrivateKey(identity)
	if err != nil {
		return nil, fmt.Errorf("encode identity key: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, fmt.Errorf("create identity directory: %w", err)
	}
	if err := os.WriteFile(path, encoded, 0600); err != nil {
		return nil, fmt.Errorf("persist identity key: %w", err)
	}
	return identity, nil
}
