// Package peer manages direct libp2p connections and full database replication.
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
	libcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"
	"sunya-p2p-mesh-tokens/internal/database"
)

const (
	protocolID = protocol.ID("/sunya/full-token-sync/1.0.0")
	maxMessage = 1 << 20
	timeout    = 12 * time.Second
)

// message carries one token record over an encrypted libp2p stream.
type message struct {
	Record database.Record `json:"record"`
}

// response confirms acceptance or reports a protocol-level error.
type response struct {
	Applied bool   `json:"applied"`
	Error   string `json:"error,omitempty"`
}

// Network owns a peer host, reconnects known peers, and synchronizes complete
// local snapshots whenever a connection is established.
type Network struct {
	host      host.Host
	store     *database.Store
	seeds     []peer.AddrInfo
	announced []ma.Multiaddr
	ctx       context.Context
	cancel    context.CancelFunc

	mu     sync.Mutex
	synced map[peer.ID]bool
	wg     sync.WaitGroup
	notify *network.NotifyBundle
}

// New starts the libp2p host. Peer addresses must be supplied out of band;
// this project intentionally has no rendezvous server or automatic discovery.
func New(parent context.Context, listenAddress string, seedAddresses, announceAddresses []string, identityPath string, store *database.Store) (*Network, error) {
	if parent == nil || store == nil {
		return nil, errors.New("parent context and database are required")
	}
	identity, err := loadOrCreateIdentity(identityPath)
	if err != nil {
		return nil, fmt.Errorf("load peer identity: %w", err)
	}
	listen, err := ma.NewMultiaddr(listenAddress)
	if err != nil {
		return nil, fmt.Errorf("parse listen address: %w", err)
	}
	announced, err := parseAnnounceAddresses(announceAddresses)
	if err != nil {
		return nil, err
	}

	options := []libp2p.Option{
		libp2p.Identity(identity),
		libp2p.ListenAddrs(listen),
		// Hole punching can help when two NATs permit coordinated direct
		// connections. It still needs a working path for coordination.
		libp2p.EnableHolePunching(),
	}
	if len(announced) > 0 {
		options = append(options, libp2p.AddrsFactory(func([]ma.Multiaddr) []ma.Multiaddr {
			return append([]ma.Multiaddr(nil), announced...)
		}))
	}
	node, err := libp2p.New(options...)
	if err != nil {
		return nil, fmt.Errorf("start libp2p host: %w", err)
	}

	seeds := make([]peer.AddrInfo, 0, len(seedAddresses))
	for _, raw := range seedAddresses {
		address, err := ma.NewMultiaddr(strings.TrimSpace(raw))
		if err != nil {
			_ = node.Close()
			return nil, fmt.Errorf("parse peer multiaddress %q: %w", raw, err)
		}
		info, err := peer.AddrInfoFromP2pAddr(address)
		if err != nil {
			_ = node.Close()
			return nil, fmt.Errorf("peer address must include /p2p/peer-id: %w", err)
		}
		seeds = append(seeds, *info)
	}

	ctx, cancel := context.WithCancel(parent)
	n := &Network{
		host: node, store: store, seeds: seeds, announced: announced,
		ctx: ctx, cancel: cancel, synced: make(map[peer.ID]bool),
	}
	n.notify = &network.NotifyBundle{
		DisconnectedF: func(_ network.Network, connection network.Conn) {
			n.mu.Lock()
			delete(n.synced, connection.RemotePeer())
			n.mu.Unlock()
		},
	}
	node.Network().Notify(n.notify)
	node.SetStreamHandler(protocolID, n.handleStream)
	n.wg.Add(1)
	go n.maintainConnections()
	return n, nil
}

// ID returns this node's persistent libp2p identity.
func (n *Network) ID() peer.ID {
	return n.host.ID()
}

// Addresses returns addresses a peer can copy to dial this node.
func (n *Network) Addresses() []string {
	addresses := n.announced
	if len(addresses) == 0 {
		addresses = n.host.Addrs()
	}
	result := make([]string, 0, len(addresses))
	for _, address := range addresses {
		result = append(result, address.Encapsulate(ma.StringCast("/p2p/"+n.ID().String())).String())
	}
	return result
}

// ConnectedPeers returns peer IDs with live network connections.
func (n *Network) ConnectedPeers() []string {
	connected := n.host.Network().Peers()
	result := make([]string, 0, len(connected))
	for _, id := range connected {
		result = append(result, id.String())
	}
	return result
}

// Broadcast sends a newly saved record to every currently connected peer.
func (n *Network) Broadcast(ctx context.Context, record database.Record) error {
	peers := n.host.Network().Peers()
	if len(peers) == 0 {
		return errors.New("no peers connected; record is saved locally and will sync after connection")
	}
	var failures []string
	for _, id := range peers {
		if err := n.sendRecord(ctx, id, record); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", id, err))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("saved locally, but replication failed for %s", strings.Join(failures, "; "))
	}
	return nil
}

// Close cancels reconnect attempts and closes the libp2p host.
func (n *Network) Close() error {
	if n == nil {
		return nil
	}
	n.cancel()
	n.host.RemoveStreamHandler(protocolID)
	n.host.Network().StopNotify(n.notify)
	err := n.host.Close()
	n.wg.Wait()
	return err
}

// handleStream validates and applies one incoming record before forwarding
// newer versions to the other directly connected peers.
func (n *Network) handleStream(stream network.Stream) {
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(timeout))
	var incoming message
	decoder := json.NewDecoder(io.LimitReader(stream, maxMessage))
	if err := decoder.Decode(&incoming); err != nil {
		_ = json.NewEncoder(stream).Encode(response{Error: "invalid record message"})
		return
	}
	applied, err := n.store.ApplyIfNewer(incoming.Record)
	if err != nil {
		_ = json.NewEncoder(stream).Encode(response{Error: err.Error()})
		return
	}
	if err := json.NewEncoder(stream).Encode(response{Applied: applied}); err != nil {
		return
	}
	if applied {
		n.forwardRecord(stream.Conn().RemotePeer(), incoming.Record)
	}
}

// maintainConnections retries configured peers and performs an initial
// full-snapshot sync after each new connection or reconnection.
func (n *Network) maintainConnections() {
	defer n.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if n.ctx.Err() != nil {
			return
		}
		for _, seed := range n.seeds {
			if n.host.Network().Connectedness(seed.ID) != network.Connected {
				ctx, cancel := context.WithTimeout(n.ctx, timeout)
				if err := n.host.Connect(ctx, seed); err != nil && n.ctx.Err() == nil {
					log.Printf("connect to peer %s: %v", seed.ID, err)
				}
				cancel()
			}
		}
		n.syncNewConnections()
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// syncNewConnections drops disconnected peers from sync state and syncs each
// newly connected peer exactly once for the current connection.
func (n *Network) syncNewConnections() {
	connected := n.host.Network().Peers()
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
				log.Printf("initial database sync with %s: %v", id, err)
				n.mu.Lock()
				delete(n.synced, id)
				n.mu.Unlock()
			}
		}
	}
}

// syncPeer sends every local record to a newly connected peer.
func (n *Network) syncPeer(id peer.ID) error {
	records, err := n.store.List()
	if err != nil {
		return err
	}
	for _, record := range records {
		ctx, cancel := context.WithTimeout(n.ctx, timeout)
		err := n.sendRecord(ctx, id, record)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

// sendRecord writes one record and waits for its peer's acknowledgement.
func (n *Network) sendRecord(ctx context.Context, id peer.ID, record database.Record) error {
	stream, err := n.host.NewStream(ctx, id, protocolID)
	if err != nil {
		return fmt.Errorf("open replication stream: %w", err)
	}
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(timeout))
	if err := json.NewEncoder(stream).Encode(message{Record: record}); err != nil {
		return fmt.Errorf("send record: %w", err)
	}
	var result response
	if err := json.NewDecoder(io.LimitReader(stream, maxMessage)).Decode(&result); err != nil {
		return fmt.Errorf("read peer response: %w", err)
	}
	if result.Error != "" {
		return errors.New(result.Error)
	}
	return nil
}

// forwardRecord propagates a newly accepted version without sending it back
// to its source. Version checks prevent replication loops.
func (n *Network) forwardRecord(source peer.ID, record database.Record) {
	for _, id := range n.host.Network().Peers() {
		if id == source {
			continue
		}
		ctx, cancel := context.WithTimeout(n.ctx, timeout)
		if err := n.sendRecord(ctx, id, record); err != nil && n.ctx.Err() == nil {
			log.Printf("forward record to peer %s: %v", id, err)
		}
		cancel()
	}
}

// parseAnnounceAddresses checks optional externally dialable addresses.
func parseAnnounceAddresses(rawAddresses []string) ([]ma.Multiaddr, error) {
	addresses := make([]ma.Multiaddr, 0, len(rawAddresses))
	for _, raw := range rawAddresses {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		address, err := ma.NewMultiaddr(strings.TrimSpace(raw))
		if err != nil {
			return nil, fmt.Errorf("parse announced address %q: %w", raw, err)
		}
		if _, err := peer.AddrInfoFromP2pAddr(address); err == nil {
			return nil, fmt.Errorf("announced address %q must not contain /p2p/peer-id", raw)
		}
		addresses = append(addresses, address)
	}
	return addresses, nil
}

// loadOrCreateIdentity persists an Ed25519 key so a peer ID survives restarts.
func loadOrCreateIdentity(path string) (libcrypto.PrivKey, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("identity file path is required")
	}
	data, err := os.ReadFile(path)
	if err == nil {
		key, err := libcrypto.UnmarshalPrivateKey(data)
		if err != nil {
			return nil, fmt.Errorf("decode identity key: %w", err)
		}
		return key, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read identity key: %w", err)
	}
	key, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate identity key: %w", err)
	}
	data, err = libcrypto.MarshalPrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("encode identity key: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, fmt.Errorf("create identity directory: %w", err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		return nil, fmt.Errorf("write identity key: %w", err)
	}
	return key, nil
}
