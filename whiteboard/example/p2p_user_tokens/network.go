package main

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
)

const (
	replicationProtocol  = protocol.ID("/sunya/user-token/1.0.0")
	maxPeerMessageBytes  = 1 << 20
	peerOperationTimeout = 10 * time.Second
)

type replicationMessage struct {
	Record userTokenRecord `json:"record"`
}

type replicationResponse struct {
	Applied bool   `json:"applied"`
	Error   string `json:"error,omitempty"`
}

// peerNetwork owns this node's libp2p host and synchronizes its local records
// with configured bootstrap peers and any connected inbound peers.
type peerNetwork struct {
	host      host.Host
	store     *userTokenStore
	seeds     []peer.AddrInfo
	announced []ma.Multiaddr
	ctx       context.Context
	cancel    context.CancelFunc

	mu       sync.Mutex
	synced   map[peer.ID]bool
	workerMu sync.Mutex
	closing  bool
	wg       sync.WaitGroup
}

// newPeerNetwork creates an encrypted libp2p host with a persistent identity.
// announceAddresses optionally overrides wildcard listen addresses with host
// addresses that remote peers can actually dial.
func newPeerNetwork(parent context.Context, listenAddress string, seedAddresses, announceAddresses []string, identityPath string, store *userTokenStore) (*peerNetwork, error) {
	if parent == nil {
		return nil, errors.New("peer network context is required")
	}
	if store == nil {
		return nil, errors.New("peer network database is required")
	}
	identity, err := loadOrCreateIdentity(identityPath)
	if err != nil {
		return nil, fmt.Errorf("load peer identity: %w", err)
	}
	address, err := ma.NewMultiaddr(listenAddress)
	if err != nil {
		return nil, fmt.Errorf("parse P2P listen address: %w", err)
	}
	announced, err := parseAnnounceAddresses(announceAddresses)
	if err != nil {
		return nil, err
	}
	options := []libp2p.Option{
		libp2p.Identity(identity),
		libp2p.ListenAddrs(address),
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
	for _, rawAddress := range seedAddresses {
		rawAddress = strings.TrimSpace(rawAddress)
		if rawAddress == "" {
			continue
		}
		addr, err := ma.NewMultiaddr(rawAddress)
		if err != nil {
			_ = node.Close()
			return nil, fmt.Errorf("parse peer address %q: %w", rawAddress, err)
		}
		info, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			_ = node.Close()
			return nil, fmt.Errorf("parse peer identity from %q: %w", rawAddress, err)
		}
		seeds = append(seeds, *info)
	}

	ctx, cancel := context.WithCancel(parent)
	n := &peerNetwork{
		host: node, store: store, seeds: seeds, announced: announced,
		ctx: ctx, cancel: cancel, synced: make(map[peer.ID]bool),
	}
	node.SetStreamHandler(replicationProtocol, n.handleReplication)
	n.wg.Add(1)
	go n.maintainConnections()
	return n, nil
}

// PeerID returns the stable libp2p identity of this node.
func (n *peerNetwork) PeerID() peer.ID {
	return n.host.ID()
}

// ListenAddresses returns dialable multiaddresses including this node's ID.
func (n *peerNetwork) ListenAddresses() []string {
	baseAddresses := n.announced
	if len(baseAddresses) == 0 {
		baseAddresses = n.host.Addrs()
	}
	addresses := make([]string, 0, len(baseAddresses))
	for _, address := range baseAddresses {
		addresses = append(addresses, address.Encapsulate(ma.StringCast("/p2p/"+n.host.ID().String())).String())
	}
	return addresses
}

// parseAnnounceAddresses validates optional dialable multiaddresses supplied
// by the operator. They should contain the external IP/DNS name and TCP port,
// without a trailing /p2p/peer-id component.
func parseAnnounceAddresses(rawAddresses []string) ([]ma.Multiaddr, error) {
	addresses := make([]ma.Multiaddr, 0, len(rawAddresses))
	for _, rawAddress := range rawAddresses {
		rawAddress = strings.TrimSpace(rawAddress)
		if rawAddress == "" {
			continue
		}
		address, err := ma.NewMultiaddr(rawAddress)
		if err != nil {
			return nil, fmt.Errorf("parse advertised address %q: %w", rawAddress, err)
		}
		if _, err := peer.AddrInfoFromP2pAddr(address); err == nil {
			return nil, fmt.Errorf("advertised address %q must not include /p2p/peer-id", rawAddress)
		}
		addresses = append(addresses, address)
	}
	return addresses, nil
}

// ConnectedPeers returns the IDs of currently connected peers.
func (n *peerNetwork) ConnectedPeers() []string {
	peers := n.host.Network().Peers()
	ids := make([]string, 0, len(peers))
	for _, id := range peers {
		ids = append(ids, id.String())
	}
	return ids
}

// Replicate sends record to every currently connected peer. A local save is
// kept even when this reports a transport error; the UI presents that status.
func (n *peerNetwork) Replicate(ctx context.Context, record userTokenRecord) error {
	peers := n.host.Network().Peers()
	if len(peers) == 0 {
		return errors.New("no peers are connected; the record is saved locally only")
	}
	var failures []string
	for _, id := range peers {
		if err := n.sendRecord(ctx, id, record); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", id, err))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("record saved locally but replication failed for %s", strings.Join(failures, "; "))
	}
	return nil
}

// Close stops peer maintenance and closes the libp2p host.
func (n *peerNetwork) Close() error {
	if n == nil {
		return nil
	}
	n.workerMu.Lock()
	n.closing = true
	n.workerMu.Unlock()
	n.cancel()
	n.host.RemoveStreamHandler(replicationProtocol)
	hostErr := n.host.Close()
	n.wg.Wait()
	return hostErr
}

// handleReplication applies one record, responds to the sender, then forwards
// accepted updates so synchronization propagates beyond direct neighbors.
func (n *peerNetwork) handleReplication(stream network.Stream) {
	if !n.beginWorker() {
		_ = stream.Reset()
		return
	}
	defer n.wg.Done()
	defer stream.Close()
	_ = stream.SetReadDeadline(time.Now().Add(peerOperationTimeout))
	_ = stream.SetWriteDeadline(time.Now().Add(peerOperationTimeout))

	var message replicationMessage
	decoder := json.NewDecoder(io.LimitReader(stream, maxPeerMessageBytes))
	if err := decoder.Decode(&message); err != nil {
		_ = json.NewEncoder(stream).Encode(replicationResponse{Error: "invalid replication message"})
		return
	}
	applied, err := n.store.ApplyIfNewer(message.Record)
	if err != nil {
		_ = json.NewEncoder(stream).Encode(replicationResponse{Error: err.Error()})
		return
	}
	if err := json.NewEncoder(stream).Encode(replicationResponse{Applied: applied}); err != nil {
		return
	}
	if applied {
		n.broadcastAcceptedRecord(stream.Conn().RemotePeer(), message.Record)
	}
}

// maintainConnections retries configured seeds and sends a full local snapshot
// once to every newly connected peer.
func (n *peerNetwork) maintainConnections() {
	defer n.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if n.ctx.Err() != nil {
			return
		}
		for _, seed := range n.seeds {
			if n.host.Network().Connectedness(seed.ID) != network.Connected {
				ctx, cancel := context.WithTimeout(n.ctx, peerOperationTimeout)
				if err := n.host.Connect(ctx, seed); err != nil && n.ctx.Err() == nil {
					log.Printf("connect to peer %s: %v", seed.ID, err)
				}
				cancel()
			}
		}
		connected := n.host.Network().Peers()
		connectedSet := make(map[peer.ID]struct{}, len(connected))
		for _, id := range connected {
			connectedSet[id] = struct{}{}
		}
		n.mu.Lock()
		for id := range n.synced {
			if _, isConnected := connectedSet[id]; !isConnected {
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
					log.Printf("initial sync with peer %s: %v", id, err)
					n.mu.Lock()
					delete(n.synced, id)
					n.mu.Unlock()
				}
			}
		}
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// beginWorker prevents new stream handlers from joining the wait group during
// shutdown, avoiding Add/Wait races.
func (n *peerNetwork) beginWorker() bool {
	n.workerMu.Lock()
	defer n.workerMu.Unlock()
	if n.closing {
		return false
	}
	n.wg.Add(1)
	return true
}

// syncPeer sends every locally stored record to one newly connected peer.
func (n *peerNetwork) syncPeer(id peer.ID) error {
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

// sendRecord opens one bounded request/response stream to a peer.
func (n *peerNetwork) sendRecord(ctx context.Context, id peer.ID, record userTokenRecord) error {
	stream, err := n.host.NewStream(ctx, id, replicationProtocol)
	if err != nil {
		return fmt.Errorf("open replication stream: %w", err)
	}
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(peerOperationTimeout))
	if err := json.NewEncoder(stream).Encode(replicationMessage{Record: record}); err != nil {
		return fmt.Errorf("send replication record: %w", err)
	}
	var response replicationResponse
	if err := json.NewDecoder(io.LimitReader(stream, maxPeerMessageBytes)).Decode(&response); err != nil {
		return fmt.Errorf("read replication response: %w", err)
	}
	if response.Error != "" {
		return errors.New(response.Error)
	}
	return nil
}

// broadcastAcceptedRecord forwards a newly accepted version to every connected
// peer except its sender. Idempotent version checks stop replication loops.
func (n *peerNetwork) broadcastAcceptedRecord(exclude peer.ID, record userTokenRecord) {
	for _, id := range n.host.Network().Peers() {
		if id == exclude {
			continue
		}
		ctx, cancel := context.WithTimeout(n.ctx, peerOperationTimeout)
		if err := n.sendRecord(ctx, id, record); err != nil && n.ctx.Err() == nil {
			log.Printf("forward token record to peer %s: %v", id, err)
		}
		cancel()
	}
}

// loadOrCreateIdentity keeps a node's peer ID stable across process restarts.
func loadOrCreateIdentity(path string) (libcrypto.PrivKey, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("identity file path is required")
	}
	data, err := os.ReadFile(path)
	if err == nil {
		identity, err := libcrypto.UnmarshalPrivateKey(data)
		if err != nil {
			return nil, fmt.Errorf("decode identity file: %w", err)
		}
		return identity, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read identity file: %w", err)
	}

	identity, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate Ed25519 identity: %w", err)
	}
	encoded, err := libcrypto.MarshalPrivateKey(identity)
	if err != nil {
		return nil, fmt.Errorf("encode identity file: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, fmt.Errorf("create identity directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil, fmt.Errorf("read concurrently created identity file: %w", readErr)
		}
		identity, decodeErr := libcrypto.UnmarshalPrivateKey(data)
		if decodeErr != nil {
			return nil, fmt.Errorf("decode concurrently created identity file: %w", decodeErr)
		}
		return identity, nil
	}
	if err != nil {
		return nil, fmt.Errorf("create identity file: %w", err)
	}
	if _, err := file.Write(encoded); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("write identity file: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return nil, fmt.Errorf("close identity file: %w", err)
	}
	return identity, nil
}
