// Command token-node runs one local token database peer with DHT discovery.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"sunya-p2p-dht-tokens/internal/database"
	"sunya-p2p-dht-tokens/internal/peer"
	"sunya-p2p-dht-tokens/internal/web"
)

// main parses node settings and starts the database, libp2p network, DHT, and
// local web UI. The process exits cleanly on Ctrl+C or SIGTERM.
func main() {
	httpAddress := flag.String("http", "127.0.0.1:8083", "HTTP address for this node's local UI")
	listenAddress := flag.String("p2p", "/ip4/0.0.0.0/tcp/4002", "libp2p listen multiaddress")
	announceAddresses := flag.String("announce", "", "comma-separated externally dialable addresses without /p2p/peer-id")
	databasePath := flag.String("db", "./data/node", "node-local LevelDB directory")
	identityPath := flag.String("identity", "", "persistent peer identity key file")
	bootstrapAddresses := flag.String("bootstrap", "", "comma-separated reachable Kademlia DHT bootstrap multiaddresses")
	relayAddresses := flag.String("relays", "", "comma-separated Circuit Relay v2 peer multiaddresses")
	discoveryTag := flag.String("network", "", "DHT rendezvous namespace shared by this peer group (choose a unique value)")
	dhtMode := flag.String("dht-mode", "auto", "DHT participation mode: auto, client, or server")
	flag.Parse()
	if *identityPath == "" {
		*identityPath = *databasePath + ".peer.key"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *httpAddress, peer.Config{
		ListenAddress:   *listenAddress,
		AnnounceAddress: split(*announceAddresses),
		BootstrapPeers:  split(*bootstrapAddresses),
		RelayPeers:      split(*relayAddresses),
		DiscoveryTag:    *discoveryTag,
		IdentityPath:    *identityPath,
		DHTMode:         *dhtMode,
	}, *databasePath); err != nil {
		log.Fatal(err)
	}
}

// run owns the application lifecycle and reports startup, serving, and
// shutdown errors to main.
func run(ctx context.Context, httpAddress string, config peer.Config, databasePath string) error {
	store, err := database.Open(databasePath)
	if err != nil {
		return err
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("close database: %v", err)
		}
	}()

	network, err := peer.New(ctx, config, store)
	if err != nil {
		return err
	}
	defer func() {
		if err := network.Close(); err != nil {
			log.Printf("close P2P network: %v", err)
		}
	}()

	handler, err := web.NewHandler(store, network)
	if err != nil {
		return fmt.Errorf("create web handler: %w", err)
	}
	server := &http.Server{
		Addr: httpAddress, Handler: handler, ReadHeaderTimeout: 5 * time.Second,
	}
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- server.ListenAndServe()
	}()

	fmt.Printf("Peer ID: %s\n", network.ID())
	fmt.Println("Peer addresses (direct and, when reserved, relay):")
	for _, address := range network.Addresses() {
		fmt.Printf("  %s\n", address)
	}
	fmt.Printf("DHT network tag: %s\n", config.DiscoveryTag)
	fmt.Printf("Local UI: http://%s\n", httpAddress)

	select {
	case err := <-serveErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve UI: %w", err)
		}
		return nil
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			closeErr := server.Close()
			return errors.Join(fmt.Errorf("shut down UI: %w", err), closeErr)
		}
		return nil
	}
}

// split returns trimmed non-empty values from a comma-separated flag.
func split(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			values = append(values, value)
		}
	}
	return values
}
