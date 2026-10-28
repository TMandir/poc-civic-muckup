// Command token-node runs one local database peer and its browser UI.
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

	"sunya-p2p-mesh-tokens/internal/database"
	"sunya-p2p-mesh-tokens/internal/peer"
	"sunya-p2p-mesh-tokens/internal/web"
)

// main parses node settings, starts the database/network/UI, and shuts them
// down gracefully on an interrupt or termination signal.
func main() {
	httpAddress := flag.String("http", ":8083", "HTTP address for this node's local UI")
	listenAddress := flag.String("p2p", "/ip4/0.0.0.0/tcp/4002", "libp2p listen multiaddress")
	announce := flag.String("announce", "", "comma-separated externally dialable P2P addresses without /p2p/peer-id")
	databasePath := flag.String("db", "./data/node", "local LevelDB directory")
	identityPath := flag.String("identity", "", "persistent peer identity key file")
	peerAddresses := flag.String("peers", "", "comma-separated peer multiaddresses including /p2p/peer-id")
	flag.Parse()
	if *identityPath == "" {
		*identityPath = identityForDatabase(*databasePath)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *httpAddress, *listenAddress, split(*announce), *databasePath, *identityPath, split(*peerAddresses)); err != nil {
		log.Fatal(err)
	}
}

// run owns the application lifecycle and returns startup, serving, or shutdown
// errors to main.
func run(ctx context.Context, httpAddress, listenAddress string, announced []string, databasePath, identityPath string, peerAddresses []string) error {
	store, err := database.Open(databasePath)
	if err != nil {
		return err
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("close database: %v", err)
		}
	}()

	network, err := peer.New(ctx, listenAddress, peerAddresses, announced, identityPath, store)
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
	fmt.Println("Peer dial addresses:")
	for _, address := range network.Addresses() {
		fmt.Printf("  %s\n", address)
	}
	fmt.Printf("Local UI: http://localhost%s\n", httpAddress)

	select {
	case err := <-serveErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve UI: %w", err)
		}
		return nil
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
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

// identityForDatabase returns a stable, sibling path for a node identity.
func identityForDatabase(databasePath string) string {
	return databasePath + ".peer.key"
}
