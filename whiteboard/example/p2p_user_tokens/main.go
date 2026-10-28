package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	httpAddress := flag.String("http", ":8083", "HTTP address for this node's web UI")
	p2pAddress := flag.String("p2p", "/ip4/0.0.0.0/tcp/4002", "libp2p listen multiaddress")
	announceList := flag.String("announce", "", "comma-separated externally dialable P2P multiaddresses, without /p2p/peer-id")
	databasePath := flag.String("db", "./data", "node-local LevelDB directory")
	identityPath := flag.String("identity", "", "node identity file (defaults beside the database)")
	peerList := flag.String("peers", "", "comma-separated libp2p peer multiaddresses")
	flag.Parse()
	if *identityPath == "" {
		*identityPath = filepath.Join(filepath.Dir(*databasePath), filepath.Base(*databasePath)+".peer.key")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := OpenStore(*databasePath)
	if err != nil {
		log.Fatalf("open token database: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("close token database: %v", err)
		}
	}()

	peers, err := newPeerNetwork(ctx, *p2pAddress, splitPeerAddresses(*peerList), splitPeerAddresses(*announceList), *identityPath, store)
	if err != nil {
		log.Fatalf("start peer network: %v", err)
	}
	defer func() {
		if err := peers.Close(); err != nil {
			log.Printf("close peer network: %v", err)
		}
	}()

	handler, err := NewHandler(store, peers)
	if err != nil {
		log.Fatalf("create HTTP handler: %v", err)
	}
	server := &http.Server{
		Addr:              *httpAddress,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- server.ListenAndServe()
	}()

	fmt.Printf("Node peer ID: %s\n", peers.PeerID())
	fmt.Printf("P2P addresses:\n")
	for _, address := range peers.ListenAddresses() {
		fmt.Printf("  %s\n", address)
	}
	fmt.Printf("Web UI: http://localhost%s\n", *httpAddress)

	select {
	case err := <-serveErrors:
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve web UI: %v", err)
		}
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			log.Printf("shut down web server: %v", err)
			if closeErr := server.Close(); closeErr != nil {
				log.Printf("close web server: %v", closeErr)
			}
		}
	}
}

// splitPeerAddresses turns a comma-separated flag into trimmed address values.
func splitPeerAddresses(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	addresses := make([]string, 0, len(parts))
	for _, part := range parts {
		if address := strings.TrimSpace(part); address != "" {
			addresses = append(addresses, address)
		}
	}
	return addresses
}
