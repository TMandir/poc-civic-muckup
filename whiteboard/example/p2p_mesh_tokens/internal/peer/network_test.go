package peer

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	libpeer "github.com/libp2p/go-libp2p/core/peer"
	"sunya-p2p-mesh-tokens/internal/compare"
	"sunya-p2p-mesh-tokens/internal/database"
)

func TestPeersExchangeFullDatabaseAndNewRecords(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstStore, err := database.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open first database: %v", err)
	}
	defer firstStore.Close()
	secondStore, err := database.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open second database: %v", err)
	}
	defer secondStore.Close()

	firstRecord := database.Record{
		FrontendTokenKey: "initial", Token: "saved-before-peer-join", Information: "initial",
		UpdatedAt: time.Now().UTC(), Origin: "peer-initial",
	}
	if _, err := firstStore.ApplyIfNewer(firstRecord); err != nil {
		t.Fatalf("save initial record: %v", err)
	}

	firstPort, secondPort := freePort(t), freePort(t)
	firstListen := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", firstPort)
	secondListen := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", secondPort)
	first, err := New(ctx, firstListen, nil, []string{firstListen}, t.TempDir()+"/first.key", firstStore)
	if err != nil {
		t.Fatalf("start first peer: %v", err)
	}
	defer first.Close()
	seedAddress := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", firstPort, first.ID())
	secondIdentity := t.TempDir() + "/second.key"
	second, err := New(ctx, secondListen, []string{seedAddress}, nil, secondIdentity, secondStore)
	if err != nil {
		t.Fatalf("start joining peer: %v", err)
	}
	defer second.Close()

	waitFor(t, 12*time.Second, func() bool {
		record, err := secondStore.Lookup("initial")
		return err == nil && record.Token == "saved-before-peer-join"
	})
	newRecord := database.Record{
		FrontendTokenKey: "live", Token: "live-update", Information: "sent after connection",
		UpdatedAt: time.Now().UTC(), Origin: first.ID().String(),
	}
	if _, err := firstStore.ApplyIfNewer(newRecord); err != nil {
		t.Fatalf("save live update: %v", err)
	}
	if err := first.Broadcast(ctx, newRecord); err != nil {
		t.Fatalf("broadcast live update: %v", err)
	}
	waitFor(t, 12*time.Second, func() bool {
		record, err := secondStore.Lookup("live")
		return err == nil && record.Token == "live-update"
	})
	result, err := compare.CompareTokenValues(secondStore, "initial", "live")
	if err != nil {
		t.Fatalf("compare records in joining peer's local database: %v", err)
	}
	if got := compare.ConcatenateTokenValues(result); got != "saved-before-peer-joinlive-update" {
		t.Fatalf("joined peer comparison = %q", got)
	}

	if err := second.Close(); err != nil {
		t.Fatalf("stop joining peer: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		return len(first.ConnectedPeers()) == 0
	})
	firstOfflineRecord := database.Record{
		FrontendTokenKey: "while-first-online", Token: "from-first", Information: "offline interval",
		UpdatedAt: time.Now().UTC(), Origin: first.ID().String(),
	}
	secondOfflineRecord := database.Record{
		FrontendTokenKey: "while-second-offline", Token: "from-second", Information: "offline interval",
		UpdatedAt: time.Now().UTC(), Origin: second.ID().String(),
	}
	if _, err := firstStore.ApplyIfNewer(firstOfflineRecord); err != nil {
		t.Fatalf("save first offline-interval record: %v", err)
	}
	if _, err := secondStore.ApplyIfNewer(secondOfflineRecord); err != nil {
		t.Fatalf("save second offline-interval record: %v", err)
	}
	restarted, err := New(ctx, secondListen, []string{seedAddress}, nil, secondIdentity, secondStore)
	if err != nil {
		t.Fatalf("restart joining peer: %v", err)
	}
	defer restarted.Close()
	waitFor(t, 12*time.Second, func() bool {
		firstGot, firstErr := firstStore.Lookup("while-second-offline")
		secondGot, secondErr := secondStore.Lookup("while-first-online")
		return firstErr == nil && secondErr == nil &&
			firstGot.Token == "from-second" && secondGot.Token == "from-first"
	})
}

func TestLoadOrCreateIdentityPreservesPeerID(t *testing.T) {
	path := t.TempDir() + "/peer.key"
	first, err := loadOrCreateIdentity(path)
	if err != nil {
		t.Fatalf("create identity: %v", err)
	}
	second, err := loadOrCreateIdentity(path)
	if err != nil {
		t.Fatalf("load identity: %v", err)
	}
	firstID, err := libpeer.IDFromPrivateKey(first)
	if err != nil {
		t.Fatalf("derive first ID: %v", err)
	}
	secondID, err := libpeer.IDFromPrivateKey(second)
	if err != nil {
		t.Fatalf("derive second ID: %v", err)
	}
	if firstID != secondID {
		t.Fatalf("peer ID changed from %s to %s", firstID, secondID)
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve TCP port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("release TCP port: %v", err)
	}
	return port
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
