package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/syndtr/goleveldb/leveldb"
)

func TestCompareTokenValuesReadsLocalDatabase(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	for _, record := range []userTokenRecord{
		{FrontendTokenKey: "first", Token: "one", Information: "a", UpdatedAt: time.Now(), Origin: "peer-a"},
		{FrontendTokenKey: "second", Token: "🧭two", Information: "b", UpdatedAt: time.Now(), Origin: "peer-b"},
	} {
		if _, err := store.ApplyIfNewer(record); err != nil {
			t.Fatalf("save record: %v", err)
		}
	}

	comparison, err := CompareTokenValues(store, "first", "second")
	if err != nil {
		t.Fatalf("compare local records: %v", err)
	}
	if comparison.FirstValueLength != 3 || comparison.SecondValueLength != 4 {
		t.Fatalf("value lengths = %d and %d, want 3 and 4", comparison.FirstValueLength, comparison.SecondValueLength)
	}
	if got := ConcatenateTokenValues(comparison); got != "one🧭two" {
		t.Fatalf("concatenated value = %q", got)
	}
}

func TestApplyIfNewerConvergesOnTimestampAndTieBreak(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	older := userTokenRecord{FrontendTokenKey: "key", Token: "older", Information: "a", UpdatedAt: at, Origin: "peer-a"}
	newer := userTokenRecord{FrontendTokenKey: "key", Token: "newer", Information: "a", UpdatedAt: at.Add(time.Second), Origin: "peer-a"}
	tieLoser := userTokenRecord{FrontendTokenKey: "tie-key", Token: "tie-loser", Information: "a", UpdatedAt: at, Origin: "peer-a"}
	tieWinner := userTokenRecord{FrontendTokenKey: "tie-key", Token: "tie-winner", Information: "a", UpdatedAt: at, Origin: "peer-z"}

	for _, record := range []userTokenRecord{newer, older, tieLoser, tieWinner} {
		if _, err := store.ApplyIfNewer(record); err != nil {
			t.Fatalf("apply record: %v", err)
		}
	}
	got, err := store.Lookup("key")
	if err != nil {
		t.Fatalf("lookup winner: %v", err)
	}
	if got.Token != "newer" {
		t.Fatalf("winning token = %q, want newer", got.Token)
	}
	got, err = store.Lookup("tie-key")
	if err != nil {
		t.Fatalf("lookup tie-break winner: %v", err)
	}
	if got.Token != "tie-winner" {
		t.Fatalf("equal-timestamp winning token = %q, want tie-winner", got.Token)
	}
}

func TestPeerReplicationUpdatesRemoteLocalDatabase(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	firstStore, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatalf("open first store: %v", err)
	}
	defer firstStore.Close()
	secondStore, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatalf("open second store: %v", err)
	}
	defer secondStore.Close()

	initialRecord := userTokenRecord{
		FrontendTokenKey: "initial", Token: "from-initial-sync", Information: "test",
		UpdatedAt: time.Now().UTC(), Origin: "seed-peer",
	}
	if _, err := firstStore.ApplyIfNewer(initialRecord); err != nil {
		t.Fatalf("save initial record: %v", err)
	}
	firstPort := availableTCPPort(t)
	secondPort := availableTCPPort(t)
	firstListenAddress := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", firstPort)
	first, err := newPeerNetwork(ctx, firstListenAddress, nil, []string{firstListenAddress}, t.TempDir()+"/first.peer.key", firstStore)
	if err != nil {
		t.Fatalf("start first peer: %v", err)
	}
	defer first.Close()
	if got, want := first.ListenAddresses()[0], firstListenAddress+"/p2p/"+first.PeerID().String(); got != want {
		t.Fatalf("advertised peer address = %q, want %q", got, want)
	}
	secondAddress := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", secondPort)
	seed := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", firstPort, first.PeerID())
	second, err := newPeerNetwork(ctx, secondAddress, []string{seed}, nil, t.TempDir()+"/second.peer.key", secondStore)
	if err != nil {
		t.Fatalf("start second peer: %v", err)
	}
	defer second.Close()

	waitFor(t, 10*time.Second, func() bool {
		got, err := secondStore.Lookup("initial")
		return err == nil && got.Token == "from-initial-sync"
	})
	waitFor(t, 10*time.Second, func() bool {
		return len(first.ConnectedPeers()) > 0
	})

	record := userTokenRecord{
		FrontendTokenKey: "replicated", Token: "from-first", Information: "test",
		UpdatedAt: time.Now().UTC(), Origin: first.PeerID().String(),
	}
	if _, err := firstStore.ApplyIfNewer(record); err != nil {
		t.Fatalf("save first record: %v", err)
	}
	if err := first.Replicate(ctx, record); err != nil {
		t.Fatalf("replicate newly saved record: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		got, err := secondStore.Lookup("replicated")
		return err == nil && got.Token == "from-first"
	})
	comparison, err := CompareTokenValues(secondStore, "initial", "replicated")
	if err != nil {
		t.Fatalf("compare peer-replicated local records: %v", err)
	}
	if got := ConcatenateTokenValues(comparison); got != "from-initial-syncfrom-first" {
		t.Fatalf("comparison on second peer = %q", got)
	}
	if _, err := CompareTokenValues(secondStore, "replicated", "missing"); !errors.Is(err, leveldb.ErrNotFound) {
		t.Fatalf("compare missing local record error = %v, want LevelDB not-found", err)
	}
}

func TestParseAnnounceAddressesRequiresAddressWithoutPeerID(t *testing.T) {
	addresses, err := parseAnnounceAddresses([]string{" /ip4/192.0.2.10/tcp/4002 "})
	if err != nil {
		t.Fatalf("parse valid advertised address: %v", err)
	}
	if len(addresses) != 1 || addresses[0].String() != "/ip4/192.0.2.10/tcp/4002" {
		t.Fatalf("parsed announced addresses = %v", addresses)
	}
	identity, err := loadOrCreateIdentity(t.TempDir() + "/test.peer.key")
	if err != nil {
		t.Fatalf("create test identity: %v", err)
	}
	id, err := peer.IDFromPrivateKey(identity)
	if err != nil {
		t.Fatalf("derive test peer ID: %v", err)
	}
	if _, err := parseAnnounceAddresses([]string{"/ip4/192.0.2.10/tcp/4002/p2p/" + id.String()}); err == nil {
		t.Fatal("expected advertised address containing /p2p/peer-id to be rejected")
	}
}

func TestPeerIdentityPersistsAcrossRestart(t *testing.T) {
	path := t.TempDir() + "/peer.key"
	first, err := loadOrCreateIdentity(path)
	if err != nil {
		t.Fatalf("create identity: %v", err)
	}
	second, err := loadOrCreateIdentity(path)
	if err != nil {
		t.Fatalf("reload identity: %v", err)
	}
	firstID, err := peer.IDFromPrivateKey(first)
	if err != nil {
		t.Fatalf("derive first peer ID: %v", err)
	}
	secondID, err := peer.IDFromPrivateKey(second)
	if err != nil {
		t.Fatalf("derive second peer ID: %v", err)
	}
	if firstID != secondID {
		t.Fatalf("peer ID changed after reload: %s != %s", firstID, secondID)
	}
}

func TestHandlerSubmitsAndComparesReplicatedLocalRecords(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	address := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", availableTCPPort(t))
	peers, err := newPeerNetwork(ctx, address, nil, nil, t.TempDir()+"/node.peer.key", store)
	if err != nil {
		t.Fatalf("start peer network: %v", err)
	}
	defer peers.Close()
	handler, err := NewHandler(store, peers)
	if err != nil {
		t.Fatalf("create handler: %v", err)
	}

	for key, token := range map[string]string{"a": "one", "b": "two"} {
		form := url.Values{
			"frontendTokenKey": {key},
			"token":            {token},
			"information":      {"test"},
		}
		request := httptest.NewRequest(http.MethodPost, "/submit", strings.NewReader(form.Encode()))
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusAccepted {
			t.Fatalf("POST /submit status = %d, want 202 without peers: %s", response.Code, response.Body)
		}
		if !strings.Contains(response.Body.String(), "saved locally only") {
			t.Fatalf("no-peer response should report local-only save: %s", response.Body)
		}
	}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/?compare=1&firstKey=a&secondKey=b", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET comparison status = %d, want 200: %s", response.Code, response.Body)
	}
	if !strings.Contains(response.Body.String(), "<pre>onetwo</pre>") {
		t.Fatalf("comparison response missing local values: %s", response.Body)
	}
}

func availableTCPPort(t *testing.T) int {
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
