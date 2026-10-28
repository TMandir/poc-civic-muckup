package peer

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	libpeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"sunya-p2p-dht-tokens/internal/compare"
	"sunya-p2p-dht-tokens/internal/database"
)

func TestDHTDiscoveryAndFullDatabaseSync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bootstrapHost, bootstrapDHT, bootstrapAddress := startLocalDHTBootstrap(t, ctx)
	defer bootstrapHost.Close()
	defer bootstrapDHT.Close()
	firstStore := openTestStore(t)
	defer firstStore.Close()
	secondStore := openTestStore(t)
	defer secondStore.Close()

	initial := database.Record{
		FrontendTokenKey: "existing", Token: "present-before-node-two", Information: "snapshot sync",
		UpdatedAt: time.Now().UTC(), Origin: "peer-seed",
	}
	if _, err := firstStore.ApplyIfNewer(initial); err != nil {
		t.Fatalf("save initial record: %v", err)
	}

	bootstrap := []string{bootstrapAddress}
	first := startTestPeer(t, ctx, firstStore, bootstrap, "dht-integration-test")
	defer first.Close()
	second := startTestPeer(t, ctx, secondStore, bootstrap, "dht-integration-test")
	defer second.Close()

	waitFor(t, 75*time.Second, func() bool {
		got, err := secondStore.Lookup("existing")
		return err == nil && got.Token == "present-before-node-two" && len(second.ConnectedPeers()) > 0
	})
	if got := second.ConnectedPeers(); len(got) != 1 || got[0] != first.ID().String() {
		t.Fatalf("application peers = %v; want only discovered peer %s", got, first.ID())
	}

	live := database.Record{
		FrontendTokenKey: "live", Token: "forwarded-over-dht-discovered-link",
		Information: "live sync", UpdatedAt: time.Now().UTC(), Origin: first.ID().String(),
	}
	if _, err := firstStore.ApplyIfNewer(live); err != nil {
		t.Fatalf("save live record: %v", err)
	}
	if err := first.Broadcast(ctx, live); err != nil {
		t.Fatalf("broadcast live record: %v", err)
	}
	waitFor(t, 12*time.Second, func() bool {
		got, err := secondStore.Lookup("live")
		return err == nil && got.Token == live.Token
	})

	result, err := compare.CompareTokenValues(secondStore, "existing", "live")
	if err != nil {
		t.Fatalf("compare synchronized records: %v", err)
	}
	if got := compare.ConcatenateTokenValues(result); got != initial.Token+live.Token {
		t.Fatalf("comparison result = %q", got)
	}
}

func TestParseDHTMode(t *testing.T) {
	for _, test := range []struct {
		input string
		want  dht.ModeOpt
	}{
		{"", dht.ModeAuto},
		{"auto", dht.ModeAuto},
		{"client", dht.ModeClient},
		{"server", dht.ModeServer},
	} {
		got, err := parseDHTMode(test.input)
		if err != nil || got != test.want {
			t.Errorf("parseDHTMode(%q) = %v, %v; want %v", test.input, got, err, test.want)
		}
	}
	if _, err := parseDHTMode("unsupported"); err == nil {
		t.Fatal("expected invalid DHT mode to return error")
	}
}

func startLocalDHTBootstrap(t *testing.T, ctx context.Context) (host.Host, *dht.IpfsDHT, string) {
	t.Helper()
	address := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", freePort(t))
	listenAddress, err := multiaddr.NewMultiaddr(address)
	if err != nil {
		t.Fatalf("parse local bootstrap address: %v", err)
	}
	node, err := libp2p.New(libp2p.ListenAddrs(listenAddress))
	if err != nil {
		t.Fatalf("start DHT bootstrap host: %v", err)
	}
	router, err := dht.New(
		ctx,
		node,
		dht.Mode(dht.ModeServer),
		dht.RoutingTableFilter(func(interface{}, libpeer.ID) bool { return true }),
		dht.QueryFilter(dht.PrivateQueryFilter),
	)
	if err != nil {
		_ = node.Close()
		t.Fatalf("start local DHT server: %v", err)
	}
	if err := router.Bootstrap(ctx); err != nil {
		_ = router.Close()
		_ = node.Close()
		t.Fatalf("bootstrap local DHT server: %v", err)
	}
	info := libpeer.AddrInfo{ID: node.ID(), Addrs: node.Addrs()}
	addresses, err := libpeer.AddrInfoToP2pAddrs(&info)
	if err != nil || len(addresses) == 0 {
		_ = router.Close()
		_ = node.Close()
		t.Fatalf("build local DHT bootstrap multiaddress: %v", err)
	}
	return node, router, addresses[0].String()
}

func startTestPeer(t *testing.T, ctx context.Context, store *database.Store, bootstrap []string, namespace string) *Network {
	t.Helper()
	address := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", freePort(t))
	node, err := New(ctx, Config{
		ListenAddress:        address,
		BootstrapPeers:       bootstrap,
		DiscoveryTag:         namespace,
		IdentityPath:         t.TempDir() + "/peer.key",
		DHTMode:              "server",
		AllowPrivateDHTPeers: true,
	}, store)
	if err != nil {
		t.Fatalf("start DHT peer: %v", err)
	}
	return node
}

func openTestStore(t *testing.T) *database.Store {
	t.Helper()
	store, err := database.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	return store
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
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
