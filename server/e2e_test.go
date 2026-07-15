package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rohithreddy/distkv/client"
)

// freePorts grabs n distinct free TCP ports.
func freePorts(t *testing.T, n int) []int {
	t.Helper()
	var ports []int
	var listeners []net.Listener
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	for _, l := range listeners {
		l.Close()
	}
	return ports
}

func startCluster(t *testing.T, n int) ([]*Server, []string) {
	t.Helper()
	ports := freePorts(t, 2*n)
	peers := map[uint64]string{}
	for i := 0; i < n; i++ {
		peers[uint64(i+1)] = fmt.Sprintf("127.0.0.1:%d", ports[n+i])
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	var servers []*Server
	var kvAddrs []string
	base := t.TempDir()
	for i := 0; i < n; i++ {
		kvAddr := fmt.Sprintf("127.0.0.1:%d", ports[i])
		srv, err := New(Options{
			ID:         uint64(i + 1),
			Dir:        filepath.Join(base, fmt.Sprintf("n%d", i+1)),
			ListenKV:   kvAddr,
			ListenRaft: peers[uint64(i+1)],
			Peers:      peers,
			Logger:     logger,
		})
		if err != nil {
			t.Fatal(err)
		}
		go srv.Serve()
		servers = append(servers, srv)
		kvAddrs = append(kvAddrs, kvAddr)
	}
	t.Cleanup(func() {
		for _, s := range servers {
			s.Stop()
		}
	})
	return servers, kvAddrs
}

func TestEndToEndPutGetDelete(t *testing.T) {
	_, addrs := startCluster(t, 3)
	c := client.New(addrs)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := c.Put(ctx, "hello", []byte("world")); err != nil {
		t.Fatal(err)
	}
	v, found, err := c.Get(ctx, "hello")
	if err != nil || !found || string(v) != "world" {
		t.Fatalf("get = %q %v %v", v, found, err)
	}
	if err := c.Delete(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := c.Get(ctx, "hello"); found {
		t.Fatal("key still present after delete")
	}
}

func TestEndToEndManyKeys(t *testing.T) {
	_, addrs := startCluster(t, 3)
	c := client.New(addrs)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for i := 0; i < 200; i++ {
		if err := c.Put(ctx, fmt.Sprintf("k%03d", i), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 200; i++ {
		v, found, err := c.Get(ctx, fmt.Sprintf("k%03d", i))
		if err != nil || !found || string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("k%03d = %q %v %v", i, v, found, err)
		}
	}
}

func TestEndToEndLeaderKillFailover(t *testing.T) {
	servers, addrs := startCluster(t, 3)
	c := client.New(addrs)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := c.Put(ctx, "durable", []byte("v1")); err != nil {
		t.Fatal(err)
	}

	// Find and stop the leader.
	var leaderIdx = -1
	for i, addr := range addrs {
		st, err := c.Status(ctx, addr)
		if err == nil && st.IsLeader {
			leaderIdx = i
			break
		}
	}
	if leaderIdx == -1 {
		t.Fatal("no leader found")
	}
	servers[leaderIdx].Stop()

	// Writes and reads must succeed against the surviving majority.
	start := time.Now()
	if err := c.Put(ctx, "after-failover", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	t.Logf("write succeeded %.2fs after leader kill", time.Since(start).Seconds())

	v, found, err := c.Get(ctx, "durable")
	if err != nil || !found || string(v) != "v1" {
		t.Fatalf("durable = %q %v %v", v, found, err)
	}
}
