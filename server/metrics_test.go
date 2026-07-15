package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rohithreddy/distkv/client"
)

func TestMetricsEndpoint(t *testing.T) {
	ports := freePorts(t, 10)
	peers := map[uint64]string{
		1: fmt.Sprintf("127.0.0.1:%d", ports[3]),
		2: fmt.Sprintf("127.0.0.1:%d", ports[4]),
		3: fmt.Sprintf("127.0.0.1:%d", ports[5]),
	}
	kvAddrs := []string{
		fmt.Sprintf("127.0.0.1:%d", ports[0]),
		fmt.Sprintf("127.0.0.1:%d", ports[1]),
		fmt.Sprintf("127.0.0.1:%d", ports[2]),
	}
	metricsAddr := fmt.Sprintf("127.0.0.1:%d", ports[6])

	base := t.TempDir()
	var servers []*Server
	for i := 0; i < 3; i++ {
		opts := Options{
			ID:         uint64(i + 1),
			Dir:        fmt.Sprintf("%s/n%d", base, i+1),
			ListenKV:   kvAddrs[i],
			ListenRaft: peers[uint64(i+1)],
			Peers:      peers,
		}
		if i == 0 {
			opts.ListenMetrics = metricsAddr
		}
		srv, err := New(opts)
		if err != nil {
			t.Fatal(err)
		}
		go srv.Serve()
		servers = append(servers, srv)
	}
	t.Cleanup(func() {
		for _, s := range servers {
			s.Stop()
		}
	})
	time.Sleep(400 * time.Millisecond)

	c := client.New(kvAddrs)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.Put(ctx, "metrics-key", []byte("v")); err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get("http://" + metricsAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	text := string(body)
	for _, want := range []string{
		"distkv_kv_requests_total",
		"distkv_is_leader",
		"distkv_commit_index",
		"distkv_proposals_total",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q", want)
		}
	}
}
