package linearizability

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/rohithreddy/distkv/client"
	"github.com/rohithreddy/distkv/server"
)

const (
	numClients   = 5
	numKeys      = 8
	workDuration = 3 * time.Second
	checkTimeout = 30 * time.Second
)

func TestLinearizability(t *testing.T) {
	if testing.Short() {
		t.Skip("linearizability test skipped in -short mode")
	}

	addrs := startCluster(t, 3)
	time.Sleep(500 * time.Millisecond)

	var (
		mu  sync.Mutex
		ops []porcupine.Operation
	)
	deadline := time.Now().Add(workDuration)
	var wg sync.WaitGroup

	for cid := 0; cid < numClients; cid++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			c := client.New(addrs)
			ctx := context.Background()
			rng := rand.New(rand.NewSource(int64(clientID + 1)))

			for time.Now().Before(deadline) {
				key := fmt.Sprintf("key%d", rng.Intn(numKeys))
				var in KVInput
				var out KVOutput

				switch rng.Intn(3) {
				case 0:
					val := []byte(fmt.Sprintf("v%d-%d", clientID, rng.Int()))
					in = KVInput{Op: "put", Key: key, Value: val}
					call := time.Now().UnixNano()
					err := retry(ctx, func() error {
						return c.Put(ctx, key, val)
					})
					if err != nil {
						t.Errorf("client %d put: %v", clientID, err)
						return
					}
					returnTime := time.Now().UnixNano()
					mu.Lock()
					ops = append(ops, porcupine.Operation{
						ClientId: clientID,
						Input:    in,
						Call:     call,
						Output:   out,
						Return:   returnTime,
					})
					mu.Unlock()

				case 1:
					in = KVInput{Op: "get", Key: key}
					call := time.Now().UnixNano()
					var val []byte
					var found bool
					err := retry(ctx, func() error {
						var e error
						val, found, e = c.Get(ctx, key)
						return e
					})
					if err != nil {
						t.Errorf("client %d get: %v", clientID, err)
						return
					}
					out = KVOutput{Value: append([]byte(nil), val...), Found: found}
					returnTime := time.Now().UnixNano()
					mu.Lock()
					ops = append(ops, porcupine.Operation{
						ClientId: clientID,
						Input:    in,
						Call:     call,
						Output:   out,
						Return:   returnTime,
					})
					mu.Unlock()

				default:
					in = KVInput{Op: "delete", Key: key}
					call := time.Now().UnixNano()
					err := retry(ctx, func() error {
						return c.Delete(ctx, key)
					})
					if err != nil {
						t.Errorf("client %d delete: %v", clientID, err)
						return
					}
					returnTime := time.Now().UnixNano()
					mu.Lock()
					ops = append(ops, porcupine.Operation{
						ClientId: clientID,
						Input:    in,
						Call:     call,
						Output:   out,
						Return:   returnTime,
					})
					mu.Unlock()
				}
			}
		}(cid)
	}

	wg.Wait()
	if t.Failed() {
		t.Fatal("client errors during workload")
	}
	if len(ops) == 0 {
		t.Fatal("no operations recorded")
	}
	t.Logf("checking linearizability of %d operations", len(ops))

	result, info := porcupine.CheckOperationsVerbose(KVModel, ops, checkTimeout)
	if result != porcupine.Ok {
		path := filepath.Join(t.TempDir(), "linearizability.html")
		if err := porcupine.VisualizePath(KVModel, info, path); err != nil {
			t.Fatalf("linearizability check failed (%v); could not write viz: %v", result, err)
		}
		t.Fatalf("history not linearizable (%v); see %s", result, path)
	}
}

func retry(ctx context.Context, fn func() error) error {
	backoff := 10 * time.Millisecond
	for {
		err := fn()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		time.Sleep(backoff)
		if backoff < 200*time.Millisecond {
			backoff *= 2
		}
	}
}

func startCluster(t *testing.T, n int) []string {
	t.Helper()
	ports := freePorts(t, 2*n)
	peers := map[uint64]string{}
	for i := 0; i < n; i++ {
		peers[uint64(i+1)] = fmt.Sprintf("127.0.0.1:%d", ports[n+i])
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	var servers []*server.Server
	var kvAddrs []string
	base := t.TempDir()
	for i := 0; i < n; i++ {
		kvAddr := fmt.Sprintf("127.0.0.1:%d", ports[i])
		srv, err := server.New(server.Options{
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
	return kvAddrs
}

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
