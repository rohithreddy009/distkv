// distkv runs a single DistKV node.
//
// Example 3-node cluster:
//
//	distkv -id 1 -dir data/n1 -kv :7001 -raft :8001 -peers 1=localhost:8001,2=localhost:8002,3=localhost:8003
//	distkv -id 2 -dir data/n2 -kv :7002 -raft :8002 -peers 1=localhost:8001,2=localhost:8002,3=localhost:8003
//	distkv -id 3 -dir data/n3 -kv :7003 -raft :8003 -peers 1=localhost:8001,2=localhost:8002,3=localhost:8003
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/rohithreddy/distkv/server"
)

func main() {
	id := flag.Uint64("id", 0, "node ID (must be unique, > 0)")
	dir := flag.String("dir", "", "data directory")
	kvAddr := flag.String("kv", "", "client-facing gRPC listen address, e.g. :7001")
	raftAddr := flag.String("raft", "", "raft gRPC listen address, e.g. :8001")
	peersFlag := flag.String("peers", "", "comma-separated id=host:port raft addresses, including self")
	verbose := flag.Bool("v", false, "debug logging")
	flag.Parse()

	if *id == 0 || *dir == "" || *kvAddr == "" || *raftAddr == "" || *peersFlag == "" {
		flag.Usage()
		os.Exit(2)
	}

	peers := map[uint64]string{}
	for _, part := range strings.Split(*peersFlag, ",") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			fmt.Fprintf(os.Stderr, "bad peer spec %q\n", part)
			os.Exit(2)
		}
		pid, err := strconv.ParseUint(kv[0], 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad peer id %q\n", kv[0])
			os.Exit(2)
		}
		peers[pid] = kv[1]
	}

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	srv, err := server.New(server.Options{
		ID:         *id,
		Dir:        *dir,
		ListenKV:   *kvAddr,
		ListenRaft: *raftAddr,
		Peers:      peers,
		Logger:     logger,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "start: %v\n", err)
		os.Exit(1)
	}
	if err := srv.Serve(); err != nil {
		fmt.Fprintf(os.Stderr, "serve: %v\n", err)
		os.Exit(1)
	}
}
