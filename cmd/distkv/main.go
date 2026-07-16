// distkv runs a single DistKV node.
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
	peersFlag := flag.String("peers", "", "comma-separated id=host:port raft addresses (bootstrap cluster)")
	joinFlag := flag.String("join", "", "comma-separated id=host:port seed peers when joining an existing cluster")
	metricsAddr := flag.String("metrics", "", "Prometheus /metrics HTTP address, e.g. :9001")
	verbose := flag.Bool("v", false, "debug logging")
	flag.Parse()

	if *id == 0 || *dir == "" || *kvAddr == "" || *raftAddr == "" {
		flag.Usage()
		os.Exit(2)
	}
	if *peersFlag == "" && *joinFlag == "" {
		fmt.Fprintln(os.Stderr, "need -peers (bootstrap) or -join (new member)")
		os.Exit(2)
	}

	peers := map[uint64]string{}
	spec := *peersFlag
	if spec == "" {
		spec = *joinFlag
	}
	for _, part := range strings.Split(spec, ",") {
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
	// Joining node: bootstrap config is just self; cluster learns membership from Raft log.
	if *joinFlag != "" && *peersFlag == "" {
		peers = map[uint64]string{*id: *raftAddr}
		for _, part := range strings.Split(*joinFlag, ",") {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				pid, _ := strconv.ParseUint(kv[0], 10, 64)
				peers[pid] = kv[1]
			}
		}
		peers[*id] = *raftAddr
	}

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	if *joinFlag != "" && *peersFlag == "" {
		var seeds []uint64
		for _, part := range strings.Split(*joinFlag, ",") {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) != 2 {
				continue
			}
			pid, err := strconv.ParseUint(kv[0], 10, 64)
			if err != nil {
				continue
			}
			peers[pid] = kv[1]
			if pid != *id {
				seeds = append(seeds, pid)
			}
		}
		peers[*id] = *raftAddr
		srv, err := server.New(server.Options{
			ID:            *id,
			Dir:           *dir,
			ListenKV:      *kvAddr,
			ListenRaft:    *raftAddr,
			ListenMetrics: *metricsAddr,
			Peers:         peers,
			BootstrapIDs:  seeds,
			Logger:        logger,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "start: %v\n", err)
			os.Exit(1)
		}
		if err := srv.Serve(); err != nil {
			fmt.Fprintf(os.Stderr, "serve: %v\n", err)
			os.Exit(1)
		}
		return
	}

	srv, err := server.New(server.Options{
		ID:            *id,
		Dir:           *dir,
		ListenKV:      *kvAddr,
		ListenRaft:    *raftAddr,
		ListenMetrics: *metricsAddr,
		Peers:         peers,
		Logger:        logger,
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
