package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rohithreddy/distkv/proto/kvpb"
	"github.com/rohithreddy/distkv/raft"
	"github.com/rohithreddy/distkv/storage"
)

// SnapshotThreshold is the number of applied entries between automatic
// Raft snapshots (log compaction).
const SnapshotThreshold = 10000

type Options struct {
	ID           uint64
	Dir          string
	ListenKV     string            // client-facing gRPC address
	ListenRaft   string            // raft-facing gRPC address
	ListenMetrics string           // Prometheus /metrics HTTP address ("" = disabled)
	Peers        map[uint64]string // peer ID -> raft address (including self)
	Logger       *slog.Logger
}

// Server ties together the Raft node, state machine, and gRPC services.
type Server struct {
	kvpb.UnimplementedKVServer

	opts   Options
	node   *raft.Node
	sm     *stateMachine
	logger *slog.Logger

	mu      sync.Mutex
	waiters map[uint64]chan waitResult // log index -> waiter
	applied uint64
	sinceSnap int

	grpcKV   *grpc.Server
	grpcRaft *grpc.Server

	metricsStop chan struct{}
}

type waitResult struct {
	term uint64
	err  error
}

func New(opts Options) (*Server, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	engine, err := storage.OpenEngine(filepath.Join(opts.Dir, "engine"), storage.EngineOptions{
		FlushSize: 4 << 20,
		// Raft's own WAL provides durability for unapplied entries; the
		// engine syncs on flush.
		SyncWAL: false,
	})
	if err != nil {
		return nil, err
	}
	sm := newStateMachine(engine)

	peerIDs := make([]uint64, 0, len(opts.Peers))
	for id := range opts.Peers {
		peerIDs = append(peerIDs, id)
	}

	applyCh := make(chan raft.ApplyMsg, 1024)
	node, err := raft.NewNode(raft.Config{
		ID:     opts.ID,
		Peers:  peerIDs,
		Dir:    filepath.Join(opts.Dir, "raft"),
		Logger: opts.Logger,
	}, newGRPCTransport(opts.Peers), applyCh)
	if err != nil {
		engine.Close()
		return nil, err
	}

	s := &Server{
		opts:    opts,
		node:    node,
		sm:      sm,
		logger:  opts.Logger,
		waiters: map[uint64]chan waitResult{},
	}

	// Restore from the latest snapshot before applying anything.
	if snap, idx, _ := node.ReadSnapshot(); idx > 0 {
		if err := sm.restore(snap); err != nil {
			return nil, fmt.Errorf("restore snapshot: %w", err)
		}
		s.applied = idx
	}

	go s.applyLoop(applyCh)
	if opts.ListenMetrics != "" {
		s.metricsStop = make(chan struct{})
		go s.runMetricsCollector(s.metricsStop)
	}
	return s, nil
}

// applyLoop consumes committed entries, applies them to the state machine,
// and wakes any client waiting on that index.
func (s *Server) applyLoop(applyCh chan raft.ApplyMsg) {
	for msg := range applyCh {
		switch {
		case msg.SnapshotValid:
			if err := s.sm.restore(msg.Snapshot); err != nil {
				s.logger.Error("snapshot restore failed", "err", err)
				continue
			}
			s.mu.Lock()
			s.applied = msg.SnapshotIndex
			s.mu.Unlock()

		case msg.CommandValid:
			var applyErr error
			if len(msg.Command) > 0 {
				cmd, err := decodeCommand(msg.Command)
				applyErr = err
				if err == nil {
					applyErr = s.sm.apply(cmd)
				}
			} // empty command = leader no-op: only advances applied index
			if applyErr != nil {
				s.logger.Error("apply failed", "index", msg.CommandIndex, "err", applyErr)
			}
			s.mu.Lock()
			s.applied = msg.CommandIndex
			s.sinceSnap++
			needSnap := s.sinceSnap >= SnapshotThreshold
			if needSnap {
				s.sinceSnap = 0
			}
			if ch, ok := s.waiters[msg.CommandIndex]; ok {
				ch <- waitResult{term: msg.CommandTerm, err: applyErr}
				delete(s.waiters, msg.CommandIndex)
			}
			s.mu.Unlock()

			if needSnap {
				if data, err := s.sm.snapshot(); err == nil {
					if err := s.node.Snapshot(msg.CommandIndex, data); err != nil {
						s.logger.Error("snapshot failed", "err", err)
					}
				}
			}
		}
	}
}

// propose replicates a command and waits for it to be applied locally.
func (s *Server) propose(ctx context.Context, cmd Command) error {
	data, err := encodeCommand(cmd)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	index, term, err := s.node.Propose(data)
	if err != nil {
		return s.notLeaderErr()
	}

	ch := make(chan waitResult, 1)
	s.mu.Lock()
	s.waiters[index] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.waiters, index)
		s.mu.Unlock()
	}()

	select {
	case res := <-ch:
		if res.term != term {
			// A different leader's entry got committed at our index.
			return s.notLeaderErr()
		}
		if res.err != nil {
			return status.Error(codes.Internal, res.err.Error())
		}
		return nil
	case <-ctx.Done():
		return status.Error(codes.DeadlineExceeded, "commit timed out")
	case <-time.After(5 * time.Second):
		return status.Error(codes.Unavailable, "commit timed out (lost quorum or leadership?)")
	}
}

func (s *Server) notLeaderErr() error {
	_, _, leaderID := s.node.Status()
	return status.Errorf(codes.FailedPrecondition, "not leader; leader=%d", leaderID)
}

// --- KV RPCs ---

func (s *Server) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	start := time.Now()
	proposalsTotal.Inc()
	err := s.propose(ctx, Command{
		Op: "put", Key: req.Key, Value: req.Value,
		ClientID: req.ClientId, Seq: req.Seq,
	})
	observeKV("put", start, err)
	if err != nil {
		return nil, err
	}
	return &kvpb.PutResponse{}, nil
}

func (s *Server) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	start := time.Now()
	proposalsTotal.Inc()
	err := s.propose(ctx, Command{
		Op: "delete", Key: req.Key,
		ClientID: req.ClientId, Seq: req.Seq,
	})
	observeKV("delete", start, err)
	if err != nil {
		return nil, err
	}
	return &kvpb.DeleteResponse{}, nil
}

// Get serves a linearizable read via ReadIndex: quorum confirms leadership,
// then the state machine applies through the read index before lookup.
func (s *Server) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	start := time.Now()
	riStart := time.Now()
	readIdx, err := s.node.ReadIndex(ctx)
	readIndexLatency.Observe(time.Since(riStart).Seconds())
	if err != nil {
		observeKV("get", start, err)
		return nil, s.notLeaderErr()
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		s.mu.Lock()
		applied := s.applied
		s.mu.Unlock()
		if applied >= readIdx {
			break
		}
		if time.Now().After(deadline) {
			err = status.Error(codes.Unavailable, "apply lag on read")
			observeKV("get", start, err)
			return nil, err
		}
		select {
		case <-ctx.Done():
			err = status.Error(codes.DeadlineExceeded, ctx.Err().Error())
			observeKV("get", start, err)
			return nil, err
		case <-time.After(time.Millisecond):
		}
	}
	val, found, err := s.sm.get(req.Key)
	if err != nil {
		observeKV("get", start, err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	observeKV("get", start, nil)
	return &kvpb.GetResponse{Found: found, Value: val}, nil
}

func (s *Server) Status(ctx context.Context, req *kvpb.StatusRequest) (*kvpb.StatusResponse, error) {
	term, isLeader, leaderID := s.node.Status()
	s.mu.Lock()
	applied := s.applied
	s.mu.Unlock()
	return &kvpb.StatusResponse{
		NodeId:       s.opts.ID,
		IsLeader:     isLeader,
		LeaderId:     leaderID,
		Term:         term,
		CommitIndex:  s.node.CommitIndex(),
		AppliedIndex: applied,
	}, nil
}

// Serve starts both gRPC listeners and blocks until one fails.
func (s *Server) Serve() error {
	raftLis, err := net.Listen("tcp", s.opts.ListenRaft)
	if err != nil {
		return err
	}
	kvLis, err := net.Listen("tcp", s.opts.ListenKV)
	if err != nil {
		return err
	}

	s.grpcRaft = grpc.NewServer()
	s.grpcKV = grpc.NewServer()
	raftSvc := &raftService{node: s.node}
	registerRaft(s.grpcRaft, raftSvc)
	kvpb.RegisterKVServer(s.grpcKV, s)

	errCh := make(chan error, 3)
	go func() { errCh <- s.grpcRaft.Serve(raftLis) }()
	go func() { errCh <- s.grpcKV.Serve(kvLis) }()
	if s.opts.ListenMetrics != "" {
		metricsLis, err := net.Listen("tcp", s.opts.ListenMetrics)
		if err != nil {
			return err
		}
		mux := http.NewServeMux()
		mux.Handle("/metrics", metricsHandler())
		go func() {
			s.logger.Info("metrics up", "addr", s.opts.ListenMetrics)
			errCh <- http.Serve(metricsLis, mux)
		}()
	}
	s.logger.Info("distkv node up", "id", s.opts.ID, "kv", s.opts.ListenKV, "raft", s.opts.ListenRaft)
	return <-errCh
}

func (s *Server) Stop() {
	if s.metricsStop != nil {
		close(s.metricsStop)
	}
	if s.grpcKV != nil {
		s.grpcKV.Stop()
	}
	if s.grpcRaft != nil {
		s.grpcRaft.Stop()
	}
	s.node.Stop()
}
