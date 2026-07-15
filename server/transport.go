package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rohithreddy/distkv/proto/raftpb"
	"github.com/rohithreddy/distkv/raft"
)

const rpcTimeout = 2 * time.Second

// grpcTransport implements raft.Transport over gRPC with lazy, cached
// connections to peers.
type grpcTransport struct {
	mu    sync.Mutex
	addrs map[uint64]string
	conns map[uint64]raftpb.RaftClient
}

func newGRPCTransport(addrs map[uint64]string) *grpcTransport {
	return &grpcTransport{addrs: addrs, conns: map[uint64]raftpb.RaftClient{}}
}

func (t *grpcTransport) client(peer uint64) (raftpb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.conns[peer]; ok {
		return c, nil
	}
	addr, ok := t.addrs[peer]
	if !ok {
		return nil, fmt.Errorf("unknown peer %d", peer)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c := raftpb.NewRaftClient(conn)
	t.conns[peer] = c
	return c, nil
}

func (t *grpcTransport) RequestVote(peer uint64, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	return c.RequestVote(ctx, req)
}

func (t *grpcTransport) AppendEntries(peer uint64, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	return c.AppendEntries(ctx, req)
}

func (t *grpcTransport) InstallSnapshot(peer uint64, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	return c.InstallSnapshot(ctx, req)
}

func (t *grpcTransport) ReadIndex(peer uint64, req *raftpb.ReadIndexRequest) (*raftpb.ReadIndexResponse, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	return c.ReadIndex(ctx, req)
}

func registerRaft(s *grpc.Server, svc raftpb.RaftServer) {
	raftpb.RegisterRaftServer(s, svc)
}

// raftService exposes the local Raft node's RPC handlers over gRPC.
type raftService struct {
	raftpb.UnimplementedRaftServer
	node *raft.Node
}

func (s *raftService) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	return s.node.HandleRequestVote(req), nil
}

func (s *raftService) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	return s.node.HandleAppendEntries(req), nil
}

func (s *raftService) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	return s.node.HandleInstallSnapshot(req), nil
}

func (s *raftService) ReadIndex(ctx context.Context, req *raftpb.ReadIndexRequest) (*raftpb.ReadIndexResponse, error) {
	return s.node.HandleReadIndex(req), nil
}
