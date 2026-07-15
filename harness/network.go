// Package harness provides an in-process multi-node Raft cluster with a
// simulated network supporting partitions, message drops, and delays, for
// deterministic fault-injection testing.
package harness

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/rohithreddy/distkv/proto/raftpb"
	"github.com/rohithreddy/distkv/raft"
)

var errUnreachable = errors.New("simnet: peer unreachable")

// SimNetwork routes RPCs between in-process Raft nodes and can inject
// faults: full partitions, per-link cuts, random drops, and latency.
type SimNetwork struct {
	mu       sync.Mutex
	nodes    map[uint64]*raft.Node
	discon   map[uint64]bool            // node fully disconnected
	cutLinks map[[2]uint64]bool         // directed link cut
	dropRate float64                    // probability of dropping any message
	maxDelay time.Duration              // random per-message delay in [0, maxDelay)
}

func NewSimNetwork() *SimNetwork {
	return &SimNetwork{
		nodes:    map[uint64]*raft.Node{},
		discon:   map[uint64]bool{},
		cutLinks: map[[2]uint64]bool{},
	}
}

func (s *SimNetwork) Register(id uint64, n *raft.Node) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[id] = n
}

func (s *SimNetwork) Unregister(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.nodes, id)
}

// Disconnect isolates a node from everyone (both directions).
func (s *SimNetwork) Disconnect(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.discon[id] = true
}

func (s *SimNetwork) Reconnect(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.discon[id] = false
}

// Partition splits the cluster into groups; nodes can only talk within
// their own group.
func (s *SimNetwork) Partition(groups ...[]uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cutLinks = map[[2]uint64]bool{}
	group := map[uint64]int{}
	for gi, g := range groups {
		for _, id := range g {
			group[id] = gi
		}
	}
	var all []uint64
	for _, g := range groups {
		all = append(all, g...)
	}
	for _, a := range all {
		for _, b := range all {
			if a != b && group[a] != group[b] {
				s.cutLinks[[2]uint64{a, b}] = true
			}
		}
	}
}

// Heal removes all partitions and link cuts.
func (s *SimNetwork) Heal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cutLinks = map[[2]uint64]bool{}
	for id := range s.discon {
		s.discon[id] = false
	}
}

// SetDropRate makes the network drop messages randomly.
func (s *SimNetwork) SetDropRate(p float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropRate = p
}

// SetMaxDelay adds a random delay in [0, d) to every message.
func (s *SimNetwork) SetMaxDelay(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxDelay = d
}

// route checks reachability from -> to and returns the target node.
func (s *SimNetwork) route(from, to uint64) (*raft.Node, time.Duration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.discon[from] || s.discon[to] || s.cutLinks[[2]uint64{from, to}] {
		return nil, 0, errUnreachable
	}
	if s.dropRate > 0 && rand.Float64() < s.dropRate {
		return nil, 0, errUnreachable
	}
	n, ok := s.nodes[to]
	if !ok {
		return nil, 0, errUnreachable
	}
	var delay time.Duration
	if s.maxDelay > 0 {
		delay = time.Duration(rand.Int63n(int64(s.maxDelay)))
	}
	return n, delay, nil
}

// IsConnected reports whether the node is not fully disconnected.
func (s *SimNetwork) IsConnected(id uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.discon[id]
}

// Transport returns a raft.Transport bound to the given source node.
func (s *SimNetwork) Transport(from uint64) raft.Transport {
	return &simTransport{net: s, from: from}
}

type simTransport struct {
	net  *SimNetwork
	from uint64
}

func (t *simTransport) RequestVote(peer uint64, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	n, delay, err := t.net.route(t.from, peer)
	if err != nil {
		return nil, err
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	resp := n.HandleRequestVote(req)
	// Reply also traverses the network: re-check reachability.
	if _, _, err := t.net.route(peer, t.from); err != nil {
		return nil, err
	}
	return resp, nil
}

func (t *simTransport) AppendEntries(peer uint64, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	n, delay, err := t.net.route(t.from, peer)
	if err != nil {
		return nil, err
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	resp := n.HandleAppendEntries(req)
	if _, _, err := t.net.route(peer, t.from); err != nil {
		return nil, err
	}
	return resp, nil
}

func (t *simTransport) InstallSnapshot(peer uint64, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	n, delay, err := t.net.route(t.from, peer)
	if err != nil {
		return nil, err
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	resp := n.HandleInstallSnapshot(req)
	if _, _, err := t.net.route(peer, t.from); err != nil {
		return nil, err
	}
	return resp, nil
}

func (t *simTransport) ReadIndex(peer uint64, req *raftpb.ReadIndexRequest) (*raftpb.ReadIndexResponse, error) {
	n, delay, err := t.net.route(t.from, peer)
	if err != nil {
		return nil, err
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	resp := n.HandleReadIndex(req)
	if _, _, err := t.net.route(peer, t.from); err != nil {
		return nil, err
	}
	return resp, nil
}
