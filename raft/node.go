package raft

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/rohithreddy/distkv/proto/raftpb"
)

// Transport delivers RPCs to peers. Implementations: gRPC (production) and
// an in-memory simulated network (tests).
type Transport interface {
	RequestVote(peer uint64, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error)
	AppendEntries(peer uint64, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error)
	InstallSnapshot(peer uint64, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error)
	ReadIndex(peer uint64, req *raftpb.ReadIndexRequest) (*raftpb.ReadIndexResponse, error)
}

// ApplyMsg is sent on the apply channel for each committed entry or
// installed snapshot.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex uint64
	CommandTerm  uint64

	ConfChangeValid bool
	ConfChange      *raftpb.ConfChange
	ConfChangeIndex uint64
	ConfChangeTerm  uint64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

type role int

const (
	follower role = iota
	candidate
	leader
)

func (r role) String() string {
	switch r {
	case follower:
		return "follower"
	case candidate:
		return "candidate"
	default:
		return "leader"
	}
}

type Config struct {
	ID    uint64
	Peers []uint64
	Addrs map[uint64]string // optional raft addresses for bootstrap
	Dir   string

	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration

	Logger *slog.Logger
}

func (c *Config) fill() {
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 300 * time.Millisecond
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

type Node struct {
	cfg       Config
	transport Transport
	applyCh   chan ApplyMsg

	mu    sync.Mutex
	store *diskStorage
	conf  ConfState
	addrs map[uint64]string
	role  role

	leaderID    uint64
	commitIndex uint64
	lastApplied uint64

	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64
	lastSentAt map[uint64]time.Time
	inflight   map[uint64]bool

	electionDeadline time.Time
	kick             chan struct{}

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}

	applyCond *sync.Cond
}

var ErrNotLeader = errors.New("not leader")
var ErrStopped = errors.New("raft node stopped")
var ErrReadIndexTimeout = errors.New("readindex: quorum ack timeout")

func NewNode(cfg Config, transport Transport, applyCh chan ApplyMsg) (*Node, error) {
	cfg.fill()
	store, err := openDiskStorage(cfg.Dir, cfg.Peers)
	if err != nil {
		return nil, err
	}
	addrs := map[uint64]string{}
	for k, v := range cfg.Addrs {
		addrs[k] = v
	}
	n := &Node{
		cfg:       cfg,
		transport: transport,
		applyCh:   applyCh,
		store:     store,
		conf:      store.confState(),
		addrs:     addrs,
		role:      follower,
		kick:      make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	n.applyCond = sync.NewCond(&n.mu)
	if store.snapIndex > 0 {
		n.commitIndex = store.snapIndex
		n.lastApplied = store.snapIndex
	}
	n.resetElectionTimerLocked()
	go n.run()
	go n.applier()
	return n, nil
}

func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopCh)
		n.mu.Lock()
		n.applyCond.Broadcast()
		n.mu.Unlock()
		<-n.doneCh
		n.mu.Lock()
		n.store.close()
		n.mu.Unlock()
	})
}

func (n *Node) Status() (uint64, bool, uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.store.term, n.role == leader, n.leaderID
}

func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

func (n *Node) AppliedIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}

func (n *Node) ConfState() ConfState {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.conf.clone()
}

func (n *Node) MemberAddrs() map[uint64]string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make(map[uint64]string, len(n.addrs))
	for k, v := range n.addrs {
		out[k] = v
	}
	return out
}

func (n *Node) Propose(command []byte) (uint64, uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != leader {
		return 0, 0, ErrNotLeader
	}
	return n.appendEntryLocked(&raftpb.Entry{
		Term:  n.store.term,
		Index: n.store.lastIndex() + 1,
		Type:  raftpb.EntryType_ENTRY_NORMAL,
		Data:  command,
	})
}

func (n *Node) ProposeConfChange(cc *raftpb.ConfChange) (uint64, uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != leader {
		return 0, 0, ErrNotLeader
	}
	if cc.Type != raftpb.ConfChangeType_CONF_CHANGE_LEAVE_JOINT && n.conf.InJoint() {
		return 0, 0, ErrConfChangeInProgress
	}
	tmp := n.conf.clone()
	if _, err := tmp.ApplyConfChange(cc); err != nil {
		return 0, 0, err
	}
	return n.appendEntryLocked(&raftpb.Entry{
		Term:       n.store.term,
		Index:      n.store.lastIndex() + 1,
		Type:       raftpb.EntryType_ENTRY_CONF_CHANGE,
		ConfChange: cc,
	})
}

func (n *Node) appendEntryLocked(e *raftpb.Entry) (uint64, uint64, error) {
	if err := n.store.appendEntries([]*raftpb.Entry{e}); err != nil {
		return 0, 0, err
	}
	n.matchIndex[n.cfg.ID] = e.Index
	n.kickRun()
	return e.Index, e.Term, nil
}

func (n *Node) kickRun() {
	select {
	case n.kick <- struct{}{}:
	default:
	}
}

func (n *Node) resetElectionTimerLocked() {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	n.electionDeadline = time.Now().Add(n.cfg.ElectionTimeoutMin + time.Duration(rand.Int63n(int64(span)+1)))
}

func (n *Node) peerIDsLocked() []uint64 { return n.conf.AllPeerIDs() }

func (n *Node) run() {
	defer close(n.doneCh)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.kick:
		case <-ticker.C:
		}

		n.mu.Lock()
		switch n.role {
		case follower, candidate:
			if time.Now().After(n.electionDeadline) {
				n.startElectionLocked()
			}
			n.mu.Unlock()
		case leader:
			n.broadcastAppendLocked()
			n.mu.Unlock()
		}
	}
}

func (n *Node) LastLogIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.store.lastIndex()
}

func (n *Node) startElectionLocked() {
	if !n.conf.Contains(n.cfg.ID) {
		n.resetElectionTimerLocked()
		return
	}
	// Fresh join node (only self in config, empty log): wait for leader contact.
	if len(n.conf.VoterIDs()) == 1 && n.store.lastIndex() == n.store.snapIndex {
		n.resetElectionTimerLocked()
		return
	}
	n.role = candidate
	newTerm := n.store.term + 1
	if err := n.store.setHardState(newTerm, n.cfg.ID); err != nil {
		n.cfg.Logger.Error("persist vote failed", "err", err)
		return
	}
	n.leaderID = 0
	n.resetElectionTimerLocked()

	lastIdx := n.store.lastIndex()
	lastTerm, _ := n.store.termAt(lastIdx)
	req := &raftpb.RequestVoteRequest{
		Term:         newTerm,
		CandidateId:  n.cfg.ID,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}
	peers := n.peerIDsLocked()
	votes := 1
	var voteMu sync.Mutex
	for _, p := range peers {
		if p == n.cfg.ID {
			continue
		}
		go func(peer uint64) {
			resp, err := n.transport.RequestVote(peer, req)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if resp.Term > n.store.term {
				n.stepDownLocked(resp.Term)
				return
			}
			if n.role != candidate || n.store.term != newTerm || !resp.VoteGranted {
				return
			}
			voteMu.Lock()
			votes++
			won := n.conf.HasVoteQuorum(votes)
			voteMu.Unlock()
			if won {
				n.becomeLeaderLocked()
			}
		}(p)
	}
}

func (n *Node) becomeLeaderLocked() {
	if n.role == leader {
		return
	}
	n.role = leader
	n.leaderID = n.cfg.ID
	n.nextIndex = map[uint64]uint64{}
	n.matchIndex = map[uint64]uint64{}
	n.lastSentAt = map[uint64]time.Time{}
	n.inflight = map[uint64]bool{}
	last := n.store.lastIndex()
	for _, p := range n.peerIDsLocked() {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	n.matchIndex[n.cfg.ID] = last
	n.cfg.Logger.Info("became leader", "id", n.cfg.ID, "term", n.store.term)

	noop := &raftpb.Entry{Term: n.store.term, Index: last + 1, Type: raftpb.EntryType_ENTRY_NORMAL}
	if err := n.store.appendEntries([]*raftpb.Entry{noop}); err != nil {
		n.cfg.Logger.Error("append no-op failed", "err", err)
	} else {
		n.matchIndex[n.cfg.ID] = noop.Index
		n.nextIndex[n.cfg.ID] = noop.Index + 1
	}
	n.broadcastAppendLocked()
}

func (n *Node) stepDownLocked(term uint64) {
	if term > n.store.term {
		if err := n.store.setHardState(term, 0); err != nil {
			n.cfg.Logger.Error("persist term failed", "err", err)
		}
	}
	if n.role != follower {
		n.cfg.Logger.Debug("stepping down", "id", n.cfg.ID, "term", term)
	}
	n.role = follower
	n.resetElectionTimerLocked()
}

func (n *Node) broadcastAppendLocked() {
	if err := n.store.sync(); err != nil {
		n.cfg.Logger.Error("log sync failed", "err", err)
		return
	}
	for _, p := range n.peerIDsLocked() {
		if p == n.cfg.ID {
			continue
		}
		n.sendAppendLocked(p)
	}
}

func (n *Node) sendAppendLocked(peer uint64) {
	ni := n.nextIndex[peer]
	hasNew := n.store.lastIndex() >= ni
	if !hasNew && time.Since(n.lastSentAt[peer]) < n.cfg.HeartbeatInterval {
		return
	}
	n.lastSentAt[peer] = time.Now()

	if ni < n.store.firstIndex() {
		if n.inflight[peer] {
			return
		}
		n.inflight[peer] = true
		req := &raftpb.InstallSnapshotRequest{
			Term:              n.store.term,
			LeaderId:          n.cfg.ID,
			LastIncludedIndex: n.store.snapIndex,
			LastIncludedTerm:  n.store.snapTerm,
			Data:              n.store.snapshot,
			ConfState:         encodeConfState(n.conf),
		}
		go func() {
			defer func() {
				n.mu.Lock()
				delete(n.inflight, peer)
				n.mu.Unlock()
			}()
			n.sendSnapshot(peer, req)
		}()
		return
	}

	prevIdx := ni - 1
	prevTerm, ok := n.store.termAt(prevIdx)
	if !ok {
		return
	}
	entries := n.store.entriesFrom(ni)
	entCopy := make([]*raftpb.Entry, len(entries))
	copy(entCopy, entries)
	req := &raftpb.AppendEntriesRequest{
		Term:         n.store.term,
		LeaderId:     n.cfg.ID,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entCopy,
		LeaderCommit: n.commitIndex,
	}
	if n.inflight[peer] {
		return
	}
	n.inflight[peer] = true
	go func() {
		defer func() {
			n.mu.Lock()
			delete(n.inflight, peer)
			n.mu.Unlock()
		}()
		n.sendAppend(peer, req)
	}()
}

func (n *Node) sendAppend(peer uint64, req *raftpb.AppendEntriesRequest) {
	resp, err := n.transport.AppendEntries(peer, req)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.store.term {
		n.stepDownLocked(resp.Term)
		return
	}
	if n.role != leader || n.store.term != req.Term {
		return
	}
	if resp.Success {
		match := req.PrevLogIndex + uint64(len(req.Entries))
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
		}
		if match+1 > n.nextIndex[peer] {
			n.nextIndex[peer] = match + 1
		}
		n.advanceCommitLocked()
		return
	}
	if resp.ConflictTerm != 0 {
		next := resp.ConflictIndex
		for i := n.store.lastIndex(); i >= n.store.firstIndex(); i-- {
			t, ok := n.store.termAt(i)
			if !ok {
				break
			}
			if t == resp.ConflictTerm {
				next = i + 1
				break
			}
			if t < resp.ConflictTerm {
				break
			}
		}
		n.nextIndex[peer] = max(1, next)
	} else if resp.ConflictIndex > 0 {
		n.nextIndex[peer] = resp.ConflictIndex
	} else if n.nextIndex[peer] > 1 {
		n.nextIndex[peer]--
	}
	n.kickRun()
}

func (n *Node) sendSnapshot(peer uint64, req *raftpb.InstallSnapshotRequest) {
	resp, err := n.transport.InstallSnapshot(peer, req)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.store.term {
		n.stepDownLocked(resp.Term)
		return
	}
	if n.role != leader || n.store.term != req.Term {
		return
	}
	if req.LastIncludedIndex > n.matchIndex[peer] {
		n.matchIndex[peer] = req.LastIncludedIndex
	}
	if req.LastIncludedIndex+1 > n.nextIndex[peer] {
		n.nextIndex[peer] = req.LastIncludedIndex + 1
	}
}

func (n *Node) advanceCommitLocked() {
	majority := n.conf.MaxCommitIndex(n.matchIndex)
	if majority <= n.commitIndex {
		return
	}
	if err := n.store.sync(); err != nil {
		n.cfg.Logger.Error("log sync failed", "err", err)
		return
	}
	t, ok := n.store.termAt(majority)
	if !ok || t != n.store.term {
		return
	}
	n.commitIndex = majority
	n.applyCond.Broadcast()
	n.kickRun()
}

func (n *Node) applyConfChangeLocked(cc *raftpb.ConfChange) error {
	next, err := n.conf.ApplyConfChange(cc)
	if err != nil {
		return err
	}
	n.conf = next
	if len(cc.Context) > 0 && cc.Type == raftpb.ConfChangeType_CONF_CHANGE_ADD_NODE {
		n.addrs[cc.NodeId] = string(cc.Context)
	}
	if cc.Type == raftpb.ConfChangeType_CONF_CHANGE_REMOVE_NODE {
		delete(n.addrs, cc.NodeId)
		if cc.NodeId == n.cfg.ID {
			n.role = follower
			n.leaderID = 0
		}
	}
	if n.role == leader && n.nextIndex != nil {
		switch cc.Type {
		case raftpb.ConfChangeType_CONF_CHANGE_ADD_NODE:
			if _, ok := n.nextIndex[cc.NodeId]; !ok {
				n.nextIndex[cc.NodeId] = n.store.firstIndex()
				n.matchIndex[cc.NodeId] = 0
			}
		case raftpb.ConfChangeType_CONF_CHANGE_REMOVE_NODE:
			delete(n.nextIndex, cc.NodeId)
			delete(n.matchIndex, cc.NodeId)
			delete(n.lastSentAt, cc.NodeId)
		}
	}
	if cc.Type == raftpb.ConfChangeType_CONF_CHANGE_LEAVE_JOINT && n.role == leader && n.nextIndex != nil {
		for id := range n.conf.Voters {
			if _, ok := n.nextIndex[id]; !ok {
				last := n.store.lastIndex()
				n.nextIndex[id] = last + 1
				n.matchIndex[id] = 0
			}
		}
	}
	return n.store.setConfState(n.conf)
}

func (n *Node) HandleRequestVote(req *raftpb.RequestVoteRequest) *raftpb.RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term > n.store.term {
		n.stepDownLocked(req.Term)
	}
	resp := &raftpb.RequestVoteResponse{Term: n.store.term}
	if req.Term < n.store.term {
		return resp
	}
	if !n.conf.Contains(n.cfg.ID) {
		return resp
	}
	lastIdx := n.store.lastIndex()
	lastTerm, _ := n.store.termAt(lastIdx)
	upToDate := req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIdx)

	if (n.store.votedFor == 0 || n.store.votedFor == req.CandidateId) && upToDate {
		if err := n.store.setHardState(n.store.term, req.CandidateId); err != nil {
			n.cfg.Logger.Error("persist vote failed", "err", err)
			return resp
		}
		resp.VoteGranted = true
		n.resetElectionTimerLocked()
	}
	return resp
}

func (n *Node) HandleAppendEntries(req *raftpb.AppendEntriesRequest) *raftpb.AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &raftpb.AppendEntriesResponse{Term: n.store.term}
	if req.Term < n.store.term {
		return resp
	}
	if req.Term > n.store.term || n.role != follower {
		n.stepDownLocked(req.Term)
	}
	resp.Term = n.store.term
	n.leaderID = req.LeaderId
	n.resetElectionTimerLocked()

	if req.PrevLogIndex > n.store.lastIndex() {
		resp.ConflictIndex = n.store.lastIndex() + 1
		return resp
	}
	if req.PrevLogIndex >= n.store.firstIndex() || req.PrevLogIndex == n.store.snapIndex {
		t, ok := n.store.termAt(req.PrevLogIndex)
		if ok && t != req.PrevLogTerm {
			resp.ConflictTerm = t
			ci := req.PrevLogIndex
			for ci > n.store.firstIndex() {
				pt, ok := n.store.termAt(ci - 1)
				if !ok || pt != t {
					break
				}
				ci--
			}
			resp.ConflictIndex = ci
			return resp
		}
		if !ok {
			resp.ConflictIndex = n.store.firstIndex()
			return resp
		}
	}

	var toAppend []*raftpb.Entry
	for i, e := range req.Entries {
		if e.Index <= n.store.snapIndex {
			continue
		}
		if e.Index <= n.store.lastIndex() {
			t, _ := n.store.termAt(e.Index)
			if t == e.Term {
				continue
			}
		}
		toAppend = req.Entries[i:]
		break
	}
	if len(toAppend) > 0 {
		if err := n.store.appendEntries(toAppend); err != nil {
			n.cfg.Logger.Error("append entries failed", "err", err)
			return resp
		}
		if err := n.store.sync(); err != nil {
			n.cfg.Logger.Error("log sync failed", "err", err)
			return resp
		}
	}

	if req.LeaderCommit > n.commitIndex {
		lastNew := req.PrevLogIndex + uint64(len(req.Entries))
		n.commitIndex = min(req.LeaderCommit, max(lastNew, n.commitIndex))
		n.applyCond.Broadcast()
	}
	resp.Success = true
	return resp
}

func (n *Node) HandleInstallSnapshot(req *raftpb.InstallSnapshotRequest) *raftpb.InstallSnapshotResponse {
	n.mu.Lock()

	resp := &raftpb.InstallSnapshotResponse{Term: n.store.term}
	if req.Term < n.store.term {
		n.mu.Unlock()
		return resp
	}
	if req.Term > n.store.term || n.role != follower {
		n.stepDownLocked(req.Term)
	}
	resp.Term = n.store.term
	n.leaderID = req.LeaderId
	n.resetElectionTimerLocked()

	if req.LastIncludedIndex <= n.store.snapIndex {
		n.mu.Unlock()
		return resp
	}
	conf := n.conf
	if len(req.ConfState) > 0 {
		if cs, err := decodeConfState(req.ConfState); err == nil {
			conf = cs
		}
	}
	if err := n.store.saveSnapshot(req.LastIncludedIndex, req.LastIncludedTerm, req.Data, conf); err != nil {
		n.cfg.Logger.Error("save snapshot failed", "err", err)
		return resp
	}
	n.conf = conf
	n.commitIndex = req.LastIncludedIndex
	n.lastApplied = req.LastIncludedIndex

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      req.Data,
		SnapshotIndex: req.LastIncludedIndex,
		SnapshotTerm:  req.LastIncludedTerm,
	}
	n.mu.Unlock()
	select {
	case n.applyCh <- msg:
	case <-n.stopCh:
	}
	return resp
}

func (n *Node) Snapshot(index uint64, data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if index > n.lastApplied {
		return errors.New("cannot snapshot beyond applied index")
	}
	term, ok := n.store.termAt(index)
	if !ok {
		return nil
	}
	return n.store.saveSnapshot(index, term, data, n.conf)
}

func (n *Node) ReadSnapshot() ([]byte, uint64, uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.store.snapshot, n.store.snapIndex, n.store.snapTerm
}

func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	n.mu.Lock()
	if n.role != leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	readIdx := n.commitIndex
	term := n.store.term
	leaderID := n.cfg.ID
	peers := n.peerIDsLocked()
	need := majority(len(peers))
	if n.conf.InJoint() {
		jm := majority(len(n.conf.Joint))
		vm := majority(len(n.conf.Voters))
		if jm > need {
			need = jm
		}
		if vm > need {
			need = vm
		}
	}
	n.mu.Unlock()

	acks := 1
	var ackMu sync.Mutex
	done := make(chan struct{})
	var once sync.Once
	signal := func() { once.Do(func() { close(done) }) }

	req := &raftpb.ReadIndexRequest{Term: term, LeaderId: leaderID}
	for _, p := range peers {
		if p == n.cfg.ID {
			continue
		}
		go func(peer uint64) {
			resp, err := n.transport.ReadIndex(peer, req)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if resp.Term > n.store.term {
				n.stepDownLocked(resp.Term)
				return
			}
			if n.role != leader || n.store.term != term {
				return
			}
			if resp.Ack {
				ackMu.Lock()
				acks++
				if acks >= need {
					signal()
				}
				ackMu.Unlock()
			}
		}(p)
	}

	select {
	case <-done:
		n.mu.Lock()
		stillLeader := n.role == leader && n.store.term == term
		n.mu.Unlock()
		if !stillLeader {
			return 0, ErrNotLeader
		}
		return readIdx, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-time.After(2 * time.Second):
		return 0, ErrReadIndexTimeout
	}
}

func (n *Node) HandleReadIndex(req *raftpb.ReadIndexRequest) *raftpb.ReadIndexResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &raftpb.ReadIndexResponse{Term: n.store.term}
	if req.Term < n.store.term {
		return resp
	}
	if req.Term > n.store.term {
		n.stepDownLocked(req.Term)
	}
	if req.Term != n.store.term || n.role == candidate {
		return resp
	}
	n.leaderID = req.LeaderId
	n.resetElectionTimerLocked()
	resp.Ack = true
	return resp
}

func (n *Node) applier() {
	for {
		n.mu.Lock()
		for n.lastApplied >= n.commitIndex {
			select {
			case <-n.stopCh:
				n.mu.Unlock()
				return
			default:
			}
			n.applyCond.Wait()
			select {
			case <-n.stopCh:
				n.mu.Unlock()
				return
			default:
			}
		}
		var msgs []ApplyMsg
		for n.lastApplied < n.commitIndex {
			next := n.lastApplied + 1
			if next < n.store.firstIndex() {
				n.lastApplied = n.store.snapIndex
				continue
			}
			e := n.store.entryAt(next)
			if e.Type == raftpb.EntryType_ENTRY_CONF_CHANGE && e.ConfChange != nil {
				cc := e.ConfChange
				if err := n.applyConfChangeLocked(cc); err != nil {
					n.cfg.Logger.Error("apply conf change failed", "err", err)
				}
				msgs = append(msgs, ApplyMsg{
					ConfChangeValid: true,
					ConfChange:      cc,
					ConfChangeIndex: e.Index,
					ConfChangeTerm:  e.Term,
				})
			} else {
				msgs = append(msgs, ApplyMsg{
					CommandValid: true,
					Command:      e.Data,
					CommandIndex: e.Index,
					CommandTerm:  e.Term,
				})
			}
			n.lastApplied = next
		}
		n.mu.Unlock()
		for _, m := range msgs {
			select {
			case n.applyCh <- m:
			case <-n.stopCh:
				return
			}
		}
	}
}
