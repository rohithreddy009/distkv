package raft

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sort"
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
	// Committed log entry (CommandValid true).
	CommandValid bool
	Command      []byte
	CommandIndex uint64
	CommandTerm  uint64

	// Installed snapshot (SnapshotValid true).
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
	ID    uint64   // this node's ID (must be > 0)
	Peers []uint64 // all cluster member IDs, including self
	Dir   string   // persistence directory

	// Timing. Defaults: 150-300ms election timeout, 50ms heartbeat.
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

// Node is a single Raft peer. All state transitions happen inside the run()
// goroutine; external calls communicate with it via mu-protected state reads
// and message passing.
type Node struct {
	cfg       Config
	transport Transport
	applyCh   chan ApplyMsg

	mu    sync.Mutex
	store *diskStorage
	role  role

	leaderID    uint64
	commitIndex uint64
	lastApplied uint64

	// Leader volatile state.
	nextIndex    map[uint64]uint64
	matchIndex   map[uint64]uint64
	lastSentAt   map[uint64]time.Time

	electionDeadline time.Time

	// Signals the run loop that something may need doing (replication,
	// commit advancement).
	kick chan struct{}

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
	store, err := openDiskStorage(cfg.Dir)
	if err != nil {
		return nil, err
	}
	n := &Node{
		cfg:       cfg,
		transport: transport,
		applyCh:   applyCh,
		store:     store,
		role:      follower,
		kick:      make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	n.applyCond = sync.NewCond(&n.mu)
	// A restarting node with a snapshot must tell the state machine to
	// restore from it before applying subsequent entries.
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

// Status returns (term, isLeader, leaderID).
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

// Propose appends a command to the leader's log. Returns the entry's index
// and term, or ErrNotLeader.
func (n *Node) Propose(command []byte) (uint64, uint64, error) {
	n.mu.Lock()
	if n.role != leader {
		n.mu.Unlock()
		return 0, 0, ErrNotLeader
	}
	index := n.store.lastIndex() + 1
	term := n.store.term
	entry := &raftpb.Entry{Term: term, Index: index, Data: command}
	if err := n.store.appendEntries([]*raftpb.Entry{entry}); err != nil {
		n.mu.Unlock()
		return 0, 0, err
	}
	n.matchIndex[n.cfg.ID] = index
	n.mu.Unlock()
	n.kickRun()
	return index, term, nil
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

// run is the main event loop: election timeouts for followers/candidates,
// heartbeat/replication ticks for leaders.
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

// --- elections ---

func (n *Node) startElectionLocked() {
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
	n.cfg.Logger.Debug("starting election", "id", n.cfg.ID, "term", newTerm)

	votes := 1 // self
	var voteMu sync.Mutex
	for _, p := range n.cfg.Peers {
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
			won := votes*2 > len(n.cfg.Peers)
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
	last := n.store.lastIndex()
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	n.matchIndex[n.cfg.ID] = last
	n.cfg.Logger.Info("became leader", "id", n.cfg.ID, "term", n.store.term)

	// Append a no-op entry for the new term. Without it, entries from prior
	// terms can never be committed (§5.4.2) and a freshly elected leader
	// over an old log would stall forever.
	noop := &raftpb.Entry{Term: n.store.term, Index: last + 1}
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

// --- replication (leader side) ---

func (n *Node) broadcastAppendLocked() {
	// Group commit: one fsync covers every entry appended since the last
	// sync (possibly many concurrent proposals).
	if err := n.store.sync(); err != nil {
		n.cfg.Logger.Error("log sync failed", "err", err)
		return
	}
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			continue
		}
		n.sendAppendLocked(p)
	}
}

// sendAppendLocked fires one replication RPC at peer if due: either there
// are new entries to send or the heartbeat interval elapsed.
func (n *Node) sendAppendLocked(peer uint64) {
	ni := n.nextIndex[peer]
	hasNew := n.store.lastIndex() >= ni
	if !hasNew && time.Since(n.lastSentAt[peer]) < n.cfg.HeartbeatInterval {
		return
	}
	n.lastSentAt[peer] = time.Now()

	if ni < n.store.firstIndex() {
		// Peer is behind the snapshot: send it instead of log entries.
		req := &raftpb.InstallSnapshotRequest{
			Term:              n.store.term,
			LeaderId:          n.cfg.ID,
			LastIncludedIndex: n.store.snapIndex,
			LastIncludedTerm:  n.store.snapTerm,
			Data:              n.store.snapshot,
		}
		go n.sendSnapshot(peer, req)
		return
	}

	prevIdx := ni - 1
	prevTerm, ok := n.store.termAt(prevIdx)
	if !ok {
		return // race with concurrent compaction; retried next tick
	}
	entries := n.store.entriesFrom(ni)
	// Copy: the slice aliases store.log which may be compacted concurrently.
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
	go n.sendAppend(peer, req)
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
	// Rejected: back up nextIndex using conflict hints.
	if resp.ConflictTerm != 0 {
		// Find our last entry with ConflictTerm; if found, next = that+1.
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
	n.sendAppendLocked(peer)
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

// advanceCommitLocked commits the highest index replicated on a majority,
// but only for entries from the current term (Raft §5.4.2).
func (n *Node) advanceCommitLocked() {
	matches := make([]uint64, 0, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		matches = append(matches, n.matchIndex[p])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	majority := matches[len(n.cfg.Peers)/2]

	if majority <= n.commitIndex {
		return
	}
	// The leader's own vote toward the majority is only valid once its
	// log is durable.
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
	// Let followers learn the new commit index promptly.
	n.broadcastAppendLocked()
}

// --- RPC handlers (called by transport) ---

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
	// Election restriction: candidate's log must be at least as up-to-date.
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

	// Consistency check on the previous entry.
	if req.PrevLogIndex > n.store.lastIndex() {
		resp.ConflictIndex = n.store.lastIndex() + 1
		return resp
	}
	if req.PrevLogIndex >= n.store.firstIndex() || req.PrevLogIndex == n.store.snapIndex {
		t, ok := n.store.termAt(req.PrevLogIndex)
		if ok && t != req.PrevLogTerm {
			// Report the first index of the conflicting term.
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
	} else {
		// prev is below our snapshot: entries there are committed and match.
	}

	// Append entries not already present; truncate on conflict.
	var toAppend []*raftpb.Entry
	for i, e := range req.Entries {
		if e.Index <= n.store.snapIndex {
			continue // already covered by snapshot
		}
		if e.Index <= n.store.lastIndex() {
			t, _ := n.store.termAt(e.Index)
			if t == e.Term {
				continue // already have it
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
		// Must be durable before acknowledging to the leader.
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
	defer n.mu.Unlock()

	resp := &raftpb.InstallSnapshotResponse{Term: n.store.term}
	if req.Term < n.store.term {
		return resp
	}
	if req.Term > n.store.term || n.role != follower {
		n.stepDownLocked(req.Term)
	}
	resp.Term = n.store.term
	n.leaderID = req.LeaderId
	n.resetElectionTimerLocked()

	if req.LastIncludedIndex <= n.commitIndex {
		return resp // stale snapshot
	}
	if err := n.store.saveSnapshot(req.LastIncludedIndex, req.LastIncludedTerm, req.Data); err != nil {
		n.cfg.Logger.Error("save snapshot failed", "err", err)
		return resp
	}
	n.commitIndex = req.LastIncludedIndex
	n.lastApplied = req.LastIncludedIndex

	// Deliver the snapshot to the state machine (outside the lock).
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
	n.mu.Lock()
	return resp
}

// --- snapshotting (state machine side) ---

// Snapshot tells Raft the state machine has serialized its state through
// index; the log can be compacted.
func (n *Node) Snapshot(index uint64, data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if index > n.lastApplied {
		return errors.New("cannot snapshot beyond applied index")
	}
	term, ok := n.store.termAt(index)
	if !ok {
		return nil // already compacted
	}
	return n.store.saveSnapshot(index, term, data)
}

// ReadSnapshot returns the latest persisted snapshot (for restart restore).
func (n *Node) ReadSnapshot() ([]byte, uint64, uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.store.snapshot, n.store.snapIndex, n.store.snapTerm
}

// ReadIndex confirms leadership with a quorum heartbeat, then returns the
// commit index the state machine must apply through before a linearizable
// read. Does not write to the Raft log.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	n.mu.Lock()
	if n.role != leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	readIdx := n.commitIndex
	term := n.store.term
	leaderID := n.cfg.ID
	peers := append([]uint64(nil), n.cfg.Peers...)
	n.mu.Unlock()

	quorum := len(peers)/2 + 1
	acks := 1 // leader counts toward quorum
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
				if acks >= quorum {
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

// HandleReadIndex acknowledges a valid leader heartbeat for linearizable reads.
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

// --- applier ---

// applier delivers committed entries to applyCh in order.
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
		// Collect a batch of committed-but-unapplied entries.
		var msgs []ApplyMsg
		for n.lastApplied < n.commitIndex {
			next := n.lastApplied + 1
			if next < n.store.firstIndex() {
				// Compacted under us (snapshot install); skip forward.
				n.lastApplied = n.store.snapIndex
				continue
			}
			// Note: leader no-op entries (empty Data) are delivered too so
			// consumers can track the applied index; they must ignore them.
			e := n.store.entryAt(next)
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      e.Data,
				CommandIndex: e.Index,
				CommandTerm:  e.Term,
			})
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