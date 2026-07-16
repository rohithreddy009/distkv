package harness

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rohithreddy/distkv/proto/raftpb"
	"github.com/rohithreddy/distkv/raft"
)

// Cluster is an in-process Raft cluster for tests. Each node's applied
// commands are recorded so tests can assert consistency.
type Cluster struct {
	T       testingT
	Net     *SimNetwork
	IDs     []uint64
	BaseDir string

	mu      sync.Mutex
	nodes   map[uint64]*raft.Node
	applyCh map[uint64]chan raft.ApplyMsg
	// applied[id] = ordered list of committed commands the node applied
	applied map[uint64][]appliedEntry
	stopped map[uint64]bool
	closers []func()
}

type appliedEntry struct {
	Index uint64
	Data  []byte
}

type testingT interface {
	Fatal(args ...any)
	Fatalf(format string, args ...any)
	Logf(format string, args ...any)
	Helper()
	TempDir() string
}

// NewCluster starts n in-process Raft nodes connected by a SimNetwork.
func NewCluster(t testingT, n int) *Cluster {
	c := &Cluster{
		T:       t,
		Net:     NewSimNetwork(),
		BaseDir: t.TempDir(),
		nodes:   map[uint64]*raft.Node{},
		applyCh: map[uint64]chan raft.ApplyMsg{},
		applied: map[uint64][]appliedEntry{},
		stopped: map[uint64]bool{},
	}
	for i := 1; i <= n; i++ {
		c.IDs = append(c.IDs, uint64(i))
	}
	for _, id := range c.IDs {
		c.startNode(id, c.IDs)
	}
	return c
}

func (c *Cluster) startNode(id uint64, peers []uint64) {
	dir := filepath.Join(c.BaseDir, fmt.Sprintf("node%d", id))
	applyCh := make(chan raft.ApplyMsg, 256)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	node, err := raft.NewNode(raft.Config{
		ID:     id,
		Peers:  append([]uint64(nil), peers...),
		Dir:    dir,
		Logger: logger,
	}, c.Net.Transport(id), applyCh)
	if err != nil {
		c.T.Fatalf("start node %d: %v", id, err)
	}

	c.mu.Lock()
	c.nodes[id] = node
	c.applyCh[id] = applyCh
	c.stopped[id] = false
	c.mu.Unlock()
	c.Net.Register(id, node)

	// Drain applies into the record.
	go func() {
		for msg := range applyCh {
			c.mu.Lock()
			if msg.CommandValid && len(msg.Command) > 0 { // skip leader no-ops
				c.applied[id] = append(c.applied[id], appliedEntry{Index: msg.CommandIndex, Data: msg.Command})
			}
			c.mu.Unlock()
		}
	}()
}

// StopNode simulates a crash: the node halts and drops off the network.
// Its on-disk state survives for RestartNode.
func (c *Cluster) StopNode(id uint64) {
	c.mu.Lock()
	node := c.nodes[id]
	ch := c.applyCh[id]
	c.stopped[id] = true
	c.mu.Unlock()

	c.Net.Unregister(id)
	node.Stop()
	close(ch)
}

// RestartNode restarts a previously stopped node from its persisted state.
// The applied record is reset (the state machine also restarts).
func (c *Cluster) RestartNode(id uint64) {
	c.mu.Lock()
	c.applied[id] = nil
	c.mu.Unlock()
	c.startNode(id, c.IDs)
}

func (c *Cluster) Node(id uint64) *raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// Leader returns the current leader's ID, waiting up to timeout. Requires
// exactly one leader among reachable live nodes at the moment of the check.
func (c *Cluster) Leader(timeout time.Duration) (uint64, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []uint64
		c.mu.Lock()
		for id, n := range c.nodes {
			if c.stopped[id] || !c.Net.IsConnected(id) {
				continue
			}
			if _, isLeader, _ := n.Status(); isLeader {
				leaders = append(leaders, id)
			}
		}
		c.mu.Unlock()
		if len(leaders) == 1 {
			return leaders[0], nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return 0, fmt.Errorf("no single leader within %v", timeout)
}

// LeaderAmong waits for exactly one leader among the given nodes.
func (c *Cluster) LeaderAmong(ids []uint64, timeout time.Duration) (uint64, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []uint64
		for _, id := range ids {
			c.mu.Lock()
			n, stopped := c.nodes[id], c.stopped[id]
			c.mu.Unlock()
			if stopped {
				continue
			}
			if _, isLeader, _ := n.Status(); isLeader {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			return leaders[0], nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return 0, fmt.Errorf("no single leader among %v within %v", ids, timeout)
}

// ProposeVia submits a command via the leader among ids, retrying until timeout.
func (c *Cluster) ProposeVia(ids []uint64, cmd []byte, timeout time.Duration) (uint64, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		id, err := c.LeaderAmong(ids, deadline.Sub(time.Now()))
		if err != nil {
			return 0, err
		}
		idx, _, err := c.Node(id).Propose(cmd)
		if err == nil {
			return idx, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return 0, fmt.Errorf("propose timed out")
}

// Propose submits a command via the current leader, retrying on leader
// changes until timeout. Returns the log index it was proposed at.
func (c *Cluster) Propose(cmd []byte, timeout time.Duration) (uint64, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		id, err := c.Leader(deadline.Sub(time.Now()))
		if err != nil {
			return 0, err
		}
		idx, _, err := c.Node(id).Propose(cmd)
		if err == nil {
			return idx, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return 0, fmt.Errorf("propose timed out")
}

// WaitApplied waits until at least count entries are applied on node id.
func (c *Cluster) WaitApplied(id uint64, count int, timeout time.Duration) []appliedEntry {
	c.T.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		got := len(c.applied[id])
		entries := append([]appliedEntry(nil), c.applied[id]...)
		c.mu.Unlock()
		if got >= count {
			return entries
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.mu.Lock()
	got := len(c.applied[id])
	c.mu.Unlock()
	c.T.Fatalf("node %d applied %d entries, want >= %d", id, got, count)
	return nil
}

// Applied returns a copy of node id's applied entries.
func (c *Cluster) Applied(id uint64) []appliedEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]appliedEntry(nil), c.applied[id]...)
}

// CheckConsistency asserts that no two nodes applied different commands at
// the same log index (the core Raft state-machine safety property).
func (c *Cluster) CheckConsistency() {
	c.T.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()

	seen := map[uint64]string{} // index -> command
	owner := map[uint64]uint64{}
	for id := range c.nodes {
		for _, e := range c.applied[id] {
			if prev, ok := seen[e.Index]; ok {
				if prev != string(e.Data) {
					c.T.Fatalf("divergence at index %d: node %d applied %q, node %d applied %q",
						e.Index, id, e.Data, owner[e.Index], prev)
				}
			} else {
				seen[e.Index] = string(e.Data)
				owner[e.Index] = id
			}
		}
	}
}

// AddMember starts a new node and adds it to the running cluster.
func (c *Cluster) AddMember(id uint64) {
	c.T.Helper()
	c.mu.Lock()
	for _, existing := range c.IDs {
		if existing == id {
			c.mu.Unlock()
			c.T.Fatalf("node %d already in cluster", id)
		}
	}
	c.IDs = append(c.IDs, id)
	c.mu.Unlock()

	var bootstrap []uint64
	for _, x := range c.IDs {
		if x != id {
			bootstrap = append(bootstrap, x)
		}
	}
	c.startNode(id, bootstrap)
	c.changeMembership(&raftpb.ConfChange{
		Type:   raftpb.ConfChangeType_CONF_CHANGE_ADD_NODE,
		NodeId: id,
	})
	c.waitConf(id, len(c.IDs), 3*time.Second)
}

// RemoveMember removes a node from the cluster and stops it.
func (c *Cluster) RemoveMember(id uint64) {
	c.T.Helper()
	c.changeMembership(&raftpb.ConfChange{
		Type:   raftpb.ConfChangeType_CONF_CHANGE_REMOVE_NODE,
		NodeId: id,
	})
	c.StopNode(id)
	c.mu.Lock()
	var next []uint64
	for _, x := range c.IDs {
		if x != id {
			next = append(next, x)
		}
	}
	c.IDs = next
	c.mu.Unlock()
}

func (c *Cluster) changeMembership(cc *raftpb.ConfChange) {
	c.T.Helper()
	if err := c.proposeConfChange(cc); err != nil {
		c.T.Fatal(err)
	}
	switch cc.Type {
	case raftpb.ConfChangeType_CONF_CHANGE_ADD_NODE:
		c.waitLeaderConf(func(conf raft.ConfState) bool { return conf.InJoint() })
	case raftpb.ConfChangeType_CONF_CHANGE_REMOVE_NODE:
		c.waitLeaderConf(func(conf raft.ConfState) bool { return conf.InJoint() })
	}
	if cc.Type != raftpb.ConfChangeType_CONF_CHANGE_LEAVE_JOINT {
		if err := c.proposeConfChange(&raftpb.ConfChange{
			Type: raftpb.ConfChangeType_CONF_CHANGE_LEAVE_JOINT,
		}); err != nil {
			c.T.Fatal(err)
		}
		c.waitLeaderConf(func(conf raft.ConfState) bool { return !conf.InJoint() })
	}
	time.Sleep(50 * time.Millisecond)
}

func (c *Cluster) waitLeaderConf(ok func(raft.ConfState) bool) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		leader, err := c.Leader(deadline.Sub(time.Now()))
		if err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if ok(c.Node(leader).ConfState()) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.T.Fatal("timed out waiting for config state on leader")
}

func (c *Cluster) proposeConfChange(cc *raftpb.ConfChange) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		leader, err := c.Leader(deadline.Sub(time.Now()))
		if err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if _, _, err := c.Node(leader).ProposeConfChange(cc); err == nil {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("propose conf change timed out: %+v", cc)
}

func (c *Cluster) waitConf(id uint64, voters int, timeout time.Duration) {
	c.T.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conf := c.Node(id).ConfState()
		if len(conf.VoterIDs()) >= voters && !conf.InJoint() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.T.Fatalf("node %d conf not ready (log=%d conf=%v joint=%v) after %v",
		id, c.Node(id).LastLogIndex(), c.Node(id).ConfState().VoterIDs(), c.Node(id).ConfState().InJoint(), timeout)
}

// Shutdown stops all nodes.
func (c *Cluster) Shutdown() {
	c.mu.Lock()
	ids := make([]uint64, 0, len(c.nodes))
	for id := range c.nodes {
		if !c.stopped[id] {
			ids = append(ids, id)
		}
	}
	c.mu.Unlock()
	for _, id := range ids {
		c.StopNode(id)
	}
}
