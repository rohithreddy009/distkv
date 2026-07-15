package harness

import (
	"fmt"
	"testing"
	"time"
)

func TestBasicReplication(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	for i := 0; i < 10; i++ {
		if _, err := c.Propose([]byte(fmt.Sprintf("cmd%d", i)), 3*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range c.IDs {
		entries := c.WaitApplied(id, 10, 5*time.Second)
		for i := 0; i < 10; i++ {
			if string(entries[i].Data) != fmt.Sprintf("cmd%d", i) {
				t.Fatalf("node %d entry %d = %q", id, i, entries[i].Data)
			}
		}
	}
	c.CheckConsistency()
}

func TestReplicationWithFollowerDown(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	leader, err := c.Leader(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var follower uint64
	for _, id := range c.IDs {
		if id != leader {
			follower = id
			break
		}
	}
	c.Net.Disconnect(follower)

	// Cluster keeps committing with 2/3 nodes.
	for i := 0; i < 5; i++ {
		if _, err := c.Propose([]byte(fmt.Sprintf("x%d", i)), 3*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	// The disconnected follower catches up after healing.
	c.Net.Reconnect(follower)
	c.WaitApplied(follower, 5, 5*time.Second)
	c.CheckConsistency()
}

func TestNoCommitWithoutQuorum(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	leader, err := c.Leader(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// Cut the leader off from both followers, then propose to it.
	var rest []uint64
	for _, id := range c.IDs {
		if id != leader {
			rest = append(rest, id)
		}
	}
	c.Net.Partition([]uint64{leader}, rest)

	before := c.Node(leader).CommitIndex()
	if _, _, err := c.Node(leader).Propose([]byte("doomed")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	if got := c.Node(leader).CommitIndex(); got != before {
		t.Fatalf("isolated leader advanced commit from %d to %d", before, got)
	}
}

func TestLogDivergenceRepair(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	leader, err := c.Leader(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// Commit a baseline entry everywhere.
	if _, err := c.Propose([]byte("base"), 3*time.Second); err != nil {
		t.Fatal(err)
	}
	for _, id := range c.IDs {
		c.WaitApplied(id, 1, 5*time.Second)
	}

	// Isolate the leader and stuff uncommitted entries into its log.
	var rest []uint64
	for _, id := range c.IDs {
		if id != leader {
			rest = append(rest, id)
		}
	}
	c.Net.Partition([]uint64{leader}, rest)
	for i := 0; i < 3; i++ {
		c.Node(leader).Propose([]byte(fmt.Sprintf("orphan%d", i)))
	}

	// Majority side elects a new leader and commits different entries.
	if _, err := c.LeaderAmong(rest, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := c.ProposeVia(rest, []byte(fmt.Sprintf("winner%d", i)), 3*time.Second); err != nil {
			t.Fatal(err)
		}
	}

	// Heal: old leader must discard orphans and adopt the winner log.
	c.Net.Heal()
	entries := c.WaitApplied(leader, 4, 5*time.Second)
	for i := 0; i < 3; i++ {
		if string(entries[i+1].Data) != fmt.Sprintf("winner%d", i) {
			t.Fatalf("old leader applied %q at pos %d, want winner%d", entries[i+1].Data, i+1, i)
		}
	}
	c.CheckConsistency()
}

func TestCrashRestartDurability(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	for i := 0; i < 20; i++ {
		if _, err := c.Propose([]byte(fmt.Sprintf("d%d", i)), 3*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range c.IDs {
		c.WaitApplied(id, 20, 5*time.Second)
	}

	// Crash all nodes, restart, verify the full log re-applies.
	for _, id := range c.IDs {
		c.StopNode(id)
	}
	for _, id := range c.IDs {
		c.RestartNode(id)
	}
	if _, err := c.Leader(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	for _, id := range c.IDs {
		entries := c.WaitApplied(id, 20, 10*time.Second)
		for i := 0; i < 20; i++ {
			if string(entries[i].Data) != fmt.Sprintf("d%d", i) {
				t.Fatalf("node %d entry %d = %q after restart", id, i, entries[i].Data)
			}
		}
	}
	c.CheckConsistency()
}

func TestConcurrentProposals(t *testing.T) {
	c := NewCluster(t, 5)
	defer c.Shutdown()

	const total = 100
	done := make(chan error, total)
	for i := 0; i < total; i++ {
		go func(i int) {
			_, err := c.Propose([]byte(fmt.Sprintf("c%03d", i)), 10*time.Second)
			done <- err
		}(i)
	}
	for i := 0; i < total; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range c.IDs {
		c.WaitApplied(id, total, 10*time.Second)
	}
	c.CheckConsistency()
}
