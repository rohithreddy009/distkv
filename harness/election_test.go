package harness

import (
	"testing"
	"time"
)

func TestInitialElection(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	if _, err := c.Leader(3 * time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestLeaderFailover(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	old, err := c.Leader(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c.Net.Disconnect(old)

	newLeader, err := c.Leader(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if newLeader == old {
		t.Fatalf("expected a new leader, still %d", old)
	}
}

func TestNoElectionWithoutQuorum(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	leader, err := c.Leader(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// Isolate two nodes: the remaining one cannot win an election.
	var lone uint64
	for _, id := range c.IDs {
		if id != leader {
			lone = id
			break
		}
	}
	for _, id := range c.IDs {
		if id != lone {
			c.Net.Disconnect(id)
		}
	}
	time.Sleep(1 * time.Second)
	if _, isLeader, _ := c.Node(lone).Status(); isLeader {
		t.Fatal("minority node became leader")
	}
}

func TestLeaderRejoinsAsFollower(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	old, err := c.Leader(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c.Net.Disconnect(old)
	if _, err := c.Leader(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	c.Net.Reconnect(old)
	time.Sleep(500 * time.Millisecond)

	// Exactly one leader must remain.
	if _, err := c.Leader(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, id := range c.IDs {
		if _, isLeader, _ := c.Node(id).Status(); isLeader {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", count)
	}
}

func TestElectionWithFlakyNetwork(t *testing.T) {
	c := NewCluster(t, 5)
	defer c.Shutdown()

	c.Net.SetDropRate(0.2)
	defer c.Net.SetDropRate(0)
	if _, err := c.Leader(10 * time.Second); err != nil {
		t.Fatal(err)
	}
}
