package harness

import (
	"context"
	"testing"
	"time"

	"github.com/rohithreddy/distkv/raft"
)

func TestReadIndexRequiresLeader(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	follower := c.IDs[0]
	if _, isLeader, _ := c.Node(follower).Status(); isLeader {
		follower = c.IDs[1]
	}
	if _, err := c.Node(follower).ReadIndex(context.Background()); err != raft.ErrNotLeader {
		t.Fatalf("follower ReadIndex = %v, want ErrNotLeader", err)
	}
}

func TestReadIndexQuorumAck(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	leader, err := c.Leader(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := c.Node(leader).ReadIndex(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Propose([]byte("readidx-check"), 3*time.Second); err != nil {
		t.Fatal(err)
	}
	idx2, err := c.Node(leader).ReadIndex(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if idx2 < idx {
		t.Fatalf("read index went backwards: %d -> %d", idx, idx2)
	}
}

func TestReadIndexFailsWithoutQuorum(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	leader, err := c.Leader(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// Isolate leader from both followers: quorum ack impossible.
	for _, id := range c.IDs {
		if id != leader {
			c.Net.Disconnect(id)
		}
	}
	_, err = c.Node(leader).ReadIndex(context.Background())
	if err != raft.ErrReadIndexTimeout {
		t.Fatalf("ReadIndex = %v, want timeout", err)
	}
}
