package harness

import (
	"fmt"
	"testing"
	"time"
)

func TestSnapshotCompactsLog(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	for i := 0; i < 30; i++ {
		if _, err := c.Propose([]byte(fmt.Sprintf("s%d", i)), 3*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	leader, err := c.Leader(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	entries := c.WaitApplied(leader, 30, 5*time.Second)
	snapAt := entries[19].Index
	if err := c.Node(leader).Snapshot(snapAt, []byte("snap-through-20")); err != nil {
		t.Fatal(err)
	}
	// Cluster still functions after compaction.
	if _, err := c.Propose([]byte("after-snap"), 3*time.Second); err != nil {
		t.Fatal(err)
	}
	c.WaitApplied(leader, 31, 5*time.Second)
	c.CheckConsistency()
}

func TestInstallSnapshotOnLaggingFollower(t *testing.T) {
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

	// Commit entries the lagging follower will never see in log form.
	for i := 0; i < 20; i++ {
		if _, err := c.Propose([]byte(fmt.Sprintf("z%d", i)), 3*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	leader, err = c.Leader(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	entries := c.WaitApplied(leader, 20, 5*time.Second)
	// Compact the leader's log past everything the follower has.
	if err := c.Node(leader).Snapshot(entries[19].Index, []byte("full-state-through-20")); err != nil {
		t.Fatal(err)
	}

	// Reconnect: follower can only catch up via InstallSnapshot.
	c.Net.Reconnect(follower)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		snap, idx, _ := c.Node(follower).ReadSnapshot()
		if idx >= entries[19].Index && string(snap) == "full-state-through-20" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	snap, idx, _ := c.Node(follower).ReadSnapshot()
	if idx < entries[19].Index {
		t.Fatalf("follower snapshot index %d, want >= %d", idx, entries[19].Index)
	}
	if string(snap) != "full-state-through-20" {
		t.Fatalf("follower snapshot data %q", snap)
	}

	// And it should apply new entries on top of the snapshot.
	if _, err := c.Propose([]byte("post"), 3*time.Second); err != nil {
		t.Fatal(err)
	}
	dl := time.Now().Add(5 * time.Second)
	for time.Now().Before(dl) {
		for _, e := range c.Applied(follower) {
			if string(e.Data) == "post" {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("follower never applied post-snapshot entry")
}

func TestRestartFromSnapshot(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	for i := 0; i < 25; i++ {
		if _, err := c.Propose([]byte(fmt.Sprintf("r%d", i)), 3*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	leader, err := c.Leader(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	entries := c.WaitApplied(leader, 25, 5*time.Second)
	if err := c.Node(leader).Snapshot(entries[24].Index, []byte("state-25")); err != nil {
		t.Fatal(err)
	}

	c.StopNode(leader)
	c.RestartNode(leader)

	// The restarted node must come back with the snapshot available and
	// resume from it, not re-apply from index 1.
	snap, idx, _ := c.Node(leader).ReadSnapshot()
	if string(snap) != "state-25" || idx != entries[24].Index {
		t.Fatalf("restarted snapshot = %q @ %d", snap, idx)
	}
	if got := c.Node(leader).AppliedIndex(); got < idx {
		t.Fatalf("applied index %d below snapshot %d", got, idx)
	}

	// New proposals still reach it.
	if _, err := c.Propose([]byte("fresh"), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	dl := time.Now().Add(5 * time.Second)
	for time.Now().Before(dl) {
		for _, e := range c.Applied(leader) {
			if string(e.Data) == "fresh" {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("restarted node never applied new entry")
}
