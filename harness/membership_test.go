package harness

import (
	"testing"
	"time"

	"github.com/rohithreddy/distkv/proto/raftpb"
)

func TestMembershipAddNode(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()
	time.Sleep(300 * time.Millisecond)

	if _, err := c.Propose([]byte("before-add"), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	c.AddMember(4)

	if _, err := c.Propose([]byte("after-add"), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	c.WaitApplied(4, 1, 3*time.Second)
	c.CheckConsistency()
}

func TestMembershipRemoveNode(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()
	time.Sleep(300 * time.Millisecond)
	c.AddMember(4)

	if _, err := c.Propose([]byte("on-four-nodes"), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	c.RemoveMember(4)

	if _, err := c.Propose([]byte("back-to-three"), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	c.CheckConsistency()
}

func TestMembershipConfStatePersisted(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()
	time.Sleep(300 * time.Millisecond)
	c.AddMember(4)

	c.StopNode(4)
	c.RestartNode(4)

	conf := c.Node(4).ConfState()
	if !conf.Contains(4) || len(conf.VoterIDs()) != 4 {
		t.Fatalf("persisted conf = %+v, want 4 voters including 4", conf.VoterIDs())
	}
}

func TestMembershipRejectQuorumLoss(t *testing.T) {
	c := NewCluster(t, 1)
	defer c.Shutdown()
	time.Sleep(200 * time.Millisecond)

	_, _, err := c.Node(1).ProposeConfChange(&raftpb.ConfChange{
		Type:   raftpb.ConfChangeType_CONF_CHANGE_REMOVE_NODE,
		NodeId: 1,
	})
	if err == nil {
		t.Fatal("expected quorum loss error")
	}
}

func TestMembershipJointTransition(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()
	time.Sleep(300 * time.Millisecond)

	leader, err := c.Leader(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Node(leader).ProposeConfChange(&raftpb.ConfChange{
		Type:   raftpb.ConfChangeType_CONF_CHANGE_ADD_NODE,
		NodeId: 4,
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if !c.Node(leader).ConfState().InJoint() {
		t.Fatal("expected joint config after add proposal commits")
	}
}
