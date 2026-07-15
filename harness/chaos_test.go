package harness

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// TestRandomizedChaos runs a 5-node cluster through rounds of random
// faults (partitions, node crashes/restarts, flaky network) while
// continuously proposing commands, then heals everything and verifies:
//   - every command proposed successfully is eventually applied everywhere
//   - no two nodes ever applied different commands at the same index
func TestRandomizedChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test skipped in -short mode")
	}
	c := NewCluster(t, 5)
	defer c.Shutdown()

	rng := rand.New(rand.NewSource(1)) // deterministic scenario
	var proposed []string
	seq := 0

	proposeSome := func(n int, timeout time.Duration) {
		for i := 0; i < n; i++ {
			cmd := fmt.Sprintf("chaos-%04d", seq)
			if _, err := c.Propose([]byte(cmd), timeout); err == nil {
				proposed = append(proposed, cmd)
				seq++
			}
		}
	}

	down := map[uint64]bool{}
	for round := 0; round < 8; round++ {
		switch rng.Intn(3) {
		case 0: // random partition into two groups
			perm := rng.Perm(5)
			var a, b []uint64
			for i, p := range perm {
				id := c.IDs[p]
				if down[id] {
					continue
				}
				if i < 2 {
					a = append(a, id)
				} else {
					b = append(b, id)
				}
			}
			c.Net.Partition(a, b)
		case 1: // crash one node (keep majority alive)
			if len(down) < 2 {
				id := c.IDs[rng.Intn(5)]
				if !down[id] {
					c.StopNode(id)
					down[id] = true
				}
			}
		case 2: // flaky network
			c.Net.SetDropRate(0.15)
		}

		proposeSome(5, 2*time.Second)

		// Heal this round's faults; restart one downed node.
		c.Net.Heal()
		c.Net.SetDropRate(0)
		for id := range down {
			c.RestartNode(id)
			delete(down, id)
			break
		}
		proposeSome(5, 3*time.Second)
	}

	// Final heal and full recovery.
	c.Net.Heal()
	c.Net.SetDropRate(0)
	for id := range down {
		c.RestartNode(id)
		delete(down, id)
	}
	proposeSome(3, 5*time.Second)

	if len(proposed) == 0 {
		t.Fatal("no commands were ever proposed successfully")
	}
	t.Logf("%d commands committed through chaos", len(proposed))

	// Every live node must eventually apply every acknowledged command.
	deadline := time.Now().Add(15 * time.Second)
	for _, id := range c.IDs {
	waitNode:
		for {
			have := map[string]bool{}
			for _, e := range c.Applied(id) {
				have[string(e.Data)] = true
			}
			missing := 0
			for _, cmd := range proposed {
				if !have[cmd] {
					missing++
				}
			}
			if missing == 0 {
				break waitNode
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d still missing %d/%d acknowledged commands", id, missing, len(proposed))
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	c.CheckConsistency()
}
