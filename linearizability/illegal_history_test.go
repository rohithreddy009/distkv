package linearizability

import (
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// TestLinearizabilityRejectsIllegalHistory proves the checker catches bad histories.
// Run with -v to get the HTML visualization path.
func TestLinearizabilityRejectsIllegalHistory(t *testing.T) {
	putCall := int64(1)
	putReturn := int64(2)
	getCall := int64(3)
	getReturn := int64(4)

	ops := []porcupine.Operation{
		{
			ClientId: 0,
			Input:    KVInput{Op: "put", Key: "key0", Value: []byte("hello")},
			Call:     putCall,
			Output:   KVOutput{},
			Return:   putReturn,
		},
		{
			ClientId: 1,
			Input:    KVInput{Op: "get", Key: "key0"},
			Call:     getCall,
			Output:   KVOutput{Value: []byte("wrong"), Found: true}, // should be "hello"
			Return:   getReturn,
		},
	}

	result, info := porcupine.CheckOperationsVerbose(KVModel, ops, 5*time.Second)
	if result == porcupine.Ok {
		t.Fatal("expected illegal history to be rejected")
	}

	path := "illegal-example.html"
	if err := porcupine.VisualizePath(KVModel, info, path); err != nil {
		t.Fatalf("visualize: %v", err)
	}
	t.Logf("checker correctly rejected history (%v); open %s in a browser", result, path)
}
