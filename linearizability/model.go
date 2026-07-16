package linearizability

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"sort"

	"github.com/anishathalye/porcupine"
)

// KVInput is a single logical operation against the reference model.
type KVInput struct {
	Op    string // "put", "get", "delete"
	Key   string
	Value []byte
}

// KVOutput is the client-visible result of an operation.
type KVOutput struct {
	Value []byte
	Found bool
}

type keyState struct {
	present bool
	val     string
}

// KVModel is the sequential specification DistKV must be linearizable with.
// Operations are partitioned by key so each key is checked independently.
var KVModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		m := make(map[string][]porcupine.Operation)
		for _, op := range history {
			key := op.Input.(KVInput).Key
			m[key] = append(m[key], op)
		}
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, m[k])
		}
		return out
	},
	PartitionEvent: func(history []porcupine.Event) [][]porcupine.Event {
		m := make(map[string][]porcupine.Event)
		match := make(map[int]string)
		for _, ev := range history {
			if ev.Kind == porcupine.CallEvent {
				key := ev.Value.(KVInput).Key
				m[key] = append(m[key], ev)
				match[ev.Id] = key
			} else {
				m[match[ev.Id]] = append(m[match[ev.Id]], ev)
			}
		}
		out := make([][]porcupine.Event, 0, len(m))
		for _, v := range m {
			out = append(out, v)
		}
		return out
	},
	Init: func() any {
		return keyState{}
	},
	Step: func(stateAny, inputAny, outputAny any) (bool, any) {
		st := stateAny.(keyState)
		in := inputAny.(KVInput)
		out := outputAny.(KVOutput)

		switch in.Op {
		case "put":
			return true, keyState{present: true, val: string(in.Value)}
		case "delete":
			return true, keyState{}
		case "get":
			if !st.present {
				return !out.Found, st
			}
			return out.Found && bytes.Equal(out.Value, []byte(st.val)), st
		default:
			return false, st
		}
	},
	Equal: func(a, b any) bool {
		s1 := a.(keyState)
		s2 := b.(keyState)
		return s1.present == s2.present && s1.val == s2.val
	},
	Hash: func(stateAny any) uint64 {
		st := stateAny.(keyState)
		h := fnv.New64a()
		if st.present {
			h.Write([]byte{1})
			h.Write([]byte(st.val))
		}
		return h.Sum64()
	},
	DescribeOperation: func(inputAny, outputAny any) string {
		in := inputAny.(KVInput)
		out := outputAny.(KVOutput)
		switch in.Op {
		case "put":
			return fmt.Sprintf("put(%q, %q)", in.Key, in.Value)
		case "delete":
			return fmt.Sprintf("delete(%q)", in.Key)
		case "get":
			if out.Found {
				return fmt.Sprintf("get(%q) -> %q", in.Key, out.Value)
			}
			return fmt.Sprintf("get(%q) -> not found", in.Key)
		default:
			return "<invalid>"
		}
	},
}
