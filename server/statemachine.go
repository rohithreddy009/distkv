package server

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/rohithreddy/distkv/storage"
)

// Command is the operation replicated through the Raft log.
type Command struct {
	Op       string // "put" | "delete"
	Key      string
	Value    []byte
	ClientID string
	Seq      uint64
}

func encodeCommand(c Command) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeCommand(b []byte) (Command, error) {
	var c Command
	err := gob.NewDecoder(bytes.NewReader(b)).Decode(&c)
	return c, err
}

// stateMachine applies committed commands to the storage engine with
// exactly-once semantics per client (dedup by ClientID+Seq).
type stateMachine struct {
	mu      sync.Mutex
	engine  *storage.Engine
	lastSeq map[string]uint64 // clientID -> highest applied seq
}

func newStateMachine(engine *storage.Engine) *stateMachine {
	return &stateMachine{engine: engine, lastSeq: map[string]uint64{}}
}

// apply executes a command unless it is a duplicate retry.
func (sm *stateMachine) apply(c Command) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if c.ClientID != "" && c.Seq > 0 && sm.lastSeq[c.ClientID] >= c.Seq {
		return nil // duplicate
	}
	var err error
	switch c.Op {
	case "put":
		err = sm.engine.Put(c.Key, c.Value)
	case "delete":
		err = sm.engine.Delete(c.Key)
	default:
		err = fmt.Errorf("unknown op %q", c.Op)
	}
	if err != nil {
		return err
	}
	if c.ClientID != "" {
		sm.lastSeq[c.ClientID] = c.Seq
	}
	return nil
}

func (sm *stateMachine) get(key string) ([]byte, bool, error) {
	return sm.engine.Get(key)
}

// snapshotState is what gets serialized into a Raft snapshot.
type snapshotState struct {
	Data    map[string][]byte
	LastSeq map[string]uint64
}

func (sm *stateMachine) snapshot() ([]byte, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	data, err := sm.engine.SnapshotEntries()
	if err != nil {
		return nil, err
	}
	seq := make(map[string]uint64, len(sm.lastSeq))
	for k, v := range sm.lastSeq {
		seq[k] = v
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snapshotState{Data: data, LastSeq: seq}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (sm *stateMachine) restore(snap []byte) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var st snapshotState
	if err := gob.NewDecoder(bytes.NewReader(snap)).Decode(&st); err != nil {
		return err
	}
	if err := sm.engine.Reset(st.Data); err != nil {
		return err
	}
	sm.lastSeq = st.LastSeq
	if sm.lastSeq == nil {
		sm.lastSeq = map[string]uint64{}
	}
	return nil
}
