package storage

import (
	"math/rand"
	"sync"
)

const maxHeight = 12

// memtable is a concurrent-read, single-writer skiplist keyed by string.
// A nil value slice paired with tombstone=true marks a deletion.
type memtable struct {
	mu    sync.RWMutex
	head  *skipNode
	size  int // approximate bytes
	count int
}

type skipNode struct {
	key       string
	value     []byte
	tombstone bool
	next      []*skipNode
}

func newMemtable() *memtable {
	return &memtable{head: &skipNode{next: make([]*skipNode, maxHeight)}}
}

func randomHeight() int {
	h := 1
	for h < maxHeight && rand.Intn(4) == 0 {
		h++
	}
	return h
}

func (m *memtable) put(key string, value []byte, tombstone bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	update := make([]*skipNode, maxHeight)
	x := m.head
	for i := maxHeight - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
		update[i] = x
	}
	if n := x.next[0]; n != nil && n.key == key {
		m.size += len(value) - len(n.value)
		n.value = value
		n.tombstone = tombstone
		return
	}
	h := randomHeight()
	n := &skipNode{key: key, value: value, tombstone: tombstone, next: make([]*skipNode, h)}
	for i := 0; i < h; i++ {
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}
	m.size += len(key) + len(value) + 32
	m.count++
}

// get returns (value, tombstone, found).
func (m *memtable) get(key string) ([]byte, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	x := m.head
	for i := maxHeight - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
	}
	if n := x.next[0]; n != nil && n.key == key {
		return n.value, n.tombstone, true
	}
	return nil, false, false
}

// entries returns all entries in sorted key order.
func (m *memtable) entries() []kvEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]kvEntry, 0, m.count)
	for n := m.head.next[0]; n != nil; n = n.next[0] {
		out = append(out, kvEntry{key: n.key, value: n.value, tombstone: n.tombstone})
	}
	return out
}

func (m *memtable) approxSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

type kvEntry struct {
	key       string
	value     []byte
	tombstone bool
}
