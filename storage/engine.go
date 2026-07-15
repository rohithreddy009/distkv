package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Engine is an LSM-style embedded KV store:
// writes go WAL -> memtable; memtable flushes to immutable SSTables;
// SSTables are merge-compacted when they pile up.
type Engine struct {
	mu        sync.RWMutex
	dir       string
	wal       *WAL
	walPath   string
	mem       *memtable
	tables    []*sstable // newest first
	nextSST   uint64
	flushSize int
	syncWAL   bool
}

type EngineOptions struct {
	// FlushSize is the memtable size in bytes that triggers a flush.
	FlushSize int
	// SyncWAL controls whether each write batch fsyncs the WAL.
	// The Raft layer has its own durable log, so the state-machine engine
	// can safely run with SyncWAL=false and rebuild from Raft on crash.
	SyncWAL bool
}

func DefaultEngineOptions() EngineOptions {
	return EngineOptions{FlushSize: 4 << 20, SyncWAL: true}
}

const maxTablesBeforeCompact = 6

func OpenEngine(dir string, opts EngineOptions) (*Engine, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if opts.FlushSize == 0 {
		opts.FlushSize = 4 << 20
	}
	e := &Engine{
		dir:       dir,
		mem:       newMemtable(),
		flushSize: opts.FlushSize,
		syncWAL:   opts.SyncWAL,
		walPath:   filepath.Join(dir, "engine.wal"),
	}

	// Load existing SSTables, newest (highest seq) first.
	names, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		return nil, err
	}
	sort.Slice(names, func(i, j int) bool { return sstSeq(names[i]) > sstSeq(names[j]) })
	for _, n := range names {
		t, err := openSSTable(n)
		if err != nil {
			return nil, fmt.Errorf("open sstable %s: %w", n, err)
		}
		e.tables = append(e.tables, t)
		if s := sstSeq(n); s >= e.nextSST {
			e.nextSST = s + 1
		}
	}

	// Replay WAL into memtable.
	err = ReplayWAL(e.walPath, func(p []byte) error {
		key, val, tomb, err := decodeWALRecord(p)
		if err != nil {
			return err
		}
		e.mem.put(key, val, tomb)
		return nil
	})
	if err != nil {
		return nil, err
	}

	e.wal, err = OpenWAL(e.walPath)
	if err != nil {
		return nil, err
	}
	return e, nil
}

func sstSeq(path string) uint64 {
	base := strings.TrimSuffix(filepath.Base(path), ".sst")
	n, _ := strconv.ParseUint(base, 10, 64)
	return n
}

func encodeWALRecord(key string, val []byte, tomb bool) []byte {
	b := make([]byte, 0, 5+len(key)+len(val))
	var flags byte
	if tomb {
		flags = 1
	}
	b = append(b, flags)
	var kl [4]byte
	binary.LittleEndian.PutUint32(kl[:], uint32(len(key)))
	b = append(b, kl[:]...)
	b = append(b, key...)
	b = append(b, val...)
	return b
}

func decodeWALRecord(p []byte) (string, []byte, bool, error) {
	if len(p) < 5 {
		return "", nil, false, fmt.Errorf("short wal record")
	}
	tomb := p[0]&1 == 1
	klen := int(binary.LittleEndian.Uint32(p[1:5]))
	if len(p) < 5+klen {
		return "", nil, false, fmt.Errorf("bad wal record")
	}
	key := string(p[5 : 5+klen])
	val := append([]byte(nil), p[5+klen:]...)
	return key, val, tomb, nil
}

func (e *Engine) Put(key string, value []byte) error {
	return e.write(key, value, false)
}

func (e *Engine) Delete(key string) error {
	return e.write(key, nil, true)
}

func (e *Engine) write(key string, value []byte, tomb bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.wal.Append(encodeWALRecord(key, value, tomb)); err != nil {
		return err
	}
	if e.syncWAL {
		if err := e.wal.Sync(); err != nil {
			return err
		}
	}
	e.mem.put(key, value, tomb)

	if e.mem.approxSize() >= e.flushSize {
		return e.flushLocked()
	}
	return nil
}

// Get returns (value, found).
func (e *Engine) Get(key string) ([]byte, bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if v, tomb, ok := e.mem.get(key); ok {
		if tomb {
			return nil, false, nil
		}
		return v, true, nil
	}
	for _, t := range e.tables { // newest first
		v, tomb, ok, err := t.get(key)
		if err != nil {
			return nil, false, err
		}
		if ok {
			if tomb {
				return nil, false, nil
			}
			return v, true, nil
		}
	}
	return nil, false, nil
}

// Flush forces the memtable to an SSTable.
func (e *Engine) Flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.flushLocked()
}

func (e *Engine) flushLocked() error {
	entries := e.mem.entries()
	if len(entries) == 0 {
		return nil
	}
	path := filepath.Join(e.dir, fmt.Sprintf("%012d.sst", e.nextSST))
	if err := writeSSTable(path, entries); err != nil {
		return err
	}
	t, err := openSSTable(path)
	if err != nil {
		return err
	}
	e.nextSST++
	e.tables = append([]*sstable{t}, e.tables...)

	// Reset WAL: memtable contents are now durable in the SSTable.
	if err := e.wal.Close(); err != nil {
		return err
	}
	if err := os.Remove(e.walPath); err != nil {
		return err
	}
	e.wal, err = OpenWAL(e.walPath)
	if err != nil {
		return err
	}
	e.mem = newMemtable()

	if len(e.tables) > maxTablesBeforeCompact {
		return e.compactLocked()
	}
	return nil
}

// compactLocked merges all SSTables into one, dropping tombstones and
// older shadowed versions.
func (e *Engine) compactLocked() error {
	merged := map[string]kvEntry{}
	// Oldest first so newer entries overwrite.
	for i := len(e.tables) - 1; i >= 0; i-- {
		entries, err := e.tables[i].all()
		if err != nil {
			return err
		}
		for _, en := range entries {
			merged[en.key] = en
		}
	}
	keys := make([]string, 0, len(merged))
	for k, en := range merged {
		if !en.tombstone { // full compaction: tombstones can be dropped
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	out := make([]kvEntry, 0, len(keys))
	for _, k := range keys {
		out = append(out, merged[k])
	}

	path := filepath.Join(e.dir, fmt.Sprintf("%012d.sst", e.nextSST))
	if err := writeSSTable(path, out); err != nil {
		return err
	}
	t, err := openSSTable(path)
	if err != nil {
		return err
	}
	e.nextSST++

	old := e.tables
	e.tables = []*sstable{t}
	for _, o := range old {
		o.close()
		os.Remove(o.path)
	}
	return nil
}

// SnapshotEntries returns a consistent full dump of live entries (no
// tombstones), used for Raft snapshots.
func (e *Engine) SnapshotEntries() (map[string][]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	merged := map[string]kvEntry{}
	for i := len(e.tables) - 1; i >= 0; i-- {
		entries, err := e.tables[i].all()
		if err != nil {
			return nil, err
		}
		for _, en := range entries {
			merged[en.key] = en
		}
	}
	for _, en := range e.mem.entries() {
		merged[en.key] = en
	}
	out := make(map[string][]byte, len(merged))
	for k, en := range merged {
		if !en.tombstone {
			out[k] = en.value
		}
	}
	return out, nil
}

// Reset wipes the engine and loads the given entries (Raft snapshot install).
func (e *Engine) Reset(entries map[string][]byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, t := range e.tables {
		t.close()
		os.Remove(t.path)
	}
	e.tables = nil
	e.mem = newMemtable()
	if err := e.wal.Close(); err != nil {
		return err
	}
	if err := os.Remove(e.walPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	var err error
	e.wal, err = OpenWAL(e.walPath)
	if err != nil {
		return err
	}
	for k, v := range entries {
		if err := e.wal.Append(encodeWALRecord(k, v, false)); err != nil {
			return err
		}
		e.mem.put(k, v, false)
	}
	return e.wal.Sync()
}

func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, t := range e.tables {
		t.close()
	}
	return e.wal.Close()
}
