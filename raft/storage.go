package raft

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/rohithreddy/distkv/proto/raftpb"
	"github.com/rohithreddy/distkv/storage"
)

// diskStorage persists Raft hard state (term, votedFor), the log, and
// snapshots. The log lives in an append-only WAL; on snapshot the WAL is
// rewritten (atomically via rename) to drop compacted entries.
//
// WAL record types:
//   1: hard state   term(8) | votedFor(8)
//   2: log append   term(8) | index(8) | data
//   3: truncate     from-index(8)  (drop entries >= from)
type diskStorage struct {
	dir     string
	wal     *storage.WAL
	walPath string

	term     uint64
	votedFor uint64 // 0 = none
	dirty    bool   // appended entries not yet fsynced (group commit)

	// log[0] corresponds to index snapIndex+1
	log       []*raftpb.Entry
	snapIndex uint64
	snapTerm  uint64
	snapshot  []byte
}

const (
	recHardState byte = 1
	recAppend    byte = 2
	recTruncate  byte = 3
)

func openDiskStorage(dir string) (*diskStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &diskStorage{dir: dir, walPath: filepath.Join(dir, "raft.wal")}

	// Load snapshot first: log indices in the WAL are relative to it.
	meta, err := os.ReadFile(filepath.Join(dir, "snapshot.meta"))
	if err == nil && len(meta) == 16 {
		s.snapIndex = binary.LittleEndian.Uint64(meta[0:8])
		s.snapTerm = binary.LittleEndian.Uint64(meta[8:16])
		s.snapshot, err = os.ReadFile(filepath.Join(dir, "snapshot.bin"))
		if err != nil {
			return nil, fmt.Errorf("snapshot.meta exists but snapshot.bin unreadable: %w", err)
		}
	}

	err = storage.ReplayWAL(s.walPath, func(p []byte) error {
		if len(p) < 1 {
			return fmt.Errorf("empty raft wal record")
		}
		switch p[0] {
		case recHardState:
			s.term = binary.LittleEndian.Uint64(p[1:9])
			s.votedFor = binary.LittleEndian.Uint64(p[9:17])
		case recAppend:
			e := &raftpb.Entry{
				Term:  binary.LittleEndian.Uint64(p[1:9]),
				Index: binary.LittleEndian.Uint64(p[9:17]),
				Data:  append([]byte(nil), p[17:]...),
			}
			// Entries at or below the snapshot were compacted already.
			if e.Index > s.snapIndex {
				s.log = append(s.log, e)
			}
		case recTruncate:
			from := binary.LittleEndian.Uint64(p[1:9])
			for len(s.log) > 0 && s.log[len(s.log)-1].Index >= from {
				s.log = s.log[:len(s.log)-1]
			}
		default:
			return fmt.Errorf("unknown raft wal record type %d", p[0])
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	s.wal, err = storage.OpenWAL(s.walPath)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *diskStorage) setHardState(term, votedFor uint64) error {
	s.term, s.votedFor = term, votedFor
	rec := make([]byte, 17)
	rec[0] = recHardState
	binary.LittleEndian.PutUint64(rec[1:9], term)
	binary.LittleEndian.PutUint64(rec[9:17], votedFor)
	if err := s.wal.Append(rec); err != nil {
		return err
	}
	return s.wal.Sync()
}

// appendEntries appends entries without fsync (group commit); call sync()
// before acting on their durability. Truncates any conflicting suffix first.
func (s *diskStorage) appendEntries(entries []*raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	first := entries[0].Index
	// Truncate in-memory + record truncation if we overwrite.
	if n := len(s.log); n > 0 && s.log[n-1].Index >= first {
		rec := make([]byte, 9)
		rec[0] = recTruncate
		binary.LittleEndian.PutUint64(rec[1:9], first)
		if err := s.wal.Append(rec); err != nil {
			return err
		}
		for len(s.log) > 0 && s.log[len(s.log)-1].Index >= first {
			s.log = s.log[:len(s.log)-1]
		}
	}
	for _, e := range entries {
		rec := make([]byte, 17+len(e.Data))
		rec[0] = recAppend
		binary.LittleEndian.PutUint64(rec[1:9], e.Term)
		binary.LittleEndian.PutUint64(rec[9:17], e.Index)
		copy(rec[17:], e.Data)
		if err := s.wal.Append(rec); err != nil {
			return err
		}
		s.log = append(s.log, e)
	}
	s.dirty = true
	return nil
}

// sync fsyncs pending appends. Batches all appends since the last sync
// into one fsync (group commit).
func (s *diskStorage) sync() error {
	if !s.dirty {
		return nil
	}
	if err := s.wal.Sync(); err != nil {
		return err
	}
	s.dirty = false
	return nil
}

// saveSnapshot persists the snapshot and compacts the log through index.
func (s *diskStorage) saveSnapshot(index, term uint64, data []byte) error {
	if index <= s.snapIndex {
		return nil
	}
	// 1. Write snapshot data + meta atomically.
	binPath := filepath.Join(s.dir, "snapshot.bin")
	if err := atomicWrite(binPath, data); err != nil {
		return err
	}
	meta := make([]byte, 16)
	binary.LittleEndian.PutUint64(meta[0:8], index)
	binary.LittleEndian.PutUint64(meta[8:16], term)
	if err := atomicWrite(filepath.Join(s.dir, "snapshot.meta"), meta); err != nil {
		return err
	}

	// 2. Compact the in-memory log.
	var kept []*raftpb.Entry
	for _, e := range s.log {
		if e.Index > index {
			kept = append(kept, e)
		}
	}
	s.log = kept
	s.snapIndex, s.snapTerm, s.snapshot = index, term, data

	// 3. Rewrite the WAL without compacted entries (atomic rename).
	tmp := s.walPath + ".tmp"
	os.Remove(tmp)
	nw, err := storage.OpenWAL(tmp)
	if err != nil {
		return err
	}
	rec := make([]byte, 17)
	rec[0] = recHardState
	binary.LittleEndian.PutUint64(rec[1:9], s.term)
	binary.LittleEndian.PutUint64(rec[9:17], s.votedFor)
	if err := nw.Append(rec); err != nil {
		return err
	}
	for _, e := range s.log {
		r := make([]byte, 17+len(e.Data))
		r[0] = recAppend
		binary.LittleEndian.PutUint64(r[1:9], e.Term)
		binary.LittleEndian.PutUint64(r[9:17], e.Index)
		copy(r[17:], e.Data)
		if err := nw.Append(r); err != nil {
			return err
		}
	}
	if err := nw.Close(); err != nil {
		return err
	}
	if err := s.wal.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.walPath); err != nil {
		return err
	}
	s.wal, err = storage.OpenWAL(s.walPath)
	s.dirty = false // rewrite was fully synced by Close
	return err
}

func atomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// --- log accessors (indices are absolute Raft indices) ---

func (s *diskStorage) firstIndex() uint64 { return s.snapIndex + 1 }

func (s *diskStorage) lastIndex() uint64 {
	if n := len(s.log); n > 0 {
		return s.log[n-1].Index
	}
	return s.snapIndex
}

// termAt returns the term of the entry at index, or (0,false) if unknown
// (compacted below snapshot or beyond last).
func (s *diskStorage) termAt(index uint64) (uint64, bool) {
	if index == s.snapIndex {
		return s.snapTerm, true
	}
	if index < s.firstIndex() || index > s.lastIndex() {
		return 0, false
	}
	return s.log[index-s.firstIndex()].Term, true
}

// entriesFrom returns entries in [lo, lastIndex]. Caller must ensure lo >= firstIndex.
func (s *diskStorage) entriesFrom(lo uint64) []*raftpb.Entry {
	if lo < s.firstIndex() || lo > s.lastIndex() {
		return nil
	}
	return s.log[lo-s.firstIndex():]
}

func (s *diskStorage) entryAt(index uint64) *raftpb.Entry {
	return s.log[index-s.firstIndex()]
}

func (s *diskStorage) close() error { return s.wal.Close() }
