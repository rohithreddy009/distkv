package raft

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"

	"github.com/rohithreddy/distkv/proto/raftpb"
	"github.com/rohithreddy/distkv/storage"
)

// diskStorage persists Raft hard state (term, votedFor), the log, and
// snapshots. The log lives in an append-only WAL; on snapshot the WAL is
// rewritten (atomically via rename) to drop compacted entries.
//
// WAL record types:
//   1: hard state   term(8) | votedFor(8)
//   2: log append   term(8) | index(8) | proto(Entry)
//   3: truncate     from-index(8)  (drop entries >= from)
//   4: conf state   encoded ConfState
type diskStorage struct {
	dir     string
	wal     *storage.WAL
	walPath string

	term     uint64
	votedFor uint64 // 0 = none
	dirty    bool   // appended entries not yet fsynced (group commit)

	conf ConfState

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
	recConfState byte = 4
)

func openDiskStorage(dir string, bootstrap []uint64) (*diskStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &diskStorage{
		dir:     dir,
		walPath: filepath.Join(dir, "raft.wal"),
		conf:    NewConfState(bootstrap),
	}

	// Load snapshot first: log indices in the WAL are relative to it.
	meta, err := os.ReadFile(filepath.Join(dir, "snapshot.meta"))
	if err == nil && len(meta) >= 16 {
		s.snapIndex = binary.LittleEndian.Uint64(meta[0:8])
		s.snapTerm = binary.LittleEndian.Uint64(meta[8:16])
		s.snapshot, err = os.ReadFile(filepath.Join(dir, "snapshot.bin"))
		if err != nil {
			return nil, fmt.Errorf("snapshot.meta exists but snapshot.bin unreadable: %w", err)
		}
		if confData, err := os.ReadFile(filepath.Join(dir, "snapshot.conf")); err == nil {
			if cs, err := decodeConfState(confData); err == nil && len(cs.Voters) > 0 {
				s.conf = cs
			}
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
			e := decodeEntry(p[1:])
			if e.Index > s.snapIndex {
				s.log = append(s.log, e)
			}
		case recTruncate:
			from := binary.LittleEndian.Uint64(p[1:9])
			for len(s.log) > 0 && s.log[len(s.log)-1].Index >= from {
				s.log = s.log[:len(s.log)-1]
			}
		case recConfState:
			cs, err := decodeConfState(p[1:])
			if err != nil {
				return err
			}
			if len(cs.Voters) > 0 {
				s.conf = cs
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

func encodeEntry(e *raftpb.Entry) []byte {
	body, err := proto.Marshal(e)
	if err != nil {
		panic(err)
	}
	rec := make([]byte, 17+len(body))
	rec[0] = recAppend
	binary.LittleEndian.PutUint64(rec[1:9], e.Term)
	binary.LittleEndian.PutUint64(rec[9:17], e.Index)
	copy(rec[17:], body)
	return rec
}

func decodeEntry(p []byte) *raftpb.Entry {
	if len(p) < 16 {
		return &raftpb.Entry{}
	}
	term := binary.LittleEndian.Uint64(p[0:8])
	index := binary.LittleEndian.Uint64(p[8:16])
	body := p[16:]
	e := &raftpb.Entry{}
	if err := proto.Unmarshal(body, e); err != nil {
		return &raftpb.Entry{Term: term, Index: index, Data: append([]byte(nil), body...)}
	}
	e.Term = term
	e.Index = index
	return e
}

func (s *diskStorage) confState() ConfState { return s.conf }

func (s *diskStorage) setConfState(c ConfState) error {
	s.conf = c
	rec := append([]byte{recConfState}, encodeConfState(c)...)
	if err := s.wal.Append(rec); err != nil {
		return err
	}
	return s.wal.Sync()
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
		if err := s.wal.Append(encodeEntry(e)); err != nil {
			return err
		}
		s.log = append(s.log, e)
	}
	s.dirty = true
	return nil
}

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

func (s *diskStorage) saveSnapshot(index, term uint64, data []byte, conf ConfState) error {
	if index <= s.snapIndex {
		return nil
	}
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
	if err := atomicWrite(filepath.Join(s.dir, "snapshot.conf"), encodeConfState(conf)); err != nil {
		return err
	}

	var kept []*raftpb.Entry
	for _, e := range s.log {
		if e.Index > index {
			kept = append(kept, e)
		}
	}
	s.log = kept
	s.snapIndex, s.snapTerm, s.snapshot = index, term, data
	s.conf = conf

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
	if csRec := append([]byte{recConfState}, encodeConfState(conf)...); true {
		if err := nw.Append(csRec); err != nil {
			return err
		}
	}
	for _, e := range s.log {
		if err := nw.Append(encodeEntry(e)); err != nil {
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
	s.dirty = false
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

func (s *diskStorage) firstIndex() uint64 { return s.snapIndex + 1 }

func (s *diskStorage) lastIndex() uint64 {
	if n := len(s.log); n > 0 {
		return s.log[n-1].Index
	}
	return s.snapIndex
}

func (s *diskStorage) termAt(index uint64) (uint64, bool) {
	if index == s.snapIndex {
		return s.snapTerm, true
	}
	if index < s.firstIndex() || index > s.lastIndex() {
		return 0, false
	}
	return s.log[index-s.firstIndex()].Term, true
}

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
