package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

// SSTable file format:
//
//   data block:   repeated records: flags(1) | klen(4) | vlen(4) | key | value
//   index block:  repeated: klen(4) | key | offset(8)   (every indexEvery-th key)
//   footer:       indexOffset(8) | indexCount(4) | crc-of-index(4) | magic(8)
//
// Records are sorted by key. flags bit0 = tombstone.
const (
	sstMagic   = 0x5354414B564B5631 // "STAKVKV1"
	indexEvery = 16
)

type sstIndexEntry struct {
	key    string
	offset int64
}

// sstable is an immutable on-disk sorted table with a sparse in-memory index.
type sstable struct {
	path   string
	f      *os.File
	index  []sstIndexEntry
	minKey string
	maxKey string
}

// writeSSTable persists sorted entries to path. Entries must be sorted by key.
func writeSSTable(path string, entries []kvEntry) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)

	var off int64
	var index []sstIndexEntry
	var scratch [9]byte
	for i, e := range entries {
		if i%indexEvery == 0 {
			index = append(index, sstIndexEntry{key: e.key, offset: off})
		}
		var flags byte
		if e.tombstone {
			flags = 1
		}
		scratch[0] = flags
		binary.LittleEndian.PutUint32(scratch[1:5], uint32(len(e.key)))
		binary.LittleEndian.PutUint32(scratch[5:9], uint32(len(e.value)))
		if _, err := w.Write(scratch[:]); err != nil {
			return err
		}
		if _, err := w.WriteString(e.key); err != nil {
			return err
		}
		if _, err := w.Write(e.value); err != nil {
			return err
		}
		off += int64(9 + len(e.key) + len(e.value))
	}

	// index block
	indexOff := off
	crc := crc32.NewIEEE()
	iw := io.MultiWriter(w, crc)
	for _, ie := range index {
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], uint32(len(ie.key)))
		iw.Write(b[:])
		io.WriteString(iw, ie.key)
		var o [8]byte
		binary.LittleEndian.PutUint64(o[:], uint64(ie.offset))
		iw.Write(o[:])
	}

	var footer [24]byte
	binary.LittleEndian.PutUint64(footer[0:8], uint64(indexOff))
	binary.LittleEndian.PutUint32(footer[8:12], uint32(len(index)))
	binary.LittleEndian.PutUint32(footer[12:16], crc.Sum32())
	binary.LittleEndian.PutUint64(footer[16:24], sstMagic)
	if _, err := w.Write(footer[:]); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return f.Close()
}

func openSSTable(path string) (*sstable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() < 24 {
		f.Close()
		return nil, fmt.Errorf("sstable %s too small", path)
	}
	var footer [24]byte
	if _, err := f.ReadAt(footer[:], st.Size()-24); err != nil {
		f.Close()
		return nil, err
	}
	if binary.LittleEndian.Uint64(footer[16:24]) != sstMagic {
		f.Close()
		return nil, fmt.Errorf("sstable %s: bad magic", path)
	}
	indexOff := int64(binary.LittleEndian.Uint64(footer[0:8]))
	count := int(binary.LittleEndian.Uint32(footer[8:12]))
	wantCRC := binary.LittleEndian.Uint32(footer[12:16])

	indexBytes := make([]byte, st.Size()-24-indexOff)
	if _, err := f.ReadAt(indexBytes, indexOff); err != nil {
		f.Close()
		return nil, err
	}
	if crc32.ChecksumIEEE(indexBytes) != wantCRC {
		f.Close()
		return nil, fmt.Errorf("sstable %s: index checksum mismatch", path)
	}

	index := make([]sstIndexEntry, 0, count)
	p := 0
	for i := 0; i < count; i++ {
		klen := int(binary.LittleEndian.Uint32(indexBytes[p : p+4]))
		p += 4
		key := string(indexBytes[p : p+klen])
		p += klen
		off := int64(binary.LittleEndian.Uint64(indexBytes[p : p+8]))
		p += 8
		index = append(index, sstIndexEntry{key: key, offset: off})
	}

	t := &sstable{path: path, f: f, index: index}
	if len(index) > 0 {
		t.minKey = index[0].key
		// maxKey found by scanning last block; cheap enough at open time.
		last, err := t.scanBlock(index[len(index)-1].offset, indexOff, func(e kvEntry) bool { return false })
		if err != nil {
			f.Close()
			return nil, err
		}
		t.maxKey = last
	}
	return t, nil
}

// scanBlock scans records from off until limit or until fn returns true.
// Returns the last key seen.
func (t *sstable) scanBlock(off, limit int64, fn func(kvEntry) bool) (string, error) {
	r := bufio.NewReader(io.NewSectionReader(t.f, off, limit-off))
	var lastKey string
	for {
		var hdr [9]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return lastKey, nil
		}
		klen := int(binary.LittleEndian.Uint32(hdr[1:5]))
		vlen := int(binary.LittleEndian.Uint32(hdr[5:9]))
		kb := make([]byte, klen)
		if _, err := io.ReadFull(r, kb); err != nil {
			return lastKey, err
		}
		vb := make([]byte, vlen)
		if _, err := io.ReadFull(r, vb); err != nil {
			return lastKey, err
		}
		lastKey = string(kb)
		if fn(kvEntry{key: lastKey, value: vb, tombstone: hdr[0]&1 == 1}) {
			return lastKey, nil
		}
	}
}

func (t *sstable) indexLimit() (int64, error) {
	st, err := t.f.Stat()
	if err != nil {
		return 0, err
	}
	var footer [24]byte
	if _, err := t.f.ReadAt(footer[:], st.Size()-24); err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(footer[0:8])), nil
}

// get returns (value, tombstone, found).
func (t *sstable) get(key string) ([]byte, bool, bool, error) {
	if len(t.index) == 0 || key < t.minKey || key > t.maxKey {
		return nil, false, false, nil
	}
	// Find the last index entry with key <= target.
	i := sort.Search(len(t.index), func(i int) bool { return t.index[i].key > key }) - 1
	if i < 0 {
		return nil, false, false, nil
	}
	limit, err := t.indexLimit()
	if err != nil {
		return nil, false, false, err
	}
	var found kvEntry
	var ok bool
	_, err = t.scanBlock(t.index[i].offset, limit, func(e kvEntry) bool {
		if e.key == key {
			found, ok = e, true
			return true
		}
		return e.key > key
	})
	if err != nil {
		return nil, false, false, err
	}
	if !ok {
		return nil, false, false, nil
	}
	return found.value, found.tombstone, true, nil
}

// all returns every entry in the table, sorted.
func (t *sstable) all() ([]kvEntry, error) {
	limit, err := t.indexLimit()
	if err != nil {
		return nil, err
	}
	var out []kvEntry
	_, err = t.scanBlock(0, limit, func(e kvEntry) bool {
		out = append(out, e)
		return false
	})
	return out, err
}

func (t *sstable) close() error { return t.f.Close() }
