package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// WAL is an append-only write-ahead log. Each record is:
//   crc32(4) | len(4) | payload(len)
// Records with a bad checksum or truncated tail (torn write on crash) are
// discarded on replay; everything before the corruption point is recovered.
type WAL struct {
	f   *os.File
	buf *bufio.Writer
	off int64
}

func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &WAL{f: f, buf: bufio.NewWriter(f), off: st.Size()}, nil
}

// Append writes a record. Durable only after Sync.
func (w *WAL) Append(payload []byte) error {
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], crc32.ChecksumIEEE(payload))
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(payload)))
	if _, err := w.buf.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.buf.Write(payload); err != nil {
		return err
	}
	w.off += int64(8 + len(payload))
	return nil
}

// Sync flushes buffered records and fsyncs the file.
func (w *WAL) Sync() error {
	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *WAL) Close() error {
	if err := w.Sync(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

// Replay reads all valid records from a WAL file, invoking fn for each.
// Stops silently at the first corrupted/truncated record.
func ReplayWAL(path string, fn func(payload []byte) error) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		var hdr [8]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return nil // clean EOF or torn header
		}
		crc := binary.LittleEndian.Uint32(hdr[0:4])
		n := binary.LittleEndian.Uint32(hdr[4:8])
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil // torn payload
		}
		if crc32.ChecksumIEEE(payload) != crc {
			return nil // corruption; discard tail
		}
		if err := fn(payload); err != nil {
			return fmt.Errorf("wal replay: %w", err)
		}
	}
}
