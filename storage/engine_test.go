package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func openTest(t *testing.T, dir string, flushSize int) *Engine {
	t.Helper()
	e, err := OpenEngine(dir, EngineOptions{FlushSize: flushSize, SyncWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestPutGetDelete(t *testing.T) {
	e := openTest(t, t.TempDir(), 1<<20)
	defer e.Close()

	if err := e.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	v, ok, err := e.Get("a")
	if err != nil || !ok || string(v) != "1" {
		t.Fatalf("get a = %q %v %v", v, ok, err)
	}
	if err := e.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := e.Get("a"); ok {
		t.Fatal("deleted key still visible")
	}
	if _, ok, _ := e.Get("missing"); ok {
		t.Fatal("missing key found")
	}
}

func TestOverwrite(t *testing.T) {
	e := openTest(t, t.TempDir(), 1<<20)
	defer e.Close()

	e.Put("k", []byte("v1"))
	e.Put("k", []byte("v2"))
	v, ok, _ := e.Get("k")
	if !ok || string(v) != "v2" {
		t.Fatalf("got %q", v)
	}
}

func TestFlushAndReadFromSSTable(t *testing.T) {
	e := openTest(t, t.TempDir(), 1<<20)
	defer e.Close()

	for i := 0; i < 500; i++ {
		e.Put(fmt.Sprintf("key%04d", i), []byte(fmt.Sprintf("val%d", i)))
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		v, ok, err := e.Get(fmt.Sprintf("key%04d", i))
		if err != nil || !ok || string(v) != fmt.Sprintf("val%d", i) {
			t.Fatalf("key%04d = %q %v %v", i, v, ok, err)
		}
	}
}

func TestCrashRecoveryFromWAL(t *testing.T) {
	dir := t.TempDir()
	e := openTest(t, dir, 1<<20)
	e.Put("x", []byte("1"))
	e.Put("y", []byte("2"))
	e.Delete("x")
	// Simulate crash: do not Close (WAL already fsynced per write).

	e2 := openTest(t, dir, 1<<20)
	defer e2.Close()
	if _, ok, _ := e2.Get("x"); ok {
		t.Fatal("x should be deleted after recovery")
	}
	v, ok, _ := e2.Get("y")
	if !ok || string(v) != "2" {
		t.Fatalf("y = %q %v", v, ok)
	}
}

func TestTornWALTailIgnored(t *testing.T) {
	dir := t.TempDir()
	e := openTest(t, dir, 1<<20)
	e.Put("good", []byte("v"))
	e.wal.Sync()

	// Append garbage to simulate a torn write.
	f, _ := os.OpenFile(filepath.Join(dir, "engine.wal"), os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write([]byte{0xde, 0xad, 0xbe})
	f.Close()

	e2 := openTest(t, dir, 1<<20)
	defer e2.Close()
	v, ok, _ := e2.Get("good")
	if !ok || string(v) != "v" {
		t.Fatalf("good = %q %v", v, ok)
	}
}

func TestCompaction(t *testing.T) {
	dir := t.TempDir()
	e := openTest(t, dir, 1<<20)
	defer e.Close()

	// Create many SSTables to trigger compaction.
	for round := 0; round < 10; round++ {
		for i := 0; i < 100; i++ {
			e.Put(fmt.Sprintf("k%03d", i), []byte(fmt.Sprintf("r%d", round)))
		}
		if round < 5 {
			e.Delete(fmt.Sprintf("k%03d", round))
		}
		if err := e.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	// After compaction there should be few files.
	files, _ := filepath.Glob(filepath.Join(dir, "*.sst"))
	if len(files) > maxTablesBeforeCompact+1 {
		t.Fatalf("expected compaction, got %d sstables", len(files))
	}
	// Latest values win.
	for i := 0; i < 100; i++ {
		v, ok, _ := e.Get(fmt.Sprintf("k%03d", i))
		if !ok || string(v) != "r9" {
			t.Fatalf("k%03d = %q %v", i, v, ok)
		}
	}
}

func TestSnapshotAndReset(t *testing.T) {
	e := openTest(t, t.TempDir(), 1<<20)
	e.Put("a", []byte("1"))
	e.Put("b", []byte("2"))
	e.Delete("a")
	e.Flush()
	e.Put("c", []byte("3"))

	snap, err := e.SnapshotEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(snap) != 2 || string(snap["b"]) != "2" || string(snap["c"]) != "3" {
		t.Fatalf("snapshot = %v", snap)
	}
	e.Close()

	dir2 := t.TempDir()
	e2 := openTest(t, dir2, 1<<20)
	defer e2.Close()
	e2.Put("old", []byte("junk"))
	if err := e2.Reset(snap); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := e2.Get("old"); ok {
		t.Fatal("old key survived reset")
	}
	v, ok, _ := e2.Get("b")
	if !ok || string(v) != "2" {
		t.Fatalf("b = %q %v", v, ok)
	}
}

func TestLargeWorkloadAcrossFlushes(t *testing.T) {
	dir := t.TempDir()
	e := openTest(t, dir, 8<<10) // tiny flush size to force many flushes
	want := map[string]string{}
	for i := 0; i < 3000; i++ {
		k := fmt.Sprintf("key%05d", i%700)
		v := fmt.Sprintf("val%d", i)
		e.Put(k, []byte(v))
		want[k] = v
	}
	e.Close()

	e2 := openTest(t, dir, 8<<10)
	defer e2.Close()
	for k, wv := range want {
		v, ok, err := e2.Get(k)
		if err != nil || !ok || string(v) != wv {
			t.Fatalf("%s = %q %v %v (want %q)", k, v, ok, err, wv)
		}
	}
}
