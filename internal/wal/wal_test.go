package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestWALWriteAndReplay verifies writing normal keys and tombstones across
// all three sync modes (SyncNone, SyncAlways, SyncBatch) and replaying into a MemTable.
func TestWALWriteAndReplay(t *testing.T) {
	modes := []struct {
		name string
		mode SyncMode
	}{
		{"SyncNone", SyncNone},
		{"SyncAlways", SyncAlways},
		{"SyncBatch", SyncBatch},
	}

	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{
				Dir:           dir,
				SyncMode:      tc.mode,
				BatchInterval: 2 * time.Millisecond,
				BatchSize:     16,
			}

			seg, err := NewSegment(opts, 1)
			if err != nil {
				t.Fatalf("failed to create segment: %v", err)
			}

			writes := []struct {
				k []byte
				v []byte
				t bool
			}{
				{[]byte("user:1"), []byte("alice"), false},
				{[]byte("user:2"), []byte("bob"), false},
				{[]byte("user:3"), []byte("charlie"), false},
				{[]byte("user:2"), nil, true}, // Tombstone
				{[]byte("user:4"), []byte("dave"), false},
			}

			for _, w := range writes {
				if err := seg.Write(w.k, w.v, w.t); err != nil {
					t.Fatalf("write failed: %v", err)
				}
			}

			if err := seg.Close(); err != nil {
				t.Fatalf("close failed: %v", err)
			}

			// Replay segment into MemTable
			walPath := filepath.Join(dir, "00001.wal")
			m, err := ReplaySegment(walPath)
			if err != nil {
				t.Fatalf("replay failed: %v", err)
			}

			// 1. Verify user:1 survives and is NOT a tombstone
			val, isDeleted, found := m.Get([]byte("user:1"))
			if !found || isDeleted || !bytes.Equal(val, []byte("alice")) {
				t.Errorf("user:1 mismatch: found=%v, isDeleted=%v, val=%s", found, isDeleted, val)
			}

			// 2. Verify user:2 was deleted (found as a tombstone, or marked deleted)
			_, isDeleted, found = m.Get([]byte("user:2"))
			if !found || !isDeleted {
				t.Errorf("expected user:2 to exist as a tombstone (found=%v, isDeleted=%v)", found, isDeleted)
			}

			// 3. Verify user:3 and user:4 survive
			val, isDeleted, found = m.Get([]byte("user:3"))
			if !found || isDeleted || !bytes.Equal(val, []byte("charlie")) {
				t.Errorf("user:3 mismatch: found=%v, isDeleted=%v, val=%s", found, isDeleted, val)
			}

			val, isDeleted, found = m.Get([]byte("user:4"))
			if !found || isDeleted || !bytes.Equal(val, []byte("dave")) {
				t.Errorf("user:4 mismatch: found=%v, isDeleted=%v, val=%s", found, isDeleted, val)
			}
		})
	}
}

// TestWALConcurrentWrites tests race conditions during high concurrent writes,
// especially targeting the SyncBatch queue loop.
func TestWALConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:           dir,
		SyncMode:      SyncBatch,
		BatchInterval: 2 * time.Millisecond,
		BatchSize:     32,
	}

	seg, err := NewSegment(opts, 42)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}

	const goroutines = 20
	const writesPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(gid int) {
			defer wg.Done()
			for j := 0; j < writesPerGoroutine; j++ {
				key := []byte("key-" + string(rune('A'+gid)) + "-" + string(rune('0'+j)))
				val := []byte("val-payload")
				if err := seg.Write(key, val, false); err != nil {
					t.Errorf("concurrent write failed: %v", err)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	if err := seg.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Verify all writes replayed correctly
	walPath := filepath.Join(dir, "00042.wal")
	m, err := ReplaySegment(walPath)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	expectedTotal := goroutines * writesPerGoroutine
	var count int
	it := m.Iterator()
	for {
		entry := it.Next()
		if entry == nil {
			break
		}
		count++
	}

	if count != expectedTotal {
		t.Fatalf("expected %d entries after replay, got %d", expectedTotal, count)
	}
}

// TestWALCorruptRecordReplay verifies that corrupt bytes in a WAL segment
// cleanly error out during ReplaySegment instead of silent skips.
func TestWALCorruptRecordReplay(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:      dir,
		SyncMode: SyncAlways,
	}

	seg, err := NewSegment(opts, 10)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}

	if err := seg.Write([]byte("valid_key"), []byte("valid_val"), false); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	_ = seg.Close()

	walPath := filepath.Join(dir, "00010.wal")
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("failed to read wal: %v", err)
	}

	// Corrupt the last byte (checksum)
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(walPath, data, 0644); err != nil {
		t.Fatalf("failed to write corrupted wal: %v", err)
	}

	_, err = ReplaySegment(walPath)
	if err == nil {
		t.Fatalf("expected ReplaySegment to return error on corrupted CRC, got nil")
	}
}

// TestDiscoverSegments verifies segment discovery and sorting by ID.
func TestDiscoverSegments(t *testing.T) {
	dir := t.TempDir()

	// Create arbitrary segment files out of order
	fileNames := []string{"00003.wal", "00001.wal", "00012.wal", "ignore.tmp", "00002.wal"}
	for _, f := range fileNames {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("dummy"), 0644); err != nil {
			t.Fatalf("failed to create dummy file: %v", err)
		}
	}

	ids, err := DiscoverSegments(dir)
	if err != nil {
		t.Fatalf("DiscoverSegments failed: %v", err)
	}

	expected := []uint64{1, 2, 3, 12}
	if len(ids) != len(expected) {
		t.Fatalf("expected %d IDs, got %d", len(expected), len(ids))
	}

	for i, id := range ids {
		if id != expected[i] {
			t.Errorf("at index %d: expected ID %d, got %d", i, expected[i], id)
		}
	}
}

// TestWALRemove verifies segment deletion and cleanup.
func TestWALRemove(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:      dir,
		SyncMode: SyncNone,
	}

	seg, err := NewSegment(opts, 7)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}

	_ = seg.Write([]byte("k"), []byte("v"), false)

	walPath := filepath.Join(dir, "00007.wal")
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		t.Fatalf("file should exist before removal")
	}

	if err := seg.Remove(); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	if _, err := os.Stat(walPath); !os.IsNotExist(err) {
		t.Errorf("file should not exist after Remove")
	}
}