package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rajnish-oss/LSM_storage_engine/internal/wal"
)

func newTestDB(t *testing.T, modifyCfg func(*Config)) (*DB, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "lsm_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	cfg := DefaultConfig(dir)
	cfg.WALSyncMode = wal.SyncAlways // Ensure deterministic durability in tests
	if modifyCfg != nil {
		modifyCfg(&cfg)
	}

	db, err := Open(cfg)
	if err != nil {
		_ = os.RemoveAll(dir)
		t.Fatalf("failed to open test db: %v", err)
	}

	return db, dir
}

// 1. Basic Put, Get, Overwrite, Delete, and Missing Key validation
func TestDB_BasicOperations(t *testing.T) {
	db, dir := newTestDB(t, nil)
	defer os.RemoveAll(dir)
	defer db.Close()

	key := []byte("user_101")
	val := []byte("alice")

	// Get non-existent
	if _, err := db.Get(key); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}

	// Put and Get
	if err := db.Put(key, val); err != nil {
		t.Fatalf("failed to put key: %v", err)
	}

	got, err := db.Get(key)
	if err != nil || !bytes.Equal(got, val) {
		t.Fatalf("expected value %s, got %s (err: %v)", val, got, err)
	}

	// Overwrite
	valUpdated := []byte("alice_updated")
	if err := db.Put(key, valUpdated); err != nil {
		t.Fatalf("failed to overwrite key: %v", err)
	}

	got, err = db.Get(key)
	if err != nil || !bytes.Equal(got, valUpdated) {
		t.Fatalf("expected overwritten value %s, got %s", valUpdated, got)
	}

	// Delete and Tombstone verification
	if err := db.Delete(key); err != nil {
		t.Fatalf("failed to delete key: %v", err)
	}

	if _, err := db.Get(key); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound after delete, got %v", err)
	}
}

// 2. MemTable rotation and persistence across SSTables
func TestDB_FlushAndSSTableRead(t *testing.T) {
	// Set threshold small so that each record forces a rotation
	db, dir := newTestDB(t, func(c *Config) {
		c.MemTableThreshold = 64
		c.MaxImmTables = 5
	})
	defer os.RemoveAll(dir)
	defer db.Close()

	entries := map[string]string{
		"k1": "val_large_payload_0000000001",
		"k2": "val_large_payload_0000000002",
		"k3": "val_large_payload_0000000003",
		"k4": "val_large_payload_0000000004",
	}

	for k, v := range entries {
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("failed to write %s: %v", k, err)
		}
	}

	// Allow the background flushWorker to drain the immutable queue into SSTables
	time.Sleep(100 * time.Millisecond)

	for k, expectedVal := range entries {
		got, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("failed to get key %s from disk/imm: %v", k, err)
		}
		if string(got) != expectedVal {
			t.Fatalf("mismatch for key %s: expected %s, got %s", k, expectedVal, string(got))
		}
	}
}

// 3. Backpressure when immutable queue fills to MaxImmTables
func TestDB_BackpressureUnderBurst(t *testing.T) {
	db, dir := newTestDB(t, func(c *Config) {
		c.MemTableThreshold = 32
		c.MaxImmTables = 2
	})
	defer os.RemoveAll(dir)
	defer db.Close()

	var wg sync.WaitGroup
	numWriters := 8
	recordsPerWriter := 25

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < recordsPerWriter; i++ {
				k := []byte(fmt.Sprintf("w_%d_k_%04d", workerID, i))
				v := []byte(fmt.Sprintf("w_%d_v_%04d", workerID, i))
				if err := db.Put(k, v); err != nil {
					t.Errorf("write stalled/failed: %v", err)
					return
				}
			}
		}(w)
	}

	wg.Wait()

	// Verify all writes survived backpressure
	for w := 0; w < numWriters; w++ {
		for i := 0; i < recordsPerWriter; i++ {
			k := []byte(fmt.Sprintf("w_%d_k_%04d", workerID(w), i))
			expected := fmt.Sprintf("w_%d_v_%04d", workerID(w), i)
			got, err := db.Get(k)
			if err != nil || string(got) != expected {
				t.Fatalf("data corruption or lost write under load: key %s, err %v", k, err)
			}
		}
	}
}

func workerID(id int) int { return id }

// 4. WAL Recovery: writes must survive an abrupt simulated process restart
func TestDB_CrashRecoveryWAL(t *testing.T) {
	dir, err := os.MkdirTemp("", "lsm_wal_recovery_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cfg := DefaultConfig(dir)
	cfg.WALSyncMode = wal.SyncAlways
	cfg.MemTableThreshold = 1024 * 1024 // Large enough not to flush automatically

	db1, err := Open(cfg)
	if err != nil {
		t.Fatalf("initial open failed: %v", err)
	}

	key1 := []byte("wal_k1")
	val1 := []byte("wal_v1")
	key2 := []byte("wal_k2")
	val2 := []byte("wal_v2")

	_ = db1.Put(key1, val1)
	_ = db1.Put(key2, val2)
	_ = db1.Delete(key1) // Tombstone should persist to WAL

	// Simulate crash: close the WAL handle without triggering a graceful MemTable flush
	db1.mu.Lock()
	if db1.activeWAL != nil {
		_ = db1.activeWAL.Close()
	}
	// Do not call db1.Close(), stop flushWorker directly
	close(db1.closeCh)
	db1.flushCond.Broadcast()
	db1.mu.Unlock()
	db1.flushWg.Wait()

	// Reopen DB to trigger recoverWAL()
	db2, err := Open(cfg)
	if err != nil {
		t.Fatalf("failed to reopen DB: %v", err)
	}
	defer db2.Close()

	// key1 was deleted
	if _, err := db2.Get(key1); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected key1 to be deleted on recovery, got %v", err)
	}

	// key2 should exist
	got2, err := db2.Get(key2)
	if err != nil || !bytes.Equal(got2, val2) {
		t.Fatalf("expected %s on recovery, got %s (err: %v)", val2, got2, err)
	}
}

// 5. Engine Close safety and state validation
func TestDB_Close(t *testing.T) {
	db, dir := newTestDB(t, nil)
	defer os.RemoveAll(dir)

	k := []byte("test_key")
	v := []byte("test_value")

	if err := db.Put(k, v); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Writes and Reads should fail with ErrClosed
	if err := db.Put([]byte("after_close"), []byte("val")); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed on Put, got %v", err)
	}

	if _, err := db.Get(k); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed on Get, got %v", err)
	}
}