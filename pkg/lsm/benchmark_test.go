package lsm

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
	"path/filepath"
	"errors"

	"github.com/rajnish-oss/LSM_storage_engine/internal/wal"
	"github.com/rajnish-oss/LSM_storage_engine/internal/memtable"
	"github.com/rajnish-oss/LSM_storage_engine/internal/sstable"
)

func populateDBForReadBench(b *testing.B, numKeys int) (*DB, func()) {
	b.Helper()

	dir, err := os.MkdirTemp("", "lsm_bench_read_*")
	if err != nil {
		b.Fatalf("failed to create read bench dir: %v", err)
	}

	// Use smaller threshold to trigger flushes and generate multiple SSTables on disk
	cfg := Config{
		DataDir:           dir,
		MemTableThreshold: 256 * 1024, // 256 KB forces periodic flushes
		MaxImmTables:      8,
		WALSyncMode:       wal.SyncBatch,
		WALBatchInterval:  5 * time.Millisecond,
		WALBatchSize:      256,
		SamplingInterval:  16,
	}

	db, err := Open(cfg)
	if err != nil {
		_ = os.RemoveAll(dir)
		b.Fatalf("failed to open read bench db: %v", err)
	}

	val := make([]byte, 128)
	rng := rand.New(rand.NewSource(42))

	// Pre-populate with deterministic keys
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key_%016d", rng.Int63n(int64(numKeys*10))))
		if err := db.Put(key, val); err != nil {
			_ = db.Close()
			_ = os.RemoveAll(dir)
			b.Fatalf("failed to populate key %d: %v", i, err)
		}
	}

	// Force an explicit rotation to push remaining active memtable into immQueue,
	// then sleep briefly to allow background flushWorker to write SSTables to disk.
	db.mu.Lock()
	if db.mem.Size() > 0 {
		_ = db.rotateMemTable()
	}
	db.mu.Unlock()

	time.Sleep(200 * time.Millisecond)

	cleanup := func() {
		_ = db.Close()
		_ = os.RemoveAll(dir)
	}

	return db, cleanup
}
func setupBenchmarkDB(b *testing.B) (*DB, func()) {
	b.Helper()

	dir, err := os.MkdirTemp("", "lsm_bench_*")
	if err != nil {
		b.Fatalf("failed to create bench dir: %v", err)
	}

	cfg := Config{
		DataDir:            dir,
		MemTableThreshold:  4 * 1024 * 1024, // 4 MB
		MaxImmTables:       4,
		SamplingInterval  : 16,
	}

	db, err := Open(cfg)
	if err != nil {
		os.RemoveAll(dir)
		b.Fatalf("failed to open bench db: %v", err)
	}

	cleanup := func() {
		_ = db.Close()
		_ = os.RemoveAll(dir)
	}

	return db, cleanup
}

func BenchmarkDB_SequentialWrites(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b)
	defer cleanup()

	val := make([]byte, 128) // fixed payload
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key_%016d", i))
		if err := db.Put(key, val); err != nil {
			b.Fatalf("put failed: %v", err)
		}
	}
}

func BenchmarkDB_RandomWrites(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b)
	defer cleanup()

	val := make([]byte, 128)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Non-sequential key distribution
		key := []byte(fmt.Sprintf("key_%016d", rand.Int63()))
		if err := db.Put(key, val); err != nil {
			b.Fatalf("put failed: %v", err)
		}
	}
}

func BenchmarkDB_ReadMiss_WithoutBloomFilter(b *testing.B) {
	dir, err := os.MkdirTemp("", "lsm_bench_nobloom_*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// 1. Build a test MemTable with resident data
	mem := memtable.NewMemTable()
	val := make([]byte, 128)
	numKeys := 10_000
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key_%016d", i*2)) // Even keys exist
		mem.Put(key, val)
	}

	// 2. Flush to an on-disk SSTable
	sstPath := filepath.Join(dir, "bench_nobloom.sst")
	sst, err := sstable.WriteSSTable(mem, sstPath, 16)
	if err != nil {
		b.Fatalf("failed to write sstable: %v", err)
	}

	// 3. Remove companion .filter file to ensure zero Bloom assistance
	filterPath := sstPath + ".filter"
	_ = os.Remove(filterPath)

	b.ResetTimer()
	b.ReportAllocs()

	// 4. Query odd keys: forces sparse index binary search + disk scan on each miss
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key_%016d", i*2+1))
		_, err := sst.Get(key)
		if !errors.Is(err, ErrNotFound) {
			b.Fatalf("expected ErrNotFound, got: %v", err)
		}
	}
}

func BenchmarkDB_ReadMiss_WithBloomFilter(b *testing.B) {
	db, cleanup := populateDBForReadBench(b, 10_000)
	defer cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("non_existent_key_%d", i))
		_, err := db.Get(key)
		if !errors.Is(err, ErrKeyNotFound) {
			b.Fatalf("expected not found, got: %v", err)
		}
	}
}

func BenchmarkDB_ConcurrentWrites(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b)
	defer cleanup()

	val := make([]byte, 128)
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		id := rand.Int63()
		for pb.Next() {
			key := []byte(fmt.Sprintf("k_%016d", id))
			if err := db.Put(key, val); err != nil {
				b.Errorf("concurrent put failed: %v", err)
			}
			id++
		}
	})
}