package lsm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rajnish-oss/LSM_storage_engine/internal/memtable"
	"github.com/rajnish-oss/LSM_storage_engine/internal/sstable"
	"github.com/rajnish-oss/LSM_storage_engine/internal/wal"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrDatabaseClosed = errors.New("database is closed")
)

// Options holds engine configuration parameters.
type Options struct {
	DataDir             string
	MemTableThreshold   int64 // In bytes (e.g., 4MB = 4 * 1024 * 1024)
	SSTableSamplingStep int   // Interval for sparse index entries (e.g., 16 or 64)
	SyncWAL             bool  // If true, call fsync on every write
}

func DefaultOptions(dataDir string) Options {
	return Options{
		DataDir:             dataDir,
		MemTableThreshold:   4 * 1024 * 1024, // 4MB default
		SSTableSamplingStep: 16,
		SyncWAL:             false,
	}
}

// DB represents the LSM storage engine instance.
type DB struct {
	mu        sync.RWMutex
	flushMu   sync.Mutex // Serializes flush operations
	opts      Options
	activeMem *memtable.MemTable
	immMem    *memtable.MemTable
	wal       *wal.WAL
	sstables  []*sstable.SSTable
	closed    bool
}

// Open initializes or recovers an LSM DB directory.
func Open(opts Options) (*DB, error) {
	if err := os.MkdirAll(opts.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}

	walPath := filepath.Join(opts.DataDir, "active.wal")

	// Replay existing WAL if present
	recoveredMem, err := wal.ReplayWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to replay WAL: %w", err)
	}

	w, err := wal.NewWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	db := &DB{
		opts:      opts,
		activeMem: recoveredMem,
		wal:       w,
		sstables:  make([]*sstable.SSTable, 0),
	}

	return db, nil
}

// Put writes or updates a key-value pair.
func (db *DB) Put(key, value []byte) error {
	return db.write(key, value, false)
}

// Delete appends a deletion tombstone for the given key.
func (db *DB) Delete(key []byte) error {
	return db.write(key, nil, true)
}

func (db *DB) write(key, value []byte, isTombstone bool) error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrDatabaseClosed
	}

	// 1. Write to Write-Ahead Log
	if err := db.wal.Write(key, value, isTombstone); err != nil {
		db.mu.Unlock()
		return fmt.Errorf("wal write failed: %w", err)
	}

	if db.opts.SyncWAL {
		if err := db.wal.Sync(); err != nil {
			db.mu.Unlock()
			return fmt.Errorf("wal sync failed: %w", err)
		}
	}

	// 2. Insert into Active SkipList MemTable
	if isTombstone {
		db.activeMem.Delete(key)
	} else {
		db.activeMem.Put(key, value)
	}

	// 3. Check flush threshold
	needsFlush := db.activeMem.Size() >= db.opts.MemTableThreshold && db.immMem == nil
	if needsFlush {
		// Freeze active table into immutable table
		db.immMem = db.activeMem
		db.activeMem = memtable.NewMemTable()

		// Rotate WAL
		_ = db.wal.Close()
		oldWalPath := filepath.Join(db.opts.DataDir, "active.wal")
		flushingWalPath := filepath.Join(db.opts.DataDir, fmt.Sprintf("flushing-%d.wal", time.Now().UnixNano()))
		_ = os.Rename(oldWalPath, flushingWalPath)

		newWal, err := wal.NewWAL(oldWalPath)
		if err != nil {
			db.mu.Unlock()
			return fmt.Errorf("failed to initialize rotated WAL: %w", err)
		}
		db.wal = newWal

		// Release main RWLock before performing disk flush
		db.mu.Unlock()
		go db.flushMemTable(flushingWalPath)
		return nil
	}

	db.mu.Unlock()
	return nil
}

// flushMemTable writes immMem to a new SSTable file on disk.
func (db *DB) flushMemTable(walToClean string) {
	db.flushMu.Lock()
	defer db.flushMu.Unlock()

	db.mu.RLock()
	tableToFlush := db.immMem
	db.mu.RUnlock()

	if tableToFlush == nil || tableToFlush.Size() == 0 {
		return
	}

	sstPath := filepath.Join(db.opts.DataDir, fmt.Sprintf("sst-%d.db", time.Now().UnixNano()))
	sst, err := sstable.WriteSSTable(tableToFlush, sstPath, db.opts.SSTableSamplingStep)
	if err != nil {
		// In a production engine, emit telemetry or alert compaction scheduler
		return
	}

	db.mu.Lock()
	// Prepend so the newest SSTable is checked first during Get()
	db.sstables = append([]*sstable.SSTable{sst}, db.sstables...)
	db.immMem = nil
	db.mu.Unlock()

	// Safely clean up the flushed WAL file
	_ = os.Remove(walToClean)
}

// Get resolves the latest value for key across the hierarchy.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrDatabaseClosed
	}

	// 1. Check Active MemTable
	if val, deleted, found := db.activeMem.Get(key); found {
		db.mu.RUnlock()
		if deleted {
			return nil, ErrKeyNotFound
		}
		return val, nil
	}

	// 2. Check Immutable MemTable (if currently flushing)
	if db.immMem != nil {
		if val, deleted, found := db.immMem.Get(key); found {
			db.mu.RUnlock()
			if deleted {
				return nil, ErrKeyNotFound
			}
			return val, nil
		}
	}

	// Snapshot SSTable references to search while holding read lock
	tables := make([]*sstable.SSTable, len(db.sstables))
	copy(tables, db.sstables)
	db.mu.RUnlock()

	// 3. Search SSTables (from newest to oldest)
	for _, sst := range tables {
		val, err := sst.Get(key)
		if err == nil {
			return val, nil
		}
		if errors.Is(err, sstable.ErrDeleted) {
			return nil, ErrKeyNotFound
		}
		if !errors.Is(err, sstable.ErrNotFound) {
			return nil, err
		}
	}

	return nil, ErrKeyNotFound
}

// Close flushes any remaining active data and safely closes handles.
func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true

	// If activeMem has records, trigger a synchronous flush
	if db.activeMem.Size() > 0 {
		sstPath := filepath.Join(db.opts.DataDir, fmt.Sprintf("sst-%d.db", time.Now().UnixNano()))
		sst, err := sstable.WriteSSTable(db.activeMem, sstPath, db.opts.SSTableSamplingStep)
		if err == nil {
			db.sstables = append([]*sstable.SSTable{sst}, db.sstables...)
		}
	}

	err := db.wal.Close()
	db.mu.Unlock()
	return err
}