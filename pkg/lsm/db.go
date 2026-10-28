package lsm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rajnish-oss/LSM_storage_engine/internal/memtable"
	"github.com/rajnish-oss/LSM_storage_engine/internal/sstable"
	"github.com/rajnish-oss/LSM_storage_engine/internal/wal"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrClosed      = errors.New("database is closed")
)

type Config struct {
	DataDir           string
	MemTableThreshold int64        // Max size in bytes before rotating active MemTable
	MaxImmTables      int
	WALSyncMode       wal.SyncMode // SyncNone, SyncAlways, SyncBatch
	WALBatchInterval  time.Duration
	WALBatchSize      int
	SamplingInterval  int // Sparse index interval for WriteSSTable
}

func DefaultConfig(dataDir string) Config {
	return Config{
		DataDir:           dataDir,
		MemTableThreshold: 4 * 1024 * 1024, // 4MB
		MaxImmTables:      3,
		WALSyncMode:       wal.SyncBatch,
		WALBatchInterval:  5 * time.Millisecond,
		WALBatchSize:      256,
		SamplingInterval:  16,
	}
}

type flushTask struct {
	mem memtable.Reader
	wal *wal.WALSegment
}

type DB struct {
	mu           sync.RWMutex
	cfg          Config
	mem          *memtable.MemTable
	immQueue     []flushTask
	activeWAL    *wal.WALSegment
	flushingWAL  *wal.WALSegment
	currentSegID uint64
	sstables     []*sstable.SSTable
	flushCond    *sync.Cond
	closeCh      chan struct{}
	flushWg      sync.WaitGroup
	isClosed     uint32
}

// Open initializes directories, replays un-persisted WAL segments, and starts the background flusher.
func Open(cfg Config) (*DB, error) {
	walDir := filepath.Join(cfg.DataDir, "wal")
	sstDir := filepath.Join(cfg.DataDir, "sst")

	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create wal dir: %w", err)
	}
	if err := os.MkdirAll(sstDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create sst dir: %w", err)
	}

	db := &DB{
		cfg:      cfg,
		mem:      memtable.NewMemTable(),
		immQueue: make([]flushTask, 0, cfg.MaxImmTables),
		closeCh:  make(chan struct{}),
	}
	db.flushCond = sync.NewCond(&db.mu)

	// 1. Recover un-persisted records from existing WAL segments
	if err := db.recoverWAL(walDir); err != nil {
		return nil, fmt.Errorf("wal recovery failed: %w", err)
	}

	// 2. Open next active WAL segment
	db.currentSegID++
	walOpts := wal.Options{
		Dir:           walDir,
		SyncMode:      cfg.WALSyncMode,
		BatchInterval: cfg.WALBatchInterval,
		BatchSize:     cfg.WALBatchSize,
	}
	activeSeg, err := wal.NewSegment(walOpts, db.currentSegID)
	if err != nil {
		return nil, fmt.Errorf("failed to create active wal segment: %w", err)
	}
	db.activeWAL = activeSeg

	// 3. Start background flush worker
	db.flushWg.Add(1)
	go db.flushWorker()

	return db, nil
}

func (db *DB) recoverWAL(walDir string) error {
	segIDs, err := wal.DiscoverSegments(walDir)
	if err != nil {
		return err
	}

	for _, id := range segIDs {
		segPath := filepath.Join(walDir, fmt.Sprintf("%05d.wal", id))
		replayedMem, err := wal.ReplaySegment(segPath)
		if err != nil {
			return err
		}

		it := replayedMem.Iterator()
		for it.HasNext() {
			node := it.Next()
			if node.Deleted {
				db.mem.Delete(node.Key)
			} else {
				db.mem.Put(node.Key, node.Value)
			}
		}

		if id > db.currentSegID {
			db.currentSegID = id
		}

		_ = os.Remove(segPath)
	}

	return nil
}

func (db *DB) Put(key, value []byte) error {
	return db.write(key, value, false)
}

func (db *DB) Delete(key []byte) error {
	return db.write(key, nil, true)
}

func (db *DB) write(key, value []byte, isTombstone bool) error {
	if atomic.LoadUint32(&db.isClosed) == 1 {
		return ErrClosed
	}

	db.mu.Lock()

	for len(db.immQueue) >= db.cfg.MaxImmTables {
		db.flushCond.Wait()
		if atomic.LoadUint32(&db.isClosed) == 1 {
			db.mu.Unlock()
			return ErrClosed
		}
	}

// 2. ROTATION: Check if active MemTable exceeds threshold
	if db.mem.Size() >= db.cfg.MemTableThreshold {
		if err := db.rotateMemTable(); err != nil {
			db.mu.Unlock()
			return fmt.Errorf("memtable rotation failed: %w", err)
		}
	}

	currentWAL := db.activeWAL
	currentMem := db.mem
	db.mu.Unlock()

	// Append to WAL
	if err := currentWAL.Write(key, value, isTombstone); err != nil {
		return fmt.Errorf("wal write failed: %w", err)
	}

	// Insert into in-memory table
	if isTombstone {
		 currentMem.Delete(key)
	}else{
		currentMem.Put(key, value)
	}
	return nil
}

// rotateMemTable freezes the current active table, creates a new one, and steps WAL segment generation.
// Must be called with db.mu held.
func (db *DB) rotateMemTable() error {
	db.immQueue = append(db.immQueue, flushTask{
		mem: db.mem,
		wal: db.activeWAL,
	})

	db.currentSegID++
	walOpts := wal.Options{
		Dir:           filepath.Join(db.cfg.DataDir, "wal"),
		SyncMode:      db.cfg.WALSyncMode,
		BatchInterval: db.cfg.WALBatchInterval,
		BatchSize:     db.cfg.WALBatchSize,
	}

	newWAL, err := wal.NewSegment(walOpts, db.currentSegID)
	if err != nil {
		return err
	}

	db.activeWAL = newWAL
	db.mem = memtable.NewMemTable()

	db.flushCond.Signal()
	return nil
}

// Get checks mem, imm, and on-disk SSTables.
func (db *DB) Get(key []byte) ([]byte, error) {
	if atomic.LoadUint32(&db.isClosed) == 1 {
		return nil, ErrClosed
	}

	db.mu.RLock()
	// 1. Search active MemTable
	val, tombstone, found := db.mem.Get(key)
	if found {
		db.mu.RUnlock()
		if tombstone {
			return nil, ErrKeyNotFound
		}
		return val, nil
	}

	// 2. Search immutable queue (newest to oldest)
	for i := len(db.immQueue) - 1; i >= 0; i-- {
		val, tombstone, found := db.immQueue[i].mem.Get(key)
		if found {
			db.mu.RUnlock()
			if tombstone {
				return nil, ErrKeyNotFound
			}
			return val, nil
		}
	}

	// Copy SSTable slice pointers to scan without holding RLock during disk reads
	tables := make([]*sstable.SSTable, len(db.sstables))
	copy(tables, db.sstables)
	db.mu.RUnlock()

	// 3. Search SSTables newest to oldest
	for i := len(tables) - 1; i >= 0; i-- {
		val, err := tables[i].Get(key)
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

func (db *DB) flushWorker() {
	defer db.flushWg.Done()

	for {
		db.mu.Lock()
		for len(db.immQueue) == 0 {
			select {
			case <-db.closeCh:
				// Engine closing: finish draining any remaining tables in queue
				if len(db.immQueue) == 0 {
					db.mu.Unlock()
					return
				}
			default:
			}
			db.flushCond.Wait()
		}

		// Pop oldest task (FIFO)
		task := db.immQueue[0]
		db.mu.Unlock()

		// DISK I/O: performed without holding db.mu
		sstPath := filepath.Join(db.cfg.DataDir, "sst", fmt.Sprintf("%05d.sst", task.wal.SegmentID()))
		newSST, err := sstable.WriteSSTable(task.mem, sstPath, db.cfg.SamplingInterval)
		if err != nil {
			// In production, log or trigger recovery
			continue
		}

		// Clean up flushed table and notify stalled writers
		db.mu.Lock()
		db.sstables = append(db.sstables, newSST)
		db.immQueue = db.immQueue[1:]

		// Signal stalled writers waiting for free buffer space
		db.flushCond.Broadcast()
		db.mu.Unlock()

		// Safely discard persisted WAL segment
		_ = task.wal.Remove()
	}
}

// Close flushes any remaining active table and frees open handles.
func (db *DB) Close() error {
	if !atomic.CompareAndSwapUint32(&db.isClosed, 0, 1) {
		return nil
	}

	db.mu.Lock()
	if db.mem.Size() > 0 {
		_ = db.rotateMemTable()
	}
	db.mu.Unlock()

	// Signal shutdown to flusher and unblock waiters
	close(db.closeCh)
	db.flushCond.Broadcast()
	db.flushWg.Wait()

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activeWAL != nil {
		return db.activeWAL.Close()
	}

	return nil
}