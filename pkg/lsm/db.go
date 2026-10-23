package lsm

import (
	"fmt"
	"github.com/rajnish-oss/LSM_storage_engine/internal/manifest"
	"github.com/rajnish-oss/LSM_storage_engine/internal/memtable"
	"github.com/rajnish-oss/LSM_storage_engine/internal/sstable"
	"github.com/rajnish-oss/LSM_storage_engine/internal/wal"
	"log"
	"os"
)

type DB[K comparable, V any] struct {
	memtable            *memtable.MemTable[K, V]
	maxMemtableSize     int
	memtableSize        int
	wal                 *wal.WAL[K, V]
	walPath             string
	sstables            []*sstable.SSTable[K, V]
	sstableCounter      int
	manifest            *manifest.Manifest
	manifestPath        string
	compactionThreshold int
}

func NewDB[K comparable, V any](maxMemtableSize int, compactionThreshold int) (*DB[K, V], error) {
	walPath := "db.wal"
	memtable, err := wal.ReplayWAL[K, V](walPath) // this is the replay
	if err != nil {
		return nil, err
	}

	wal, err := wal.NewWAL[K, V](walPath)
	if err != nil {
		return nil, err
	}

	manifestPath := "MANIFEST"
	manifest, err := manifest.ReadManifest(manifestPath)
	if err != nil {
		return nil, err
	}

	sstables := make([]*sstable.SSTable[K, V], len(manifest.SSTablePaths))
	for i, path := range manifest.SSTablePaths {
		sstables[i] = &sstable.SSTable[K, V]{Path: path}
	}

	return &DB[K, V]{
		memtable:            memtable,
		maxMemtableSize:     maxMemtableSize,
		memtableSize:        len(memtable.Data),
		wal:                 wal,
		walPath:             walPath,
		manifest:            manifest,
		manifestPath:        manifestPath,
		sstables:            sstables,
		compactionThreshold: compactionThreshold,
	}, nil
}

func (db *DB[K, V]) Compact() error {
	compactedSSTablePath := fmt.Sprintf("data-compacted-%d.sstable", db.sstableCounter)
	compactedSSTable, err := MergeSSTables(db.sstables, compactedSSTablePath)
	if err != nil {
		return err
	}
	// write new SSTable to MANIFEST file
	db.manifest.SSTablePaths = []string{compactedSSTablePath}
	if err := manifest.WriteManifest(db.manifestPath, db.manifest); err != nil {
		return err
	}
	//note delete only after writing manifest
	for _, sstable := range db.sstables {
		if err := os.Remove(sstable.Path); err != nil {
			log.Printf("Failed to remove old sstable %s: %v", sstable.Path, err)
		}
	}

	db.sstables = []*sstable.SSTable[K, V]{compactedSSTable}
	db.sstableCounter++

	return nil
}

func (db *DB[K, V]) Put(key K, value V) error {

	if err := db.wal.Write(key, value); err != nil {
		return err
	}

	db.memtable.Put(key, value)
	db.memtableSize++

	if db.memtableSize >= db.maxMemtableSize {
		if err := db.flushMemtable(); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB[K, V]) flushMemtable() error {
	sstablePath := fmt.Sprintf("data-%d.sstable", db.sstableCounter)
	sstable, err := sstable.WriteSSTable(db.memtable, sstablePath, 16)
	if err != nil {
		return err
	}

	db.sstables = append(db.sstables, sstable)
	db.sstableCounter++

	db.manifest.SSTablePaths = append(db.manifest.SSTablePaths, sstablePath)
	if err := manifest.WriteManifest(db.manifestPath, db.manifest); err != nil {
		return err
	}

	db.memtable = memtable.NewMemTable[K, V]()
	db.memtableSize = 0

	if len(db.sstables) >= db.compactionThreshold {
		if err := db.Compact(); err != nil {
			log.Printf("Compaction failed: %v", err)
			return err
		}
	}

	return nil
}

func (db *DB[K, V]) Get(key K) (V, error) {
	if val, ok := db.memtable.Get(key); ok {
		if any(val).(string) == TOMBSTONE {
			var zero V
			return zero, ErrNotFound
		}
		return val, nil
	}

	for i := len(db.sstables) - 1; i >= 0; i-- {
		table := db.sstables[i]
		val, err := table.Get(key)

		if err != nil {
			var zero V

			if err == sstable.ErrDeleted {
				return zero, ErrNotFound
			}
			if err == sstable.ErrNotFound {
				continue
			}
			return zero, err
		}

		return val, nil
	}

	var zero V
	return zero, ErrNotFound
}

func (db *DB[K, V]) Delete(key K) error {
	return db.Put(key, any(TOMBSTONE).(V))
}
