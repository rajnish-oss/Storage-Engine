package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/rajnish-oss/LSM_storage_engine/internal/memtable"
)

// TestWriteSSTableEmptyMemtable ensures writing an empty MemTable fails fast.
func TestWriteSSTableEmptyMemtable(t *testing.T) {
	dir := t.TempDir()
	sstPath := filepath.Join(dir, "empty.sst")

	m := memtable.NewMemTable()
	_, err := WriteSSTable(m, sstPath, 16)
	if !errors.Is(err, EmptyMemtable) {
		t.Fatalf("expected EmptyMemtable error, got: %v", err)
	}
}

// TestWriteAndReadSSTable verifies basic round-trip lookups, deleted tombstones,
// min/max key boundary checks, and Bloom filter exclusion.
func TestWriteAndReadSSTable(t *testing.T) {
	dir := t.TempDir()
	sstPath := filepath.Join(dir, "00001.sst")

	m := memtable.NewMemTable()
	m.Put([]byte("key_b"), []byte("val_b"))
	m.Put([]byte("key_a"), []byte("val_a"))
	m.Put([]byte("key_d"), []byte("val_d"))
	m.Delete([]byte("key_c")) // Tombstone insertion

	sst, err := WriteSSTable(m, sstPath, 2)
	if err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}

	// Verify MinKey and MaxKey tracking
	if !bytes.Equal(sst.MinKey, []byte("key_a")) {
		t.Errorf("expected MinKey key_a, got %s", sst.MinKey)
	}
	if !bytes.Equal(sst.MaxKey, []byte("key_d")) {
		t.Errorf("expected MaxKey key_d, got %s", sst.MaxKey)
	}

	// Verify .filter file creation on disk
	if _, err := os.Stat(sstPath + ".filter"); os.IsNotExist(err) {
		t.Errorf("expected bloom filter file %s.filter to exist", sstPath)
	}

	// 1. Existing active keys
	val, err := sst.Get([]byte("key_a"))
	if err != nil || !bytes.Equal(val, []byte("val_a")) {
		t.Errorf("key_a lookup failed: val=%s, err=%v", val, err)
	}

	val, err = sst.Get([]byte("key_d"))
	if err != nil || !bytes.Equal(val, []byte("val_d")) {
		t.Errorf("key_d lookup failed: val=%s, err=%v", val, err)
	}

	// 2. Deleted tombstone key
	_, err = sst.Get([]byte("key_c"))
	if !errors.Is(err, ErrDeleted) {
		t.Errorf("expected ErrDeleted for tombstone key_c, got: %v", err)
	}

	// 3. Out-of-bounds keys (smaller than MinKey / larger than MaxKey)
	_, err = sst.Get([]byte("key_0"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound for key below min, got: %v", err)
	}

	_, err = sst.Get([]byte("key_z"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound for key above max, got: %v", err)
	}

	// 4. Missing key within min/max bounds (should miss via Bloom filter / sparse scan)
	_, err = sst.Get([]byte("key_ba"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound for missing intermediate key, got: %v", err)
	}
}

// TestSSTableSparseIndexSampling verifies sparse index density and targeted seek ranges
// across larger datasets that exceed multiple sample intervals.
func TestSSTableSparseIndexSampling(t *testing.T) {
	dir := t.TempDir()
	sstPath := filepath.Join(dir, "sparse.sst")

	m := memtable.NewMemTable()
	const totalKeys = 100
	const interval = 10

	for i := 0; i < totalKeys; i++ {
		k := []byte(fmt.Sprintf("key_%04d", i))
		v := []byte(fmt.Sprintf("val_%04d", i))
		m.Put(k, v)
	}

	sst, err := WriteSSTable(m, sstPath, interval)
	if err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}

	// 100 items with interval 10 should yield 10 sparse index entries (0, 10, 20, ..., 90)
	expectedEntries := totalKeys / interval
	if len(sst.SparseIndex) != expectedEntries {
		t.Fatalf("expected %d sparse index entries, got %d", expectedEntries, len(sst.SparseIndex))
	}

	// Verify all keys can still be resolved through the sparse index blocks
	for i := 0; i < totalKeys; i++ {
		k := []byte(fmt.Sprintf("key_%04d", i))
		wantVal := []byte(fmt.Sprintf("val_%04d", i))

		gotVal, err := sst.Get(k)
		if err != nil {
			t.Fatalf("failed lookup for %s: %v", k, err)
		}
		if !bytes.Equal(gotVal, wantVal) {
			t.Fatalf("value mismatch for %s: got %s, want %s", k, gotVal, wantVal)
		}
	}
}

// TestBloomFilterBypass verifies that non-existent keys bypass file reads
// without touching disk when filtered by the Bloom filter.
func TestBloomFilterBypass(t *testing.T) {
	dir := t.TempDir()
	sstPath := filepath.Join(dir, "filter_bypass.sst")

	m := memtable.NewMemTable()
	for i := 0; i < 20; i++ {
		m.Put([]byte(fmt.Sprintf("item_%02d", i)), []byte("val"))
	}

	sst, err := WriteSSTable(m, sstPath, 4)
	if err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}

	// Between item_00 and item_19: "item_05_missing"
	testKey := []byte("item_05_missing")

	if sst.filter.Contains(testKey) {
		t.Skip("hash collision in bloom filter; skipping bypass assertion")
	}

	// Temporarily delete SSTable file: if Bloom filter works, Get() exits before os.Open()
	if err := os.Remove(sstPath); err != nil {
		t.Fatalf("failed removing sstable file: %v", err)
	}

	_, err = sst.Get(testKey)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound directly from Bloom filter check, got: %v", err)
	}
}