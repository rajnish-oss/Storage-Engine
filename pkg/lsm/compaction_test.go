package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/rajnish-oss/LSM_storage_engine/internal/codec"
	"github.com/rajnish-oss/LSM_storage_engine/internal/filter"
	"github.com/rajnish-oss/LSM_storage_engine/internal/sstable"
)

// helper to create a mock SSTable raw file with given entries
type testRecord struct {
	Key         string
	Val         string
	IsTombstone bool
}

func createTestSSTable(t *testing.T, dir string, sstName string, records []testRecord, sampling int) *sstable.SSTable {
	t.Helper()
	path := filepath.Join(dir, sstName)
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create dummy sst %s: %v", sstName, err)
	}
	defer f.Close()

	var (
		sparseIndex []sstable.SparseEntry
		offset      int64
		minKey      []byte
		maxKey      []byte
	)

	bf := filter.NewBloomFilter(len(records)+10, 0.01)

	for i, r := range records {
		k := []byte(r.Key)
		v := []byte(r.Val)

		if i == 0 {
			minKey = make([]byte, len(k))
			copy(minKey, k)
		}
		maxKey = k

		if i%sampling == 0 {
			sparseIndex = append(sparseIndex, sstable.SparseEntry{
				Key:    k,
				Offset: offset,
			})
		}

		n, err := codec.EncodeEntry(f, k, v, r.IsTombstone)
		if err != nil {
			t.Fatalf("failed to encode entry: %v", err)
		}
		offset += int64(n)
		bf.Add(k)
	}

	bfData, _ := bf.MarshalBinary()
	_ = os.WriteFile(path+".filter", bfData, 0644)
	_ = f.Sync()

	return &sstable.SSTable{
		Path:        path,
		MinKey:      minKey,
		MaxKey:      maxKey,
		SparseIndex: sparseIndex,
	}
}

// readAllCompactedEntries decodes all key-values sequentially from an output SSTable
func readAllCompactedEntries(t *testing.T, sstPath string) []testRecord {
	t.Helper()
	f, err := os.Open(sstPath)
	if err != nil {
		t.Fatalf("failed to open compacted sst: %v", err)
	}
	defer f.Close()

	var records []testRecord
	for {
		entry, err := codec.DecodeEntry(f)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("unexpected read error: %v", err)
		}
		records = append(records, testRecord{
			Key:         string(entry.Key),
			Val:         string(entry.Value),
			IsTombstone: entry.IsTombstone,
		})
	}
	return records
}

// 1. Tests multi-table K-way merge, deduplication, and newest value precedence
func TestMergeSSTables_PrecedenceAndDeduplication(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "compaction_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Older SSTable (Index 0 in slice)
	oldSST := createTestSSTable(t, tempDir, "00001.sst", []testRecord{
		{Key: "apple", Val: "v1_old"},
		{Key: "banana", Val: "v1_keep"},
		{Key: "cherry", Val: "v1_old"},
		{Key: "date", Val: "v1_old"},
	}, 2)

	// Newer SSTable (Index 1 in slice - takes precedence)
	newSST := createTestSSTable(t, tempDir, "00002.sst", []testRecord{
		{Key: "apple", Val: "v2_new"},            // Overwrite apple
		{Key: "cherry", Val: "", IsTombstone: true}, // Delete cherry
		{Key: "elderberry", Val: "v2_new"},       // New key inserted
	}, 2)

	outPath := filepath.Join(tempDir, "compacted_001.sst")
	samplingInterval := 2

	// Passing [oldSST, newSST]: index 1 has higher priority than index 0
	compacted, err := MergeSSTables([]*sstable.SSTable{oldSST, newSST}, outPath, samplingInterval)
	if err != nil {
		t.Fatalf("MergeSSTables failed: %v", err)
	}

	// Read emitted records back
	records := readAllCompactedEntries(t, outPath)

	expected := []testRecord{
		{Key: "apple", Val: "v2_new"},       // v2 won over v1
		{Key: "banana", Val: "v1_keep"},     // preserved
		{Key: "date", Val: "v1_old"},        // preserved
		{Key: "elderberry", Val: "v2_new"},  // inserted
		// cherry must be purged completely
	}

	if len(records) != len(expected) {
		t.Fatalf("expected %d records after compaction, got %d: %+v", len(expected), len(records), records)
	}

	for i, exp := range expected {
		if records[i].Key != exp.Key || records[i].Val != exp.Val {
			t.Errorf("record %d mismatch: expected {%s, %s}, got {%s, %s}",
				i, exp.Key, exp.Val, records[i].Key, records[i].Val)
		}
	}

	// Verify MinKey and MaxKey metadata
	if string(compacted.MinKey) != "apple" {
		t.Errorf("expected MinKey 'apple', got '%s'", string(compacted.MinKey))
	}
	if string(compacted.MaxKey) != "elderberry" {
		t.Errorf("expected MaxKey 'elderberry', got '%s'", string(compacted.MaxKey))
	}
}

// 2. Tests that the companion Bloom Filter is generated, saved, and rejects absent keys
func TestMergeSSTables_BloomFilterCreation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "compaction_bf_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	sst := createTestSSTable(t, tempDir, "00001.sst", []testRecord{
		{Key: "user:1", Val: "data1"},
		{Key: "user:2", Val: "data2"},
		{Key: "user:3", Val: "data3"},
	}, 2)

	outPath := filepath.Join(tempDir, "compacted_bf.sst")
	_, err = MergeSSTables([]*sstable.SSTable{sst}, outPath, 2)
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	// Verify .filter file exists
	filterPath := outPath + ".filter"
	data, err := os.ReadFile(filterPath)
	if err != nil {
		t.Fatalf("failed to read generated bloom filter file: %v", err)
	}

	bf := &filter.BloomFilter{}
	if err := bf.UnmarshalBinary(data); err != nil {
		t.Fatalf("failed to unmarshal bloom filter: %v", err)
	}

	// Positive assertions
	if !bf.Contains([]byte("user:1")) || !bf.Contains([]byte("user:2")) {
		t.Fatalf("bloom filter failed to identify keys present in compacted table")
	}

	// Negative assertion
	if bf.Contains([]byte("non_existent_key_999")) {
		t.Fatalf("false positive on fresh non-existent key")
	}
}

// 3. Tests that the Sparse Index contains valid Seek offsets
func TestMergeSSTables_SparseIndexOffsets(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "compaction_sparse_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create 10 keys
	var records []testRecord
	for i := 0; i < 10; i++ {
		records = append(records, testRecord{
			Key: fmt.Sprintf("k_%02d", i),
			Val: fmt.Sprintf("payload_%02d", i),
		})
	}

	sst := createTestSSTable(t, tempDir, "00001.sst", records, 2)
	outPath := filepath.Join(tempDir, "compacted_sparse.sst")
	samplingInterval := 4

	compacted, err := MergeSSTables([]*sstable.SSTable{sst}, outPath, samplingInterval)
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	f, err := os.Open(outPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Every indexed entry in SparseIndex must match the exact record starting at that file offset
	for _, entry := range compacted.SparseIndex {
		_, err := f.Seek(entry.Offset, io.SeekStart)
		if err != nil {
			t.Fatalf("seek to offset %d failed: %v", entry.Offset, err)
		}

		decoded, err := codec.DecodeEntry(f)
		if err != nil {
			t.Fatalf("failed to decode entry at offset %d: %v", entry.Offset, err)
		}

		if !bytes.Equal(decoded.Key, entry.Key) {
			t.Fatalf("sparse index key mismatch: expected %s, got %s at offset %d",
				string(entry.Key), string(decoded.Key), entry.Offset)
		}
	}
}

// 4. Boundary cases: empty input slice & empty SSTables
func TestMergeSSTables_EdgeCases(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "compaction_edge_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Zero input tables
	_, err = MergeSSTables(nil, filepath.Join(tempDir, "out.sst"), 16)
	if err == nil {
		t.Fatalf("expected error when compacting zero sstables, got nil")
	}

	// SSTables containing only tombstones
	tombSST := createTestSSTable(t, tempDir, "tomb.sst", []testRecord{
		{Key: "k1", Val: "", IsTombstone: true},
		{Key: "k2", Val: "", IsTombstone: true},
	}, 2)

	outPath := filepath.Join(tempDir, "tomb_out.sst")
	compacted, err := MergeSSTables([]*sstable.SSTable{tombSST}, outPath, 16)
	if err != nil {
		t.Fatalf("failed to compact tombstones: %v", err)
	}

	records := readAllCompactedEntries(t, outPath)
	if len(records) != 0 {
		t.Fatalf("expected 0 entries after all tombstones purged, got %d", len(records))
	}
	if len(compacted.MinKey) != 0 || len(compacted.MaxKey) != 0 {
		t.Fatalf("expected empty MinKey/MaxKey on all-tombstone table")
	}
}