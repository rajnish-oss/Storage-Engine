package lsm

import (
	"bytes"
	"container/heap"
	"errors"
	"io"
	"os"

	"github.com/rajnish-oss/LSM_storage_engine/internal/codec"
	"github.com/rajnish-oss/LSM_storage_engine/internal/filter"
	"github.com/rajnish-oss/LSM_storage_engine/internal/sstable"
)

type HeapItem struct {
	Entry        *codec.Entry
	SSTableIndex int
}

type MinHeap []*HeapItem

func (h MinHeap) Len() int      { return len(h) }
func (h MinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *MinHeap) Push(x any)    { *h = append(*h, x.(*HeapItem)) }
func (h *MinHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

func (h MinHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].Entry.Key, h[j].Entry.Key)
	if cmp != 0 {
		return cmp < 0
	}
	// Newer SSTables have a higher index, so their keys take precedence
	return h[i].SSTableIndex > h[j].SSTableIndex
}

// MergeSSTables executes a streaming K-way merge across input SSTables,
// drops overwritten keys and tombstones, and emits a new SSTable with an active Bloom filter.
func MergeSSTables(sstables []*sstable.SSTable, newPath string, samplingInterval int) (*sstable.SSTable, error) {
	if len(sstables) == 0 {
		return nil, errors.New("no sstables provided for compaction")
	}

	if samplingInterval <= 0 {
		samplingInterval = 16
	}

	newFile, err := os.Create(newPath)
	if err != nil {
		return nil, err
	}
	defer newFile.Close()

	files := make([]*os.File, len(sstables))
	for i, sst := range sstables {
		f, err := os.Open(sst.Path)
		if err != nil {
			for j := 0; j < i; j++ {
				files[j].Close()
			}
			return nil, err
		}
		files[i] = f
		defer files[i].Close()
	}

	h := &MinHeap{}
	heap.Init(h)

	// Estimate upper-bound keys across all tables for the Bloom filter sizing
	// Fallback to 1024 if sampling estimates are small to keep false positives low
	estimatedKeys := 0
	for _, sst := range sstables {
		estimatedKeys += len(sst.SparseIndex) * samplingInterval
	}
	if estimatedKeys <= 0 {
		estimatedKeys = 1024
	}

	bf := filter.NewBloomFilter(estimatedKeys, 0.01)

	// Prime the min-heap with the first entry from each SSTable
	for i, file := range files {
		entry, err := codec.DecodeEntry(file)
		if err != nil {
			if errors.Is(err, io.EOF) {
				continue
			}
			return nil, err
		}
		heap.Push(h, &HeapItem{Entry: entry, SSTableIndex: i})
	}

	var (
		lastSeenKey []byte
		hasLastKey  bool
		minKey      []byte
		maxKey      []byte
		sparseIndex []sstable.SparseEntry
		totalOffset int64
		idxCounter  int
	)

	// Stream merge records from the heap
	for h.Len() > 0 {
		item := heap.Pop(h).(*HeapItem)
		currentKey := item.Entry.Key

		// Advance the SSTable from which this item was popped
		nextEntry, err := codec.DecodeEntry(files[item.SSTableIndex])
		if err == nil {
			heap.Push(h, &HeapItem{Entry: nextEntry, SSTableIndex: item.SSTableIndex})
		} else if !errors.Is(err, io.EOF) {
			return nil, err
		}

		// Deduplicate: If key matches lastSeenKey, it is an older version; drop it.
		if hasLastKey && bytes.Equal(currentKey, lastSeenKey) {
			continue
		}

		lastSeenKey = append(lastSeenKey[:0], currentKey...)
		hasLastKey = true

		// Drop tombstones during compaction
		if item.Entry.IsTombstone {
			continue
		}

		if idxCounter == 0 {
			minKey = append([]byte(nil), item.Entry.Key...)
		}
		maxKey = append([]byte(nil), item.Entry.Key...)

		// Checkpoint sparse index
		if idxCounter%samplingInterval == 0 {
			sparseIndex = append(sparseIndex, sstable.SparseEntry{
				Key:    append([]byte(nil), item.Entry.Key...),
				Offset: totalOffset,
			})
		}

		n, err := codec.EncodeEntry(newFile, item.Entry.Key, item.Entry.Value, false)
		if err != nil {
			return nil, err
		}

		totalOffset += int64(n)
		bf.Add(item.Entry.Key)
		idxCounter++
	}

	finalMaxKey := make([]byte, len(maxKey))
	copy(finalMaxKey, maxKey)


	bfData, err := bf.MarshalBinary()
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(newPath+".filter", bfData, 0644); err != nil {
		return nil, err
	}

	if err := newFile.Sync(); err != nil {
		return nil, err
	}

	return &sstable.SSTable{
		Path:        newPath,
		MinKey:      minKey,
		MaxKey:      finalMaxKey,
		SparseIndex: sparseIndex,
	}, nil
}