package lsm

import (
	"container/heap"
	"encoding/gob"
	"github.com/rajnish-oss/LSM_storage_engine/internal/sstable"
	"io"
	"os"
)

type HeapItem[K comparable, V any] struct {
	Pair         sstable.Pair[K, V]
	SSTableIndex int
}

type MinHeap[K comparable, V any] []*HeapItem[K, V]

func (h MinHeap[K, V]) Less(i, j int) bool {

	keyI := any(h[i].Pair.Key).(string)
	keyJ := any(h[j].Pair.Key).(string)
	if keyI != keyJ {
		return keyI < keyJ
	}

	return h[i].SSTableIndex > h[j].SSTableIndex
}

func MergeSSTables[K comparable, V any](sstables []*sstable.SSTable[K, V], newPath string) (*sstable.SSTable[K, V], error) {
	newFile, err := os.Create(newPath) // create a new sstable file
	if err != nil {
		return nil, err
	}
	defer newFile.Close() // prevent memory leak by ensuring file is closed

	newEncoder := gob.NewEncoder(newFile) // initialize encoder for new SSTable file

	files := make([]*os.File, len(sstables))        // open all the sstables
	decoders := make([]*gob.Decoder, len(sstables)) // initialize one decoder per ssltable file
	for i, sstable := range sstables {
		files[i], err = os.Open(sstable.Path)
		if err != nil {
			return nil, err
		}
		defer files[i].Close() // prevent memory leak by ensuring file is closed
		decoders[i] = gob.NewDecoder(files[i])
	}

	// read first pair from each sstable and store in a pair array
	pairs := make([]sstable.Pair[K, V], len(decoders))
	emptySSTables := make([]bool, len(decoders)) // track empty sstables
	for i, decoder := range decoders {
		if err := decoder.Decode(&pairs[i]); err != nil {
			if err == io.EOF {
				emptySSTables[i] = true
				continue
			}
			return nil, err
		}
	}

	// push those pairs onto heap
	h := &MinHeap[K, V]{}
	for i, pair := range pairs {
		if !emptySSTables[i] {
			heap.Push(h, &HeapItem[K, V]{Pair: pair, SSTableIndex: i})
		}
	}

	// init the min-heap calculation algorithm from container/heap package
	heap.Init(h)

	var lastKey K
	firstKey := true

	// pop the min item from heap and store it into new sstable
	for h.Len() > 0 {
		item := heap.Pop(h).(*HeapItem[K, V])

		// If this key is a duplicate of the last one we saw, skip it
		if !firstKey && item.Pair.Key == lastKey {
			// We only care about the version from the newest SSTable,
			// which we have already processed
		} else {
			if any(item.Pair.Value).(string) != TOMBSTONE {
				// discard deleted
				if err := newEncoder.Encode(item.Pair); err != nil {
					return nil, err
				}
			}
		}

		lastKey = item.Pair.Key
		firstKey = false

		// Push the next item from the same SSTable into the heap
		var nextPair sstable.Pair[K, V]
		if err := decoders[item.SSTableIndex].Decode(&nextPair); err == nil {
			heap.Push(h, &HeapItem[K, V]{Pair: nextPair, SSTableIndex: item.SSTableIndex})
		} else if err != io.EOF {
			return nil, err
		}
	}

	return &sstable.SSTable[K, V]{Path: newPath}, nil
}
