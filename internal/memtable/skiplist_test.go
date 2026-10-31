package memtable

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestSkipListSequentialOrder(t *testing.T) {
	sl := NewSkipList(DefaultMaxLevel, DefaultProbability)

	keys := []string{"orange", "apple", "banana", "grape", "mango", "peach"}
	for _, k := range keys {
		sl.Put([]byte(k), []byte("val-"+k), false)
	}

	it := sl.NewIterator()
	var collected []string
	for it.HasNext() {
		node := it.Next()
		collected = append(collected, string(node.Key))
	}

	expected := []string{"apple", "banana", "grape", "mango", "orange", "peach"}
	if len(collected) != len(expected) {
		t.Fatalf("expected %d keys, got %d", len(expected), len(collected))
	}

	for i := range expected {
		if collected[i] != expected[i] {
			t.Errorf("at index %d: expected %s, got %s", i, expected[i], collected[i])
		}
	}
}

func TestSkipListOverwritesAndDeletes(t *testing.T) {
	sl := NewSkipList(8, 0.5)

	sl.Put([]byte("key1"), []byte("value_initial"), false)
	val, del, ok := sl.Get([]byte("key1"))
	if !ok || del || !bytes.Equal(val, []byte("value_initial")) {
		t.Fatalf("unexpected read: ok=%v, del=%v, val=%s", ok, del, val)
	}

	// Overwrite value
	sl.Put([]byte("key1"), []byte("value_updated"), false)
	val, del, ok = sl.Get([]byte("key1"))
	if !ok || del || !bytes.Equal(val, []byte("value_updated")) {
		t.Fatalf("unexpected overwrite: ok=%v, del=%v, val=%s", ok, del, val)
	}

	// Tombstone delete
	sl.Put([]byte("key1"), nil, true)
	_, del, ok = sl.Get([]byte("key1"))
	if !ok || !del {
		t.Fatalf("expected tombstone hit: ok=%v, del=%v", ok, del)
	}
}

func TestSkipListConcurrentRaces(t *testing.T) {
	sl := NewSkipList(16, 0.5)
	const goroutines = 20
	const opsPerWorker = 100

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Concurrent Writers
	for i := 0; i < goroutines; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				key := []byte(fmt.Sprintf("key-%03d-%03d", workerID, j))
				val := []byte("payload")
				sl.Put(key, val, false)
			}
		}(i)
	}

	// Concurrent Readers
	for i := 0; i < goroutines; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				key := []byte(fmt.Sprintf("key-%03d-%03d", workerID, j))
				sl.Get(key)
			}
		}(i)
	}

	wg.Wait()
}