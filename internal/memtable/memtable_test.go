package memtable

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// TestMemTablePutGetDelete tests basic engine semantics: insertions,
// overwrites, explicit deletions (tombstones), and missing key lookups.
func TestMemTablePutGetDelete(t *testing.T) {
	m := NewMemTable()

	// Initial write
	m.Put([]byte("account:101"), []byte("balance_500"))
	val, isDeleted, found := m.Get([]byte("account:101"))
	if !found || isDeleted || !bytes.Equal(val, []byte("balance_500")) {
		t.Fatalf("unexpected read: found=%v, isDeleted=%v, val=%s", found, isDeleted, val)
	}

	// Overwrite existing key
	m.Put([]byte("account:101"), []byte("balance_1200"))
	val, isDeleted, found = m.Get([]byte("account:101"))
	if !found || isDeleted || !bytes.Equal(val, []byte("balance_1200")) {
		t.Fatalf("unexpected overwrite: found=%v, isDeleted=%v, val=%s", found, isDeleted, val)
	}

	// Delete (tombstone)
	m.Delete([]byte("account:101"))
	val, isDeleted, found = m.Get([]byte("account:101"))
	if !found || !isDeleted {
		t.Fatalf("expected tombstone record: found=%v, isDeleted=%v", found, isDeleted)
	}

	// Non-existent key lookup
	_, _, found = m.Get([]byte("account:999"))
	if found {
		t.Fatalf("expected found=false for missing key")
	}
}

// TestMemTableSizeTracking ensures Size() tracks data volume accurately as keys are added.
func TestMemTableSizeTracking(t *testing.T) {
	m := NewMemTable()

	if initial := m.Size(); initial != 0 {
		t.Fatalf("expected empty memtable size to be 0, got %d", initial)
	}

	k1 := []byte("key:alpha")
	v1 := []byte("payload_12345")
	m.Put(k1, v1)

	sizeAfterFirst := m.Size()
	if sizeAfterFirst <= 0 {
		t.Fatalf("expected positive size after insert, got %d", sizeAfterFirst)
	}

	// Overwriting with a larger value must increase size
	v2 := bytes.Repeat([]byte("X"), 512)
	m.Put(k1, v2)

	if m.Size() <= sizeAfterFirst {
		t.Fatalf("expected size to grow after overwrite with larger payload: before=%d, after=%d",
			sizeAfterFirst, m.Size())
	}
}

// TestMemTableIteratorInOrder verifies that the SkipList iterator preserves sorted byte order.
func TestMemTableIteratorInOrder(t *testing.T) {
	m := NewMemTable()

	keys := []string{"zebra", "apple", "mango", "banana", "cherry"}
	for _, k := range keys {
		m.Put([]byte(k), []byte("v_"+k))
	}

	it := m.Iterator()
	var extracted []string
	for it.HasNext() {
		entry := it.Next()
		extracted = append(extracted, string(entry.Key))
	}

	expected := []string{"apple", "banana", "cherry", "mango", "zebra"}
	if len(extracted) != len(expected) {
		t.Fatalf("expected %d entries, got %d", len(expected), len(extracted))
	}

	for i := range expected {
		if extracted[i] != expected[i] {
			t.Errorf("at index %d: expected %s, got %s", i, expected[i], extracted[i])
		}
	}
}

// TestMemTableConcurrentOperations tests thread-safety under heavy concurrent readers and writers.
func TestMemTableConcurrentOperations(t *testing.T) {
	m := NewMemTable()

	const writers = 10
	const readers = 10
	const opsPerRoutine = 100

	var wg sync.WaitGroup
	wg.Add(writers + readers)

	// Concurrent Writers
	for i := 0; i < writers; i++ {
		go func(wid int) {
			defer wg.Done()
			for j := 0; j < opsPerRoutine; j++ {
				k := []byte(fmt.Sprintf("user:%02d:%04d", wid, j))
				v := []byte("session_payload_active")
				if j%10 == 0 {
					m.Delete(k)
				} else {
					m.Put(k, v)
				}
			}
		}(i)
	}

	// Concurrent Readers
	for i := 0; i < readers; i++ {
		go func(rid int) {
			defer wg.Done()
			for j := 0; j < opsPerRoutine; j++ {
				k := []byte(fmt.Sprintf("user:%02d:%04d", rid, j))
				m.Get(k)
			}
		}(i)
	}

	wg.Wait()
}