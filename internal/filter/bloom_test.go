package filter

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"testing"
)

// TestBloomFilterNoFalseNegatives guarantees that any inserted key is always found.
func TestBloomFilterNoFalseNegatives(t *testing.T) {
	n := 10_000
	bf := NewBloomFilter(n, 0.01)

	// Insert keys
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key_%06d", i)
		bf.Add([]byte(key))
	}

	// Verify every single inserted key returns true
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key_%06d", i)
		if !bf.Contains([]byte(key)) {
			t.Fatalf("false negative detected: key %s was added but Contains() returned false", key)
		}
	}
}

// TestBloomFilterFalsePositiveRate measures actual false positive rate against the configured 1%.
func TestBloomFilterFalsePositiveRate(t *testing.T) {
	n := 50_000
	targetFPR := 0.01
	bf := NewBloomFilter(n, targetFPR)

	// Populate with [0, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("inserted_%d", i)
		bf.Add([]byte(key))
	}

	// Probe with completely different keys [0, testQueries)
	testQueries := 100_000
	falsePositives := 0

	for i := 0; i < testQueries; i++ {
		queryKey := fmt.Sprintf("absent_%d", i)
		if bf.Contains([]byte(queryKey)) {
			falsePositives++
		}
	}

	measuredFPR := float64(falsePositives) / float64(testQueries)
	t.Logf("Total Queries: %d, False Positives: %d, Measured FPR: %.4f (Target: %.2f)",
		testQueries, falsePositives, measuredFPR, targetFPR)

	// Allow a reasonable margin of error above the 1% target (e.g., max 1.5%)
	maxAcceptableFPR := 0.015
	if measuredFPR > maxAcceptableFPR {
		t.Fatalf("measured FPR %.4f exceeds maximum acceptable threshold of %.4f",
			measuredFPR, maxAcceptableFPR)
	}
}

// TestBloomFilterSerialization verifies the bitset and metadata survive gob/disk round-trips.
func TestBloomFilterSerialization(t *testing.T) {
	bf := NewBloomFilter(1_000, 0.01)

	testKeys := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	for _, k := range testKeys {
		bf.Add([]byte(k))
	}

	// Encode to memory buffer
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(bf); err != nil {
		t.Fatalf("failed to encode bloom filter: %v", err)
	}

	// Decode into a new instance
	var loadedBF BloomFilter
	if err := gob.NewDecoder(&buf).Decode(&loadedBF); err != nil {
		t.Fatalf("failed to decode bloom filter: %v", err)
	}

	// Check metadata
	if loadedBF.Size != bf.Size || loadedBF.K != bf.K {
		t.Fatalf("metadata mismatch: got size=%d, k=%d; want size=%d, k=%d",
			loadedBF.Size, loadedBF.K, bf.Size, bf.K)
	}

	// Check key presence
	for _, k := range testKeys {
		if !loadedBF.Contains([]byte(k)) {
			t.Fatalf("loaded filter failed to match key %s", k)
		}
	}

	// Check a definitely absent key
	if loadedBF.Contains([]byte("definitely_not_present_xyz_123")) {
		t.Logf("note: absent key matched due to false positive (possible, but rare)")
	}
}

// BenchmarkBloomFilterOperations benchmarks read/write speeds.
func BenchmarkBloomFilterAdd(b *testing.B) {
	bf := NewBloomFilter(b.N, 0.01)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Add([]byte("bench_key_sample"))
	}
}

func BenchmarkBloomFilterContains(b *testing.B) {
	bf := NewBloomFilter(100_000, 0.01)
	key := []byte("bench_query_key")
	bf.Add(key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bf.Contains(key)
	}
}
