package filter

import (

	"github.com/spaolacci/murmur3"
	"math"
	"errors"
	"encoding/binary"
)

type BloomFilter struct {
	Bitset []byte
	Size   uint64 // total bits (m)
	K      uint32 // number of hash functions
}

var (
	ErrCorruptFilter = errors.New("corrupt bloom filter binary payload")
)

func (bf *BloomFilter) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 8+4+len(bf.Bitset))
	binary.BigEndian.PutUint64(buf[0:8], bf.Size)
	binary.BigEndian.PutUint32(buf[8:12], bf.K)
	copy(buf[12:], bf.Bitset)
	return buf, nil
}

// UnmarshalBinary reconstructs a BloomFilter from a packed byte slice.
func (bf *BloomFilter) UnmarshalBinary(data []byte) error {
	if len(data) < 12 {
		return ErrCorruptFilter
	}

	bf.Size = binary.BigEndian.Uint64(data[0:8])
	bf.K = binary.BigEndian.Uint32(data[8:12])

	expectedBytes := int((bf.Size + 7) / 8)
	bitsetPayload := data[12:]

	if len(bitsetPayload) != expectedBytes {
		return ErrCorruptFilter
	}

	bf.Bitset = make([]byte, expectedBytes)
	copy(bf.Bitset, bitsetPayload)

	return nil
}

// NewBloomFilter Sizes the Bitset given expected elements (n) and false positive rate (p)
// Optimal m = - (n * ln(p)) / (ln(2)^2)
// Optimal k = (m / n) * ln(2)
func NewBloomFilter(n int, p float64) *BloomFilter {
	if n <= 0 {
		n = 1
	}
	if p <= 0 || p >= 1 {
		p = 0.01
	}

	m := uint64(math.Ceil(-float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))
	k := uint32(math.Ceil((float64(m) / float64(n)) * math.Log(2)))

	// Byte-align the Bitset
	byteSize := (m + 7) / 8
	return &BloomFilter{
		Bitset: make([]byte, byteSize),
		Size:   byteSize * 8,
		K:      k,
	}
}


func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := murmur3.Sum128(key)

	for i := uint32(0); i < bf.K; i++ {
		combined := (h1 + uint64(i)*h2) % bf.Size
		bf.Bitset[combined/8] |= (1 << (combined % 8))
	}
}

func (bf *BloomFilter) Contains(key []byte) bool {
	h1, h2 := murmur3.Sum128(key)
	for i := uint32(0); i < bf.K; i++ {
		combined := (h1 + uint64(i)*h2) % bf.Size
		if (bf.Bitset[combined/8] & (1 << (combined % 8))) == 0 {
			return false // Definitely does not exist
		}
	}
	return true // Might exist
}