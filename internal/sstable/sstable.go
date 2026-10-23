package sstable

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/rajnish-oss/LSM_storage_engine/internal/filter"
	"github.com/rajnish-oss/LSM_storage_engine/internal/codec"
	"github.com/rajnish-oss/LSM_storage_engine/internal/memtable"
	"io"
	"os"
	"sort"
)

var (
	ErrNotFound   = errors.New("key not found")
	ErrDeleted    = errors.New("key deleted")
	EmptyMemtable = errors.New("empty memtable")
)

const Tombstone = "__DELETED__"

type Pair[K comparable, V any] struct {
	Key   K
	Value V
}

type SparseEntry[K comparable] struct {
	Key    K
	Offset int64
}

type SSTable[K comparable, V any] struct {
	Path string

	MinKey string
	MaxKey string

	SparseIndex []SparseEntry[K]

	filter *filter.BloomFilter
}

type offsetWriter struct {
	w       io.Writer
	written int64
}

func (ow *offsetWriter) Write(b []byte) (int, error) {
	n, err := ow.w.Write(b)
	ow.written += int64(n)
	return n, err
}

func saveFilter(path string, bf *filter.BloomFilter) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return gob.NewEncoder(f).Encode(bf)
}

func loadFilter(path string) (*filter.BloomFilter, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var bf filter.BloomFilter
	if err := gob.NewDecoder(f).Decode(&bf); err != nil {
		return nil, err
	}
	return &bf, nil
}

func WriteSSTable[K comparable, V any](memtable *memtable.MemTable[K, V], path string, samplingInterval int) (*SSTable[K, V], error) {
	if len(memtable.Data) == 0 {
		return nil, EmptyMemtable
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	pairs := make([]Pair[K, V], 0, len(memtable.Data))
	for k, v := range memtable.Data {
		pairs = append(pairs, Pair[K, V]{Key: k, Value: v})
	}

	sort.Slice(pairs, func(i, j int) bool {
		return any(pairs[i].Key).(string) < any(pairs[j].Key).(string)
	})

	filter := filter.NewBloomFilter(len(pairs), 0.01)

	ow := &offsetWriter{w: file}

	encoder := gob.NewEncoder(ow)
	sparseIndex := make([]SparseEntry[K], 0, len(pairs)/samplingInterval)

	for i, pair := range pairs {
		if i%samplingInterval == 0 {
			sparseIndex = append(sparseIndex, SparseEntry[K]{
				Key:    pair.Key,
				Offset: ow.written,
			})
		}
		if err := encoder.Encode(pair); err != nil {
			return nil, err
		}

		keyBytes := []byte(any(pair.Key).(string))
		filter.Add(keyBytes)
	}

	if err := saveFilter(path+".filter", filter); err != nil {
		return nil, err
	}

	return &SSTable[K, V]{
		Path:        path,
		MinKey:      any(pairs[0].Key).(string),
		MaxKey:      any(pairs[len(pairs)-1].Key).(string),
		SparseIndex: sparseIndex,
		filter:      filter,
	}, nil
}

func (s *SSTable[K, V]) Get(key K) (V, error) {
	var zero V

	targetStr := fmt.Sprint(key)

	if targetStr < s.MinKey || targetStr > s.MaxKey {
		return zero, ErrNotFound
	}

	keyBytes := []byte(any(key).(string))

	// Fast Path: skip file opening and linear gob scanning if key is absent
	if s.filter != nil && !s.filter.Contains(keyBytes) {
		return zero, ErrNotFound
	}

	idx := sort.Search(len(s.SparseIndex), func(i int) bool {
		return fmt.Sprint(s.SparseIndex[i].Key) > targetStr
	}) - 1

	if idx < 0 {
		return zero, ErrNotFound
	}

	startOffset := s.SparseIndex[idx].Offset
	var endOffset int64 = -1
	if idx+1 < len(s.SparseIndex) {
		endOffset = s.SparseIndex[idx+1].Offset
	}

	file, err := os.Open(s.Path)
	if err != nil {
		return zero, err
	}
	defer file.Close()

	if _, err := file.Seek(startOffset, io.SeekStart); err != nil {
		return zero, err
	}

	var decoder *gob.Decoder
	if endOffset != -1 {
		blockSize := endOffset - startOffset
		// This limits gob from pulling bytes past the current block boundary
		limitedReader := io.LimitReader(file, blockSize)
		decoder = gob.NewDecoder(limitedReader)
	} else {
		// If it's the last block, read up to the physical End of File (EOF)
		decoder = gob.NewDecoder(file)
	}

	for {
		var pair Pair[K, V]
		if err := decoder.Decode(&pair); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return zero, err
		}

		keyInDB := fmt.Sprint(pair.Key)
		if keyInDB == targetStr {
			if fmt.Sprint(pair.Value) == Tombstone {
				return zero, ErrDeleted
			}
			return pair.Value, nil
		}

		// Since keys are strictly sorted, terminate early if we pass target
		if keyInDB > targetStr {
			break
		}
	}

	return zero, ErrNotFound
}
