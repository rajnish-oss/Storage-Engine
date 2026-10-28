package sstable

import (
	"bytes"
	"errors"
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

type SparseEntry struct {
	Key    []byte
	Offset int64
}

type SSTable struct {
	Path string

	MinKey 		[]byte
	MaxKey      []byte

	SparseIndex []SparseEntry

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
	// Bloom filter can be marshaled directly or encoded via binary bytes
	data, err := bf.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	return err
}

func loadFilter(path string) (*filter.BloomFilter, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var bf filter.BloomFilter
	if err := bf.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return &bf, nil
}

func WriteSSTable(m *memtable.MemTable, path string, samplingInterval int) (*SSTable, error) {
	if m.Size() == 0 {
		return nil, EmptyMemtable
	}

	if samplingInterval <= 0 {
		samplingInterval = 16
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	ow := &offsetWriter{w: file}
	bf := filter.NewBloomFilter(int(m.Size()), 0.01)

	var sparseIndex []SparseEntry
	var minKey, maxKey []byte

	it := m.Iterator()
	idx := 0

	for it.HasNext() {
		node := it.Next()

		if idx == 0 {
			minKey = make([]byte, len(node.Key))
			copy(minKey, node.Key)
		}

		maxKey = node.Key

		if idx%samplingInterval == 0 {
			keyCopy := make([]byte, len(node.Key))
			copy(keyCopy, node.Key)
			sparseIndex = append(sparseIndex, SparseEntry{
				Key:    keyCopy,
				Offset: ow.written,
			})
		}

		if _, err := codec.EncodeEntry(ow, node.Key, node.Value, node.Deleted); err != nil {
			return nil, err
		}

		bf.Add(node.Key)
		idx++
	}

	finalMaxKey := make([]byte, len(maxKey))
	copy(finalMaxKey, maxKey)

	if err := saveFilter(path+".filter", bf); err != nil {
		return nil, err
	}

	return &SSTable{
		Path:        path,
		MinKey:      minKey,
		MaxKey:      finalMaxKey,
		SparseIndex: sparseIndex,
		filter:      bf,
	}, nil
}

func (s *SSTable) Get(key []byte) ([]byte, error) {
	if bytes.Compare(key, s.MinKey) < 0 || bytes.Compare(key, s.MaxKey) > 0 {
		return nil, ErrNotFound
	}

	if s.filter != nil && !s.filter.Contains(key) {
		return nil, ErrNotFound
	}

	idx := sort.Search(len(s.SparseIndex), func(i int) bool {
		return bytes.Compare(s.SparseIndex[i].Key, key) > 0
	}) - 1

	if idx < 0 {
		idx = 0
	}

	startOffset := s.SparseIndex[idx].Offset
	var endOffset int64 = -1
	if idx+1 < len(s.SparseIndex) {
		endOffset = s.SparseIndex[idx+1].Offset
	}

	file, err := os.Open(s.Path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if _, err := file.Seek(startOffset, io.SeekStart); err != nil {
		return nil, err
	}

	var reader io.Reader = file
	if endOffset != -1 {
		blockSize := endOffset - startOffset
		reader = io.LimitReader(file, blockSize)
	}

	for {
		entry, err := codec.DecodeEntry(reader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		if bytes.Equal(entry.Key, key) {
			if entry.IsTombstone {
				return nil, ErrDeleted
			}
			return entry.Value, nil
		}

		if bytes.Compare(entry.Key, key) > 0 {
			break
		}
	}

	return nil, ErrNotFound
}
