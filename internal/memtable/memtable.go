package memtable

import (
	"errors"
)

var (
	ErrMemTableFrozen = errors.New("memtable is frozen/immutable")
)

type Reader interface {
	Get(key []byte) (value []byte, deleted bool, found bool)
	Size() int64
	Iterator() *Iterator
}

type MemTable struct {
	list *SkipList
}

func NewMemTable() *MemTable {
	return &MemTable{
		list: NewSkipList(DefaultMaxLevel, DefaultProbability),
	}
}

func (m *MemTable) Put(key, value []byte) {
	m.list.Put(key, value, false)
}

func (m *MemTable) Delete(key []byte) {
	m.list.Put(key, nil, true)
}

func (m *MemTable) Get(key []byte) ([]byte, bool, bool) {
	return m.list.Get(key)
}

func (m *MemTable) Size() int64 {
	return m.list.SizeBytes()
}

func (m *MemTable) Iterator() *Iterator {
	return m.list.NewIterator()
}