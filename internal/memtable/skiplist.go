package memtable

import (
	"bytes"
	"math/rand"
	"sync"
	"time"
)

const (
	DefaultMaxLevel    = 16
	DefaultProbability = 0.5
)

type Node struct {
	Key     []byte
	Value   []byte
	Deleted bool   // Tombstone marker for LSM deletes
	forward []*Node
}

func newNode(key, val []byte, deleted bool, level int) *Node {
	return &Node{
		Key:     key,
		Value:   val,
		Deleted: deleted,
		forward: make([]*Node, level),
	}
}

type SkipList struct {
	mu          sync.RWMutex
	head        *Node
	maxLevel    int
	level       int
	probability float64
	randSource  *rand.Rand
	sizeBytes   int64 // Tracks approximate in-memory size for flush checks
	count       int
}

func NewSkipList(maxLevel int, p float64) *SkipList {
	if maxLevel <= 0 {
		maxLevel = DefaultMaxLevel
	}
	if p <= 0 || p >= 1 {
		p = DefaultProbability
	}

	return &SkipList{
		head:        newNode(nil, nil, false, maxLevel),
		maxLevel:    maxLevel,
		level:       1,
		probability: p,
		randSource:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *SkipList) randomLevel() int {
	lvl := 1
	for lvl < s.maxLevel && s.randSource.Float64() < s.probability {
		lvl++
	}
	return lvl
}

func (s *SkipList) Put(key, val []byte, deleted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	update := make([]*Node, s.maxLevel)
	curr := s.head

	for i := s.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && bytes.Compare(curr.forward[i].Key, key) < 0 {
			curr = curr.forward[i]
		}
		update[i] = curr
	}

	curr = curr.forward[0]

	// Key already exists — update value and delta size
	if curr != nil && bytes.Equal(curr.Key, key) {
		delta := int64(len(val)) - int64(len(curr.Value))
		s.sizeBytes += delta
		curr.Value = val
		curr.Deleted = deleted
		return
	}

	// Key does not exist — insert new node
	lvl := s.randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}

	nn := newNode(key, val, deleted, lvl)
	for i := 0; i < lvl; i++ {
		nn.forward[i] = update[i].forward[i]
		update[i].forward[i] = nn
	}

	// Key length + Value length + Struct overhead + Pointer slice overhead
	s.sizeBytes += int64(len(key) + len(val) + (lvl * 8) + 32)
	s.count++
}

func (s *SkipList) Get(key []byte) ([]byte, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	curr := s.head
	for i := s.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && bytes.Compare(curr.forward[i].Key, key) < 0 {
			curr = curr.forward[i]
		}
	}

	curr = curr.forward[0]
	if curr != nil && bytes.Equal(curr.Key, key) {
		return curr.Value, curr.Deleted, true
	}

	return nil, false, false
}

func (s *SkipList) SizeBytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sizeBytes
}

func (s *SkipList) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.count
}

type Iterator struct {
	current *Node
}

func (s *SkipList) NewIterator() *Iterator {
	return &Iterator{current: s.head}
}

func (it *Iterator) HasNext() bool {
	return it.current != nil && it.current.forward[0] != nil
}

func (it *Iterator) Next() *Node {
	if !it.HasNext() {
		return nil
	}
	it.current = it.current.forward[0]
	return it.current
}