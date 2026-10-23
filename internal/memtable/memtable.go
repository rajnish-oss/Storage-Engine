package memtable

type MemTable[K comparable, V any] struct {
	Data map[K]V
}

func NewMemTable[K comparable, V any]() *MemTable[K, V] {
	return &MemTable[K, V]{
		Data: make(map[K]V),
	}
}

func (m *MemTable[K, V]) Put(key K, value V) {
	m.Data[key] = value
}

func (m *MemTable[K, V]) Get(key K) (V, bool) {
	value, ok := m.Data[key]
	var zero V
	if !ok {
		return zero, false
	}
	return value, true
}
