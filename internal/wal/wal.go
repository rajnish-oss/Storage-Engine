package wal

import (
	"encoding/gob"
	"github.com/rajnish-oss/LSM_storage_engine/internal/memtable"
	"io"
	"os"
)

type WAL[K comparable, V any] struct {
	file    *os.File
	encoder *gob.Encoder
}

type WALEntry[K comparable, V any] struct {
	Key   K
	Value V
}

func NewWAL[K comparable, V any](path string) (*WAL[K, V], error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL[K, V]{
		file:    file,
		encoder: gob.NewEncoder(file),
	}, nil
}

func (wal *WAL[K, V]) Write(key K, value V) error {
	entry := WALEntry[K, V]{Key: key, Value: value}
	return wal.encoder.Encode(&entry)
}

func ReplayWAL[K comparable, V any](path string) (*memtable.MemTable[K, V], error) {
	memtable := memtable.NewMemTable[K, V]()
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// If the file doesn't exist, that's fine. Return an empty memtable.
			return memtable, nil
		}
		return nil, err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	for {
		var entry WALEntry[K, V]
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break // We've reached the end of the file.
			}
			return nil, err
		}
		memtable.Put(entry.Key, entry.Value)
	}

	return memtable, nil
}
