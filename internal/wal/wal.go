package wal

import (
	"errors"
	"io"
	"os"
	"sync"

	"github.com/rajnish-oss/LSM_storage_engine/internal/codec"
	"github.com/rajnish-oss/LSM_storage_engine/internal/memtable"
)

type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
}

func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{
		file: file,
		path: path,
	}, nil
}

func (w *WAL) Write(key, value []byte, isTombstone bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	_, err := codec.EncodeEntry(w.file, key, value, isTombstone)
	return err
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.file.Sync()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		err := w.file.Close()
		w.file = nil
		return err
	}
	return nil
}

func (w *WAL) Remove() error {
	if err := w.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		return err
	}
	return os.Remove(w.path)
}

func ReplayWAL(path string) (*memtable.MemTable, error) {
	m := memtable.NewMemTable()

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return m, nil
		}
		return nil, err
	}
	defer file.Close()

	for {
		entry, err := codec.DecodeEntry(file)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		if entry.IsTombstone {
			m.Delete(entry.Key)
		} else {
			m.Put(entry.Key, entry.Value)
		}
	}

	return m, nil
}