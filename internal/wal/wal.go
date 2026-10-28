package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rajnish-oss/LSM_storage_engine/internal/codec"
	"github.com/rajnish-oss/LSM_storage_engine/internal/memtable"
)

type SyncMode int

const (
	// SyncNone relies on OS page-cache flushing (highest throughput, risk of data loss on power cut).
	SyncNone SyncMode = iota
	// SyncAlways calls fsync after every single write (strict durability, high latency).
	SyncAlways
	// SyncBatch batches concurrent writes and issues a single amortized fsync.
	SyncBatch
)

type writeRequest struct {
	key         []byte
	value       []byte
	isTombstone bool
	errCh       chan error
}

type Options struct {
	Dir           string        // Directory storing the .wal segments
	SyncMode      SyncMode      // Durability level
	BatchInterval time.Duration // Max wait before syncing a batch in SyncBatch mode
	BatchSize     int           // Max pending requests before triggering immediate batch sync
}

type WALSegment struct {
	mu         sync.Mutex
	file       *os.File
	writer     *bufio.Writer
	segmentID  uint64
	path       string
	syncMode   SyncMode
	writeQueue chan writeRequest
	closeCh    chan struct{}
	wg         sync.WaitGroup
}

func NewSegment(opts Options, segmentID uint64) (*WALSegment, error) {
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create wal dir: %w", err)
	}

	filename := fmt.Sprintf("%05d.wal", segmentID)
	fullPath := filepath.Join(opts.Dir, filename)

	file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal segment: %w", err)
	}

	seg := &WALSegment{
		file:       file,
		writer:     bufio.NewWriterSize(file, 64*1024), // 64KB write buffer
		segmentID:  segmentID,
		path:       fullPath,
		syncMode:   opts.SyncMode,
		closeCh:    make(chan struct{}),
	}

	if opts.SyncMode == SyncBatch {
		batchSize := opts.BatchSize
		if batchSize <= 0 {
			batchSize = 256
		}
		interval := opts.BatchInterval
		if interval <= 0 {
			interval = 5 * time.Millisecond
		}

		seg.writeQueue = make(chan writeRequest, batchSize)
		seg.wg.Add(1)
		go seg.groupCommitLoop(interval, batchSize)
	}

	return seg, nil
}

func (s *WALSegment) Write(key, value []byte, isTombstone bool) error {
	if s.syncMode == SyncBatch {
		req := writeRequest{
			key:         key,
			value:       value,
			isTombstone: isTombstone,
			errCh:       make(chan error, 1),
		}
		select {
		case s.writeQueue <- req:
			return <-req.errCh
		case <-s.closeCh:
			return errors.New("wal segment closed")
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := codec.EncodeEntry(s.writer, key, value, isTombstone); err != nil {
		return err
	}

	if s.syncMode == SyncAlways {
		if err := s.writer.Flush(); err != nil {
			return err
		}
		return s.file.Sync()
	}

	return nil
}

func (s *WALSegment) groupCommitLoop(interval time.Duration, maxBatch int) {
	defer s.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	requests := make([]writeRequest, 0, maxBatch)

	flushBatch := func() {
		if len(requests) == 0 {
			return
		}

		s.mu.Lock()
		var writeErr error
		for _, req := range requests {
			if _, err := codec.EncodeEntry(s.writer, req.key, req.value, req.isTombstone); err != nil {
				writeErr = err
				break
			}
		}

		if writeErr == nil {
			if err := s.writer.Flush(); err != nil {
				writeErr = err
			} else {
				writeErr = s.file.Sync()
			}
		}
		s.mu.Unlock()

		for _, req := range requests {
			req.errCh <- writeErr
		}
		requests = requests[:0]
	}

	for {
		select {
		case req := <-s.writeQueue:
			requests = append(requests, req)
			if len(requests) >= maxBatch {
				flushBatch()
			}
		case <-ticker.C:
			flushBatch()
		case <-s.closeCh:
			// Drain remaining writes before terminating
			for {
				select {
				case req := <-s.writeQueue:
					requests = append(requests, req)
				default:
					flushBatch()
					return
				}
			}
		}
	}
}

func (s *WALSegment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.writer.Flush(); err != nil {
		return err
	}
	return s.file.Sync()
}

func (s *WALSegment) Close() error {
	if s.syncMode == SyncBatch {
		close(s.closeCh)
		s.wg.Wait()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return nil
	}

	var flushErr error
	if s.writer != nil {
		flushErr = s.writer.Flush()
	}

	syncErr := s.file.Sync()
	closeErr := s.file.Close()
	s.file = nil

	if flushErr != nil {
		return flushErr
	}
	if syncErr != nil && !errors.Is(syncErr, os.ErrClosed) {
		return syncErr
	}
	return closeErr
}

func (s *WALSegment) Remove() error {
	if err := s.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		return err
	}
	return os.Remove(s.path)
}

func (s *WALSegment) SegmentID() uint64 {
	return s.segmentID
}

func ReplaySegment(path string) (*memtable.MemTable, error) {
	m := memtable.NewMemTable()

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return m, nil
		}
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		entry, err := codec.DecodeEntry(reader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("corrupt wal record in %s: %w", filepath.Base(path), err)
		}

		if entry.IsTombstone {
			m.Delete(entry.Key)
		} else {
			m.Put(entry.Key, entry.Value)
		}
	}

	return m, nil
}


func DiscoverSegments(walDir string) ([]uint64, error) {
	entries, err := os.ReadDir(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var segmentIDs []uint64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".wal") {
			continue
		}
		base := strings.TrimSuffix(entry.Name(), ".wal")
		id, err := strconv.ParseUint(base, 10, 64)
		if err != nil {
			continue
		}
		segmentIDs = append(segmentIDs, id)
	}

	sort.Slice(segmentIDs, func(i, j int) bool {
		return segmentIDs[i] < segmentIDs[j]
	})

	return segmentIDs, nil
}