package raft

import (
	"bytes"
	"io"
)

type SnapshotStore interface {
	newSnapshot(index int64, timestamp int64) Snapshot
	CurrentSnapshot() Snapshot
}

type Snapshot interface {
	Index() int64
	Timestamp() int64
	Reader() io.ReadCloser
	Writer() io.WriteCloser
}

func newMemorySnapshotStore() SnapshotStore {
	return &memorySnapshotStore{
		snapshots: make(map[int64]Snapshot),
	}
}

type memorySnapshotStore struct {
	snapshots       map[int64]Snapshot
	currentSnapshot Snapshot
}

func (s *memorySnapshotStore) newSnapshot(index int64, timestamp int64) Snapshot {
	snapshot := &memorySnapshot{
		index:     index,
		timestamp: timestamp,
		bytes:     make([]byte, 0, 1024*1024),
	}
	s.snapshots[index] = snapshot
	s.currentSnapshot = snapshot
	return snapshot
}

func (s *memorySnapshotStore) CurrentSnapshot() Snapshot {
	return s.currentSnapshot
}

type memorySnapshot struct {
	index     int64
	timestamp int64
	bytes     []byte
}

func (s *memorySnapshot) Index() int64 {
	return s.index
}

func (s *memorySnapshot) Timestamp() int64 {
	return s.timestamp
}

func (s *memorySnapshot) Reader() io.ReadCloser {
	return &memoryReader{
		reader: bytes.NewReader(s.bytes),
	}
}

func (s *memorySnapshot) Writer() io.WriteCloser {
	return &memoryWriter{
		writer: bytes.NewBuffer(s.bytes),
	}
}

type memoryReader struct {
	reader io.Reader
}

func (r *memoryReader) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}

func (r *memoryReader) Close() error {
	return nil
}

type memoryWriter struct {
	writer io.Writer
}

func (w *memoryWriter) Write(p []byte) (n int, err error) {
	return w.writer.Write(p)
}

func (w *memoryWriter) Close() error {
	return nil
}
