package raft

import (
	"io"
	"sync"
)

// RaftLog provides for reading and writing entries in the Raft log
type RaftLog interface {
	io.Closer

	// Writer returns the Raft log writer
	Writer() RaftLogWriter

	// OpenReader opens a Raft log reader
	OpenReader(index int64) RaftLogReader
}

// RaftLogWriter supports writing entries to the Raft log
type RaftLogWriter interface {
	io.Closer

	// LastIndex returns the last index written to the log
	LastIndex() int64

	// LastEntry returns the last entry written to the log
	LastEntry() *IndexedEntry

	// Append appends the given entry to the log
	Append(entry *RaftLogEntry) *IndexedEntry

	// Reset resets the log writer to the given index
	Reset(index int64)

	// Truncate truncates the tail of the log to the given index
	Truncate(index int64)

	// Lock locks the writer
	Lock()

	// Unlock unlocks the writer
	Unlock()
}

// RaftLogReader supports reading of entries from the Raft log
type RaftLogReader interface {
	io.Closer

	// FirstIndex returns the first index in the log
	FirstIndex() int64

	// LastIndex returns the last index in the log
	LastIndex() int64

	// CurrentIndex returns the current index of the reader
	CurrentIndex() int64

	// CurrentEntry returns the current IndexedEntry
	CurrentEntry() *IndexedEntry

	// NextIndex returns the next index in the log
	NextIndex() int64

	// NextEntry advances the log index and returns the next entry in the log
	NextEntry() *IndexedEntry

	// Reset resets the log reader to the given index
	Reset(index int64)

	// Lock locks the reader
	Lock()

	// Unlock unlocks the reader
	Unlock()
}

// IndexedEntry is an indexed Raft log entry
type IndexedEntry struct {
	Index int64
	Entry *RaftLogEntry
}

func newMemoryLog() RaftLog {
	log := &memoryRaftLog{
		entries:    make([]*IndexedEntry, 0, 1024),
		firstIndex: 1,
	}
	log.writer = &memoryRaftLogWriter{
		log: log,
	}
	return log
}

type memoryRaftLog struct {
	entries    []*IndexedEntry
	firstIndex int64
	mu         sync.RWMutex
	writer     *memoryRaftLogWriter
}

func (l *memoryRaftLog) Writer() RaftLogWriter {
	return l.writer
}

func (l *memoryRaftLog) OpenReader(index int64) RaftLogReader {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var readerIndex int
	for i := 0; i < len(l.entries); i++ {
		if l.entries[i].Index == index {
			readerIndex = i - 1
			break
		}
	}
	return &memoryRaftLogReader{
		log:   l,
		index: readerIndex,
	}
}

func (l *memoryRaftLog) Close() error {
	return nil
}

type memoryRaftLogWriter struct {
	log *memoryRaftLog
}

func (w *memoryRaftLogWriter) LastIndex() int64 {
	if entry := w.LastEntry(); entry != nil {
		return entry.Index
	}
	return 0
}

func (w *memoryRaftLogWriter) LastEntry() *IndexedEntry {
	if len(w.log.entries) == 0 {
		return nil
	}
	return w.log.entries[len(w.log.entries)-1]
}

func (w *memoryRaftLogWriter) nextIndex() int64 {
	if len(w.log.entries) == 0 {
		return w.log.firstIndex
	}
	return w.log.entries[len(w.log.entries)-1].Index + 1
}

func (w *memoryRaftLogWriter) Append(entry *RaftLogEntry) *IndexedEntry {
	indexed := &IndexedEntry{
		Index: w.nextIndex(),
		Entry: entry,
	}
	w.log.entries = append(w.log.entries, indexed)
	return indexed
}

func (w *memoryRaftLogWriter) Reset(index int64) {
	w.log.entries = w.log.entries[:0]
	w.log.firstIndex = index
}

func (w *memoryRaftLogWriter) Truncate(index int64) {
	for i := 0; i < len(w.log.entries); i++ {
		if w.log.entries[i].Index >= index {
			w.log.entries = w.log.entries[:i]
		}
	}
}

func (w *memoryRaftLogWriter) Lock() {
	w.log.mu.Lock()
}

func (w *memoryRaftLogWriter) Unlock() {
	w.log.mu.Unlock()
}

func (w *memoryRaftLogWriter) Close() error {
	panic("implement me")
}

type memoryRaftLogReader struct {
	log   *memoryRaftLog
	index int
}

func (r *memoryRaftLogReader) FirstIndex() int64 {
	if len(r.log.entries) == 0 {
		return r.log.firstIndex - 1
	}
	return r.log.entries[0].Index
}

func (r *memoryRaftLogReader) LastIndex() int64 {
	if len(r.log.entries) == 0 {
		return r.log.firstIndex - 1
	}
	return r.log.entries[len(r.log.entries)-1].Index
}

func (r *memoryRaftLogReader) CurrentIndex() int64 {
	if len(r.log.entries) == 0 {
		return r.log.firstIndex - 1
	}
	return r.log.entries[r.index].Index
}

func (r *memoryRaftLogReader) CurrentEntry() *IndexedEntry {
	if len(r.log.entries) == 0 {
		return nil
	}
	return r.log.entries[r.index]
}

func (r *memoryRaftLogReader) NextIndex() int64 {
	if len(r.log.entries) == 0 {
		return r.log.firstIndex
	}
	return r.log.entries[r.index].Index + 1
}

func (r *memoryRaftLogReader) NextEntry() *IndexedEntry {
	if len(r.log.entries) >= r.index {
		r.index++
		return r.log.entries[r.index]
	}
	return nil
}

func (r *memoryRaftLogReader) Reset(index int64) {
	for i := 0; i < len(r.log.entries); i++ {
		if r.log.entries[i].Index >= index {
			r.index = i - 1
			break
		}
	}
}

func (r *memoryRaftLogReader) Lock() {
	r.log.mu.RLock()
}

func (r *memoryRaftLogReader) Unlock() {
	r.log.mu.RUnlock()
}

func (r *memoryRaftLogReader) Close() error {
	return nil
}
