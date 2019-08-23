// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"io"
)

// RaftLog provides for reading and writing entries in the Raft log
type RaftLog interface {
	io.Closer

	// Writer returns the Raft log writer
	Writer() RaftLogWriter

	// OpenReader opens a Raft log reader
	OpenReader(index Index) RaftLogReader
}

// RaftLogWriter supports writing entries to the Raft log
type RaftLogWriter interface {
	io.Closer

	// LastIndex returns the last index written to the log
	LastIndex() Index

	// LastEntry returns the last entry written to the log
	LastEntry() *IndexedEntry

	// Append appends the given entry to the log
	Append(entry *RaftLogEntry) *IndexedEntry

	// Reset resets the log writer to the given index
	Reset(index Index)

	// Truncate truncates the tail of the log to the given index
	Truncate(index Index)
}

// RaftLogReader supports reading of entries from the Raft log
type RaftLogReader interface {
	io.Closer

	// FirstIndex returns the first index in the log
	FirstIndex() Index

	// LastIndex returns the last index in the log
	LastIndex() Index

	// CurrentIndex returns the current index of the reader
	CurrentIndex() Index

	// CurrentEntry returns the current IndexedEntry
	CurrentEntry() *IndexedEntry

	// NextIndex returns the next index in the log
	NextIndex() Index

	// NextEntry advances the log index and returns the next entry in the log
	NextEntry() *IndexedEntry

	// Reset resets the log reader to the given index
	Reset(index Index)
}

// IndexedEntry is an indexed Raft log entry
type IndexedEntry struct {
	Index Index
	Entry *RaftLogEntry
}

func newMemoryLog() RaftLog {
	log := &memoryRaftLog{
		entries:    make([]*IndexedEntry, 0, 1024),
		firstIndex: 1,
		readers:    make([]*memoryRaftLogReader, 0, 10),
	}
	log.writer = &memoryRaftLogWriter{
		log: log,
	}
	return log
}

type memoryRaftLog struct {
	entries    []*IndexedEntry
	firstIndex Index
	writer     *memoryRaftLogWriter
	readers    []*memoryRaftLogReader
}

func (l *memoryRaftLog) Writer() RaftLogWriter {
	return l.writer
}

func (l *memoryRaftLog) OpenReader(index Index) RaftLogReader {
	readerIndex := -1
	for i := 0; i < len(l.entries); i++ {
		if l.entries[i].Index == index {
			readerIndex = i - 1
			break
		}
	}
	reader := &memoryRaftLogReader{
		log:   l,
		index: readerIndex,
	}
	l.readers = append(l.readers, reader)
	return reader
}

func (l *memoryRaftLog) Close() error {
	return nil
}

type memoryRaftLogWriter struct {
	log *memoryRaftLog
}

func (w *memoryRaftLogWriter) LastIndex() Index {
	if entry := w.LastEntry(); entry != nil {
		return entry.Index
	}
	return w.log.firstIndex - 1
}

func (w *memoryRaftLogWriter) LastEntry() *IndexedEntry {
	if len(w.log.entries) == 0 {
		return nil
	}
	return w.log.entries[len(w.log.entries)-1]
}

func (w *memoryRaftLogWriter) nextIndex() Index {
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

func (w *memoryRaftLogWriter) Reset(index Index) {
	w.log.entries = w.log.entries[:0]
	w.log.firstIndex = index
	for _, reader := range w.log.readers {
		reader.maybeReset()
	}
}

func (w *memoryRaftLogWriter) Truncate(index Index) {
	for i := 0; i < len(w.log.entries); i++ {
		if w.log.entries[i].Index > index {
			w.log.entries = w.log.entries[:i]
			break
		}
	}
	for _, reader := range w.log.readers {
		reader.maybeReset()
	}
}

func (w *memoryRaftLogWriter) Close() error {
	panic("implement me")
}

type memoryRaftLogReader struct {
	log   *memoryRaftLog
	index int
}

func (r *memoryRaftLogReader) FirstIndex() Index {
	return r.log.firstIndex
}

func (r *memoryRaftLogReader) LastIndex() Index {
	if len(r.log.entries) == 0 {
		return r.log.firstIndex - 1
	}
	return r.log.entries[len(r.log.entries)-1].Index
}

func (r *memoryRaftLogReader) CurrentIndex() Index {
	if r.index == -1 || len(r.log.entries) == 0 {
		return r.log.firstIndex - 1
	}
	return r.log.entries[r.index].Index
}

func (r *memoryRaftLogReader) CurrentEntry() *IndexedEntry {
	if r.index == -1 || len(r.log.entries) == 0 {
		return nil
	}
	return r.log.entries[r.index]
}

func (r *memoryRaftLogReader) NextIndex() Index {
	if r.index == -1 || len(r.log.entries) == 0 {
		return r.log.firstIndex
	}
	return r.log.entries[r.index].Index + 1
}

func (r *memoryRaftLogReader) NextEntry() *IndexedEntry {
	if len(r.log.entries) > r.index+1 {
		r.index++
		return r.log.entries[r.index]
	}
	return nil
}

func (r *memoryRaftLogReader) Reset(index Index) {
	for i := 0; i < len(r.log.entries); i++ {
		if r.log.entries[i].Index >= index {
			r.index = i - 1
			break
		}
	}
}

func (r *memoryRaftLogReader) maybeReset() {
	if r.index >= 0 && len(r.log.entries) <= r.index {
		r.index = len(r.log.entries) - 1
	}
}

func (r *memoryRaftLogReader) Close() error {
	return nil
}
