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

package store

import (
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/store/log"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/store/snapshot"
)

// NewMemoryStore returns a new in-memory store
func NewMemoryStore() Store {
	log := log.NewMemoryLog()
	return &store{
		log:      log,
		reader:   log.OpenReader(0),
		writer:   log.Writer(),
		snapshot: snapshot.NewMemorySnapshotStore(),
	}
}

// Store provides storage interfaces for Raft state
type Store interface {
	// Log returns the Raft log
	Log() log.Log

	// Reader returns the primary Raft log reader
	Reader() log.LogReader

	// Writer returns the primary Raft log writer
	Writer() log.LogWriter

	// Snapshot returns the snapshot store
	Snapshot() snapshot.SnapshotStore

	// Close closes the store
	Close() error
}

// store is the default implementation of Store
type store struct {
	log      log.Log
	reader   log.LogReader
	writer   log.LogWriter
	snapshot snapshot.SnapshotStore
}

func (s *store) Log() log.Log {
	return s.log
}

func (s *store) Reader() log.LogReader {
	return s.reader
}

func (s *store) Writer() log.LogWriter {
	return s.writer
}

func (s *store) Snapshot() snapshot.SnapshotStore {
	return s.snapshot
}

func (s *store) Close() error {
	s.log.Close()
	s.snapshot.Close()
	return nil
}
