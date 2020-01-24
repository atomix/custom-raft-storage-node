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

package state

import (
	"github.com/atomix/go-framework/pkg/atomix/node"
	"github.com/atomix/go-framework/pkg/atomix/service"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	raft "github.com/atomix/raft-replica/pkg/atomix/raft/protocol"
	"github.com/atomix/raft-replica/pkg/atomix/raft/store"
	"github.com/atomix/raft-replica/pkg/atomix/raft/store/log"
	"github.com/atomix/raft-replica/pkg/atomix/raft/util"
	"time"
)

// NewManager returns a new Raft state manager
func NewManager(member raft.MemberID, store store.Store, registry *node.Registry) Manager {
	sm := &manager{
		member: member,
		log:    util.NewNodeLogger(string(member)),
		reader: store.Log().OpenReader(0),
		ch:     make(chan *change, stateBufferSize),
	}
	sm.state = node.NewPrimitiveStateMachine(registry, sm)
	go sm.start()
	return sm
}

// Manager provides a state machine to which to apply Raft log entries
type Manager interface {
	// ApplyIndex reads and applies the given index to the state machine
	ApplyIndex(index raft.Index)

	// Apply applies a committed entry to the state machine
	ApplyEntry(entry *log.Entry, stream streams.WriteStream)

	// Close closes the state manager
	Close() error
}

const (
	// stateBufferSize is the size of the state manager's change channel buffer
	stateBufferSize = 1024
)

// manager manages the Raft state machine
type manager struct {
	member       raft.MemberID
	state        node.StateMachine
	log          util.Logger
	currentIndex raft.Index
	currentTime  time.Time
	lastApplied  raft.Index
	reader       log.Reader
	operation    service.OperationType
	ch           chan *change
}

// Node returns the local node identifier
func (m *manager) Node() string {
	return string(m.member)
}

// applyIndex applies entries up to the given index
func (m *manager) ApplyIndex(index raft.Index) {
	m.ch <- &change{
		entry: &log.Entry{
			Index: index,
		},
	}
}

// ApplyEntry enqueues the given entry to be applied to the state machine, returning output on the given channel
func (m *manager) ApplyEntry(entry *log.Entry, stream streams.WriteStream) {
	m.ch <- &change{
		entry:  entry,
		stream: stream,
	}
}

func (m *manager) updateClock(index raft.Index, timestamp time.Time) {
	m.currentIndex = index
	if timestamp.UnixNano() > m.currentTime.UnixNano() {
		m.currentTime = timestamp
	}
}

// start begins applying entries to the state machine
func (m *manager) start() {
	for change := range m.ch {
		m.execChange(change)
	}
}

// execChange executes the given change on the state machine
func (m *manager) execChange(change *change) {
	defer func() {
		err := recover()
		if err != nil {
			m.log.Error("Recovered from panic %v", err)
		}
	}()
	if change.entry.Entry != nil {
		// If the entry is a query, apply it without incrementing the lastApplied index
		if query, ok := change.entry.Entry.Entry.(*raft.LogEntry_Query); ok {
			m.execQuery(change.entry.Index, change.entry.Entry.Timestamp, query.Query, change.stream)
		} else {
			m.execPendingChanges(change.entry.Index - 1)
			m.execEntry(change.entry, change.stream)
			m.lastApplied = change.entry.Index
		}
	} else if change.entry.Index > m.lastApplied {
		m.execPendingChanges(change.entry.Index - 1)
		m.execEntry(change.entry, change.stream)
		m.lastApplied = change.entry.Index
	}
}

// execPendingChanges reads and executes changes up to the given index
func (m *manager) execPendingChanges(index raft.Index) {
	if m.lastApplied < index {
		for m.lastApplied < index {
			entry := m.reader.NextEntry()
			if entry != nil {
				m.execEntry(entry, nil)
				m.lastApplied = entry.Index
			} else {
				return
			}
		}
	}
}

// execEntry applies the given entry to the state machine and returns the result(s) on the given channel
func (m *manager) execEntry(entry *log.Entry, stream streams.WriteStream) {
	if entry.Entry == nil {
		m.reader.Reset(entry.Index)
		entry = m.reader.NextEntry()
	}

	switch e := entry.Entry.Entry.(type) {
	case *raft.LogEntry_Query:
		m.execQuery(entry.Index, entry.Entry.Timestamp, e.Query, stream)
	case *raft.LogEntry_Command:
		m.log.Trace("Applying command %d", entry.Index)
		m.execCommand(entry.Index, entry.Entry.Timestamp, e.Command, stream)
	case *raft.LogEntry_Configuration:
		m.execConfig(entry.Index, entry.Entry.Timestamp, e.Configuration, stream)
	case *raft.LogEntry_Initialize:
		m.execInit(entry.Index, entry.Entry.Timestamp, e.Initialize, stream)
	}
}

func (m *manager) execInit(index raft.Index, timestamp time.Time, init *raft.InitializeEntry, stream streams.WriteStream) {
	m.updateClock(index, timestamp)
	if stream != nil {
		stream.Value(nil)
		stream.Close()
	}
}

func (m *manager) execConfig(index raft.Index, timestamp time.Time, config *raft.ConfigurationEntry, stream streams.WriteStream) {
	m.updateClock(index, timestamp)
	if stream != nil {
		stream.Value(nil)
		stream.Close()
	}
}

func (m *manager) execQuery(index raft.Index, timestamp time.Time, query *raft.QueryEntry, stream streams.WriteStream) {
	m.log.Trace("Applying query %d", index)
	m.operation = service.OpTypeQuery
	m.state.Query(query.Value, stream)
}

func (m *manager) execCommand(index raft.Index, timestamp time.Time, command *raft.CommandEntry, stream streams.WriteStream) {
	m.updateClock(index, timestamp)
	m.operation = service.OpTypeCommand
	m.state.Command(command.Value, stream)
}

type change struct {
	entry  *log.Entry
	stream streams.WriteStream
}

func (m *manager) Index() uint64 {
	return uint64(m.currentIndex)
}

func (m *manager) Timestamp() time.Time {
	return m.currentTime
}

func (m *manager) OperationType() service.OperationType {
	return m.operation
}

func (m *manager) Close() error {
	return nil
}
