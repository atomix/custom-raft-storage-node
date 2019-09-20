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
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	raft "github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/store"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/store/log"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/util"
	"time"
)

// NewManager returns a new Raft state manager
func NewManager(raft raft.Raft, store store.Store, registry *node.Registry) Manager {
	sm := &manager{
		raft:   raft,
		log:    util.NewNodeLogger(string(raft.Member())),
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
	ApplyEntry(entry *log.Entry, ch chan<- node.Output)

	// Close closes the state manager
	Close() error
}

const (
	// stateBufferSize is the size of the state manager's change channel buffer
	stateBufferSize = 1024
)

// manager manages the Raft state machine
type manager struct {
	raft         raft.Raft
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
	return string(m.raft.Member())
}

// applyIndex applies entries up to the given index
func (m *manager) ApplyIndex(index raft.Index) {
	m.ch <- &change{
		entry: &log.Entry{
			Index: index,
		},
	}
}

// applyEntry enqueues the given entry to be applied to the state machine, returning output on the given channel
func (m *manager) ApplyEntry(entry *log.Entry, ch chan<- node.Output) {
	m.ch <- &change{
		entry:  entry,
		result: ch,
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
	if change.entry.Entry != nil {
		// If the entry is a query, apply it without incrementing the lastApplied index
		if query, ok := change.entry.Entry.Entry.(*raft.LogEntry_Query); ok {
			m.execQuery(change.entry.Index, change.entry.Entry.Timestamp, query.Query, change.result)
		} else {
			m.execPendingChanges(change.entry.Index - 1)
			m.execEntry(change.entry, change.result)
			m.lastApplied = change.entry.Index
		}
	} else if change.entry.Index > m.lastApplied {
		m.execPendingChanges(change.entry.Index - 1)
		m.execEntry(change.entry, change.result)
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
func (m *manager) execEntry(entry *log.Entry, ch chan<- node.Output) {
	m.log.Trace("Applying %d", entry.Index)
	if entry.Entry == nil {
		m.reader.Reset(entry.Index)
		entry = m.reader.NextEntry()
	}

	switch e := entry.Entry.Entry.(type) {
	case *raft.LogEntry_Query:
		m.execQuery(entry.Index, entry.Entry.Timestamp, e.Query, ch)
	case *raft.LogEntry_Command:
		m.execCommand(entry.Index, entry.Entry.Timestamp, e.Command, ch)
	case *raft.LogEntry_Configuration:
		m.execConfig(entry.Index, entry.Entry.Timestamp, e.Configuration, ch)
	case *raft.LogEntry_Initialize:
		m.execInit(entry.Index, entry.Entry.Timestamp, e.Initialize, ch)
	}
}

func (m *manager) execInit(index raft.Index, timestamp time.Time, init *raft.InitializeEntry, ch chan<- node.Output) {
	m.updateClock(index, timestamp)
	if ch != nil {
		ch <- node.Output{}
		close(ch)
	}
}

func (m *manager) execConfig(index raft.Index, timestamp time.Time, config *raft.ConfigurationEntry, ch chan<- node.Output) {
	m.updateClock(index, timestamp)
	if ch != nil {
		ch <- node.Output{}
		close(ch)
	}
}

func (m *manager) execQuery(index raft.Index, timestamp time.Time, query *raft.QueryEntry, ch chan<- node.Output) {
	m.operation = service.OpTypeQuery
	m.state.Query(query.Value, ch)
}

func (m *manager) execCommand(index raft.Index, timestamp time.Time, command *raft.CommandEntry, ch chan<- node.Output) {
	m.updateClock(index, timestamp)
	m.operation = service.OpTypeCommand
	m.state.Command(command.Value, ch)
}

type change struct {
	entry  *log.Entry
	result chan<- node.Output
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
