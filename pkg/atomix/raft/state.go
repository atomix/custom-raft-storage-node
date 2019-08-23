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
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	log "github.com/sirupsen/logrus"
	"time"
)

const (
	stateBufferSize = 1024
)

// newStateManager returns a new Raft state manager
func newStateManager(server *RaftServer, registry *service.ServiceRegistry) *stateManager {
	sm := &stateManager{
		server: server,
		reader: server.log.OpenReader(0),
		ch:     make(chan *change, stateBufferSize),
	}
	sm.state = service.NewPrimitiveStateMachine(registry, sm)
	return sm
}

// stateManager manages the Raft state machine
type stateManager struct {
	server       *RaftServer
	state        service.StateMachine
	currentIndex Index
	currentTime  time.Time
	lastApplied  Index
	reader       RaftLogReader
	operation    service.OperationType
	ch           chan *change
}

// applyIndex applies entries up to the given index
func (m *stateManager) applyIndex(index Index) {
	m.ch <- &change{
		entry: &IndexedEntry{
			Index: index,
		},
	}
}

// applyEntry enqueues the given entry to be applied to the state machine, returning output on the given channel
func (m *stateManager) applyEntry(entry *IndexedEntry, ch chan service.Output) {
	m.ch <- &change{
		entry:  entry,
		result: ch,
	}
}

func (m *stateManager) updateClock(index Index, timestamp time.Time) {
	m.currentIndex = index
	if timestamp.UnixNano() > m.currentTime.UnixNano() {
		m.currentTime = timestamp
	}
}

// start begins applying entries to the state machine
func (m *stateManager) start() {
	for change := range m.ch {
		m.execChange(change)
	}
}

// execChange executes the given change on the state machine
func (m *stateManager) execChange(change *change) {
	if change.entry.Index > m.lastApplied {
		m.execPendingChanges(change.entry.Index - 1)
		m.execEntry(change.entry, change.result)
		m.lastApplied = change.entry.Index
	} else if query, ok := change.entry.Entry.Entry.(*RaftLogEntry_Query); ok {
		m.execQuery(change.entry.Index, change.entry.Entry.Timestamp, query.Query, change.result)
	}
}

// execPendingChanges reads and executes changes up to the given index
func (m *stateManager) execPendingChanges(index Index) {
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
func (m *stateManager) execEntry(entry *IndexedEntry, ch chan service.Output) {
	log.WithField("memberID", m.server.cluster.member).Tracef("Applying %d", entry.Index)
	if entry.Entry == nil {
		m.reader.Reset(entry.Index)
		entry = m.reader.NextEntry()
	}

	switch e := entry.Entry.Entry.(type) {
	case *RaftLogEntry_Query:
		m.execQuery(entry.Index, entry.Entry.Timestamp, e.Query, ch)
	case *RaftLogEntry_Command:
		m.execCommand(entry.Index, entry.Entry.Timestamp, e.Command, ch)
	case *RaftLogEntry_Configuration:
		m.execConfig(entry.Index, entry.Entry.Timestamp, e.Configuration, ch)
	case *RaftLogEntry_Initialize:
		m.execInit(entry.Index, entry.Entry.Timestamp, e.Initialize, ch)
	}
}

func (m *stateManager) execInit(index Index, timestamp time.Time, init *InitializeEntry, ch chan service.Output) {
	m.updateClock(index, timestamp)
	if ch != nil {
		ch <- service.Output{}
		close(ch)
	}
}

func (m *stateManager) execConfig(index Index, timestamp time.Time, config *ConfigurationEntry, ch chan service.Output) {
	m.updateClock(index, timestamp)
	if ch != nil {
		ch <- service.Output{}
		close(ch)
	}
}

func (m *stateManager) execQuery(index Index, timestamp time.Time, query *QueryEntry, ch chan service.Output) {
	m.operation = service.OpTypeQuery
	m.state.Query(query.Value, ch)
}

func (m *stateManager) execCommand(index Index, timestamp time.Time, command *CommandEntry, ch chan service.Output) {
	m.updateClock(index, timestamp)
	m.operation = service.OpTypeCommand
	m.state.Command(command.Value, ch)
}

type change struct {
	entry  *IndexedEntry
	result chan service.Output
}

func (m *stateManager) Index() uint64 {
	return uint64(m.currentIndex)
}

func (m *stateManager) Timestamp() time.Time {
	return m.currentTime
}

func (m *stateManager) OperationType() service.OperationType {
	return m.operation
}
