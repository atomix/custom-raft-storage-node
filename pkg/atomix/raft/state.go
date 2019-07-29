package raft

import (
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"time"
)

// newStateManager returns a new Raft state manager
func newStateManager(raft *RaftServer, registry *service.ServiceRegistry) *stateManager {
	sm := &stateManager{
		reader: raft.log.OpenReader(0),
		ch:     make(chan *change),
	}
	sm.state = service.NewPrimitiveStateMachine(registry, sm)
	return sm
}

// stateManager manages the Raft state machine
type stateManager struct {
	state        service.StateMachine
	currentIndex int64
	currentTime  time.Time
	lastApplied  int64
	reader       RaftLogReader
	operation    service.OperationType
	ch           chan *change
}

// applyEntry enqueues the given entry to be applied to the state machine, returning output on the given channel
func (m *stateManager) applyEntry(entry *IndexedEntry, ch chan service.Output) {
	m.ch <- &change{
		entry:  entry,
		result: ch,
	}
}

func (m *stateManager) updateClock(index int64, timestamp int64) {
	m.currentIndex = index
	if timestamp > m.currentTime.UnixNano() {
		m.currentTime = time.Unix(0, timestamp)
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
	m.execPendingChanges(change.entry.Index - 1)
	m.execEntry(change.entry, change.result)
	m.lastApplied = change.entry.Index
}

// execPendingChanges reads and executes changes up to the given index
func (m *stateManager) execPendingChanges(index int64) {
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

// stop stops applying entries to the state machine
func (m *stateManager) stop() {
	close(m.ch)
}

// execEntry applies the given entry to the state machine and returns the result(s) on the given channel
func (m *stateManager) execEntry(entry *IndexedEntry, ch chan service.Output) {
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

func (m *stateManager) execInit(index int64, timestamp int64, init *InitializeEntry, ch chan service.Output) {
	m.updateClock(index, timestamp)
	if ch != nil {
		ch <- service.Output{}
		close(ch)
	}
}

func (m *stateManager) execConfig(index int64, timestamp int64, config *ConfigurationEntry, ch chan service.Output) {
	m.updateClock(index, timestamp)
	if ch != nil {
		ch <- service.Output{}
		close(ch)
	}
}

func (m *stateManager) execQuery(index int64, timestamp int64, query *QueryEntry, ch chan service.Output) {
	m.operation = service.OpTypeQuery
	m.state.Query(query.Value, ch)
}

func (m *stateManager) execCommand(index int64, timestamp int64, command *CommandEntry, ch chan service.Output) {
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
