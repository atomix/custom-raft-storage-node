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
	"github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/hashicorp/raft"
	"io"
	"sync"
	"time"
)

// newStateMachine returns a new primitive state machine
func newStateMachine(cluster cluster.Cluster, registry *node.Registry) *StateMachine {
	fsm := &StateMachine{
		node: cluster.MemberID,
	}
	fsm.state = node.NewPrimitiveStateMachine(registry, fsm)
	return fsm
}

type StateMachine struct {
	node      string
	state     node.StateMachine
	index     uint64
	timestamp time.Time
	operation service.OperationType
	mu        sync.RWMutex
}

func (s *StateMachine) Node() string {
	return s.node
}

func (s *StateMachine) Index() uint64 {
	return s.index
}

func (s *StateMachine) Timestamp() time.Time {
	panic("implement me")
}

func (s *StateMachine) OperationType() service.OperationType {
	return s.operation
}

func (s *StateMachine) Apply(log *raft.Log) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.index = log.Index
	// TODO: Write the context timestamp
	s.operation = service.OpTypeCommand
	ch := make(chan node.Output)
	s.state.Command(log.Data, ch)
	return ch
}

func (s *StateMachine) Query(value []byte) chan node.Output {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.operation = service.OpTypeQuery
	ch := make(chan node.Output)
	s.state.Query(value, ch)
	return ch
}

func (s *StateMachine) Snapshot() (raft.FSMSnapshot, error) {
	return &stateMachineSnapshot{
		state: s.state,
	}, nil
}

func (s *StateMachine) Restore(reader io.ReadCloser) error {
	return s.state.Install(reader)
}

type stateMachineSnapshot struct {
	state node.StateMachine
}

func (s *stateMachineSnapshot) Persist(sink raft.SnapshotSink) error {
	return s.state.Snapshot(&stateMachineSnapshotWriter{
		sink: sink,
	})
}

func (s *stateMachineSnapshot) Release() {

}

type stateMachineSnapshotWriter struct {
	sink raft.SnapshotSink
}

func (w *stateMachineSnapshotWriter) Write(data []byte) (n int, err error) {
	return w.sink.Write(data)
}
