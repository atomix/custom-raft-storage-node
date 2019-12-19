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
	"encoding/binary"
	"github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	streams "github.com/atomix/atomix-go-node/pkg/atomix/stream"
	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"io"
	"sync"
	"time"
)

// newStateMachine returns a new primitive state machine
func newStateMachine(cluster cluster.Cluster, registry *node.Registry, streams *streamManager) *StateMachine {
	fsm := &StateMachine{
		node:    cluster.MemberID,
		streams: streams,
	}
	fsm.state = node.NewPrimitiveStateMachine(registry, fsm)
	return fsm
}

type StateMachine struct {
	node      string
	state     node.StateMachine
	streams   *streamManager
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
	return s.timestamp
}

func (s *StateMachine) OperationType() service.OperationType {
	return s.operation
}

func (s *StateMachine) Apply(log *raft.Log) interface{} {
	entry := &Entry{}
	if err := proto.Unmarshal(log.Data, entry); err != nil {
		return err
	}

	var stream streams.Stream
	ch := s.streams.getStream(entry.StreamID)
	if ch != nil {
		stream = streams.NewChannelStream(ch)
	} else {
		stream = streams.NewNilStream()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.index = log.Index
	if entry.Timestamp.After(s.timestamp) {
		s.timestamp = entry.Timestamp
	}
	s.operation = service.OpTypeCommand
	s.state.Command(entry.Value, stream)
	return nil
}

func (s *StateMachine) Query(value []byte, ch chan<- streams.Result) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.operation = service.OpTypeQuery
	s.state.Query(value, streams.NewChannelStream(ch))
	return nil
}

func (s *StateMachine) Snapshot() (raft.FSMSnapshot, error) {
	return &stateMachineSnapshot{
		state: s,
	}, nil
}

func (s *StateMachine) Restore(reader io.ReadCloser) error {
	bytes := make([]byte, 4)
	if _, err := reader.Read(bytes); err != nil {
		return err
	}
	secs := int64(binary.BigEndian.Uint64(bytes))
	if _, err := reader.Read(bytes); err != nil {
		return err
	}
	nanos := int64(binary.BigEndian.Uint64(bytes))
	s.timestamp = time.Unix(secs, nanos)
	return s.state.Install(reader)
}

type stateMachineSnapshot struct {
	state *StateMachine
}

func (s *stateMachineSnapshot) Persist(sink raft.SnapshotSink) error {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint64(bytes, uint64(s.state.timestamp.Second()))
	if _, err := sink.Write(bytes); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(bytes, uint64(s.state.timestamp.Nanosecond()))
	if _, err := sink.Write(bytes); err != nil {
		return err
	}
	return s.state.state.Snapshot(&stateMachineSnapshotWriter{
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
