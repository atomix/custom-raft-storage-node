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
	"fmt"
	"github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/config"
	"github.com/hashicorp/raft"
	"time"
)

// newRaft returns a new Raft instance
func newRaft(cluster cluster.Cluster, protocol *config.ProtocolConfig, fsm *StateMachine) (raft.ServerAddress, *raft.Raft, error) {
	member, ok := cluster.Members[cluster.MemberID]
	if !ok {
		panic("Local member is not present in cluster configuration!")
	}

	config := &raft.Config{
		ProtocolVersion:    raft.ProtocolVersionMax,
		HeartbeatTimeout:   protocol.GetElectionTimeoutOrDefault(),
		ElectionTimeout:    protocol.GetElectionTimeoutOrDefault(),
		CommitTimeout:      protocol.GetElectionTimeoutOrDefault(),
		MaxAppendEntries:   2,
		SnapshotInterval:   protocol.GetSnapshotIntervalOrDefault(),
		SnapshotThreshold:  protocol.GetSnapshotThresholdOrDefault(),
		LeaderLeaseTimeout: protocol.GetElectionTimeoutOrDefault(),
		LocalID:            raft.ServerID(cluster.MemberID),
	}

	store := raft.NewInmemStore()
	snaps := raft.NewInmemSnapshotStore()
	//dir, err := os.Getwd()
	//if err != nil {
	//	return nil, err
	//}
	//store, err := raftmdb.NewMDBStore(dir)
	//if err != nil {
	//	return nil, err
	//}
	//snaps, err := raft.NewFileSnapshotStore(dir, 1, nil)
	//if err != nil {
	//	return nil, err
	//}
	trans, err := raft.NewTCPTransport(fmt.Sprintf("%s:%d", member.Host, member.ProtocolPort), nil, 2, time.Second, nil)
	if err != nil {
		return "", nil, err
	}
	r, err := raft.NewRaft(config, fsm, store, store, snaps, trans)
	if err != nil {
		return "", nil, err
	}
	return trans.LocalAddr(), r, nil
}
