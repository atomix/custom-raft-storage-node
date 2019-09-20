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

package roles

import (
	"context"
	"github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/config"
	raft "github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/state"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/store"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func newTestState(client raft.Client) (raft.Raft, state.Manager, store.Store) {
	members := cluster.Cluster{
		MemberID: "foo",
		Members: map[string]cluster.Member{
			"foo": {
				ID:   "foo",
				Host: "localhost",
				Port: 5000,
			},
			"bar": {
				ID:   "bar",
				Host: "localhost",
				Port: 5001,
			},
			"baz": {
				ID:   "baz",
				Host: "localhost",
				Port: 5002,
			},
		},
	}
	raft := raft.NewRaft(raft.NewCluster(members), &config.ProtocolConfig{}, client)
	store := store.NewMemoryStore()
	state := state.NewManager(raft, store, node.GetRegistry())
	return raft, state, store
}

func TestRole(t *testing.T) {
	protocol, sm, stores := newTestState(&raft.UnimplementedClient{})
	role := newRaftRole(protocol, sm, stores, util.NewNodeLogger(string(protocol.Member())))

	joinResponse, err := role.Join(context.TODO(), &raft.JoinRequest{})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_ERROR, joinResponse.Status)

	leaveResponse, err := role.Leave(context.TODO(), &raft.LeaveRequest{})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_ERROR, leaveResponse.Status)

	configureResponse, err := role.Configure(context.TODO(), &raft.ConfigureRequest{})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_ERROR, configureResponse.Status)

	reconfigureResponse, err := role.Reconfigure(context.TODO(), &raft.ReconfigureRequest{})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_ERROR, reconfigureResponse.Status)

	pollResponse, err := role.Poll(context.TODO(), &raft.PollRequest{})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_ERROR, pollResponse.Status)

	voteResponse, err := role.Vote(context.TODO(), &raft.VoteRequest{})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_ERROR, voteResponse.Status)

	transferResponse, err := role.Transfer(context.TODO(), &raft.TransferRequest{})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_ERROR, transferResponse.Status)

	appendResponse, err := role.Append(context.TODO(), &raft.AppendRequest{})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_ERROR, appendResponse.Status)
}
