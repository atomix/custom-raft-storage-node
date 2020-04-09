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
	"testing"
	"time"

	raft "github.com/atomix/raft-storage/pkg/atomix/raft/protocol"
	"github.com/atomix/raft-storage/pkg/atomix/raft/protocol/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestFollowerPollQuorum(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	acceptPoll(client)
	rejectPoll(client).AnyTimes()
	acceptVote(client).AnyTimes()
	failAppend(client).AnyTimes()

	protocol, sm, stores := newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl))
	role := newFollowerRole(protocol, sm, stores).(*FollowerRole)
	assert.NoError(t, role.Start())
	role.raft.ReadLock()
	assert.Equal(t, raft.Term(0), role.raft.Term())
	assert.Nil(t, role.raft.Leader())
	role.raft.ReadUnlock()
	assert.Equal(t, raft.RoleCandidate, awaitRole(role.raft, raft.RoleCandidate))
}

func TestFollowerPollFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	rejectPoll(client).AnyTimes()
	acceptVote(client).AnyTimes()
	failAppend(client).AnyTimes()

	protocol, sm, stores := newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl))
	role := newFollowerRole(protocol, sm, stores).(*FollowerRole)
	assert.NoError(t, role.Start())
	role.raft.ReadLock()
	assert.Equal(t, raft.Term(0), role.raft.Term())
	assert.Nil(t, role.raft.Leader())
	role.raft.ReadUnlock()

	time.Sleep(5 * time.Second)
	assert.Equal(t, raft.RoleType(""), role.raft.Role())
}

func TestFollowerPollRestart(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	rejectPoll(client).Times(4)
	acceptPoll(client).AnyTimes()

	protocol, sm, stores := newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl))
	role := newFollowerRole(protocol, sm, stores).(*FollowerRole)
	assert.NoError(t, role.Start())
	role.raft.ReadLock()
	assert.Equal(t, raft.Term(0), role.raft.Term())
	assert.Nil(t, role.raft.Leader())
	role.raft.ReadUnlock()

	assert.Equal(t, raft.RoleCandidate, awaitRole(role.raft, raft.RoleCandidate))
}
