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
	raft "github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestCandidatePollQuorum(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	acceptVote(client)
	rejectVote(client).AnyTimes()

	protocol, sm, stores := newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl))
	role := newCandidateRole(protocol, sm, stores)
	assert.NoError(t, role.Start())
	assert.Equal(t, raft.RoleLeader, awaitRole(role.raft, raft.RoleLeader))
}

func TestCandidatePollFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	rejectVote(client).AnyTimes()

	protocol, sm, stores := newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl))
	role := newCandidateRole(protocol, sm, stores)
	assert.NoError(t, role.Start())
	assert.Equal(t, raft.RoleFollower, awaitRole(role.raft, raft.RoleFollower))
}

func TestCandidateVoteTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	delayFailVote(client, 5*time.Second).AnyTimes()

	protocol, sm, stores := newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl))
	role := newCandidateRole(protocol, sm, stores)
	assert.NoError(t, role.Start())
	assert.Equal(t, raft.Term(1), awaitTerm(role.raft, raft.Term(1)))

	// Verify that the term is incremented and the candidate votes for itself
	role.raft.ReadLock()
	assert.Equal(t, raft.Term(1), role.raft.Term())
	assert.Nil(t, role.raft.Leader())
	assert.Equal(t, role.raft.Member(), *role.raft.LastVotedFor())
	role.raft.ReadUnlock()

	// Verify that the term is incremented and the candidate votes for itself again
	assert.Equal(t, raft.Term(2), awaitTerm(role.raft, raft.Term(2)))
	assert.Equal(t, raft.RoleType(""), role.raft.Role())
	assert.Equal(t, raft.Term(2), role.raft.Term())
	assert.Nil(t, role.raft.Leader())
	assert.Equal(t, role.raft.Member(), *role.raft.LastVotedFor())
}
