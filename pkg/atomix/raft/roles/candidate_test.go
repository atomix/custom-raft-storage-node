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
	"testing"
	"time"

	raft "github.com/atomix/raft-storage/pkg/atomix/raft/protocol"
	"github.com/atomix/raft-storage/pkg/atomix/raft/protocol/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestCandidateVote(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	delayFailVote(client, 5*time.Second).AnyTimes()

	role := newTestRole(client, newCandidateRole, mockFollower(ctrl), mockLeader(ctrl)).(*CandidateRole)
	assert.NoError(t, role.raft.SetTerm(1))
	assert.NoError(t, role.Start())
	awaitTerm(role.raft, raft.Term(2))

	response, err := role.Vote(context.TODO(), &raft.VoteRequest{
		Term:         raft.Term(1),
		Candidate:    raft.MemberID("bar"),
		LastLogIndex: 1,
		LastLogTerm:  1,
	})
	assert.NoError(t, err)
	assert.False(t, response.Voted)
	assert.Equal(t, raft.Term(2), response.Term)

	role = newTestRole(client, newCandidateRole, mockFollower(ctrl), mockLeader(ctrl)).(*CandidateRole)
	assert.NoError(t, role.raft.SetTerm(1))
	assert.NoError(t, role.Start())
	awaitTerm(role.raft, raft.Term(2))

	response, err = role.Vote(context.TODO(), &raft.VoteRequest{
		Term:         raft.Term(3),
		Candidate:    raft.MemberID("bar"),
		LastLogIndex: 1,
		LastLogTerm:  1,
	})
	assert.NoError(t, err)
	assert.True(t, response.Voted)
	assert.Equal(t, raft.RoleFollower, awaitRole(role.raft, raft.RoleFollower))
	assert.Equal(t, raft.Term(3), awaitTerm(role.raft, raft.Term(3)))

	role = newTestRole(client, newCandidateRole, mockFollower(ctrl), mockLeader(ctrl)).(*CandidateRole)
	assert.NoError(t, role.raft.SetTerm(1))
	assert.NoError(t, role.Start())
	awaitTerm(role.raft, raft.Term(2))

	response, err = role.Vote(context.TODO(), &raft.VoteRequest{
		Term:         raft.Term(3),
		Candidate:    raft.MemberID("bar"),
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	assert.NoError(t, err)
	assert.True(t, response.Voted)
	assert.Equal(t, raft.RoleFollower, awaitRole(role.raft, raft.RoleFollower))
	assert.Equal(t, raft.Term(3), awaitTerm(role.raft, raft.Term(3)))
}

func TestCandidateVoteQuorum(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	acceptVote(client)
	rejectVote(client).AnyTimes()

	protocol, sm, stores := newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl))
	role := newCandidateRole(protocol, sm, stores).(*CandidateRole)
	assert.NoError(t, role.Start())
	assert.Equal(t, raft.RoleLeader, awaitRole(role.raft, raft.RoleLeader))
}

func TestCandidateVoteFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	rejectVote(client).AnyTimes()

	protocol, sm, stores := newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl))
	role := newCandidateRole(protocol, sm, stores).(*CandidateRole)
	assert.NoError(t, role.Start())
	assert.Equal(t, raft.RoleFollower, awaitRole(role.raft, raft.RoleFollower))
}

func TestCandidateVoteTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	delayFailVote(client, 5*time.Second).AnyTimes()

	protocol, sm, stores := newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl))
	role := newCandidateRole(protocol, sm, stores).(*CandidateRole)
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
