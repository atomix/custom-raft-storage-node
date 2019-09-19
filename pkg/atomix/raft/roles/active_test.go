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
	raft "github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/util"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestActiveAppend(t *testing.T) {
	protocol, sm, stores := newTestArgs()
	role := newActiveRole(protocol, sm, stores, util.NewNodeLogger(string(protocol.Member())))

	// Test accepting the current term/leader
	bar := raft.MemberID("bar")
	assert.NoError(t, role.raft.SetTerm(1))
	assert.NoError(t, role.raft.SetLeader(&bar))

	response, err := role.Append(context.TODO(), &raft.AppendRequest{
		Term:         1,
		Leader:       "bar",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*raft.RaftLogEntry{},
		CommitIndex:  0,
	})

	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.True(t, response.Succeeded)
	assert.Equal(t, raft.Term(1), response.Term)
	assert.Equal(t, raft.Term(1), role.raft.Term())
	assert.Equal(t, raft.MemberID("bar"), *role.raft.Leader())

	// Test updating the term/leader
	response, err = role.Append(context.TODO(), &raft.AppendRequest{
		Term:         2,
		Leader:       "baz",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*raft.RaftLogEntry{},
		CommitIndex:  0,
	})

	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.Equal(t, raft.Term(2), response.Term)
	assert.True(t, response.Succeeded)
	assert.Equal(t, raft.Term(2), role.raft.Term())
	assert.Equal(t, raft.MemberID("baz"), *role.raft.Leader())
}

func TestActivePoll(t *testing.T) {
	protocol, sm, stores := newTestArgs()
	role := newActiveRole(protocol, sm, stores, util.NewNodeLogger(string(protocol.Member())))

	// Test rejecting a poll for an old term
	bar := raft.MemberID("bar")
	assert.NoError(t, role.raft.SetTerm(2))
	assert.NoError(t, role.raft.SetLeader(&bar))

	response, err := role.Poll(context.TODO(), &raft.PollRequest{
		Term:         1,
		Candidate:    "baz",
		LastLogIndex: raft.Index(0),
		LastLogTerm:  raft.Term(0),
	})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.False(t, response.Accepted)
	assert.Equal(t, raft.Term(2), response.Term)

	// Test that the node votes if there are no entries in its log
	response, err = role.Poll(context.TODO(), &raft.PollRequest{
		Term:         2,
		Candidate:    "baz",
		LastLogIndex: 10,
		LastLogTerm:  2,
	})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.True(t, response.Accepted)
	assert.Equal(t, raft.Term(2), response.Term)

	// Test that the poll is rejected if the LastLogTerm is lower than the log's last entry term
	role.store.Writer().Append(&raft.RaftLogEntry{
		Term:      raft.Term(1),
		Timestamp: time.Now(),
		Entry: &raft.RaftLogEntry_Initialize{
			Initialize: &raft.InitializeEntry{},
		},
	})
	role.store.Writer().Append(&raft.RaftLogEntry{
		Term:      raft.Term(2),
		Timestamp: time.Now(),
		Entry: &raft.RaftLogEntry_Initialize{
			Initialize: &raft.InitializeEntry{},
		},
	})
	role.store.Writer().Append(&raft.RaftLogEntry{
		Term:      raft.Term(2),
		Timestamp: time.Now(),
		Entry: &raft.RaftLogEntry_Initialize{
			Initialize: &raft.InitializeEntry{},
		},
	})

	assert.Equal(t, raft.Index(3), role.store.Writer().LastIndex())

	response, err = role.Poll(context.TODO(), &raft.PollRequest{
		Term:         2,
		Candidate:    "baz",
		LastLogIndex: 10,
		LastLogTerm:  1,
	})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.False(t, response.Accepted)
	assert.Equal(t, raft.Term(2), response.Term)

	// Test that the poll is rejected if the candidate's last index is less than the local log's last index in the same term
	response, err = role.Poll(context.TODO(), &raft.PollRequest{
		Term:         2,
		Candidate:    "baz",
		LastLogIndex: 2,
		LastLogTerm:  2,
	})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.False(t, response.Accepted)
	assert.Equal(t, raft.Term(2), response.Term)

	// Test that the poll is accepted if the candidate's last index is greater than the local log's last index in an equal or greater term
	response, err = role.Poll(context.TODO(), &raft.PollRequest{
		Term:         2,
		Candidate:    "baz",
		LastLogIndex: 10,
		LastLogTerm:  2,
	})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.True(t, response.Accepted)
	assert.Equal(t, raft.Term(2), response.Term)

	response, err = role.Poll(context.TODO(), &raft.PollRequest{
		Term:         3,
		Candidate:    "baz",
		LastLogIndex: 10,
		LastLogTerm:  3,
	})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.True(t, response.Accepted)
	assert.Equal(t, raft.Term(3), response.Term)
	assert.Equal(t, raft.Term(3), role.raft.Term())
}

func TestActiveVote(t *testing.T) {
	protocol, sm, stores := newTestArgs()
	role := newActiveRole(protocol, sm, stores, util.NewNodeLogger(string(protocol.Member())))

	// Test rejecting a vote request for an old term
	bar := raft.MemberID("bar")
	assert.NoError(t, role.raft.SetTerm(2))
	assert.NoError(t, role.raft.SetLeader(&bar))

	response, err := role.Vote(context.TODO(), &raft.VoteRequest{
		Term:         1,
		Candidate:    "baz",
		LastLogIndex: raft.Index(0),
		LastLogTerm:  raft.Term(0),
	})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.False(t, response.Voted)
	assert.Equal(t, raft.Term(2), response.Term)

	// Test that the node rejects the vote request if it already has a leader for the requested term
	response, err = role.Vote(context.TODO(), &raft.VoteRequest{
		Term:         2,
		Candidate:    "baz",
		LastLogIndex: raft.Index(0),
		LastLogTerm:  raft.Term(0),
	})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.False(t, response.Voted)
	assert.Equal(t, raft.Term(2), response.Term)

	// Test that the node rejects the vote request if it's from an unknown member
	response, err = role.Vote(context.TODO(), &raft.VoteRequest{
		Term:         2,
		Candidate:    "none",
		LastLogIndex: raft.Index(0),
		LastLogTerm:  raft.Term(0),
	})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.False(t, response.Voted)
	assert.Equal(t, raft.Term(2), response.Term)

	// Test that the node votes if there are no entries in its log
	response, err = role.Vote(context.TODO(), &raft.VoteRequest{
		Term:         3,
		Candidate:    "baz",
		LastLogIndex: 10,
		LastLogTerm:  2,
	})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.True(t, response.Voted)
	assert.Equal(t, raft.Term(3), response.Term)
	assert.Equal(t, raft.Term(3), role.raft.Term())
	assert.Nil(t, role.raft.Leader())
	assert.Equal(t, raft.MemberID("baz"), *role.raft.LastVotedFor())

	// Reset the server state
	assert.NoError(t, role.raft.SetTerm(3))
	assert.NoError(t, role.raft.SetLeader(&bar))

	// Test that the request is rejected if the LastLogTerm is lower than the log's last entry term
	role.store.Writer().Append(&raft.RaftLogEntry{
		Term:      raft.Term(2),
		Timestamp: time.Now(),
		Entry: &raft.RaftLogEntry_Initialize{
			Initialize: &raft.InitializeEntry{},
		},
	})
	role.store.Writer().Append(&raft.RaftLogEntry{
		Term:      raft.Term(3),
		Timestamp: time.Now(),
		Entry: &raft.RaftLogEntry_Initialize{
			Initialize: &raft.InitializeEntry{},
		},
	})
	role.store.Writer().Append(&raft.RaftLogEntry{
		Term:      raft.Term(3),
		Timestamp: time.Now(),
		Entry: &raft.RaftLogEntry_Initialize{
			Initialize: &raft.InitializeEntry{},
		},
	})

	assert.Equal(t, raft.Index(3), role.store.Writer().LastIndex())

	response, err = role.Vote(context.TODO(), &raft.VoteRequest{
		Term:         4,
		Candidate:    "baz",
		LastLogIndex: 10,
		LastLogTerm:  1,
	})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.False(t, response.Voted)
	assert.Equal(t, raft.Term(4), response.Term)
	assert.Equal(t, raft.Term(4), role.raft.Term())
	assert.Nil(t, role.raft.Leader())
	assert.Nil(t, role.raft.LastVotedFor())

	// Test that the request is rejected if the candidate's last index is less than the local log's last index in the same term
	response, err = role.Vote(context.TODO(), &raft.VoteRequest{
		Term:         4,
		Candidate:    "baz",
		LastLogIndex: 2,
		LastLogTerm:  3,
	})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.False(t, response.Voted)
	assert.Equal(t, raft.Term(4), response.Term)
	assert.Equal(t, raft.Term(4), role.raft.Term())
	assert.Nil(t, role.raft.Leader())
	assert.Nil(t, role.raft.LastVotedFor())

	// Test that the vote is granted if the candidate's last index is greater than the local log's last index in an equal or greater term
	response, err = role.Vote(context.TODO(), &raft.VoteRequest{
		Term:         4,
		Candidate:    "baz",
		LastLogIndex: 10,
		LastLogTerm:  3,
	})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.True(t, response.Voted)
	assert.Equal(t, raft.Term(4), response.Term)
	assert.Equal(t, raft.Term(4), role.raft.Term())
	assert.Nil(t, role.raft.Leader())
	assert.Equal(t, raft.MemberID("baz"), *role.raft.LastVotedFor())

	response, err = role.Vote(context.TODO(), &raft.VoteRequest{
		Term:         5,
		Candidate:    "baz",
		LastLogIndex: 10,
		LastLogTerm:  5,
	})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.True(t, response.Voted)
	assert.Equal(t, raft.Term(5), response.Term)
	assert.Equal(t, raft.Term(5), role.raft.Term())
	assert.Nil(t, role.raft.Leader())
	assert.Equal(t, raft.MemberID("baz"), *role.raft.LastVotedFor())

	// Test that the node rejects any vote once it's already voted for a candidate
	response, err = role.Vote(context.TODO(), &raft.VoteRequest{
		Term:         5,
		Candidate:    "bar",
		LastLogIndex: 10,
		LastLogTerm:  3,
	})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.False(t, response.Voted)

	response, err = role.Vote(context.TODO(), &raft.VoteRequest{
		Term:         5,
		Candidate:    "bar",
		LastLogIndex: 10,
		LastLogTerm:  4,
	})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.False(t, response.Voted)
}
