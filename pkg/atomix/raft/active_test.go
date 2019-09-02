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
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestActiveAppend(t *testing.T) {
	server := newTestServer()
	role := newActiveRole(server)

	// Test accepting the current term/leader
	server.term = 1
	server.leader = "bar"

	response, err := role.Append(context.TODO(), &AppendRequest{
		Term:         1,
		Leader:       "bar",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*RaftLogEntry{},
		CommitIndex:  0,
	})

	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.True(t, response.Succeeded)
	assert.Equal(t, Term(1), response.Term)
	assert.Equal(t, Term(1), server.term)
	assert.Equal(t, MemberID("bar"), server.leader)

	// Test updating the term/leader
	response, err = role.Append(context.TODO(), &AppendRequest{
		Term:         2,
		Leader:       "baz",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*RaftLogEntry{},
		CommitIndex:  0,
	})

	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.Equal(t, Term(2), response.Term)
	assert.True(t, response.Succeeded)
	assert.Equal(t, Term(2), server.term)
	assert.Equal(t, MemberID("baz"), server.leader)
}

func TestActivePoll(t *testing.T) {
	server := newTestServer()
	role := newActiveRole(server)

	// Test rejecting a poll for an old term
	server.term = 2
	server.leader = "bar"

	response, err := role.Poll(context.TODO(), &PollRequest{
		Term:         1,
		Candidate:    "baz",
		LastLogIndex: Index(0),
		LastLogTerm:  Term(0),
	})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.False(t, response.Accepted)
	assert.Equal(t, Term(2), response.Term)

	// Test that the node votes if there are no entries in its log
	response, err = role.Poll(context.TODO(), &PollRequest{
		Term:         2,
		Candidate:    "baz",
		LastLogIndex: 10,
		LastLogTerm:  2,
	})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.True(t, response.Accepted)
	assert.Equal(t, Term(2), response.Term)

	// Test that the poll is rejected if the LastLogTerm is lower than the log's last entry term
	server.writer.Append(&RaftLogEntry{
		Term:      Term(1),
		Timestamp: time.Now(),
		Entry: &RaftLogEntry_Initialize{
			Initialize: &InitializeEntry{},
		},
	})
	server.writer.Append(&RaftLogEntry{
		Term:      Term(2),
		Timestamp: time.Now(),
		Entry: &RaftLogEntry_Initialize{
			Initialize: &InitializeEntry{},
		},
	})
	server.writer.Append(&RaftLogEntry{
		Term:      Term(2),
		Timestamp: time.Now(),
		Entry: &RaftLogEntry_Initialize{
			Initialize: &InitializeEntry{},
		},
	})

	assert.Equal(t, Index(3), server.writer.LastIndex())

	response, err = role.Poll(context.TODO(), &PollRequest{
		Term:         2,
		Candidate:    "baz",
		LastLogIndex: 10,
		LastLogTerm:  1,
	})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.False(t, response.Accepted)
	assert.Equal(t, Term(2), response.Term)

	// Test that the poll is rejected if the candidate's last index is less than the local log's last index in the same term
	response, err = role.Poll(context.TODO(), &PollRequest{
		Term:         2,
		Candidate:    "baz",
		LastLogIndex: 2,
		LastLogTerm:  2,
	})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.False(t, response.Accepted)
	assert.Equal(t, Term(2), response.Term)

	// Test that the poll is accepted if the candidate's last index is greater than the local log's last index in an equal or greater term
	response, err = role.Poll(context.TODO(), &PollRequest{
		Term:         2,
		Candidate:    "baz",
		LastLogIndex: 10,
		LastLogTerm:  2,
	})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.True(t, response.Accepted)
	assert.Equal(t, Term(2), response.Term)

	response, err = role.Poll(context.TODO(), &PollRequest{
		Term:         3,
		Candidate:    "baz",
		LastLogIndex: 10,
		LastLogTerm:  3,
	})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.True(t, response.Accepted)
	assert.Equal(t, Term(3), response.Term)
	assert.Equal(t, Term(3), server.term)
}

func TestActiveVote(t *testing.T) {
	server := newTestServer()
	role := newActiveRole(server)

	// Test rejecting a vote request for an old term
	server.term = 2
	server.leader = "bar"

	response, err := role.Vote(context.TODO(), &VoteRequest{
		Term:         1,
		Candidate:    "baz",
		LastLogIndex: Index(0),
		LastLogTerm:  Term(0),
	})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.False(t, response.Voted)
	assert.Equal(t, Term(2), response.Term)

	// Test that the node rejects the vote request if it already has a leader for the requested term
	response, err = role.Vote(context.TODO(), &VoteRequest{
		Term:         2,
		Candidate:    "baz",
		LastLogIndex: Index(0),
		LastLogTerm:  Term(0),
	})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.False(t, response.Voted)
	assert.Equal(t, Term(2), response.Term)

	// Test that the node rejects the vote request if it's from an unknown member
	response, err = role.Vote(context.TODO(), &VoteRequest{
		Term:         2,
		Candidate:    "none",
		LastLogIndex: Index(0),
		LastLogTerm:  Term(0),
	})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.False(t, response.Voted)
	assert.Equal(t, Term(2), response.Term)

	// Test that the node votes if there are no entries in its log
	response, err = role.Vote(context.TODO(), &VoteRequest{
		Term:         3,
		Candidate:    "baz",
		LastLogIndex: 10,
		LastLogTerm:  2,
	})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.True(t, response.Voted)
	assert.Equal(t, Term(3), response.Term)
	assert.Equal(t, Term(3), server.term)
	assert.Equal(t, MemberID(""), server.leader)
	assert.Equal(t, MemberID("baz"), *server.lastVotedFor)

	// Reset the server state
	server.term = 2
	server.leader = "bar"
	server.lastVotedFor = nil

	// Test that the request is rejected if the LastLogTerm is lower than the log's last entry term
	server.writer.Append(&RaftLogEntry{
		Term:      Term(1),
		Timestamp: time.Now(),
		Entry: &RaftLogEntry_Initialize{
			Initialize: &InitializeEntry{},
		},
	})
	server.writer.Append(&RaftLogEntry{
		Term:      Term(2),
		Timestamp: time.Now(),
		Entry: &RaftLogEntry_Initialize{
			Initialize: &InitializeEntry{},
		},
	})
	server.writer.Append(&RaftLogEntry{
		Term:      Term(2),
		Timestamp: time.Now(),
		Entry: &RaftLogEntry_Initialize{
			Initialize: &InitializeEntry{},
		},
	})

	assert.Equal(t, Index(3), server.writer.LastIndex())

	response, err = role.Vote(context.TODO(), &VoteRequest{
		Term:         3,
		Candidate:    "baz",
		LastLogIndex: 10,
		LastLogTerm:  1,
	})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.False(t, response.Voted)
	assert.Equal(t, Term(3), response.Term)
	assert.Equal(t, Term(3), server.term)
	assert.Equal(t, MemberID(""), server.leader)
	assert.Nil(t, server.lastVotedFor)

	// Reset the server state
	server.term = 2
	server.leader = "bar"
	server.lastVotedFor = nil

	// Test that the request is rejected if the candidate's last index is less than the local log's last index in the same term
	response, err = role.Vote(context.TODO(), &VoteRequest{
		Term:         3,
		Candidate:    "baz",
		LastLogIndex: 2,
		LastLogTerm:  2,
	})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.False(t, response.Voted)
	assert.Equal(t, Term(3), response.Term)
	assert.Equal(t, Term(3), server.term)
	assert.Equal(t, MemberID(""), server.leader)
	assert.Nil(t, server.lastVotedFor)

	// Reset the server state
	server.term = 2
	server.leader = "bar"
	server.lastVotedFor = nil

	// Test that the vote is granted if the candidate's last index is greater than the local log's last index in an equal or greater term
	response, err = role.Vote(context.TODO(), &VoteRequest{
		Term:         3,
		Candidate:    "baz",
		LastLogIndex: 10,
		LastLogTerm:  2,
	})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.True(t, response.Voted)
	assert.Equal(t, Term(3), response.Term)
	assert.Equal(t, Term(3), server.term)
	assert.Equal(t, MemberID(""), server.leader)
	assert.Equal(t, MemberID("baz"), *server.lastVotedFor)

	// Reset the server state
	server.term = 2
	server.leader = "bar"
	server.lastVotedFor = nil

	response, err = role.Vote(context.TODO(), &VoteRequest{
		Term:         3,
		Candidate:    "baz",
		LastLogIndex: 10,
		LastLogTerm:  3,
	})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.True(t, response.Voted)
	assert.Equal(t, Term(3), response.Term)
	assert.Equal(t, Term(3), server.term)
	assert.Equal(t, MemberID(""), server.leader)
	assert.Equal(t, MemberID("baz"), *server.lastVotedFor)

	// Test that the node rejects any vote once it's already voted for a candidate
	response, err = role.Vote(context.TODO(), &VoteRequest{
		Term:         3,
		Candidate:    "bar",
		LastLogIndex: 10,
		LastLogTerm:  2,
	})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.False(t, response.Voted)

	response, err = role.Vote(context.TODO(), &VoteRequest{
		Term:         3,
		Candidate:    "bar",
		LastLogIndex: 10,
		LastLogTerm:  3,
	})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.False(t, response.Voted)
}
