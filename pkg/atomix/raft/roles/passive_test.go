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
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestUpdateTermAndLeader(t *testing.T) {
	server := newTestServer()
	role := newPassiveRole(server)

	result := role.updateTermAndLeader(Term(1), MemberID("foo"))
	assert.True(t, result)
	assert.Equal(t, Term(1), server.term)
	assert.Equal(t, MemberID("foo"), server.leader)

	result = role.updateTermAndLeader(Term(1), MemberID("foo"))
	assert.False(t, result)
}

func TestPassiveAppend(t *testing.T) {
	server := newTestServer()
	role := newPassiveRole(server)

	// Test updating the term/leader
	response, err := role.Append(context.TODO(), &AppendRequest{
		Term:         2,
		Leader:       "bar",
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
	assert.Equal(t, MemberID("bar"), server.leader)

	// Test rejecting an old term/leader
	response, err = role.Append(context.TODO(), &AppendRequest{
		Term:         1,
		Leader:       "foo",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*RaftLogEntry{},
		CommitIndex:  0,
	})

	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.Equal(t, Term(2), response.Term)
	assert.False(t, response.Succeeded)

	// Test appending initial entries
	response, err = role.Append(context.TODO(), &AppendRequest{
		Term:         2,
		Leader:       "bar",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []*RaftLogEntry{
			{
				Term:      2,
				Timestamp: time.Now(),
				Entry: &RaftLogEntry_Initialize{
					Initialize: &InitializeEntry{},
				},
			},
			{
				Term:      2,
				Timestamp: time.Now(),
				Entry: &RaftLogEntry_Initialize{
					Initialize: &InitializeEntry{},
				},
			},
		},
		CommitIndex: 0,
	})

	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.Equal(t, Term(2), response.Term)
	assert.True(t, response.Succeeded)
	assert.Equal(t, Index(2), response.LastLogIndex)

	// Test committing entries
	response, err = role.Append(context.TODO(), &AppendRequest{
		Term:         2,
		Leader:       "bar",
		PrevLogIndex: 2,
		PrevLogTerm:  2,
		Entries:      []*RaftLogEntry{},
		CommitIndex:  1,
	})

	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.Equal(t, Term(2), response.Term)
	assert.True(t, response.Succeeded)
	assert.Equal(t, Index(1), server.commitIndex)

	// Test rejecting a request due to missing entries
	response, err = role.Append(context.TODO(), &AppendRequest{
		Term:         2,
		Leader:       "bar",
		PrevLogIndex: 3,
		PrevLogTerm:  2,
		Entries: []*RaftLogEntry{
			{
				Term:      2,
				Timestamp: time.Now(),
				Entry: &RaftLogEntry_Initialize{
					Initialize: &InitializeEntry{},
				},
			},
		},
		CommitIndex: 1,
	})

	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.Equal(t, Term(2), response.Term)
	assert.False(t, response.Succeeded)
	assert.Equal(t, Index(2), response.LastLogIndex)

	// Test rejecting entries for an inconsistent term
	response, err = role.Append(context.TODO(), &AppendRequest{
		Term:         3,
		Leader:       "baz",
		PrevLogIndex: 2,
		PrevLogTerm:  3,
		Entries: []*RaftLogEntry{
			{
				Term:      2,
				Timestamp: time.Now(),
				Entry: &RaftLogEntry_Initialize{
					Initialize: &InitializeEntry{},
				},
			},
		},
		CommitIndex: 1,
	})

	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.Equal(t, Term(3), response.Term)
	assert.False(t, response.Succeeded)
	assert.Equal(t, Index(1), response.LastLogIndex)
	assert.Equal(t, Term(3), server.term)
	assert.Equal(t, MemberID("baz"), server.leader)

	// Test replacing entries from a prior term
	response, err = role.Append(context.TODO(), &AppendRequest{
		Term:         3,
		Leader:       "baz",
		PrevLogIndex: 1,
		PrevLogTerm:  2,
		Entries: []*RaftLogEntry{
			{
				Term:      3,
				Timestamp: time.Now(),
				Entry: &RaftLogEntry_Initialize{
					Initialize: &InitializeEntry{},
				},
			},
			{
				Term:      3,
				Timestamp: time.Now(),
				Entry: &RaftLogEntry_Initialize{
					Initialize: &InitializeEntry{},
				},
			},
		},
		CommitIndex: 3,
	})

	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_OK, response.Status)
	assert.Equal(t, Term(3), response.Term)
	assert.True(t, response.Succeeded)
	assert.Equal(t, Index(3), response.LastLogIndex)
}
