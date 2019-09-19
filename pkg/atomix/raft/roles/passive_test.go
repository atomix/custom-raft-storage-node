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

func TestUpdateTermAndLeader(t *testing.T) {
	protocol, sm, stores := newTestArgs()
	role := newPassiveRole(protocol, sm, stores, util.NewNodeLogger(string(protocol.Member())))

	foo := raft.MemberID("foo")
	result := role.updateTermAndLeader(raft.Term(1), &foo)
	assert.True(t, result)
	assert.Equal(t, raft.Term(1), role.raft.Term())
	assert.Equal(t, &foo, role.raft.Leader())

	result = role.updateTermAndLeader(raft.Term(1), &foo)
	assert.False(t, result)
}

func TestPassiveAppend(t *testing.T) {
	protocol, sm, stores := newTestArgs()
	role := newPassiveRole(protocol, sm, stores, util.NewNodeLogger(string(protocol.Member())))

	// Test updating the term/leader
	response, err := role.Append(context.TODO(), &raft.AppendRequest{
		Term:         2,
		Leader:       "bar",
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
	assert.Equal(t, raft.MemberID("bar"), *role.raft.Leader())

	// Test rejecting an old term/leader
	response, err = role.Append(context.TODO(), &raft.AppendRequest{
		Term:         1,
		Leader:       "foo",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*raft.RaftLogEntry{},
		CommitIndex:  0,
	})

	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.Equal(t, raft.Term(2), response.Term)
	assert.False(t, response.Succeeded)

	// Test appending initial entries
	response, err = role.Append(context.TODO(), &raft.AppendRequest{
		Term:         2,
		Leader:       "bar",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []*raft.RaftLogEntry{
			{
				Term:      2,
				Timestamp: time.Now(),
				Entry: &raft.RaftLogEntry_Initialize{
					Initialize: &raft.InitializeEntry{},
				},
			},
			{
				Term:      2,
				Timestamp: time.Now(),
				Entry: &raft.RaftLogEntry_Initialize{
					Initialize: &raft.InitializeEntry{},
				},
			},
		},
		CommitIndex: 0,
	})

	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.Equal(t, raft.Term(2), response.Term)
	assert.True(t, response.Succeeded)
	assert.Equal(t, raft.Index(2), response.LastLogIndex)

	// Test committing entries
	response, err = role.Append(context.TODO(), &raft.AppendRequest{
		Term:         2,
		Leader:       "bar",
		PrevLogIndex: 2,
		PrevLogTerm:  2,
		Entries:      []*raft.RaftLogEntry{},
		CommitIndex:  1,
	})

	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.Equal(t, raft.Term(2), response.Term)
	assert.True(t, response.Succeeded)
	assert.Equal(t, raft.Index(1), role.raft.CommitIndex())

	// Test rejecting a request due to missing entries
	response, err = role.Append(context.TODO(), &raft.AppendRequest{
		Term:         2,
		Leader:       "bar",
		PrevLogIndex: 3,
		PrevLogTerm:  2,
		Entries: []*raft.RaftLogEntry{
			{
				Term:      2,
				Timestamp: time.Now(),
				Entry: &raft.RaftLogEntry_Initialize{
					Initialize: &raft.InitializeEntry{},
				},
			},
		},
		CommitIndex: 1,
	})

	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.Equal(t, raft.Term(2), response.Term)
	assert.False(t, response.Succeeded)
	assert.Equal(t, raft.Index(2), response.LastLogIndex)

	// Test rejecting entries for an inconsistent term
	response, err = role.Append(context.TODO(), &raft.AppendRequest{
		Term:         3,
		Leader:       "baz",
		PrevLogIndex: 2,
		PrevLogTerm:  3,
		Entries: []*raft.RaftLogEntry{
			{
				Term:      2,
				Timestamp: time.Now(),
				Entry: &raft.RaftLogEntry_Initialize{
					Initialize: &raft.InitializeEntry{},
				},
			},
		},
		CommitIndex: 1,
	})

	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.Equal(t, raft.Term(3), response.Term)
	assert.False(t, response.Succeeded)
	assert.Equal(t, raft.Index(1), response.LastLogIndex)
	assert.Equal(t, raft.Term(3), role.raft.Term())
	assert.Equal(t, raft.MemberID("baz"), *role.raft.Leader())

	// Test replacing entries from a prior term
	response, err = role.Append(context.TODO(), &raft.AppendRequest{
		Term:         3,
		Leader:       "baz",
		PrevLogIndex: 1,
		PrevLogTerm:  2,
		Entries: []*raft.RaftLogEntry{
			{
				Term:      3,
				Timestamp: time.Now(),
				Entry: &raft.RaftLogEntry_Initialize{
					Initialize: &raft.InitializeEntry{},
				},
			},
			{
				Term:      3,
				Timestamp: time.Now(),
				Entry: &raft.RaftLogEntry_Initialize{
					Initialize: &raft.InitializeEntry{},
				},
			},
		},
		CommitIndex: 3,
	})

	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.Equal(t, raft.Term(3), response.Term)
	assert.True(t, response.Succeeded)
	assert.Equal(t, raft.Index(3), response.LastLogIndex)
}
