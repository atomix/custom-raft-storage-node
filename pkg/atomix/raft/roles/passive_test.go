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
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	raft "github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol/mock"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/util"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestUpdateTermAndLeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	protocol, sm, stores := newTestState(mock.NewMockClient(ctrl))
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
	ctrl := gomock.NewController(t)
	protocol, sm, stores := newTestState(mock.NewMockClient(ctrl))
	role := newPassiveRole(protocol, sm, stores, util.NewNodeLogger(string(protocol.Member())))

	// Test updating the term/leader
	response, err := role.Append(context.TODO(), &raft.AppendRequest{
		Term:         2,
		Leader:       "bar",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*raft.LogEntry{},
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
		Entries:      []*raft.LogEntry{},
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
		Entries: []*raft.LogEntry{
			{
				Term:      2,
				Timestamp: time.Now(),
				Entry: &raft.LogEntry_Initialize{
					Initialize: &raft.InitializeEntry{},
				},
			},
			{
				Term:      2,
				Timestamp: time.Now(),
				Entry: &raft.LogEntry_Initialize{
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
		Entries:      []*raft.LogEntry{},
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
		Entries: []*raft.LogEntry{
			{
				Term:      2,
				Timestamp: time.Now(),
				Entry: &raft.LogEntry_Initialize{
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
		Entries: []*raft.LogEntry{
			{
				Term:      2,
				Timestamp: time.Now(),
				Entry: &raft.LogEntry_Initialize{
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
		Entries: []*raft.LogEntry{
			{
				Term:      3,
				Timestamp: time.Now(),
				Entry: &raft.LogEntry_Initialize{
					Initialize: &raft.InitializeEntry{},
				},
			},
			{
				Term:      3,
				Timestamp: time.Now(),
				Entry: &raft.LogEntry_Initialize{
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

func TestPassiveCommand(t *testing.T) {
	ctrl := gomock.NewController(t)
	protocol, sm, stores := newTestState(mock.NewMockClient(ctrl))
	role := newPassiveRole(protocol, sm, stores, util.NewNodeLogger(string(protocol.Member())))
	assert.NoError(t, role.raft.SetTerm(raft.Term(1)))

	ch := make(chan *raft.CommandStreamResponse, 1)
	err := role.Command(&raft.CommandRequest{}, ch)
	assert.NoError(t, err)
	response := <-ch
	assert.True(t, response.Succeeded())
	assert.Equal(t, raft.ResponseStatus_ERROR, response.Response.Status)
	assert.Equal(t, raft.ResponseError_ILLEGAL_MEMBER_STATE, response.Response.Error)
	assert.Equal(t, raft.Term(1), response.Response.Term)
	assert.Equal(t, raft.MemberID(""), response.Response.Leader)

	assert.NoError(t, role.raft.SetLeader(&role.raft.Members()[1]))
	ch = make(chan *raft.CommandStreamResponse, 1)
	err = role.Command(&raft.CommandRequest{}, ch)
	assert.NoError(t, err)
	response = <-ch
	assert.True(t, response.Succeeded())
	assert.Equal(t, raft.ResponseStatus_ERROR, response.Response.Status)
	assert.Equal(t, raft.ResponseError_ILLEGAL_MEMBER_STATE, response.Response.Error)
	assert.Equal(t, raft.Term(1), response.Response.Term)
	assert.Equal(t, role.raft.Members()[1], response.Response.Leader)
}

func TestPassiveQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	expectQuery(client).AnyTimes()
	protocol, sm, stores := newTestState(client)
	role := newPassiveRole(protocol, sm, stores, util.NewNodeLogger(string(protocol.Member())))
	assert.NoError(t, role.raft.SetTerm(raft.Term(1)))

	// With no leader and no commits, the role should return an error
	ch := make(chan *raft.QueryStreamResponse, 1)
	err := role.Query(&raft.QueryRequest{}, ch)
	assert.NoError(t, err)
	response := <-ch
	assert.True(t, response.Succeeded())
	assert.Equal(t, raft.ResponseStatus_ERROR, response.Response.Status)
	assert.Equal(t, raft.ResponseError_NO_LEADER, response.Response.Error)

	// With no commits and a leader, the role should forward the request
	assert.NoError(t, role.raft.SetLeader(&role.raft.Members()[1]))
	ch = make(chan *raft.QueryStreamResponse, 1)
	err = role.Query(&raft.QueryRequest{}, ch)
	assert.NoError(t, err)
	response = <-ch
	assert.True(t, response.Succeeded())
	assert.Equal(t, raft.ResponseStatus_OK, response.Response.Status)

	bytes, _ := proto.Marshal(&service.ServiceRequest{
		Request: &service.ServiceRequest_Metadata{
			Metadata: &service.MetadataRequest{},
		},
	})

	// With commits caught up, the role should handle sequential requests
	role.store.Writer().Append(&raft.LogEntry{
		Term:      raft.Term(1),
		Timestamp: time.Now(),
		Entry: &raft.LogEntry_Initialize{
			Initialize: &raft.InitializeEntry{},
		},
	})
	role.raft.SetCommitIndex(raft.Index(1))
	role.raft.Commit(raft.Index(1))
	ch = make(chan *raft.QueryStreamResponse, 1)
	err = role.Query(&raft.QueryRequest{
		Value:           bytes,
		ReadConsistency: raft.ReadConsistency_SEQUENTIAL,
	}, ch)
	assert.NoError(t, err)
	response = <-ch
	assert.True(t, response.Succeeded())
	assert.Equal(t, raft.ResponseStatus_OK, response.Response.Status)

	// Requests with stronger consistency requirements should be forwarded to the leader
	ch = make(chan *raft.QueryStreamResponse, 1)
	err = role.Query(&raft.QueryRequest{ReadConsistency: raft.ReadConsistency_LINEARIZABLE_LEASE}, ch)
	assert.NoError(t, err)
	response = <-ch
	assert.True(t, response.Succeeded())
	assert.Equal(t, raft.ResponseStatus_OK, response.Response.Status)
	ch = make(chan *raft.QueryStreamResponse, 1)
	err = role.Query(&raft.QueryRequest{ReadConsistency: raft.ReadConsistency_LINEARIZABLE}, ch)
	assert.NoError(t, err)
	response = <-ch
	assert.True(t, response.Succeeded())
	assert.Equal(t, raft.ResponseStatus_OK, response.Response.Status)
}
