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
	"errors"
	"github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/config"
	raft "github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol/mock"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/state"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/store"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/store/log"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/util"
	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func newTestState(client raft.Client, roles ...raft.Role) (raft.Raft, state.Manager, store.Store) {
	roleFuncs := make(map[raft.RoleType]func(raft.Raft) raft.Role)
	for _, role := range roles {
		roleFuncs[role.Type()] = func(role raft.Role) func(raft.Raft) raft.Role {
			return func(raft.Raft) raft.Role {
				return role
			}
		}(role)
	}

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

	cluster := raft.NewCluster(members)
	store := store.NewMemoryStore()
	state := state.NewManager(cluster.Member(), store, node.GetRegistry())
	electionTimeout := 1 * time.Second
	config := &config.ProtocolConfig{
		ElectionTimeout: &electionTimeout,
	}
	raft := raft.NewRaft(cluster, config, client, roleFuncs)
	return raft, state, store
}

func TestRole(t *testing.T) {
	ctrl := gomock.NewController(t)
	protocol, sm, stores := newTestState(mock.NewMockClient(ctrl))
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

// awaitRole blocks until the role is set to the given role
func awaitRole(r raft.Raft, role raft.RoleType) raft.RoleType {
	ch := make(chan raft.RoleType, 1)
	r.ReadLock()
	if r.Role() == role {
		ch <- role
	} else {
		r.Watch(func(event raft.Event) {
			if event.Type == raft.EventTypeRole && event.Role == role {
				ch <- role
			}
		})
	}
	r.ReadUnlock()
	return <-ch
}

// awaitTerm blocks until the term is set to the given term
func awaitTerm(r raft.Raft, term raft.Term) raft.Term {
	ch := make(chan raft.Term, 1)
	r.ReadLock()
	if r.Term() == term {
		ch <- term
	} else {
		r.Watch(func(event raft.Event) {
			if event.Type == raft.EventTypeTerm && event.Term == term {
				ch <- term
			}
		})
	}
	r.ReadUnlock()
	return <-ch
}

// awaitLeader blocks until the leader is set to the given leader
func awaitLeader(r raft.Raft, leader *raft.MemberID) *raft.MemberID {
	ch := make(chan *raft.MemberID, 1)
	r.ReadLock()
	if (r.Leader() == nil && leader == nil) || (r.Leader() != nil && leader != nil && *r.Leader() == *leader) {
		ch <- leader
	} else {
		r.Watch(func(event raft.Event) {
			if event.Type == raft.EventTypeLeader && ((event.Leader == nil && leader == nil) || (event.Leader != nil && leader != nil && *event.Leader == *leader)) {
				ch <- leader
			}
		})
	}
	r.ReadUnlock()
	return <-ch
}

// awaitIndex blocks until the entry at the given index is appended
func awaitIndex(r raft.Raft, log log.Log, index raft.Index) raft.Index {
	return awaitEntry(r, log, index).Index
}

// awaitEntry blocks until the entry at the given index is appended
func awaitEntry(r raft.Raft, log log.Log, index raft.Index) *log.Entry {
	reader := log.OpenReader(0)
	for {
		r.ReadLock()
		if reader.LastIndex() >= index {
			reader.Reset(index)
			entry := reader.NextEntry()
			r.ReadUnlock()
			return entry
		}
		r.ReadUnlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// awaitCommit blocks until the given index has been committed
func awaitCommit(r raft.Raft, index raft.Index) raft.Index {
	for {
		r.ReadLock()
		if r.CommitIndex() >= index {
			r.ReadUnlock()
			return index
		}
		r.ReadUnlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// mockRole mocks a role
func mockRole(ctrl *gomock.Controller, roleType raft.RoleType) raft.Role {
	role := mock.NewMockRole(ctrl)
	role.EXPECT().Type().Return(roleType).AnyTimes()
	role.EXPECT().Start().Return(nil).AnyTimes()
	role.EXPECT().Stop().Return(nil).AnyTimes()
	return role
}

// mockFollower mocks a follower role
func mockFollower(ctrl *gomock.Controller) raft.Role {
	return mockRole(ctrl, raft.RoleFollower)
}

// mockCandidate mocks a candidate role
func mockCandidate(ctrl *gomock.Controller) raft.Role {
	return mockRole(ctrl, raft.RoleCandidate)
}

// mockLeader mocks a leader role
func mockLeader(ctrl *gomock.Controller) raft.Role {
	return mockRole(ctrl, raft.RoleLeader)
}

// expectQuery expects a successful query response
func expectQuery(client *mock.MockClient) *gomock.Call {
	return client.EXPECT().
		Query(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *raft.QueryRequest, member raft.MemberID) (<-chan *raft.QueryStreamResponse, error) {
			ch := make(chan *raft.QueryStreamResponse, 1)
			ch <- &raft.QueryStreamResponse{
				StreamMessage: &raft.StreamMessage{},
				Response: &raft.QueryResponse{
					Status: raft.ResponseStatus_OK,
				},
			}
			defer close(ch)
			return ch, nil
		})
}

// acceptPoll expects a successful poll response
func acceptPoll(client *mock.MockClient) *gomock.Call {
	return client.EXPECT().
		Poll(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *raft.PollRequest, member raft.MemberID) (*raft.PollResponse, error) {
			return &raft.PollResponse{
				Status:   raft.ResponseStatus_OK,
				Term:     request.Term,
				Accepted: true,
			}, nil
		})
}

// rejectPoll rejects poll requests
func rejectPoll(client *mock.MockClient) *gomock.Call {
	return client.EXPECT().
		Poll(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *raft.PollRequest, member raft.MemberID) (*raft.PollResponse, error) {
			return &raft.PollResponse{
				Status:   raft.ResponseStatus_OK,
				Term:     request.Term,
				Accepted: false,
			}, nil
		})
}

// acceptVote expects a successful vote response
func acceptVote(client *mock.MockClient) *gomock.Call {
	return client.EXPECT().
		Vote(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *raft.VoteRequest, member raft.MemberID) (*raft.VoteResponse, error) {
			return &raft.VoteResponse{
				Status: raft.ResponseStatus_OK,
				Term:   request.Term,
				Voted:  true,
			}, nil
		})
}

// rejectVote rejects a vote request
func rejectVote(client *mock.MockClient) *gomock.Call {
	return client.EXPECT().
		Vote(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *raft.VoteRequest, member raft.MemberID) (*raft.VoteResponse, error) {
			return &raft.VoteResponse{
				Status: raft.ResponseStatus_OK,
				Term:   request.Term,
				Voted:  false,
			}, nil
		})
}

// delayFailVote delays and then fails a vote request
func delayFailVote(client *mock.MockClient, delay time.Duration) *gomock.Call {
	return client.EXPECT().
		Vote(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *raft.VoteRequest, member raft.MemberID) (*raft.VoteResponse, error) {
			time.Sleep(delay)
			return nil, errors.New("VoteRequest failed")
		})
}

// succeedAppend responds successfully to an append request
func succeedAppend(client *mock.MockClient) *gomock.Call {
	return client.EXPECT().
		Append(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *raft.AppendRequest, member raft.MemberID) (*raft.AppendResponse, error) {
			return &raft.AppendResponse{
				Status:       raft.ResponseStatus_OK,
				Term:         request.Term,
				Succeeded:    true,
				LastLogIndex: request.PrevLogIndex + raft.Index(len(request.Entries)),
			}, nil
		})
}

// failAppend expects and rejects an append request
func failAppend(client *mock.MockClient) *gomock.Call {
	return client.EXPECT().
		Append(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, errors.New("AppendRequest failed"))
}

func init() {
	logrus.SetLevel(logrus.TraceLevel)
}
