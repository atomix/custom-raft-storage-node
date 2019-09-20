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
	raft "github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFollowerHeartbeatTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	expectPoll(client).AnyTimes()
	expectVote(client).AnyTimes()
	expectAppend(client).AnyTimes()

	protocol, sm, stores := newTestState(client)
	role := newFollowerRole(protocol, sm, stores)
	assert.NoError(t, role.Start())

	roleCh := make(chan raft.RoleType, 1)
	role.raft.WatchRole(func(roleType raft.RoleType) {
		roleCh <- roleType
	})
	assert.Equal(t, raft.RoleCandidate, <-roleCh)
}

// expectPoll expects a successful poll response
func expectPoll(client *mock.MockClient) *gomock.Call {
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

// expectVote expects a successful vote response
func expectVote(client *mock.MockClient) *gomock.Call {
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

// expectAppend expects and rejects an append request
func expectAppend(client *mock.MockClient) *gomock.Call {
	return client.EXPECT().
		Append(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, errors.New("not implemented"))
}
