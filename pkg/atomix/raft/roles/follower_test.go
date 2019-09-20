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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFollowerHeartbeatTimeout(t *testing.T) {
	protocol, sm, stores := newTestState(&voteProtocol{})
	role := newFollowerRole(protocol, sm, stores)
	assert.NoError(t, role.Start())

	roleCh := make(chan raft.RoleType, 1)
	role.raft.WatchRole(func(roleType raft.RoleType) {
		roleCh <- roleType
	})
	assert.Equal(t, raft.RoleCandidate, <-roleCh)
}

type voteProtocol struct {
	raft.UnimplementedProtocol
}

func (p *voteProtocol) Poll(ctx context.Context, request *raft.PollRequest, member raft.MemberID) (*raft.PollResponse, error) {
	return &raft.PollResponse{
		Status:   raft.ResponseStatus_OK,
		Term:     request.Term,
		Accepted: true,
	}, nil
}

func (p *voteProtocol) Vote(ctx context.Context, request *raft.VoteRequest, member raft.MemberID) (*raft.VoteResponse, error) {
	return &raft.VoteResponse{
		Status: raft.ResponseStatus_OK,
		Term:   request.Term,
		Voted:  true,
	}, nil
}
