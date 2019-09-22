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
)

func TestFollowerHeartbeatTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	expectPoll(client).AnyTimes()
	expectVote(client).AnyTimes()
	expectAppend(client).AnyTimes()

	protocol, sm, stores := newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl))
	role := newFollowerRole(protocol, sm, stores)
	assert.NoError(t, role.Start())

	roleCh := make(chan raft.RoleType, 1)
	role.raft.WatchRole(func(roleType raft.RoleType) {
		roleCh <- roleType
	})
	assert.Equal(t, raft.RoleCandidate, <-roleCh)
}
