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

func TestLeaderInit(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	succeedAppend(client).AnyTimes()

	role := newLeaderRole(newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl)))
	assert.NoError(t, role.raft.SetTerm(raft.Term(1)))
	assert.NoError(t, role.Start())
	role.raft.ReadLock()
	assert.Equal(t, raft.Term(1), role.raft.Term())
	assert.Equal(t, role.raft.Member(), *role.raft.Leader())
	role.raft.ReadUnlock()

	entry := awaitEntry(role.raft, role.store.Log(), raft.Index(1))
	assert.Equal(t, raft.Term(1), entry.Entry.Term)
	assert.NotNil(t, entry.Entry.GetInitialize())

	assert.Equal(t, raft.Index(1), awaitCommit(role.raft, raft.Index(1)))
}
