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
	"github.com/atomix/go-framework/pkg/atomix/service"
	raft "github.com/atomix/raft-replica/pkg/atomix/raft/protocol"
	"github.com/atomix/raft-replica/pkg/atomix/raft/protocol/mock"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestLeaderInit(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	succeedAppend(client).AnyTimes()

	role := newLeaderRole(newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl))).(*LeaderRole)
	assert.NoError(t, role.raft.SetTerm(raft.Term(1)))
	assert.NoError(t, role.Start())
	role.raft.ReadLock()
	assert.Equal(t, raft.RoleLeader, role.Type())
	assert.Equal(t, raft.Term(1), role.raft.Term())
	assert.Equal(t, role.raft.Member(), *role.raft.Leader())
	role.raft.ReadUnlock()

	entry := awaitEntry(role.raft, role.store.Log(), raft.Index(1))
	assert.Equal(t, raft.Term(1), entry.Entry.Term)
	assert.NotNil(t, entry.Entry.GetInitialize())

	assert.Equal(t, raft.Index(1), awaitCommit(role.raft, raft.Index(1)))
}

func TestLeaderInitStepDown(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	failAppend(client).AnyTimes()

	role := newLeaderRole(newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl))).(*LeaderRole)
	assert.NoError(t, role.raft.SetTerm(raft.Term(1)))
	assert.NoError(t, role.Start())
	assert.Equal(t, raft.RoleFollower, awaitRole(role.raft, raft.RoleFollower))
}

func TestLeaderCommitStepDown(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	succeedAppend(client)
	succeedAppend(client)
	failAppend(client).AnyTimes()

	role := newLeaderRole(newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl))).(*LeaderRole)
	assert.NoError(t, role.raft.SetTerm(raft.Term(1)))
	assert.NoError(t, role.Start())
	assert.Equal(t, raft.Index(1), awaitCommit(role.raft, raft.Index(1)))
	assert.Equal(t, raft.RoleFollower, awaitRole(role.raft, raft.RoleFollower))
}

func TestLeaderPoll(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	succeedAppend(client).AnyTimes()

	role := newLeaderRole(newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl))).(*LeaderRole)
	assert.NoError(t, role.raft.SetTerm(raft.Term(1)))
	assert.NoError(t, role.Start())

	response, err := role.Poll(context.TODO(), &raft.PollRequest{
		Term:         raft.Term(1),
		Candidate:    role.raft.Members()[1],
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	assert.NoError(t, err)
	assert.False(t, response.Accepted)
}

func TestLeaderVote(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	succeedAppend(client).AnyTimes()

	role := newLeaderRole(newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl))).(*LeaderRole)
	assert.NoError(t, role.raft.SetTerm(raft.Term(1)))
	assert.NoError(t, role.Start())
	awaitIndex(role.raft, role.store.Log(), raft.Index(1))

	response, err := role.Vote(context.TODO(), &raft.VoteRequest{
		Term:         raft.Term(2),
		Candidate:    role.raft.Members()[1],
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	assert.NoError(t, err)
	assert.False(t, response.Voted)
	assert.Equal(t, raft.RoleFollower, awaitRole(role.raft, raft.RoleFollower))
	assert.Equal(t, raft.Term(2), awaitTerm(role.raft, raft.Term(2)))

	role = newLeaderRole(newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl))).(*LeaderRole)
	assert.NoError(t, role.raft.SetTerm(raft.Term(1)))
	assert.NoError(t, role.Start())
	awaitIndex(role.raft, role.store.Log(), raft.Index(1))

	response, err = role.Vote(context.TODO(), &raft.VoteRequest{
		Term:         raft.Term(2),
		Candidate:    role.raft.Members()[1],
		LastLogIndex: 1,
		LastLogTerm:  1,
	})
	assert.NoError(t, err)
	assert.True(t, response.Voted)
	assert.Equal(t, raft.RoleFollower, awaitRole(role.raft, raft.RoleFollower))
	assert.Equal(t, raft.Term(2), awaitTerm(role.raft, raft.Term(2)))
}

func TestLeaderAppendPriorTerm(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	succeedAppend(client).AnyTimes()

	role := newLeaderRole(newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl))).(*LeaderRole)
	assert.NoError(t, role.raft.SetTerm(raft.Term(2)))
	assert.NoError(t, role.Start())

	response, err := role.Append(context.TODO(), &raft.AppendRequest{
		Term:         raft.Term(1),
		Leader:       raft.MemberID("bar"),
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []*raft.LogEntry{},
		CommitIndex:  0,
	})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.False(t, response.Succeeded)
}

func TestLeaderAppendGreaterTerm(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	succeedAppend(client).AnyTimes()

	role := newLeaderRole(newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl))).(*LeaderRole)
	assert.NoError(t, role.raft.SetTerm(raft.Term(1)))
	assert.NoError(t, role.Start())
	awaitIndex(role.raft, role.store.Log(), raft.Index(1))

	leader := raft.MemberID("bar")
	response, err := role.Append(context.TODO(), &raft.AppendRequest{
		Term:         raft.Term(2),
		Leader:       leader,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []*raft.LogEntry{},
		CommitIndex:  1,
	})
	assert.NoError(t, err)
	assert.Equal(t, raft.ResponseStatus_OK, response.Status)
	assert.True(t, response.Succeeded)
	assert.Equal(t, raft.RoleFollower, awaitRole(role.raft, raft.RoleFollower))
	assert.Equal(t, raft.Term(2), awaitTerm(role.raft, raft.Term(2)))
	assert.Equal(t, &leader, awaitLeader(role.raft, &leader))
}

func TestLeaderSendSnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)

	// Expect a single install request per node
	succeedInstallTo(client, raft.MemberID("bar"))
	succeedInstallTo(client, raft.MemberID("baz"))

	// Expect a single append request per node
	succeedAppendTo(client, raft.MemberID("bar"))
	succeedAppendTo(client, raft.MemberID("baz"))

	role := newLeaderRole(newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl))).(*LeaderRole)

	// Reset the log to index 100 and add some entries
	role.store.Log().Writer().Reset(raft.Index(100))
	role.store.Log().Writer().Append(&raft.LogEntry{
		Term:      raft.Term(1),
		Timestamp: time.Now(),
		Entry: &raft.LogEntry_Initialize{
			Initialize: &raft.InitializeEntry{},
		},
	})
	role.store.Log().Writer().Append(&raft.LogEntry{
		Term:      raft.Term(2),
		Timestamp: time.Now(),
		Entry: &raft.LogEntry_Initialize{
			Initialize: &raft.InitializeEntry{},
		},
	})

	// Add a snapshot to the log at index 100
	snapshot := role.store.Snapshot().NewSnapshot(raft.Index(100), time.Now())
	writer := snapshot.Writer()
	_, _ = writer.Write([]byte("abc"))
	writer.Close()

	assert.NoError(t, role.raft.SetTerm(raft.Term(3)))
	assert.NoError(t, role.Start())
	assert.Equal(t, raft.Index(102), awaitCommit(role.raft, raft.Index(102)))

	// Verify that the init entry was written at index 102
	role.raft.ReadLock()
	reader := role.store.Log().OpenReader(102)
	entry := reader.NextEntry()
	assert.Equal(t, raft.Index(102), entry.Index)
	assert.Equal(t, raft.Term(3), entry.Entry.Term)
	role.raft.ReadUnlock()

	assert.Equal(t, raft.Index(102), awaitCommit(role.raft, raft.Index(102)))
}

func TestLeaderCommand(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	succeedAppend(client).AnyTimes()

	role := newLeaderRole(newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl))).(*LeaderRole)
	assert.NoError(t, role.raft.SetTerm(raft.Term(1)))
	assert.NoError(t, role.Start())

	request := &raft.CommandRequest{
		Value: newOpenSessionRequest(),
	}
	ch := make(chan *raft.CommandStreamResponse, 1)
	err := role.Command(request, ch)
	assert.NoError(t, err)
	response := <-ch
	assert.True(t, response.Succeeded())
	assert.Equal(t, raft.ResponseStatus_OK, response.Response.Status)
	sessionID := getSessionID(response.Response.Output)

	role.raft.ReadLock()
	assert.Equal(t, raft.Index(2), role.raft.CommitIndex())
	role.raft.ReadUnlock()

	request = &raft.CommandRequest{
		Value: newSetRequest("Set", sessionID, 1),
	}
	ch = make(chan *raft.CommandStreamResponse, 1)
	err = role.Command(request, ch)
	assert.NoError(t, err)
	response = <-ch
	assert.True(t, response.Succeeded())
	assert.Equal(t, raft.ResponseStatus_OK, response.Response.Status)

	role.raft.ReadLock()
	assert.Equal(t, raft.Index(3), role.raft.CommitIndex())
	role.raft.ReadUnlock()

	_, ok := <-ch
	assert.False(t, ok)

	request = &raft.CommandRequest{
		Value: newSetRequest("SetStream", sessionID, 2),
	}
	ch = make(chan *raft.CommandStreamResponse, 3)
	err = role.Command(request, ch)
	assert.NoError(t, err)
	response = <-ch
	assert.True(t, response.Succeeded())
	assert.Equal(t, raft.ResponseStatus_OK, response.Response.Status)
	response = <-ch
	assert.True(t, response.Succeeded())
	assert.Equal(t, raft.ResponseStatus_OK, response.Response.Status)
	response = <-ch
	assert.True(t, response.Succeeded())
	assert.Equal(t, raft.ResponseStatus_OK, response.Response.Status)

	role.raft.ReadLock()
	assert.Equal(t, raft.Index(4), role.raft.CommitIndex())
	role.raft.ReadUnlock()

	_, ok = <-ch
	assert.False(t, ok)

	request = &raft.CommandRequest{
		Value: newSetRequest("SetError", sessionID, 3),
	}
	ch = make(chan *raft.CommandStreamResponse, 1)
	err = role.Command(request, ch)
	assert.NoError(t, err)
	response = <-ch
	assert.True(t, response.Succeeded())
	assert.Equal(t, raft.ResponseStatus_ERROR, response.Response.Status)

	role.raft.ReadLock()
	assert.Equal(t, raft.Index(5), role.raft.CommitIndex())
	role.raft.ReadUnlock()

	_, ok = <-ch
	assert.False(t, ok)
}

func TestLeaderQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	succeedAppend(client).AnyTimes()

	role := newLeaderRole(newTestState(client, mockFollower(ctrl), mockCandidate(ctrl), mockLeader(ctrl))).(*LeaderRole)
	assert.NoError(t, role.raft.SetTerm(raft.Term(1)))
	assert.NoError(t, role.Start())

	command := &raft.CommandRequest{
		Value: newOpenSessionRequest(),
	}
	commandCh := make(chan *raft.CommandStreamResponse, 1)
	err := role.Command(command, commandCh)
	assert.NoError(t, err)
	commandResponse := <-commandCh
	assert.True(t, commandResponse.Succeeded())
	assert.Equal(t, raft.ResponseStatus_OK, commandResponse.Response.Status)
	sessionID := getSessionID(commandResponse.Response.Output)

	role.raft.ReadLock()
	assert.Equal(t, raft.Index(2), role.raft.CommitIndex())
	role.raft.ReadUnlock()

	query := &raft.QueryRequest{
		Value:           newGetRequest("Get", sessionID, 0),
		ReadConsistency: raft.ReadConsistency_SEQUENTIAL,
	}
	queryCh := make(chan *raft.QueryStreamResponse, 1)
	err = role.Query(query, queryCh)
	assert.NoError(t, err)

	queryResponse := <-queryCh
	assert.True(t, queryResponse.Succeeded())
	assert.Equal(t, raft.ResponseStatus_OK, queryResponse.Response.Status)

	role.raft.ReadLock()
	assert.Equal(t, raft.Index(2), role.raft.CommitIndex())
	role.raft.ReadUnlock()

	_, ok := <-queryCh
	assert.False(t, ok)

	query = &raft.QueryRequest{
		Value:           newGetRequest("Get", sessionID, 0),
		ReadConsistency: raft.ReadConsistency_LINEARIZABLE_LEASE,
	}
	queryCh = make(chan *raft.QueryStreamResponse, 1)
	err = role.Query(query, queryCh)
	assert.NoError(t, err)

	queryResponse = <-queryCh
	assert.True(t, queryResponse.Succeeded())
	assert.Equal(t, raft.ResponseStatus_OK, queryResponse.Response.Status)

	role.raft.ReadLock()
	assert.Equal(t, raft.Index(2), role.raft.CommitIndex())
	role.raft.ReadUnlock()

	_, ok = <-queryCh
	assert.False(t, ok)

	query = &raft.QueryRequest{
		Value:           newGetRequest("Get", sessionID, 0),
		ReadConsistency: raft.ReadConsistency_LINEARIZABLE,
	}
	queryCh = make(chan *raft.QueryStreamResponse, 1)
	err = role.Query(query, queryCh)
	assert.NoError(t, err)

	queryResponse = <-queryCh
	assert.True(t, queryResponse.Succeeded())
	assert.Equal(t, raft.ResponseStatus_OK, queryResponse.Response.Status)

	role.raft.ReadLock()
	assert.Equal(t, raft.Index(2), role.raft.CommitIndex())
	role.raft.ReadUnlock()

	_, ok = <-queryCh
	assert.False(t, ok)

	query = &raft.QueryRequest{
		Value: newGetRequest("GetStream", sessionID, 0),
	}
	queryCh = make(chan *raft.QueryStreamResponse, 3)
	err = role.Query(query, queryCh)
	assert.NoError(t, err)

	queryResponse = <-queryCh
	assert.True(t, queryResponse.Succeeded())
	assert.Equal(t, raft.ResponseStatus_OK, queryResponse.Response.Status)

	queryResponse = <-queryCh
	assert.True(t, queryResponse.Succeeded())
	assert.Equal(t, raft.ResponseStatus_OK, queryResponse.Response.Status)

	queryResponse = <-queryCh
	assert.True(t, queryResponse.Succeeded())
	assert.Equal(t, raft.ResponseStatus_OK, queryResponse.Response.Status)

	role.raft.ReadLock()
	assert.Equal(t, raft.Index(2), role.raft.CommitIndex())
	role.raft.ReadUnlock()

	_, ok = <-queryCh
	assert.False(t, ok)

	query = &raft.QueryRequest{
		Value: newGetRequest("GetError", sessionID, 0),
	}
	queryCh = make(chan *raft.QueryStreamResponse, 1)
	err = role.Query(query, queryCh)
	assert.NoError(t, err)

	queryResponse = <-queryCh
	assert.True(t, queryResponse.Succeeded())
	assert.Equal(t, raft.ResponseStatus_ERROR, queryResponse.Response.Status)

	role.raft.ReadLock()
	assert.Equal(t, raft.Index(2), role.raft.CommitIndex())
	role.raft.ReadUnlock()

	_, ok = <-queryCh
	assert.False(t, ok)
}

func newOpenSessionRequest() []byte {
	timeout := 30 * time.Second
	bytes, _ := proto.Marshal(&service.SessionRequest{
		Request: &service.SessionRequest_OpenSession{
			OpenSession: &service.OpenSessionRequest{
				Timeout: &timeout,
			},
		},
	})
	return newCommandRequest(bytes)
}

func getSessionID(bytes []byte) uint64 {
	return getOpenSessionResponse(bytes).SessionID
}

func getOpenSessionResponse(bytes []byte) *service.OpenSessionResponse {
	serviceResponse := &service.ServiceResponse{}
	_ = proto.Unmarshal(bytes, serviceResponse)
	sessionResponse := &service.SessionResponse{}
	_ = proto.Unmarshal(serviceResponse.GetCommand(), sessionResponse)
	return sessionResponse.GetOpenSession()
}

func newSetRequest(setType string, sessionID uint64, commandID uint64) []byte {
	bytes, _ := proto.Marshal(&SetRequest{
		Value: "Hello world!",
	})
	bytes, _ = proto.Marshal(&service.SessionRequest{
		Request: &service.SessionRequest_Command{
			Command: &service.SessionCommandRequest{
				Context: &service.SessionCommandContext{
					SessionID:      sessionID,
					SequenceNumber: commandID,
				},
				Name:  setType,
				Input: bytes,
			},
		},
	})
	return newCommandRequest(bytes)
}

func newCommandRequest(bytes []byte) []byte {
	bytes, _ = proto.Marshal(&service.ServiceRequest{
		Id: &service.ServiceId{
			Type:      "test",
			Name:      "test",
			Namespace: "test",
		},
		Request: &service.ServiceRequest_Command{
			Command: bytes,
		},
	})
	return bytes
}

func newGetRequest(getType string, sessionID uint64, commandID uint64) []byte {
	bytes, _ := proto.Marshal(&GetRequest{})
	bytes, _ = proto.Marshal(&service.SessionRequest{
		Request: &service.SessionRequest_Query{
			Query: &service.SessionQueryRequest{
				Context: &service.SessionQueryContext{
					SessionID:          sessionID,
					LastSequenceNumber: commandID,
				},
				Name:  getType,
				Input: bytes,
			},
		},
	})
	return newQueryRequest(bytes)
}

func newQueryRequest(bytes []byte) []byte {
	bytes, _ = proto.Marshal(&service.ServiceRequest{
		Id: &service.ServiceId{
			Type:      "test",
			Name:      "test",
			Namespace: "test",
		},
		Request: &service.ServiceRequest_Query{
			Query: bytes,
		},
	})
	return bytes
}
