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
	"github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func newTestServer() *Server {
	config := cluster.Cluster{
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

	return NewServer(config, node.GetRegistry(), 5*time.Second)
}

func TestRole(t *testing.T) {
	role := newRaftRole(newTestServer())

	joinResponse, err := role.Join(context.TODO(), &JoinRequest{})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_ERROR, joinResponse.Status)

	leaveResponse, err := role.Leave(context.TODO(), &LeaveRequest{})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_ERROR, leaveResponse.Status)

	configureResponse, err := role.Configure(context.TODO(), &ConfigureRequest{})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_ERROR, configureResponse.Status)

	reconfigureResponse, err := role.Reconfigure(context.TODO(), &ReconfigureRequest{})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_ERROR, reconfigureResponse.Status)

	pollResponse, err := role.Poll(context.TODO(), &PollRequest{})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_ERROR, pollResponse.Status)

	voteResponse, err := role.Vote(context.TODO(), &VoteRequest{})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_ERROR, voteResponse.Status)

	transferResponse, err := role.Transfer(context.TODO(), &TransferRequest{})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_ERROR, transferResponse.Status)

	appendResponse, err := role.Append(context.TODO(), &AppendRequest{})
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_ERROR, appendResponse.Status)

	installCh := make(chan *InstallResponse, 1)
	err = role.Install(newTestInstallServer(installCh))
	installResponse := <-installCh
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_ERROR, installResponse.Status)

	commandCh := make(chan *CommandResponse, 1)
	err = role.Command(&CommandRequest{}, newTestCommandServer(commandCh))
	commandResponse := <-commandCh
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_ERROR, commandResponse.Status)

	queryCh := make(chan *QueryResponse, 1)
	err = role.Query(&QueryRequest{}, newTestQueryServer(queryCh))
	response := <-queryCh
	assert.NoError(t, err)
	assert.Equal(t, ResponseStatus_ERROR, response.Status)
}

func newTestInstallServer(ch chan<- *InstallResponse) RaftService_InstallServer {
	return &installServer{ch: ch}
}

type installServer struct {
	*raftServiceInstallServer
	ch chan<- *InstallResponse
}

func (s *installServer) SendAndClose(response *InstallResponse) error {
	s.ch <- response
	return nil
}

func newTestCommandServer(ch chan<- *CommandResponse) RaftService_CommandServer {
	return commandServer{ch: ch}
}

type commandServer struct {
	raftServiceCommandServer
	ch chan<- *CommandResponse
}

func (s commandServer) Send(response *CommandResponse) error {
	s.ch <- response
	return nil
}

func newTestQueryServer(ch chan<- *QueryResponse) RaftService_QueryServer {
	return queryServer{ch: ch}
}

type queryServer struct {
	raftServiceQueryServer
	ch chan<- *QueryResponse
}

func (s queryServer) Send(response *QueryResponse) error {
	s.ch <- response
	return nil
}
