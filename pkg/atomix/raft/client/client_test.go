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

package client

import (
	"context"
	"github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	raft "github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"testing"
)

func newTestClient(client raft.Client) *Client {
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
	return newClient(raft.NewCluster(members), client, raft.ReadConsistency_SEQUENTIAL)
}

func TestClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	protocol := mock.NewMockClient(ctrl)

	members := []raft.MemberID{
		"foo",
		"bar",
		"baz",
	}
	protocol.EXPECT().
		Command(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *raft.CommandRequest, member raft.MemberID) (<-chan *raft.CommandStreamResponse, error) {
			ch := make(chan *raft.CommandStreamResponse, 2)
			ch <- raft.NewCommandStreamResponse(&raft.CommandResponse{
				Status:  raft.ResponseStatus_OK,
				Leader:  raft.MemberID("foo"),
				Term:    raft.Term(1),
				Members: members,
				Output:  []byte("foo"),
			}, nil)
			ch <- raft.NewCommandStreamResponse(&raft.CommandResponse{
				Status:  raft.ResponseStatus_OK,
				Leader:  raft.MemberID("foo"),
				Term:    raft.Term(1),
				Members: members,
				Output:  []byte("bar"),
			}, nil)
			close(ch)
			return ch, nil
		}).AnyTimes()
	protocol.EXPECT().
		Query(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *raft.QueryRequest, member raft.MemberID) (<-chan *raft.QueryStreamResponse, error) {
			ch := make(chan *raft.QueryStreamResponse, 2)
			ch <- raft.NewQueryStreamResponse(&raft.QueryResponse{
				Status: raft.ResponseStatus_OK,
				Output: []byte("bar"),
			}, nil)
			ch <- raft.NewQueryStreamResponse(&raft.QueryResponse{
				Status: raft.ResponseStatus_OK,
				Output: []byte("baz"),
			}, nil)
			close(ch)
			return ch, nil
		}).AnyTimes()

	client := newTestClient(protocol)

	ch := make(chan node.Output)
	assert.NoError(t, client.Write(context.Background(), []byte("Hello world!"), ch))
	assert.Equal(t, "foo", string((<-ch).Value))
	assert.Equal(t, "bar", string((<-ch).Value))
	_, ok := <-ch
	assert.False(t, ok)

	ch = make(chan node.Output)
	assert.NoError(t, client.Read(context.Background(), []byte("Hello world again!"), ch))
	assert.Equal(t, "bar", string((<-ch).Value))
	assert.Equal(t, "baz", string((<-ch).Value))
	_, ok = <-ch
	assert.False(t, ok)
}
