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
	"github.com/atomix/atomix-api/proto/atomix/controller"
	"github.com/atomix/atomix-api/proto/atomix/headers"
	"github.com/atomix/atomix-api/proto/atomix/map"
	"github.com/atomix/atomix-api/proto/atomix/primitive"
	"github.com/atomix/atomix-go-node/pkg/atomix"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"testing"
	"time"
)

func TestNode(t *testing.T) {
	config := &controller.PartitionConfig{
		Partition: &controller.PartitionId{
			Partition: 1,
			Group: &controller.PartitionGroupId{
				Name:      "test",
				Namespace: "default",
			},
		},
		Members: []*controller.NodeConfig{
			{
				ID:   "foo",
				Host: "localhost",
				Port: 5679,
			},
		},
	}
	protocol := NewProtocol(&RaftProtocolConfig{})
	node := atomix.NewNode("foo", config, protocol)
	go func() {
		_ = node.Start()
	}()
	time.Sleep(1 * time.Second)
	defer func() {
		_ = node.Stop()
	}()

	conn, err := grpc.Dial("localhost:5678", grpc.WithInsecure())
	assert.NoError(t, err)

	client := _map.NewMapServiceClient(conn)

	timeout := 5 * time.Second
	createResponse, err := client.Create(context.TODO(), &_map.CreateRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
		},
		Timeout: &timeout,
	})
	assert.NoError(t, err)

	sessionID := createResponse.Header.SessionID
	index := createResponse.Header.Index

	sizeResponse, err := client.Size(context.TODO(), &_map.SizeRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionID: sessionID,
			Index:     index,
			RequestID: 0,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), sizeResponse.Size)
	index = sizeResponse.Header.Index

	putResponse, err := client.Put(context.TODO(), &_map.PutRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionID: sessionID,
			Index:     index,
			RequestID: 1,
		},
		Key:   "foo",
		Value: []byte("Hello world!"),
	})
	assert.NoError(t, err)
	assert.Equal(t, _map.ResponseStatus_OK, putResponse.Status)
	index = putResponse.Header.Index

	getResponse, err := client.Get(context.TODO(), &_map.GetRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionID: sessionID,
			Index:     index,
			RequestID: 1,
		},
		Key: "foo",
	})
	assert.NoError(t, err)
	assert.Equal(t, "Hello world!", string(getResponse.Value))
}
