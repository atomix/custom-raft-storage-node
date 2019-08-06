package raft

import (
	"context"
	"github.com/atomix/atomix-go-node/pkg/atomix"
	"github.com/atomix/atomix-go-node/proto/atomix/controller"
	"github.com/atomix/atomix-go-node/proto/atomix/headers"
	"github.com/atomix/atomix-go-node/proto/atomix/map"
	"github.com/atomix/atomix-go-node/proto/atomix/primitive"
	"github.com/golang/protobuf/ptypes/duration"
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
				Id:   "foo",
				Host: "localhost",
				Port: 5679,
			},
		},
	}
	protocol := NewRaftProtocol(&RaftProtocolConfig{})
	node := atomix.NewNode("foo", config, protocol)
	go node.Start()
	time.Sleep(1 * time.Second)
	defer node.Stop()

	conn, err := grpc.Dial("localhost:5678", grpc.WithInsecure())
	assert.NoError(t, err)

	client := _map.NewMapServiceClient(conn)

	createResponse, err := client.Create(context.TODO(), &_map.CreateRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
		},
		Timeout: &duration.Duration{
			Seconds: 5,
		},
	})
	assert.NoError(t, err)

	sessionID := createResponse.Header.SessionId
	index := createResponse.Header.Index

	sizeResponse, err := client.Size(context.TODO(), &_map.SizeRequest{
		Header: &headers.RequestHeader{
			Name: &primitive.Name{
				Name:      "test",
				Namespace: "test",
			},
			SessionId: sessionID,
			Index:     index,
			RequestId: 0,
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
			SessionId: sessionID,
			Index:     index,
			RequestId: 1,
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
			SessionId: sessionID,
			Index:     index,
			RequestId: 1,
		},
		Key: "foo",
	})
	assert.NoError(t, err)
	assert.Equal(t, "Hello world!", string(getResponse.Value))
	index = putResponse.Header.Index
}
