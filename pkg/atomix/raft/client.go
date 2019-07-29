package raft

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"github.com/atomix/atomix-go-node/pkg/atomix"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"google.golang.org/grpc"
	"io"
	"sync"
)

// newRaftClient returns a new Raft client
func newRaftClient(consistency ReadConsistency) *RaftClient {
	return &RaftClient{
		consistency: consistency,
	}
}

// RaftClient is a service Client implementation for the Raft consensus protocol
type RaftClient struct {
	service.Client
	members     map[string]*atomix.Member
	membersList *list.List
	memberNode  *list.Element
	member      *atomix.Member
	memberConn  *grpc.ClientConn
	client      RaftServiceClient
	consistency ReadConsistency
	requestID   int64
	mu          sync.Mutex
}

func (c *RaftClient) Connect(cluster atomix.Cluster) error {
	c.members = make(map[string]*atomix.Member)
	c.membersList = list.New()
	for name, member := range cluster.Members {
		c.members[name] = &member
		c.membersList.PushBack(&member)
	}
	return nil
}

func (c *RaftClient) Write(ctx context.Context, in []byte, ch chan<- service.Output) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requestID++
	request := &CommandRequest{
		SequenceNumber: c.requestID,
		Value:          in,
	}
	return c.write(ctx, request, ch)
}

func (c *RaftClient) Read(ctx context.Context, in []byte, ch chan<- service.Output) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	request := &QueryRequest{
		Value:           in,
		ReadConsistency: c.consistency,
	}
	return c.read(ctx, request, ch)
}

// resetConn resets the client connection to reconnect to a new member
func (c *RaftClient) resetConn() {
	if c.memberConn != nil {
		c.memberConn.Close()
	}
	c.member = nil
	c.client = nil
}

// getConn gets the current gRPC client connection
func (c *RaftClient) getConn() (*grpc.ClientConn, error) {
	if c.member == nil {
		if c.memberNode == nil {
			c.memberNode = c.membersList.Front()
		} else {
			c.memberNode = c.memberNode.Next()
			if c.memberNode == nil {
				c.memberNode = c.membersList.Front()
			}
		}
		c.member = c.memberNode.Value.(*atomix.Member)
	}

	if c.memberConn == nil {
		conn, err := grpc.Dial(fmt.Sprintf("%s:%d", c.member.Host, c.member.Port))
		if err != nil {
			return nil, err
		}
		c.memberConn = conn
	}
	return c.memberConn, nil
}

// getClient gets the current Raft client connection
func (c *RaftClient) getClient() (RaftServiceClient, error) {
	if c.client == nil {
		conn, err := c.getConn()
		if err != nil {
			return nil, err
		}
		c.client = NewRaftServiceClient(conn)
	}
	return c.client, nil
}

func (c *RaftClient) write(ctx context.Context, request *CommandRequest, ch chan<- service.Output) error {
	client, err := c.getClient()
	if err != nil {
		return err
	}

	stream, err := client.Command(ctx, request)
	if err != nil {
		return err
	}

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if response.Status == ResponseStatus_OK {
			ch <- service.Output{
				Value: response.Output,
			}
		} else {
			ch <- service.Output{
				Error: errors.New(response.Message),
			}
		}
	}
	return nil
}

func (c *RaftClient) read(ctx context.Context, request *QueryRequest, ch chan<- service.Output) error {
	client, err := c.getClient()
	if err != nil {
		return err
	}

	stream, err := client.Query(ctx, request)
	if err != nil {
		return err
	}

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if response.Status == ResponseStatus_OK {
			ch <- service.Output{
				Value: response.Output,
			}
		} else {
			ch <- service.Output{
				Error: errors.New(response.Message),
			}
		}
	}
	return nil
}

func (c *RaftClient) Close() error {
	return nil
}
