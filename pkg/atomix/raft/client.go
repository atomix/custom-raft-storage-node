package raft

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"github.com/atomix/atomix-go-node/pkg/atomix"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	log "github.com/sirupsen/logrus"
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
	members      map[string]atomix.Member
	membersList  *list.List
	memberNode   *list.Element
	member       atomix.Member
	memberConn   *grpc.ClientConn
	client       RaftServiceClient
	leader       atomix.Member
	leaderConn   *grpc.ClientConn
	leaderClient RaftServiceClient
	consistency  ReadConsistency
	requestID    int64
	mu           sync.Mutex
}

func (c *RaftClient) Connect(cluster atomix.Cluster) error {
	c.members = make(map[string]atomix.Member)
	c.membersList = list.New()
	for name, member := range cluster.Members {
		c.members[name] = member
		c.membersList.PushBack(member)
	}
	return nil
}

func (c *RaftClient) Write(ctx context.Context, in []byte, ch chan<- service.Output) error {
	request := &CommandRequest{
		Value: in,
	}
	go c.write(ctx, request, ch)
	return nil
}

func (c *RaftClient) Read(ctx context.Context, in []byte, ch chan<- service.Output) error {
	request := &QueryRequest{
		Value:           in,
		ReadConsistency: c.consistency,
	}
	go c.read(ctx, request, ch)
	return nil
}

// resetLeaderConn resets the client's connection to the leader
func (c *RaftClient) resetLeaderConn() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leaderConn != nil {
		c.leaderConn.Close()
		c.leaderConn = nil
	}
	c.leader = atomix.Member{}
	c.leaderClient = nil
}

// getLeaderConn gets the gRPC client connection to the leader
func (c *RaftClient) getLeaderConn() (*grpc.ClientConn, error) {
	if c.leader.ID == "" {
		return c.getConn()
	}
	if c.leaderConn != nil {
		return c.leaderConn, nil
	}

	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", c.leader.Host, c.leader.Port), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	c.leaderConn = conn
	return c.leaderConn, nil
}

// getClient gets the current Raft client connection for the leader
func (c *RaftClient) getLeaderClient() (RaftServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leaderClient == nil {
		conn, err := c.getLeaderConn()
		if err != nil {
			return nil, err
		}
		c.leaderClient = NewRaftServiceClient(conn)
	}
	return c.leaderClient, nil
}

func (c *RaftClient) write(ctx context.Context, request *CommandRequest, ch chan<- service.Output) error {
	client, err := c.getLeaderClient()
	if err != nil {
		return err
	}

	log.Tracef("Sending CommandRequest %+v", request)
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
			c.resetLeaderConn()
			return err
		}

		log.Tracef("Received CommandResponse %+v", response)
		if response.Status == ResponseStatus_OK {
			ch <- service.Output{
				Value: response.Output,
			}
		} else if response.Error == RaftError_ILLEGAL_MEMBER_STATE {
			if c.leader.ID == "" || response.Leader != c.leader.ID {
				leader, ok := c.members[response.Leader]
				if ok {
					c.resetLeaderConn()
					c.leader = leader
					return c.write(ctx, request, ch)
				} else {
					ch <- service.Output{
						Error: errors.New(response.Message),
					}
					return nil
				}
			} else {
				ch <- service.Output{
					Error: errors.New(response.Message),
				}
				return nil
			}
		} else {
			ch <- service.Output{
				Error: errors.New(response.Message),
			}
		}
	}
	return nil
}

// resetConn resets the client connection to reconnect to a new member
func (c *RaftClient) resetConn() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.memberConn != nil {
		c.memberConn.Close()
		c.memberConn = nil
	}
	c.member = atomix.Member{}
	c.client = nil
}

// getConn gets the current gRPC client connection
func (c *RaftClient) getConn() (*grpc.ClientConn, error) {
	if c.member.ID == "" {
		if c.memberNode == nil {
			c.memberNode = c.membersList.Front()
		} else {
			c.memberNode = c.memberNode.Next()
			if c.memberNode == nil {
				c.memberNode = c.membersList.Front()
			}
		}
		c.member = c.memberNode.Value.(atomix.Member)
	}

	if c.memberConn == nil {
		conn, err := grpc.Dial(fmt.Sprintf("%s:%d", c.member.Host, c.member.Port), grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		c.memberConn = conn
	}
	return c.memberConn, nil
}

// getClient gets the current Raft client connection
func (c *RaftClient) getClient() (RaftServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client == nil {
		conn, err := c.getConn()
		if err != nil {
			return nil, err
		}
		c.client = NewRaftServiceClient(conn)
	}
	return c.client, nil
}

func (c *RaftClient) read(ctx context.Context, request *QueryRequest, ch chan<- service.Output) error {
	client, err := c.getClient()
	if err != nil {
		return err
	}

	log.Tracef("Sending QueryRequest %+v", request)
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

		log.Tracef("Received QueryResponse %+v", response)
		if response.Status == ResponseStatus_OK {
			ch <- service.Output{
				Value: response.Output,
			}
		} else if response.Error == RaftError_ILLEGAL_MEMBER_STATE {
			c.resetConn()
			return c.read(ctx, request, ch)
		} else {
			ch <- service.Output{
				Error: errors.New(response.Message),
			}
		}
	}
	return nil
}

func (c *RaftClient) Close() error {
	if c.memberConn != nil {
		c.memberConn.Close()
	}
	if c.leaderConn != nil {
		c.leaderConn.Close()
	}
	return nil
}
