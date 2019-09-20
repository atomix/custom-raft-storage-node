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
	"container/list"
	"context"
	"errors"
	"fmt"
	"github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	raft "github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"io"
	"sync"
)

// NewClient returns a new Raft client
func NewClient(consistency raft.ReadConsistency) *Client {
	return &Client{
		consistency: consistency,
	}
}

// Client is a service Client implementation for the Raft consensus protocol
type Client struct {
	node.Client
	members      map[string]cluster.Member
	membersList  *list.List
	memberNode   *list.Element
	member       cluster.Member
	memberConn   *grpc.ClientConn
	client       raft.RaftServiceClient
	leader       cluster.Member
	leaderConn   *grpc.ClientConn
	leaderClient raft.RaftServiceClient
	consistency  raft.ReadConsistency
	mu           sync.Mutex
}

// Connect connects the client to the given cluster
func (c *Client) Connect(config cluster.Cluster) error {
	c.members = make(map[string]cluster.Member)
	c.membersList = list.New()
	for name, member := range config.Members {
		c.members[name] = member
		c.membersList.PushBack(member)
	}
	return nil
}

// Write sends a write operation to the cluster
func (c *Client) Write(ctx context.Context, in []byte, ch chan<- node.Output) error {
	request := &raft.CommandRequest{
		Value: in,
	}

	errCh := make(chan error)
	go func() {
		if err := c.write(ctx, request, ch); err != nil {
			errCh <- err
		}
		close(errCh)
	}()
	return <-errCh
}

// Read sends a read operation to the cluster
func (c *Client) Read(ctx context.Context, in []byte, ch chan<- node.Output) error {
	request := &raft.QueryRequest{
		Value:           in,
		ReadConsistency: c.consistency,
	}

	errCh := make(chan error)
	go func() {
		if err := c.read(ctx, request, ch); err != nil {
			errCh <- err
		}
		close(errCh)
	}()
	return <-errCh
}

// resetLeaderConn resets the client's connection to the leader
func (c *Client) resetLeaderConn() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leaderConn != nil {
		_ = c.leaderConn.Close()
		c.leaderConn = nil
	}
	c.leader = cluster.Member{}
	c.leaderClient = nil
}

// getLeaderConn gets the gRPC client connection to the leader
func (c *Client) getLeaderConn() (*grpc.ClientConn, error) {
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
func (c *Client) getLeaderClient() (raft.RaftServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leaderClient == nil {
		conn, err := c.getLeaderConn()
		if err != nil {
			return nil, err
		}
		c.leaderClient = raft.NewRaftServiceClient(conn)
	}
	return c.leaderClient, nil
}

// write sends the given write request to the cluster
func (c *Client) write(ctx context.Context, request *raft.CommandRequest, ch chan<- node.Output) error {
	client, err := c.getLeaderClient()
	if err != nil {
		return err
	}

	log.Tracef("Sending CommandRequest %+v", request)
	stream, err := client.Command(ctx, request)
	if err != nil {
		return err
	}
	go c.receiveWrite(ctx, request, ch, stream)
	return nil
}

func (c *Client) receiveWrite(ctx context.Context, request *raft.CommandRequest, ch chan<- node.Output, stream raft.RaftService_CommandClient) {
	for {
		response, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				c.resetLeaderConn()
				ch <- node.Output{
					Error: err,
				}
			}
			close(ch)
			return
		}

		log.Tracef("Received CommandResponse %+v", response)
		if response.Status == raft.ResponseStatus_OK {
			ch <- node.Output{
				Value: response.Output,
			}
		} else if response.Error == raft.ResponseError_ILLEGAL_MEMBER_STATE {
			leaderID := string(response.Leader)
			if c.leader.ID == "" || leaderID != c.leader.ID {
				leader, ok := c.members[leaderID]
				if ok {
					c.resetLeaderConn()
					c.leader = leader
					if err := c.write(ctx, request, ch); err != nil {
						ch <- node.Output{
							Error: err,
						}
					}
					return
				}
				ch <- node.Output{
					Error: errors.New(response.Message),
				}
				return
			}
			ch <- node.Output{
				Error: errors.New(response.Message),
			}
			return
		} else {
			ch <- node.Output{
				Error: errors.New(response.Message),
			}
		}
	}
}

// resetConn resets the client connection to reconnect to a new member
func (c *Client) resetConn() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.memberConn != nil {
		_ = c.memberConn.Close()
		c.memberConn = nil
	}
	c.member = cluster.Member{}
	c.client = nil
}

// getConn gets the current gRPC client connection
func (c *Client) getConn() (*grpc.ClientConn, error) {
	if c.member.ID == "" {
		if c.memberNode == nil {
			c.memberNode = c.membersList.Front()
		} else {
			c.memberNode = c.memberNode.Next()
			if c.memberNode == nil {
				c.memberNode = c.membersList.Front()
			}
		}
		c.member = c.memberNode.Value.(cluster.Member)
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
func (c *Client) getClient() (raft.RaftServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client == nil {
		conn, err := c.getConn()
		if err != nil {
			return nil, err
		}
		c.client = raft.NewRaftServiceClient(conn)
	}
	return c.client, nil
}

// read sends the given read request to the cluster
func (c *Client) read(ctx context.Context, request *raft.QueryRequest, ch chan<- node.Output) error {
	client, err := c.getClient()
	if err != nil {
		close(ch)
		return err
	}

	log.Tracef("Sending QueryRequest %+v", request)
	stream, err := client.Query(ctx, request)
	if err != nil {
		close(ch)
		return err
	}
	go c.receiveRead(ctx, request, ch, stream)
	return nil
}

func (c *Client) receiveRead(ctx context.Context, request *raft.QueryRequest, ch chan<- node.Output, stream raft.RaftService_QueryClient) {
	for {
		response, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				ch <- node.Output{
					Error: err,
				}
			}
			close(ch)
			return
		}

		log.Tracef("Received QueryResponse %+v", response)
		if response.Status == raft.ResponseStatus_OK {
			ch <- node.Output{
				Value: response.Output,
			}
		} else if response.Error == raft.ResponseError_ILLEGAL_MEMBER_STATE {
			c.resetConn()
			if err := c.read(ctx, request, ch); err != nil {
				ch <- node.Output{
					Error: err,
				}
			}
			return
		} else {
			ch <- node.Output{
				Error: errors.New(response.Message),
			}
		}
	}
}

// Close closes the client
func (c *Client) Close() error {
	if c.memberConn != nil {
		_ = c.memberConn.Close()
	}
	if c.leaderConn != nil {
		_ = c.leaderConn.Close()
	}
	return nil
}
