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
	"github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	streams "github.com/atomix/atomix-go-node/pkg/atomix/stream"
	raft "github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
)

// NewClient returns a new Raft client
func NewClient(config cluster.Cluster, consistency raft.ReadConsistency) *Client {
	cluster := raft.NewCluster(config)
	return newClient(cluster, raft.NewClient(cluster), consistency)
}

// newClient returns a new Raft client
func newClient(cluster raft.Cluster, client raft.Client, consistency raft.ReadConsistency) *Client {
	members := list.New()
	for _, member := range cluster.Members() {
		members.PushBack(member)
	}
	return &Client{
		client:      client,
		members:     members,
		consistency: consistency,
		log:         util.NewNodeLogger(string(cluster.Member())),
	}
}

// Client is a service Client implementation for the Raft consensus protocol
type Client struct {
	node.Client
	members     *list.List
	memberNode  *list.Element
	member      *raft.MemberID
	leader      *raft.MemberID
	client      raft.Client
	consistency raft.ReadConsistency
	mu          sync.RWMutex
	log         util.Logger
}

// MustLeader returns whether requests must be handled by a leader
func (c *Client) MustLeader() bool {
	return false
}

// IsLeader returns whether this node is the leader
func (c *Client) IsLeader() bool {
	return false
}

// Leader returns the current leader
func (c *Client) Leader() string {
	return ""
}

// Write sends a write operation to the cluster
func (c *Client) Write(ctx context.Context, in []byte, stream streams.WriteStream) error {
	request := &raft.CommandRequest{
		Value: in,
	}

	errCh := make(chan error)
	go func() {
		if err := c.write(ctx, request, stream); err != nil {
			errCh <- err
		}
		close(errCh)
	}()
	return <-errCh
}

// Read sends a read operation to the cluster
func (c *Client) Read(ctx context.Context, in []byte, stream streams.WriteStream) error {
	request := &raft.QueryRequest{
		Value:           in,
		ReadConsistency: c.consistency,
	}

	errCh := make(chan error)
	go func() {
		if err := c.read(ctx, request, stream); err != nil {
			errCh <- err
		}
		close(errCh)
	}()
	return <-errCh
}

// getLeader gets the leader node or a random member
func (c *Client) getLeader() raft.MemberID {
	c.mu.RLock()
	if c.leader != nil {
		defer c.mu.RUnlock()
		return *c.leader
	}
	if c.member != nil {
		defer c.mu.RUnlock()
		return *c.member
	}
	c.mu.RUnlock()
	return c.getMemberSafe()
}

// resetLeader resets the leader
func (c *Client) resetLeader(expected raft.MemberID, leader *raft.MemberID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader == nil && leader != nil {
		c.leader = leader
		return true
	} else if c.leader != nil && leader == nil {
		c.leader = nil
		return true
	} else if c.leader != nil && leader != nil && *c.leader != *leader {
		c.leader = leader
		return true
	} else if c.leader == nil && leader == nil {
		if c.member != nil && *c.member == expected {
			c.member = nil
		}
		return true
	}
	return false
}

// write sends the given write request to the cluster
func (c *Client) write(ctx context.Context, request *raft.CommandRequest, stream streams.WriteStream) error {
	go c.sendWrite(ctx, request, stream)
	return nil
}

// retryWrite retries a write request
func (c *Client) retryWrite(ctx context.Context, request *raft.CommandRequest, stream streams.WriteStream, leader raft.MemberID) {
	c.resetLeader(leader, nil)
	go c.sendWrite(ctx, request, stream)
}

// sendWrite sends a write request
func (c *Client) sendWrite(ctx context.Context, request *raft.CommandRequest, stream streams.WriteStream) {
	leader := c.getLeader()
	c.log.Trace("Sending CommandRequest %+v to %s", request, leader)
	ch, err := c.client.Command(ctx, request, leader)
	if err != nil {
		c.log.Trace("Received CommandRequest error %s from %s", err, leader)
		if e, ok := status.FromError(err); ok {
			if e.Code() == codes.Unavailable {
				c.retryWrite(ctx, request, stream, leader)
				return
			}
		}
		stream.Error(err)
		stream.Close()
	} else {
		c.receiveWrite(ctx, request, stream, leader, ch)
	}
}

// receiveWrite process write responses
func (c *Client) receiveWrite(ctx context.Context, request *raft.CommandRequest, stream streams.WriteStream, leader raft.MemberID, ch <-chan *raft.CommandStreamResponse) {
	for streamResponse := range ch {
		if streamResponse.Failed() {
			c.log.Trace("Received CommandResponse error %s from %s", streamResponse.Error, leader)
			c.resetLeader(leader, nil)
			if e, ok := status.FromError(streamResponse.Error); ok {
				if e.Code() == codes.Unavailable {
					c.retryWrite(ctx, request, stream, leader)
					return
				}
			}

			stream.Error(streamResponse.Error)
			stream.Close()
			return
		}

		response := streamResponse.Response
		c.log.Trace("Received CommandResponse %+v from %s", response, leader)
		if response.Status == raft.ResponseStatus_OK {
			stream.Value(response.Output)
		} else if response.Error == raft.ResponseError_ILLEGAL_MEMBER_STATE {
			// If possible, update the current leader
			if leader == response.Leader {
				c.retryWrite(ctx, request, stream, leader)
			} else if response.Leader != "" && c.resetLeader(leader, &response.Leader) {
				c.sendWrite(ctx, request, stream)
			} else if response.Leader == "" && c.resetLeader(leader, nil) {
				c.sendWrite(ctx, request, stream)
			} else {
				stream.Error(errors.New(response.Message))
				stream.Close()
			}
			return
		} else {
			stream.Error(errors.New(response.Message))
		}
	}
	stream.Close()
}

// resetMember resets the member connection
func (c *Client) resetMember() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.member = nil
}

// getMember gets the current member
func (c *Client) getMember() raft.MemberID {
	c.mu.RLock()
	if c.member == nil {
		c.mu.RUnlock()
		return c.getMemberSafe()
	}
	defer c.mu.RUnlock()
	return *c.member
}

// getMemberSafe gets or sets the current member using a write lock
func (c *Client) getMemberSafe() raft.MemberID {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.member == nil {
		if c.memberNode != nil && c.memberNode.Next() != nil {
			c.memberNode = c.memberNode.Next()
		} else {
			c.memberNode = c.members.Front()
		}
		member := c.memberNode.Value.(raft.MemberID)
		c.member = &member
	}
	return *c.member
}

// read sends the given read request to the cluster
func (c *Client) read(ctx context.Context, request *raft.QueryRequest, stream streams.WriteStream) error {
	go c.sendRead(ctx, request, stream)
	return nil
}

// retryRead retries a read request
func (c *Client) retryRead(ctx context.Context, request *raft.QueryRequest, stream streams.WriteStream) {
	c.resetMember()
	go c.sendRead(ctx, request, stream)
}

// sendRead sends a read request
func (c *Client) sendRead(ctx context.Context, request *raft.QueryRequest, stream streams.WriteStream) {
	member := c.getMember()
	c.log.Trace("Sending QueryRequest %+v to %s", request, member)
	ch, err := c.client.Query(ctx, request, member)
	if err != nil {
		c.log.Trace("Received QueryRequest error %s from %s", err, member)
		if e, ok := status.FromError(err); ok {
			if e.Code() == codes.Unavailable {
				c.retryRead(ctx, request, stream)
				return
			}
		}
		stream.Error(err)
		stream.Close()
	} else {
		c.receiveRead(ctx, request, stream, member, ch)
	}
}

func (c *Client) receiveRead(ctx context.Context, request *raft.QueryRequest, stream streams.WriteStream, member raft.MemberID, ch <-chan *raft.QueryStreamResponse) {
	for streamResponse := range ch {
		if streamResponse.Failed() {
			c.log.Trace("Received QueryResponse error %s from %s", streamResponse.Error, member)
			if e, ok := status.FromError(streamResponse.Error); ok {
				if e.Code() == codes.Unavailable {
					c.retryRead(ctx, request, stream)
					return
				}
			}

			stream.Error(streamResponse.Error)
			stream.Close()
			return
		}

		response := streamResponse.Response
		c.log.Trace("Received QueryResponse %+v from %s", response, member)
		if response.Status == raft.ResponseStatus_OK {
			stream.Value(response.Output)
		} else if response.Error == raft.ResponseError_ILLEGAL_MEMBER_STATE {
			c.resetMember()
			c.sendRead(ctx, request, stream)
			return
		} else {
			stream.Error(errors.New(response.Message))
		}
	}
	stream.Close()
}

// Close closes the client
func (c *Client) Close() error {
	return nil
}
