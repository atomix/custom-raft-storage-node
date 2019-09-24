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
	raft "github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol"
	log "github.com/sirupsen/logrus"
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
func (c *Client) resetLeader(leader *raft.MemberID) bool {
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
	}
	return false
}

// write sends the given write request to the cluster
func (c *Client) write(ctx context.Context, request *raft.CommandRequest, ch chan<- node.Output) error {
	log.Tracef("Sending CommandRequest %+v", request)
	stream, err := c.client.Command(ctx, request, c.getLeader())
	if err != nil {
		return err
	}
	go c.receiveWrite(ctx, request, ch, stream)
	return nil
}

// receiveWrite process write responses
func (c *Client) receiveWrite(ctx context.Context, request *raft.CommandRequest, ch chan<- node.Output, stream <-chan *raft.CommandStreamResponse) {
	for streamResponse := range stream {
		if streamResponse.Failed() {
			c.resetLeader(nil)
			ch <- node.Output{
				Error: streamResponse.Error,
			}
			close(ch)
			return
		}

		response := streamResponse.Response
		log.Tracef("Received CommandResponse %+v", response)
		if response.Status == raft.ResponseStatus_OK {
			ch <- node.Output{
				Value: response.Output,
			}
		} else if response.Error == raft.ResponseError_ILLEGAL_MEMBER_STATE {
			// If possible, update the current leader
			if c.resetLeader(&response.Leader) {
				if err := c.write(ctx, request, ch); err != nil {
					ch <- node.Output{
						Error: err,
					}
					close(ch)
				}
				return
			}
			c.mu.Unlock()

			ch <- node.Output{
				Error: errors.New(response.Message),
			}
			close(ch)
			return
		} else {
			ch <- node.Output{
				Error: errors.New(response.Message),
			}
		}
	}
	close(ch)
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
func (c *Client) read(ctx context.Context, request *raft.QueryRequest, ch chan<- node.Output) error {
	log.Tracef("Sending QueryRequest %+v", request)
	stream, err := c.client.Query(ctx, request, c.getMember())
	if err != nil {
		close(ch)
		return err
	}
	go c.receiveRead(ctx, request, ch, stream)
	return nil
}

func (c *Client) receiveRead(ctx context.Context, request *raft.QueryRequest, ch chan<- node.Output, stream <-chan *raft.QueryStreamResponse) {
	for streamResponse := range stream {
		if streamResponse.Failed() {
			ch <- node.Output{
				Error: streamResponse.Error,
			}
			close(ch)
			return
		}

		response := streamResponse.Response
		log.Tracef("Received QueryResponse %+v", response)
		if response.Status == raft.ResponseStatus_OK {
			ch <- node.Output{
				Value: response.Output,
			}
		} else if response.Error == raft.ResponseError_ILLEGAL_MEMBER_STATE {
			c.resetMember()
			if err := c.read(ctx, request, ch); err != nil {
				ch <- node.Output{
					Error: err,
				}
				close(ch)
			}
			return
		} else {
			ch <- node.Output{
				Error: errors.New(response.Message),
			}
		}
	}
	close(ch)
}

// Close closes the client
func (c *Client) Close() error {
	return nil
}
