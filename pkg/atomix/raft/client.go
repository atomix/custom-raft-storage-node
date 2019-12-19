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
	"fmt"
	"github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	streams "github.com/atomix/atomix-go-node/pkg/atomix/stream"
	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"time"
)

const clientTimeout = 15 * time.Second

// newClient returns a new Raft consensus protocol client
func newClient(cluster cluster.Cluster, r *raft.Raft, fsm *StateMachine, streams *streamManager) *Client {
	member := cluster.Members[cluster.MemberID]
	address := fmt.Sprintf("%s:%d", member.Host, member.Port)
	return &Client{
		address: raft.ServerAddress(address),
		cluster: cluster,
		raft:    r,
		state:   fsm,
		streams: streams,
	}
}

// Client is the Raft client
type Client struct {
	address raft.ServerAddress
	cluster cluster.Cluster
	raft    *raft.Raft
	state   *StateMachine
	streams *streamManager
}

func (c *Client) MustLeader() bool {
	return true
}

func (c *Client) IsLeader() bool {
	return c.raft.Leader() == c.address
}

func (c *Client) Leader() string {
	return string(c.raft.Leader())
}

func (c *Client) Write(ctx context.Context, input []byte, stream streams.Stream) error {
	streamID, ch := c.streams.newStream()
	entry := &Entry{
		Value:     input,
		StreamID:  streamID,
		Timestamp: time.Now(),
	}
	bytes, err := proto.Marshal(entry)
	if err != nil {
		return err
	}
	go func() {
		for result := range ch {
			stream.Send(result)
		}
		c.streams.deleteStream(streamID)
	}()
	future := c.raft.Apply(bytes, clientTimeout)
	if future.Error() != nil {
		c.streams.deleteStream(streamID)
		return future.Error()
	}
	response := future.Response()
	if response != nil {
		return response.(error)
	}
	return nil
}

func (c *Client) Read(ctx context.Context, input []byte, stream streams.Stream) error {
	resultCh := make(chan streams.Result)
	go func() {
		for result := range resultCh {
			stream.Send(result)
		}
	}()
	return c.state.Query(input, resultCh)
}
