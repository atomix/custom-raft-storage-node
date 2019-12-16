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
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	"github.com/hashicorp/raft"
	"time"
)

const clientTimeout = 15 * time.Second

// newClient returns a new Raft consensus protocol client
func newClient(r *raft.Raft, fsm *StateMachine) *Client {
	return &Client{
		raft:  r,
		state: fsm,
	}
}

// Client is the Raft client
type Client struct {
	raft  *raft.Raft
	state *StateMachine
}

func (c *Client) Write(ctx context.Context, input []byte, ch chan<- node.Output) error {
	future := c.raft.Apply(input, clientTimeout)
	if future.Error() != nil {
		return future.Error()
	}
	responseCh := future.Response().(chan node.Output)
	go func() {
		for response := range responseCh {
			ch <- response
		}
	}()
	return nil
}

func (c *Client) Read(ctx context.Context, input []byte, ch chan<- node.Output) error {
	responseCh := c.state.Query(input)
	go func() {
		for response := range responseCh {
			ch <- response
		}
	}()
	return nil
}
