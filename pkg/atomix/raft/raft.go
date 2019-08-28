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
	"github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"time"
)

// MemberID is the ID of a Raft cluster member
type MemberID string

// Index is a Raft log index
type Index uint64

// Term is a Raft term
type Term uint64

// NewProtocol returns a new Raft Protocol instance
func NewProtocol(config *RaftProtocolConfig) *Protocol {
	return &Protocol{
		config: config,
	}
}

// Protocol is an implementation of the Protocol interface providing the Raft consensus protocol
type Protocol struct {
	service.Protocol
	config *RaftProtocolConfig
	client *Client
	server *Server
}

// Start starts the Raft protocol
func (p *Protocol) Start(cluster cluster.Cluster, registry *service.Registry) error {
	electionTimeout := 5 * time.Second
	if p.config.ElectionTimeout != nil {
		electionTimeout = *p.config.ElectionTimeout
	}

	p.client = newClient(ReadConsistency_SEQUENTIAL)
	if err := p.client.Connect(cluster); err != nil {
		return err
	}

	p.server = NewServer(cluster, registry, electionTimeout)
	go func() {
		_ = p.server.Start()
	}()
	return p.server.waitForReady()
}

// Client returns the Raft protocol client
func (p *Protocol) Client() service.Client {
	return p.client
}

// Stop stops the Raft protocol
func (p *Protocol) Stop() error {
	_ = p.client.Close()
	return p.server.Stop()
}
