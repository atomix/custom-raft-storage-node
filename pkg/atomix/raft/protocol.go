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
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/client"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/config"
	raft "github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol"
)

// NewProtocol returns a new Raft Protocol instance
func NewProtocol(config *config.ProtocolConfig) *Protocol {
	return &Protocol{
		config: config,
	}
}

// Protocol is an implementation of the Protocol interface providing the Raft consensus protocol
type Protocol struct {
	node.Protocol
	config *config.ProtocolConfig
	client *client.Client
	server *Server
}

// Start starts the Raft protocol
func (p *Protocol) Start(cluster cluster.Cluster, registry *node.Registry) error {
	p.client = client.NewClient(raft.ReadConsistency_SEQUENTIAL)
	if err := p.client.Connect(cluster); err != nil {
		return err
	}

	p.server = NewServer(cluster, registry, p.config)
	go p.server.Start()
	return p.server.WaitForReady()
}

// Client returns the Raft protocol client
func (p *Protocol) Client() node.Client {
	return p.client
}

// Stop stops the Raft protocol
func (p *Protocol) Stop() error {
	_ = p.client.Close()
	return p.server.Stop()
}
