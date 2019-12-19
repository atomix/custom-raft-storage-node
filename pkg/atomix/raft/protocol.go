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
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/config"
)

// NewProtocol returns a new Raft Protocol instance
func NewProtocol(config *config.ProtocolConfig) *Protocol {
	return &Protocol{
		config: config,
	}
}

// Protocol is an implementation of the Client interface providing the Raft consensus protocol
type Protocol struct {
	node.Protocol
	config *config.ProtocolConfig
	client *Client
	server *Server
}

// Start starts the Raft protocol
func (p *Protocol) Start(cluster cluster.Cluster, registry *node.Registry) error {
	streams := newStreamManager()
	fsm := newStateMachine(cluster, registry, streams)
	address, raft, err := newRaft(cluster, p.config, fsm)
	if err != nil {
		return err
	}
	ports := make(map[string]int)
	for _, member := range cluster.Members {
		ports[member.Host] = member.APIPort
	}
	p.server = newServer(cluster, raft)
	p.client = newClient(address, ports, raft, fsm, streams)
	go p.server.Start()
	return nil
}

// Client returns the Raft protocol client
func (p *Protocol) Client() node.Client {
	return p.client
}

// Stop stops the Raft protocol
func (p *Protocol) Stop() error {
	return p.server.Stop()
}
