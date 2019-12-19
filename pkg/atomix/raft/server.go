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
	"fmt"
	"github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/hashicorp/raft"
	"sort"
)

// newServer returns a new Raft consensus protocol server
func newServer(cluster cluster.Cluster, r *raft.Raft) *Server {
	servers := make([]raft.Server, 0, len(cluster.Members))
	for memberID, member := range cluster.Members {
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(memberID),
			Address: raft.ServerAddress(fmt.Sprintf("%s:%d", member.Host, member.ProtocolPort)),
		})
	}
	sort.Slice(servers, func(i, j int) bool {
		return servers[i].ID < servers[j].ID
	})
	config := raft.Configuration{
		Servers: servers,
	}
	return &Server{
		raft:   r,
		config: config,
	}
}

// Server implements the Raft consensus protocol server
type Server struct {
	raft   *raft.Raft
	config raft.Configuration
}

// Start starts the Raft server
func (s *Server) Start() error {
	if err := s.raft.BootstrapCluster(s.config); err.Error() != nil {
		return err.Error()
	}
	return nil
}

// Stop shuts down the Raft server
func (s *Server) Stop() error {
	return s.raft.Shutdown().Error()
}
