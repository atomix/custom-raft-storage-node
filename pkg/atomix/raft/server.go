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
	"errors"
	"fmt"
	"github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/config"
	raft "github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/roles"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/state"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/store"
	"google.golang.org/grpc"
	"net"
	"sync"
)

// NewServer returns a new Raft consensus protocol server
func NewServer(clusterConfig cluster.Cluster, registry *node.Registry, protocolConfig *config.ProtocolConfig) *Server {
	member, ok := clusterConfig.Members[clusterConfig.MemberID]
	if !ok {
		panic("Local member is not present in cluster configuration!")
	}

	cluster := raft.NewCluster(clusterConfig)
	protocol := raft.NewClient(cluster)
	raft := raft.NewRaft(cluster, protocolConfig, protocol)
	store := store.NewMemoryStore()
	state := state.NewManager(raft, store, registry)
	server := &Server{
		raft:  raft,
		state: state,
		store: store,
		port:  member.Port,
		mu:    sync.Mutex{},
	}
	return server
}

// Server implements the Raft consensus protocol server
type Server struct {
	raft   raft.Raft
	state  state.Manager
	store  store.Store
	server *grpc.Server
	port   int
	mu     sync.Mutex
}

// Start starts the Raft server
func (s *Server) Start() error {
	s.mu.Lock()

	// Initialize the Raft state
	s.raft.Init()

	// Transition the node to the follower role
	go s.raft.SetRole(roles.NewInitialRole(s.raft, s.state, s.store))

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return err
	}

	s.server = grpc.NewServer()
	raft.RegisterRaftServiceServer(s.server, s.raft)
	s.mu.Unlock()
	return s.server.Serve(lis)
}

// WaitForReady blocks the current goroutine until the server is ready
func (s *Server) WaitForReady() error {
	ch := make(chan struct{})
	s.raft.WatchStatus(func(status raft.Status) {
		if status == raft.StatusReady {
			ch <- struct{}{}
			close(ch)
		}
	})
	_, ok := <-ch
	if ok {
		return nil
	}
	return errors.New("server stopped")
}

// Stop shuts down the Raft server
func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.server != nil {
		s.server.Stop()
	}
	s.raft.Close()
	s.state.Close()
	s.store.Close()
	return nil
}
