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
	"google.golang.org/grpc"
	"sync"
	"time"
)

// newCluster returns a new Cluster with the given configuration
func newCluster(config cluster.Cluster) *Cluster {
	members := make(map[MemberID]*RaftMember)
	locations := make(map[MemberID]cluster.Member)
	memberIDs := make([]MemberID, 0, len(config.Members))
	for id, member := range config.Members {
		members[MemberID(id)] = &RaftMember{
			MemberID: MemberID(member.ID),
			Type:     RaftMember_ACTIVE,
			Updated:  time.Now(),
		}
		locations[MemberID(id)] = member
		memberIDs = append(memberIDs, MemberID(id))
	}
	return &Cluster{
		member:    MemberID(config.MemberID),
		members:   members,
		memberIDs: memberIDs,
		locations: locations,
		conns:     make(map[MemberID]*grpc.ClientConn),
		clients:   make(map[MemberID]RaftServiceClient),
	}
}

// Cluster manages the Raft cluster configuration
type Cluster struct {
	member    MemberID
	members   map[MemberID]*RaftMember
	memberIDs []MemberID
	locations map[MemberID]cluster.Member
	conns     map[MemberID]*grpc.ClientConn
	clients   map[MemberID]RaftServiceClient
	mu        sync.RWMutex
}

// getConn returns a connection for the given member
func (c *Cluster) getConn(member MemberID) (*grpc.ClientConn, error) {
	_, ok := c.members[member]
	if !ok {
		return nil, fmt.Errorf("unknown member %s", member)
	}

	conn, ok := c.conns[member]
	if !ok {
		location, ok := c.locations[member]
		if !ok {
			return nil, fmt.Errorf("unknown member %s", member)
		}

		conn, err := grpc.Dial(fmt.Sprintf("%s:%d", location.Host, location.Port), grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		c.conns[member] = conn
		return conn, nil
	}
	return conn, nil
}

// getClient gets the RaftServiceClient for the given member
func (c *Cluster) getClient(member MemberID) (RaftServiceClient, error) {
	c.mu.RLock()
	client, ok := c.clients[member]
	c.mu.RUnlock()
	if !ok {
		c.mu.Lock()
		client, ok = c.clients[member]
		if !ok {
			conn, err := c.getConn(member)
			if err != nil {
				c.mu.Unlock()
				return nil, err
			}
			client = NewRaftServiceClient(conn)
			c.clients[member] = client
			c.mu.Unlock()
		}
	}
	return client, nil
}

// resetClient resets the client/connection to the given member
func (c *Cluster) resetClient(member MemberID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	conn, ok := c.conns[member]
	if ok {
		_ = conn.Close()
		delete(c.conns, member)
		delete(c.clients, member)
	}
}
