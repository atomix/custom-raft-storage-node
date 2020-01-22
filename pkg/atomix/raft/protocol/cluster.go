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

package protocol

import (
	"fmt"
	node "github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"google.golang.org/grpc"
	"sync"
	"time"
)

// Cluster provides cluster information for the Raft protocol
type Cluster interface {
	// Member returns the local member ID
	Member() MemberID

	// Members returns a list of all members in the Raft cluster
	Members() []MemberID

	// GetMember returns a Member by ID
	GetMember(memberID MemberID) *Member

	// GetClient gets a RaftServiceClient connection for the given member
	GetClient(memberID MemberID) (RaftServiceClient, error)
}

// NewCluster returns a new Cluster with the given configuration
func NewCluster(config node.Cluster) Cluster {
	members := make(map[MemberID]*Member)
	locations := make(map[MemberID]node.Member)
	memberIDs := make([]MemberID, 0, len(config.Members))
	for id, member := range config.Members {
		members[MemberID(id)] = &Member{
			MemberID: MemberID(member.ID),
			Type:     Member_ACTIVE,
			Updated:  time.Now(),
		}
		locations[MemberID(id)] = member
		memberIDs = append(memberIDs, MemberID(id))
	}
	return &cluster{
		member:    MemberID(config.MemberID),
		members:   members,
		memberIDs: memberIDs,
		locations: locations,
		conns:     make(map[MemberID]*grpc.ClientConn),
		clients:   make(map[MemberID]RaftServiceClient),
	}
}

// Cluster manages the Raft cluster configuration
type cluster struct {
	member    MemberID
	members   map[MemberID]*Member
	memberIDs []MemberID
	locations map[MemberID]node.Member
	conns     map[MemberID]*grpc.ClientConn
	clients   map[MemberID]RaftServiceClient
	mu        sync.RWMutex
}

func (c *cluster) Member() MemberID {
	return c.member
}

func (c *cluster) Members() []MemberID {
	return c.memberIDs
}

func (c *cluster) GetMember(memberID MemberID) *Member {
	return c.members[memberID]
}

// getConn returns a connection for the given member
func (c *cluster) getConn(member MemberID) (*grpc.ClientConn, error) {
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

		conn, err := grpc.Dial(fmt.Sprintf("%s:%d", location.Host, location.ProtocolPort), grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		c.conns[member] = conn
		return conn, nil
	}
	return conn, nil
}

// getClient gets the RaftServiceClient for the given member
func (c *cluster) GetClient(member MemberID) (RaftServiceClient, error) {
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
