package raft

import (
	"errors"
	"fmt"
	"github.com/atomix/atomix-go-node/pkg/atomix"
	"google.golang.org/grpc"
	"sync"
	"time"
)

// newRaftCluster returns a new RaftCluster with the given configuration
func newRaftCluster(cluster atomix.Cluster) *RaftCluster {
	members := make(map[string]*RaftMember)
	memberIDs := make([]string, 0, len(cluster.Members))
	for id, member := range cluster.Members {
		members[id] = &RaftMember{
			MemberId: member.ID,
			Type:     RaftMember_ACTIVE,
			Updated:  time.Now().UnixNano(),
		}
		memberIDs = append(memberIDs, id)
	}
	return &RaftCluster{
		member:    cluster.MemberID,
		members:   members,
		memberIDs: memberIDs,
		locations: cluster.Members,
		conns:     make(map[string]*grpc.ClientConn),
		clients:   make(map[string]RaftServiceClient),
	}
}

// RaftCluster manages the Raft cluster configuration
type RaftCluster struct {
	member    string
	members   map[string]*RaftMember
	memberIDs []string
	locations map[string]atomix.Member
	conns     map[string]*grpc.ClientConn
	clients   map[string]RaftServiceClient
	mu        sync.RWMutex
}

// getClient returns a connection for the given member
func (c *RaftCluster) getConn(member string) (*grpc.ClientConn, error) {
	_, ok := c.members[member]
	if !ok {
		return nil, errors.New(fmt.Sprintf("unknown member %s", member))
	}

	conn, ok := c.conns[member]
	if !ok {
		location, ok := c.locations[member]
		if !ok {
			return nil, errors.New(fmt.Sprintf("unknown member %s", member))
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

func (c *RaftCluster) getClient(member string) (RaftServiceClient, error) {
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

func (c *RaftCluster) resetClient(member string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	conn, ok := c.conns[member]
	if ok {
		conn.Close()
		delete(c.conns, member)
		delete(c.clients, member)
	}
}
