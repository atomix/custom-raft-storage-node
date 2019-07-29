package raft

import (
	"github.com/atomix/atomix-go-node/pkg/atomix"
	"google.golang.org/grpc"
	"time"
)

// newRaftCluster returns a new RaftCluster with the given configuration
func newRaftCluster(cluster atomix.Cluster) *RaftCluster {
	members := make(map[string]*RaftMember)
	for id, member := range cluster.Members {
		members[id] = &RaftMember{
			MemberId: member.ID,
			Type:     RaftMember_ACTIVE,
			Updated:  time.Now().UnixNano(),
		}
	}
	return &RaftCluster{
		member:    cluster.MemberID,
		members:   members,
		locations: cluster.Members,
		conns:     make(map[string]*grpc.ClientConn),
	}
}

// RaftCluster manages the Raft cluster configuration
type RaftCluster struct {
	member    string
	members   map[string]*RaftMember
	memberIDs []string
	locations map[string]atomix.Member
	conns     map[string]*grpc.ClientConn
}
