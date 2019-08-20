package raft

import (
	"github.com/atomix/atomix-go-node/pkg/atomix"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"time"
)

// MemberID is the ID of a Raft cluster member
type MemberID string

// Index is a Raft log index
type Index uint64

// Term is a Raft term
type Term uint64

func NewRaftProtocol(config *RaftProtocolConfig) *RaftProtocol {
	return &RaftProtocol{
		config: config,
	}
}

// RaftProtocol is an implementation of the Protocol interface providing the Raft consensus protocol
type RaftProtocol struct {
	atomix.Protocol
	config *RaftProtocolConfig
	client *RaftClient
	server *RaftServer
}

func (p *RaftProtocol) Start(cluster atomix.Cluster, registry *service.ServiceRegistry) error {
	electionTimeout := 5 * time.Second
	if p.config.ElectionTimeout != nil {
		electionTimeout = *p.config.ElectionTimeout
	}

	p.client = newRaftClient(ReadConsistency_SEQUENTIAL)
	if err := p.client.Connect(cluster); err != nil {
		return err
	}

	p.server = NewRaftServer(cluster, registry, electionTimeout)
	go p.server.Start()
	return p.server.waitForReady()
}

func (p *RaftProtocol) Client() service.Client {
	return p.client
}

func (p *RaftProtocol) Stop() error {
	p.client.Close()
	return p.server.Stop()
}
