package raft

import (
	"github.com/atomix/atomix-go-node/pkg/atomix"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"time"
)

// RaftProtocol is an implementation of the Protocol interface providing the Raft consensus protocol
type RaftProtocol struct {
	atomix.Protocol
	config *RaftProtocolConfig
	client *RaftClient
	server *RaftServer
}

func (p *RaftProtocol) Start(cluster atomix.Cluster, registry *service.ServiceRegistry) error {
	p.server = newRaftServer(cluster, 5*time.Second)
	if err := p.server.Start(); err != nil {
		return err
	}

	p.client = newRaftClient()
	if err := p.client.Connect(cluster); err != nil {
		return err
	}
	return nil
}

func (p *RaftProtocol) Client() service.Client {
	return p.client
}

func (p *RaftProtocol) Stop() error {
	p.client.Close()
	return p.server.Stop()
}
