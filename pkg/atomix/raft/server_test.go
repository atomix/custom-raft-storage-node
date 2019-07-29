package raft

import (
	"context"
	"github.com/atomix/atomix-go-node/pkg/atomix"
	"github.com/atomix/atomix-go-node/pkg/atomix/map"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
	"time"
)

func TestRaftNode(t *testing.T) {
	cluster := atomix.Cluster{
		MemberID: "foo",
		Members: map[string]atomix.Member{
			"foo": {
				ID:   "foo",
				Host: "localhost",
				Port: 5001,
			},
		},
	}

	server := newServer("foo", cluster)
	go server.Start()
	server.waitForReady()

	client := newRaftClient(ReadConsistency_SEQUENTIAL)
	assert.NoError(t, client.Connect(cluster))

	bytes, err := proto.Marshal(&service.SessionRequest{
		Request: &service.SessionRequest_OpenSession{
			OpenSession: &service.OpenSessionRequest{
				Timeout: int64(30 * time.Second),
			},
		},
	})
	assert.NoError(t, err)
	bytes, _ = proto.Marshal(&service.ServiceRequest{
		Id: &service.ServiceId{
			Type:      "map",
			Name:      "test",
			Namespace: "test",
		},
		Request: &service.ServiceRequest_Command{
			Command: bytes,
		},
	})
	ch := make(chan service.Output)
	assert.NoError(t, client.Write(context.Background(), bytes, ch))
	out := <-ch

	serviceResponse := &service.ServiceResponse{}
	proto.Unmarshal(out.Value, serviceResponse)
	sessionResponse := &service.SessionResponse{}
	proto.Unmarshal(serviceResponse.GetCommand(), sessionResponse)
	assert.NotEqual(t, 0, sessionResponse.GetCommand().Context.Index)
}

func TestRaftCluster(t *testing.T) {
	cluster := atomix.Cluster{
		MemberID: "foo",
		Members: map[string]atomix.Member{
			"foo": {
				ID:   "foo",
				Host: "localhost",
				Port: 5001,
			},
			"bar": {
				ID:   "bar",
				Host: "localhost",
				Port: 5002,
			},
			"baz": {
				ID:   "baz",
				Host: "localhost",
				Port: 5003,
			},
		},
	}

	serverFoo := newServer("foo", cluster)
	serverBar := newServer("bar", cluster)
	serverBaz := newServer("baz", cluster)

	wg := &sync.WaitGroup{}
	wg.Add(3)
	go startServer(serverFoo, wg)
	go startServer(serverBar, wg)
	go startServer(serverBaz, wg)
	wg.Wait()
}

func newServer(memberID string, cluster atomix.Cluster) *RaftServer {
	cluster.MemberID = memberID
	return NewRaftServer(cluster, getServiceRegistry(), 5*time.Second)
}

func startServer(server *RaftServer, wg *sync.WaitGroup) {
	defer wg.Done()
	go func() {
		if err := server.Start(); err != nil {
			wg.Done()
		}
	}()
	server.waitForReady()
}

func getServiceRegistry() *service.ServiceRegistry {
	registry := service.NewServiceRegistry()
	_map.RegisterMapService(registry)
	return registry
}

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.TraceLevel)
}
