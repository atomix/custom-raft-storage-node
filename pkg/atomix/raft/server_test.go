package raft

import (
	"context"
	"github.com/atomix/atomix-go-node/pkg/atomix"
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
	defer server.Stop()
	server.waitForReady()

	client := newRaftClient(ReadConsistency_SEQUENTIAL)
	assert.NoError(t, client.Connect(cluster))

	ch := make(chan service.Output)
	assert.NoError(t, client.Write(context.Background(), newOpenSessionRequest(t), ch))
	out := <-ch
	assert.True(t, out.Succeeded())
	openSessionResponse := getOpenSessionResponse(t, out.Value)
	assert.NotEqual(t, 0, openSessionResponse.SessionId)
	sessionID := openSessionResponse.SessionId

	ch = make(chan service.Output)
	bytes, err := proto.Marshal(&SetRequest{
		Value: "Hello world!",
	})
	assert.NoError(t, err)
	assert.NoError(t, client.Write(context.Background(), newCommandRequest(t, sessionID, 1, "set", bytes), ch))
	out = <-ch
	assert.True(t, out.Succeeded())
	commandResponse := getCommandResponse(t, out.Value)
	setResponse := &SetResponse{}
	assert.NoError(t, proto.Unmarshal(commandResponse.Output, setResponse))

	ch = make(chan service.Output)
	bytes, err = proto.Marshal(&GetRequest{})
	assert.NoError(t, err)
	assert.NoError(t, client.Read(context.Background(), newQueryRequest(t, sessionID, commandResponse.Context.Index, 1, "get", bytes), ch))
	out = <-ch
	assert.True(t, out.Succeeded())
	queryResponse := getQueryResponse(t, out.Value)
	getResponse := &GetResponse{}
	assert.NoError(t, proto.Unmarshal(queryResponse.Output, getResponse))
	assert.Equal(t, "Hello world!", getResponse.Value)
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

	defer serverFoo.Stop()
	defer serverBar.Stop()
	defer serverBaz.Stop()
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

func newOpenSessionRequest(t *testing.T) []byte {
	bytes, err := proto.Marshal(&service.SessionRequest{
		Request: &service.SessionRequest_OpenSession{
			OpenSession: &service.OpenSessionRequest{
				Timeout: int64(30 * time.Second),
			},
		},
	})
	assert.NoError(t, err)
	return newTestCommandRequest(t, bytes)
}

func getOpenSessionResponse(t *testing.T, bytes []byte) *service.OpenSessionResponse {
	serviceResponse := &service.ServiceResponse{}
	assert.NoError(t, proto.Unmarshal(bytes, serviceResponse))
	sessionResponse := &service.SessionResponse{}
	assert.NoError(t, proto.Unmarshal(serviceResponse.GetCommand(), sessionResponse))
	return sessionResponse.GetOpenSession()
}

func newKeepAliveRequest(t *testing.T, sessionID uint64, commandID uint64, streams map[uint64]uint64) []byte {
	bytes, err := proto.Marshal(&service.SessionRequest{
		Request: &service.SessionRequest_KeepAlive{
			KeepAlive: &service.KeepAliveRequest{
				SessionId:       sessionID,
				CommandSequence: commandID,
				Streams:         streams,
			},
		},
	})
	assert.NoError(t, err)
	return newTestCommandRequest(t, bytes)
}

func newCloseSessionRequest(t *testing.T, sessionID uint64) []byte {
	bytes, err := proto.Marshal(&service.SessionRequest{
		Request: &service.SessionRequest_CloseSession{
			CloseSession: &service.CloseSessionRequest{
				SessionId: sessionID,
			},
		},
	})
	assert.NoError(t, err)
	return newTestCommandRequest(t, bytes)
}

func newCommandRequest(t *testing.T, sessionID uint64, commandID uint64, name string, bytes []byte) []byte {
	bytes, err := proto.Marshal(&service.SessionRequest{
		Request: &service.SessionRequest_Command{
			Command: &service.SessionCommandRequest{
				Context: &service.SessionCommandContext{
					SessionId:      sessionID,
					SequenceNumber: commandID,
				},
				Name:  name,
				Input: bytes,
			},
		},
	})
	assert.NoError(t, err)
	return newTestCommandRequest(t, bytes)
}

func getCommandResponse(t *testing.T, bytes []byte) *service.SessionCommandResponse {
	serviceResponse := &service.ServiceResponse{}
	assert.NoError(t, proto.Unmarshal(bytes, serviceResponse))
	sessionResponse := &service.SessionResponse{}
	assert.NoError(t, proto.Unmarshal(serviceResponse.GetCommand(), sessionResponse))
	return sessionResponse.GetCommand()
}

func newQueryRequest(t *testing.T, sessionID uint64, lastIndex uint64, lastCommandID uint64, name string, bytes []byte) []byte {
	bytes, err := proto.Marshal(&service.SessionRequest{
		Request: &service.SessionRequest_Query{
			Query: &service.SessionQueryRequest{
				Context: &service.SessionQueryContext{
					SessionId:          sessionID,
					LastIndex:          lastIndex,
					LastSequenceNumber: lastCommandID,
				},
				Name:  name,
				Input: bytes,
			},
		},
	})
	assert.NoError(t, err)
	return newTestQueryRequest(t, bytes)
}

func getQueryResponse(t *testing.T, bytes []byte) *service.SessionQueryResponse {
	serviceResponse := &service.ServiceResponse{}
	assert.NoError(t, proto.Unmarshal(bytes, serviceResponse))
	sessionResponse := &service.SessionResponse{}
	assert.NoError(t, proto.Unmarshal(serviceResponse.GetQuery(), sessionResponse))
	return sessionResponse.GetQuery()
}

func newTestCommandRequest(t *testing.T, bytes []byte) []byte {
	bytes, err := proto.Marshal(&service.ServiceRequest{
		Id: &service.ServiceId{
			Type:      "test",
			Name:      "test",
			Namespace: "test",
		},
		Request: &service.ServiceRequest_Command{
			Command: bytes,
		},
	})
	assert.NoError(t, err)
	return bytes
}

func newTestQueryRequest(t *testing.T, bytes []byte) []byte {
	bytes, err := proto.Marshal(&service.ServiceRequest{
		Id: &service.ServiceId{
			Type:      "test",
			Name:      "test",
			Namespace: "test",
		},
		Request: &service.ServiceRequest_Query{
			Query: bytes,
		},
	})
	assert.NoError(t, err)
	return bytes
}

func getServiceRegistry() *service.ServiceRegistry {
	registry := service.NewServiceRegistry()
	registerTestService(registry)
	return registry
}

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.TraceLevel)
}
