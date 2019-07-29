package raft

import (
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/golang/protobuf/proto"
)

// registerTestService registers the test service in the given service registry
func registerTestService(registry *service.ServiceRegistry) {
	registry.Register("test", newTestService)
}

// newTestService returns a new TestService
func newTestService(context service.Context) service.Service {
	service := &TestService{
		SessionizedService: service.NewSessionizedService(context),
	}
	service.init()
	return service
}

// TestService is a state machine for a test primitive
type TestService struct {
	*service.SessionizedService
	value string
}

// init initializes the test service
func (s *TestService) init() {
	s.Executor.Register("set", s.Set)
	s.Executor.Register("get", s.Get)
}

// Backup backs up the map service
func (s *TestService) Backup() ([]byte, error) {
	snapshot := &TestValueSnapshot{
		Value: s.value,
	}
	return proto.Marshal(snapshot)
}

// Restore restores the map service
func (s *TestService) Restore(bytes []byte) error {
	snapshot := &TestValueSnapshot{}
	if err := proto.Unmarshal(bytes, snapshot); err != nil {
		return err
	}
	s.value = snapshot.Value
	return nil
}

func (s *TestService) Get(value []byte, ch chan<- service.Result) {
	ch <- s.NewResult(proto.Marshal(&GetResponse{
		Value: s.value,
	}))
}

func (s *TestService) Set(value []byte, ch chan<- service.Result) {
	request := &SetRequest{}
	if err := proto.Unmarshal(value, request); err != nil {
		ch <- s.NewFailure(err)
	} else {
		s.value = request.Value
		ch <- s.NewResult(proto.Marshal(&SetResponse{}))
	}
}
