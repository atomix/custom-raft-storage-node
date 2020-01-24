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

package roles

import (
	"errors"
	"github.com/atomix/go-framework/pkg/atomix/node"
	"github.com/atomix/go-framework/pkg/atomix/service"
	"github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/golang/protobuf/proto"
)

func init() {
	node.RegisterService("test", newTestService)
}

// newTestService returns a new testService
func newTestService(context service.Context) service.Service {
	service := &testService{
		SessionizedService: service.NewSessionizedService(context),
	}
	service.init()
	return service
}

// testService is a state machine for a test primitive
type testService struct {
	*service.SessionizedService
	value string
}

// init initializes the test service
func (s *testService) init() {
	s.Executor.RegisterUnaryOperation("Set", s.Set)
	s.Executor.RegisterStreamOperation("SetStream", s.SetStream)
	s.Executor.RegisterUnaryOperation("SetError", s.SetError)
	s.Executor.RegisterUnaryOperation("Get", s.Get)
	s.Executor.RegisterStreamOperation("GetStream", s.GetStream)
	s.Executor.RegisterUnaryOperation("GetError", s.GetError)
}

// Backup backs up the map service
func (s *testService) Backup() ([]byte, error) {
	snapshot := &TestValueSnapshot{
		Value: s.value,
	}
	return proto.Marshal(snapshot)
}

// Restore restores the map service
func (s *testService) Restore(bytes []byte) error {
	snapshot := &TestValueSnapshot{}
	if err := proto.Unmarshal(bytes, snapshot); err != nil {
		return err
	}
	s.value = snapshot.Value
	return nil
}

// Get gets the test value
func (s *testService) Get(value []byte) ([]byte, error) {
	return proto.Marshal(&GetResponse{
		Value: s.value,
	})
}

// Get gets the test value
func (s *testService) GetStream(value []byte, stream stream.WriteStream) {
	defer stream.Close()
	stream.Result(proto.Marshal(&GetResponse{
		Value: s.value,
	}))
	stream.Result(proto.Marshal(&GetResponse{
		Value: s.value,
	}))
	stream.Result(proto.Marshal(&GetResponse{
		Value: s.value,
	}))
}

// Get gets the test value
func (s *testService) GetError(value []byte) ([]byte, error) {
	return nil, errors.New("error")
}

// Set sets the test value
func (s *testService) Set(value []byte) ([]byte, error) {
	request := &SetRequest{}
	if err := proto.Unmarshal(value, request); err != nil {
		return nil, err
	}
	s.value = request.Value
	return proto.Marshal(&SetResponse{})
}

// Set sets the test value
func (s *testService) SetStream(value []byte, stream stream.WriteStream) {
	defer stream.Close()
	request := &SetRequest{}
	if err := proto.Unmarshal(value, request); err != nil {
		stream.Error(err)
	} else {
		stream.Result(proto.Marshal(&SetResponse{}))
		stream.Result(proto.Marshal(&SetResponse{}))
		stream.Result(proto.Marshal(&SetResponse{}))
	}
}

// Set sets the test value
func (s *testService) SetError(value []byte) ([]byte, error) {
	request := &SetRequest{}
	if err := proto.Unmarshal(value, request); err != nil {
		return nil, err
	}
	return nil, errors.New("error")
}
