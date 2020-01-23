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

package node

import (
	"github.com/atomix/go-framework/pkg/atomix/service"
	"github.com/golang/protobuf/proto"
)

func init() {
	RegisterService("test", newTestService)
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
	s.Executor.RegisterUnaryOperation("set", s.Set)
	s.Executor.RegisterUnaryOperation("get", s.Get)
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

// Get gets the value
func (s *TestService) Get(value []byte) ([]byte, error) {
	return proto.Marshal(&GetResponse{
		Value: s.value,
	})
}

// Set sets the value
func (s *TestService) Set(value []byte) ([]byte, error) {
	request := &SetRequest{}
	if err := proto.Unmarshal(value, request); err != nil {
		return nil, err
	}
	s.value = request.Value
	return proto.Marshal(&SetResponse{})
}
