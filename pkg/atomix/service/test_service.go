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

package service

import "github.com/golang/protobuf/proto"

// RegisterTestService registers the map service in the given service registry
func RegisterTestService(registry *ServiceRegistry) {
	registry.Register("test", newTestService)
}

// newTestService returns a new TestService
func newTestService(context Context) Service {
	service := &TestService{
		SessionizedService: NewSessionizedService(context),
	}
	service.init()
	return service
}

// TestService is a state machine for a test primitive
type TestService struct {
	*SessionizedService
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

func (s *TestService) Get(value []byte, ch chan<- Result) {
	ch <- s.NewResult(proto.Marshal(&GetResponse{
		Value: s.value,
	}))
}

func (s *TestService) Set(value []byte, ch chan<- Result) {
	request := &SetRequest{}
	if err := proto.Unmarshal(value, request); err != nil {
		ch <- s.NewFailure(err)
	} else {
		s.value = request.Value
		ch <- s.NewResult(proto.Marshal(&SetResponse{}))
	}
}
