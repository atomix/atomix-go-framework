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

package set

import (
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/golang/protobuf/proto"
)

func init() {
	service.RegisterService("set", newService)
}

// newService returns a new Service
func newService(context service.Context) service.Service {
	service := &Service{
		SessionizedService: service.NewSessionizedService(context),
		values:             make(map[string]bool),
	}
	service.init()
	return service
}

// Service is a state machine for a list primitive
type Service struct {
	*service.SessionizedService
	values map[string]bool
}

// init initializes the list service
func (s *Service) init() {
	s.Executor.Register("size", s.Size)
	s.Executor.Register("contains", s.Contains)
	s.Executor.Register("add", s.Add)
	s.Executor.Register("remove", s.Remove)
	s.Executor.Register("clear", s.Clear)
	s.Executor.Register("events", s.Events)
	s.Executor.Register("iterate", s.Iterate)
}

// Backup backs up the list service
func (s *Service) Backup() ([]byte, error) {
	snapshot := &SetSnapshot{
		Values: s.values,
	}
	return proto.Marshal(snapshot)
}

// Restore restores the list service
func (s *Service) Restore(bytes []byte) error {
	snapshot := &SetSnapshot{}
	if err := proto.Unmarshal(bytes, snapshot); err != nil {
		return err
	}
	s.values = snapshot.Values
	return nil
}

// Size gets the number of elements in the set
func (s *Service) Size(bytes []byte, ch chan<- service.Result) {
	defer close(ch)
	ch <- s.NewResult(proto.Marshal(&SizeResponse{
		Size_: int32(len(s.values)),
	}))
}

// Contains checks whether the set contains an element
func (s *Service) Contains(bytes []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &ContainsRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- s.NewFailure(err)
		return
	}

	_, ok := s.values[request.Value]
	ch <- s.NewResult(proto.Marshal(&ContainsResponse{
		Contains: ok,
	}))
}

// Add adds an element to the set
func (s *Service) Add(bytes []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &AddRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- s.NewFailure(err)
		return
	}

	if _, ok := s.values[request.Value]; !ok {
		s.values[request.Value] = true

		s.sendEvent(&ListenResponse{
			Type:  ListenResponse_ADDED,
			Value: request.Value,
		})

		ch <- s.NewResult(proto.Marshal(&AddResponse{
			Added: true,
		}))
	} else {
		ch <- s.NewResult(proto.Marshal(&AddResponse{
			Added: false,
		}))
	}
}

// Remove removes an element from the set
func (s *Service) Remove(bytes []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &RemoveRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- s.NewFailure(err)
		return
	}

	if _, ok := s.values[request.Value]; ok {
		delete(s.values, request.Value)

		s.sendEvent(&ListenResponse{
			Type:  ListenResponse_REMOVED,
			Value: request.Value,
		})

		ch <- s.NewResult(proto.Marshal(&RemoveResponse{
			Removed: true,
		}))
	} else {
		ch <- s.NewResult(proto.Marshal(&RemoveResponse{
			Removed: false,
		}))
	}
}

// Clear removes all elements from the set
func (s *Service) Clear(bytes []byte, ch chan<- service.Result) {
	defer close(ch)
	s.values = make(map[string]bool)
	ch <- s.NewResult(proto.Marshal(&ClearResponse{}))
}

// Events registers a channel on which to send set change events
func (s *Service) Events(bytes []byte, ch chan<- service.Result) {
	request := &ListenRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- s.NewFailure(err)
		close(ch)
	}

	if request.Replay {
		for value := range s.values {
			ch <- s.NewResult(proto.Marshal(&ListenResponse{
				Type:  ListenResponse_NONE,
				Value: value,
			}))
		}
	}
}

// Iterate sends all current set elements on the given channel
func (s *Service) Iterate(bytes []byte, ch chan<- service.Result) {
	defer close(ch)
	for value := range s.values {
		ch <- s.NewResult(proto.Marshal(&IterateResponse{
			Value: value,
		}))
	}
}

func (s *Service) sendEvent(event *ListenResponse) {
	bytes, err := proto.Marshal(event)
	for _, session := range s.Sessions() {
		for _, ch := range session.ChannelsOf("events") {
			ch <- s.NewResult(bytes, err)
		}
	}
}
