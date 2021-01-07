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
	"github.com/atomix/api/go/atomix/primitive/set"
	"github.com/atomix/go-framework/pkg/atomix/storage/rsm"
)

func init() {
	registerServiceFunc(newService)
}

func newService(scheduler rsm.Scheduler, context rsm.ServiceContext) Service {
	return &setService{
		Service: rsm.NewService(scheduler, context),
		values:  make(map[string]bool),
	}
}

// setService is a state machine for a list primitive
type setService struct {
	rsm.Service
	values  map[string]bool
	streams []ServiceEventsStream
}

func (s *setService) notify(event *set.EventsOutput) error {
	for _, stream := range s.streams {
		if err := stream.Notify(event); err != nil {
			return err
		}
	}
	return nil
}

func (s *setService) Size() (*set.SizeOutput, error) {
	return &set.SizeOutput{
		Size_: uint32(len(s.values)),
	}, nil
}

func (s *setService) Contains(input *set.ContainsInput) (*set.ContainsOutput, error) {
	_, ok := s.values[input.Value]
	return &set.ContainsOutput{
		Contains: ok,
	}, nil
}

func (s *setService) Add(input *set.AddInput) (*set.AddOutput, error) {
	if _, ok := s.values[input.Value]; !ok {
		s.values[input.Value] = true
		err := s.notify(&set.EventsOutput{
			Type:  set.EventsOutput_ADD,
			Value: input.Value,
		})
		if err != nil {
			return nil, err
		}
		return &set.AddOutput{
			Added: true,
		}, nil
	}
	return &set.AddOutput{
		Added: false,
	}, nil
}

func (s *setService) Remove(input *set.RemoveInput) (*set.RemoveOutput, error) {
	if _, ok := s.values[input.Value]; ok {
		delete(s.values, input.Value)
		err := s.notify(&set.EventsOutput{
			Type:  set.EventsOutput_REMOVE,
			Value: input.Value,
		})
		if err != nil {
			return nil, err
		}
		return &set.RemoveOutput{
			Removed: true,
		}, nil
	}
	return &set.RemoveOutput{
		Removed: false,
	}, nil
}

func (s *setService) Clear() error {
	s.values = make(map[string]bool)
	return nil
}

func (s *setService) Events(input *set.EventsInput, stream ServiceEventsStream) error {
	s.streams = append(s.streams, stream)
	return nil
}

func (s *setService) Elements(input *set.ElementsInput, stream ServiceElementsStream) error {
	for value := range s.values {
		err := stream.Notify(&set.ElementsOutput{
			Value: value,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *setService) Snapshot(writer ServiceSnapshotWriter) error {
	for value := range s.values {
		err := writer.Write(&set.SnapshotEntry{
			Value: value,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *setService) Restore(entry *set.SnapshotEntry) error {
	s.values[entry.Value] = true
	return nil
}
