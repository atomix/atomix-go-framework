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
	setapi "github.com/atomix/api/go/atomix/primitive/set"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/meta"
	"github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
)

func init() {
	registerServiceFunc(newService)
}

func newService(scheduler rsm.Scheduler, context rsm.ServiceContext) Service {
	return &setService{
		Service: rsm.NewService(scheduler, context),
		values:  make(map[string]meta.ObjectMeta),
	}
}

// setService is a state machine for a list primitive
type setService struct {
	rsm.Service
	values  map[string]meta.ObjectMeta
	streams []ServiceEventsStream
}

func (s *setService) notify(event setapi.Event) error {
	output := &setapi.EventsOutput{
		Event: event,
	}
	for _, stream := range s.streams {
		if err := stream.Notify(output); err != nil {
			return err
		}
	}
	return nil
}

func (s *setService) Size() (*setapi.SizeOutput, error) {
	return &setapi.SizeOutput{
		Size_: uint32(len(s.values)),
	}, nil
}

func (s *setService) Contains(input *setapi.ContainsInput) (*setapi.ContainsOutput, error) {
	_, ok := s.values[input.Element.Value]
	return &setapi.ContainsOutput{
		Contains: ok,
	}, nil
}

func (s *setService) Add(input *setapi.AddInput) (*setapi.AddOutput, error) {
	if _, ok := s.values[input.Element.Value]; ok {
		return nil, errors.NewAlreadyExists("value already exists")
	}

	s.values[input.Element.Value] = meta.New(input.Element.ObjectMeta)
	err := s.notify(setapi.Event{
		Type:    setapi.Event_ADD,
		Element: input.Element,
	})
	if err != nil {
		return nil, err
	}
	return &setapi.AddOutput{
		Element: input.Element,
	}, nil
}

func (s *setService) Remove(input *setapi.RemoveInput) (*setapi.RemoveOutput, error) {
	object, ok := s.values[input.Element.Value]
	if !ok {
		return nil, errors.NewNotFound("value not found")
	}

	if !object.Equal(meta.New(input.Element.ObjectMeta)) {
		return nil, errors.NewConflict("metadata mismatch")
	}

	delete(s.values, input.Element.Value)

	element := setapi.Element{
		ObjectMeta: object.Proto(),
		Value:      input.Element.Value,
	}
	err := s.notify(setapi.Event{
		Type:    setapi.Event_REMOVE,
		Element: element,
	})
	if err != nil {
		return nil, err
	}
	return &setapi.RemoveOutput{
		Element: element,
	}, nil
}

func (s *setService) Clear() error {
	s.values = make(map[string]meta.ObjectMeta)
	return nil
}

func (s *setService) Events(input *setapi.EventsInput, stream ServiceEventsStream) error {
	if input.Replay {
		for value, metadata := range s.values {
			err := stream.Notify(&setapi.EventsOutput{
				Event: setapi.Event{
					Type: setapi.Event_REPLAY,
					Element: setapi.Element{
						ObjectMeta: metadata.Proto(),
						Value:      value,
					},
				},
			})
			if err != nil {
				return err
			}
		}
	}
	s.streams = append(s.streams, stream)
	return nil
}

func (s *setService) Elements(input *setapi.ElementsInput, stream ServiceElementsStream) error {
	for value, object := range s.values {
		err := stream.Notify(&setapi.ElementsOutput{
			Element: setapi.Element{
				ObjectMeta: object.Proto(),
				Value:      value,
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *setService) Snapshot(writer ServiceSnapshotWriter) error {
	for value, object := range s.values {
		err := writer.Write(&setapi.SnapshotEntry{
			Element: setapi.Element{
				ObjectMeta: object.Proto(),
				Value:      value,
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *setService) Restore(entry *setapi.SnapshotEntry) error {
	s.values[entry.Element.Value] = meta.New(entry.Element.ObjectMeta)
	return nil
}
