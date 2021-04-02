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
	"github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm"
)

func init() {
	registerServiceFunc(newService)
}

func newService(scheduler rsm.Scheduler, context rsm.ServiceContext) Service {
	return &setService{
		Service: rsm.NewService(scheduler, context),
		values:  make(map[string]meta.ObjectMeta),
		streams: make(map[rsm.StreamID]ServiceEventsStream),
	}
}

// setService is a state machine for a list primitive
type setService struct {
	rsm.Service
	values  map[string]meta.ObjectMeta
	streams map[rsm.StreamID]ServiceEventsStream
}

func (s *setService) notify(event setapi.Event) error {
	output := &setapi.EventsResponse{
		Event: event,
	}
	for _, stream := range s.streams {
		if err := stream.Notify(output); err != nil {
			return err
		}
	}
	return nil
}

func (s *setService) Size(*setapi.SizeRequest) (*setapi.SizeResponse, error) {
	return &setapi.SizeResponse{
		Size_: uint32(len(s.values)),
	}, nil
}

func (s *setService) Contains(request *setapi.ContainsRequest) (*setapi.ContainsResponse, error) {
	_, ok := s.values[request.Element.Value]
	return &setapi.ContainsResponse{
		Contains: ok,
	}, nil
}

func (s *setService) Add(request *setapi.AddRequest) (*setapi.AddResponse, error) {
	if _, ok := s.values[request.Element.Value]; ok {
		return nil, errors.NewAlreadyExists("value already exists")
	}

	s.values[request.Element.Value] = meta.FromProto(request.Element.ObjectMeta)
	err := s.notify(setapi.Event{
		Type:    setapi.Event_ADD,
		Element: request.Element,
	})
	if err != nil {
		return nil, err
	}
	return &setapi.AddResponse{
		Element: request.Element,
	}, nil
}

func (s *setService) Remove(request *setapi.RemoveRequest) (*setapi.RemoveResponse, error) {
	object, ok := s.values[request.Element.Value]
	if !ok {
		return nil, errors.NewNotFound("value not found")
	}

	if !object.Equal(meta.FromProto(request.Element.ObjectMeta)) {
		return nil, errors.NewConflict("metadata mismatch")
	}

	delete(s.values, request.Element.Value)

	element := setapi.Element{
		ObjectMeta: object.Proto(),
		Value:      request.Element.Value,
	}
	err := s.notify(setapi.Event{
		Type:    setapi.Event_REMOVE,
		Element: element,
	})
	if err != nil {
		return nil, err
	}
	return &setapi.RemoveResponse{
		Element: element,
	}, nil
}

func (s *setService) Clear(*setapi.ClearRequest) (*setapi.ClearResponse, error) {
	s.values = make(map[string]meta.ObjectMeta)
	return &setapi.ClearResponse{}, nil
}

func (s *setService) Events(request *setapi.EventsRequest, stream ServiceEventsStream) (rsm.StreamCloser, error) {
	if request.Replay {
		for value, metadata := range s.values {
			err := stream.Notify(&setapi.EventsResponse{
				Event: setapi.Event{
					Type: setapi.Event_REPLAY,
					Element: setapi.Element{
						ObjectMeta: metadata.Proto(),
						Value:      value,
					},
				},
			})
			if err != nil {
				return nil, err
			}
		}
	}
	s.streams[stream.ID()] = stream
	return func() {
		delete(s.streams, stream.ID())
	}, nil
}

func (s *setService) Elements(request *setapi.ElementsRequest, stream ServiceElementsStream) (rsm.StreamCloser, error) {
	for value, object := range s.values {
		err := stream.Notify(&setapi.ElementsResponse{
			Element: setapi.Element{
				ObjectMeta: object.Proto(),
				Value:      value,
			},
		})
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}
