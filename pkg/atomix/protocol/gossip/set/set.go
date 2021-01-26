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
	"context"
	metaapi "github.com/atomix/api/go/atomix/primitive/meta"
	setapi "github.com/atomix/api/go/atomix/primitive/set"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/meta"
	"github.com/atomix/go-framework/pkg/atomix/time"
	"sync"
)

func init() {
	registerService(func(protocol GossipProtocol) (Service, error) {
		service := &setService{
			protocol: protocol,
			elements: make(map[string]*SetElement),
		}
		if err := protocol.Server().Register(&setHandler{service}); err != nil {
			return nil, err
		}
		return service, nil
	})
}

type setHandler struct {
	service *setService
}

func (s *setHandler) Update(ctx context.Context, update *SetElement) error {
	s.service.mu.Lock()
	defer s.service.mu.Unlock()
	stored, ok := s.service.elements[update.Value]
	if !ok || meta.FromProto(update.ObjectMeta).After(meta.FromProto(stored.ObjectMeta)) {
		s.service.elements[update.Value] = update
		return s.service.protocol.Group().Update(ctx, update)
	}
	return nil
}

func (s *setHandler) Read(ctx context.Context, key string) (*SetElement, error) {
	s.service.mu.RLock()
	defer s.service.mu.RUnlock()
	return s.service.elements[key], nil
}

func (s *setHandler) List(ctx context.Context, ch chan<- SetElement) error {
	s.service.mu.RLock()
	defer s.service.mu.RUnlock()
	for _, element := range s.service.elements {
		ch <- *element
	}
	return nil
}

var _ GossipHandler = &setHandler{}

func newStateElement(element *setapi.Element) *SetElement {
	if element == nil {
		return nil
	}
	return &SetElement{
		ObjectMeta: element.ObjectMeta,
		Value:      element.Value,
	}
}

func newSetElement(state *SetElement) *setapi.Element {
	if state == nil {
		return nil
	}
	return &setapi.Element{
		ObjectMeta: state.ObjectMeta,
		Value:      state.Value,
	}
}

type setService struct {
	protocol GossipProtocol
	elements map[string]*SetElement
	streams  []chan<- setapi.EventsResponse
	mu       sync.RWMutex
}

func (s *setService) Size(ctx context.Context, _ *setapi.SizeRequest) (*setapi.SizeResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &setapi.SizeResponse{
		Size_: uint32(len(s.elements)),
	}, nil
}

func (s *setService) Contains(ctx context.Context, request *setapi.ContainsRequest) (*setapi.ContainsResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	element, ok := s.elements[request.Element.Value]
	if !ok || element.Type == metaapi.ObjectMeta_TOMBSTONE {
		return &setapi.ContainsResponse{
			Contains: false,
		}, nil
	}

	if !meta.FromProto(element.ObjectMeta).Equal(meta.FromProto(request.Element.ObjectMeta)) {
		return nil, errors.NewConflict("element header mismatch")
	}
	return &setapi.ContainsResponse{
		Contains: true,
	}, nil
}

func (s *setService) Add(ctx context.Context, request *setapi.AddRequest) (*setapi.AddResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	element, ok := s.elements[request.Element.Value]
	if ok && element.Type != metaapi.ObjectMeta_TOMBSTONE {
		return &setapi.AddResponse{
			Element: *newSetElement(element),
		}, nil
	}

	element = newStateElement(&request.Element)
	element.ObjectMeta = meta.NewTimestamped(time.NewTimestamp(*request.Headers.Timestamp)).Proto()
	s.elements[element.Value] = element

	s.notify(setapi.Event{
		Type:    setapi.Event_ADD,
		Element: *newSetElement(element),
	})

	if err := s.protocol.Group().Update(ctx, element); err != nil {
		return nil, err
	}

	return &setapi.AddResponse{
		Element: *newSetElement(element),
	}, nil
}

func (s *setService) Remove(ctx context.Context, request *setapi.RemoveRequest) (*setapi.RemoveResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	element, ok := s.elements[request.Element.Value]
	if !ok || element.Type == metaapi.ObjectMeta_TOMBSTONE {
		return nil, errors.NewNotFound("element '%s' not found", request.Element.Value)
	}

	storedMeta := meta.FromProto(element.ObjectMeta)
	updateMeta := meta.FromProto(request.Element.ObjectMeta)
	if storedMeta.Timestamp.After(updateMeta.Timestamp) {
		return nil, errors.NewConflict("concurrent update")
	}

	element = newStateElement(&request.Element)
	element.ObjectMeta = meta.NewTimestamped(time.NewTimestamp(*request.Headers.Timestamp)).AsTombstone().Proto()
	s.elements[element.Value] = element

	s.notify(setapi.Event{
		Type:    setapi.Event_REMOVE,
		Element: *newSetElement(element),
	})

	if err := s.protocol.Group().Update(ctx, element); err != nil {
		return nil, err
	}

	return &setapi.RemoveResponse{
		Element: *newSetElement(element),
	}, nil
}

func (s *setService) Clear(ctx context.Context, _ *setapi.ClearRequest) (*setapi.ClearResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, element := range s.elements {
		if element.Type != metaapi.ObjectMeta_TOMBSTONE {
			element.Type = metaapi.ObjectMeta_TOMBSTONE
			if err := s.protocol.Group().Update(ctx, element); err != nil {
				return nil, err
			}
		}
	}
	return &setapi.ClearResponse{}, nil
}

func (s *setService) Events(ctx context.Context, request *setapi.EventsRequest, ch chan<- setapi.EventsResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.streams = append(s.streams, ch)
	ch <- setapi.EventsResponse{}
	if request.Replay {
		for _, element := range s.elements {
			ch <- setapi.EventsResponse{
				Event: setapi.Event{
					Type:    setapi.Event_REPLAY,
					Element: *newSetElement(element),
				},
			}
		}
	}
	return nil
}

func (s *setService) Elements(ctx context.Context, request *setapi.ElementsRequest, ch chan<- setapi.ElementsResponse) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, element := range s.elements {
		ch <- setapi.ElementsResponse{
			Element: *newSetElement(element),
		}
	}
	return nil
}

func (s *setService) notify(event setapi.Event) {
	for _, ch := range s.streams {
		ch <- setapi.EventsResponse{
			Event: event,
		}
	}
}
