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
	setapi "github.com/atomix/atomix-api/go/atomix/primitive/set"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
)

func init() {
	registerServiceFunc(newService)
}

func newService(context ServiceContext) Service {
	return &setService{
		ServiceContext: context,
		values:         make(map[string]meta.ObjectMeta),
	}
}

// setService is a state machine for a list primitive
type setService struct {
	ServiceContext
	values map[string]meta.ObjectMeta
}

func (s *setService) Backup(writer SnapshotWriter) error {
	values := make([]SetValue, 0, len(s.values))
	for value, obj := range s.values {
		values = append(values, SetValue{
			ObjectMeta: obj.Proto(),
			Value:      value,
		})
	}
	return writer.WriteState(&SetState{
		Values: values,
	})
}

func (s *setService) Restore(reader SnapshotReader) error {
	state, err := reader.ReadState()
	if err != nil {
		return err
	}
	s.values = make(map[string]meta.ObjectMeta)
	for _, value := range state.Values {
		s.values[value.Value] = meta.FromProto(value.ObjectMeta)
	}
	return nil
}

func (s *setService) notify(event setapi.Event) {
	output := &setapi.EventsResponse{
		Event: event,
	}
	for _, events := range s.Proposals().Events().List() {
		events.Notify(output)
	}
}

func (s *setService) Size(size SizeQuery) (*setapi.SizeResponse, error) {
	return &setapi.SizeResponse{
		Size_: uint32(len(s.values)),
	}, nil
}

func (s *setService) Contains(contains ContainsQuery) (*setapi.ContainsResponse, error) {
	_, ok := s.values[contains.Request().Element.Value]
	return &setapi.ContainsResponse{
		Contains: ok,
	}, nil
}

func (s *setService) Add(add AddProposal) (*setapi.AddResponse, error) {
	if _, ok := s.values[add.Request().Element.Value]; ok {
		return nil, errors.NewAlreadyExists("value already exists")
	}

	s.values[add.Request().Element.Value] = meta.FromProto(add.Request().Element.ObjectMeta)
	s.notify(setapi.Event{
		Type:    setapi.Event_ADD,
		Element: add.Request().Element,
	})
	return &setapi.AddResponse{
		Element: add.Request().Element,
	}, nil
}

func (s *setService) Remove(remove RemoveProposal) (*setapi.RemoveResponse, error) {
	object, ok := s.values[remove.Request().Element.Value]
	if !ok {
		return nil, errors.NewNotFound("value not found")
	}

	if !object.Equal(meta.FromProto(remove.Request().Element.ObjectMeta)) {
		return nil, errors.NewConflict("metadata mismatch")
	}

	delete(s.values, remove.Request().Element.Value)

	element := setapi.Element{
		ObjectMeta: object.Proto(),
		Value:      remove.Request().Element.Value,
	}
	s.notify(setapi.Event{
		Type:    setapi.Event_REMOVE,
		Element: element,
	})
	return &setapi.RemoveResponse{
		Element: element,
	}, nil
}

func (s *setService) Clear(ClearProposal) (*setapi.ClearResponse, error) {
	s.values = make(map[string]meta.ObjectMeta)
	return &setapi.ClearResponse{}, nil
}

func (s *setService) Events(events EventsProposal) {
	events.Notify(&setapi.EventsResponse{})
	if events.Request().Replay {
		for value, metadata := range s.values {
			events.Notify(&setapi.EventsResponse{
				Event: setapi.Event{
					Type: setapi.Event_REPLAY,
					Element: setapi.Element{
						ObjectMeta: metadata.Proto(),
						Value:      value,
					},
				},
			})
		}
	}
}

func (s *setService) Elements(elements ElementsQuery) {
	defer elements.Close()
	for value, object := range s.values {
		elements.Notify(&setapi.ElementsResponse{
			Element: setapi.Element{
				ObjectMeta: object.Proto(),
				Value:      value,
			},
		})
	}
}
