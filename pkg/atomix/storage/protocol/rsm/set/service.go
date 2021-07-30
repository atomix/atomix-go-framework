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

func (s *setService) notify(event setapi.Event) error {
	output := &setapi.EventsResponse{
		Event: event,
	}
	for _, events := range s.Proposals().Events().List() {
		if err := events.Notify(output); err != nil {
			return err
		}
	}
	return nil
}

func (s *setService) Size(size SizeQuery) error {
	return size.Reply(&setapi.SizeResponse{
		Size_: uint32(len(s.values)),
	})
}

func (s *setService) Contains(contains ContainsQuery) error {
	request, err := contains.Request()
	if err != nil {
		return err
	}

	_, ok := s.values[request.Element.Value]
	return contains.Reply(&setapi.ContainsResponse{
		Contains: ok,
	})
}

func (s *setService) Add(add AddProposal) error {
	request, err := add.Request()
	if err != nil {
		return err
	}

	if _, ok := s.values[request.Element.Value]; ok {
		return errors.NewAlreadyExists("value already exists")
	}

	s.values[request.Element.Value] = meta.FromProto(request.Element.ObjectMeta)
	err = s.notify(setapi.Event{
		Type:    setapi.Event_ADD,
		Element: request.Element,
	})
	if err != nil {
		return err
	}
	return add.Reply(&setapi.AddResponse{
		Element: request.Element,
	})
}

func (s *setService) Remove(remove RemoveProposal) error {
	request, err := remove.Request()
	if err != nil {
		return err
	}

	object, ok := s.values[request.Element.Value]
	if !ok {
		return errors.NewNotFound("value not found")
	}

	if !object.Equal(meta.FromProto(request.Element.ObjectMeta)) {
		return errors.NewConflict("metadata mismatch")
	}

	delete(s.values, request.Element.Value)

	element := setapi.Element{
		ObjectMeta: object.Proto(),
		Value:      request.Element.Value,
	}
	err = s.notify(setapi.Event{
		Type:    setapi.Event_REMOVE,
		Element: element,
	})
	if err != nil {
		return err
	}
	return remove.Reply(&setapi.RemoveResponse{
		Element: element,
	})
}

func (s *setService) Clear(clear ClearProposal) error {
	s.values = make(map[string]meta.ObjectMeta)
	return clear.Reply(&setapi.ClearResponse{})
}

func (s *setService) Events(events EventsProposal) error {
	request, err := events.Request()
	if err != nil {
		return err
	}

	if request.Replay {
		for value, metadata := range s.values {
			err := events.Notify(&setapi.EventsResponse{
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
	return nil
}

func (s *setService) Elements(elements ElementsQuery) error {
	defer elements.Close()
	for value, object := range s.values {
		err := elements.Notify(&setapi.ElementsResponse{
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
