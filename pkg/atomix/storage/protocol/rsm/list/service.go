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

package list

import (
	listapi "github.com/atomix/atomix-api/go/atomix/primitive/list"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
)

func init() {
	registerServiceFunc(newService)
}

func newService(context ServiceContext) Service {
	return &listService{
		ServiceContext: context,
	}
}

// listService is a state machine for a list primitive
type listService struct {
	ServiceContext
	items []listapi.Value
}

func (l *listService) SetState(state *ListState) error {
	return nil
}

func (l *listService) GetState() (*ListState, error) {
	return &ListState{}, nil
}

func (l *listService) notify(event listapi.Event) error {
	output := &listapi.EventsResponse{
		Event: event,
	}
	for _, events := range l.Proposals().Events().List() {
		if err := events.Notify(output); err != nil {
			return err
		}
	}
	return nil
}

func (l *listService) Size(size SizeProposal) error {
	return size.Reply(&listapi.SizeResponse{
		Size_: uint32(len(l.items)),
	})
}

func (l *listService) Append(app AppendProposal) error {
	index := len(l.items)
	l.items = append(l.items, app.Request().Value)
	err := l.notify(listapi.Event{
		Type: listapi.Event_ADD,
		Item: listapi.Item{
			Index: uint32(index),
			Value: app.Request().Value,
		},
	})
	if err != nil {
		return err
	}
	return app.Reply(&listapi.AppendResponse{})
}

func (l *listService) Insert(insert InsertProposal) error {
	index := insert.Request().Item.Index
	if index >= uint32(len(l.items)) {
		return errors.NewInvalid("index %d out of bounds", index)
	}

	oldValue := l.items[index]
	if err := checkPreconditions(oldValue, insert.Request().Preconditions); err != nil {
		return err
	}

	value := insert.Request().Item.Value
	values := append(l.items, value)
	copy(values[index+1:], values[index:])
	values[index] = value
	l.items = values

	err := l.notify(listapi.Event{
		Type: listapi.Event_ADD,
		Item: listapi.Item{
			Index: uint32(index),
			Value: value,
		},
	})
	if err != nil {
		return err
	}
	return insert.Reply(&listapi.InsertResponse{})
}

func (l *listService) Get(get GetProposal) error {
	index := int(get.Request().Index)
	if index >= len(l.items) {
		return errors.NewInvalid("index %d out of bounds", index)
	}
	value := l.items[index]
	return get.Reply(&listapi.GetResponse{
		Item: listapi.Item{
			Index: uint32(index),
			Value: value,
		},
	})
}

func (l *listService) Set(set SetProposal) error {
	index := set.Request().Item.Index
	if index >= uint32(len(l.items)) {
		return errors.NewInvalid("index %d out of bounds", index)
	}

	oldValue := l.items[index]
	if err := checkPreconditions(oldValue, set.Request().Preconditions); err != nil {
		return err
	}

	newValue := set.Request().Item.Value
	l.items[index] = newValue

	err := l.notify(listapi.Event{
		Type: listapi.Event_REMOVE,
		Item: listapi.Item{
			Index: uint32(index),
			Value: oldValue,
		},
	})
	if err != nil {
		return err
	}
	err = l.notify(listapi.Event{
		Type: listapi.Event_ADD,
		Item: listapi.Item{
			Index: uint32(index),
			Value: newValue,
		},
	})
	if err != nil {
		return err
	}
	return set.Reply(&listapi.SetResponse{})
}

func (l *listService) Remove(remove RemoveProposal) error {
	index := remove.Request().Index
	if index >= uint32(len(l.items)) {
		return errors.NewInvalid("index %d out of bounds", index)
	}

	value := l.items[index]
	if err := checkPreconditions(value, remove.Request().Preconditions); err != nil {
		return err
	}

	l.items = append(l.items[:index], l.items[index+1:]...)

	err := l.notify(listapi.Event{
		Type: listapi.Event_REPLAY,
		Item: listapi.Item{
			Index: uint32(index),
			Value: value,
		},
	})
	if err != nil {
		return err
	}
	return remove.Reply(&listapi.RemoveResponse{
		Item: listapi.Item{
			Index: uint32(index),
			Value: value,
		},
	})
}

func (l *listService) Clear(clear ClearProposal) error {
	l.items = []listapi.Value{}
	return clear.Reply(&listapi.ClearResponse{})
}

func (l *listService) Events(events EventsProposal) error {
	return nil
}

func (l *listService) Elements(elements ElementsProposal) error {
	defer elements.Close()
	for index, value := range l.items {
		err := elements.Notify(&listapi.ElementsResponse{
			Item: listapi.Item{
				Index: uint32(index),
				Value: value,
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func checkPreconditions(value listapi.Value, preconditions []listapi.Precondition) error {
	for _, precondition := range preconditions {
		switch p := precondition.Precondition.(type) {
		case *listapi.Precondition_Metadata:
			if !meta.Equal(value.ObjectMeta, *p.Metadata) {
				return errors.NewConflict("metadata precondition failed")
			}
		}
	}
	return nil
}
