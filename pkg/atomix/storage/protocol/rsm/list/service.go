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

func (l *listService) Backup(writer SnapshotWriter) error {
	values := make([]ListValue, 0, len(l.items))
	for _, item := range l.items {
		values = append(values, ListValue{
			ObjectMeta: item.ObjectMeta,
			Value:      item.Value,
		})
	}
	return writer.WriteState(&ListState{
		Values: values,
	})
}

func (l *listService) Restore(reader SnapshotReader) error {
	state, err := reader.ReadState()
	if err != nil {
		return err
	}
	l.items = make([]listapi.Value, 0, len(state.Values))
	for _, value := range state.Values {
		l.items = append(l.items, listapi.Value{
			ObjectMeta: value.ObjectMeta,
			Value:      value.Value,
		})
	}
	return nil
}

func (l *listService) notify(event listapi.Event) {
	output := &listapi.EventsResponse{
		Event: event,
	}
	for _, events := range l.Proposals().Events().List() {
		events.Notify(output)
	}
}

func (l *listService) Size(SizeQuery) (*listapi.SizeResponse, error) {
	return &listapi.SizeResponse{
		Size_: uint32(len(l.items)),
	}, nil
}

func (l *listService) Append(app AppendProposal) (*listapi.AppendResponse, error) {
	index := len(l.items)
	l.items = append(l.items, app.Request().Value)
	l.notify(listapi.Event{
		Type: listapi.Event_ADD,
		Item: listapi.Item{
			Index: uint32(index),
			Value: app.Request().Value,
		},
	})
	return &listapi.AppendResponse{}, nil
}

func (l *listService) Insert(insert InsertProposal) (*listapi.InsertResponse, error) {
	index := insert.Request().Item.Index
	if index >= uint32(len(l.items)) {
		return nil, errors.NewInvalid("index %d out of bounds", index)
	}

	oldValue := l.items[index]
	if err := checkPreconditions(oldValue, insert.Request().Preconditions); err != nil {
		return nil, err
	}

	value := insert.Request().Item.Value
	values := append(l.items, value)
	copy(values[index+1:], values[index:])
	values[index] = value
	l.items = values

	l.notify(listapi.Event{
		Type: listapi.Event_ADD,
		Item: listapi.Item{
			Index: uint32(index),
			Value: value,
		},
	})
	return &listapi.InsertResponse{}, nil
}

func (l *listService) Get(get GetQuery) (*listapi.GetResponse, error) {
	index := int(get.Request().Index)
	if index >= len(l.items) {
		return nil, errors.NewInvalid("index %d out of bounds", index)
	}
	value := l.items[index]
	return &listapi.GetResponse{
		Item: listapi.Item{
			Index: uint32(index),
			Value: value,
		},
	}, nil
}

func (l *listService) Set(set SetProposal) (*listapi.SetResponse, error) {
	index := set.Request().Item.Index
	if index >= uint32(len(l.items)) {
		return nil, errors.NewInvalid("index %d out of bounds", index)
	}

	oldValue := l.items[index]
	if err := checkPreconditions(oldValue, set.Request().Preconditions); err != nil {
		return nil, err
	}

	newValue := set.Request().Item.Value
	l.items[index] = newValue

	l.notify(listapi.Event{
		Type: listapi.Event_REMOVE,
		Item: listapi.Item{
			Index: index,
			Value: oldValue,
		},
	})
	l.notify(listapi.Event{
		Type: listapi.Event_ADD,
		Item: listapi.Item{
			Index: index,
			Value: newValue,
		},
	})
	return &listapi.SetResponse{}, nil
}

func (l *listService) Remove(remove RemoveProposal) (*listapi.RemoveResponse, error) {
	index := remove.Request().Index
	if index >= uint32(len(l.items)) {
		return nil, errors.NewInvalid("index %d out of bounds", index)
	}

	value := l.items[index]
	if err := checkPreconditions(value, remove.Request().Preconditions); err != nil {
		return nil, err
	}

	l.items = append(l.items[:index], l.items[index+1:]...)

	l.notify(listapi.Event{
		Type: listapi.Event_REMOVE,
		Item: listapi.Item{
			Index: index,
			Value: value,
		},
	})
	return &listapi.RemoveResponse{
		Item: listapi.Item{
			Index: index,
			Value: value,
		},
	}, nil
}

func (l *listService) Clear(ClearProposal) (*listapi.ClearResponse, error) {
	l.items = []listapi.Value{}
	return &listapi.ClearResponse{}, nil
}

func (l *listService) Events(events EventsProposal) {
	events.Notify(&listapi.EventsResponse{})
}

func (l *listService) Elements(elements ElementsQuery) {
	defer elements.Close()
	for index, value := range l.items {
		elements.Notify(&listapi.ElementsResponse{
			Item: listapi.Item{
				Index: uint32(index),
				Value: value,
			},
		})
	}
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
