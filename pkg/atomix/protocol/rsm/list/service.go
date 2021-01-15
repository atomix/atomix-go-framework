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
	listapi "github.com/atomix/api/go/atomix/primitive/list"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/meta"
	"github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
)

func init() {
	registerServiceFunc(newService)
}

func newService(scheduler rsm.Scheduler, context rsm.ServiceContext) Service {
	return &listService{
		Service: rsm.NewService(scheduler, context),
	}
}

// listService is a state machine for a list primitive
type listService struct {
	rsm.Service
	items   []listapi.Value
	streams []ServiceEventsStream
}

func (l *listService) notify(event listapi.Event) error {
	output := &listapi.EventsOutput{
		Event: event,
	}
	for _, stream := range l.streams {
		if err := stream.Notify(output); err != nil {
			return err
		}
	}
	return nil
}

func (l *listService) Size() (*listapi.SizeOutput, error) {
	return &listapi.SizeOutput{
		Size_: uint32(len(l.items)),
	}, nil
}

func (l *listService) Contains(input *listapi.ContainsInput) (*listapi.ContainsOutput, error) {
	for _, value := range l.items {
		if value.Value == input.Value.Value {
			if !meta.Equal(value.ObjectMeta, input.Value.ObjectMeta) {
				return nil, errors.NewConflict("metadata mismatch")
			}
			return &listapi.ContainsOutput{
				Contains: true,
			}, nil
		}
	}
	return &listapi.ContainsOutput{
		Contains: false,
	}, nil
}

func (l *listService) Append(input *listapi.AppendInput) (*listapi.AppendOutput, error) {
	index := len(l.items)
	l.items = append(l.items, input.Value)
	err := l.notify(listapi.Event{
		Type: listapi.Event_ADD,
		Item: listapi.Item{
			Index: uint32(index),
			Value: input.Value,
		},
	})
	if err != nil {
		return nil, err
	}
	return &listapi.AppendOutput{}, nil
}

func (l *listService) Insert(input *listapi.InsertInput) (*listapi.InsertOutput, error) {
	index := input.Item.Index
	if index >= uint32(len(l.items)) {
		return nil, errors.NewInvalid("index %d out of bounds", index)
	}

	oldValue := l.items[index]
	if err := checkPreconditions(oldValue, input.Preconditions); err != nil {
		return nil, err
	}

	value := input.Item.Value
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
		return nil, err
	}
	return &listapi.InsertOutput{}, nil
}

func (l *listService) Get(input *listapi.GetInput) (*listapi.GetOutput, error) {
	index := int(input.Index)
	if index >= len(l.items) {
		return nil, errors.NewInvalid("index %d out of bounds", index)
	}
	value := l.items[index]
	return &listapi.GetOutput{
		Item: listapi.Item{
			Index: uint32(index),
			Value: value,
		},
	}, nil
}

func (l *listService) Set(input *listapi.SetInput) (*listapi.SetOutput, error) {
	index := input.Item.Index
	if index >= uint32(len(l.items)) {
		return nil, errors.NewInvalid("index %d out of bounds", index)
	}

	oldValue := l.items[index]
	if err := checkPreconditions(oldValue, input.Preconditions); err != nil {
		return nil, err
	}

	newValue := input.Item.Value
	l.items[index] = newValue

	err := l.notify(listapi.Event{
		Type: listapi.Event_REMOVE,
		Item: listapi.Item{
			Index: uint32(index),
			Value: oldValue,
		},
	})
	if err != nil {
		return nil, err
	}
	err = l.notify(listapi.Event{
		Type: listapi.Event_ADD,
		Item: listapi.Item{
			Index: uint32(index),
			Value: newValue,
		},
	})
	if err != nil {
		return nil, err
	}
	return &listapi.SetOutput{}, nil
}

func (l *listService) Remove(input *listapi.RemoveInput) (*listapi.RemoveOutput, error) {
	index := input.Index
	if index >= uint32(len(l.items)) {
		return nil, errors.NewInvalid("index %d out of bounds", index)
	}

	value := l.items[index]
	if err := checkPreconditions(value, input.Preconditions); err != nil {
		return nil, err
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
		return nil, err
	}
	return &listapi.RemoveOutput{
		Item: listapi.Item{
			Index: uint32(index),
			Value: value,
		},
	}, nil
}

func (l *listService) Clear() error {
	l.items = []listapi.Value{}
	return nil
}

func (l *listService) Events(input *listapi.EventsInput, stream ServiceEventsStream) error {
	l.streams = append(l.streams, stream)
	return nil
}

func (l *listService) Elements(input *listapi.ElementsInput, stream ServiceElementsStream) error {
	defer stream.Close()
	for index, value := range l.items {
		err := stream.Notify(&listapi.ElementsOutput{
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

func (l *listService) Snapshot(writer ServiceSnapshotWriter) error {
	defer writer.Close()
	for index, value := range l.items {
		err := writer.Write(&listapi.SnapshotEntry{
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

func (l *listService) Restore(input *listapi.SnapshotEntry) error {
	l.items = append(l.items, input.Item.Value)
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
