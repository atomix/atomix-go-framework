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
	"github.com/atomix/api/go/atomix/primitive/list"
	"github.com/atomix/go-framework/pkg/atomix/errors"
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
	values  []string
	streams []ServiceEventsStream
}

func (l *listService) notify(event *list.EventsOutput) error {
	for _, stream := range l.streams {
		if err := stream.Notify(event); err != nil {
			return err
		}
	}
	return nil
}

func (l *listService) Size() (*list.SizeOutput, error) {
	return &list.SizeOutput{
		Size_: uint32(len(l.values)),
	}, nil
}

func (l *listService) Contains(input *list.ContainsInput) (*list.ContainsOutput, error) {
	for _, value := range l.values {
		if value == input.Value {
			return &list.ContainsOutput{
				Contains: true,
			}, nil
		}
	}
	return &list.ContainsOutput{
		Contains: false,
	}, nil
}

func (l *listService) Append(input *list.AppendInput) (*list.AppendOutput, error) {
	l.values = append(l.values, input.Value)
	return &list.AppendOutput{}, nil
}

func (l *listService) Insert(input *list.InsertInput) (*list.InsertOutput, error) {
	index := input.Index
	if index >= uint32(len(l.values)) {
		return nil, errors.NewInvalid("index %d out of bounds", index)
	}

	values := l.values
	l.values = append(append(values[:index], input.Value), values[index:]...)

	err := l.notify(&list.EventsOutput{
		Type:  list.EventsOutput_ADD,
		Index: index,
		Value: input.Value,
	})
	if err != nil {
		return nil, err
	}
	return &list.InsertOutput{}, nil
}

func (l *listService) Get(input *list.GetInput) (*list.GetOutput, error) {
	index := int(input.Index)
	if index >= len(l.values) {
		return nil, errors.NewInvalid("index %d out of bounds", index)
	}
	value := l.values[index]
	return &list.GetOutput{
		Value: value,
	}, nil
}

func (l *listService) Set(input *list.SetInput) (*list.SetOutput, error) {
	index := input.Index
	if index >= uint32(len(l.values)) {
		return nil, errors.NewInvalid("index %d out of bounds", index)
	}

	oldValue := l.values[index]
	l.values[index] = input.Value

	err := l.notify(&list.EventsOutput{
		Type:  list.EventsOutput_REMOVE,
		Index: index,
		Value: oldValue,
	})
	if err != nil {
		return nil, err
	}
	err = l.notify(&list.EventsOutput{
		Type:  list.EventsOutput_ADD,
		Index: index,
		Value: input.Value,
	})
	if err != nil {
		return nil, err
	}
	return &list.SetOutput{}, nil
}

func (l *listService) Remove(input *list.RemoveInput) (*list.RemoveOutput, error) {
	index := input.Index
	if index >= uint32(len(l.values)) {
		return nil, errors.NewInvalid("index %d out of bounds", index)
	}

	value := l.values[index]
	l.values = append(l.values[:index], l.values[index+1:]...)

	err := l.notify(&list.EventsOutput{
		Type:  list.EventsOutput_REMOVE,
		Index: index,
		Value: value,
	})
	if err != nil {
		return nil, err
	}
	return &list.RemoveOutput{}, nil
}

func (l *listService) Clear() error {
	l.values = []string{}
	return nil
}

func (l *listService) Events(input *list.EventsInput, stream ServiceEventsStream) error {
	l.streams = append(l.streams, stream)
	return nil
}

func (l *listService) Elements(input *list.ElementsInput, stream ServiceElementsStream) error {
	for _, value := range l.values {
		err := stream.Notify(&list.ElementsOutput{
			Value: value,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *listService) Snapshot(writer ServiceSnapshotWriter) error {
	for _, value := range l.values {
		err := writer.Write(&list.SnapshotEntry{
			Value: value,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *listService) Restore(input *list.SnapshotEntry) error {
	l.values = append(l.values, input.Value)
	return nil
}
