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
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/golang/protobuf/proto"
)

func init() {
	node.RegisterService("list", newService)
}

// newService returns a new Service
func newService(context service.Context) service.Service {
	service := &Service{
		SessionizedService: service.NewSessionizedService(context),
		values:             make([]string, 0),
	}
	service.init()
	return service
}

// Service is a state machine for a list primitive
type Service struct {
	*service.SessionizedService
	values []string
}

// init initializes the list service
func (l *Service) init() {
	l.Executor.Register("size", l.Size)
	l.Executor.Register("contains", l.Contains)
	l.Executor.Register("append", l.Append)
	l.Executor.Register("insert", l.Insert)
	l.Executor.Register("get", l.Get)
	l.Executor.Register("set", l.Set)
	l.Executor.Register("remove", l.Remove)
	l.Executor.Register("clear", l.Clear)
	l.Executor.Register("events", l.Events)
	l.Executor.Register("iterate", l.Iterate)
}

// Backup backs up the list service
func (l *Service) Backup() ([]byte, error) {
	snapshot := &ListSnapshot{
		Values: l.values,
	}
	return proto.Marshal(snapshot)
}

// Restore restores the list service
func (l *Service) Restore(bytes []byte) error {
	snapshot := &ListSnapshot{}
	if err := proto.Unmarshal(bytes, snapshot); err != nil {
		return err
	}
	l.values = snapshot.Values
	return nil
}

// Size gets the size of the list
func (l *Service) Size(bytes []byte, ch chan<- service.Result) {
	defer close(ch)
	ch <- l.NewResult(proto.Marshal(&SizeResponse{
		Size_: int32(len(l.values)),
	}))
}

// Contains checks whether the list contains a value
func (l *Service) Contains(bytes []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &ContainsRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- l.NewFailure(err)
		return
	}

	for _, value := range l.values {
		if value == request.Value {
			ch <- l.NewResult(proto.Marshal(&ContainsResponse{
				Contains: true,
			}))
			return
		}
	}

	ch <- l.NewResult(proto.Marshal(&ContainsResponse{
		Contains: false,
	}))
}

// Append adds a value to the end of the list
func (l *Service) Append(bytes []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &AppendRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- l.NewFailure(err)
		return
	}

	index := len(l.values)
	l.values = append(l.values, request.Value)

	l.sendEvent(&ListenResponse{
		Type:  ListenResponse_ADDED,
		Index: uint32(index),
		Value: request.Value,
	})

	ch <- l.NewResult(proto.Marshal(&AppendResponse{
		Status: ResponseStatus_OK,
	}))
}

// Insert inserts a value at a specific index in the list
func (l *Service) Insert(bytes []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &InsertRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- l.NewFailure(err)
		return
	}

	index := request.Index
	if index >= uint32(len(l.values)) {
		ch <- l.NewResult(proto.Marshal(&InsertResponse{
			Status: ResponseStatus_OUT_OF_BOUNDS,
		}))
		return
	}

	intIndex := int(index)
	values := make([]string, 0)
	for i := range l.values {
		if i == intIndex {
			values = append(values, request.Value)
		}
		values = append(values, l.values[i])
	}

	l.values = values

	l.sendEvent(&ListenResponse{
		Type:  ListenResponse_ADDED,
		Index: index,
		Value: request.Value,
	})

	ch <- l.NewResult(proto.Marshal(&InsertResponse{
		Status: ResponseStatus_OK,
	}))
}

// Set sets the value at a specific index in the list
func (l *Service) Set(bytes []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &SetRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- l.NewFailure(err)
		return
	}

	index := request.Index
	if index >= uint32(len(l.values)) {
		ch <- l.NewResult(proto.Marshal(&InsertResponse{
			Status: ResponseStatus_OUT_OF_BOUNDS,
		}))
		return
	}

	oldValue := l.values[index]
	l.values[index] = request.Value

	l.sendEvent(&ListenResponse{
		Type:  ListenResponse_REMOVED,
		Index: index,
		Value: oldValue,
	})
	l.sendEvent(&ListenResponse{
		Type:  ListenResponse_ADDED,
		Index: index,
		Value: request.Value,
	})

	ch <- l.NewResult(proto.Marshal(&SetResponse{
		Status: ResponseStatus_OK,
	}))
}

// Get gets the value at a specific index in the list
func (l *Service) Get(bytes []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &GetRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- l.NewFailure(err)
		return
	}

	index := request.Index
	if index >= uint32(len(l.values)) {
		ch <- l.NewResult(proto.Marshal(&GetResponse{
			Status: ResponseStatus_OUT_OF_BOUNDS,
		}))
		return
	}

	value := l.values[index]
	ch <- l.NewResult(proto.Marshal(&GetResponse{
		Status: ResponseStatus_OK,
		Value:  value,
	}))
}

// Remove removes an index from the list
func (l *Service) Remove(bytes []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &RemoveRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- l.NewFailure(err)
		return
	}

	index := request.Index
	if index >= uint32(len(l.values)) {
		ch <- l.NewResult(proto.Marshal(&RemoveResponse{
			Status: ResponseStatus_OUT_OF_BOUNDS,
		}))
		return
	}

	value := l.values[index]
	l.values = append(l.values[:index], l.values[index+1:]...)

	l.sendEvent(&ListenResponse{
		Type:  ListenResponse_REMOVED,
		Index: index,
		Value: value,
	})

	ch <- l.NewResult(proto.Marshal(&RemoveResponse{
		Status: ResponseStatus_OK,
		Value:  value,
	}))
}

// Clear removes all indexes from the list
func (l *Service) Clear(bytes []byte, ch chan<- service.Result) {
	defer close(ch)
	l.values = make([]string, 0)
	ch <- l.NewResult(proto.Marshal(&ClearResponse{}))
}

// Events registers a channel to send list change events
func (l *Service) Events(bytes []byte, ch chan<- service.Result) {
	request := &ListenRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- l.NewFailure(err)
		close(ch)
	}

	if request.Replay {
		for index, value := range l.values {
			ch <- l.NewResult(proto.Marshal(&ListenResponse{
				Type:  ListenResponse_NONE,
				Index: uint32(index),
				Value: value,
			}))
		}
	}
}

// Iterate sends all current values on the given channel
func (l *Service) Iterate(bytes []byte, ch chan<- service.Result) {
	defer close(ch)
	for _, value := range l.values {
		ch <- l.NewResult(proto.Marshal(&IterateResponse{
			Value: value,
		}))
	}
}

func (l *Service) sendEvent(event *ListenResponse) {
	bytes, err := proto.Marshal(event)
	for _, session := range l.Sessions() {
		for _, ch := range session.ChannelsOf("events") {
			ch <- l.NewResult(bytes, err)
		}
	}
}
