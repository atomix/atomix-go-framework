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
	"github.com/atomix/atomix-go-node/pkg/atomix/stream"
	"github.com/atomix/atomix-go-node/pkg/atomix/util"
	"github.com/golang/protobuf/proto"
	"io"
)

func init() {
	node.RegisterService(listType, newService)
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
	l.Executor.RegisterUnaryOperation(opSize, l.Size)
	l.Executor.RegisterUnaryOperation(opContains, l.Contains)
	l.Executor.RegisterUnaryOperation(opAppend, l.Append)
	l.Executor.RegisterUnaryOperation(opInsert, l.Insert)
	l.Executor.RegisterUnaryOperation(opGet, l.Get)
	l.Executor.RegisterUnaryOperation(opSet, l.Set)
	l.Executor.RegisterUnaryOperation(opRemove, l.Remove)
	l.Executor.RegisterUnaryOperation(opClear, l.Clear)
	l.Executor.RegisterStreamOperation(opEvents, l.Events)
	l.Executor.RegisterStreamOperation(opIterate, l.Iterate)
}

// Snapshot takes a snapshot of the service
func (l *Service) Snapshot(writer io.Writer) error {
	if err := l.SessionizedService.Snapshot(writer); err != nil {
		return err
	}

	if err := util.WriteVarInt(writer, len(l.values)); err != nil {
		return err
	}
	return util.WriteSlice(writer, l.values, func(value string) ([]byte, error) {
		return []byte(value), nil
	})
}

// Install restores the service from a snapshot
func (l *Service) Install(reader io.Reader) error {
	if err := l.SessionizedService.Install(reader); err != nil {
		return err
	}

	length, err := util.ReadVarInt(reader)
	if err != nil {
		return err
	}
	l.values = make([]string, length)
	return util.ReadSlice(reader, l.values, func(data []byte) (string, error) {
		return string(data), nil
	})
}

// Size gets the size of the list
func (l *Service) Size(bytes []byte) ([]byte, error) {
	return proto.Marshal(&SizeResponse{
		Size_: int32(len(l.values)),
	})
}

// Contains checks whether the list contains a value
func (l *Service) Contains(bytes []byte) ([]byte, error) {
	request := &ContainsRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		return nil, err
	}

	for _, value := range l.values {
		if value == request.Value {
			return proto.Marshal(&ContainsResponse{
				Contains: true,
			})
		}
	}

	return proto.Marshal(&ContainsResponse{
		Contains: false,
	})
}

// Append adds a value to the end of the list
func (l *Service) Append(bytes []byte) ([]byte, error) {
	request := &AppendRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		return nil, err
	}

	index := len(l.values)
	l.values = append(l.values, request.Value)

	l.sendEvent(&ListenResponse{
		Type:  ListenResponse_ADDED,
		Index: uint32(index),
		Value: request.Value,
	})

	return proto.Marshal(&AppendResponse{
		Status: ResponseStatus_OK,
	})
}

// Insert inserts a value at a specific index in the list
func (l *Service) Insert(bytes []byte) ([]byte, error) {
	request := &InsertRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		return nil, err
	}

	index := request.Index
	if index >= uint32(len(l.values)) {
		return proto.Marshal(&InsertResponse{
			Status: ResponseStatus_OUT_OF_BOUNDS,
		})
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

	return proto.Marshal(&InsertResponse{
		Status: ResponseStatus_OK,
	})
}

// Set sets the value at a specific index in the list
func (l *Service) Set(bytes []byte) ([]byte, error) {
	request := &SetRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		return nil, err
	}

	index := request.Index
	if index >= uint32(len(l.values)) {
		return proto.Marshal(&InsertResponse{
			Status: ResponseStatus_OUT_OF_BOUNDS,
		})
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

	return proto.Marshal(&SetResponse{
		Status: ResponseStatus_OK,
	})
}

// Get gets the value at a specific index in the list
func (l *Service) Get(bytes []byte) ([]byte, error) {
	request := &GetRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		return nil, err
	}

	index := request.Index
	if index >= uint32(len(l.values)) {
		return proto.Marshal(&GetResponse{
			Status: ResponseStatus_OUT_OF_BOUNDS,
		})
	}

	value := l.values[index]
	return proto.Marshal(&GetResponse{
		Status: ResponseStatus_OK,
		Value:  value,
	})
}

// Remove removes an index from the list
func (l *Service) Remove(bytes []byte) ([]byte, error) {
	request := &RemoveRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		return nil, err
	}

	index := request.Index
	if index >= uint32(len(l.values)) {
		return proto.Marshal(&RemoveResponse{
			Status: ResponseStatus_OUT_OF_BOUNDS,
		})
	}

	value := l.values[index]
	l.values = append(l.values[:index], l.values[index+1:]...)

	l.sendEvent(&ListenResponse{
		Type:  ListenResponse_REMOVED,
		Index: index,
		Value: value,
	})

	return proto.Marshal(&RemoveResponse{
		Status: ResponseStatus_OK,
		Value:  value,
	})
}

// Clear removes all indexes from the list
func (l *Service) Clear(bytes []byte) ([]byte, error) {
	l.values = make([]string, 0)
	return proto.Marshal(&ClearResponse{})
}

// Events registers a channel to send list change events
func (l *Service) Events(bytes []byte, stream stream.WriteStream) {
	request := &ListenRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		stream.Error(err)
		stream.Close()
		return
	}

	// Send an OPEN response to notify the client the stream is open
	stream.Result(proto.Marshal(&ListenResponse{
		Type: ListenResponse_OPEN,
	}))

	if request.Replay {
		for index, value := range l.values {
			stream.Result(proto.Marshal(&ListenResponse{
				Type:  ListenResponse_NONE,
				Index: uint32(index),
				Value: value,
			}))
		}
	}
}

// Iterate sends all current values on the given channel
func (l *Service) Iterate(bytes []byte, stream stream.WriteStream) {
	defer stream.Close()
	for _, value := range l.values {
		stream.Result(proto.Marshal(&IterateResponse{
			Value: value,
		}))
	}
}

func (l *Service) sendEvent(event *ListenResponse) {
	bytes, err := proto.Marshal(event)
	for _, session := range l.Sessions() {
		for _, stream := range session.StreamsOf(opEvents) {
			stream.Result(bytes, err)
		}
	}
}
