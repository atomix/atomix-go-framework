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

package rsm

import (
	"container/list"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	streams "github.com/atomix/atomix-go-framework/pkg/atomix/stream"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util"
	"github.com/gogo/protobuf/proto"
	"io"
	"time"
)

// NewStateMachine returns an initialized StateMachine
func NewStateMachine(registry *Registry) StateMachine {
	return &primitiveManagerStateMachine{
		registry: registry,
	}
}

// primitiveManagerStateMachine is a state machine for primitives that support sessions
type primitiveManagerStateMachine struct {
	registry *Registry
	state    *primitiveManagerState
}

func (m *primitiveManagerStateMachine) Snapshot(writer io.Writer) error {
	snapshot, err := m.state.snapshot()
	if err != nil {
		return err
	}
	bytes, err := proto.Marshal(snapshot)
	if err != nil {
		return err
	}
	return util.WriteBytes(writer, bytes)
}

func (m *primitiveManagerStateMachine) Restore(reader io.Reader) error {
	data, err := util.ReadBytes(reader)
	if err != nil {
		return err
	}
	snapshot := &StateMachineSnapshot{}
	if err := proto.Unmarshal(data, snapshot); err != nil {
		return err
	}
	state := &primitiveManagerState{
		registry: m.registry,
	}
	if err := state.restore(snapshot); err != nil {
		return err
	}
	m.state = state
	return nil
}

func (m *primitiveManagerStateMachine) Command(bytes []byte, stream streams.WriteStream) {
	request := &StateMachineCommandRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		stream.Error(err)
		stream.Close()
	} else {
		stream = streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return proto.Marshal(value.(*StateMachineCommandResponse))
		})
		m.state.command(request, stream)
	}
}

func (m *primitiveManagerStateMachine) Query(bytes []byte, stream streams.WriteStream) {
	request := &StateMachineQueryRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		stream.Error(err)
		stream.Close()
	} else {
		stream = streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return proto.Marshal(value.(*StateMachineQueryResponse))
		})
		m.state.query(request, stream)
	}
}

func newManagerState(registry *Registry) *primitiveManagerState {
	return &primitiveManagerState{
		registry:       registry,
		clients:        make(map[ClientID]*clientState),
		services:       make(map[ServiceID]*serviceState),
		pendingQueries: make(map[Index]*list.List),
		scheduler:      newScheduler(),
	}
}

type primitiveManagerState struct {
	registry       *Registry
	index          Index
	timestamp      time.Time
	clients        map[ClientID]*clientState
	services       map[ServiceID]*serviceState
	pendingQueries map[Index]*list.List
	scheduler      *serviceScheduler
}

func (s *primitiveManagerState) snapshot() (*StateMachineSnapshot, error) {
	clientSnapshots := make([]*ClientSnapshot, 0, len(s.clients))
	for _, client := range s.clients {
		clientSnapshot, err := client.snapshot()
		if err != nil {
			return nil, err
		}
		clientSnapshots = append(clientSnapshots, clientSnapshot)
	}
	serviceSnapshots := make([]*ServiceSnapshot, 0, len(s.services))
	for _, service := range s.services {
		serviceSnapshot, err := service.snapshot()
		if err != nil {
			return nil, err
		}
		serviceSnapshots = append(serviceSnapshots, serviceSnapshot)
	}
	return &StateMachineSnapshot{
		Index:     s.index,
		Timestamp: s.timestamp,
		Clients:   clientSnapshots,
		Services:  serviceSnapshots,
	}, nil
}

func (s *primitiveManagerState) restore(snapshot *StateMachineSnapshot) error {
	s.index = snapshot.Index
	s.timestamp = snapshot.Timestamp
	s.clients = make(map[ClientID]*clientState)
	for _, clientSnapshot := range snapshot.Clients {
		client := newClientState(s, clientSnapshot.ClientID)
		if err := client.restore(clientSnapshot); err != nil {
			return err
		}
		s.clients[client.clientID] = client
	}
	s.services = make(map[ServiceID]*serviceState)
	for _, serviceSnapshot := range snapshot.Services {
		service := newServiceState(serviceSnapshot.ServiceID)
		if err := service.restore(serviceSnapshot); err != nil {
			return err
		}
		s.services[service.serviceID] = service
	}
	return nil
}

func (s *primitiveManagerState) command(request *StateMachineCommandRequest, stream streams.WriteStream) {
	s.index++
	if request.Timestamp != nil && request.Timestamp.After(s.timestamp) {
		s.timestamp = *request.Timestamp
	}
	s.scheduler.runScheduledTasks(s.timestamp)

	switch r := request.Request.(type) {
	case *StateMachineCommandRequest_ClientCommand:
		s.clientCommand(r.ClientCommand, streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return &StateMachineCommandResponse{
				Response: &StateMachineCommandResponse_ClientCommand{
					ClientCommand: value.(*ClientCommandResponse),
				},
			}, nil
		}))
	case *StateMachineCommandRequest_ClientConnect:
		s.clientConnect(r.ClientConnect, streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return &StateMachineCommandResponse{
				Response: &StateMachineCommandResponse_ClientConnect{
					ClientConnect: value.(*ClientConnectResponse),
				},
			}, nil
		}))
	case *StateMachineCommandRequest_ClientKeepAlive:
		s.clientKeepAlive(r.ClientKeepAlive, streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return &StateMachineCommandResponse{
				Response: &StateMachineCommandResponse_ClientKeepAlive{
					ClientKeepAlive: value.(*ClientKeepAliveResponse),
				},
			}, nil
		}))
	case *StateMachineCommandRequest_ClientClose:
		s.clientClose(r.ClientClose, streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return &StateMachineCommandResponse{
				Response: &StateMachineCommandResponse_ClientClose{
					ClientClose: value.(*ClientCloseResponse),
				},
			}, nil
		}))
	}

	s.scheduler.runImmediateTasks()

	indexQueries, ok := s.pendingQueries[s.index]
	if ok {
		elem := indexQueries.Front()
		for elem != nil {
			query := elem.Value.(pendingQuery)
			s.query(query.request, query.stream)
			elem = elem.Next()
		}
		delete(s.pendingQueries, s.index)
	}
}

func (s *primitiveManagerState) clientConnect(request *ClientConnectRequest, stream streams.WriteStream) {
	clientID := ClientID(s.index)
	client := newClientState(s, clientID)
	client.connect(request, stream)
}

func (s *primitiveManagerState) clientKeepAlive(request *ClientKeepAliveRequest, stream streams.WriteStream) {
	client, ok := s.clients[request.ClientID]
	if !ok {
		stream.Error(errors.NewNotFound("client not found"))
		stream.Close()
		return
	}
	client.keepAlive(request, stream)
}

func (s *primitiveManagerState) clientClose(request *ClientCloseRequest, stream streams.WriteStream) {
	client, ok := s.clients[request.ClientID]
	if !ok {
		stream.Error(errors.NewNotFound("client not found"))
		stream.Close()
		return
	}
	client.close(request, stream)
}

func (s *primitiveManagerState) clientCommand(request *ClientCommandRequest, stream streams.WriteStream) {
	client, ok := s.clients[request.ClientID]
	if !ok {
		stream.Error(errors.NewNotFound("client not found"))
		stream.Close()
		return
	}
	client.command(request, stream)
}

func (s *primitiveManagerState) query(request *StateMachineQueryRequest, stream streams.WriteStream) {
	if request.SyncIndex > s.index {
		indexQueries, ok := s.pendingQueries[request.SyncIndex]
		if !ok {
			indexQueries = list.New()
			s.pendingQueries[request.SyncIndex] = indexQueries
		}
		indexQueries.PushBack(pendingQuery{
			request: request,
			stream:  stream,
		})
		return
	}

	switch r := request.Request.(type) {
	case *StateMachineQueryRequest_ClientQuery:
		s.clientQuery(r.ClientQuery, streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return &StateMachineQueryResponse{
				Response: &StateMachineQueryResponse_ClientQuery{
					ClientQuery: value.(*ClientQueryResponse),
				},
			}, nil
		}))
	}
}

func (s *primitiveManagerState) clientQuery(request *ClientQueryRequest, stream streams.WriteStream) {
	client, ok := s.clients[request.ClientID]
	if !ok {
		stream.Error(errors.NewNotFound("client not found"))
		stream.Close()
		return
	}
	client.query(request, stream)
}

type pendingQuery struct {
	request *StateMachineQueryRequest
	stream  streams.WriteStream
}
