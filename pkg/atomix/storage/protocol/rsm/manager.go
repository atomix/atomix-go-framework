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
	"encoding/json"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	streams "github.com/atomix/atomix-go-framework/pkg/atomix/stream"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/gogo/protobuf/proto"
	"io"
	"sync"
	"time"
)

// NewStateMachine returns a new RSM state machine
func NewStateMachine(registry *Registry) StateMachine {
	return &primitiveServiceStateMachine{
		manager:  newServiceManager(registry),
		registry: registry,
	}
}

// StateMachine applies commands from a protocol to a collection of state machines
type StateMachine interface {
	// Snapshot snapshots the state machine state to the given writer
	Snapshot(writer io.Writer) error

	// Restore restores the state machine state from the given reader
	Restore(reader io.Reader) error

	// Command applies a command to the state machine
	Command(bytes []byte, stream streams.WriteStream)

	// Query applies a query to the state machine
	Query(bytes []byte, stream streams.WriteStream)
}

type primitiveServiceStateMachine struct {
	manager  *primitiveServiceManager
	registry *Registry
}

func (s *primitiveServiceStateMachine) Snapshot(writer io.Writer) error {
	snapshot, err := s.manager.snapshot()
	if err != nil {
		return err
	}
	bytes, err := proto.Marshal(snapshot)
	if err != nil {
		return err
	}
	return util.WriteBytes(writer, bytes)
}

func (s *primitiveServiceStateMachine) Restore(reader io.Reader) error {
	data, err := util.ReadBytes(reader)
	if err != nil {
		return err
	}
	snapshot := &StateMachineSnapshot{}
	if err := proto.Unmarshal(data, snapshot); err != nil {
		return err
	}
	s.manager = newServiceManager(s.registry)
	if err := s.manager.restore(snapshot); err != nil {
		return err
	}
	return nil
}

func (s *primitiveServiceStateMachine) Command(bytes []byte, stream streams.WriteStream) {
	request := &CommandRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		stream.Error(err)
		stream.Close()
	} else {
		stream = streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			response := value.(*CommandResponse)
			log.Debugf("Completing CommandResponse %+v", response)
			bytes, err := proto.Marshal(response)
			if err != nil {
				log.Debugf("CommandRequest failed: %v", err)
				return nil, err
			}
			return bytes, nil
		})
		log.Debugf("Applying CommandRequest %+v", request)
		s.manager.command(request, stream)
	}
}

func (s *primitiveServiceStateMachine) Query(bytes []byte, stream streams.WriteStream) {
	request := &QueryRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		stream.Error(err)
		stream.Close()
	} else {
		stream = streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			response := value.(*QueryResponse)
			log.Debugf("Completing QueryResponse %+v", response)
			bytes, err := proto.Marshal(response)
			if err != nil {
				log.Debugf("QueryRequest failed: %v", err)
				return nil, err
			}
			return bytes, nil
		})
		log.Debugf("Applying QueryRequest %+v", request)
		s.manager.query(request, stream)
	}
}

func newServiceManager(registry *Registry) *primitiveServiceManager {
	return &primitiveServiceManager{
		sessions:  make(map[SessionID]*primitiveSession),
		services:  make(map[ServiceID]*primitiveService),
		queries:   make(map[Index]*list.List),
		registry:  registry,
		scheduler: newScheduler(),
	}
}

type primitiveServiceManager struct {
	sessions  map[SessionID]*primitiveSession
	services  map[ServiceID]*primitiveService
	queries   map[Index]*list.List
	queriesMu sync.RWMutex
	registry  *Registry
	scheduler *serviceScheduler
	index     Index
	timestamp time.Time
}

func (m *primitiveServiceManager) snapshot() (*StateMachineSnapshot, error) {
	log.Debugf("Backing up state to snapshot at index %d", m.index)
	sessions := make([]*SessionSnapshot, 0, len(m.sessions))
	for _, session := range m.sessions {
		sessionSnapshot, err := session.snapshot()
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, sessionSnapshot)
	}

	services := make([]*ServiceSnapshot, 0, len(m.services))
	for _, service := range m.services {
		serviceSnapshot, err := service.snapshot()
		if err != nil {
			return nil, err
		}
		services = append(services, serviceSnapshot)
	}
	return &StateMachineSnapshot{
		Index:     m.index,
		Timestamp: m.timestamp,
		Sessions:  sessions,
		Services:  services,
	}, nil
}

func (m *primitiveServiceManager) restore(snapshot *StateMachineSnapshot) error {
	log.Debugf("Restoring state from snapshot at index %d", snapshot.Index)
	m.index = snapshot.Index
	m.timestamp = snapshot.Timestamp
	m.sessions = make(map[SessionID]*primitiveSession)
	m.services = make(map[ServiceID]*primitiveService)
	m.queries = make(map[Index]*list.List)

	for _, sessionSnapshot := range snapshot.Sessions {
		session := newSession(m)
		if err := session.restore(sessionSnapshot); err != nil {
			return err
		}
	}

	for _, serviceSnapshot := range snapshot.Services {
		service := newService(m)
		if err := service.restore(serviceSnapshot); err != nil {
			return err
		}
	}
	return nil
}

func (m *primitiveServiceManager) command(request *CommandRequest, stream streams.WriteStream) {
	m.index++
	if request.Timestamp != nil && request.Timestamp.After(m.timestamp) {
		m.timestamp = *request.Timestamp
	}
	m.scheduler.runScheduledTasks(m.timestamp)

	switch r := request.Request.(type) {
	case *CommandRequest_SessionCommand:
		m.sessionCommand(r.SessionCommand, streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return &CommandResponse{
				Index: m.index,
				Response: &CommandResponse_SessionCommand{
					SessionCommand: value.(*SessionCommandResponse),
				},
			}, nil
		}))
	case *CommandRequest_KeepAlive:
		m.keepAlive(r.KeepAlive, streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return &CommandResponse{
				Index: m.index,
				Response: &CommandResponse_KeepAlive{
					KeepAlive: value.(*KeepAliveResponse),
				},
			}, nil
		}))
	case *CommandRequest_OpenSession:
		m.openSession(r.OpenSession, streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return &CommandResponse{
				Index: m.index,
				Response: &CommandResponse_OpenSession{
					OpenSession: value.(*OpenSessionResponse),
				},
			}, nil
		}))
	case *CommandRequest_CloseSession:
		m.closeSession(r.CloseSession, streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return &CommandResponse{
				Index: m.index,
				Response: &CommandResponse_CloseSession{
					CloseSession: value.(*CloseSessionResponse),
				},
			}, nil
		}))
	}

	m.scheduler.runImmediateTasks()

	m.queriesMu.RLock()
	queries, ok := m.queries[m.index]
	m.queriesMu.RUnlock()
	if ok {
		m.queriesMu.Lock()
		elem := queries.Front()
		for elem != nil {
			query := elem.Value.(primitiveServiceQuery)
			log.Debugf("Dequeued QueryRequest at index %d: %+v", m.index, query.request)
			m.indexQuery(query.request, query.stream)
			elem = elem.Next()
		}
		delete(m.queries, m.index)
		m.queriesMu.Unlock()
	}
}

func (m *primitiveServiceManager) sessionCommand(request *SessionCommandRequest, stream streams.WriteStream) {
	session, ok := m.sessions[request.SessionID]
	if !ok {
		stream.Error(errors.NewNotFound("session not found"))
		stream.Close()
		return
	}

	switch r := request.Request.(type) {
	case *SessionCommandRequest_ServiceCommand:
		m.serviceCommand(r.ServiceCommand, session, streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return &SessionCommandResponse{
				Response: &SessionCommandResponse_ServiceCommand{
					ServiceCommand: value.(*ServiceCommandResponse),
				},
			}, nil
		}))
	case *SessionCommandRequest_CreateService:
		m.createService(r.CreateService, session, streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return &SessionCommandResponse{
				Response: &SessionCommandResponse_CreateService{
					CreateService: value.(*CreateServiceResponse),
				},
			}, nil
		}))
	case *SessionCommandRequest_CloseService:
		m.closeService(r.CloseService, session, streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return &SessionCommandResponse{
				Response: &SessionCommandResponse_CloseService{
					CloseService: value.(*CloseServiceResponse),
				},
			}, nil
		}))
	}
}

func (m *primitiveServiceManager) serviceCommand(request *ServiceCommandRequest, session *primitiveSession, stream streams.WriteStream) {
	service, ok := session.getService(request.ServiceID)
	if !ok {
		stream.Error(errors.NewNotFound("session not found"))
		stream.Close()
		return
	}
	service.command(request.RequestID).execute(request, stream)
}

func (m *primitiveServiceManager) createService(request *CreateServiceRequest, session *primitiveSession, stream streams.WriteStream) {
	defer stream.Close()
	var service *primitiveService
	for _, s := range m.services {
		if s.Type() == request.Type && s.Namespace() == request.Namespace && s.Name() == request.Name {
			service = s
			break
		}
	}

	if service == nil {
		service = newService(m)
		if err := service.open(ServiceID(m.index), request.ServiceInfo); err != nil {
			stream.Error(err)
			return
		}
	}

	_, ok := session.getService(service.serviceID)
	if !ok {
		serviceSession := newServiceSession(service)
		if err := serviceSession.open(session.sessionID); err != nil {
			stream.Error(err)
			return
		}
	}

	stream.Value(&CreateServiceResponse{
		ServiceID: service.serviceID,
	})
}

func (m *primitiveServiceManager) closeService(request *CloseServiceRequest, session *primitiveSession, stream streams.WriteStream) {
	defer stream.Close()
	service, ok := session.getService(request.ServiceID)
	if !ok {
		stream.Error(errors.NewNotFound("session not found"))
		return
	}
	if err := service.close(); err != nil {
		stream.Error(err)
		return
	}
	stream.Value(&CloseServiceResponse{})
}

func (m *primitiveServiceManager) keepAlive(request *KeepAliveRequest, stream streams.WriteStream) {
	defer stream.Close()
	session, ok := m.sessions[request.SessionID]
	if !ok {
		stream.Error(errors.NewNotFound("session not found"))
		return
	}

	requestFilter := &bloom.BloomFilter{}
	if err := json.Unmarshal(request.RequestFilter, requestFilter); err != nil {
		log.Warn("Failed to decode request filter", err)
	}

	if err := session.keepAlive(request.LastRequestID, requestFilter); err != nil {
		stream.Error(err)
		return
	}
	stream.Value(&KeepAliveResponse{})

	for _, session := range m.sessions {
		if m.timestamp.After(session.lastUpdated.Add(session.timeout)) {
			log.Infof("Session %d expired after %s", session.sessionID, m.timestamp.Sub(session.lastUpdated))
			if err := session.close(); err != nil {
				log.Error(err)
			}
		}
	}
}

func (m *primitiveServiceManager) openSession(request *OpenSessionRequest, stream streams.WriteStream) {
	defer stream.Close()
	session := newSession(m)
	if err := session.open(SessionID(m.index), request.Timeout); err != nil {
		stream.Error(err)
		return
	}
	stream.Value(&OpenSessionResponse{
		SessionID: session.sessionID,
	})
}

func (m *primitiveServiceManager) closeSession(request *CloseSessionRequest, stream streams.WriteStream) {
	defer stream.Close()
	session, ok := m.sessions[request.SessionID]
	if !ok {
		stream.Error(errors.NewNotFound("session not found"))
		return
	}
	if err := session.close(); err != nil {
		stream.Error(err)
		return
	}
	stream.Value(&CloseSessionResponse{})
}

func (m *primitiveServiceManager) query(request *QueryRequest, stream streams.WriteStream) {
	if request.LastIndex > m.index {
		log.Debugf("Enqueued QueryRequest at index %d: %+v", m.index, request)
		m.queriesMu.Lock()
		queries, ok := m.queries[request.LastIndex]
		if !ok {
			queries = list.New()
			m.queries[request.LastIndex] = queries
		}
		queries.PushBack(primitiveServiceQuery{
			request: request,
			stream:  stream,
		})
		m.queriesMu.Unlock()
	} else {
		m.indexQuery(request, stream)
	}
}

func (m *primitiveServiceManager) indexQuery(request *QueryRequest, stream streams.WriteStream) {
	switch r := request.Request.(type) {
	case *QueryRequest_SessionQuery:
		m.sessionQuery(r.SessionQuery, streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return &QueryResponse{
				Response: &QueryResponse_SessionQuery{
					SessionQuery: value.(*SessionQueryResponse),
				},
			}, nil
		}))
	}
}

func (m *primitiveServiceManager) sessionQuery(request *SessionQueryRequest, stream streams.WriteStream) {
	session, ok := m.sessions[request.SessionID]
	if !ok {
		stream.Error(errors.NewNotFound("session not found"))
		stream.Close()
		return
	}

	switch r := request.Request.(type) {
	case *SessionQueryRequest_ServiceQuery:
		m.serviceQuery(r.ServiceQuery, session, streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return &SessionQueryResponse{
				Response: &SessionQueryResponse_ServiceQuery{
					ServiceQuery: value.(*ServiceQueryResponse),
				},
			}, nil
		}))
	}
}

func (m *primitiveServiceManager) serviceQuery(request *ServiceQueryRequest, session *primitiveSession, stream streams.WriteStream) {
	service, ok := session.getService(request.ServiceID)
	if !ok {
		stream.Error(errors.NewNotFound("session not found"))
		stream.Close()
		return
	}
	service.query().execute(request, stream)
}

type primitiveServiceQuery struct {
	request *QueryRequest
	stream  streams.WriteStream
}
