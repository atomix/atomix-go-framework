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

package service

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/atomix/go-framework/pkg/atomix/util"
	"github.com/gogo/protobuf/proto"
	"io"
	"strings"
	"time"
)

// NewManager returns an initialized Manager
func NewManager(registry *Registry, context Context) *Manager {
	return &Manager{
		registry:     registry,
		context:      context,
		scheduler:    newScheduler(),
		sessions:     make(map[uint64]*Session),
		services:     make(map[qualifiedServiceName]Service),
		indexQueries: make(map[uint64]*list.List),
	}
}

// Manager is a Manager implementation for primitives that support sessions
type Manager struct {
	registry     *Registry
	context      Context
	sessions     map[uint64]*Session
	services     map[qualifiedServiceName]Service
	scheduler    *scheduler
	indexQueries map[uint64]*list.List
}

// Snapshot takes a snapshot of the service
func (m *Manager) Snapshot(writer io.Writer) error {
	if err := m.snapshotSessions(writer); err != nil {
		return err
	}
	if err := m.snapshotServices(writer); err != nil {
		return err
	}
	return nil
}

func (m *Manager) snapshotSessions(writer io.Writer) error {
	return util.WriteMap(writer, m.sessions, func(id uint64, session *Session) ([]byte, error) {
		streams := make([]*SessionStreamSnapshot, 0, len(session.streams))
		for _, stream := range session.streams {
			streams = append(streams, &SessionStreamSnapshot{
				StreamId:       stream.ID,
				Type:           stream.Type,
				SequenceNumber: stream.responseID,
				LastCompleted:  stream.completeID,
			})
		}
		services := make([]string, 0, len(session.services))
		for service := range session.services {
			services = append(services, string(service))
		}
		snapshot := &SessionSnapshot{
			SessionID:       session.ID,
			Timeout:         session.Timeout,
			Timestamp:       session.LastUpdated,
			CommandSequence: session.commandSequence,
			Streams:         streams,
			Services:        services,
		}
		return proto.Marshal(snapshot)
	})
}

func (m *Manager) snapshotServices(writer io.Writer) error {
	count := make([]byte, 4)
	binary.BigEndian.PutUint32(count, uint32(len(m.services)))
	_, err := writer.Write(count)
	if err != nil {
		return err
	}

	for id, service := range m.services {
		serviceID := &ServiceId{
			Type:      service.Type(),
			Name:      id.name(),
			Namespace: id.namespace(),
		}
		bytes, err := proto.Marshal(serviceID)
		if err != nil {
			return err
		}

		length := make([]byte, 4)
		binary.BigEndian.PutUint32(length, uint32(len(bytes)))

		_, err = writer.Write(length)
		if err != nil {
			return err
		}

		_, err = writer.Write(bytes)
		if err != nil {
			return err
		}

		err = service.Backup(writer)
		if err != nil {
			return err
		}
	}
	return nil
}

// Install installs a snapshot of the service
func (m *Manager) Install(reader io.Reader) error {
	if err := m.installSessions(reader); err != nil {
		return err
	}
	if err := m.installServices(reader); err != nil {
		return err
	}
	return nil
}

func (m *Manager) installSessions(reader io.Reader) error {
	m.sessions = make(map[uint64]*Session)
	return util.ReadMap(reader, m.sessions, func(data []byte) (uint64, *Session, error) {
		snapshot := &SessionSnapshot{}
		if err := proto.Unmarshal(data, snapshot); err != nil {
			return 0, nil, err
		}

		session := &Session{
			ID:               snapshot.SessionID,
			Timeout:          time.Duration(snapshot.Timeout),
			LastUpdated:      snapshot.Timestamp,
			ctx:              m.context,
			commandSequence:  snapshot.CommandSequence,
			commandCallbacks: make(map[uint64]func()),
			queryCallbacks:   make(map[uint64]*list.List),
			results:          make(map[uint64]streams.Result),
			streams:          make(map[uint64]*sessionStream),
		}

		sessionStreams := make(map[uint64]*sessionStream)
		for _, stream := range snapshot.Streams {
			sessionStreams[stream.StreamId] = &sessionStream{
				ID:         stream.StreamId,
				Type:       stream.Type,
				session:    session,
				responseID: stream.SequenceNumber,
				completeID: stream.LastCompleted,
				ctx:        m.context,
				stream:     streams.NewNilStream(),
				results:    list.New(),
			}
		}
		session.streams = sessionStreams

		sessionServices := make(map[qualifiedServiceName]bool)
		for _, service := range snapshot.Services {
			sessionServices[qualifiedServiceName(service)] = true
		}
		session.services = sessionServices
		return session.ID, session, nil
	})
}

func (m *Manager) installServices(reader io.Reader) error {
	services := make(map[qualifiedServiceName]Service)

	countBytes := make([]byte, 4)
	n, err := reader.Read(countBytes)
	if err != nil {
		return err
	} else if n <= 0 {
		return nil
	}

	lengthBytes := make([]byte, 4)
	count := int(binary.BigEndian.Uint32(countBytes))
	for i := 0; i < count; i++ {
		n, err = reader.Read(lengthBytes)
		if err != nil {
			return err
		}
		if n > 0 {
			length := binary.BigEndian.Uint32(lengthBytes)
			bytes := make([]byte, length)
			_, err = reader.Read(bytes)
			if err != nil {
				return err
			}

			serviceID := &service.ServiceId{}
			if err = proto.Unmarshal(bytes, serviceID); err != nil {
				return err
			}
			service := m.registry.GetType(serviceID.Type)(m.scheduler, m.context)
			services[newQualifiedServiceName(serviceID.Namespace, serviceID.Name)] = service
			if err := service.Restore(reader); err != nil {
				return err
			}
		}
	}
	m.services = services

	for _, session := range m.sessions {
		for serviceName := range session.services {
			if service, ok := m.services[serviceName]; ok {
				service.addSession(session)
			}
		}
	}
	return nil
}

// Command handles a service command
func (m *Manager) Command(bytes []byte, stream streams.WriteStream) {
	request := &SessionRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		stream.Error(err)
		stream.Close()
	} else {
		m.scheduler.runScheduledTasks(m.context.Timestamp())

		switch r := request.Request.(type) {
		case *SessionRequest_Command:
			m.applyCommand(r.Command, stream)
		case *SessionRequest_OpenSession:
			m.applyOpenSession(r.OpenSession, stream)
		case *SessionRequest_KeepAlive:
			m.applyKeepAlive(r.KeepAlive, stream)
		case *SessionRequest_CloseSession:
			m.applyCloseSession(r.CloseSession, stream)
		}

		m.scheduler.runImmediateTasks()
		m.runIndexQueries(m.context.Index())
	}
}

func (m *Manager) applyCommand(request *SessionCommandRequest, stream streams.WriteStream) {
	session, ok := m.sessions[request.Context.SessionID]
	if !ok {
		util.SessionEntry(m.context.Node(), request.Context.SessionID).
			Warn("Unknown session")
		stream.Error(fmt.Errorf("unknown session %d", request.Context.SessionID))
		stream.Close()
	} else {
		sequenceNumber := request.Context.SequenceNumber
		if sequenceNumber != 0 && sequenceNumber <= session.commandSequence {
			result, ok := session.getResult(sequenceNumber)
			if ok {
				stream.Send(result)
				stream.Close()
			} else {
				streamCtx := session.getStream(sequenceNumber)
				if streamCtx != nil {
					streamCtx.replay(stream)
				} else {
					stream.Error(fmt.Errorf("sequence number %d has already been acknowledged", sequenceNumber))
					stream.Close()
				}
			}
		} else if sequenceNumber > session.nextCommandSequence() {
			session.scheduleCommand(sequenceNumber, func() {
				util.SessionEntry(m.context.Node(), request.Context.SessionID).
					Tracef("Executing command %d", sequenceNumber)
				m.applySessionCommand(request, stream)
			})
		} else {
			util.SessionEntry(m.context.Node(), request.Context.SessionID).
				Tracef("Executing command %d", sequenceNumber)
			m.applySessionCommand(request, stream)
		}
	}
}

func (m *Manager) applySessionCommand(request *SessionCommandRequest, stream streams.WriteStream) {
	session, ok := m.sessions[request.Context.SessionID]
	if !ok {
		util.SessionEntry(m.context.Node(), request.Context.SessionID).
			Warn("Unknown session")
		stream.Error(fmt.Errorf("unknown session %d", request.Context.SessionID))
		stream.Close()
	} else {
		m.applyServiceCommand(request.Command, request.Context, session, stream)
		session.completeCommand(request.Context.SequenceNumber)
	}
}

func (m *Manager) applyServiceCommand(request *ServiceCommandRequest, context *SessionCommandContext, session *Session, stream streams.WriteStream) {
	switch request.Request.(type) {
	case *ServiceCommandRequest_Operation:
		m.applyServiceCommandOperation(request, context, session, stream)
	case *ServiceCommandRequest_Create:
		m.applyServiceCommandCreate(request, context, session, stream)
	case *ServiceCommandRequest_Close:
		m.applyServiceCommandClose(request, context, session, stream)
	case *ServiceCommandRequest_Delete:
		m.applyServiceCommandDelete(request, context, session, stream)
	default:
		stream.Error(fmt.Errorf("unknown service command"))
		stream.Close()
	}
}

func (m *Manager) applyServiceCommandOperation(request *ServiceCommandRequest, context *SessionCommandContext, session *Session, stream streams.WriteStream) {
	name := newQualifiedServiceName(request.Service.Namespace, request.Service.Name)
	service, ok := m.services[name]
	if !ok {
		stream.Error(fmt.Errorf("unknown service %s", name))
		stream.Close()
		return
	}
	if !session.services[name] {
		stream.Error(fmt.Errorf("no open session for service %s", name))
		stream.Close()
		return
	}

	operation := service.getOperation(request.GetOperation().Method)
	if unaryOp, ok := operation.(UnaryOperation); ok {
		output, err := unaryOp.Execute(request.GetOperation().Value)
		result := session.addResult(context.SequenceNumber, streams.Result{
			Value: output,
			Error: err,
		})
		stream.Send(result)
		stream.Close()
	} else if streamOp, ok := operation.(StreamingOperation); ok {
		streamCtx := session.addStream(context.SequenceNumber, request.GetOperation().Method, stream)
		streamOp.Execute(request.GetOperation().Value, streamCtx)
	} else {
		stream.Close()
	}
	session.completeCommand(context.SequenceNumber)
}

func (m *Manager) applyServiceCommandCreate(request *ServiceCommandRequest, context *SessionCommandContext, session *Session, stream streams.WriteStream) {
	name := newQualifiedServiceName(request.Service.Namespace, request.Service.Name)
	service, ok := m.services[name]
	if !ok {
		serviceType := m.registry.GetType(request.Service.Type)
		if serviceType == nil {
			stream.Error(fmt.Errorf("unknown service %s", name))
			stream.Close()
			return
		}
		service = serviceType(m.scheduler, m.context)
		m.services[name] = service
	}
	if !session.services[name] {
		session.services[name] = true
		service.addSession(session)
		if open, ok := service.(SessionOpen); ok {
			open.SessionOpen(session)
		}
	}
	stream.Result(proto.Marshal(&SessionCommandResponse{
		Context: &SessionResponseContext{
			Index:    m.context.Index(),
			Sequence: context.SequenceNumber,
			Type:     SessionResponseType_RESPONSE,
		},
		Response: &ServiceCommandResponse{
			Response: &ServiceCommandResponse_Create{
				Create: &ServiceCreateResponse{},
			},
		},
	}))
	stream.Close()
}

func (m *Manager) applyServiceCommandClose(request *ServiceCommandRequest, context *SessionCommandContext, session *Session, stream streams.WriteStream) {
	name := newQualifiedServiceName(request.Service.Namespace, request.Service.Name)
	service, ok := m.services[name]
	if !ok {
		stream.Error(fmt.Errorf("unknown service %s", name))
		stream.Close()
		return
	}
	if session.services[name] {
		delete(session.services, name)
		service.removeSession(session)
		if closed, ok := service.(SessionClosed); ok {
			closed.SessionClosed(session)
		}
	}
	stream.Result(proto.Marshal(&SessionCommandResponse{
		Context: &SessionResponseContext{
			Index:    m.context.Index(),
			Sequence: context.SequenceNumber,
			Type:     SessionResponseType_RESPONSE,
		},
		Response: &ServiceCommandResponse{
			Response: &ServiceCommandResponse_Close{
				Close: &ServiceCloseResponse{},
			},
		},
	}))
	stream.Close()
}

func (m *Manager) applyServiceCommandDelete(request *ServiceCommandRequest, context *SessionCommandContext, session *Session, stream streams.WriteStream) {
	name := newQualifiedServiceName(request.Service.Namespace, request.Service.Name)
	_, ok := m.services[name]
	if !ok {
		stream.Error(fmt.Errorf("unknown service %s", name))
		stream.Close()
		return
	}
	delete(m.services, name)
	for _, session := range m.sessions {
		delete(session.services, name)
	}

	stream.Result(proto.Marshal(&SessionCommandResponse{
		Context: &SessionResponseContext{
			Index:    m.context.Index(),
			Sequence: context.SequenceNumber,
			Type:     SessionResponseType_RESPONSE,
		},
		Response: &ServiceCommandResponse{
			Response: &ServiceCommandResponse_Delete{
				Delete: &ServiceDeleteResponse{},
			},
		},
	}))
	stream.Close()
}

func (m *Manager) applyOpenSession(request *OpenSessionRequest, stream streams.WriteStream) {
	session := newSession(m.context, request.Timeout)
	m.sessions[session.ID] = session
	stream.Result(proto.Marshal(&SessionResponse{
		Response: &SessionResponse_OpenSession{
			OpenSession: &OpenSessionResponse{
				SessionID: session.ID,
			},
		},
	}))
	stream.Close()
}

// applyKeepAlive applies a KeepAliveRequest to the service
func (m *Manager) applyKeepAlive(request *KeepAliveRequest, stream streams.WriteStream) {
	session, ok := m.sessions[request.SessionID]
	if !ok {
		util.SessionEntry(m.context.Node(), request.SessionID).
			Warn("Unknown session")
		stream.Error(fmt.Errorf("unknown session %d", request.SessionID))
	} else {
		util.SessionEntry(m.context.Node(), request.SessionID).
			Tracef("Recording keep-alive %v", request)

		// Update the session's last updated timestamp to prevent it from expiring
		session.LastUpdated = m.context.Timestamp()

		// Clear the results up to the given command sequence number
		session.ack(request.CommandSequence, request.Streams)

		// Expire sessions that have not been kept alive
		m.expireSessions()

		// Send the response
		stream.Result(proto.Marshal(&SessionResponse{
			Response: &SessionResponse_KeepAlive{
				KeepAlive: &KeepAliveResponse{},
			},
		}))
	}
	stream.Close()
}

// expireSessions expires sessions that have not been kept alive within their timeout
func (m *Manager) expireSessions() {
	for id, session := range m.sessions {
		if session.timedOut(m.context.Timestamp()) {
			session.close()
			delete(m.sessions, id)
			for name := range session.services {
				if service, ok := m.services[name]; ok {
					service.removeSession(session)
					if expired, ok := service.(SessionExpired); ok {
						expired.SessionExpired(session)
					}
				}
			}
		}
	}
}

func (m *Manager) applyCloseSession(request *CloseSessionRequest, stream streams.WriteStream) {
	session, ok := m.sessions[request.SessionID]
	if !ok {
		util.SessionEntry(m.context.Node(), request.SessionID).
			Warn("Unknown session")
		stream.Error(fmt.Errorf("unknown session %d", request.SessionID))
	} else {
		// Close the session and notify the service.
		delete(m.sessions, session.ID)
		session.close()
		for name := range session.services {
			if service, ok := m.services[name]; ok {
				service.removeSession(session)
				if closed, ok := service.(SessionClosed); ok {
					closed.SessionClosed(session)
				}
			}
		}

		// Send the response
		stream.Result(proto.Marshal(&SessionResponse{
			Response: &SessionResponse_CloseSession{
				CloseSession: &CloseSessionResponse{},
			},
		}))
	}
	stream.Close()
}

// Query handles a service query
func (m *Manager) Query(bytes []byte, stream streams.WriteStream) {
	request := &SessionRequest{}
	err := proto.Unmarshal(bytes, request)
	if err != nil {
		stream.Error(err)
		stream.Close()
	} else {
		query := request.GetQuery()
		if query.Context.LastIndex > m.context.Index() {
			util.SessionEntry(m.context.Node(), query.Context.SessionID).
				Tracef("Query index %d greater than last index %d", query.Context.LastIndex, m.context.Index())
			m.indexQuery(query, stream)
		} else {
			util.SessionEntry(m.context.Node(), query.Context.SessionID).
				Tracef("Sequencing query %d <= %d", query.Context.LastIndex, m.context.Index())
			m.sequenceQuery(query, stream)
		}
	}
}

func (m *Manager) indexQuery(query *SessionQueryRequest, stream streams.WriteStream) {
	queries, ok := m.indexQueries[query.Context.LastIndex]
	if !ok {
		queries = list.New()
		m.indexQueries[query.Context.LastIndex] = queries
	}
	queries.PushBack(func() {
		m.sequenceQuery(query, stream)
	})
}

func (m *Manager) runIndexQueries(index uint64) {
	tasks, ok := m.indexQueries[index]
	if ok {
		task := tasks.Front()
		for task != nil {
			task.Value.(func())()
			task = task.Next()
		}
		delete(m.indexQueries, index)
	}
}

func (m *Manager) sequenceQuery(request *SessionQueryRequest, stream streams.WriteStream) {
	session, ok := m.sessions[request.Context.SessionID]
	if !ok {
		util.SessionEntry(m.context.Node(), request.Context.SessionID).
			Warn("Unknown session")
		stream.Error(fmt.Errorf("unknown session %d", request.Context.SessionID))
		stream.Close()
	} else {
		sequenceNumber := request.Context.LastSequenceNumber
		if sequenceNumber > session.commandSequence {
			util.SessionEntry(m.context.Node(), request.Context.SessionID).
				Tracef("Query ID %d greater than last ID %d", sequenceNumber, session.commandSequence)
			session.scheduleQuery(sequenceNumber, func() {
				util.SessionEntry(m.context.Node(), request.Context.SessionID).
					Tracef("Executing query %d", sequenceNumber)
				m.applyServiceQuery(request.Query, request.Context, session, stream)
			})
		} else {
			util.SessionEntry(m.context.Node(), request.Context.SessionID).
				Tracef("Executing query %d", sequenceNumber)
			m.applyServiceQuery(request.Query, request.Context, session, stream)
		}
	}
}

func (m *Manager) applyServiceQuery(request *ServiceQueryRequest, context *SessionQueryContext, session *Session, stream streams.WriteStream) {
	switch request.Request.(type) {
	case *ServiceQueryRequest_Operation:
		m.applyServiceQueryOperation(request, context, session, stream)
	default:
		stream.Error(fmt.Errorf("unknown service query"))
		stream.Close()
	}
}

func (m *Manager) applyServiceQueryOperation(request *ServiceQueryRequest, context *SessionQueryContext, session *Session, stream streams.WriteStream) {
	name := newQualifiedServiceName(request.Service.Namespace, request.Service.Name)
	service, ok := m.services[name]

	// If the service does not exist, reject the operation
	if !ok {
		stream.Error(fmt.Errorf("unknown service %s", request.Service))
		stream.Close()
		return
	}

	// If the session is not open for the service, reject the operation
	if !session.services[name] {
		stream.Error(fmt.Errorf("no open session for service %s", name))
		stream.Close()
		return
	}

	// Set the current session on the service
	service.setCurrentSession(session)

	// Get the service operation
	operation := service.getOperation(request.GetOperation().Method)
	if operation == nil {
		stream.Error(fmt.Errorf("unknown operation: %s", request.GetOperation().Method))
		stream.Close()
		return
	}

	index := m.context.Index()
	responseStream := streams.NewEncodingStream(stream, func(value interface{}) (interface{}, error) {
		return proto.Marshal(&SessionResponse{
			Response: &SessionResponse_Query{
				Query: &SessionQueryResponse{
					Context: &SessionResponseContext{
						Index:    index,
						Sequence: context.LastSequenceNumber,
					},
					Response: &ServiceQueryResponse{
						Response: &ServiceQueryResponse_Operation{
							Operation: &ServiceOperationResponse{
								Result: value.([]byte),
							},
						},
					},
				},
			},
		})
	})

	if unaryOp, ok := operation.(UnaryOperation); ok {
		stream.Result(unaryOp.Execute(request.GetOperation().Value))
		stream.Close()
	} else if streamOp, ok := operation.(StreamingOperation); ok {
		stream.Result(proto.Marshal(&SessionResponse{
			Response: &SessionResponse_Query{
				Query: &SessionQueryResponse{
					Context: &SessionResponseContext{
						Index: index,
						Type:  SessionResponseType_OPEN_STREAM,
					},
				},
			},
		}))

		responseStream = streams.NewCloserStream(responseStream, func(_ streams.WriteStream) {
			stream.Result(proto.Marshal(&SessionResponse{
				Response: &SessionResponse_Query{
					Query: &SessionQueryResponse{
						Context: &SessionResponseContext{
							Index: index,
							Type:  SessionResponseType_CLOSE_STREAM,
						},
					},
				},
			}))
		})

		streamOp.Execute(request.GetOperation().Value, responseStream)
	} else {
		stream.Close()
	}
}

const separator = "."

type qualifiedServiceName string

func newQualifiedServiceName(namespace, name string) qualifiedServiceName {
	return qualifiedServiceName(namespace + separator + name)
}

func (n qualifiedServiceName) namespace() string {
	return strings.Split(string(n), separator)[0]
}

func (n qualifiedServiceName) name() string {
	return strings.Split(string(n), separator)[1]
}
