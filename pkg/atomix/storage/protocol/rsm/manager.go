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
	"bytes"
	"container/list"
	"fmt"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	streams "github.com/atomix/atomix-go-framework/pkg/atomix/stream"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util"
	"github.com/gogo/protobuf/proto"
	"io"
	"time"
)

// NewManager returns an initialized Manager
func NewManager(cluster cluster.Cluster, registry *Registry) *Manager {
	member, _ := cluster.Member()
	return &Manager{
		cluster:   cluster,
		member:    member,
		registry:  registry,
		context:   &managerContext{},
		scheduler: newScheduler(),
		sessions:  make(map[SessionID]*sessionManager),
		services:  make(map[ServiceID]Service),
	}
}

// managerContext is the state machine manager context
type managerContext struct {
	index     Index
	timestamp time.Time
}

// Manager is a Manager implementation for primitives that support sessions
type Manager struct {
	cluster   cluster.Cluster
	member    *cluster.Member
	registry  *Registry
	context   *managerContext
	sessions  map[SessionID]*sessionManager
	services  map[ServiceID]Service
	scheduler *scheduler
}

// Snapshot takes a snapshot of the service
func (m *Manager) Snapshot(writer io.Writer) error {
	snapshot := &StateMachineSnapshot{
		Index:     uint64(m.context.index),
		Timestamp: m.context.timestamp,
		Sessions:  make([]SessionSnapshot, 0, len(m.sessions)),
		Services:  make([]ServiceSnapshot, 0, len(m.services)),
	}

	for _, session := range m.sessions {
		services := make([]*SessionServiceSnapshot, 0, len(session.services))
		for _, service := range session.services {
			streams := make([]*SessionStreamSnapshot, 0, len(service.streams))
			for _, stream := range service.streams {
				streams = append(streams, &SessionStreamSnapshot{
					StreamID:   uint64(stream.id),
					RequestID:  stream.requestID,
					Type:       string(stream.op),
					ResponseID: stream.responseID,
					CompleteID: stream.completeID,
				})
			}
			services = append(services, &SessionServiceSnapshot{
				ServiceId: ServiceId(service.service),
				Streams:   streams,
			})
		}
		commands := make([]*SessionCommandRequest, 0, len(session.commandRequests))
		for _, command := range session.commandRequests {
			commands = append(commands, command.request)
		}
		snapshot.Sessions = append(snapshot.Sessions, SessionSnapshot{
			SessionID:     uint64(session.id),
			Timeout:       session.timeout,
			Timestamp:     session.lastUpdated,
			LastRequestID: session.commandID,
			Services:      services,
			Commands:      commands,
		})
	}

	for _, service := range m.services {
		var b bytes.Buffer
		if err := service.Backup(&b); err != nil {
			return err
		}
		snapshot.Services = append(snapshot.Services, ServiceSnapshot{
			ServiceID: ServiceId(service.ServiceID()),
			Index:     uint64(service.Index()),
			Data:      b.Bytes(),
		})
	}

	bytes, err := proto.Marshal(snapshot)
	if err != nil {
		return err
	}

	return util.WriteBytes(writer, bytes)
}

// Install installs a snapshot of the service
func (m *Manager) Install(reader io.Reader) error {
	data, err := util.ReadBytes(reader)
	if err != nil {
		return err
	}

	snapshot := &StateMachineSnapshot{}
	err = proto.Unmarshal(data, snapshot)
	if err != nil {
		return err
	}

	m.context.index = Index(snapshot.Index)
	m.context.timestamp = snapshot.Timestamp

	for _, sessionSnapshot := range snapshot.Sessions {
		sessionManager := &sessionManager{
			id:              SessionID(sessionSnapshot.SessionID),
			timeout:         sessionSnapshot.Timeout,
			lastUpdated:     sessionSnapshot.Timestamp,
			ctx:             m.context,
			commandID:       sessionSnapshot.LastRequestID,
			commandRequests: make(map[uint64]sessionCommand),
			queryCallbacks:  make(map[uint64]*list.List),
			results:         make(map[uint64]streams.Result),
			services:        make(map[ServiceID]*serviceSession),
		}

		for _, service := range sessionSnapshot.Services {
			session := &serviceSession{
				sessionManager: sessionManager,
				service:        ServiceID(service.ServiceId),
				streams:        make(map[uint64]*sessionStream),
			}

			for _, stream := range service.Streams {
				session.streams[stream.RequestID] = &sessionStream{
					opStream: &opStream{
						id:      StreamID(stream.StreamID),
						op:      OperationID(stream.Type),
						session: session,
						stream:  streams.NewNilStream(),
					},
					cluster:    m.cluster,
					member:     m.member,
					requestID:  stream.RequestID,
					responseID: stream.ResponseID,
					completeID: stream.CompleteID,
					ctx:        m.context,
					results:    list.New(),
				}
			}
			sessionManager.services[ServiceID(service.ServiceId)] = session
		}
		for _, command := range sessionSnapshot.Commands {
			sessionManager.commandRequests[command.Context.RequestID] = sessionCommand{
				request: command,
				stream:  streams.NewNilStream(),
			}
		}
		m.sessions[sessionManager.id] = sessionManager
	}

	for _, serviceSnapshot := range snapshot.Services {
		serviceID := ServiceID(serviceSnapshot.ServiceID)
		f := m.registry.GetService(serviceID.Type)
		service := f(m.scheduler, newServiceContext(serviceID, m.context))
		service.setIndex(Index(serviceSnapshot.Index))
		for _, sessionMgr := range m.sessions {
			if session, ok := sessionMgr.services[serviceID]; ok {
				service.addSession(session)
			}
		}
		if err := service.Restore(bytes.NewReader(serviceSnapshot.Data)); err != nil {
			return err
		}
		m.services[serviceID] = service
	}
	return nil
}

// Command handles a service command
func (m *Manager) Command(bytes []byte, stream streams.WriteStream) {
	request := &StateMachineRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		stream.Error(err)
		stream.Close()
	} else {
		stream = streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return proto.Marshal(&StateMachineResponse{
				Response: value.(*SessionResponse),
			})
		})

		m.context.index++
		if request.Timestamp.After(m.context.timestamp) {
			m.context.timestamp = request.Timestamp
		}
		m.scheduler.runScheduledTasks(m.context.timestamp)

		switch r := request.Request.Request.(type) {
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
		m.scheduler.runIndex(m.context.index)
	}
}

func (m *Manager) applyCommand(request *SessionCommandRequest, stream streams.WriteStream) {
	sessionManager, ok := m.sessions[SessionID(request.Context.SessionID)]
	if !ok {
		log.WithFields(
			logging.String("NodeID", string(m.member.NodeID)),
			logging.Uint64("SessionID", request.Context.SessionID)).
			Warn("Unknown session")
		stream.Error(fmt.Errorf("unknown session %d", request.Context.SessionID))
		stream.Close()
	} else {
		requestID := request.Context.RequestID
		if requestID != 0 && requestID <= sessionManager.commandID {
			serviceID := ServiceID(request.Command.Service)

			session := sessionManager.getService(serviceID)
			if session == nil {
				stream.Error(fmt.Errorf("no open session for service %s", serviceID))
				stream.Close()
				return
			}

			result, ok := session.getUnaryResult(requestID)
			if ok {
				stream.Send(result)
				stream.Close()
			} else {
				streamCtx := session.getStream(requestID)
				if streamCtx != nil {
					streamCtx.replay(stream)
				} else {
					stream.Error(fmt.Errorf("sequence number %d has already been acknowledged", requestID))
					stream.Close()
				}
			}
		} else if requestID > sessionManager.nextCommandID() {
			sessionManager.scheduleCommand(request, stream)
		} else {
			log.WithFields(
				logging.String("NodeID", string(m.member.NodeID)),
				logging.Uint64("SessionID", request.Context.SessionID)).
				Debugf("Executing command %d", requestID)
			m.applySessionCommand(request, sessionManager, stream)
		}
	}
}

func (m *Manager) applySessionCommand(request *SessionCommandRequest, session *sessionManager, stream streams.WriteStream) {
	m.applyServiceCommand(request.Command, request.Context, session, stream)
	nextRequest, nextStream, ok := session.nextCommand(request)
	if ok {
		m.applyCommand(nextRequest, nextStream)
	}
}

func (m *Manager) applyServiceCommand(request ServiceCommandRequest, context SessionCommandContext, sessionManager *sessionManager, stream streams.WriteStream) {
	switch request.Request.(type) {
	case *ServiceCommandRequest_Operation:
		m.applyServiceCommandOperation(request, context, sessionManager, stream)
	case *ServiceCommandRequest_Create:
		m.applyServiceCommandCreate(request, context, sessionManager, stream)
	case *ServiceCommandRequest_Close:
		m.applyServiceCommandClose(request, context, sessionManager, stream)
	case *ServiceCommandRequest_Delete:
		m.applyServiceCommandDelete(request, context, sessionManager, stream)
	default:
		stream.Error(fmt.Errorf("unknown service command"))
		stream.Close()
	}
}

func (m *Manager) applyServiceCommandOperation(request ServiceCommandRequest, context SessionCommandContext, sessionManager *sessionManager, stream streams.WriteStream) {
	serviceID := ServiceID(request.Service)

	service, ok := m.services[serviceID]
	if !ok {
		stream.Error(fmt.Errorf("unknown service %s", serviceID))
		stream.Close()
		return
	}

	session := sessionManager.getService(serviceID)
	if session == nil {
		stream.Error(fmt.Errorf("no open session for service %s", serviceID))
		stream.Close()
		return
	}

	operationID := OperationID(request.GetOperation().Method)
	service.setIndex(service.Index() + 1)
	operation := service.GetOperation(operationID)
	if unaryOp, ok := operation.(UnaryOperation); ok {
		output, err := unaryOp.Execute(request.GetOperation().Value, session)
		result := session.addUnaryResult(context.RequestID, streams.Result{
			Value: output,
			Error: err,
		})
		stream.Send(result)
		stream.Close()
	} else if streamOp, ok := operation.(StreamingOperation); ok {
		sessionStream := session.addStream(context.RequestID, operationID, stream)
		closer, err := streamOp.Execute(request.GetOperation().Value, session, sessionStream)
		if err != nil {
			stream.Error(err)
			stream.Close()
		} else {
			sessionStream.setCloser(closer)
		}
	} else {
		stream.Close()
	}
}

func (m *Manager) applyServiceCommandCreate(request ServiceCommandRequest, context SessionCommandContext, sessionManager *sessionManager, stream streams.WriteStream) {
	defer stream.Close()

	serviceID := ServiceID(request.Service)

	service, ok := m.services[serviceID]
	if !ok {
		f := m.registry.GetService(request.Service.Type)
		if f == nil {
			stream.Value(&SessionResponse{
				Type: SessionResponseType_RESPONSE,
				Status: SessionResponseStatus{
					Code:    SessionResponseCode_INVALID,
					Message: fmt.Sprintf("unknown primitive type '%s'", request.Service.Type),
				},
				Response: &SessionResponse_Command{
					Command: &SessionCommandResponse{
						Context: SessionResponseContext{
							SessionID: context.SessionID,
							RequestID: context.RequestID,
							Index:     uint64(m.context.index),
						},
						Response: ServiceCommandResponse{
							Response: &ServiceCommandResponse_Create{
								Create: &ServiceCreateResponse{},
							},
						},
					},
				},
			})
			return
		}
		service = f(m.scheduler, newServiceContext(serviceID, m.context))
		m.services[serviceID] = service
	}

	session := sessionManager.getService(serviceID)
	if session == nil {
		session = sessionManager.addService(serviceID)
		service.addSession(session)
		if open, ok := service.(SessionOpenService); ok {
			open.SessionOpen(session)
		}
	}

	stream.Value(&SessionResponse{
		Type: SessionResponseType_RESPONSE,
		Status: SessionResponseStatus{
			Code: SessionResponseCode_OK,
		},
		Response: &SessionResponse_Command{
			Command: &SessionCommandResponse{
				Context: SessionResponseContext{
					SessionID: context.SessionID,
					RequestID: context.RequestID,
					Index:     uint64(m.context.index),
				},
				Response: ServiceCommandResponse{
					Response: &ServiceCommandResponse_Create{
						Create: &ServiceCreateResponse{},
					},
				},
			},
		},
	})
}

func (m *Manager) applyServiceCommandClose(request ServiceCommandRequest, context SessionCommandContext, sessionManager *sessionManager, stream streams.WriteStream) {
	serviceID := ServiceID(request.Service)

	service, ok := m.services[serviceID]
	if ok {
		session := sessionManager.removeService(serviceID)
		if session != nil {
			service.removeSession(session)
			if closed, ok := service.(SessionClosedService); ok {
				closed.SessionClosed(session)
			}
		}
	}

	stream.Value(&SessionResponse{
		Type: SessionResponseType_RESPONSE,
		Status: SessionResponseStatus{
			Code: SessionResponseCode_OK,
		},
		Response: &SessionResponse_Command{
			Command: &SessionCommandResponse{
				Context: SessionResponseContext{
					SessionID: context.SessionID,
					RequestID: context.RequestID,
					Index:     uint64(m.context.index),
				},
				Response: ServiceCommandResponse{
					Response: &ServiceCommandResponse_Close{
						Close: &ServiceCloseResponse{},
					},
				},
			},
		},
	})
	stream.Close()
}

func (m *Manager) applyServiceCommandDelete(request ServiceCommandRequest, context SessionCommandContext, session *sessionManager, stream streams.WriteStream) {
	defer stream.Close()

	serviceID := ServiceID(request.Service)

	_, ok := m.services[serviceID]
	if !ok {
		stream.Value(&SessionResponse{
			Type: SessionResponseType_RESPONSE,
			Status: SessionResponseStatus{
				Code:    SessionResponseCode_NOT_FOUND,
				Message: fmt.Sprintf("unknown service '%s'", serviceID),
			},
			Response: &SessionResponse_Command{
				Command: &SessionCommandResponse{
					Context: SessionResponseContext{
						SessionID: context.SessionID,
						RequestID: context.RequestID,
						Index:     uint64(m.context.index),
					},
					Response: ServiceCommandResponse{
						Response: &ServiceCommandResponse_Delete{
							Delete: &ServiceDeleteResponse{},
						},
					},
				},
			},
		})
	} else {
		delete(m.services, serviceID)
		for _, session := range m.sessions {
			session.removeService(serviceID)
		}

		stream.Value(&SessionResponse{
			Type: SessionResponseType_RESPONSE,
			Status: SessionResponseStatus{
				Code: SessionResponseCode_OK,
			},
			Response: &SessionResponse_Command{
				Command: &SessionCommandResponse{
					Context: SessionResponseContext{
						SessionID: context.SessionID,
						RequestID: context.RequestID,
						Index:     uint64(m.context.index),
					},
					Response: ServiceCommandResponse{
						Response: &ServiceCommandResponse_Delete{
							Delete: &ServiceDeleteResponse{},
						},
					},
				},
			},
		})
	}
}

func (m *Manager) applyOpenSession(request *OpenSessionRequest, stream streams.WriteStream) {
	session := newSessionManager(m.cluster, m.context, ClientID(request.ClientID), request.Timeout)
	m.sessions[session.id] = session
	stream.Value(&SessionResponse{
		Response: &SessionResponse_OpenSession{
			OpenSession: &OpenSessionResponse{
				SessionID: uint64(session.id),
			},
		},
	})
	stream.Close()
}

// applyKeepAlive applies a KeepAliveRequest to the service
func (m *Manager) applyKeepAlive(request *KeepAliveRequest, stream streams.WriteStream) {
	session, ok := m.sessions[SessionID(request.SessionID)]
	if !ok {
		log.WithFields(
			logging.String("NodeID", string(m.member.NodeID)),
			logging.Uint64("SessionID", request.SessionID)).
			Warn("Unknown session")
		stream.Error(fmt.Errorf("unknown session %d", request.SessionID))
	} else {
		log.WithFields(
			logging.String("NodeID", string(m.member.NodeID)),
			logging.Uint64("SessionID", request.SessionID)).
			Debugf("Recording keep-alive %v", request)

		// Update the session's last updated timestamp to prevent it from expiring
		session.lastUpdated = m.context.timestamp

		// Clear the results up to the given command sequence number
		for _, service := range session.services {
			service.ack(request.AckRequestID, request.Streams)
		}

		// Expire sessions that have not been kept alive
		m.expireSessions()

		// Send the response
		stream.Value(&SessionResponse{
			Response: &SessionResponse_KeepAlive{
				KeepAlive: &KeepAliveResponse{},
			},
		})
	}
	stream.Close()
}

// expireSessions expires sessions that have not been kept alive within their timeout
func (m *Manager) expireSessions() {
	for id, sessionManager := range m.sessions {
		if sessionManager.timedOut(m.context.timestamp) {
			sessionManager.close()
			delete(m.sessions, id)
			for _, session := range sessionManager.services {
				service, ok := m.services[session.service]
				if ok {
					service.removeSession(session)
					if expired, ok := service.(SessionExpiredService); ok {
						expired.SessionExpired(session)
					}
				}
			}
		}
	}
}

func (m *Manager) applyCloseSession(request *CloseSessionRequest, stream streams.WriteStream) {
	sessionManager, ok := m.sessions[SessionID(request.SessionID)]
	if !ok {
		log.WithFields(
			logging.String("NodeID", string(m.member.NodeID)),
			logging.Uint64("SessionID", request.SessionID)).
			Warn("Unknown session")
		stream.Error(fmt.Errorf("unknown session %d", request.SessionID))
	} else {
		// Close the session and notify the service.
		delete(m.sessions, sessionManager.id)
		sessionManager.close()
		for _, session := range sessionManager.services {
			service, ok := m.services[session.service]
			if ok {
				service.removeSession(session)
				if expired, ok := service.(SessionExpiredService); ok {
					expired.SessionExpired(session)
				}
			}
		}

		// Send the response
		stream.Value(&SessionResponse{
			Response: &SessionResponse_CloseSession{
				CloseSession: &CloseSessionResponse{},
			},
		})
	}
	stream.Close()
}

// Query handles a service query
func (m *Manager) Query(bytes []byte, stream streams.WriteStream) {
	request := &StateMachineRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		stream.Error(err)
		stream.Close()
	} else {
		stream = streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
			if err != nil {
				return nil, err
			}
			return proto.Marshal(&StateMachineResponse{
				Response: value.(*SessionResponse),
			})
		})
		query := request.Request.GetQuery()
		if Index(query.Context.LastIndex) > m.context.index {
			log.WithFields(
				logging.String("NodeID", string(m.member.NodeID))).
				Debugf("Query index %d greater than last index %d", query.Context.LastIndex, m.context.index)
			m.scheduler.RunAtIndex(Index(query.Context.LastIndex), func() {
				m.sequenceQuery(query, stream)
			})
		} else {
			m.sequenceQuery(query, stream)
		}
	}
}

func (m *Manager) sequenceQuery(request *SessionQueryRequest, stream streams.WriteStream) {
	sessionManager, ok := m.sessions[SessionID(request.Context.SessionID)]
	if !ok {
		log.WithFields(
			logging.String("NodeID", string(m.member.NodeID)),
			logging.Uint64("SessionID", request.Context.SessionID)).
			Warn("Unknown session")
		stream.Error(fmt.Errorf("unknown session %d", request.Context.SessionID))
		stream.Close()
	} else {
		sequenceNumber := request.Context.LastRequestID
		if sequenceNumber > sessionManager.commandID {
			log.WithFields(
				logging.String("NodeID", string(m.member.NodeID)),
				logging.Uint64("SessionID", request.Context.SessionID)).
				Debugf("Query ID %d greater than last ID %d", sequenceNumber, sessionManager.commandID)
			sessionManager.scheduleQuery(sequenceNumber, func() {
				log.WithFields(
					logging.String("NodeID", string(m.member.NodeID)),
					logging.Uint64("SessionID", request.Context.SessionID)).
					Debugf("Executing query %d", sequenceNumber)
				m.applyServiceQuery(request.Query, request.Context, sessionManager, stream)
			})
		} else {
			log.WithFields(
				logging.String("NodeID", string(m.member.NodeID)),
				logging.Uint64("SessionID", request.Context.SessionID)).
				Debugf("Executing query %d", sequenceNumber)
			m.applyServiceQuery(request.Query, request.Context, sessionManager, stream)
		}
	}
}

func (m *Manager) applyServiceQuery(request ServiceQueryRequest, context SessionQueryContext, sessionManager *sessionManager, stream streams.WriteStream) {
	switch request.Request.(type) {
	case *ServiceQueryRequest_Operation:
		m.applyServiceQueryOperation(request, context, sessionManager, stream)
	case *ServiceQueryRequest_Metadata:
		m.applyServiceQueryMetadata(request, context, sessionManager, stream)
	default:
		stream.Error(fmt.Errorf("unknown service query"))
		stream.Close()
	}
}

func (m *Manager) applyServiceQueryOperation(request ServiceQueryRequest, context SessionQueryContext, sessionManager *sessionManager, stream streams.WriteStream) {
	serviceID := ServiceID(*request.Service)

	service, ok := m.services[serviceID]

	// If the service does not exist, reject the operation
	if !ok {
		stream.Error(fmt.Errorf("unknown service %s", serviceID))
		stream.Close()
		return
	}

	session := sessionManager.getService(serviceID)
	if session == nil {
		stream.Error(fmt.Errorf("no open session for service %s", serviceID))
		stream.Close()
		return
	}

	// Get the service operation
	operationID := OperationID(request.GetOperation().Method)
	operation := service.GetOperation(operationID)
	if operation == nil {
		stream.Error(fmt.Errorf("unknown operation: %s", request.GetOperation().Method))
		stream.Close()
		return
	}

	index := m.context.index
	responseStream := streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
		return &SessionResponse{
			Type: SessionResponseType_RESPONSE,
			Status: SessionResponseStatus{
				Code:    getCode(err),
				Message: getMessage(err),
			},
			Response: &SessionResponse_Query{
				Query: &SessionQueryResponse{
					Context: SessionResponseContext{
						SessionID: context.SessionID,
						Index:     uint64(index),
					},
					Response: ServiceQueryResponse{
						Response: &ServiceQueryResponse_Operation{
							Operation: &ServiceOperationResponse{
								Result: value.([]byte),
							},
						},
					},
				},
			},
		}, nil
	})

	if unaryOp, ok := operation.(UnaryOperation); ok {
		responseStream.Result(unaryOp.Execute(request.GetOperation().Value, session))
		responseStream.Close()
	} else if streamOp, ok := operation.(StreamingOperation); ok {
		stream.Value(&SessionResponse{
			Type: SessionResponseType_OPEN_STREAM,
			Status: SessionResponseStatus{
				Code: SessionResponseCode_OK,
			},
			Response: &SessionResponse_Query{
				Query: &SessionQueryResponse{
					Context: SessionResponseContext{
						SessionID: context.SessionID,
						Index:     uint64(index),
					},
				},
			},
		})

		responseStream = streams.NewCloserStream(responseStream, func(_ streams.WriteStream) {
			stream.Value(&SessionResponse{
				Type: SessionResponseType_CLOSE_STREAM,
				Status: SessionResponseStatus{
					Code: SessionResponseCode_OK,
				},
				Response: &SessionResponse_Query{
					Query: &SessionQueryResponse{
						Context: SessionResponseContext{
							SessionID: context.SessionID,
							Index:     uint64(index),
						},
					},
				},
			})
		})

		queryStream := &queryStream{
			opStream: &opStream{
				stream:  responseStream,
				id:      StreamID(m.context.index),
				op:      operationID,
				session: session,
			},
		}

		closer, err := streamOp.Execute(request.GetOperation().Value, session, queryStream)
		if err != nil {
			stream.Error(err)
			stream.Close()
		} else {
			queryStream.setCloser(closer)
		}
	} else {
		stream.Close()
	}
}

func (m *Manager) applyServiceQueryMetadata(request ServiceQueryRequest, context SessionQueryContext, session *sessionManager, stream streams.WriteStream) {
	defer stream.Close()

	services := []*ServiceId{}
	serviceType := request.GetMetadata().Type
	for name, service := range m.services {
		if serviceType == "" || service.ServiceType() == serviceType {
			services = append(services, &ServiceId{
				Type:    service.ServiceType(),
				Cluster: name.Cluster,
				Name:    name.Name,
			})
		}
	}

	stream.Value(&SessionResponse{
		Type: SessionResponseType_RESPONSE,
		Status: SessionResponseStatus{
			Code: SessionResponseCode_OK,
		},
		Response: &SessionResponse_Query{
			Query: &SessionQueryResponse{
				Context: SessionResponseContext{
					SessionID: context.SessionID,
					RequestID: context.LastRequestID,
					Index:     uint64(m.context.index),
				},
				Response: ServiceQueryResponse{
					Response: &ServiceQueryResponse_Metadata{
						Metadata: &ServiceMetadataResponse{
							Services: services,
						},
					},
				},
			},
		},
	})
}
