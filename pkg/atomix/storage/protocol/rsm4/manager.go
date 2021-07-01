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
		cluster:  cluster,
		member:   member,
		registry: registry,
	}
}

// Manager is a Manager implementation for primitives that support sessions
type Manager struct {
	cluster        cluster.Cluster
	member         *cluster.Member
	registry       *Registry
	sessionTimeout time.Duration
	state          *stateManager
}

// Snapshot takes a snapshot of the service
func (m *Manager) Snapshot(writer io.Writer) error {
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

// Install installs a snapshot of the service
func (m *Manager) Install(reader io.Reader) error {
	data, err := util.ReadBytes(reader)
	if err != nil {
		return err
	}
	snapshot := &StateMachineSnapshot{}
	if err := proto.Unmarshal(data, snapshot); err != nil {
		return err
	}
	state := &stateManager{
		cluster:        m.cluster,
		member:         m.member,
		registry:       m.registry,
		sessionTimeout: m.sessionTimeout,
	}
	if err := state.restore(snapshot); err != nil {
		return err
	}
	m.state = state
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
			return proto.Marshal(value.(*StateMachineResponse))
		})
		m.state.command(request, stream)
	}
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
			return proto.Marshal(value.(*StateMachineResponse))
		})
		m.state.query(request, stream)
	}
}

// stateManager is a Manager implementation for primitives that support sessions
type stateManager struct {
	cluster        cluster.Cluster
	member         *cluster.Member
	registry       *Registry
	sessionTimeout time.Duration
	index          Index
	timestamp      time.Time
	sessions       map[SessionID]*sessionState
	services       map[ServiceID]*serviceState
	scheduler      *serviceScheduler
}

func (m *stateManager) snapshot() (*StateMachineSnapshot, error) {
	sessions := make([]*SessionSnapshot, 0, len(m.sessions))
	for _, session := range m.sessions {
		sessionSnapshot, err := session.snapshot()
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, sessionSnapshot)

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
			ClientID:      string(session.clientID),
			SessionID:     uint64(session.sessionID),
			Timeout:       session.timeout,
			Timestamp:     session.lastUpdated,
			LastRequestID: session.commandID,
			Services:      services,
			Commands:      commands,
		})
	}

	for _, service := range m.services {
		serviceSnapshot := &ServiceSnapshot{}
		if err := service.snapshot(serviceSnapshot); err != nil {
			return err
		}
		snapshot.Services = append(snapshot.Services, serviceSnapshot)

		var b bytes.Buffer
		if err := service.Backup(&b); err != nil {
			return nil, err
		}
		serviceSnapshot, err := service.snapshot()
		if err != nil {
			return nil, err
		}
		serviceSnapshots = append(serviceSnapshots, *serviceSnapshot)

		snapshot.Services = append(snapshot.Services, ServiceSnapshot{
			ServiceID: ServiceId(service.ServiceID()),
			Index:     uint64(service.Index()),
			Data:      b.Bytes(),
		})
	}
	return nil
}

func (m *stateManager) restore(snapshot *StateMachineSnapshot) error {
	m.index = Index(snapshot.Index)
	m.timestamp = snapshot.Timestamp

	for _, serviceSnapshot := range snapshot.Services {
		service := &serviceState{}
		if err := service.restore(serviceSnapshot); err != nil {
			return err
		}

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

	for _, sessionSnapshot := range snapshot.Sessions {
		managerSession := newSession(m, SessionID(sessionSnapshot.SessionID))
		if err := managerSession.restore(sessionSnapshot); err != nil {
			return err
		}

		managerSession := &managerSession{
			id:              SessionID(sessionSnapshot.SessionID),
			timeout:         sessionSnapshot.Timeout,
			lastUpdated:     sessionSnapshot.Timestamp,
			ctx:             m.context,
			commandID:       sessionSnapshot.LastRequestID,
			commandRequests: make(map[uint64]commandRequest),
			queryCallbacks:  make(map[uint64]*list.List),
			results:         make(map[uint64]streams.Result),
			services:        make(map[ServiceID]*managerSessionService),
		}

		for _, service := range sessionSnapshot.Services {
			session := &managerSessionService{
				sessionState: managerSession,
				service:      ServiceID(service.ServiceId),
				streams:      make(map[uint64]*commandStreamManager),
			}

			for _, stream := range service.Streams {
				session.streams[stream.RequestID] = &commandStreamManager{
					sessionStreamManager: &sessionStreamManager{
						id:      StreamID(stream.StreamID),
						op:      OperationID(stream.Type),
						session: session,
						stream:  streams.NewNilStream(),
					},
					cluster:       m.cluster,
					member:        m.member,
					requestID:     stream.RequestID,
					responseID:    stream.ResponseID,
					ackResponseID: stream.CompleteID,
					ctx:           m.context,
					results:       list.New(),
				}
			}
			managerSession.services[ServiceID(service.ServiceId)] = session
		}
		for _, command := range sessionSnapshot.Commands {
			managerSession.commandRequests[command.Context.RequestID] = commandRequest{
				request: command,
				stream:  streams.NewNilStream(),
			}
		}
		m.sessions[managerSession.sessionID] = managerSession
	}
	return nil
}

// Command handles a service command
func (m *stateManager) command(request *StateMachineRequest, stream streams.WriteStream) {
	stream = streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
		if err != nil {
			return nil, err
		}
		return proto.Marshal(&StateMachineResponse{
			Response: value.(*SessionResponse),
		})
	})

	m.index++
	if request.Timestamp.After(m.timestamp) {
		m.timestamp = request.Timestamp
	}
	m.scheduler.runScheduledTasks(m.timestamp)

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
	m.scheduler.runIndex(m.index)
}

func (m *stateManager) applyCommand(request *SessionCommandRequest, stream streams.WriteStream) {
	session, ok := m.sessions[request.Context.SessionID]
	if !ok {
		log.WithFields(
			logging.String("NodeID", string(m.member.NodeID)),
			logging.Uint64("SessionID", uint64(request.Context.SessionID))).
			Warn("Unknown session")
		stream.Error(fmt.Errorf("unknown session %d", request.Context.SessionID))
		stream.Close()
	} else {
		requestID := request.Context.RequestID
		if requestID != 0 && requestID <= session.commandID {
			serviceID := ServiceID(request.Command.Service)

			service, ok := session.getService(request.Command.ServiceID)
			if service == nil {
				stream.Error(fmt.Errorf("no open session for service %s", serviceID))
				stream.Close()
				return
			}

			response, ok := session.getResponse(requestID)
			if ok {
				stream.Send(streams.Result{Value: response})
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
		} else if requestID > session.nextCommandID() {
			session.scheduleCommand(request, stream)
		} else {
			log.WithFields(
				logging.String("NodeID", string(m.member.NodeID)),
				logging.Uint64("SessionID", request.Context.SessionID)).
				Debugf("Executing command %d", requestID)
			m.applySessionCommand(request, session, stream)
		}
	}
}

func (m *stateManager) applySessionCommand(request *SessionCommandRequest, session *sessionState, stream streams.WriteStream) {
	m.applyServiceCommand(request.Command, request.Context, session, stream)

	nextQuery, nextStream, ok := session.nextQuery()
	for ok {
		m.applyServiceQuery(nextQuery.Query, nextQuery.Context, session, nextStream)
		nextQuery, nextStream, ok = session.nextQuery()
	}

	nextCommand, nextStream, ok := session.nextCommand()
	if ok {
		m.applySessionCommand(nextCommand, session, nextStream)
	}
}

func (m *stateManager) applyServiceCommand(request ServiceCommandRequest, context SessionCommandContext, sessionManager *sessionState, stream streams.WriteStream) {
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

func (m *stateManager) applyServiceCommandOperation(request ServiceCommandRequest, context SessionCommandContext, sessionManager *sessionState, stream streams.WriteStream) {
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
	service.setTimestamp(m.timestamp)
	operation := service.GetOperation(operationID)
	if unaryOp, ok := operation.(UnaryOperation); ok {
		output, err := unaryOp.Execute(request.GetOperation().Value, session)
		response := session.addResponse(context.RequestID, streams.Result{
			Value: output,
			Error: err,
		})
		stream.Send(streams.Result{Value: response})
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

func (m *stateManager) applyServiceCommandCreate(request ServiceCommandRequest, context SessionCommandContext, sessionManager *sessionState, stream streams.WriteStream) {
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
							Index:     uint64(m.index),
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
		service = newService(m, serviceID)
		m.services[serviceID] = service
	}

	session := sessionManager.getService(serviceID)
	if session == nil {
		session = sessionManager.addService(serviceID)
		service.addSession(session)
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
					Index:     uint64(m.index),
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

func (m *stateManager) applyServiceCommandClose(request ServiceCommandRequest, context SessionCommandContext, sessionManager *sessionState, stream streams.WriteStream) {
	serviceID := ServiceID(request.Service)

	service, ok := m.services[serviceID]
	if ok {
		session := sessionManager.removeService(serviceID)
		if session != nil {
			service.removeSession(session)
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
					Index:     uint64(m.index),
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

func (m *stateManager) applyServiceCommandDelete(request ServiceCommandRequest, context SessionCommandContext, session *sessionState, stream streams.WriteStream) {
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
						Index:     m.index,
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
						Index:     m.index,
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

func (m *stateManager) applyOpenSession(request *OpenSessionRequest, stream streams.WriteStream) {
	session := newSession(m, SessionID(m.index))
	m.sessions[session.sessionID] = session
	stream.Value(&SessionResponse{
		Response: &SessionResponse_OpenSession{
			OpenSession: &OpenSessionResponse{
				SessionID: session.sessionID,
			},
		},
	})
	stream.Close()
}

// applyKeepAlive applies a KeepAliveRequest to the service
func (m *stateManager) applyKeepAlive(request *KeepAliveRequest, stream streams.WriteStream) {
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
		session.lastUpdated = m.timestamp

		// Clear the commandResponses up to the given command sequence number
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
func (m *stateManager) expireSessions() {
	for id, session := range m.sessions {
		if session.timedOut(m.timestamp) {
			session.close()
			delete(m.sessions, id)
			for _, sessionService := range session.services {
				service, ok := m.services[sessionService.service]
				if ok {
					service.expireSession(sessionService)
				}
			}
		}
	}
}

func (m *stateManager) applyCloseSession(request *CloseSessionRequest, stream streams.WriteStream) {
	session, ok := m.sessions[SessionID(request.SessionID)]
	if !ok {
		log.WithFields(
			logging.String("NodeID", string(m.member.NodeID)),
			logging.Uint64("SessionID", request.SessionID)).
			Warn("Unknown session")
		stream.Error(fmt.Errorf("unknown session %d", request.SessionID))
	} else {
		// Close the session and notify the service.
		delete(m.sessions, session.sessionID)
		session.close()
		for _, sessionService := range session.services {
			service, ok := m.services[sessionService.service]
			if ok {
				service.expireSession(sessionService)
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
func (m *stateManager) query(request *StateMachineRequest, stream streams.WriteStream) {
	stream = streams.NewEncodingStream(stream, func(value interface{}, err error) (interface{}, error) {
		if err != nil {
			return nil, err
		}
		return proto.Marshal(&StateMachineResponse{
			Response: value.(*SessionResponse),
		})
	})
	query := request.Request.GetQuery()
	if Index(query.Context.LastIndex) > m.index {
		log.WithFields(
			logging.String("NodeID", string(m.member.NodeID))).
			Debugf("Query index %d greater than last index %d", query.Context.LastIndex, m.index)
		m.scheduler.RunAtIndex(Index(query.Context.LastIndex), func() {
			m.sequenceQuery(query, stream)
		})
	} else {
		m.sequenceQuery(query, stream)
	}
}

func (m *stateManager) sequenceQuery(request *SessionQueryRequest, stream streams.WriteStream) {
	sessionManager, ok := m.sessions[request.Context.SessionID]
	if !ok {
		log.WithFields(
			logging.String("NodeID", string(m.member.NodeID)),
			logging.Uint64("SessionID", uint64(request.Context.SessionID))).
			Warn("Unknown session")
		stream.Error(fmt.Errorf("unknown session %d", request.Context.SessionID))
		stream.Close()
	} else {
		sequenceNumber := request.Context.LastRequestID
		if sequenceNumber > sessionManager.commandID {
			log.WithFields(
				logging.String("NodeID", string(m.member.NodeID)),
				logging.Uint64("SessionID", uint64(request.Context.SessionID))).
				Debugf("Query ID %d greater than last ID %d", sequenceNumber, sessionManager.commandID)
			sessionManager.scheduleQuery(sequenceNumber, func() {
				log.WithFields(
					logging.String("NodeID", string(m.member.NodeID)),
					logging.Uint64("SessionID", uint64(request.Context.SessionID))).
					Debugf("Executing query %d", sequenceNumber)
				m.applyServiceQuery(request.Query, request.Context, sessionManager, stream)
			})
		} else {
			log.WithFields(
				logging.String("NodeID", string(m.member.NodeID)),
				logging.Uint64("SessionID", uint64(request.Context.SessionID))).
				Debugf("Executing query %d", sequenceNumber)
			m.applyServiceQuery(request.Query, request.Context, sessionManager, stream)
		}
	}
}

func (m *stateManager) applyServiceQuery(request ServiceQueryRequest, context SessionQueryContext, sessionManager *sessionState, stream streams.WriteStream) {
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

func (m *stateManager) applyServiceQueryOperation(request ServiceQueryRequest, context SessionQueryContext, session *sessionState, stream streams.WriteStream) {
	service, ok := session.getService(request.ServiceID)
	if !ok {
		stream.Error(fmt.Errorf("no open session for service %s", request.ServiceID))
		stream.Close()
		return
	}

	// Get the service operation
	operation := service.Operations().GetOperation(request.GetOperation().OperationID)
	if operation == nil {
		stream.Error(fmt.Errorf("unknown operation: %s", request.GetOperation().OperationID))
		stream.Close()
		return
	}

	index := m.index
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
						Index:     index,
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
						Index:     index,
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

		queryStream := newQueryStream(session, StreamID(m.index), responseStream)
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

func (m *stateManager) applyServiceQueryMetadata(request ServiceQueryRequest, context SessionQueryContext, session *sessionState, stream streams.WriteStream) {
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
					Index:     uint64(m.index),
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
