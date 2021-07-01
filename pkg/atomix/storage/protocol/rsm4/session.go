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
	streams "github.com/atomix/atomix-go-framework/pkg/atomix/stream"
	"time"
)

// SessionID is a session identifier
type SessionID uint64

// Session is a service session
type Session interface {
	// ID returns the session identifier
	ID() SessionID
}

func newSession(service *serviceState, sessionID SessionID) *sessionState {
	return &sessionState{
		service:          service,
		sessionID:        sessionID,
		lastUpdated:      manager.timestamp,
		commandRequests:  make(map[RequestID]commandRequest),
		commandResponses: make(map[RequestID]*SessionCommandResponse),
		queryRequests:    make(map[RequestID]*list.List),
		streams:          make(map[StreamID]*commandStreamManager),
		services:         make(map[ServiceID]bool),
	}
}

// sessionState manages the ordering of request and response streams for a single client
type sessionState struct {
	service          *serviceState
	sessionID        SessionID
	lastUpdated      time.Time
	commandID        RequestID
	commandRequests  map[RequestID]commandRequest
	commandResponses map[RequestID]*SessionResponse
	queryRequests    map[RequestID]*list.List
	streams          map[StreamID]*commandStreamManager
	services         map[ServiceID]bool
}

func (s *sessionState) ID() SessionID {
	return s.sessionID
}

func (s *sessionState) snapshot() (*SessionSnapshot, error) {
	services := make([]ServiceID, 0, len(s.services))
	for service := range s.services {
		services = append(services, service)
	}
	requests := make([]*SessionCommandRequest, 0, len(s.commandRequests))
	for _, request := range s.commandRequests {
		requests = append(requests, request.request)
	}
	responses := make([]*SessionCommandResponse, 0, len(s.commandRequests))
	for _, response := range s.commandResponses {
		responses = append(responses, response)
	}
	streams := make([]*SessionStreamSnapshot, 0, len(s.streams))
	for _, stream := range s.streams {
		streamSnapshot, err := stream.snapshot()
		if err != nil {
			return nil, err
		}
		streams = append(streams, streamSnapshot)
	}
	return &SessionSnapshot{
		SessionID:     s.sessionID,
		Timestamp:     s.lastUpdated,
		LastRequestID: s.commandID,
		Services:      services,
		Requests:      requests,
		Responses:     responses,
		Streams:       streams,
	}, nil
}

func (s *sessionState) restore(snapshot *SessionSnapshot) error {
	s.sessionID = snapshot.SessionID
	s.lastUpdated = snapshot.Timestamp
	s.commandID = snapshot.LastRequestID
	for _, serviceID := range snapshot.Services {
		s.services[serviceID] = true
		service, ok := s.stateManager.services[serviceID]
		if !ok {
			log.Error("Missing service %s", serviceID)
		} else {
			service.addSession(s)
		}
	}
	for _, request := range snapshot.Requests {
		s.commandRequests[RequestID(request.Context.RequestID)] = commandRequest{
			request: request,
			stream:  streams.NewNilStream(),
		}
	}
	for _, response := range snapshot.Responses {
		r := *response
		s.commandResponses[RequestID(response.Context.RequestID)] = &r
	}
	for _, streamSnapshot := range snapshot.Streams {
		stream := newCommandStream(s, streamSnapshot.StreamID)
		if err := stream.restore(streamSnapshot); err != nil {
			return err
		}
		s.streams[stream.ID()] = stream
	}
	return nil
}

// addService adds a service session
func (s *sessionState) addService(id ServiceID) {
	s.services[id] = true
}

// removeService removes a service session
func (s *sessionState) removeService(id ServiceID) {
	delete(s.services, id)
}

func (s *sessionState) getService(id ServiceID) (*serviceState, bool) {
	_, ok := s.services[id]
	if !ok {
		return nil, false
	}
	service, ok := s.stateManager.services[id]
	if !ok {
		return nil, false
	}
	return service, true
}

// timedOut returns a boolean indicating whether the session is timed out
func (s *sessionState) timedOut(time time.Time) bool {
	return s.lastUpdated.UnixNano() > 0 && time.Sub(s.lastUpdated) > s.timeout
}

// scheduleQuery schedules a query to be executed after the given sequence number
func (s *sessionState) scheduleQuery(requestID RequestID, f func()) {
	queries, ok := s.queryRequests[requestID]
	if !ok {
		queries = list.New()
		s.queryRequests[requestID] = queries
	}
	queries.PushBack(f)
}

// scheduleCommand schedules a command to be executed at the given sequence number
func (s *sessionState) scheduleCommand(request *SessionCommandRequest, stream streams.WriteStream) {
	s.commandRequests[request.Context.RequestID] = commandRequest{
		request: request,
		stream:  stream,
	}
}

// nextCommandID returns the next command sequence number for the session
func (s *sessionState) nextCommandID() uint64 {
	return s.commandID + 1
}

func (s *sessionState) nextQuery() (*SessionQueryRequest, streams.WriteStream, bool) {
	queryRequests, ok := s.queryRequests[s.commandID]
	if ok {
		element := queryRequests.Front()
		if element != nil {
			queryRequests.Remove(element)
			query := element.Value.(queryRequest)
			return query.request, query.stream, true
		} else {
			delete(s.queryRequests, s.commandID)
		}
	}
	return nil, nil, false
}

func (s *sessionState) nextCommand() (*SessionCommandRequest, streams.WriteStream, bool) {
	s.commandID++
	command, ok := s.commandRequests[s.commandID]
	if ok {
		delete(s.commandRequests, s.commandID)
		return command.request, command.stream, true
	}
	return nil, nil, false
}

// close closes the session and completes all its streams
func (s *sessionState) close() {
	log.Debug("Session closed")
	for _, stream := range s.streams {
		stream.Close()
	}
	for _, service := range s.services {
		service.close()
	}
}

// getResult gets a unary result
func (s *sessionState) getResponse(requestID RequestID) (*SessionResponse, bool) {
	result, ok := s.commandResponses[requestID]
	return result, ok
}

// addResponse adds a unary result
func (s *sessionState) addResponse(requestID RequestID, result streams.Result) *SessionResponse {
	response := &SessionResponse{
		Type: SessionResponseType_RESPONSE,
		Status: SessionResponseStatus{
			Code:    getCode(result.Error),
			Message: getMessage(result.Error),
		},
		Response: &SessionResponse_Command{
			Command: &SessionCommandResponse{
				Context: SessionResponseContext{
					SessionID:  s.sessionID,
					RequestID:  requestID,
					Index:      s.index,
					ResponseID: 1,
				},
				Response: ServiceCommandResponse{
					Response: &ServiceCommandResponse_Operation{
						Operation: &ServiceOperationResponse{
							result.Value.([]byte),
						},
					},
				},
			},
		},
	}
	s.commandResponses[requestID] = response
	return response
}

// addStream adds a stream at the given sequence number
func (s *sessionState) addStream(requestID uint64, op OperationID, outStream streams.WriteStream) *commandStreamManager {
	stream := &commandStreamManager{
		sessionStreamManager: &sessionStreamManager{
			id:      StreamID(requestID),
			op:      op,
			session: s,
			stream:  outStream,
		},
		requestID: requestID,
		cluster:   s.cluster,
		member:    s.member,
		ctx:       s.ctx,
		results:   list.New(),
	}
	s.streams[requestID] = stream
	stream.open()
	log.Debugf("Stream open")
	return stream
}

// getStream returns a stream by the request sequence number
func (s *sessionState) getStream(requestID uint64) *commandStreamManager {
	return s.streams[requestID]
}

// ack acknowledges response streams up to the given request sequence number
func (s *sessionState) ack(ackRequestID uint64, streams []SessionStreamContext) {
	for requestID := range s.commandResponses {
		if requestID <= ackRequestID {
			delete(s.commandResponses, requestID)
		}
	}

	streamAcks := make(map[uint64]uint64)
	for _, stream := range streams {
		streamAcks[stream.RequestID] = stream.AckResponseID
	}

	for streamID, stream := range s.streams {
		// If the stream ID is greater than the acknowledged sequence number, skip it
		if stream.requestID > ackRequestID {
			continue
		}

		// If the stream is still held by the client, ack the stream.
		// Otherwise, close the stream.
		streamAck, open := streamAcks[stream.requestID]
		if open {
			stream.ack(streamAck)
		} else {
			stream.Close()
			delete(s.streams, streamID)
		}
	}
}

var _ Session = &sessionState{}

type commandRequest struct {
	request *SessionRequest
	stream  streams.WriteStream
}

type queryRequest struct {
	request *SessionQueryRequest
	stream  streams.WriteStream
}

// managerSessionService manages a service within a session
type managerSessionService struct {
	*sessionState
	service   ServiceID
	responses map[uint64]*SessionResponse
	streams   map[uint64]*commandStreamManager
}

func (s *managerSessionService) snapshot() (*SessionServiceSnapshot, error) {
	streams := make([]*SessionStreamSnapshot, 0, len(s.streams))
	for _, stream := range s.streams {
		streamSnapshot, err := stream.snapshot()
		if err != nil {
			return nil, err
		}
		streams = append(streams, streamSnapshot)
	}
	return &SessionServiceSnapshot{
		...
	}, nil
}

func (s *managerSessionService) restore(snapshot *SessionServiceSnapshot) error {

}

// getResult gets a unary result
func (s *managerSessionService) getResponse(requestID uint64) (*SessionResponse, bool) {
	result, ok := s.responses[requestID]
	return result, ok
}

// addResponse adds a unary result
func (s *managerSessionService) addResponse(requestID uint64, result streams.Result) *SessionResponse {
	response := &SessionResponse{
		Type: SessionResponseType_RESPONSE,
		Status: SessionResponseStatus{
			Code:    getCode(result.Error),
			Message: getMessage(result.Error),
		},
		Response: &SessionResponse_Command{
			Command: &SessionCommandResponse{
				Context: SessionResponseContext{
					SessionID: uint64(s.sessionID),
					RequestID: requestID,
					Index:     uint64(s.index),
					Sequence:  1,
				},
				Response: ServiceCommandResponse{
					Response: &ServiceCommandResponse_Operation{
						Operation: &ServiceOperationResponse{
							result.Value.([]byte),
						},
					},
				},
			},
		},
	}
	s.responses[requestID] = response
	return response
}

// addStream adds a stream at the given sequence number
func (s *managerSessionService) addStream(requestID uint64, op OperationID, outStream streams.WriteStream) *commandStreamManager {
	stream := &commandStreamManager{
		sessionStreamManager: &sessionStreamManager{
			id:      StreamID(requestID),
			op:      op,
			session: s,
			stream:  outStream,
		},
		requestID: requestID,
		cluster:   s.cluster,
		member:    s.member,
		ctx:       s.ctx,
		results:   list.New(),
	}
	s.streams[requestID] = stream
	stream.open()
	log.Debugf("Stream open")
	return stream
}

// getStream returns a stream by the request sequence number
func (s *managerSessionService) getStream(requestID uint64) *commandStreamManager {
	return s.streams[requestID]
}

// ack acknowledges response streams up to the given request sequence number
func (s *managerSessionService) ack(ackRequestID uint64, streams []SessionStreamContext) {
	for requestID := range s.responses {
		if requestID <= ackRequestID {
			delete(s.responses, requestID)
		}
	}

	streamAcks := make(map[uint64]uint64)
	for _, stream := range streams {
		streamAcks[stream.RequestID] = stream.AckResponseID
	}

	for streamID, stream := range s.streams {
		// If the stream ID is greater than the acknowledged sequence number, skip it
		if stream.requestID > ackRequestID {
			continue
		}

		// If the stream is still held by the client, ack the stream.
		// Otherwise, close the stream.
		streamAck, open := streamAcks[stream.requestID]
		if open {
			stream.ack(streamAck)
		} else {
			stream.Close()
			delete(s.streams, streamID)
		}
	}
}

// close closes the session and completes all its streams
func (s *managerSessionService) close() {
	for _, stream := range s.streams {
		stream.Close()
	}
}

var _ Session = &managerSessionService{}
