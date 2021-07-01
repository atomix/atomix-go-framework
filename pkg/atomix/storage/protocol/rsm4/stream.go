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
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	streams "github.com/atomix/atomix-go-framework/pkg/atomix/stream"
)

// StreamID is a stream identifier
type StreamID uint64

type RequestID uint64

type ResponseID uint64

// Stream is a service stream
type Stream interface {
	streams.WriteStream

	// ID returns the stream identifier
	ID() StreamID

	// Session returns the stream session
	Session() Session
}

func newRequestStream(session *sessionState, serviceID ServiceID, streamID StreamID, stream streams.WriteStream) *sessionStreamManager {
	return &sessionStreamManager{
		session:   session,
		serviceID: serviceID,
		streamID:  streamID,
		stream:    stream,
	}
}

type sessionStreamManager struct {
	session   *sessionState
	serviceID ServiceID
	streamID  StreamID
	stream    streams.WriteStream
}

func (s *sessionStreamManager) ID() StreamID {
	return s.streamID
}

func (s *sessionStreamManager) Session() Session {
	return s.session
}

func (s *sessionStreamManager) Send(out streams.Result) {
	s.stream.Send(out)
}

func (s *sessionStreamManager) Result(value interface{}, err error) {
	s.stream.Result(value, err)
}

func (s *sessionStreamManager) Value(value interface{}) {
	s.stream.Value(value)
}

func (s *sessionStreamManager) Error(err error) {
	s.stream.Error(err)
}

func newQueryStream(session *sessionState, serviceID ServiceID, requestID RequestID, stream streams.WriteStream) *queryStreamManager {
	s := &queryStreamManager{
		sessionStreamManager: newRequestStream(session, serviceID, StreamID(requestID), stream),
	}
	s.open()
	return s
}

// queryStreamManager wraps a stream for a query
type queryStreamManager struct {
	*sessionStreamManager
	closed bool
}

func (s *queryStreamManager) open() {
	service, ok := s.session.getService(s.serviceID)
	if ok {
		service.addStream(s)
	}
}

func (s *queryStreamManager) Close() {
	if !s.closed {
		s.closed = true
		service, ok := s.session.getService(s.serviceID)
		if ok {
			service.removeStream(s)
		}
	}
	s.stream.Close()
}

func newCommandStream(session *sessionState, serviceID ServiceID, operationID OperationID, requestID RequestID, stream streams.WriteStream) *commandStreamManager {
	s := &commandStreamManager{
		sessionStreamManager: newRequestStream(session, serviceID, StreamID(requestID), stream),
		operationID:          operationID,
		requestID:            requestID,
		lastIndex:            session.index,
	}
	s.open()
	return s
}

// commandStreamManager manages a single stream for a session
type commandStreamManager struct {
	*sessionStreamManager
	operationID      OperationID
	requestID        RequestID
	responseID       ResponseID
	ackResponseID    ResponseID
	lastIndex        Index
	pendingResponses map[ResponseID]*SessionResponse
	closed           bool
}

// sessionStreamResult contains a single stream result
type sessionStreamResult struct {
	id       ResponseID
	index    Index
	response *SessionResponse
}

func (s *commandStreamManager) snapshot() (*SessionStreamSnapshot, error) {
	pendingResponses := make([]*SessionResponse, 0, len(s.pendingResponses))
	for _, response := range s.pendingResponses {
		pendingResponses = append(pendingResponses, response)
	}
	return &SessionStreamSnapshot{
		ServiceID:        s.serviceID,
		OperationID:      s.operationID,
		RequestID:        s.requestID,
		ResponseID:       s.responseID,
		AckResponseID:    s.ackResponseID,
		PendingResponses: pendingResponses,
	}, nil
}

func (s *commandStreamManager) restore(snapshot *SessionStreamSnapshot) error {
	s.serviceID = snapshot.ServiceID
	s.operationID = snapshot.OperationID
	s.requestID = snapshot.RequestID
	s.responseID = snapshot.ResponseID
	s.ackResponseID = snapshot.AckResponseID
	s.pendingResponses = make(map[ResponseID]*SessionResponse)
	for _, pendingResponse := range snapshot.PendingResponses {
		s.pendingResponses[pendingResponse.GetCommand().Context.ResponseID] = pendingResponse
	}
	return nil
}

// open opens the stream
func (s *commandStreamManager) open() {
	service, ok := s.session.getService(s.serviceID)
	if ok {
		service.addStream(s)
	}

	s.updateClock()

	response := &SessionResponse{
		Type: SessionResponseType_OPEN_STREAM,
		Status: SessionResponseStatus{
			Code: SessionResponseCode_OK,
		},
		Response: &SessionResponse_Command{
			Command: &SessionCommandResponse{
				Context: SessionResponseContext{
					SessionID:  s.session.ID(),
					RequestID:  s.requestID,
					Index:      s.lastIndex,
					ResponseID: s.responseID,
				},
			},
		},
	}

	out := sessionStreamResult{
		id:       s.responseID,
		index:    s.ctx.index,
		response: response,
	}
	s.results.PushBack(out)

	log.WithFields(
		logging.String("NodeID", string(s.member.NodeID)),
		logging.Uint64("SessionID", uint64(s.session.ID())),
		logging.Uint64("StreamID", uint64(s.ID()))).
		Debugf("Sending stream open %d %v", s.responseID, out.response)
	s.stream.Value(out.response)
}

func (s *commandStreamManager) updateClock() {
	// If the client acked a sequence number greater than the current event sequence number since we know the
	// client must have received it from another server.
	s.responseID++
	if s.ackResponseID > s.responseID {
		log.WithFields(
			logging.String("NodeID", string(s.member.NodeID)),
			logging.Uint64("SessionID", uint64(s.session.ID())),
			logging.Uint64("StreamID", uint64(s.ID()))).
			Debugf("Skipped completed result %d", s.responseID)
		return
	}

	// Record the last index sent on the stream
	s.lastIndex = s.session.index
}

func (s *commandStreamManager) Send(result streams.Result) {
	s.updateClock()

	// Create the stream result and add it to the commandResponses list.
	response := &SessionResponse{
		Type: SessionResponseType_RESPONSE,
		Status: SessionResponseStatus{
			Code:    getCode(result.Error),
			Message: getMessage(result.Error),
		},
		Response: &SessionResponse_Command{
			Command: &SessionCommandResponse{
				Context: SessionResponseContext{
					SessionID: uint64(s.session.ID()),
					RequestID: uint64(s.requestID),
					Index:     uint64(s.lastIndex),
					Sequence:  uint64(s.responseID),
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

	s.pendingResponses[s.responseID] = response
	s.results.PushBack(out)
	log.WithFields(
		logging.String("NodeID", string(s.member.NodeID)),
		logging.Uint64("SessionID", uint64(s.session.ID())),
		logging.Uint64("StreamID", uint64(s.ID()))).
		Debugf("Cached response %d", s.responseID)

	// If the out channel is set, send the result
	log.WithFields(
		logging.String("NodeID", string(s.member.NodeID)),
		logging.Uint64("SessionID", uint64(s.session.ID())),
		logging.Uint64("StreamID", uint64(s.ID()))).
		Debugf("Sending response %d %v", s.responseID, out.response)
	s.stream.Value(out.response)
}

func (s *commandStreamManager) Result(value interface{}, err error) {
	s.Send(streams.Result{
		Value: value,
		Error: err,
	})
}

func (s *commandStreamManager) Value(value interface{}) {
	s.Result(value, nil)
}

func (s *commandStreamManager) Error(err error) {
	s.Result(nil, err)
}

func (s *commandStreamManager) Close() {
	log.WithFields(
		logging.String("NodeID", string(s.member.NodeID)),
		logging.Uint64("SessionID", uint64(s.session.ID())),
		logging.Uint64("StreamID", uint64(s.ID()))).
		Debug("Stream closed")
	s.updateClock()

	response := &SessionResponse{
		Type: SessionResponseType_CLOSE_STREAM,
		Status: SessionResponseStatus{
			Code: SessionResponseCode_OK,
		},
		Response: &SessionResponse_Command{
			Command: &SessionCommandResponse{
				Context: SessionResponseContext{
					SessionID: uint64(s.session.ID()),
					RequestID: s.requestID,
					Index:     uint64(s.lastIndex),
					Sequence:  s.responseID,
				},
			},
		},
	}

	out := sessionStreamResult{
		id:       s.responseID,
		index:    s.ctx.index,
		response: response,
	}
	s.results.PushBack(out)

	log.WithFields(
		logging.String("NodeID", string(s.member.NodeID)),
		logging.Uint64("SessionID", uint64(s.session.ID())),
		logging.Uint64("StreamID", uint64(s.ID()))).
		Debugf("Sending stream close %d %v", s.responseID, out.response)
	s.stream.Value(out.response)
	s.stream.Close()

	if !s.closed {
		s.closed = true
		service, ok := s.session.getService(s.serviceID)
		if ok {
			service.removeStream(s)
		}
	}
}

// ack acknowledges results up to the given ID
func (s *commandStreamManager) ack(id uint64) {
	if id > s.ackResponseID {
		event := s.results.Front()
		for event != nil && event.Value.(sessionStreamResult).id <= id {
			next := event.Next()
			s.results.Remove(event)
			s.ackResponseID = event.Value.(sessionStreamResult).id
			event = next
		}
		log.WithFields(
			logging.String("NodeID", string(s.member.NodeID)),
			logging.Uint64("SessionID", uint64(s.session.ID())),
			logging.Uint64("StreamID", uint64(s.ID()))).
			Debugf("Discarded cached responses up to %d", id)
	}
}

// replay resends results on the given channel
func (s *commandStreamManager) replay(stream streams.WriteStream) {
	result := s.results.Front()
	for result != nil {
		response := result.Value.(sessionStreamResult)
		log.WithFields(
			logging.String("NodeID", string(s.member.NodeID)),
			logging.Uint64("SessionID", uint64(s.session.ID())),
			logging.Uint64("StreamID", uint64(s.ID()))).
			Debugf("Sending response %d %v", response.id, response.response)
		stream.Value(response.response)
		result = result.Next()
	}
	s.stream = stream
}
