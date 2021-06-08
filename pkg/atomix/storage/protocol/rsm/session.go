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
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	streams "github.com/atomix/atomix-go-framework/pkg/atomix/stream"
	"time"
)

// ClientID is a client identifier
type ClientID string

// SessionID is a session identifier
type SessionID uint64

// Session is a service session
type Session interface {
	// ID returns the session identifier
	ID() SessionID

	// ClientID returns the client identifier
	ClientID() ClientID
}

// newSessionManager creates a new session manager
func newSessionManager(cluster cluster.Cluster, ctx *managerContext, clientID ClientID, timeout *time.Duration) *sessionManager {
	if timeout == nil {
		defaultTimeout := 30 * time.Second
		timeout = &defaultTimeout
	}
	member, _ := cluster.Member()
	log := log.WithFields(
		logging.String("NodeID", string(member.NodeID)),
		logging.Uint64("SessionID", uint64(ctx.index)))
	session := &sessionManager{
		cluster:          cluster,
		member:           member,
		log:              log,
		id:               SessionID(ctx.index),
		clientID:         clientID,
		timeout:          *timeout,
		lastUpdated:      ctx.timestamp,
		ctx:              ctx,
		commandCallbacks: make(map[uint64]func()),
		queryCallbacks:   make(map[uint64]*list.List),
		results:          make(map[uint64]streams.Result),
		services:         make(map[ServiceID]*serviceSession),
	}
	log.Debug("Session open")
	return session
}

// sessionManager manages the ordering of request and response streams for a single client
type sessionManager struct {
	cluster          cluster.Cluster
	member           *cluster.Member
	log              logging.Logger
	id               SessionID
	clientID         ClientID
	timeout          time.Duration
	lastUpdated      time.Time
	ctx              *managerContext
	commandID        uint64
	commandCallbacks map[uint64]func()
	queryCallbacks   map[uint64]*list.List
	results          map[uint64]streams.Result
	services         map[ServiceID]*serviceSession
}

// getService gets the service session
func (s *sessionManager) getService(id ServiceID) *serviceSession {
	return s.services[id]
}

// addService adds a service session
func (s *sessionManager) addService(id ServiceID) *serviceSession {
	session := newServiceSession(s, id)
	s.services[id] = session
	return session
}

// removeService removes a service session
func (s *sessionManager) removeService(id ServiceID) *serviceSession {
	session := s.services[id]
	delete(s.services, id)
	return session
}

// timedOut returns a boolean indicating whether the session is timed out
func (s *sessionManager) timedOut(time time.Time) bool {
	return s.lastUpdated.UnixNano() > 0 && time.Sub(s.lastUpdated) > s.timeout
}

// getResult gets a unary result
func (s *sessionManager) getUnaryResult(id uint64) (streams.Result, bool) {
	result, ok := s.results[id]
	return result, ok
}

// addUnaryResult adds a unary result
func (s *sessionManager) addUnaryResult(requestID uint64, result streams.Result) streams.Result {
	response := &SessionResponse{
		Type: SessionResponseType_RESPONSE,
		Status: SessionResponseStatus{
			Code:    getCode(result.Error),
			Message: getMessage(result.Error),
		},
		Response: &SessionResponse_Command{
			Command: &SessionCommandResponse{
				Context: SessionResponseContext{
					SessionID: uint64(s.id),
					RequestID: requestID,
					Index:     uint64(s.ctx.index),
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
	result = streams.Result{
		Value: response,
	}
	s.results[requestID] = result
	return result
}

// scheduleQuery schedules a query to be executed after the given sequence number
func (s *sessionManager) scheduleQuery(sequenceNumber uint64, f func()) {
	queries, ok := s.queryCallbacks[sequenceNumber]
	if !ok {
		queries = list.New()
		s.queryCallbacks[sequenceNumber] = queries
	}
	queries.PushBack(f)
}

// scheduleCommand schedules a command to be executed at the given sequence number
func (s *sessionManager) scheduleCommand(sequenceNumber uint64, f func()) {
	s.commandCallbacks[sequenceNumber] = f
}

// nextCommandID returns the next command sequence number for the session
func (s *sessionManager) nextCommandID() uint64 {
	return s.commandID + 1
}

// completeCommand completes operations up to the given sequence number and executes commands and
// queries pending for the sequence number to be completed
func (s *sessionManager) completeCommand(sequenceNumber uint64) {
	for i := s.commandID + 1; i <= sequenceNumber; i++ {
		s.commandID = i
		queries, ok := s.queryCallbacks[i]
		if ok {
			query := queries.Front()
			for query != nil {
				query.Value.(func())()
				query = query.Next()
			}
			delete(s.queryCallbacks, i)
		}

		command, ok := s.commandCallbacks[s.nextCommandID()]
		if ok {
			command()
			delete(s.commandCallbacks, i)
		}
	}
}

// close closes the session and completes all its streams
func (s *sessionManager) close() {
	s.log.Debug("Session closed")
	for _, service := range s.services {
		service.close()
	}
}

// newServiceSession creates a new service session
func newServiceSession(session *sessionManager, service ServiceID) *serviceSession {
	return &serviceSession{
		sessionManager: session,
		service:        service,
		streams:        make(map[uint64]*sessionStream),
	}
}

// serviceSession manages sessions within a service
type serviceSession struct {
	*sessionManager
	service ServiceID
	streams map[uint64]*sessionStream
}

func (s *serviceSession) ID() SessionID {
	return s.id
}

func (s *serviceSession) ClientID() ClientID {
	return s.clientID
}

// addStream adds a stream at the given sequence number
func (s *serviceSession) addStream(requestID uint64, op OperationID, outStream streams.WriteStream) *sessionStream {
	stream := &sessionStream{
		opStream: &opStream{
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
	s.log.Debugf("Stream open")
	return stream
}

// getStream returns a stream by the request sequence number
func (s *serviceSession) getStream(requestID uint64) *sessionStream {
	return s.streams[requestID]
}

// ack acknowledges response streams up to the given request sequence number
func (s *serviceSession) ack(ackRequestID uint64, streams []SessionStreamContext) {
	for requestID := range s.results {
		if requestID <= ackRequestID {
			delete(s.results, requestID)
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
func (s *serviceSession) close() {
	for _, stream := range s.streams {
		stream.Close()
	}
}

var _ Session = &serviceSession{}
