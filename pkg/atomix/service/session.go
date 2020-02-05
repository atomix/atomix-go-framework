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
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/atomix/go-framework/pkg/atomix/util"
	"github.com/gogo/protobuf/proto"
	"time"
)

// newSession creates a new session
func newSession(ctx Context, timeout *time.Duration) *Session {
	if timeout == nil {
		defaultTimeout := 30 * time.Second
		timeout = &defaultTimeout
	}
	session := &Session{
		ID:               ctx.Index(),
		Timeout:          *timeout,
		LastUpdated:      ctx.Timestamp(),
		ctx:              ctx,
		commandCallbacks: make(map[uint64]func()),
		queryCallbacks:   make(map[uint64]*list.List),
		results:          make(map[uint64]streams.Result),
		streams:          make(map[uint64]*sessionStream),
		services:         make(map[qualifiedServiceName]bool),
	}
	util.SessionEntry(ctx.Node(), session.ID).
		Debug("Session open")
	return session
}

// Session manages the ordering of request and response streams for a single client
type Session struct {
	ID               uint64
	Timeout          time.Duration
	LastUpdated      time.Time
	ctx              Context
	commandSequence  uint64
	ackSequence      uint64
	commandCallbacks map[uint64]func()
	queryCallbacks   map[uint64]*list.List
	results          map[uint64]streams.Result
	streams          map[uint64]*sessionStream
	streamID         uint64
	services         map[qualifiedServiceName]bool
}

// timedOut returns a boolean indicating whether the session is timed out
func (s *Session) timedOut(time time.Time) bool {
	return s.LastUpdated.UnixNano() > 0 && time.Sub(s.LastUpdated) > s.Timeout
}

// StreamID returns the ID of the current stream
func (s *Session) StreamID() uint64 {
	return s.streamID
}

// Streams returns a slice of all open streams of any type owned by the session
func (s *Session) Streams() []streams.WriteStream {
	streams := make([]streams.WriteStream, 0, len(s.streams))
	for _, stream := range s.streams {
		streams = append(streams, stream)
	}
	return streams
}

// Stream returns the given stream
func (s *Session) Stream(id uint64) streams.WriteStream {
	return s.streams[id]
}

// StreamsOf returns a slice of all open streams for the given named operation owned by the session
func (s *Session) StreamsOf(op string) []streams.WriteStream {
	streams := make([]streams.WriteStream, 0, len(s.streams))
	for _, stream := range s.streams {
		if stream.Type == op {
			streams = append(streams, stream)
		}
	}
	return streams
}

// getResult gets a unary result
func (s *Session) getResult(id uint64) (streams.Result, bool) {
	result, ok := s.results[id]
	return result, ok
}

// addResult adds a unary result
func (s *Session) addResult(id uint64, result streams.Result) streams.Result {
	if result.Succeeded() {
		bytes, err := proto.Marshal(&SessionResponse{
			Response: &SessionResponse_Command{
				Command: &SessionCommandResponse{
					Context: &SessionResponseContext{
						StreamID: s.ID,
						Index:    s.ctx.Index(),
						Sequence: 1,
					},
					Response: &ServiceCommandResponse{
						Response: &ServiceCommandResponse_Operation{
							Operation: &ServiceOperationResponse{
								result.Value.([]byte),
							},
						},
					},
				},
			},
		})
		result = streams.Result{
			Value: bytes,
			Error: err,
		}
	}
	s.results[id] = result
	return result
}

// addStream adds a stream at the given sequence number
func (s *Session) addStream(id uint64, op string, outStream streams.WriteStream) streams.WriteStream {
	stream := &sessionStream{
		ID:      id,
		Type:    op,
		session: s,
		ctx:     s.ctx,
		stream:  outStream,
		results: list.New(),
	}
	s.streams[id] = stream
	s.streamID = id
	stream.open()
	util.StreamEntry(s.ctx.Node(), s.ID, id).
		Trace("Stream open")
	return stream
}

// getStream returns a stream by the request sequence number
func (s *Session) getStream(id uint64) *sessionStream {
	return s.streams[id]
}

// ack acknowledges response streams up to the given request sequence number
func (s *Session) ack(id uint64, streams map[uint64]uint64) {
	for responseID := range s.results {
		if responseID > id {
			continue
		}
		delete(s.results, responseID)
	}
	for streamID, stream := range s.streams {
		// If the stream ID is greater than the acknowledged sequence number, skip it
		if stream.ID > id {
			continue
		}

		// If the stream is still held by the client, ack the stream.
		// Otherwise, close the stream.
		streamAck, open := streams[stream.ID]
		if open {
			stream.ack(streamAck)
		} else {
			stream.Close()
			delete(s.streams, streamID)
		}
	}
	s.ackSequence = id
}

// scheduleQuery schedules a query to be executed after the given sequence number
func (s *Session) scheduleQuery(sequenceNumber uint64, f func()) {
	queries, ok := s.queryCallbacks[sequenceNumber]
	if !ok {
		queries = list.New()
		s.queryCallbacks[sequenceNumber] = queries
	}
	queries.PushBack(f)
}

// scheduleCommand schedules a command to be executed at the given sequence number
func (s *Session) scheduleCommand(sequenceNumber uint64, f func()) {
	s.commandCallbacks[sequenceNumber] = f
}

// nextCommandSequence returns the next command sequence number for the session
func (s *Session) nextCommandSequence() uint64 {
	return s.commandSequence + 1
}

// completeCommand completes operations up to the given sequence number and executes commands and
// queries pending for the sequence number to be completed
func (s *Session) completeCommand(sequenceNumber uint64) {
	for i := s.commandSequence + 1; i <= sequenceNumber; i++ {
		s.commandSequence = i
		queries, ok := s.queryCallbacks[i]
		if ok {
			query := queries.Front()
			for query != nil {
				query.Value.(func())()
				query = query.Next()
			}
			delete(s.queryCallbacks, i)
		}

		command, ok := s.commandCallbacks[s.nextCommandSequence()]
		if ok {
			command()
			delete(s.commandCallbacks, i)
		}
	}
}

// close closes the session and completes all its streams
func (s *Session) close() {
	util.SessionEntry(s.ctx.Node(), s.ID).
		Debug("Session closed")
	for _, stream := range s.streams {
		stream.Close()
	}
}
