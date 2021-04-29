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
	"github.com/gogo/protobuf/proto"
)

// StreamID is a stream identifier
type StreamID uint64

// Stream is a service stream
type Stream interface {
	streams.WriteStream

	// ID returns the stream identifier
	ID() StreamID

	// OperationID returns the stream operation identifier
	OperationID() OperationID

	// Session returns the stream session
	Session() Session
}

// StreamCloser is a function to be called when a stream is closed
type StreamCloser func()

type opStream struct {
	id      StreamID
	op      OperationID
	stream  streams.WriteStream
	session Session
	closer  StreamCloser
	closed  bool
}

func (s *opStream) ID() StreamID {
	return s.id
}

func (s *opStream) OperationID() OperationID {
	return s.op
}

func (s *opStream) Session() Session {
	return s.session
}

func (s *opStream) Send(out streams.Result) {
	s.stream.Send(out)
}

func (s *opStream) Result(value interface{}, err error) {
	s.stream.Result(value, err)
}

func (s *opStream) Value(value interface{}) {
	s.stream.Value(value)
}

func (s *opStream) Error(err error) {
	s.stream.Error(err)
}

func (s *opStream) setCloser(closer StreamCloser) {
	if s.closed {
		if closer != nil {
			closer()
		}
	} else {
		s.closer = closer
	}
}

func (s *opStream) Close() {
	if !s.closed {
		s.closed = true
		if s.closer != nil {
			s.closer()
		}
	}
	s.stream.Close()
}

// queryStream wraps a stream for a query
type queryStream struct {
	*opStream
}

// sessionStream manages a single stream for a session
type sessionStream struct {
	*opStream
	cluster    cluster.Cluster
	member     *cluster.Member
	responseID uint64
	completeID uint64
	lastIndex  Index
	ctx        PartitionContext
	results    *list.List
}

// sessionStreamResult contains a single stream result
type sessionStreamResult struct {
	id     uint64
	index  Index
	result streams.Result
}

// open opens the stream
func (s *sessionStream) open() {
	s.updateClock()

	bytes, err := proto.Marshal(&SessionResponse{
		Type: SessionResponseType_OPEN_STREAM,
		Status: SessionResponseStatus{
			Code: SessionResponseCode_OK,
		},
		Response: &SessionResponse_Command{
			Command: &SessionCommandResponse{
				Context: SessionResponseContext{
					SessionID: uint64(s.session.ID()),
					StreamID:  uint64(s.ID()),
					Index:     uint64(s.lastIndex),
					Sequence:  s.responseID,
				},
			},
		},
	})
	result := streams.Result{
		Value: bytes,
		Error: err,
	}

	out := sessionStreamResult{
		id:     s.responseID,
		index:  s.ctx.Index(),
		result: result,
	}
	s.results.PushBack(out)

	log.WithFields(
		logging.String("NodeID", string(s.member.NodeID)),
		logging.Uint64("SessionID", uint64(s.session.ID())),
		logging.Uint64("StreamID", uint64(s.ID()))).
		Debugf("Sending stream open %d %v", s.responseID, out.result)
	s.stream.Send(out.result)
}

func (s *sessionStream) updateClock() {
	// If the client acked a sequence number greater than the current event sequence number since we know the
	// client must have received it from another server.
	s.responseID++
	if s.completeID > s.responseID {
		log.WithFields(
			logging.String("NodeID", string(s.member.NodeID)),
			logging.Uint64("SessionID", uint64(s.session.ID())),
			logging.Uint64("StreamID", uint64(s.ID()))).
			Debugf("Skipped completed result %d", s.responseID)
		return
	}

	// Record the last index sent on the stream
	s.lastIndex = s.ctx.Index()
}

func (s *sessionStream) Send(result streams.Result) {
	s.updateClock()

	// Create the stream result and add it to the results list.
	bytes, err := proto.Marshal(&SessionResponse{
		Type: SessionResponseType_RESPONSE,
		Status: SessionResponseStatus{
			Code:    getCode(result.Error),
			Message: getMessage(result.Error),
		},
		Response: &SessionResponse_Command{
			Command: &SessionCommandResponse{
				Context: SessionResponseContext{
					SessionID: uint64(s.session.ID()),
					StreamID:  uint64(s.ID()),
					Index:     uint64(s.lastIndex),
					Sequence:  s.responseID,
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
	})

	out := sessionStreamResult{
		id:    s.responseID,
		index: s.ctx.Index(),
		result: streams.Result{
			Value: bytes,
			Error: err,
		},
	}
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
		Debugf("Sending response %d %v", s.responseID, out.result)
	s.stream.Send(out.result)
}

func (s *sessionStream) Result(value interface{}, err error) {
	s.Send(streams.Result{
		Value: value,
		Error: err,
	})
}

func (s *sessionStream) Value(value interface{}) {
	s.Result(value, nil)
}

func (s *sessionStream) Error(err error) {
	s.Result(nil, err)
}

func (s *sessionStream) setCloser(closer StreamCloser) {
	if s.closed {
		if closer != nil {
			closer()
		}
	} else {
		s.closer = closer
	}
}

func (s *sessionStream) Close() {
	if !s.closed {
		s.closed = true
		if s.closer != nil {
			s.closer()
		}
	}

	log.WithFields(
		logging.String("NodeID", string(s.member.NodeID)),
		logging.Uint64("SessionID", uint64(s.session.ID())),
		logging.Uint64("StreamID", uint64(s.ID()))).
		Debug("Stream closed")
	s.updateClock()

	bytes, err := proto.Marshal(&SessionResponse{
		Type: SessionResponseType_CLOSE_STREAM,
		Status: SessionResponseStatus{
			Code: SessionResponseCode_OK,
		},
		Response: &SessionResponse_Command{
			Command: &SessionCommandResponse{
				Context: SessionResponseContext{
					SessionID: uint64(s.session.ID()),
					StreamID:  uint64(s.ID()),
					Index:     uint64(s.lastIndex),
					Sequence:  s.responseID,
				},
			},
		},
	})
	result := streams.Result{
		Value: bytes,
		Error: err,
	}

	out := sessionStreamResult{
		id:     s.responseID,
		index:  s.ctx.Index(),
		result: result,
	}
	s.results.PushBack(out)

	log.WithFields(
		logging.String("NodeID", string(s.member.NodeID)),
		logging.Uint64("SessionID", uint64(s.session.ID())),
		logging.Uint64("StreamID", uint64(s.ID()))).
		Debugf("Sending stream close %d %v", s.responseID, out.result)
	s.stream.Send(out.result)
	s.stream.Close()
}

// ack acknowledges results up to the given ID
func (s *sessionStream) ack(id uint64) {
	if id > s.completeID {
		event := s.results.Front()
		for event != nil && event.Value.(sessionStreamResult).id <= id {
			next := event.Next()
			s.results.Remove(event)
			s.completeID = event.Value.(sessionStreamResult).id
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
func (s *sessionStream) replay(stream streams.WriteStream) {
	result := s.results.Front()
	for result != nil {
		response := result.Value.(sessionStreamResult)
		log.WithFields(
			logging.String("NodeID", string(s.member.NodeID)),
			logging.Uint64("SessionID", uint64(s.session.ID())),
			logging.Uint64("StreamID", uint64(s.ID()))).
			Debugf("Sending response %d %v", response.id, response.result)
		stream.Send(response.result)
		result = result.Next()
	}
	s.stream = stream
}
