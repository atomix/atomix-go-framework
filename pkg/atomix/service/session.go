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
	"fmt"
	streams "github.com/atomix/atomix-go-node/pkg/atomix/stream"
	"github.com/atomix/atomix-go-node/pkg/atomix/util"
	"github.com/golang/protobuf/proto"
	"io"
	"math"
	"time"
)

// NewSessionizedService returns an initialized SessionizedService
func NewSessionizedService(parent Context) *SessionizedService {
	ctx := &mutableContext{
		parent: parent,
	}
	return &SessionizedService{
		service: &service{
			Scheduler: newScheduler(),
			Executor:  newExecutor(),
			Context:   ctx,
		},
		context:  ctx,
		parent:   parent,
		sessions: make(map[uint64]*Session),
		onOpen:   func(session *Session) {},
		onExpire: func(session *Session) {},
		onClose:  func(session *Session) {},
	}
}

// SessionizedService is a Service implementation for primitives that support sessions
type SessionizedService struct {
	Service
	*service
	context  *mutableContext
	parent   Context
	sessions map[uint64]*Session
	session  *Session
	onOpen   func(*Session)
	onExpire func(*Session)
	onClose  func(*Session)
}

// Session returns the currently active session
func (s *SessionizedService) Session() *Session {
	return s.session
}

// Sessions returns a map of currently active sessions
func (s *SessionizedService) Sessions() map[uint64]*Session {
	return s.sessions
}

// Snapshot takes a snapshot of the service
func (s *SessionizedService) Snapshot(writer io.Writer) error {
	return s.snapshotSessions(writer)
}

func (s *SessionizedService) snapshotSessions(writer io.Writer) error {
	return util.WriteMap(writer, s.sessions, func(id uint64, session *Session) ([]byte, error) {
		streams := make([]*SessionStreamSnapshot, 0, len(session.streams))
		for _, stream := range session.streams {
			streams = append(streams, &SessionStreamSnapshot{
				StreamId:       stream.ID,
				Type:           stream.Type,
				SequenceNumber: stream.responseID,
				LastCompleted:  stream.completeID,
			})
		}
		snapshot := &SessionSnapshot{
			SessionID:       session.ID,
			Timeout:         session.Timeout,
			Timestamp:       session.LastUpdated,
			CommandSequence: session.commandSequence,
			Streams:         streams,
		}
		return proto.Marshal(snapshot)
	})
}

// Install installs a snapshot of the service
func (s *SessionizedService) Install(reader io.Reader) error {
	return s.installSessions(reader)
}

func (s *SessionizedService) installSessions(reader io.Reader) error {
	s.sessions = make(map[uint64]*Session)
	return util.ReadMap(reader, s.sessions, func(data []byte) (uint64, *Session, error) {
		snapshot := &SessionSnapshot{}
		if err := proto.Unmarshal(data, snapshot); err != nil {
			return 0, nil, err
		}

		session := &Session{
			ID:               snapshot.SessionID,
			Timeout:          time.Duration(snapshot.Timeout),
			LastUpdated:      snapshot.Timestamp,
			ctx:              s.Context,
			commandSequence:  snapshot.CommandSequence,
			commandCallbacks: make(map[uint64]func()),
			queryCallbacks:   make(map[uint64]*list.List),
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
				ctx:        s.Context,
				stream:     streams.NewNilStream(),
				results:    list.New(),
			}
		}
		session.streams = sessionStreams
		return session.ID, session, nil
	})
}

// CanDelete returns a boolean indicating whether entries up to the given index can be deleted
func (s *SessionizedService) CanDelete(index uint64) bool {
	lastCompleted := index
	for _, session := range s.sessions {
		for _, stream := range session.streams {
			lastCompleted = uint64(math.Min(float64(lastCompleted), float64(stream.completeIndex())))
		}
	}
	return lastCompleted >= index
}

// Command handles a service command
func (s *SessionizedService) Command(bytes []byte, stream streams.WriteStream) {
	s.context.setCommand()
	request := &SessionRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		stream.Error(err)
		stream.Close()
	} else {
		scheduler := s.Scheduler.(*scheduler)
		scheduler.runScheduledTasks(s.Context.Timestamp())

		switch r := request.Request.(type) {
		case *SessionRequest_Command:
			s.applyCommand(r.Command, stream)
		case *SessionRequest_OpenSession:
			s.applyOpenSession(r.OpenSession, stream)
		case *SessionRequest_KeepAlive:
			s.applyKeepAlive(r.KeepAlive, stream)
		case *SessionRequest_CloseSession:
			s.applyCloseSession(r.CloseSession, stream)
		}

		scheduler.runImmediateTasks()
		scheduler.runIndex(s.Context.Index())
	}
}

func (s *SessionizedService) applyCommand(request *SessionCommandRequest, stream streams.WriteStream) {
	session, ok := s.sessions[request.Context.SessionID]
	if !ok {
		util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), request.Context.SessionID).
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
				if stream != nil {
					streamCtx.replay(streamCtx)
				} else {
					stream.Error(fmt.Errorf("sequence number %d has already been acknowledged", sequenceNumber))
					stream.Close()
				}
			}
		} else if sequenceNumber > session.nextCommandSequence() {
			session.scheduleCommand(sequenceNumber, func() {
				util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), request.Context.SessionID).
					Tracef("Executing command %d", sequenceNumber)
				s.applySessionCommand(request, stream)
			})
		} else {
			util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), request.Context.SessionID).
				Tracef("Executing command %d", sequenceNumber)
			s.applySessionCommand(request, stream)
		}
	}
}

func (s *SessionizedService) applySessionCommand(request *SessionCommandRequest, stream streams.WriteStream) {
	session, ok := s.sessions[request.Context.SessionID]
	if !ok {
		util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), request.Context.SessionID).
			Warn("Unknown session")
		stream.Error(fmt.Errorf("unknown session %d", request.Context.SessionID))
		stream.Close()
	} else {
		s.session = session

		operation := s.Executor.GetOperation(request.Name)
		if unaryOp, ok := operation.(UnaryOperation); ok {
			output, err := unaryOp.Execute(request.Input)
			result := streams.Result{
				Value: output,
				Error: err,
			}
			stream.Send(result)
			session.addResult(request.Context.SequenceNumber, result)
		} else if streamOp, ok := operation.(StreamingOperation); ok {
			streamCtx := session.addStream(request.Context.SequenceNumber, request.Name, stream)
			streamOp.Execute(request.Input, streamCtx)
		} else {
			stream.Close()
		}
		session.completeCommand(request.Context.SequenceNumber)
	}
}

func (s *SessionizedService) applyOpenSession(request *OpenSessionRequest, stream streams.WriteStream) {
	session := newSession(s.Context, request.Timeout)
	s.sessions[session.ID] = session
	s.onOpen(session)
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
func (s *SessionizedService) applyKeepAlive(request *KeepAliveRequest, stream streams.WriteStream) {
	session, ok := s.sessions[request.SessionID]
	if !ok {
		util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), request.SessionID).
			Warn("Unknown session")
		stream.Error(fmt.Errorf("unknown session %d", request.SessionID))
	} else {
		util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), request.SessionID).
			Tracef("Recording keep-alive %v", request)

		// Update the session's last updated timestamp to prevent it from expiring
		session.LastUpdated = s.Context.Timestamp()

		// Clear the results up to the given command sequence number
		session.ack(request.CommandSequence, request.Streams)

		// Expire sessions that have not been kept alive
		s.expireSessions()

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
func (s *SessionizedService) expireSessions() {
	for id, session := range s.sessions {
		if session.timedOut(s.Context.Timestamp()) {
			session.close()
			delete(s.sessions, id)
			s.onExpire(session)
		}
	}
}

func (s *SessionizedService) applyCloseSession(request *CloseSessionRequest, stream streams.WriteStream) {
	session, ok := s.sessions[request.SessionID]
	if !ok {
		util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), request.SessionID).
			Warn("Unknown session")
		stream.Error(fmt.Errorf("unknown session %d", request.SessionID))
	} else {
		// Close the session and notify the service.
		delete(s.sessions, session.ID)
		session.close()
		s.onClose(session)

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
func (s *SessionizedService) Query(bytes []byte, stream streams.WriteStream) {
	request := &SessionRequest{}
	err := proto.Unmarshal(bytes, request)
	if err != nil {
		stream.Error(err)
		stream.Close()
	} else {
		query := request.GetQuery()
		if query.Context.LastIndex > s.Context.Index() {
			util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), query.Context.SessionID).
				Tracef("Query index %d greater than last index %d", query.Context.LastIndex, s.Context.Index())
			s.Scheduler.(*scheduler).ScheduleIndex(query.Context.LastIndex, func() {
				s.sequenceQuery(query, stream)
			})
		} else {
			util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), query.Context.SessionID).
				Tracef("Sequencing query %d <= %d", query.Context.LastIndex, s.Context.Index())
			s.sequenceQuery(query, stream)
		}
	}
}

func (s *SessionizedService) sequenceQuery(query *SessionQueryRequest, stream streams.WriteStream) {
	session, ok := s.sessions[query.Context.SessionID]
	if !ok {
		util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), query.Context.SessionID).
			Warn("Unknown session")
		stream.Error(fmt.Errorf("unknown session %d", query.Context.SessionID))
		stream.Close()
	} else {
		sequenceNumber := query.Context.LastSequenceNumber
		if sequenceNumber > session.commandSequence {
			util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), query.Context.SessionID).
				Tracef("Query ID %d greater than last ID %d", sequenceNumber, session.commandSequence)
			session.scheduleQuery(sequenceNumber, func() {
				util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), query.Context.SessionID).
					Tracef("Executing query %d", sequenceNumber)
				s.applyQuery(query, session, stream)
			})
		} else {
			util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), query.Context.SessionID).
				Tracef("Executing query %d", sequenceNumber)
			s.applyQuery(query, session, stream)
		}
	}
}

func (s *SessionizedService) applyQuery(query *SessionQueryRequest, session *Session, stream streams.WriteStream) {
	index := s.Context.Index()
	commandSequence := session.commandSequence
	stream = streams.NewEncodingStream(stream, func(value interface{}) (interface{}, error) {
		return proto.Marshal(&SessionResponse{
			Response: &SessionResponse_Query{
				Query: &SessionQueryResponse{
					Context: &SessionResponseContext{
						Index:    index,
						Sequence: commandSequence,
					},
					Output: value.([]byte),
				},
			},
		})
	})

	s.context.setQuery()

	responseStream := streams.NewEncodingStream(stream, func(value interface{}) (interface{}, error) {
		return proto.Marshal(&QueryResponse{
			Context: &ResponseContext{
				Index: s.Context.Index(),
			},
			Output: value.([]byte),
		})
	})

	operation := s.Executor.GetOperation(query.Name)
	if operation == nil {
		responseStream.Error(fmt.Errorf("unknown operation: %s", query.Name))
		responseStream.Close()
		return
	}

	if unaryOp, ok := operation.(UnaryOperation); ok {
		stream.Result(unaryOp.Execute(query.Input))
		stream.Close()
	} else if streamOp, ok := operation.(StreamingOperation); ok {
		stream.Result(proto.Marshal(&CommandResponse{
			Context: &ResponseContext{
				Index: s.Context.Index(),
				Type:  ResponseType_OPEN_STREAM,
			},
		}))

		responseStream = streams.NewCloserStream(responseStream, func(_ streams.WriteStream) {
			stream.Result(proto.Marshal(&CommandResponse{
				Context: &ResponseContext{
					Index: s.Context.Index(),
					Type:  ResponseType_CLOSE_STREAM,
				},
			}))
		})

		streamOp.Execute(query.Input, responseStream)
	} else {
		stream.Close()
	}
}

// OnOpen is called when a session is opened
func (s *SessionizedService) OnOpen(f func(*Session)) {
	s.onOpen = f
}

// OnExpire is called when a session is expired by the server
func (s *SessionizedService) OnExpire(f func(*Session)) {
	s.onExpire = f
}

// OnClose is called when a session is closed by the client
func (s *SessionizedService) OnClose(f func(*Session)) {
	s.onClose = f
}

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
		streams:          make(map[uint64]*sessionStream),
	}
	util.SessionEntry(ctx.Node(), ctx.Namespace(), ctx.Name(), session.ID).
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
func (s *Session) addResult(id uint64, result streams.Result) {
	s.results[id] = result
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
	util.StreamEntry(s.ctx.Node(), s.ctx.Namespace(), s.ctx.Name(), s.ID, id).
		Trace("Stream open")
	return stream
}

// getStream returns a stream by the request sequence number
func (s *Session) getStream(id uint64) *sessionStream {
	return s.streams[id]
}

// ack acknowledges response streams up to the given request sequence number
func (s *Session) ack(id uint64, streams map[uint64]uint64) {
	for responseId := range s.results {
		if responseId > id {
			continue
		}
		delete(s.results, responseId)
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
	util.SessionEntry(s.ctx.Node(), s.ctx.Namespace(), s.ctx.Name(), s.ID).
		Debug("Session closed")
	for _, stream := range s.streams {
		stream.Close()
	}
}

// sessionStream manages a single stream for a session
type sessionStream struct {
	ID         uint64
	Type       string
	session    *Session
	responseID uint64
	completeID uint64
	lastIndex  uint64
	ctx        Context
	stream     streams.WriteStream
	results    *list.List
}

// sessionStreamResult contains a single stream result
type sessionStreamResult struct {
	id     uint64
	index  uint64
	result streams.Result
}

// open opens the stream
func (s *sessionStream) open() {
	s.updateClock()

	bytes, err := proto.Marshal(&SessionResponse{
		Response: &SessionResponse_Command{
			Command: &SessionCommandResponse{
				Context: &SessionResponseContext{
					StreamID: s.ID,
					Index:    s.lastIndex,
					Sequence: s.responseID,
					Type:     ResponseType_OPEN_STREAM,
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

	util.StreamEntry(s.ctx.Node(), s.ctx.Namespace(), s.ctx.Name(), s.session.ID, s.ID).
		Tracef("Sending stream open %d %v", s.responseID, out.result)
	s.stream.Send(out.result)
}

func (s *sessionStream) updateClock() {
	// If the client acked a sequence number greater than the current event sequence number since we know the
	// client must have received it from another server.
	s.responseID++
	if s.completeID > s.responseID {
		util.StreamEntry(s.ctx.Node(), s.ctx.Namespace(), s.ctx.Name(), s.session.ID, s.ID).
			Debugf("Skipped completed result %d", s.responseID)
		return
	}

	// Record the last index sent on the stream
	s.lastIndex = s.ctx.Index()
}

func (s *sessionStream) Send(result streams.Result) {
	s.updateClock()

	// Create the stream result and add it to the results list.
	if result.Succeeded() {
		bytes, err := proto.Marshal(&SessionResponse{
			Response: &SessionResponse_Command{
				Command: &SessionCommandResponse{
					Context: &SessionResponseContext{
						StreamID: s.ID,
						Index:    s.lastIndex,
						Sequence: s.responseID,
					},
					Output: result.Value.([]byte),
				},
			},
		})
		result = streams.Result{
			Value: bytes,
			Error: err,
		}
	}

	out := sessionStreamResult{
		id:     s.responseID,
		index:  s.ctx.Index(),
		result: result,
	}
	s.results.PushBack(out)
	util.StreamEntry(s.ctx.Node(), s.ctx.Namespace(), s.ctx.Name(), s.session.ID, s.ID).
		Tracef("Cached response %d", s.responseID)

	// If the out channel is set, send the result
	util.StreamEntry(s.ctx.Node(), s.ctx.Namespace(), s.ctx.Name(), s.session.ID, s.ID).
		Tracef("Sending response %d %v", s.responseID, out.result)
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

func (s *sessionStream) Close() {
	util.StreamEntry(s.ctx.Node(), s.ctx.Namespace(), s.ctx.Name(), s.session.ID, s.ID).
		Trace("Stream closed")
	s.updateClock()

	bytes, err := proto.Marshal(&SessionResponse{
		Response: &SessionResponse_Command{
			Command: &SessionCommandResponse{
				Context: &SessionResponseContext{
					StreamID: s.ID,
					Index:    s.lastIndex,
					Sequence: s.responseID,
					Type:     ResponseType_CLOSE_STREAM,
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

	util.StreamEntry(s.ctx.Node(), s.ctx.Namespace(), s.ctx.Name(), s.session.ID, s.ID).
		Tracef("Sending stream close %d %v", s.responseID, out.result)
	s.stream.Send(out.result)
	s.stream.Close()
}

// completeIndex returns the highest acknowledged index in the stream
func (s *sessionStream) completeIndex() uint64 {
	event := s.results.Front()
	if event != nil {
		return event.Value.(sessionStreamResult).index - 1
	}
	return s.ctx.Index()
}

// LastIndex returns the last index in the stream
func (s *sessionStream) LastIndex() uint64 {
	if s.results.Len() > 0 {
		return s.lastIndex
	}
	return s.ctx.Index()
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
		util.StreamEntry(s.ctx.Node(), s.ctx.Namespace(), s.ctx.Name(), s.session.ID, s.ID).
			Tracef("Discarded cached responses up to %d", id)
	}
}

// replay resends results on the given channel
func (s *sessionStream) replay(stream streams.WriteStream) {
	result := s.results.Front()
	for result != nil {
		response := result.Value.(sessionStreamResult)
		util.StreamEntry(s.ctx.Node(), s.ctx.Namespace(), s.ctx.Name(), s.session.ID, s.ID).
			Tracef("Sending response %d %v", response.id, response.result)
		stream.Send(response.result)
		result = result.Next()
	}
	s.stream = stream
}
