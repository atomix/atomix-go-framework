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
	"errors"
	"fmt"
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
	if err := s.snapshotSessions(writer); err != nil {
		return err
	}
	if err := s.snapshotService(writer); err != nil {
		return err
	}
	return nil
}

func (s *SessionizedService) snapshotSessions(writer io.Writer) error {
	sessions := make([]*SessionSnapshot, 0, len(s.sessions))
	for _, session := range s.sessions {
		streams := make([]*SessionStreamSnapshot, 0, session.streams.Len())
		element := session.streams.Front()
		for element != nil {
			stream := element.Value.(*sessionStream)
			streams = append(streams, &SessionStreamSnapshot{
				StreamId:       stream.ID,
				Type:           stream.Type,
				SequenceNumber: stream.responseID,
				LastCompleted:  stream.completeID,
			})
			element = element.Next()
		}
		sessions = append(sessions, &SessionSnapshot{
			SessionID:       session.ID,
			Timeout:         session.Timeout,
			Timestamp:       session.LastUpdated,
			CommandSequence: session.commandSequence,
			Streams:         streams,
		})
	}

	snapshot := &SessionizedServiceSnapshot{
		Index:     s.context.index,
		Timestamp: uint64(s.context.time.UnixNano()),
		Sessions:  sessions,
	}
	bytes, err := proto.Marshal(snapshot)
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
	return err
}

func (s *SessionizedService) snapshotService(writer io.Writer) error {
	bytes, err := s.Backup()
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
	return nil
}

// Install installs a snapshot of the service
func (s *SessionizedService) Install(reader io.Reader) error {
	if err := s.installSessions(reader); err != nil {
		return err
	}
	if err := s.installService(reader); err != nil {
		return err
	}
	return nil
}

func (s *SessionizedService) installSessions(reader io.Reader) error {
	lengthBytes := make([]byte, 4)
	n, err := reader.Read(lengthBytes)
	if err != nil {
		return err
	}

	if n != 4 {
		return errors.New("malformed snapshot")
	}

	length := binary.BigEndian.Uint32(lengthBytes)
	bytes := make([]byte, length)
	_, err = reader.Read(bytes)
	if err != nil {
		return err
	}

	snapshot := &SessionizedServiceSnapshot{}
	s.context.index = snapshot.Index
	s.context.time = time.Unix(0, int64(snapshot.Timestamp))

	err = proto.Unmarshal(bytes, snapshot)
	if err != nil {
		return err
	}

	s.sessions = make(map[uint64]*Session)
	for _, state := range snapshot.Sessions {
		session := &Session{
			ID:              state.SessionID,
			Timeout:         time.Duration(state.Timeout),
			LastUpdated:     state.Timestamp,
			commandSequence: state.CommandSequence,
		}

		streams := list.New()
		for _, stream := range state.Streams {
			s := &sessionStream{
				ID:         stream.StreamId,
				Type:       stream.Type,
				session:    session,
				responseID: stream.SequenceNumber,
				completeID: stream.LastCompleted,
				ctx:        s.Context,
				inChan:     make(chan Result),
				results:    list.New(),
			}
			streams.PushBack(s)
		}
		session.streams = streams
		s.sessions[session.ID] = session
	}
	return nil
}

func (s *SessionizedService) installService(reader io.Reader) error {
	lengthBytes := make([]byte, 4)
	n, err := reader.Read(lengthBytes)
	if err != nil {
		return err
	}

	if n != 4 {
		return errors.New("malformed snapshot")
	}

	length := binary.BigEndian.Uint32(lengthBytes)
	bytes := make([]byte, length)
	_, err = reader.Read(bytes)
	if err != nil {
		return err
	}
	return s.Restore(bytes)
}

// CanDelete returns a boolean indicating whether entries up to the given index can be deleted
func (s *SessionizedService) CanDelete(index uint64) bool {
	lastCompleted := index
	for _, session := range s.sessions {
		element := session.streams.Front()
		for element != nil {
			stream := element.Value.(*sessionStream)
			lastCompleted = uint64(math.Min(float64(lastCompleted), float64(stream.completeIndex())))
			element = element.Next()
		}
	}
	return lastCompleted >= index
}

// Command handles a service command
func (s *SessionizedService) Command(bytes []byte, ch chan<- Result) {
	s.context.setCommand(s.parent.Timestamp())
	request := &SessionRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		if ch != nil {
			fail(ch, s.context.Index(), err)
		}
	} else {
		scheduler := s.Scheduler.(*scheduler)
		scheduler.runScheduledTasks(s.Context.Timestamp())

		switch r := request.Request.(type) {
		case *SessionRequest_Command:
			s.applyCommand(r.Command, ch)
		case *SessionRequest_OpenSession:
			s.applyOpenSession(r.OpenSession, ch)
		case *SessionRequest_KeepAlive:
			s.applyKeepAlive(r.KeepAlive, ch)
		case *SessionRequest_CloseSession:
			s.applyCloseSession(r.CloseSession, ch)
		}

		scheduler.runImmediateTasks()
		scheduler.runIndex(s.Context.Index())
	}
}

func (s *SessionizedService) applyCommand(request *SessionCommandRequest, ch chan<- Result) {
	session, ok := s.sessions[request.Context.SessionID]
	if !ok {
		if ch != nil {
			util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), request.Context.SessionID).
				Warn("Unknown session")
			fail(ch, s.context.Index(), fmt.Errorf("unknown session %d", request.Context.SessionID))
		}
	} else {
		sequenceNumber := request.Context.SequenceNumber
		if sequenceNumber != 0 && sequenceNumber <= session.commandSequence {
			stream := session.getStream(sequenceNumber)
			if stream != nil {
				if ch != nil {
					stream.replay(ch)
				}
			} else {
				if ch != nil {
					fail(ch, s.context.Index(), fmt.Errorf("sequence number %d has already been acknowledged", sequenceNumber))
				}
			}
		} else if sequenceNumber > session.nextCommandSequence() {
			session.scheduleCommand(sequenceNumber, func() {
				util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), request.Context.SessionID).
					Tracef("Executing command %d", sequenceNumber)
				s.applySessionCommand(request, ch)
			})
		} else {
			util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), request.Context.SessionID).
				Tracef("Executing command %d", sequenceNumber)
			s.applySessionCommand(request, ch)
		}
	}
}

func (s *SessionizedService) applySessionCommand(request *SessionCommandRequest, ch chan<- Result) {
	session, ok := s.sessions[request.Context.SessionID]
	if !ok {
		if ch != nil {
			util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), request.Context.SessionID).
				Warn("Unknown session")
			fail(ch, s.context.Index(), fmt.Errorf("unknown session %d", request.Context.SessionID))
		}
	} else {
		s.session = session
		stream := session.addStream(request.Context.SequenceNumber, request.Name, ch)
		if err := s.Executor.Execute(request.Name, request.Input, stream); err != nil {
			if ch != nil {
				fail(ch, s.context.Index(), err)
			}
		}
		session.completeCommand(request.Context.SequenceNumber)
	}
}

func (s *SessionizedService) applyOpenSession(request *OpenSessionRequest, ch chan<- Result) {
	session := newSession(s.Context, request.Timeout)
	s.sessions[session.ID] = session
	s.onOpen(session)
	if ch != nil {
		bytes, err := proto.Marshal(&SessionResponse{
			Response: &SessionResponse_OpenSession{
				OpenSession: &OpenSessionResponse{
					SessionID: session.ID,
				},
			},
		})
		ch <- newResult(s.context.Index(), bytes, err)
		close(ch)
	}
}

// applyKeepAlive applies a KeepAliveRequest to the service
func (s *SessionizedService) applyKeepAlive(request *KeepAliveRequest, ch chan<- Result) {
	session, ok := s.sessions[request.SessionID]
	if !ok {
		if ch != nil {
			util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), request.SessionID).
				Warn("Unknown session")
			fail(ch, s.context.Index(), fmt.Errorf("unknown session %d", request.SessionID))
		}
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
		if ch != nil {
			bytes, err := proto.Marshal(&SessionResponse{
				Response: &SessionResponse_KeepAlive{
					KeepAlive: &KeepAliveResponse{},
				},
			})
			ch <- newResult(s.context.Index(), bytes, err)
			close(ch)
		}
	}
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

func (s *SessionizedService) applyCloseSession(request *CloseSessionRequest, ch chan<- Result) {
	session, ok := s.sessions[request.SessionID]
	if !ok {
		if ch != nil {
			util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), request.SessionID).
				Warn("Unknown session")
			fail(ch, s.context.Index(), fmt.Errorf("unknown session %d", request.SessionID))
		}
	} else {
		// Close the session and notify the service.
		delete(s.sessions, session.ID)
		session.close()
		s.onClose(session)

		// Send the response
		if ch != nil {
			bytes, err := proto.Marshal(&SessionResponse{
				Response: &SessionResponse_CloseSession{
					CloseSession: &CloseSessionResponse{},
				},
			})
			ch <- newResult(s.context.Index(), bytes, err)
			close(ch)
		}
	}
}

// Query handles a service query
func (s *SessionizedService) Query(bytes []byte, ch chan<- Result) {
	request := &SessionRequest{}
	err := proto.Unmarshal(bytes, request)
	if err != nil {
		if ch != nil {
			fail(ch, s.context.Index(), err)
		}
	} else {
		query := request.GetQuery()
		if query.Context.LastIndex > s.Context.Index() {
			util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), query.Context.SessionID).
				Tracef("Query index %d greater than last index %d", query.Context.LastIndex, s.Context.Index())
			s.Scheduler.(*scheduler).ScheduleIndex(query.Context.LastIndex, func() {
				s.sequenceQuery(query, ch)
			})
		} else {
			util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), query.Context.SessionID).
				Tracef("Sequencing query %d <= %d", query.Context.LastIndex, s.Context.Index())
			s.sequenceQuery(query, ch)
		}
	}
}

func (s *SessionizedService) sequenceQuery(query *SessionQueryRequest, ch chan<- Result) {
	session, ok := s.sessions[query.Context.SessionID]
	if !ok {
		if ch != nil {
			util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), query.Context.SessionID).
				Warn("Unknown session")
			fail(ch, s.context.Index(), fmt.Errorf("unknown session %d", query.Context.SessionID))
		}
	} else {
		sequenceNumber := query.Context.LastSequenceNumber
		if sequenceNumber > session.commandSequence {
			util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), query.Context.SessionID).
				Tracef("Query ID %d greater than last ID %d", sequenceNumber, session.commandSequence)
			session.scheduleQuery(sequenceNumber, func() {
				util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), query.Context.SessionID).
					Tracef("Executing query %d", sequenceNumber)
				s.applyQuery(query, session, ch)
			})
		} else {
			util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), query.Context.SessionID).
				Tracef("Executing query %d", sequenceNumber)
			s.applyQuery(query, session, ch)
		}
	}
}

func (s *SessionizedService) applyQuery(query *SessionQueryRequest, session *Session, ch chan<- Result) {
	// If the result channel is non-nil, create a channel for transforming results.
	var queryCh chan Result
	if ch != nil {
		index := s.Context.Index()
		commandSequence := session.commandSequence

		queryCh = make(chan Result)
		go func() {
			defer close(ch)
			for result := range queryCh {
				if result.Failed() {
					ch <- result
				} else {
					bytes, err := proto.Marshal(&SessionResponse{
						Response: &SessionResponse_Query{
							Query: &SessionQueryResponse{
								Context: &SessionResponseContext{
									Index:    index,
									Sequence: commandSequence,
								},
								Output: result.Value,
							},
						},
					})
					ch <- newResult(result.Index, bytes, err)
				}
			}
		}()
	}

	s.context.setQuery()
	if err := s.Executor.Execute(query.Name, query.Input, queryCh); err != nil {
		util.SessionEntry(s.Context.Node(), s.context.Namespace(), s.context.Name(), query.Context.SessionID).
			Warnf("An application error occurred: %s", err)
		fail(ch, s.context.Index(), err)
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
		streams:          list.New(),
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
	streams          *list.List
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

// Channels returns a slice of all open channels of any type owned by the session
func (s *Session) Channels() []chan<- Result {
	channels := make([]chan<- Result, 0, s.streams.Len())
	element := s.streams.Front()
	for element != nil {
		channels = append(channels, element.Value.(*sessionStream).inChan)
		element = element.Next()
	}
	return channels
}

// Channel returns the given channel
func (s *Session) Channel(id uint64) chan<- Result {
	element := s.streams.Front()
	for element != nil {
		stream := element.Value.(*sessionStream)
		if stream.ID == id {
			return stream.inChan
		}
		element = element.Next()
	}
	return nil
}

// ChannelsOf returns a slice of all open channels for the given named operation owned by the session
func (s *Session) ChannelsOf(op string) []chan<- Result {
	channels := make([]chan<- Result, 0, s.streams.Len())
	element := s.streams.Front()
	for element != nil {
		stream := element.Value.(*sessionStream)
		if stream.Type == op {
			channels = append(channels, stream.inChan)
		}
		element = element.Next()
	}
	return channels
}

// addStream adds a stream at the given sequence number
func (s *Session) addStream(id uint64, op string, outChan chan<- Result) chan<- Result {
	stream := &sessionStream{
		ID:      id,
		Type:    op,
		session: s,
		ctx:     s.ctx,
		inChan:  make(chan Result),
		outChan: outChan,
		results: list.New(),
	}
	s.streams.PushBack(stream)
	s.streamID = id
	util.StreamEntry(s.ctx.Node(), s.ctx.Namespace(), s.ctx.Name(), s.ID, stream.ID).
		Trace("Stream open")
	go stream.process()
	return stream.inChan
}

// getStream returns a stream by the request sequence number
func (s *Session) getStream(id uint64) *sessionStream {
	element := s.streams.Back()
	for element != nil {
		stream := element.Value.(*sessionStream)
		if stream.ID == id {
			return stream
		}
		element = element.Prev()
	}
	return nil
}

// ack acknowledges response streams up to the given request sequence number
func (s *Session) ack(id uint64, streams map[uint64]uint64) {
	element := s.streams.Front()
	for element != nil {
		stream := element.Value.(*sessionStream)

		// If the stream ID is greater than the acknowledged sequence number, break out of the loop.
		if stream.ID > id {
			break
		}

		// Store the next element so it persists if we remove the current element.
		next := element.Next()

		// If the stream is still held by the client, ack the stream.
		// Otherwise, close the stream.
		streamAck, open := streams[stream.ID]
		if open {
			stream.ack(streamAck)
		} else {
			s.streams.Remove(element)
		}
		element = next
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
	element := s.streams.Front()
	for element != nil {
		element.Value.(*sessionStream).close()
		element = element.Next()
	}
}

// sessionStream manages a single stream for a session
type sessionStream struct {
	ID         uint64
	Type       string
	session    *Session
	closed     bool
	responseID uint64
	completeID uint64
	lastIndex  uint64
	ctx        Context
	inChan     chan Result
	outChan    chan<- Result
	results    *list.List
}

// sessionStreamResult contains a single stream result
type sessionStreamResult struct {
	id     uint64
	result Result
}

// process processes the stream results on the in channel and passes them to the out channel
func (s *sessionStream) process() {
	for inResult := range s.inChan {
		if s.closed {
			return
		}
		s.processResult(inResult)
	}

	util.StreamEntry(s.ctx.Node(), s.ctx.Namespace(), s.ctx.Name(), s.session.ID, s.ID).
		Trace("Finished processing responses")
	s.closed = true

	ch := s.outChan
	if ch != nil {
		close(ch)
	}
}

// processResult processes a single stream result
func (s *sessionStream) processResult(inResult Result) {
	defer func() {
		if err := recover(); err != nil {
			util.StreamEntry(s.ctx.Node(), s.ctx.Namespace(), s.ctx.Name(), s.session.ID, s.ID).
				Errorf("Recovered from panic %v", err)
		}
	}()

	// If the client acked a sequence number greater than the current event sequence number since we know the
	// client must have received it from another server.
	s.responseID++
	if s.completeID > s.responseID {
		util.StreamEntry(s.ctx.Node(), s.ctx.Namespace(), s.ctx.Name(), s.session.ID, s.ID).
			Debugf("Skipped completed result %d", s.responseID)
		return
	}

	// Record the last index sent on the stream
	s.lastIndex = inResult.Index

	// Create the stream result and add it to the results list.
	if inResult.Succeeded() {
		bytes, err := proto.Marshal(&SessionResponse{
			Response: &SessionResponse_Command{
				Command: &SessionCommandResponse{
					Context: &SessionResponseContext{
						StreamID: s.ID,
						Index:    inResult.Index,
						Sequence: s.responseID,
					},
					Output: inResult.Value,
				},
			},
		})
		inResult = newResult(inResult.Index, bytes, err)
	}

	outResult := sessionStreamResult{
		id:     s.responseID,
		result: inResult,
	}
	s.results.PushBack(outResult)
	util.StreamEntry(s.ctx.Node(), s.ctx.Namespace(), s.ctx.Name(), s.session.ID, s.ID).
		Tracef("Cached response %d", s.responseID)

	// If the out channel is set, send the result
	if s.outChan != nil {
		out := outResult.result
		util.StreamEntry(s.ctx.Node(), s.ctx.Namespace(), s.ctx.Name(), s.session.ID, s.ID).
			Tracef("Sending response %d %v", s.responseID, out)
		s.outChan <- out
	}
}

// close closes the stream
func (s *sessionStream) close() {
	defer func() {
		_ = recover()
	}()
	close(s.inChan)
}

// completeIndex returns the highest acknowledged index in the stream
func (s *sessionStream) completeIndex() uint64 {
	event := s.results.Front()
	if event != nil {
		return event.Value.(sessionStreamResult).result.Index - 1
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
func (s *sessionStream) replay(ch chan<- Result) {
	result := s.results.Front()
	for result != nil {
		response := result.Value.(sessionStreamResult)
		util.StreamEntry(s.ctx.Node(), s.ctx.Namespace(), s.ctx.Name(), s.session.ID, s.ID).
			Tracef("Sending response %d %v", response.id, response.result)
		ch <- response.result
		result = result.Next()
	}
	s.outChan = ch
}
