package service

import (
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"
	"math"
	"time"
)

// SessionizedService is a Service implementation for primitives that support sessions
type SessionizedService struct {
	Service
	scheduler Scheduler
	executor  Executor
	ctx       Context
	sessions  map[uint64]*Session
	Session   *Session
}

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
		streams := make([]*SessionStreamSnapshot, 0, len(session.idStreams))
		for _, stream := range session.idStreams {
			streams = append(streams, &SessionStreamSnapshot{
				StreamId:       stream.Id,
				Type:           stream.Type,
				SequenceNumber: stream.currentSequence,
				LastCompleted:  stream.lastCompleted,
			})
		}
		sessions = append(sessions, &SessionSnapshot{
			SessionId:       session.Id,
			Timeout:         uint64(session.Timeout),
			Timestamp:       uint64(session.LastUpdated.UnixNano()),
			CommandSequence: session.commandSequence,
			Streams:         streams,
		})
	}

	snapshot := &SessionizedServiceSnapshot{
		Sessions: sessions,
	}
	bytes, err := proto.Marshal(snapshot)
	if err != nil {
		return err
	}

	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(bytes)))
	writer.Write(length)
	writer.Write(bytes)
	return nil
}

func (s *SessionizedService) snapshotService(writer io.Writer) error {
	bytes, err := s.Backup()
	if err != nil {
		return err
	} else {
		length := make([]byte, 4)
		binary.BigEndian.PutUint32(length, uint32(len(bytes)))
		writer.Write(length)
		writer.Write(bytes)
	}
	return nil
}

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
	n, err = reader.Read(bytes)
	if err != nil {
		return err
	}

	snapshot := &SessionizedServiceSnapshot{}
	err = proto.Unmarshal(bytes, snapshot)
	if err != nil {
		return err
	}

	s.sessions = make(map[uint64]*Session)
	for _, session := range snapshot.Sessions {
		idStreams := make(map[uint64]*sessionStream)
		typeStreams := make(map[string]map[uint64]*sessionStream)
		for _, stream := range session.Streams {
			s := &sessionStream{
				Id:              stream.StreamId,
				Type:            stream.Type,
				currentSequence: stream.SequenceNumber,
				lastCompleted:   stream.LastCompleted,
			}
			idStreams[s.Id] = s
			typeIdStreams, ok := typeStreams[s.Type]
			if !ok {
				typeIdStreams = make(map[uint64]*sessionStream)
				typeStreams[s.Type] = typeIdStreams
			}
			typeIdStreams[s.Id] = s
		}
		s.sessions[session.SessionId] = &Session{
			Id:              session.SessionId,
			Timeout:         time.Duration(session.Timeout),
			LastUpdated:     time.Unix(0, int64(session.Timestamp)),
			commandSequence: session.CommandSequence,
			idStreams:       idStreams,
			typeStreams:     typeStreams,
		}
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
	n, err = reader.Read(bytes)
	if err != nil {
		return err
	}
	return s.Restore(bytes)
}

func (s *SessionizedService) CanDelete(index uint64) bool {
	lastCompleted := index
	for _, session := range s.sessions {
		for _, stream := range session.idStreams {
			lastCompleted = uint64(math.Min(float64(lastCompleted), float64(stream.completeIndex())))
		}
	}
	return lastCompleted >= index
}

func (s *SessionizedService) Command(bytes []byte, callback func([]byte, error)) {
	request := &SessionRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		callback(nil, err)
	} else {
		scheduler := s.scheduler.(*scheduler)
		scheduler.runScheduledTasks(s.ctx.Timestamp())

		switch r := request.Request.(type) {
		case *SessionRequest_Command:
			s.applyCommand(r.Command, callback)
		case *SessionRequest_OpenSession:
			s.applyOpenSession(r.OpenSession, callback)
		case *SessionRequest_KeepAlive:
			s.applyKeepAlive(r.KeepAlive, callback)
		case *SessionRequest_CloseSession:
			s.applyCloseSession(r.CloseSession, callback)
		}

		scheduler.runImmediateTasks()
	}
}

func (s *SessionizedService) applyCommand(request *SessionCommandRequest, callback func([]byte, error)) {
	session, ok := s.sessions[request.Context.SessionId]
	if !ok {
		callback(nil, errors.New(fmt.Sprintf("unknown session %d", request.Context.SessionId)))
	} else {
		sequenceNumber := request.Context.SequenceNumber
		if sequenceNumber != 0 && sequenceNumber < session.commandSequence {
			future := session.getResult(sequenceNumber)
			if future != nil {
				future.addCallback(callback)
			} else {
				callback(nil, errors.New(fmt.Sprintf("sequence number %d has already been acknowledged", sequenceNumber)))
			}
		} else if sequenceNumber > session.nextCommandSequence() {
			session.scheduleSequenceCommand(sequenceNumber, func() {
				s.applySessionCommand(request, callback)
			})
		} else {
			s.applySessionCommand(request, callback)
		}
	}
}

func (s *SessionizedService) applySessionCommand(request *SessionCommandRequest, callback func([]byte, error)) {
	session, ok := s.sessions[request.Context.SessionId]
	if !ok {
		callback(nil, errors.New(fmt.Sprintf("unknown session %d", request.Context.SessionId)))
	} else {
		s.Session = session

		// Create stream contexts prior to executing the command to ensure we're
		// sending the stream state prior to this command.
		streams := make([]*SessionStreamContext, 0, len(session.idStreams))
		for _, stream := range session.idStreams {
			streams = append(streams, &SessionStreamContext{
				StreamId: stream.Id,
				Index:    stream.LastIndex(),
				Sequence: stream.currentSequence,
			})
		}

		future := session.registerResultCallback(request.Context.SequenceNumber, callback)
		s.executor.Execute(request.Name, request.Input, func(result []byte, err error) {
			if err != nil {
				future.complete(nil, err)
			} else {
				future.complete(proto.Marshal(&SessionResponse{
					Response: &SessionResponse_Command{
						Command: &SessionCommandResponse{
							Context: &SessionResponseContext{
								Index:    s.ctx.Index(),
								Sequence: request.Context.SequenceNumber,
								Streams:  streams,
							},
							Output: result,
						},
					},
				}))
			}
		})
		session.setCommandSequence(request.Context.SequenceNumber)
	}
}

func (s *SessionizedService) applyOpenSession(request *OpenSessionRequest, callback func([]byte, error)) {
	session := newSession(s.ctx, time.Duration(request.Timeout))
	s.sessions[session.Id] = session
	s.OnOpen(session)
	callback(proto.Marshal(&SessionResponse{
		Response: &SessionResponse_OpenSession{
			OpenSession: &OpenSessionResponse{
				SessionId: session.Id,
			},
		},
	}))
}

// applyKeepAlive applies a KeepAliveRequest to the service
func (s *SessionizedService) applyKeepAlive(request *KeepAliveRequest, callback func([]byte, error)) {
	session, ok := s.sessions[request.SessionId]
	if !ok {
		callback(nil, errors.New(fmt.Sprintf("unknown session %d", request.SessionId)))
	} else {
		// Update the session's last updated timestamp to prevent it from expiring
		session.LastUpdated = s.ctx.Timestamp()

		// Clear the result futures that have been acknowledged by the client
		session.clearResults(request.CommandSequence)

		// Resend missing events starting from the last received event index
		for id, sequence := range request.Streams {
			stream, ok := session.idStreams[id]
			if ok {
				stream.resendEvents(sequence)
			}
		}

		// Expire sessions that have not been kept alive
		s.expireSessions()

		// Send the response
		callback(proto.Marshal(&SessionResponse{
			Response: &SessionResponse_KeepAlive{
				KeepAlive: &KeepAliveResponse{},
			},
		}))
	}
}

// expireSessions expires sessions that have not been kept alive within their timeout
func (s *SessionizedService) expireSessions() {
	for id, session := range s.sessions {
		if session.timedOut(s.ctx.Timestamp()) {
			session.close()
			delete(s.sessions, id)
			s.OnExpire(session)
		}
	}
}

func (s *SessionizedService) applyCloseSession(request *CloseSessionRequest, callback func([]byte, error)) {
	session, ok := s.sessions[request.SessionId]
	if !ok {
		callback(nil, errors.New(fmt.Sprintf("unknown session %d", request.SessionId)))
	} else {
		session.close()
		s.OnClose(session)
		delete(s.sessions, session.Id)
	}
}

func (s *SessionizedService) CommandStream(bytes []byte, stream Stream, callback func(error)) {
	request := &SessionRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		callback(err)
	} else {
		scheduler := s.scheduler.(*scheduler)
		scheduler.runScheduledTasks(s.ctx.Timestamp())
		s.applySequenceCommandStream(request.GetCommand(), stream, callback)
		scheduler.runImmediateTasks()
	}
}

func (s *SessionizedService) applySequenceCommandStream(request *SessionCommandRequest, stream Stream, callback func(error)) {
	session, ok := s.sessions[request.Context.SessionId]
	if !ok {
		stream.Fail(errors.New(fmt.Sprintf("unknown session %d", request.Context.SessionId)))
		callback(errors.New(fmt.Sprintf("unknown session %d", request.Context.SessionId)))
	} else {
		sequenceNumber := request.Context.SequenceNumber
		if sequenceNumber != 0 && sequenceNumber <= session.commandSequence {
			future := session.getResult(sequenceNumber)
			stream := session.getStream(sequenceNumber)
			if future != nil && stream != nil {
				stream.stream = stream
				future.addCallback(func(bytes []byte, err error) {
					callback(err)
				})
				return
			}
		}

		if sequenceNumber > session.nextCommandSequence() {
			session.scheduleSequenceCommand(sequenceNumber, func() {
				s.applySessionCommandStream(request, session, stream, callback)
			})
		} else {
			s.applySessionCommandStream(request, session, stream, callback)
		}
	}
}

func (s *SessionizedService) applySessionCommandStream(request *SessionCommandRequest, session *Session, stream Stream, callback func(error)) {
	// Set the current session for usage in the service
	s.Session = session

	sequenceNumber := request.Context.SequenceNumber

	// Create stream contexts prior to executing the command to ensure we're
	// sending the stream state prior to this command.
	// Prepend the stream to the list of streams so it's easily identifiable by the client.
	streams := make([]*SessionStreamContext, 0, len(session.idStreams)+1)
	streams[0] = &SessionStreamContext{
		StreamId: s.ctx.Index(),
		Index:    s.ctx.Index(),
	}
	for _, stream := range session.idStreams {
		streams = append(streams, &SessionStreamContext{
			StreamId: stream.Id,
			Index:    stream.LastIndex(),
			Sequence: stream.currentSequence,
		})
	}

	// Add the stream to the session and bind it to the provided stream
	sessionStream := session.addStream(s.ctx.Index(), request.Name)
	sessionStream.stream = stream

	// Create a result future and execute the streaming operation
	future := session.registerResultCallback(sequenceNumber, func(bytes []byte, err error) {
		callback(err)
	})
	s.executor.ExecuteStream(request.Name, request.Input, sessionStream, func(err error) {
		future.complete(nil, err)
	})
	callback(nil)
}

func (s *SessionizedService) Query(bytes []byte, callback func([]byte, error)) {
	request := &SessionRequest{}
	err := proto.Unmarshal(bytes, request)
	if err != nil {
		callback(nil, err)
	} else {
		query := request.GetQuery()
		if query.Context.LastIndex > s.ctx.Index() {
			s.scheduler.(*scheduler).ScheduleIndex(query.Context.LastIndex, func() {
				s.sequenceQuery(query, callback)
			})
		} else {
			s.sequenceQuery(query, callback)
		}
	}
}

func (s *SessionizedService) sequenceQuery(query *SessionQueryRequest, callback func([]byte, error)) {
	session, ok := s.sessions[query.Context.SessionId]
	if !ok {
		callback(nil, errors.New(fmt.Sprintf("unknown session %d", query.Context.SessionId)))
	} else {
		sequenceNumber := query.Context.LastSequenceNumber
		if sequenceNumber > session.commandSequence {
			session.scheduleSequenceQuery(sequenceNumber, func() {
				s.applyQuery(query, session, callback)
			})
		} else {
			s.applyQuery(query, session, callback)
		}
	}
}

func (s *SessionizedService) applyQuery(query *SessionQueryRequest, session *Session, callback func([]byte, error)) {
	s.executor.Execute(query.Name, query.Input, func(result []byte, err error) {
		if err != nil {
			callback(nil, err)
		} else {
			streams := make([]*SessionStreamContext, 0, len(session.idStreams))
			for _, stream := range session.idStreams {
				streams = append(streams, &SessionStreamContext{
					StreamId: stream.Id,
					Index:    stream.LastIndex(),
					Sequence: stream.currentSequence,
				})
			}
			callback(proto.Marshal(&SessionResponse{
				Response: &SessionResponse_Command{
					Command: &SessionCommandResponse{
						Context: &SessionResponseContext{
							Index:    s.ctx.Index(),
							Sequence: session.commandSequence,
							Streams:  streams,
						},
						Output: result,
					},
				},
			}))
		}
	})
}

func (s *SessionizedService) QueryStream(bytes []byte, stream Stream, callback func(error)) {
	request := &SessionRequest{}
	err := proto.Unmarshal(bytes, request)
	if err != nil {
		callback(err)
	} else {
		query := request.GetQuery()
		if query.Context.LastIndex > s.ctx.Index() {
			s.scheduler.(*scheduler).ScheduleIndex(query.Context.LastIndex, func() {
				s.sequenceQueryStream(query, stream, callback)
			})
		} else {
			s.sequenceQueryStream(query, stream, callback)
		}
	}
}

func (s *SessionizedService) sequenceQueryStream(query *SessionQueryRequest, stream Stream, callback func(error)) {
	session, ok := s.sessions[query.Context.SessionId]
	if !ok {
		callback(errors.New(fmt.Sprintf("unknown session %d", query.Context.SessionId)))
	} else {
		sequenceNumber := query.Context.LastSequenceNumber
		if sequenceNumber > session.commandSequence {
			session.scheduleSequenceQuery(sequenceNumber, func() {
				s.applyQueryStream(query, session, stream, callback)
			})
		} else {
			s.applyQueryStream(query, session, stream, callback)
		}
	}
}

func (s *SessionizedService) applyQueryStream(query *SessionQueryRequest, session *Session, stream Stream, callback func(error)) {
	s.executor.Execute(query.Name, query.Input, func(result []byte, err error) {
		callback(err)
	})
}

func (s *SessionizedService) OnOpen(session *Session) {

}

func (s *SessionizedService) OnExpire(session *Session) {

}

func (s *SessionizedService) OnClose(session *Session) {

}

func newSession(ctx Context, timeout time.Duration) *Session {
	return &Session{
		Id:               ctx.Index(),
		Timeout:          timeout,
		LastUpdated:      ctx.Timestamp(),
		ctx:              ctx,
		sequenceCommands: make(map[uint64]func()),
		sequenceQueries:  make(map[uint64]*list.List),
		results:          make(map[uint64]*resultFuture),
		idStreams:        make(map[uint64]*sessionStream),
		typeStreams:      make(map[string]map[uint64]*sessionStream),
	}
}

// resultFuture completes operation callbacks and stores operation results
type resultFuture struct {
	done      bool
	result    []byte
	err       error
	callbacks *list.List
}

// complete completes an operation with a result or error
func (f *resultFuture) complete(result []byte, err error) {
	f.done = true
	f.result = result
	f.err = err
	callback := f.callbacks.Front()
	for callback != nil {
		callback.Value.(func([]byte, error))(result, err)
		callback = callback.Next()
	}
}

// addCallback adds an operation result callback to the future
func (f *resultFuture) addCallback(callback func([]byte, error)) {
	if f.done {
		callback(f.result, f.err)
	} else {
		f.callbacks.PushBack(callback)
	}
}

type Session struct {
	Id               uint64
	Timeout          time.Duration
	LastUpdated      time.Time
	ctx              Context
	commandSequence  uint64
	commandAck       uint64
	sequenceCommands map[uint64]func()
	sequenceQueries  map[uint64]*list.List
	results          map[uint64]*resultFuture
	idStreams        map[uint64]*sessionStream
	typeStreams      map[string]map[uint64]*sessionStream
}

func (s *Session) timedOut(time time.Time) bool {
	return s.LastUpdated.UnixNano() > 0 && time.Sub(s.LastUpdated) > s.Timeout
}

func (s *Session) addStream(index uint64, op string) *sessionStream {
	stream := &sessionStream{
		Id:     index,
		Type:   op,
		events: list.New(),
		ctx:    s.ctx,
	}
	s.idStreams[stream.Id] = stream
	typeStreams, ok := s.typeStreams[stream.Type]
	if !ok {
		typeStreams = make(map[uint64]*sessionStream)
		s.typeStreams[stream.Type] = typeStreams
	}
	typeStreams[stream.Id] = stream
	return stream
}

func (s *Session) getStream(sequenceNumber uint64) *sessionStream {
	return s.idStreams[sequenceNumber]
}

func (s *Session) registerResultCallback(sequenceNumber uint64, callback func([]byte, error)) *resultFuture {
	callbacks := list.New()
	callbacks.PushBack(callback)
	future := &resultFuture{
		callbacks: callbacks,
	}
	s.results[sequenceNumber] = future
	return future
}

func (s *Session) getResult(sequenceNumber uint64) *resultFuture {
	return s.results[sequenceNumber]
}

func (s *Session) clearResults(sequenceNumber uint64) {
	if sequenceNumber > s.commandAck {
		for i := s.commandAck + 1; i <= sequenceNumber; i++ {
			delete(s.results, i)
			s.commandAck = i
		}
	}
}

func (s *Session) scheduleSequenceQuery(sequenceNumber uint64, f func()) {
	queries, ok := s.sequenceQueries[sequenceNumber]
	if !ok {
		queries := list.New()
		s.sequenceQueries[sequenceNumber] = queries
	}
	queries.PushBack(f)
}

func (s *Session) scheduleSequenceCommand(sequenceNumber uint64, f func()) {
	s.sequenceCommands[sequenceNumber] = f
}

func (s *Session) nextCommandSequence() uint64 {
	return s.commandSequence + 1
}

func (s *Session) setCommandSequence(sequenceNumber uint64) {
	for i := s.commandSequence + 1; i <= sequenceNumber; i++ {
		s.commandSequence = i
		queries, ok := s.sequenceQueries[i]
		if ok {
			query := queries.Front()
			for query != nil {
				query.Value.(func())()
				query = query.Next()
			}
			delete(s.sequenceQueries, i)
		}

		command, ok := s.sequenceCommands[i]
		if ok {
			command()
			delete(s.sequenceCommands, i)
		}
	}
}

// close closes the session and completes all its streams
func (s *Session) close() {
	for _, stream := range s.idStreams {
		stream.Complete()
	}
}

type sessionStream struct {
	Stream
	Id               uint64
	Type             string
	closed           bool
	currentSequence  uint64
	completeSequence uint64
	lastCompleted    uint64
	lastIndex        uint64
	events           *list.List
	ctx              Context
	stream           Stream
}

type sessionStreamEvent struct {
	sequenceNumber uint64
	index          uint64
	value          []byte
}

// completeIndex returns the highest acknowledged index in the stream
func (s *sessionStream) completeIndex() uint64 {
	event := s.events.Front()
	if event != nil {
		return event.Value.(*sessionStreamEvent).index - 1
	}
	return s.ctx.Index()
}

func (s *sessionStream) Next(value []byte) {
	if s.closed {
		return
	}

	// If the event is being published during a read operation, throw an exception.
	if s.ctx.OperationType() != OpTypeCommand {
		return
	}

	// If the client acked a sequence number greater than the current event sequence number since we know the
	// client must have received it from another server.
	s.currentSequence++
	if s.completeSequence > s.currentSequence {
		return
	}

	lastIndex := s.ctx.Index()
	event := &sessionStreamEvent{
		sequenceNumber: s.currentSequence,
		index:          lastIndex,
		value:          value,
	}
	s.events.PushBack(event)
	s.sendEvent(event)
}

func (s *sessionStream) Complete() {
	if s.closed {
		return
	}

	s.closed = true
	s.stream.Complete()
}

func (s *sessionStream) Fail(err error) {
	if s.closed {
		return
	}

	s.closed = true
	s.stream.Fail(err)
}

func (s *sessionStream) LastIndex() uint64 {
	if s.events.Len() > 0 {
		return s.lastIndex
	}
	return s.ctx.Index()
}

// clearEvents clears enqueued events up to the given sequence number
func (s *sessionStream) clearEvents(sequenceNumber uint64) {
	if sequenceNumber > s.completeSequence {
		event := s.events.Front()
		for event != nil && event.Value.(*sessionStreamEvent).sequenceNumber <= sequenceNumber {
			next := event.Next()
			s.events.Remove(event)
			s.completeSequence = event.Value.(*sessionStreamEvent).sequenceNumber
			event = next
		}
	}
}

// resendEvents resends events on the stream starting at the given sequence number
func (s *sessionStream) resendEvents(sequenceNumber uint64) {
	event := s.events.Front()
	for event != nil {
		s.sendEvent(event.Value.(*sessionStreamEvent))
		event = event.Next()
	}
}

// sendEvent sends an event on the stream
func (s *sessionStream) sendEvent(event *sessionStreamEvent) {
	if s.stream != nil {
		response := &SessionResponse{
			Response: &SessionResponse_Stream{
				Stream: &SessionStreamResponse{
					Context: &SessionStreamContext{
						StreamId: s.Id,
						Index:    event.index,
						Sequence: event.sequenceNumber,
					},
					Value: event.value,
				},
			},
		}
		bytes, err := proto.Marshal(response)
		if err != nil {
			s.stream.Fail(err)
		} else {
			s.stream.Next(bytes)
		}
	}
}
