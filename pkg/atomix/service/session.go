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

// NewSessionizedService returns an initialized SessionizedService
func NewSessionizedService(parent Context) *SessionizedService {
	ctx := &context{}
	return &SessionizedService{
		Scheduler: newScheduler(),
		Executor:  newExecutor(),
		Context:   ctx,
		context:   ctx,
		parent:    parent,
		sessions:  make(map[uint64]*Session),
	}
}

// SessionizedService is a Service implementation for primitives that support sessions
type SessionizedService struct {
	Service
	*service
	Scheduler Scheduler
	Executor  Executor
	Context   Context
	context   *context
	parent    Context
	sessions  map[uint64]*Session
	session   *Session
}

// Session returns the currently active session
func (s *SessionizedService) Session() *Session {
	return s.session
}

// Sessions returns a map of currently active sessions
func (s *SessionizedService) Sessions() map[uint64]*Session {
	return s.sessions
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
		streams := make([]*SessionStreamSnapshot, 0, len(session.streams))
		for _, stream := range session.streams {
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
		Index: s.context.index,
		Timestamp: uint64(s.context.time.UnixNano()),
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
	s.context.index = snapshot.Index
	s.context.time = time.Unix(0, int64(snapshot.Timestamp))

	err = proto.Unmarshal(bytes, snapshot)
	if err != nil {
		return err
	}

	s.sessions = make(map[uint64]*Session)
	for _, session := range snapshot.Sessions {
		streams := make(map[uint64]*sessionStream)
		for _, stream := range session.Streams {
			s := &sessionStream{
				Id:              stream.StreamId,
				Type:            stream.Type,
				currentSequence: stream.SequenceNumber,
				lastCompleted:   stream.LastCompleted,
			}
			streams[s.Id] = s
		}
		s.sessions[session.SessionId] = &Session{
			Id:              session.SessionId,
			Timeout:         time.Duration(session.Timeout),
			LastUpdated:     time.Unix(0, int64(session.Timestamp)),
			commandSequence: session.CommandSequence,
			streams:         streams,
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
		for _, stream := range session.streams {
			lastCompleted = uint64(math.Min(float64(lastCompleted), float64(stream.completeIndex())))
		}
	}
	return lastCompleted >= index
}

func (s *SessionizedService) Command(bytes []byte, ch chan<- *Result) {
	s.context.setCommand(s.parent.Timestamp())
	request := &SessionRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- s.NewFailure(err)
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
	}
}

func (s *SessionizedService) applyCommand(request *SessionCommandRequest, ch chan<- *Result) {
	session, ok := s.sessions[request.Context.SessionId]
	if !ok {
		ch <- s.NewFailure(errors.New(fmt.Sprintf("unknown session %d", request.Context.SessionId)))
	} else {
		sequenceNumber := request.Context.SequenceNumber
		if sequenceNumber != 0 && sequenceNumber < session.commandSequence {
			stream := session.getStream(sequenceNumber)
			if stream != nil {
				stream.replay(ch)
			} else {
				ch <- s.NewFailure(errors.New(fmt.Sprintf("sequence number %d has already been acknowledged", sequenceNumber)))
			}
		} else if sequenceNumber > session.nextCommandSequence() {
			session.scheduleCommand(sequenceNumber, func() {
				s.applySessionCommand(request, ch)
			})
		} else {
			s.applySessionCommand(request, ch)
		}
	}
}

func (s *SessionizedService) applySessionCommand(request *SessionCommandRequest, ch chan<- *Result) {
	session, ok := s.sessions[request.Context.SessionId]
	if !ok {
		ch <- s.NewFailure(errors.New(fmt.Sprintf("unknown session %d", request.Context.SessionId)))
	} else {
		s.session = session
		stream := session.addStream(request.Context.SequenceNumber, request.Name, ch)
		if err := s.Executor.Execute(request.Name, request.Input, stream); err != nil {
			ch <- s.NewFailure(err)
		}
		session.completeCommand(request.Context.SequenceNumber)
	}
}

func (s *SessionizedService) applyOpenSession(request *OpenSessionRequest, ch chan<- *Result) {
	session := newSession(s.Context, time.Duration(request.Timeout))
	s.sessions[session.Id] = session
	s.OnOpen(session)
	ch <- s.NewResult(proto.Marshal(&SessionResponse{
		Response: &SessionResponse_OpenSession{
			OpenSession: &OpenSessionResponse{
				SessionId: session.Id,
			},
		},
	}))
}

// applyKeepAlive applies a KeepAliveRequest to the service
func (s *SessionizedService) applyKeepAlive(request *KeepAliveRequest, ch chan<- *Result) {
	session, ok := s.sessions[request.SessionId]
	if !ok {
		ch <- s.NewFailure(errors.New(fmt.Sprintf("unknown session %d", request.SessionId)))
	} else {
		// Update the session's last updated timestamp to prevent it from expiring
		session.LastUpdated = s.Context.Timestamp()

		// Clear the results up to the given command sequence number
		session.ack(request.CommandSequence)

		// Acknowledge stream results up to the given result IDs
		for streamId, resultId := range request.Streams {
			stream, ok := session.streams[streamId]
			if ok {
				stream.ack(resultId)
			}
		}

		// Expire sessions that have not been kept alive
		s.expireSessions()

		// Send the response
		ch <- s.NewResult(proto.Marshal(&SessionResponse{
			Response: &SessionResponse_KeepAlive{
				KeepAlive: &KeepAliveResponse{},
			},
		}))
	}
}

// expireSessions expires sessions that have not been kept alive within their timeout
func (s *SessionizedService) expireSessions() {
	for id, session := range s.sessions {
		if session.timedOut(s.Context.Timestamp()) {
			session.close()
			delete(s.sessions, id)
			s.OnExpire(session)
		}
	}
}

func (s *SessionizedService) applyCloseSession(request *CloseSessionRequest, ch chan<- *Result) {
	session, ok := s.sessions[request.SessionId]
	if !ok {
		ch <- s.NewFailure(errors.New(fmt.Sprintf("unknown session %d", request.SessionId)))
	} else {
		session.close()
		s.OnClose(session)
		delete(s.sessions, session.Id)
	}
}

func (s *SessionizedService) Query(bytes []byte, ch chan<- *Result) {
	request := &SessionRequest{}
	err := proto.Unmarshal(bytes, request)
	if err != nil {
		ch <- s.NewFailure(err)
	} else {
		query := request.GetQuery()
		if query.Context.LastIndex > s.Context.Index() {
			s.Scheduler.(*scheduler).ScheduleIndex(query.Context.LastIndex, func() {
				s.sequenceQuery(query, ch)
			})
		} else {
			s.sequenceQuery(query, ch)
		}
	}
}

func (s *SessionizedService) sequenceQuery(query *SessionQueryRequest, ch chan<- *Result) {
	session, ok := s.sessions[query.Context.SessionId]
	if !ok {
		ch <- s.NewFailure(errors.New(fmt.Sprintf("unknown session %d", query.Context.SessionId)))
	} else {
		sequenceNumber := query.Context.LastSequenceNumber
		if sequenceNumber > session.commandSequence {
			session.scheduleQuery(sequenceNumber, func() {
				s.applyQuery(query, session, ch)
			})
		} else {
			s.applyQuery(query, session, ch)
		}
	}
}

func (s *SessionizedService) applyQuery(query *SessionQueryRequest, session *Session, ch chan<- *Result) {
	queryCh := make(chan *Result)
	s.context.setQuery()
	if err := s.Executor.Execute(query.Name, query.Input, queryCh); err != nil {
		ch <- s.NewFailure(err)
		return
	}

	go func() {
		for result := range queryCh {
			if result.Failed() {
				ch <- result
			} else {
				ch <- s.NewResult(proto.Marshal(&SessionResponse{
					Response: &SessionResponse_Query{
						Query: &SessionQueryResponse{
							Context: &SessionResponseContext{
								Index:    s.Context.Index(),
								Sequence: session.commandSequence,
							},
							Output: result.Output,
						},
					},
				}))
			}
		}
	}()
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
		commandCallbacks: make(map[uint64]func()),
		queryCallbacks:   make(map[uint64]*list.List),
		streams:          make(map[uint64]*sessionStream),
	}
}

// Session manages the ordering of request and response streams for a single client
type Session struct {
	Id               uint64
	Timeout          time.Duration
	LastUpdated      time.Time
	ctx              Context
	commandSequence  uint64
	ackSequence      uint64
	commandCallbacks map[uint64]func()
	queryCallbacks   map[uint64]*list.List
	streams          map[uint64]*sessionStream
}

// timedOut returns a boolean indicating whether the session is timed out
func (s *Session) timedOut(time time.Time) bool {
	return s.LastUpdated.UnixNano() > 0 && time.Sub(s.LastUpdated) > s.Timeout
}

// Channels returns a slice of all open channels of any type owned by the session
func (s *Session) Channels() []chan<- *Result {
	channels := make([]chan<- *Result, 0, len(s.streams))
	for _, stream := range s.streams {
		channels = append(channels, stream.inChan)
	}
	return channels
}

// ChannelsOf returns a slice of all open channels for the given named operation owned by the session
func (s *Session) ChannelsOf(op string) []chan<- *Result {
	channels := make([]chan<- *Result, 0, len(s.streams))
	for _, stream := range s.streams {
		if stream.Type == op {
			channels = append(channels, stream.inChan)
		}
	}
	return channels
}

// addStream adds a stream at the given sequence number
func (s *Session) addStream(sequence uint64, op string, outChan chan<- *Result) chan<- *Result {
	stream := &sessionStream{
		Id:      sequence,
		Type:    op,
		results: list.New(),
		inChan:  make(chan *Result),
		outChan: outChan,
	}
	s.streams[sequence] = stream
	go stream.process()
	return stream.inChan
}

// getStream returns a stream by the request sequence number
func (s *Session) getStream(sequenceNumber uint64) *sessionStream {
	return s.streams[sequenceNumber]
}

// ack acknowledges response streams up to the given request sequence number
func (s *Session) ack(sequenceNumber uint64) {
	if sequenceNumber > s.ackSequence {
		for i := s.ackSequence + 1; i <= sequenceNumber; i++ {
			delete(s.streams, i)
			s.ackSequence = i
		}
	}
}

// scheduleQuery schedules a query to be executed after the given sequence number
func (s *Session) scheduleQuery(sequenceNumber uint64, f func()) {
	queries, ok := s.queryCallbacks[sequenceNumber]
	if !ok {
		queries := list.New()
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

		command, ok := s.commandCallbacks[i]
		if ok {
			command()
			delete(s.commandCallbacks, i)
		}
	}
}

// close closes the session and completes all its streams
func (s *Session) close() {
	for _, stream := range s.streams {
		stream.close()
	}
}

// sessionStream manages a single stream for a session
type sessionStream struct {
	Id               uint64
	Type             string
	closed           bool
	currentSequence  uint64
	completeSequence uint64
	lastCompleted    uint64
	lastIndex        uint64
	ctx              Context
	inChan           chan *Result
	outChan          chan<- *Result
	results          *list.List
}

// sessionStreamResult contains a single stream result
type sessionStreamResult struct {
	id     uint64
	result *Result
}

// process processes the stream results on the in channel and passes them to the out channel
func (s *sessionStream) process() {
	for inResult := range s.inChan {
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

		// Record the last index sent on the stream
		s.lastIndex = inResult.Index

		// Create the stream result and add it to the results list.
		outResult := &sessionStreamResult{
			id: s.currentSequence,
			result: inResult.mutateResult(proto.Marshal(&SessionResponse{
				Response: &SessionResponse_Command{
					Command: &SessionCommandResponse{
						Context: &SessionResponseContext{
							StreamId: s.Id,
							Index:    inResult.Index,
							Sequence: s.currentSequence,
						},
						Output: inResult.Output,
					},
				},
			})),
		}
		s.results.PushBack(outResult)

		// If the out channel is set, send the result
		if s.outChan != nil {
			s.outChan <- outResult.result
		}

		s.closed = true
	}
}

// close closes the stream
func (s *sessionStream) close() {
	close(s.inChan)
}

// completeIndex returns the highest acknowledged index in the stream
func (s *sessionStream) completeIndex() uint64 {
	event := s.results.Front()
	if event != nil {
		return event.Value.(*sessionStreamResult).result.Index - 1
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
	if id > s.completeSequence {
		event := s.results.Front()
		for event != nil && event.Value.(*sessionStreamResult).id <= id {
			next := event.Next()
			s.results.Remove(event)
			s.completeSequence = event.Value.(*sessionStreamResult).id
			event = next
		}
	}
}

// replay resends results on the given channel
func (s *sessionStream) replay(ch chan<- *Result) {
	if s.outChan != nil {
		result := s.results.Front()
		for result != nil {
			ch <- result.Value.(*sessionStreamResult).result
			result = result.Next()
		}
	}
}
