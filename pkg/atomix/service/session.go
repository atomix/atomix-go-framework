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
	ctx := &mutableContext{}
	return &SessionizedService{
		service: &service{
			Scheduler: newScheduler(),
			Executor:  newExecutor(),
			Context:   ctx,
		},
		context:  ctx,
		parent:   parent,
		sessions: make(map[uint64]*Session),
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
				StreamId:       stream.ID,
				Type:           stream.Type,
				SequenceNumber: stream.eventID,
				LastCompleted:  stream.completeID,
			})
		}
		sessions = append(sessions, &SessionSnapshot{
			SessionId:       session.ID,
			Timeout:         uint64(session.Timeout),
			Timestamp:       uint64(session.LastUpdated.UnixNano()),
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
				ID:         stream.StreamId,
				Type:       stream.Type,
				eventID:    stream.SequenceNumber,
				completeID: stream.LastCompleted,
				ctx:        s.Context,
				inChan:     make(chan Result),
				results:    list.New(),
			}
			streams[s.ID] = s
		}
		s.sessions[session.SessionId] = &Session{
			ID:              session.SessionId,
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

func (s *SessionizedService) Command(bytes []byte, ch chan<- Output) {
	s.context.setCommand(s.parent.Timestamp())
	request := &SessionRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		if ch != nil {
			ch <- newFailure(err)
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
	}
}

func (s *SessionizedService) applyCommand(request *SessionCommandRequest, ch chan<- Output) {
	session, ok := s.sessions[request.Context.SessionId]
	if !ok {
		if ch != nil {
			ch <- newFailure(errors.New(fmt.Sprintf("unknown session %d", request.Context.SessionId)))
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
					ch <- newFailure(errors.New(fmt.Sprintf("sequence number %d has already been acknowledged", sequenceNumber)))
				}
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

func (s *SessionizedService) applySessionCommand(request *SessionCommandRequest, ch chan<- Output) {
	session, ok := s.sessions[request.Context.SessionId]
	if !ok {
		if ch != nil {
			ch <- newFailure(errors.New(fmt.Sprintf("unknown session %d", request.Context.SessionId)))
		}
	} else {
		s.session = session
		stream := session.addStream(request.Context.SequenceNumber, request.Name, ch)
		if err := s.Executor.Execute(request.Name, request.Input, stream); err != nil {
			if ch != nil {
				ch <- newFailure(err)
			}
		}
		session.completeCommand(request.Context.SequenceNumber)
	}
}

func (s *SessionizedService) applyOpenSession(request *OpenSessionRequest, ch chan<- Output) {
	session := newSession(s.Context, time.Duration(request.Timeout))
	s.sessions[session.ID] = session
	s.OnOpen(session)
	if ch != nil {
		ch <- newOutput(proto.Marshal(&SessionResponse{
			Response: &SessionResponse_OpenSession{
				OpenSession: &OpenSessionResponse{
					SessionId: session.ID,
				},
			},
		}))
	}
}

// applyKeepAlive applies a KeepAliveRequest to the service
func (s *SessionizedService) applyKeepAlive(request *KeepAliveRequest, ch chan<- Output) {
	session, ok := s.sessions[request.SessionId]
	if !ok {
		if ch != nil {
			ch <- newFailure(errors.New(fmt.Sprintf("unknown session %d", request.SessionId)))
		}
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
		if ch != nil {
			ch <- newOutput(proto.Marshal(&SessionResponse{
				Response: &SessionResponse_KeepAlive{
					KeepAlive: &KeepAliveResponse{},
				},
			}))
		}
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

func (s *SessionizedService) applyCloseSession(request *CloseSessionRequest, ch chan<- Output) {
	session, ok := s.sessions[request.SessionId]
	if !ok {
		if ch != nil {
			ch <- newFailure(errors.New(fmt.Sprintf("unknown session %d", request.SessionId)))
		}
	} else {
		// Close the session and notify the service.
		delete(s.sessions, session.ID)
		session.close()
		s.OnClose(session)

		// Send the response
		if ch != nil {
			ch <- newOutput(proto.Marshal(&SessionResponse{
				Response: &SessionResponse_CloseSession{
					CloseSession: &CloseSessionResponse{},
				},
			}))
		}
	}
}

func (s *SessionizedService) Query(bytes []byte, ch chan<- Output) {
	request := &SessionRequest{}
	err := proto.Unmarshal(bytes, request)
	if err != nil {
		if ch != nil {
			ch <- newFailure(err)
		}
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

func (s *SessionizedService) sequenceQuery(query *SessionQueryRequest, ch chan<- Output) {
	session, ok := s.sessions[query.Context.SessionId]
	if !ok {
		if ch != nil {
			ch <- newFailure(errors.New(fmt.Sprintf("unknown session %d", query.Context.SessionId)))
		}
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

func (s *SessionizedService) applyQuery(query *SessionQueryRequest, session *Session, ch chan<- Output) {
	// If the result channel is non-nil, create a channel for transforming results.
	var queryCh chan Result
	if ch != nil {
		queryCh = make(chan Result)
		go func() {
			defer close(ch)
			for result := range queryCh {
				if result.Failed() {
					ch <- result.Output
				} else {
					ch <- newOutput(proto.Marshal(&SessionResponse{
						Response: &SessionResponse_Query{
							Query: &SessionQueryResponse{
								Context: &SessionResponseContext{
									Index:    s.Context.Index(),
									Sequence: session.commandSequence,
								},
								Output: result.Value,
							},
						},
					}))
				}
			}
		}()
	}

	s.context.setQuery()
	if err := s.Executor.Execute(query.Name, query.Input, queryCh); err != nil {
		ch <- newFailure(err)
		return
	}
}

func (s *SessionizedService) OnOpen(session *Session) {

}

func (s *SessionizedService) OnExpire(session *Session) {

}

func (s *SessionizedService) OnClose(session *Session) {

}

func newSession(ctx Context, timeout time.Duration) *Session {
	return &Session{
		ID:               ctx.Index(),
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
	ID               uint64
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
func (s *Session) Channels() []chan<- Result {
	channels := make([]chan<- Result, 0, len(s.streams))
	for _, stream := range s.streams {
		channels = append(channels, stream.inChan)
	}
	return channels
}

// ChannelsOf returns a slice of all open channels for the given named operation owned by the session
func (s *Session) ChannelsOf(op string) []chan<- Result {
	channels := make([]chan<- Result, 0, len(s.streams))
	for _, stream := range s.streams {
		if stream.Type == op {
			channels = append(channels, stream.inChan)
		}
	}
	return channels
}

// addStream adds a stream at the given sequence number
func (s *Session) addStream(sequence uint64, op string, outChan chan<- Output) chan<- Result {
	stream := &sessionStream{
		ID:      sequence,
		Type:    op,
		ctx:     s.ctx,
		inChan:  make(chan Result),
		outChan: outChan,
		results: list.New(),
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

		command, ok := s.commandCallbacks[s.nextCommandSequence()]
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
	ID         uint64
	Type       string
	closed     bool
	eventID    uint64
	completeID uint64
	lastIndex  uint64
	ctx        Context
	inChan     chan Result
	outChan    chan<- Output
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

		// If the event is being published during a read operation, throw an exception.
		if s.ctx.OperationType() != OpTypeCommand {
			continue
		}

		// If the client acked a sequence number greater than the current event sequence number since we know the
		// client must have received it from another server.
		s.eventID++
		if s.completeID > s.eventID {
			continue
		}

		// Record the last index sent on the stream
		s.lastIndex = inResult.Index

		// Create the stream result and add it to the results list.
		if inResult.Succeeded() {
			inResult = Result{
				Index: inResult.Index,
				Output: newOutput(proto.Marshal(&SessionResponse{
					Response: &SessionResponse_Command{
						Command: &SessionCommandResponse{
							Context: &SessionResponseContext{
								StreamId: s.ID,
								Index:    inResult.Index,
								Sequence: s.eventID,
							},
							Output: inResult.Value,
						},
					},
				})),
			}
		}

		outResult := sessionStreamResult{
			id:     s.eventID,
			result: inResult,
		}
		s.results.PushBack(outResult)

		// If the out channel is set, send the result
		if s.outChan != nil {
			s.outChan <- outResult.result.Output
		}
	}

	s.closed = true

	ch := s.outChan
	if ch != nil {
		close(ch)
	}
}

// close closes the stream
func (s *sessionStream) close() {
	defer func() {
		recover()
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
	}
}

// replay resends results on the given channel
func (s *sessionStream) replay(ch chan<- Output) {
	result := s.results.Front()
	for result != nil {
		ch <- result.Value.(sessionStreamResult).result.Output
		result = result.Next()
	}
	s.outChan = ch
}
