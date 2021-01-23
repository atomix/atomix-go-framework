// Copyright 2020-present Open Networking Foundation.
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
	"context"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
	"sync"
	"time"
)

// SessionOption implements a session option
type SessionOption interface {
	prepare(options *sessionOptions)
}

// WithSessionTimeout returns a session SessionOption to configure the session timeout
func WithSessionTimeout(timeout time.Duration) SessionOption {
	return sessionTimeoutOption{timeout: timeout}
}

type sessionTimeoutOption struct {
	timeout time.Duration
}

func (o sessionTimeoutOption) prepare(options *sessionOptions) {
	options.timeout = o.timeout
}

type sessionOptions struct {
	id      string
	timeout time.Duration
}

// NewSession creates a new Session for the given partition
// name is the name of the primitive
// handler is the primitive's session handler
func NewSession(partition *Partition, opts ...SessionOption) *Session {
	options := &sessionOptions{
		id:      uuid.New().String(),
		timeout: 30 * time.Second,
	}
	for i := range opts {
		opts[i].prepare(options)
	}
	return &Session{
		Partition: partition,
		Timeout:   options.timeout,
		streams:   make(map[uint64]*StreamState),
		mu:        sync.RWMutex{},
		ticker:    time.NewTicker(options.timeout / 2),
	}
}

// Session maintains the session for a primitive
type Session struct {
	Partition  *Partition
	Timeout    time.Duration
	SessionID  uint64
	lastIndex  uint64
	requestID  uint64
	responseID uint64
	streams    map[uint64]*StreamState
	mu         sync.RWMutex
	ticker     *time.Ticker
}

// DoCommand submits a command to the service
func (s *Session) DoCommand(ctx context.Context, name string, input []byte) ([]byte, error) {
	service, err := getService(ctx)
	if err != nil {
		return nil, err
	}
	requestContext := s.nextCommandContext()
	response, responseStatus, responseContext, err := s.Partition.doCommand(ctx, name, input, service, requestContext)
	if err != nil {
		return nil, err
	}
	if responseStatus.Code != rsm.SessionResponseCode_OK {
		return nil, rsm.GetErrorFromStatus(responseStatus)
	}
	s.recordCommandResponse(requestContext, responseContext)
	return response, nil
}

// DoCommandStream submits a streaming command to the service
func (s *Session) DoCommandStream(ctx context.Context, name string, input []byte, outStream streams.WriteStream) error {
	service, err := getService(ctx)
	if err != nil {
		return err
	}
	streamState, requestContext := s.nextStream()
	ch := make(chan streams.Result)
	inStream := streams.NewChannelStream(ch)
	err = s.Partition.doCommandStream(context.Background(), name, input, service, requestContext, inStream)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case result, ok := <-ch:
				if !ok {
					s.deleteStream(streamState.ID)
					return
				}

				response := result.Value.(PartitionOutput)
				switch response.Type {
				case rsm.SessionResponseType_OPEN_STREAM:
					if streamState.serialize(response.Context) {
						outStream.Send(response.Result)
					}
				case rsm.SessionResponseType_CLOSE_STREAM:
					if streamState.serialize(response.Context) {
						outStream.Close()
						streamState.Close()
						return
					}
				case rsm.SessionResponseType_RESPONSE:
					// Record the response
					s.recordCommandResponse(requestContext, response.Context)

					// Attempt to serialize the response to the stream and skip the response if serialization failed.
					if streamState.serialize(response.Context) {
						outStream.Send(response.Result)
					}
				}
			case <-ctx.Done():
				s.deleteStream(streamState.ID)
			}
		}
	}()
	return nil
}

// DoQuery submits a query to the service
func (s *Session) DoQuery(ctx context.Context, name string, input []byte) ([]byte, error) {
	service, err := getService(ctx)
	if err != nil {
		return nil, err
	}
	requestContext := s.getQueryContext()
	response, responseStatus, responseContext, err := s.Partition.doQuery(ctx, name, input, service, requestContext)
	if err != nil {
		return nil, err
	}
	if responseStatus.Code != rsm.SessionResponseCode_OK {
		return nil, rsm.GetErrorFromStatus(responseStatus)
	}
	s.recordQueryResponse(requestContext, responseContext)
	return response, nil
}

// DoQueryStream submits a streaming query to the service
func (s *Session) DoQueryStream(ctx context.Context, name string, input []byte, stream streams.WriteStream) error {
	service, err := getService(ctx)
	if err != nil {
		return err
	}
	requestContext := s.getQueryContext()
	stream = streams.NewDecodingStream(stream, func(value interface{}, err error) (interface{}, error) {
		if err != nil {
			return nil, err
		}
		response := value.(PartitionOutput)
		s.recordQueryResponse(requestContext, response.Context)
		return SessionOutput{
			Result: streams.Result{
				Value: response.Value,
				Error: err,
			},
		}, err
	})
	return s.Partition.doQueryStream(ctx, name, input, service, requestContext, stream)
}

// DoCreateService creates the service
func (s *Session) DoCreateService(ctx context.Context) error {
	service, err := getService(ctx)
	if err != nil {
		return err
	}
	requestContext := s.nextCommandContext()
	responseStatus, responseContext, err := s.Partition.doCreateService(ctx, service, requestContext)
	if err != nil {
		return err
	}
	if responseStatus.Code != rsm.SessionResponseCode_OK {
		return rsm.GetErrorFromStatus(responseStatus)
	}
	s.recordCommandResponse(requestContext, responseContext)
	return nil
}

// DoCloseService closes the service
func (s *Session) DoCloseService(ctx context.Context) error {
	service, err := getService(ctx)
	if err != nil {
		return err
	}
	requestContext := s.nextCommandContext()
	responseStatus, responseContext, err := s.Partition.doCloseService(ctx, service, requestContext)
	if err != nil {
		return err
	}
	if responseStatus.Code != rsm.SessionResponseCode_OK {
		return rsm.GetErrorFromStatus(responseStatus)
	}
	s.recordCommandResponse(requestContext, responseContext)
	return nil
}

// DoDeleteService deletes the service
func (s *Session) DoDeleteService(ctx context.Context) error {
	service, err := getService(ctx)
	if err != nil {
		return err
	}
	requestContext := s.nextCommandContext()
	responseStatus, responseContext, err := s.Partition.doDeleteService(ctx, service, requestContext)
	if err != nil {
		return err
	}
	if responseStatus.Code != rsm.SessionResponseCode_OK {
		return rsm.GetErrorFromStatus(responseStatus)
	}
	s.recordCommandResponse(requestContext, responseContext)
	return nil
}

// open creates the session and begins keep-alives
func (s *Session) open(ctx context.Context) error {
	requestContext, _ := s.getStateContexts()
	responseStatus, responseContext, err := s.Partition.doOpenSession(ctx, requestContext, &s.Timeout)
	if err != nil {
		return err
	}

	if responseStatus.Code != rsm.SessionResponseCode_OK {
		return rsm.GetErrorFromStatus(responseStatus)
	}

	s.mu.Lock()
	s.SessionID = responseContext.SessionID
	s.lastIndex = responseContext.SessionID
	s.mu.Unlock()

	go func() {
		for range s.ticker.C {
			_ = s.keepAlive(context.TODO())
		}
	}()
	return nil
}

// keepAlive keeps the session alive
func (s *Session) keepAlive(ctx context.Context) error {
	requestContext, streamContexts := s.getStateContexts()
	responseStatus, responseContext, err := s.Partition.doKeepAliveSession(ctx, requestContext, streamContexts)
	if err != nil {
		return err
	}

	if responseStatus.Code != rsm.SessionResponseCode_OK {
		return rsm.GetErrorFromStatus(responseStatus)
	}

	s.recordCommandResponse(requestContext, responseContext)
	return nil
}

// Close closes the session
func (s *Session) Close() error {
	err := s.close(context.TODO())
	s.ticker.Stop()
	return err
}

// close closes the session
func (s *Session) close(ctx context.Context) error {
	requestContext, _ := s.getStateContexts()
	responseStatus, responseContext, err := s.Partition.doCloseSession(ctx, requestContext)
	if err != nil {
		return err
	}

	if responseStatus.Code != rsm.SessionResponseCode_OK {
		return rsm.GetErrorFromStatus(responseStatus)
	}

	s.recordCommandResponse(requestContext, responseContext)
	return nil
}

// getStateContexts gets the header for the current state of the session
func (s *Session) getStateContexts() (rsm.SessionCommandContext, []rsm.SessionStreamContext) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return rsm.SessionCommandContext{
		SessionID:      s.SessionID,
		SequenceNumber: s.requestID,
	}, s.getStreamContexts()
}

// getQueryContext gets the current read header
func (s *Session) getQueryContext() rsm.SessionQueryContext {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return rsm.SessionQueryContext{
		SessionID:          s.SessionID,
		LastSequenceNumber: s.responseID,
		LastIndex:          s.lastIndex,
	}
}

// nextCommandContext returns the next write context
func (s *Session) nextCommandContext() rsm.SessionCommandContext {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requestID = s.requestID + 1
	return rsm.SessionCommandContext{
		SessionID:      s.SessionID,
		SequenceNumber: s.requestID,
	}
}

// nextStreamHeader returns the next write stream and header
func (s *Session) nextStream() (*StreamState, rsm.SessionCommandContext) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requestID = s.requestID + 1
	stream := &StreamState{
		ID:      s.requestID,
		session: s,
	}
	s.streams[s.requestID] = stream
	command := rsm.SessionCommandContext{
		SessionID:      s.SessionID,
		SequenceNumber: s.requestID,
	}
	return stream, command
}

// recordCommandResponse records the index in a response header
func (s *Session) recordCommandResponse(requestContext rsm.SessionCommandContext, responseContext rsm.SessionResponseContext) {
	// Use a double-checked lock to avoid locking when multiple responses are received for an index.
	s.mu.RLock()
	if responseContext.Index > s.lastIndex {
		s.mu.RUnlock()
		s.mu.Lock()

		// If the request ID is greater than the highest response ID, update the response ID.
		if requestContext.SequenceNumber > s.responseID {
			s.responseID = requestContext.SequenceNumber
		}

		// If the response index has increased, update the last received index
		if responseContext.Index > s.lastIndex {
			s.lastIndex = responseContext.Index
		}
		s.mu.Unlock()
	} else {
		s.mu.RUnlock()
	}
}

// recordQueryResponse records the index in a response header
func (s *Session) recordQueryResponse(requestContext rsm.SessionQueryContext, responseContext rsm.SessionResponseContext) {
	// Use a double-checked lock to avoid locking when multiple responses are received for an index.
	s.mu.RLock()
	if responseContext.Index > s.lastIndex {
		s.mu.RUnlock()
		s.mu.Lock()

		// If the response index has increased, update the last received index
		if responseContext.Index > s.lastIndex {
			s.lastIndex = responseContext.Index
		}
		s.mu.Unlock()
	} else {
		s.mu.RUnlock()
	}
}

// deleteStream deletes the given stream from the session
func (s *Session) deleteStream(streamID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.streams, streamID)
}

// getStreamHeaders returns a slice of headers for all open streams
func (s *Session) getStreamContexts() []rsm.SessionStreamContext {
	result := make([]rsm.SessionStreamContext, 0, len(s.streams))
	for _, stream := range s.streams {
		if stream.ID <= s.responseID {
			result = append(result, stream.getHeader())
		}
	}
	return result
}

// StreamState manages the context for a single response stream within a session
type StreamState struct {
	ID         uint64
	session    *Session
	responseID uint64
	mu         sync.RWMutex
}

// getHeader returns the current header for the stream
func (s *StreamState) getHeader() rsm.SessionStreamContext {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return rsm.SessionStreamContext{
		StreamID:   s.ID,
		ResponseID: s.responseID,
	}
}

// serialize updates the stream response metadata and returns whether the response was received in sequential order
func (s *StreamState) serialize(context rsm.SessionResponseContext) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if context.Sequence == s.responseID+1 {
		s.responseID++
		return true
	}
	return false
}

// Close closes the stream
func (s *StreamState) Close() {
	s.session.deleteStream(s.ID)
}

// SessionOutput is a result for session-supporting servers containing session header information
type SessionOutput struct {
	streams.Result
}

// getService returns the service ID for the given primitive
func getService(ctx context.Context) (rsm.ServiceId, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return rsm.ServiceId{}, errors.NewInvalid("service metadata not found")
	}
	types := md.Get(primitiveTypeKey)
	names := md.Get(primitiveNameKey)
	if len(types) == 0 || len(names) == 0 {
		return rsm.ServiceId{}, errors.NewInvalid("service metadata not found")
	}
	return rsm.ServiceId{
		Type: types[0],
		Name: names[0],
	}, nil
}
