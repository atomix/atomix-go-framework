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
	"container/list"
	"context"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	rsm "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm2"
	streams "github.com/atomix/atomix-go-framework/pkg/atomix/stream"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"io"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

var log = logging.GetLogger("proxy", "rsm")

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
	retry   time.Duration
}

// NewSession creates a new Session for the given partition
// name is the name of the primitive
// handler is the primitive's session handler
func NewSession(partition cluster.Partition, log logging.Logger, opts ...SessionOption) *Session {
	options := &sessionOptions{
		id:      uuid.New().String(),
		retry:   15 * time.Second,
		timeout: time.Minute,
	}
	for i := range opts {
		opts[i].prepare(options)
	}
	return &Session{
		partition:     partition,
		Timeout:       options.timeout,
		retryInterval: options.retry,
		log:           log,
		mu:            sync.RWMutex{},
		ticker:        time.NewTicker(options.timeout / 4),
	}
}

type Session struct {
	partition     cluster.Partition
	serviceID     rsm.ServiceID
	sessionID     *uint64
	requestID     *uint64
	responseIndex *uint64
}

type sessionState struct {
	partition     cluster.Partition
	serviceID     rsm.ServiceID
	sessionID     *uint64
	requestID     *uint64
	responseIndex *uint64
}

type sessionReader struct {
	*sessionState
}

// doQuery submits a query to the service
func (s *sessionReader) doQuery(ctx context.Context, service rsm.ServiceID, operationID rsm.OperationID, input []byte) ([]byte, error) {
	requestContext := s.nextCommandContext()
	response, responseStatus, responseContext, err := s.doCommand(ctx, operationID, input, service, requestContext)
	if err != nil {
		return nil, err
	}
	if responseStatus.Code != rsm.SessionResponseCode_OK {
		return nil, rsm.GetErrorFromStatus(responseStatus)
	}
	s.recordCommandResponse(requestContext, responseContext)
	return response, nil
}

// doQueryStream submits a streaming query to the service
func (s *sessionReader) doQueryStream(ctx context.Context, service rsm.ServiceID, operationID rsm.OperationID, input []byte, outStream streams.WriteStream) error {
	streamState, requestContext := s.nextStream()
	ch := make(chan streams.Result)
	inStream := streams.NewChannelStream(ch)
	err := s.doCommandStream(context.Background(), operationID, input, service, requestContext, inStream)
	if err != nil {
		return err
	}

	go func() {
		var responseID rsm.ResponseID
		for {
			select {
			case result, ok := <-ch:
				if !ok {
					return
				}

				if result.Failed() {
					outStream.Error(result.Error)
					outStream.Close()
					return
				}

				response := result.Value.(PartitionOutput)
				if response.Context.ResponseID == responseID+1 {
					responseID++
				} else {
					continue
				}

				switch response.Type {
				case rsm.SessionResponseType_OPEN_STREAM:
					outStream.Send(response.Result)
				case rsm.SessionResponseType_CLOSE_STREAM:
					outStream.Close()
					streamState.Close()
					return
				case rsm.SessionResponseType_RESPONSE:
					// Record the response
					s.recordCommandResponse(requestContext, response.Context)
					outStream.Send(response.Result)
				}
			case <-ctx.Done():
				outStream.Error(ctx.Err())
				outStream.Close()
				return
			}
		}
	}()
	return nil
}

type commandRequest struct {
	operationID rsm.OperationID
	input       []byte
	stream      streams.WriteStream
}

type sessionWriter struct {
	*sessionState
	responseID      rsm.ResponseID
	pendingRequests *list.List
	responseStreams map[rsm.RequestID]streams.WriteStream
	connectCh       chan cluster.ReplicaID
	openCh          chan struct{}
	keepAliveCh     chan struct{}
	closeCh         chan struct{}
	commandCh       chan commandRequest
	requestCh       chan *rsm.SessionWriteRequest
	responseCh      chan *rsm.SessionWriteResponse
}

// doCommand submits a command to the service
func (s *sessionWriter) doCommand(ctx context.Context, operationID rsm.OperationID, input []byte) ([]byte, error) {
	resultCh := make(chan streams.Result, 1)
	stream := streams.NewChannelStream(resultCh)
	s.commandCh <- commandRequest{
		operationID: operationID,
		input:       input,
		stream:      stream,
	}
	result, ok := <-resultCh
	if !ok {
		return nil, errors.NewUnavailable("stream closed")
	} else if result.Failed() {
		return nil, result.Error
	}
	return result.Value.([]byte), nil
}

// doCommandStream submits a streaming command to the service
func (s *sessionWriter) doCommandStream(ctx context.Context, operationID rsm.OperationID, input []byte, outStream streams.WriteStream) error {
	resultCh := make(chan streams.Result)
	stream := streams.NewChannelStream(resultCh)
	s.commandCh <- commandRequest{
		operationID: operationID,
		input:       input,
		stream:      stream,
	}

	go func() {
		var responseID rsm.ResponseID
		for {
			select {
			case result, ok := <-ch:
				if !ok {
					return
				}

				if result.Failed() {
					outStream.Error(result.Error)
					outStream.Close()
					return
				}

				response := result.Value.(PartitionOutput)
				if response.Context.ResponseID == responseID+1 {
					responseID++
				} else {
					continue
				}

				switch response.Type {
				case rsm.SessionResponseType_OPEN_STREAM:
					outStream.Send(response.Result)
				case rsm.SessionResponseType_CLOSE_STREAM:
					outStream.Close()
					streamState.Close()
					return
				case rsm.SessionResponseType_RESPONSE:
					// Record the response
					s.recordCommandResponse(requestContext, response.Context)
					outStream.Send(response.Result)
				}
			case <-ctx.Done():
				outStream.Error(ctx.Err())
				outStream.Close()
				return
			}
		}
	}()
	return nil
}

func (s *sessionWriter) sendRequests() {
	for {
		select {
		case replicaID := <-s.connectCh:
			s.connect(replicaID)
		case <-s.openCh:
			s.sendOpen()
		case <-s.keepAliveCh:
			s.sendKeepAlive()
		case <-s.closeCh:
			s.sendClose()
		case command := <-s.commandCh:
			s.sendCommand(command)
		case response := <-s.responseCh:
			s.recvResponse(response)
		}
	}
}

func (s *sessionWriter) sendOpen() {
	request := &rsm.SessionWriteRequest{
		Request: &rsm.SessionWriteRequest_OpenSession{
			OpenSession: &rsm.OpenSessionRequest{},
		},
	}
	s.sendRequest(request, stream)
}

func (s *sessionWriter) sendKeepAlive() {
	request := &rsm.SessionWriteRequest{
		SessionID: rsm.SesionID(atomic.LoadUint64(s.sessionID)),
		RequestID: rsm.RequestID(atomic.AddUint64(s.requestID, 1)),
		Request: &rsm.SessionWriteRequest_KeepAlive{
			KeepAlive: &rsm.KeepAliveRequest{
				AckResponseID: s.responseID,
			},
		},
	}
	s.sendRequest(request, stream)
}

func (s *sessionWriter) sendClose() {
	request := &rsm.SessionWriteRequest{
		SessionID: rsm.SesionID(atomic.LoadUint64(s.sessionID)),
		RequestID: rsm.RequestID(atomic.AddUint64(s.requestID, 1)),
		Request: &rsm.SessionWriteRequest_CloseSession{
			CloseSession: &rsm.CloseSessionRequest{},
		},
	}
	s.sendRequest(request, stream)
}

func (s *sessionWriter) sendCommand(command commandRequest) {
	request := &rsm.SessionWriteRequest{
		SessionID: rsm.SesionID(atomic.LoadUint64(s.sessionID)),
		RequestID: rsm.RequestID(atomic.AddUint64(s.requestID, 1)),
		Request: &rsm.SessionWriteRequest_Command{
			Command: &rsm.CommandRequest{
				OperationID: command.operationID,
				Value:       command.input,
			},
		},
	}
	s.sendRequest(request, command.stream)
}

func (s *sessionWriter) sendRequest(request *rsm.SessionWriteRequest, stream streams.WriteStream) {
	elem := s.pendingRequests.PushBack(request)
	s.requestElems[request.RequestID] = elem
	s.responseStreams[request.RequestID] = stream
	s.requestCh <- request
}

func (s *sessionWriter) recvResponse(response *rsm.SessionWriteResponse) {
	if response.ResponseID != s.responseID+1 {
		return
	}
	s.responseID++

	stream, ok := s.responseStreams[response.RequestID]
	if !ok {
		return
	}

	switch response.Response.(type) {
	case *rsm.SessionWriteResponse_Command:
		stream.Value(response)
		switch response.Type {
		case rsm.SessionResponseType_RESPONSE | rsm.SessionResponseType_STREAM_CLOSE:
			elem := s.requestElems[response.RequestID]
			delete(s.requestElems, response.RequestID)
			s.pendingRequests.Remove(elem)
			delete(s.responseStreams, response.RequestID)
		}
	case *rsm.SessionWriteResponse_OpenSession:
	case *rsm.SessionWriteResponse_KeepAlive:
	case *rsm.SessionWriteResponse_CloseSession:
	}
}

func (s *sessionWriter) reconnect(replicaID cluster.ReplicaID) {
	go func() {
		s.connectCh <- replicaID
	}()
}

func (s *sessionWriter) connect(replicaID cluster.ReplicaID) {
	if s.requestCh != nil {
		close(s.requestCh)
	}

	for {
		replica, _ := s.partition.Replica(replicaID)
		log.Infof("Connecting to partition %d replica %s", s.partition.ID(), replicaID)
		conn, err := replica.Connect(context.Background(), cluster.WithDialOption(grpc.WithInsecure()))
		if err != nil {
			log.Warnf("Connecting to partition %d replica %s failed", s.partition.ID(), replica.ID, err)
			replicaID = s.partition.Replicas()[rand.Intn(len(s.partition.Replicas()))].ID
			continue
		}
		client := rsm.NewPartitionServiceClient(conn)
		stream, err := client.WriteStream(context.Background())
		if err != nil {
			log.Warnf("Connecting to partition %d replica %s failed", s.partition.ID(), replica.ID, err)
			replicaID = s.partition.Replicas()[rand.Intn(len(s.partition.Replicas()))].ID
			continue
		}

		requestCh := make(chan *rsm.SessionWriteRequest)
		s.requestCh = requestCh

		pendingRequests := make([]*rsm.SessionWriteRequest, 0, s.pendingRequests.Len())
		elem := s.pendingRequests.Front()
		for elem != nil {
			pendingRequests = append(pendingRequests, elem.Value.(*rsm.SessionWriteRequest))
			elem = elem.Next()
		}

		go func() {
			for _, request := range pendingRequests {
				err := stream.Send(&rsm.PartitionWriteRequest{
					PartitionID: uint32(s.partition.ID()),
					Request: rsm.ServiceWriteRequest{
						ServiceID: s.serviceID,
						Request:   *request,
					},
				})
				if err == io.EOF {
					return
				}
			}

			for request := range requestCh {
				err := stream.Send(&rsm.PartitionWriteRequest{
					PartitionID: uint32(s.partition.ID()),
					Request: rsm.ServiceWriteRequest{
						ServiceID: s.serviceID,
						Request:   *request,
					},
				})
				if err == io.EOF {
					return
				}
			}
		}()

		go func() {
			for {
				response, err := stream.Recv()
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				s.responseCh <- &response.Response.Response
			}
		}()
		break
	}
}

// DoCommand submits a command to the service
func (s *Session) DoCommand(ctx context.Context, service rsm.ServiceID, operationID rsm.OperationID, input []byte) ([]byte, error) {
	requestContext := s.nextCommandContext()
	response, responseStatus, responseContext, err := s.doCommand(ctx, operationID, input, service, requestContext)
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
func (s *Session) DoCommandStream(ctx context.Context, service rsm.ServiceID, operationID rsm.OperationID, input []byte, outStream streams.WriteStream) error {
	streamState, requestContext := s.nextStream()
	ch := make(chan streams.Result)
	inStream := streams.NewChannelStream(ch)
	err := s.doCommandStream(context.Background(), operationID, input, service, requestContext, inStream)
	if err != nil {
		return err
	}

	go func() {
		var responseID rsm.ResponseID
		for {
			select {
			case result, ok := <-ch:
				if !ok {
					return
				}

				if result.Failed() {
					outStream.Error(result.Error)
					outStream.Close()
					return
				}

				response := result.Value.(PartitionOutput)
				if response.Context.ResponseID == responseID+1 {
					responseID++
				} else {
					continue
				}

				switch response.Type {
				case rsm.SessionResponseType_OPEN_STREAM:
					outStream.Send(response.Result)
				case rsm.SessionResponseType_CLOSE_STREAM:
					outStream.Close()
					streamState.Close()
					return
				case rsm.SessionResponseType_RESPONSE:
					// Record the response
					s.recordCommandResponse(requestContext, response.Context)
					outStream.Send(response.Result)
				}
			case <-ctx.Done():
				outStream.Error(ctx.Err())
				outStream.Close()
				return
			}
		}
	}()
	return nil
}

// DoQuery submits a query to the service
func (s *Session) DoQuery(ctx context.Context, service rsm.ServiceID, operationID rsm.OperationID, input []byte) ([]byte, error) {
	requestContext := s.nextCommandContext()
	response, responseStatus, responseContext, err := s.doCommand(ctx, operationID, input, service, requestContext)
	if err != nil {
		return nil, err
	}
	if responseStatus.Code != rsm.SessionResponseCode_OK {
		return nil, rsm.GetErrorFromStatus(responseStatus)
	}
	s.recordCommandResponse(requestContext, responseContext)
	return response, nil
}

// DoQueryStream submits a streaming query to the service
func (s *Session) DoQueryStream(ctx context.Context, service rsm.ServiceID, operationID rsm.OperationID, input []byte, outStream streams.WriteStream) error {
	streamState, requestContext := s.nextStream()
	ch := make(chan streams.Result)
	inStream := streams.NewChannelStream(ch)
	err := s.doCommandStream(context.Background(), operationID, input, service, requestContext, inStream)
	if err != nil {
		return err
	}

	go func() {
		var responseID rsm.ResponseID
		for {
			select {
			case result, ok := <-ch:
				if !ok {
					return
				}

				if result.Failed() {
					outStream.Error(result.Error)
					outStream.Close()
					return
				}

				response := result.Value.(PartitionOutput)
				if response.Context.ResponseID == responseID+1 {
					responseID++
				} else {
					continue
				}

				switch response.Type {
				case rsm.SessionResponseType_OPEN_STREAM:
					outStream.Send(response.Result)
				case rsm.SessionResponseType_CLOSE_STREAM:
					outStream.Close()
					streamState.Close()
					return
				case rsm.SessionResponseType_RESPONSE:
					// Record the response
					s.recordCommandResponse(requestContext, response.Context)
					outStream.Send(response.Result)
				}
			case <-ctx.Done():
				outStream.Error(ctx.Err())
				outStream.Close()
				return
			}
		}
	}()
	return nil
}

type sessionStream struct {
}

// Session maintains the session for a primitive
type Session struct {
	partition     cluster.Partition
	Timeout       time.Duration
	SessionID     rsm.SessionID
	lastIndex     rsm.Index
	requestID     rsm.RequestID
	responseID    rsm.RequestID
	log           logging.Logger
	retryInterval time.Duration
	conn          *grpc.ClientConn
	leader        *cluster.Replica
	mu            sync.RWMutex
	ticker        *time.Ticker
}

// DoCommand submits a command to the service
func (s *Session) DoCommand(ctx context.Context, service rsm.ServiceID, operationID rsm.OperationID, input []byte) ([]byte, error) {
	requestContext := s.nextCommandContext()
	response, responseStatus, responseContext, err := s.doCommand(ctx, operationID, input, service, requestContext)
	if err != nil {
		return nil, err
	}
	if responseStatus.Code != rsm.SessionResponseCode_OK {
		return nil, rsm.GetErrorFromStatus(responseStatus)
	}
	s.recordCommandResponse(requestContext, responseContext)
	return response, nil
}

// doCommand submits a command to the service
func (s *Session) doCommand(ctx context.Context, operationID rsm.OperationID, input []byte, serviceID rsm.ServiceID, command rsm.SessionCommandContext) ([]byte, rsm.SessionResponseStatus, rsm.SessionResponseContext, error) {
	request := &rsm.StorageRequest{
		PartitionID: uint32(s.partition.ID()),
		Request: &rsm.SessionRequest{
			Request: &rsm.SessionRequest_Command{
				Command: &rsm.SessionCommandRequest{
					Context: command,
					Command: rsm.ServiceCommandRequest{
						ServiceID: serviceID,
						Request: &rsm.ServiceCommandRequest_Operation{
							Operation: &rsm.ServiceOperationRequest{
								OperationID: operationID,
								Value:       input,
							},
						},
					},
				},
			},
		},
	}

	response, err := s.doRequest(ctx, request)
	if err != nil {
		return nil, rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}
	return response.Response.GetCommand().Response.GetOperation().Result, response.Response.Status, response.Response.GetCommand().GetContext(), err
}

// DoCommandStream submits a streaming command to the service
func (s *Session) DoCommandStream(ctx context.Context, service rsm.ServiceID, operationID rsm.OperationID, input []byte, outStream streams.WriteStream) error {
	streamState, requestContext := s.nextStream()
	ch := make(chan streams.Result)
	inStream := streams.NewChannelStream(ch)
	err := s.doCommandStream(context.Background(), operationID, input, service, requestContext, inStream)
	if err != nil {
		return err
	}

	go func() {
		var responseID rsm.ResponseID
		for {
			select {
			case result, ok := <-ch:
				if !ok {
					return
				}

				if result.Failed() {
					outStream.Error(result.Error)
					outStream.Close()
					return
				}

				response := result.Value.(PartitionOutput)
				if response.Context.ResponseID == responseID+1 {
					responseID++
				} else {
					continue
				}

				switch response.Type {
				case rsm.SessionResponseType_OPEN_STREAM:
					outStream.Send(response.Result)
				case rsm.SessionResponseType_CLOSE_STREAM:
					outStream.Close()
					streamState.Close()
					return
				case rsm.SessionResponseType_RESPONSE:
					// Record the response
					s.recordCommandResponse(requestContext, response.Context)
					outStream.Send(response.Result)
				}
			case <-ctx.Done():
				outStream.Error(ctx.Err())
				outStream.Close()
				return
			}
		}
	}()
	return nil
}

// doCommandStream submits a streaming command to the service
func (s *Session) doCommandStream(ctx context.Context, operationID rsm.OperationID, input []byte, serviceID rsm.ServiceID, context rsm.SessionCommandContext, stream streams.WriteStream) error {
	request := &rsm.StorageRequest{
		PartitionID: uint32(s.partition.ID()),
		Request: &rsm.SessionRequest{
			Request: &rsm.SessionRequest_Command{
				Command: &rsm.SessionCommandRequest{
					Context: context,
					Command: rsm.ServiceCommandRequest{
						ServiceID: serviceID,
						Request: &rsm.ServiceCommandRequest_Operation{
							Operation: &rsm.ServiceOperationRequest{
								OperationID: operationID,
								Value:       input,
							},
						},
					},
				},
			},
		},
	}
	return s.doStream(ctx, request, streams.NewDecodingStream(stream, func(value interface{}, err error) (interface{}, error) {
		if err != nil {
			return nil, err
		}
		response := value.(*rsm.StorageResponse)
		commandResponse := response.Response.GetCommand()
		var result []byte
		if commandResponse.Response.GetOperation() != nil {
			result = commandResponse.Response.GetOperation().Result
		}
		return PartitionOutput{
			Type:    response.Response.Type,
			Status:  response.Response.Status,
			Context: commandResponse.Context,
			Result: streams.Result{
				Value: result,
			},
		}, nil
	}), true)
}

// DoQuery submits a query to the service
func (s *Session) DoQuery(ctx context.Context, serviceID rsm.ServiceID, operationID rsm.OperationID, input []byte, sync bool) ([]byte, error) {
	requestContext := s.getQueryContext(sync)
	response, responseStatus, responseContext, err := s.doQuery(ctx, operationID, input, serviceID, requestContext)
	if err != nil {
		return nil, err
	}
	if responseStatus.Code != rsm.SessionResponseCode_OK {
		return nil, rsm.GetErrorFromStatus(responseStatus)
	}
	s.recordQueryResponse(requestContext, responseContext)
	return response, nil
}

// doQuery submits a query to the service
func (s *Session) doQuery(ctx context.Context, operationID rsm.OperationID, input []byte, serviceID rsm.ServiceID, query rsm.SessionQueryContext) ([]byte, rsm.SessionResponseStatus, rsm.SessionResponseContext, error) {
	request := &rsm.StorageRequest{
		PartitionID: uint32(s.partition.ID()),
		Request: &rsm.SessionRequest{
			Request: &rsm.SessionRequest_Query{
				Query: &rsm.SessionQueryRequest{
					Context: query,
					Query: rsm.ServiceQueryRequest{
						ServiceID: serviceID,
						Request: &rsm.ServiceQueryRequest_Operation{
							Operation: &rsm.ServiceOperationRequest{
								OperationID: operationID,
								Value:       input,
							},
						},
					},
				},
			},
		},
	}

	response, err := s.doRequest(ctx, request)
	if err != nil {
		return nil, rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}
	return response.Response.GetQuery().Response.GetOperation().Result, response.Response.Status, response.Response.GetQuery().GetContext(), err
}

// DoQueryStream submits a streaming query to the service
func (s *Session) DoQueryStream(ctx context.Context, serviceID rsm.ServiceID, operationID rsm.OperationID, input []byte, outStream streams.WriteStream, sync bool) error {
	requestContext := s.getQueryContext(sync)
	ch := make(chan streams.Result)
	inStream := streams.NewChannelStream(ch)
	err := s.doQueryStream(context.Background(), operationID, input, serviceID, requestContext, inStream)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case result, ok := <-ch:
				if !ok {
					return
				}

				if result.Failed() {
					outStream.Error(result.Error)
					outStream.Close()
					return
				}

				response := result.Value.(PartitionOutput)
				switch response.Type {
				case rsm.SessionResponseType_OPEN_STREAM:
				case rsm.SessionResponseType_CLOSE_STREAM:
					outStream.Close()
					return
				case rsm.SessionResponseType_RESPONSE:
					s.recordQueryResponse(requestContext, response.Context)
					outStream.Send(response.Result)
				}
			case <-ctx.Done():
				outStream.Error(ctx.Err())
				outStream.Close()
				return
			}
		}
	}()
	return nil
}

// doQueryStream submits a streaming query to the service
func (s *Session) doQueryStream(ctx context.Context, operationID rsm.OperationID, input []byte, serviceID rsm.ServiceID, context rsm.SessionQueryContext, stream streams.WriteStream) error {
	request := &rsm.StorageRequest{
		PartitionID: uint32(s.partition.ID()),
		Request: &rsm.SessionRequest{
			Request: &rsm.SessionRequest_Query{
				Query: &rsm.SessionQueryRequest{
					Context: context,
					Query: rsm.ServiceQueryRequest{
						ServiceID: serviceID,
						Request: &rsm.ServiceQueryRequest_Operation{
							Operation: &rsm.ServiceOperationRequest{
								OperationID: operationID,
								Value:       input,
							},
						},
					},
				},
			},
		},
	}
	return s.doStream(ctx, request, streams.NewDecodingStream(stream, func(value interface{}, err error) (interface{}, error) {
		if err != nil {
			return nil, err
		}
		response := value.(*rsm.StorageResponse)
		queryResponse := response.Response.GetQuery()
		var result []byte
		if queryResponse.Response.GetOperation() != nil {
			result = queryResponse.Response.GetOperation().Result
		}
		return PartitionOutput{
			Type:    response.Response.Type,
			Status:  response.Response.Status,
			Context: queryResponse.Context,
			Result: streams.Result{
				Value: result,
			},
		}, nil
	}), false)
}

// doMetadata submits a metadata query to the service
func (s *Session) doMetadata(ctx context.Context, serviceType string, namespace string, context rsm.SessionQueryContext) ([]rsm.ServiceID, rsm.SessionResponseStatus, rsm.SessionResponseContext, error) {
	request := &rsm.StorageRequest{
		PartitionID: uint32(s.partition.ID()),
		Request: &rsm.SessionRequest{
			Request: &rsm.SessionRequest_Query{
				Query: &rsm.SessionQueryRequest{
					Context: context,
					Query: rsm.ServiceQueryRequest{
						Request: &rsm.ServiceQueryRequest_Metadata{
							Metadata: &rsm.ServiceMetadataRequest{
								Type: serviceType,
							},
						},
					},
				},
			},
		},
	}
	response, err := s.doRequest(ctx, request)
	if err != nil {
		return nil, rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}
	return response.Response.GetQuery().Response.GetMetadata().Services, response.Response.Status, response.Response.GetQuery().Context, nil
}

// DoCreateService creates the service
func (s *Session) DoCreateService(ctx context.Context, serviceID rsm.ServiceID) error {
	requestContext := s.nextCommandContext()
	responseStatus, responseContext, err := s.doCreateService(ctx, serviceID, requestContext)
	if err != nil {
		return err
	}
	if responseStatus.Code != rsm.SessionResponseCode_OK {
		return rsm.GetErrorFromStatus(responseStatus)
	}
	s.recordCommandResponse(requestContext, responseContext)
	return nil
}

// doCreateService creates the service
func (s *Session) doCreateService(ctx context.Context, serviceID rsm.ServiceID, context rsm.SessionCommandContext) (rsm.SessionResponseStatus, rsm.SessionResponseContext, error) {
	request := &rsm.StorageRequest{
		PartitionID: uint32(s.partition.ID()),
		Request: &rsm.SessionRequest{
			Request: &rsm.SessionRequest_Command{
				Command: &rsm.SessionCommandRequest{
					Context: context,
					Command: rsm.ServiceCommandRequest{
						ServiceID: serviceID,
						Request: &rsm.ServiceCommandRequest_Create{
							Create: &rsm.ServiceCreateRequest{},
						},
					},
				},
			},
		},
	}
	response, err := s.doRequest(ctx, request)
	if err != nil {
		return rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}
	return response.Response.Status, response.Response.GetCommand().Context, nil
}

// DoCloseService closes the service
func (s *Session) DoCloseService(ctx context.Context, serviceID rsm.ServiceID) error {
	requestContext := s.nextCommandContext()
	responseStatus, responseContext, err := s.doCloseService(ctx, serviceID, requestContext)
	if err != nil {
		return err
	}
	if responseStatus.Code != rsm.SessionResponseCode_OK {
		return rsm.GetErrorFromStatus(responseStatus)
	}
	s.recordCommandResponse(requestContext, responseContext)
	return nil
}

// doCloseService closes the service
func (s *Session) doCloseService(ctx context.Context, serviceID rsm.ServiceID, context rsm.SessionCommandContext) (rsm.SessionResponseStatus, rsm.SessionResponseContext, error) {
	request := &rsm.StorageRequest{
		PartitionID: uint32(s.partition.ID()),
		Request: &rsm.SessionRequest{
			Request: &rsm.SessionRequest_Command{
				Command: &rsm.SessionCommandRequest{
					Context: context,
					Command: rsm.ServiceCommandRequest{
						ServiceID: serviceID,
						Request: &rsm.ServiceCommandRequest_Close{
							Close: &rsm.ServiceCloseRequest{},
						},
					},
				},
			},
		},
	}
	response, err := s.doRequest(ctx, request)
	if err != nil {
		return rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}
	return response.Response.Status, response.Response.GetCommand().Context, nil
}

// DoDeleteService deletes the service
func (s *Session) DoDeleteService(ctx context.Context, serviceID rsm.ServiceID) error {
	requestContext := s.nextCommandContext()
	responseStatus, responseContext, err := s.doDeleteService(ctx, serviceID, requestContext)
	if err != nil {
		return err
	}
	if responseStatus.Code != rsm.SessionResponseCode_OK {
		return rsm.GetErrorFromStatus(responseStatus)
	}
	s.recordCommandResponse(requestContext, responseContext)
	return nil
}

// doDeleteService deletes the service
func (s *Session) doDeleteService(ctx context.Context, serviceID rsm.ServiceID, context rsm.SessionCommandContext) (rsm.SessionResponseStatus, rsm.SessionResponseContext, error) {
	request := &rsm.StorageRequest{
		PartitionID: uint32(s.partition.ID()),
		Request: &rsm.SessionRequest{
			Request: &rsm.SessionRequest_Command{
				Command: &rsm.SessionCommandRequest{
					Context: context,
					Command: rsm.ServiceCommandRequest{
						ServiceID: serviceID,
						Request: &rsm.ServiceCommandRequest_Delete{
							Delete: &rsm.ServiceDeleteRequest{},
						},
					},
				},
			},
		},
	}
	response, err := s.doRequest(ctx, request)
	if err != nil {
		return rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}
	return response.Response.Status, response.Response.GetCommand().Context, nil
}

// open creates the session and begins keep-alives
func (s *Session) open(ctx context.Context) error {
	responseStatus, responseContext, err := s.doOpenSession(ctx, &s.Timeout)
	if err != nil {
		return err
	}

	if responseStatus.Code != rsm.SessionResponseCode_OK {
		return rsm.GetErrorFromStatus(responseStatus)
	}

	s.mu.Lock()
	s.SessionID = responseContext.SessionID
	s.lastIndex = responseContext.Index
	s.mu.Unlock()

	go func() {
		for range s.ticker.C {
			go s.keepAlive(context.Background())
		}
	}()
	return nil
}

// doOpenSession opens a new session
func (s *Session) doOpenSession(ctx context.Context, timeout *time.Duration) (rsm.SessionResponseStatus, rsm.SessionResponseContext, error) {
	request := &rsm.StorageRequest{
		PartitionID: uint32(s.partition.ID()),
		Request: &rsm.SessionRequest{
			Request: &rsm.SessionRequest_OpenSession{
				OpenSession: &rsm.OpenSessionRequest{
					Timeout: timeout,
				},
			},
		},
	}
	response, err := s.doRequest(ctx, request)
	if err != nil {
		return rsm.SessionResponseStatus{}, rsm.SessionResponseContext{}, err
	}
	sessionID := response.Response.GetOpenSession().SessionID
	return response.Response.Status, rsm.SessionResponseContext{
		SessionID: sessionID,
		Index:     rsm.Index(sessionID),
	}, nil
}

// keepAlive keeps the session alive
func (s *Session) keepAlive(ctx context.Context) error {
	responseStatus, err := s.doKeepAliveSession(ctx)
	if err != nil {
		return err
	}
	if responseStatus.Code != rsm.SessionResponseCode_OK {
		return rsm.GetErrorFromStatus(responseStatus)
	}
	return nil
}

// doKeepAliveSession keeps a session alive
func (s *Session) doKeepAliveSession(ctx context.Context) (rsm.SessionResponseStatus, error) {
	s.mu.RLock()
	request := &rsm.StorageRequest{
		PartitionID: uint32(s.partition.ID()),
		Request: &rsm.SessionRequest{
			Request: &rsm.SessionRequest_KeepAlive{
				KeepAlive: &rsm.KeepAliveRequest{
					SessionID:     s.SessionID,
					AckResponseID: s.responseID,
				},
			},
		},
	}
	s.mu.RUnlock()

	response, err := s.doRequest(ctx, request)
	if err != nil {
		return rsm.SessionResponseStatus{}, err
	}
	return response.Response.Status, nil
}

// doRequest submits a storage request
func (s *Session) doRequest(ctx context.Context, request *rsm.StorageRequest) (*rsm.StorageResponse, error) {
	i := 1
	for {
		s.log.Debugf("Sending StorageRequest %+v", request)
		requestCtx, _ := context.WithTimeout(ctx, s.retryInterval)
		response, err := s.tryRequest(requestCtx, request)
		if err == nil {
			s.log.Debugf("Received StorageResponse %+v", response)
			switch response.Response.Status.Code {
			case rsm.SessionResponseCode_OK:
				return response, err
			case rsm.SessionResponseCode_NOT_LEADER:
				if response.Response.Status.Leader != "" {
					s.log.Debugf("Reconnecting to leader %s", response.Response.Status.Leader)
					s.reconnect(cluster.ReplicaID(response.Response.Status.Leader))
				} else {
					s.log.Debug("Failed to locate leader, retrying...")
					select {
					case <-time.After(10 * time.Millisecond * time.Duration(math.Min(math.Pow(2, float64(i)), 1000))):
						i++
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
			default:
				return response, rsm.GetErrorFromStatus(response.Response.Status)
			}
		} else if errors.IsTimeout(err) {
			s.log.Warnf("StorageRequest %+v timed out. Retrying...", request, err)
		} else if errors.IsCanceled(err) {
			return nil, err
		} else {
			s.log.Warnf("Sending StorageRequest %+v failed: %s", request, err)
			select {
			case <-time.After(10 * time.Millisecond * time.Duration(math.Min(math.Pow(2, float64(i)), 1000))):
				i++
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
}

// tryRequest submits a storage request
func (s *Session) tryRequest(ctx context.Context, request *rsm.StorageRequest) (*rsm.StorageResponse, error) {
	conn, err := s.connect()
	if err != nil {
		return nil, err
	}
	client := rsm.NewStorageServiceClient(conn)
	response, err := client.Request(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return response, nil
}

// doStream submits a streaming request to the service
func (s *Session) doStream(ctx context.Context, request *rsm.StorageRequest, stream streams.WriteStream, idempotent bool) error {
	go s.tryStream(ctx, request, stream, idempotent, 0, false)
	return nil
}

// tryStream submits a stream request to the service recursively
func (s *Session) tryStream(ctx context.Context, request *rsm.StorageRequest, stream streams.WriteStream, idempotent bool, attempts int, open bool) {
	conn, err := s.connect()
	if err == context.Canceled {
		stream.Error(err)
		stream.Close()
		return
	} else if err != nil {
		s.log.Warnf("StorageRequest %+v failed", request, err)
		if idempotent || !open {
			go s.retryStream(ctx, request, stream, idempotent, attempts, open)
		} else {
			stream.Error(errors.From(err))
			stream.Close()
		}
		return
	}
	client := rsm.NewStorageServiceClient(conn)

	s.log.Debugf("Sending StorageRequest %+v", request)
	responseStream, err := client.Stream(ctx, request)
	if err != nil {
		s.log.Warnf("Sending StorageRequest %+v failed: %s", request, err)
		stream.Error(err)
		stream.Close()
		return
	}

	for {
		response, err := responseStream.Recv()
		if err == io.EOF {
			stream.Close()
			return
		} else if err == context.Canceled {
			stream.Error(err)
			stream.Close()
			return
		} else if err != nil {
			s.log.Warnf("StorageRequest %+v failed", request, err)
			if idempotent || !open {
				go s.retryStream(ctx, request, stream, idempotent, attempts, open)
			} else {
				stream.Error(errors.From(err))
				stream.Close()
			}
			return
		} else {
			s.log.Debugf("Received StorageResponse %+v", response)
			switch response.Response.Status.Code {
			case rsm.SessionResponseCode_OK:
				stream.Value(response)
				switch response.Response.Type {
				case rsm.SessionResponseType_OPEN_STREAM:
					if !open {
						open = true
					}
				case rsm.SessionResponseType_CLOSE_STREAM:
					stream.Close()
					return
				}
			case rsm.SessionResponseCode_NOT_LEADER:
				if response.Response.Status.Leader != "" {
					s.log.Debugf("Reconnecting to leader %s", response.Response.Status.Leader)
					s.reconnect(cluster.ReplicaID(response.Response.Status.Leader))
				}
				go s.retryStream(ctx, request, stream, idempotent, attempts, open)
				return
			default:
				stream.Error(rsm.GetErrorFromStatus(response.Response.Status))
				stream.Close()
				return
			}
			attempts = 0
		}
	}
}

func (s *Session) retryStream(ctx context.Context, request *rsm.StorageRequest, stream streams.WriteStream, idempotent bool, attempts int, open bool) {
	attempts++
	select {
	case <-time.After(10 * time.Millisecond * time.Duration(math.Min(math.Pow(2, float64(attempts)), 1000))):
		s.tryStream(ctx, request, stream, idempotent, attempts, open)
	case <-ctx.Done():
		stream.Error(errors.NewTimeout(ctx.Err().Error()))
		stream.Close()
	}
}

// connect gets the connection to the service
func (s *Session) connect() (*grpc.ClientConn, error) {
	s.mu.RLock()
	conn := s.conn
	s.mu.RUnlock()
	if conn != nil {
		return conn, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	conn = s.conn
	if conn != nil {
		return conn, nil
	}

	if s.leader == nil {
		replicas := make([]*cluster.Replica, 0)
		for _, replica := range s.partition.Replicas() {
			replicas = append(replicas, replica)
		}
		s.leader = replicas[rand.Intn(len(replicas))]
		s.conn = nil
	}

	s.log.Infof("Connecting to partition %d replica %s", s.partition.ID(), s.leader.ID)
	conn, err := s.leader.Connect(context.Background(), cluster.WithDialOption(grpc.WithInsecure()))
	if err != nil {
		s.log.Warnf("Connecting to partition %d replica %s failed", s.partition.ID(), s.leader.ID, err)
		return nil, err
	}
	s.conn = conn
	return conn, nil
}

// reconnect the connection to the given leader
func (s *Session) reconnect(replica cluster.ReplicaID) {
	if replica == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leader.ID == replica {
		return
	}

	leader, ok := s.partition.Replica(replica)
	if !ok {
		return
	}

	s.leader = leader
	s.conn = nil
}

// disconnect closes the connections
func (s *Session) disconnect() error {
	s.mu.Lock()
	s.conn = nil
	s.mu.Unlock()
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
	responseStatus, err := s.doCloseSession(ctx)
	if err != nil {
		return err
	}

	if responseStatus.Code != rsm.SessionResponseCode_OK {
		return rsm.GetErrorFromStatus(responseStatus)
	}
	return nil
}

// doCloseSession closes a session
func (s *Session) doCloseSession(ctx context.Context) (rsm.SessionResponseStatus, error) {
	s.mu.RLock()
	request := &rsm.StorageRequest{
		PartitionID: uint32(s.partition.ID()),
		Request: &rsm.SessionRequest{
			Request: &rsm.SessionRequest_CloseSession{
				CloseSession: &rsm.CloseSessionRequest{
					SessionID: s.SessionID,
				},
			},
		},
	}
	s.mu.RUnlock()

	response, err := s.doRequest(ctx, request)
	if err != nil {
		return rsm.SessionResponseStatus{}, err
	}
	return response.Response.Status, nil
}

// getSessionID gets the header for the current state of the session
func (s *Session) getSessionID() rsm.SessionID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.SessionID
}

// getQueryContext gets the current read header
func (s *Session) getQueryContext(sync bool) rsm.SessionQueryContext {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return rsm.SessionQueryContext{
		SessionID:     s.SessionID,
		LastRequestID: s.responseID,
		LastIndex:     s.lastIndex,
		Sync:          sync,
	}
}

// nextCommandContext returns the next write context
func (s *Session) nextCommandContext() rsm.SessionCommandContext {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requestID = s.requestID + 1
	return rsm.SessionCommandContext{
		SessionID: s.SessionID,
		RequestID: s.requestID,
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
		SessionID: s.SessionID,
		RequestID: s.requestID,
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
		if requestContext.RequestID > s.responseID {
			s.responseID = requestContext.RequestID
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
		RequestID:     s.ID,
		AckResponseID: s.responseID,
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
