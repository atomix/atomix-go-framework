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
	"encoding/binary"
	"encoding/json"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm4"
	streams "github.com/atomix/atomix-go-framework/pkg/atomix/stream"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util/retry"
	"github.com/bits-and-blooms/bloom/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"io"
	"sync"
	"sync/atomic"
)

// NewSession creates a new Session for the given partition
// name is the name of the primitive
// handler is the primitive's session handler
func NewSession(client *Client, serviceID rsm.ServiceID) *Session {
	return &Session{
		client:          client,
		serviceID:       serviceID,
		pendingRequests: make(map[rsm.RequestID]bool),
		responseStreams: make(map[rsm.RequestID]*responseStream),
	}
}

// Session maintains the session for a primitive
type Session struct {
	client          *Client
	sessionID       rsm.SessionID
	serviceID       rsm.ServiceID
	lastIndex       rsm.Index
	indexMu         sync.RWMutex
	requestID       uint64
	responseID      uint64
	pendingRequests map[rsm.RequestID]bool
	responseStreams map[rsm.RequestID]*responseStream
	stateMu         sync.RWMutex
	conn            *grpc.ClientConn
	mu              sync.RWMutex
}

// DoCommand submits a command to the service
func (s *Session) DoCommand(ctx context.Context, operationID rsm.OperationID, input []byte) ([]byte, error) {
	requestID := rsm.RequestID(atomic.AddUint64(&s.requestID, 1))
	client := rsm.NewPartitionServiceClient(s.conn)
	request := &rsm.PartitionCommandRequest{
		Request: rsm.StateMachineCommandRequest{
			Request: &rsm.StateMachineCommandRequest_ClientCommand{
				ClientCommand: &rsm.ClientCommandRequest{
					ClientID: s.client.clientID,
					Request: &rsm.ClientCommandRequest_SessionCommand{
						SessionCommand: &rsm.SessionCommandRequest{
							SessionID: s.sessionID,
							RequestID: requestID,
							Request: &rsm.SessionCommandRequest_Operation{
								Operation: &rsm.OperationRequest{
									OperationID: operationID,
									Value:       input,
								},
							},
						},
					},
				},
			},
		},
	}

	s.stateMu.Lock()
	s.pendingRequests[requestID] = true
	s.stateMu.Unlock()

	response, err := client.Command(ctx, request, retry.WithRetryOn(codes.Unavailable, codes.Unknown))
	if err != nil {
		return nil, errors.From(err)
	}

	s.stateMu.Lock()
	delete(s.pendingRequests, requestID)
	s.stateMu.Unlock()

	s.indexMu.RLock()
	if response.Response.Index > s.lastIndex {
		s.indexMu.RUnlock()
		s.indexMu.Lock()
		if response.Response.Index > s.lastIndex {
			s.lastIndex = response.Response.Index
		}
		s.indexMu.Unlock()
	} else {
		s.indexMu.RUnlock()
	}

	result := response.Response.GetClientCommand().GetSessionCommand().GetOperation()
	if result.Status.Code != rsm.ResponseCode_OK {
		return nil, rsm.GetErrorFromStatus(result.Status)
	}
	return result.Value, nil
}

// DoCommandStream submits a streaming command to the service
func (s *Session) DoCommandStream(ctx context.Context, operationID rsm.OperationID, input []byte, stream streams.WriteStream) error {
	requestID := rsm.RequestID(atomic.AddUint64(&s.requestID, 1))
	request := &rsm.PartitionCommandRequest{
		Request: rsm.StateMachineCommandRequest{
			Request: &rsm.StateMachineCommandRequest_ClientCommand{
				ClientCommand: &rsm.ClientCommandRequest{
					ClientID: s.client.clientID,
					Request: &rsm.ClientCommandRequest_SessionCommand{
						SessionCommand: &rsm.SessionCommandRequest{
							SessionID: s.sessionID,
							RequestID: requestID,
							Request: &rsm.SessionCommandRequest_Operation{
								Operation: &rsm.OperationRequest{
									OperationID: operationID,
									Value:       input,
								},
							},
						},
					},
				},
			},
		},
	}

	streamState := &responseStream{}
	s.stateMu.Lock()
	s.pendingRequests[requestID] = true
	s.responseStreams[requestID] = streamState
	s.stateMu.Unlock()

	client := rsm.NewPartitionServiceClient(s.conn)
	responseStream, err := client.CommandStream(ctx, request, retry.WithRetryOn(codes.Unavailable, codes.Unknown))
	if err != nil {
		return errors.From(err)
	}

	go func() {
		defer stream.Close()
		defer func() {
			s.stateMu.Lock()
			delete(s.pendingRequests, requestID)
			delete(s.responseStreams, requestID)
			s.stateMu.Unlock()
		}()

		var lastResponseID uint64
		for {
			response, err := responseStream.Recv()
			if err == io.EOF {
				break
			} else if err != nil {
				stream.Error(errors.From(err))
				break
			}

			responseID := uint64(response.Response.GetClientCommand().GetSessionCommand().ResponseID)
			if responseID == lastResponseID+1 {
				lastResponseID = responseID
				atomic.StoreUint64(&streamState.responseID, lastResponseID)
				result := response.Response.GetClientCommand().GetSessionCommand().GetOperation()
				if result.Status.Code != rsm.ResponseCode_OK {
					stream.Error(rsm.GetErrorFromStatus(result.Status))
				} else {
					stream.Value(result.Value)
				}
			}
		}
	}()
	return nil
}

// DoQuery submits a query to the service
func (s *Session) DoQuery(ctx context.Context, operationID rsm.OperationID, input []byte) ([]byte, error) {
	lastRequestID := rsm.RequestID(atomic.LoadUint64(&s.requestID))

	s.indexMu.RLock()
	lastIndex := s.lastIndex
	s.indexMu.RUnlock()

	client := rsm.NewPartitionServiceClient(s.conn)
	request := &rsm.PartitionQueryRequest{
		Request: rsm.StateMachineQueryRequest{
			SyncIndex: lastIndex,
			Request: &rsm.StateMachineQueryRequest_ClientQuery{
				ClientQuery: &rsm.ClientQueryRequest{
					ClientID: s.client.clientID,
					Request: &rsm.ClientQueryRequest_SessionQuery{
						SessionQuery: &rsm.SessionQueryRequest{
							SessionID:     s.sessionID,
							LastRequestID: lastRequestID,
							Request: &rsm.SessionQueryRequest_Operation{
								Operation: &rsm.OperationRequest{
									OperationID: operationID,
									Value:       input,
								},
							},
						},
					},
				},
			},
		},
	}

	response, err := client.Query(ctx, request, retry.WithRetryOn(codes.Unavailable, codes.Unknown))
	if err != nil {
		return nil, errors.From(err)
	}

	result := response.Response.GetClientQuery().GetSessionQuery().GetOperation()
	if result.Status.Code != rsm.ResponseCode_OK {
		return nil, rsm.GetErrorFromStatus(result.Status)
	}
	return result.Value, nil
}

// DoQueryStream submits a streaming query to the service
func (s *Session) DoQueryStream(ctx context.Context, operationID rsm.OperationID, input []byte, stream streams.WriteStream) error {
	lastRequestID := rsm.RequestID(atomic.LoadUint64(&s.requestID))

	s.indexMu.RLock()
	lastIndex := s.lastIndex
	s.indexMu.RUnlock()

	request := &rsm.PartitionQueryRequest{
		Request: rsm.StateMachineQueryRequest{
			SyncIndex: lastIndex,
			Request: &rsm.StateMachineQueryRequest_ClientQuery{
				ClientQuery: &rsm.ClientQueryRequest{
					ClientID: s.client.clientID,
					Request: &rsm.ClientQueryRequest_SessionQuery{
						SessionQuery: &rsm.SessionQueryRequest{
							SessionID:     s.sessionID,
							LastRequestID: lastRequestID,
							Request: &rsm.SessionQueryRequest_Operation{
								Operation: &rsm.OperationRequest{
									OperationID: operationID,
									Value:       input,
								},
							},
						},
					},
				},
			},
		},
	}

	client := rsm.NewPartitionServiceClient(s.conn)
	responseStream, err := client.QueryStream(ctx, request, retry.WithRetryOn(codes.Unavailable, codes.Unknown))
	if err != nil {
		return errors.From(err)
	}

	go func() {
		defer stream.Close()
		for {
			response, err := responseStream.Recv()
			if err == io.EOF {
				break
			} else if err != nil {
				stream.Error(errors.From(err))
				break
			}

			result := response.Response.GetClientQuery().GetSessionQuery().GetOperation()
			if result.Status.Code != rsm.ResponseCode_OK {
				stream.Error(rsm.GetErrorFromStatus(result.Status))
			} else {
				stream.Value(result.Value)
			}
		}
	}()
	return nil
}

func (s *Session) open(ctx context.Context) error {
	client := rsm.NewPartitionServiceClient(s.conn)
	request := &rsm.PartitionCommandRequest{
		Request: rsm.StateMachineCommandRequest{
			Request: &rsm.StateMachineCommandRequest_ClientCommand{
				ClientCommand: &rsm.ClientCommandRequest{
					ClientID: s.client.clientID,
					Request: &rsm.ClientCommandRequest_SessionOpen{
						SessionOpen: &rsm.SessionOpenRequest{
							ServiceID: s.serviceID,
						},
					},
				},
			},
		},
	}
	response, err := client.Command(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	s.sessionID = response.Response.GetClientCommand().GetSessionOpen().SessionID
	s.lastIndex = rsm.Index(s.sessionID)
	return nil
}

func (s *Session) close(ctx context.Context) error {
	client := rsm.NewPartitionServiceClient(s.conn)
	request := &rsm.PartitionCommandRequest{
		Request: rsm.StateMachineCommandRequest{
			Request: &rsm.StateMachineCommandRequest_ClientCommand{
				ClientCommand: &rsm.ClientCommandRequest{
					ClientID: s.client.clientID,
					Request: &rsm.ClientCommandRequest_SessionClose{
						SessionClose: &rsm.SessionCloseRequest{
							SessionID: s.sessionID,
						},
					},
				},
			},
		},
	}
	response, err := client.Command(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	s.sessionID = response.Response.GetClientCommand().GetSessionOpen().SessionID
	return nil
}

func (s *Session) getState() rsm.SessionState {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	streamStates := make([]rsm.StreamState, 0, len(s.responseStreams))
	for requestID, responseStream := range s.responseStreams {
		streamState := rsm.StreamState{
			RequestID:          requestID,
			CompleteResponseID: rsm.ResponseID(atomic.LoadUint64(&responseStream.responseID)),
		}
		streamStates = append(streamStates, streamState)
	}

	requestFilter := bloom.NewWithEstimates(uint(len(s.pendingRequests)), 0.1)
	for requestID := range s.pendingRequests {
		bytes := make([]byte, 8)
		binary.BigEndian.PutUint64(bytes, uint64(requestID))
		requestFilter.Add(bytes)
	}

	requestFilterBytes, err := json.Marshal(requestFilter)
	if err != nil {
		panic(err)
	}

	return rsm.SessionState{
		SessionID:       s.sessionID,
		PendingRequests: requestFilterBytes,
		ResponseStreams: streamStates,
	}
}

type responseStream struct {
	responseID uint64
}
