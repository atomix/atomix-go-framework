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
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
	streams "github.com/atomix/atomix-go-framework/pkg/atomix/stream"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util/retry"
	"github.com/bits-and-blooms/bloom/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const chanBufSize = 1000

// The false positive rate for request/response filters
const fpRate float64 = 0.05

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
	timeout time.Duration
}

// NewSession creates a new Session for the given partition
// name is the name of the primitive
// handler is the primitive's session handler
func NewSession(partition cluster.Partition, opts ...SessionOption) *Session {
	options := &sessionOptions{
		timeout: time.Minute,
	}
	for i := range opts {
		opts[i].prepare(options)
	}
	return &Session{
		partition: partition,
		Timeout:   options.timeout,
		services:  make(map[rsm.ServiceInfo]*Service),
	}
}

// Session maintains the session for a primitive
type Session struct {
	partition  cluster.Partition
	Timeout    time.Duration
	sessionID  rsm.SessionID
	lastIndex  *sessionIndex
	requestID  *sessionRequestID
	requestCh  chan sessionRequestEvent
	conn       *grpc.ClientConn
	client     rsm.PartitionServiceClient
	services   map[rsm.ServiceInfo]*Service
	servicesMu sync.RWMutex
}

func (s *Session) GetService(ctx context.Context, serviceInfo rsm.ServiceInfo) (*Service, error) {
	if service, ok := s.getService(serviceInfo); ok {
		return service, nil
	}

	s.servicesMu.Lock()
	defer s.servicesMu.Unlock()

	service, ok := s.services[serviceInfo]
	if ok {
		return service, nil
	}

	service = newService(s, serviceInfo)
	if err := service.open(ctx); err != nil {
		return nil, err
	}
	s.services[serviceInfo] = service
	return service, nil
}

func (s *Session) getService(serviceID rsm.ServiceInfo) (*Service, bool) {
	s.servicesMu.RLock()
	defer s.servicesMu.RUnlock()
	service, ok := s.services[serviceID]
	return service, ok
}

// doCommand submits a command to the service
func (s *Session) doCommand(ctx context.Context, serviceID rsm.ServiceID, operationID rsm.OperationID, input []byte) ([]byte, error) {
	requestID := s.requestID.Next()
	request := &rsm.PartitionCommandRequest{
		PartitionID: rsm.PartitionID(s.partition.ID()),
		Request: rsm.CommandRequest{
			Request: &rsm.CommandRequest_SessionCommand{
				SessionCommand: &rsm.SessionCommandRequest{
					SessionID: s.sessionID,
					Request: &rsm.SessionCommandRequest_ServiceCommand{
						ServiceCommand: &rsm.ServiceCommandRequest{
							ServiceID: serviceID,
							RequestID: requestID,
							Operation: &rsm.OperationRequest{
								OperationID: operationID,
								Value:       input,
							},
						},
					},
				},
			},
		},
	}

	s.requestCh <- sessionRequestEvent{
		eventType: sessionRequestEventStart,
		requestID: requestID,
	}

	response, err := s.client.Command(ctx, request, retry.WithRetryOn(codes.Unavailable, codes.Unknown))
	if err != nil {
		return nil, errors.From(err)
	}

	s.lastIndex.Update(response.Response.Index)

	s.requestCh <- sessionRequestEvent{
		eventType: sessionRequestEventEnd,
		requestID: requestID,
	}

	result := response.Response.GetSessionCommand().GetServiceCommand().Operation
	if result.Status.Code != rsm.ResponseCode_OK {
		return nil, rsm.GetErrorFromStatus(result.Status)
	}
	return result.Value, nil
}

// doCommandStream submits a streaming command to the service
func (s *Session) doCommandStream(ctx context.Context, serviceID rsm.ServiceID, operationID rsm.OperationID, input []byte, stream streams.WriteStream) error {
	requestID := s.requestID.Next()
	request := &rsm.PartitionCommandRequest{
		PartitionID: rsm.PartitionID(s.partition.ID()),
		Request: rsm.CommandRequest{
			Request: &rsm.CommandRequest_SessionCommand{
				SessionCommand: &rsm.SessionCommandRequest{
					SessionID: s.sessionID,
					Request: &rsm.SessionCommandRequest_ServiceCommand{
						ServiceCommand: &rsm.ServiceCommandRequest{
							ServiceID: serviceID,
							RequestID: requestID,
							Operation: &rsm.OperationRequest{
								OperationID: operationID,
								Value:       input,
							},
						},
					},
				},
			},
		},
	}

	s.requestCh <- sessionRequestEvent{
		eventType: sessionRequestEventStart,
		requestID: requestID,
	}

	responseStream, err := s.client.CommandStream(ctx, request, retry.WithRetryOn(codes.Unavailable, codes.Unknown))
	if err != nil {
		return errors.From(err)
	}

	go func() {
		defer stream.Close()
		var lastResponseID rsm.ResponseID
		for {
			response, err := responseStream.Recv()
			if err == io.EOF {
				break
			} else if err != nil {
				stream.Error(errors.From(err))
				break
			}

			s.lastIndex.Update(response.Response.Index)

			responseID := response.Response.GetSessionCommand().GetServiceCommand().ResponseID
			if responseID == lastResponseID+1 {
				result := response.Response.GetSessionCommand().GetServiceCommand().Operation
				if result.Status.Code != rsm.ResponseCode_OK {
					stream.Error(rsm.GetErrorFromStatus(result.Status))
				} else {
					stream.Value(result.Value)
				}
				s.requestCh <- sessionRequestEvent{
					eventType:  sessionRequestEventReceive,
					requestID:  requestID,
					responseID: responseID,
				}
				lastResponseID = responseID
			}
		}
	}()
	return nil
}

// doQuery submits a query to the service
func (s *Session) doQuery(ctx context.Context, serviceID rsm.ServiceID, operationID rsm.OperationID, input []byte, sync bool) ([]byte, error) {
	request := &rsm.PartitionQueryRequest{
		PartitionID: rsm.PartitionID(s.partition.ID()),
		Sync:        sync,
		Request: rsm.QueryRequest{
			LastIndex: s.lastIndex.Get(),
			Request: &rsm.QueryRequest_SessionQuery{
				SessionQuery: &rsm.SessionQueryRequest{
					SessionID: s.sessionID,
					Request: &rsm.SessionQueryRequest_ServiceQuery{
						ServiceQuery: &rsm.ServiceQueryRequest{
							ServiceID: serviceID,
							Operation: &rsm.OperationRequest{
								OperationID: operationID,
								Value:       input,
							},
						},
					},
				},
			},
		},
	}

	response, err := s.client.Query(ctx, request, retry.WithRetryOn(codes.Unavailable, codes.Unknown))
	if err != nil {
		return nil, errors.From(err)
	}

	result := response.Response.GetSessionQuery().GetServiceQuery().Operation
	if result.Status.Code != rsm.ResponseCode_OK {
		return nil, rsm.GetErrorFromStatus(result.Status)
	}
	return result.Value, nil
}

// doQueryStream submits a streaming query to the service
func (s *Session) doQueryStream(ctx context.Context, serviceID rsm.ServiceID, operationID rsm.OperationID, input []byte, stream streams.WriteStream, sync bool) error {
	request := &rsm.PartitionQueryRequest{
		PartitionID: rsm.PartitionID(s.partition.ID()),
		Sync:        sync,
		Request: rsm.QueryRequest{
			LastIndex: s.lastIndex.Get(),
			Request: &rsm.QueryRequest_SessionQuery{
				SessionQuery: &rsm.SessionQueryRequest{
					SessionID: s.sessionID,
					Request: &rsm.SessionQueryRequest_ServiceQuery{
						ServiceQuery: &rsm.ServiceQueryRequest{
							ServiceID: serviceID,
							Operation: &rsm.OperationRequest{
								OperationID: operationID,
								Value:       input,
							},
						},
					},
				},
			},
		},
	}

	responseStream, err := s.client.QueryStream(ctx, request, retry.WithRetryOn(codes.Unavailable, codes.Unknown))
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

			result := response.Response.GetSessionQuery().GetServiceQuery().Operation
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
	conn, err := s.partition.Connect(ctx,
		cluster.WithDialScheme(resolverName),
		cluster.WithDialOption(grpc.WithInsecure()),
		cluster.WithDialOption(grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"rsm"}`)),
		cluster.WithDialOption(grpc.WithResolvers(newResolver(s.partition))),
		cluster.WithDialOption(grpc.WithUnaryInterceptor(retry.RetryingUnaryClientInterceptor(retry.WithRetryOn(codes.Unavailable, codes.Unknown)))),
		cluster.WithDialOption(grpc.WithStreamInterceptor(retry.RetryingStreamClientInterceptor(retry.WithRetryOn(codes.Unavailable, codes.Unknown)))))
	if err != nil {
		return err
	}
	s.conn = conn
	s.client = rsm.NewPartitionServiceClient(s.conn)

	request := &rsm.PartitionCommandRequest{
		PartitionID: rsm.PartitionID(s.partition.ID()),
		Request: rsm.CommandRequest{
			Request: &rsm.CommandRequest_OpenSession{
				OpenSession: &rsm.OpenSessionRequest{
					Timeout: s.Timeout,
				},
			},
		},
	}

	response, err := s.client.Command(ctx, request)
	if err != nil {
		return errors.From(err)
	}

	s.sessionID = response.Response.GetOpenSession().SessionID

	s.lastIndex = &sessionIndex{}
	s.lastIndex.Update(response.Response.Index)

	s.requestID = &sessionRequestID{}

	s.requestCh = make(chan sessionRequestEvent, chanBufSize)
	go func() {
		ticker := time.NewTicker(s.Timeout / 4)
		var requestID rsm.RequestID
		requests := make(map[rsm.RequestID]*sessionStream)
		for {
			select {
			case requestEvent := <-s.requestCh:
				switch requestEvent.eventType {
				case sessionRequestEventStart:
					for requestID < requestEvent.requestID {
						requestID++
						requests[requestID] = &sessionStream{
							nextResponseID: 1,
						}
					}
				case sessionRequestEventReceive:
					request, ok := requests[requestEvent.requestID]
					if ok {
						if requestEvent.responseID == request.nextResponseID {
							request.nextResponseID = requestEvent.responseID + 1
						}
					}
				case sessionRequestEventEnd:
					delete(requests, requestEvent.requestID)
				}
			case <-ticker.C:
				requestFilter := bloom.NewWithEstimates(uint(len(requests)), fpRate)
				responseFilter := bloom.NewWithEstimates(uint(len(requests)), fpRate)
				for requestID, stream := range requests {
					requestBytes := make([]byte, 8)
					binary.BigEndian.PutUint64(requestBytes, uint64(requestID))
					requestFilter.Add(requestBytes)
					responseBytes := make([]byte, 8)
					binary.BigEndian.PutUint64(responseBytes, uint64(stream.nextResponseID))
					responseFilter.Add(responseBytes)
				}
				go s.keepAliveSessions(context.Background(), requestID, requestFilter, responseFilter)
			}
		}
	}()
	return nil
}

func (s *Session) keepAliveSessions(ctx context.Context, requestID rsm.RequestID, requestFilter *bloom.BloomFilter, responseFilter *bloom.BloomFilter) error {
	requestFilterBytes, err := json.Marshal(requestFilter)
	if err != nil {
		return err
	}

	responseFilterBytes, err := json.Marshal(responseFilter)
	if err != nil {
		return err
	}

	request := &rsm.PartitionCommandRequest{
		PartitionID: rsm.PartitionID(s.partition.ID()),
		Request: rsm.CommandRequest{
			Request: &rsm.CommandRequest_KeepAlive{
				KeepAlive: &rsm.KeepAliveRequest{
					SessionID:      s.sessionID,
					LastRequestID:  requestID,
					RequestFilter:  requestFilterBytes,
					ResponseFilter: responseFilterBytes,
				},
			},
		},
	}

	response, err := s.client.Command(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	s.lastIndex.Update(response.Response.Index)
	return nil
}

func (s *Session) close(ctx context.Context) error {
	request := &rsm.PartitionCommandRequest{
		PartitionID: rsm.PartitionID(s.partition.ID()),
		Request: rsm.CommandRequest{
			Request: &rsm.CommandRequest_CloseSession{
				CloseSession: &rsm.CloseSessionRequest{
					SessionID: s.sessionID,
				},
			},
		},
	}

	_, err := s.client.Command(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

type sessionIndex struct {
	value uint64
}

func (i *sessionIndex) Update(index rsm.Index) {
	update := uint64(index)
	for {
		current := atomic.LoadUint64(&i.value)
		if current < update {
			updated := atomic.CompareAndSwapUint64(&i.value, current, update)
			if updated {
				break
			}
		} else {
			break
		}
	}
}

func (i *sessionIndex) Get() rsm.Index {
	value := atomic.LoadUint64(&i.value)
	return rsm.Index(value)
}

type sessionRequestID struct {
	value uint64
}

func (i *sessionRequestID) Next() rsm.RequestID {
	value := atomic.AddUint64(&i.value, 1)
	return rsm.RequestID(value)
}

type sessionRequestEventType int

const (
	sessionRequestEventStart sessionRequestEventType = iota
	sessionRequestEventReceive
	sessionRequestEventEnd
)

type sessionRequestEvent struct {
	requestID  rsm.RequestID
	responseID rsm.ResponseID
	eventType  sessionRequestEventType
}

type sessionStream struct {
	nextResponseID rsm.ResponseID
}
