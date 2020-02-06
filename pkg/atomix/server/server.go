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

package server

import (
	"context"
	"errors"
	"github.com/atomix/api/proto/atomix/headers"
	"github.com/atomix/api/proto/atomix/primitive"
	"github.com/atomix/go-framework/pkg/atomix/node"
	"github.com/atomix/go-framework/pkg/atomix/service"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/golang/protobuf/proto"
	"time"
)

// Server is a base server for servers that support sessions
type Server struct {
	Protocol node.Protocol
	Type     service.ServiceType
}

// DoCommand submits a command to the service
func (s *Server) DoCommand(ctx context.Context, name string, input []byte, header *headers.RequestHeader) ([]byte, *headers.ResponseHeader, error) {
	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(int(header.Partition))
	if partition.MustLeader() && !partition.IsLeader() {
		return nil, &headers.ResponseHeader{
			Status: headers.ResponseStatus_NOT_LEADER,
			Leader: partition.Leader(),
		}, nil
	}

	sessionRequest := &service.SessionRequest{
		Request: &service.SessionRequest_Command{
			Command: &service.SessionCommandRequest{
				Context: &service.SessionCommandContext{
					SessionID:      header.SessionID,
					SequenceNumber: header.RequestID,
				},
				Command: &service.ServiceCommandRequest{
					Service: &service.ServiceId{
						Type:      s.Type,
						Name:      header.Name.Name,
						Namespace: header.Name.Namespace,
					},
					Request: &service.ServiceCommandRequest_Operation{
						Operation: &service.ServiceOperationRequest{
							Method: name,
							Value:  input,
						},
					},
				},
			},
		},
	}

	bytes, err := proto.Marshal(sessionRequest)
	if err != nil {
		return nil, nil, err
	}

	// Create a unary stream
	stream := streams.NewUnaryStream()

	// Write the request
	if err := partition.Write(ctx, bytes, stream); err != nil {
		return nil, nil, err
	}

	// Wait for the result
	result, ok := stream.Receive()
	if !ok {
		return nil, nil, errors.New("write channel closed")
	}

	// If the result failed, return the error
	if result.Failed() {
		return nil, nil, result.Error
	}

	sessionResponse := &service.SessionResponse{}
	err = proto.Unmarshal(result.Value.([]byte), sessionResponse)
	if err != nil {
		return nil, nil, err
	}

	commandResponse := sessionResponse.GetCommand()
	responseHeader := &headers.ResponseHeader{
		SessionID:  header.SessionID,
		StreamID:   commandResponse.Context.StreamID,
		ResponseID: commandResponse.Context.Sequence,
		Index:      commandResponse.Context.Index,
	}
	return commandResponse.Response.GetOperation().Result, responseHeader, nil
}

// DoCommandStream submits a streaming command to the service
func (s *Server) DoCommandStream(ctx context.Context, name string, input []byte, header *headers.RequestHeader, stream streams.WriteStream) error {
	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(int(header.Partition))
	if partition.MustLeader() && !partition.IsLeader() {
		stream.Value(SessionOutput{
			Header: &headers.ResponseHeader{
				Status: headers.ResponseStatus_NOT_LEADER,
				Leader: partition.Leader(),
			},
		})
		stream.Close()
		return nil
	}

	sessionRequest := &service.SessionRequest{
		Request: &service.SessionRequest_Command{
			Command: &service.SessionCommandRequest{
				Context: &service.SessionCommandContext{
					SessionID:      header.SessionID,
					SequenceNumber: header.RequestID,
				},
				Command: &service.ServiceCommandRequest{
					Service: &service.ServiceId{
						Type:      s.Type,
						Name:      header.Name.Name,
						Namespace: header.Name.Namespace,
					},
					Request: &service.ServiceCommandRequest_Operation{
						Operation: &service.ServiceOperationRequest{
							Method: name,
							Value:  input,
						},
					},
				},
			},
		},
	}

	bytes, err := proto.Marshal(sessionRequest)
	if err != nil {
		return err
	}

	stream = streams.NewEncodingStream(stream, func(value interface{}) (interface{}, error) {
		sessionResponse := &service.SessionResponse{}
		err = proto.Unmarshal(value.([]byte), sessionResponse)
		if err != nil {
			return SessionOutput{
				Result: streams.Result{
					Error: err,
				},
			}, nil
		}
		commandResponse := sessionResponse.GetCommand()
		responseHeader := &headers.ResponseHeader{
			SessionID:  header.SessionID,
			StreamID:   commandResponse.Context.StreamID,
			ResponseID: commandResponse.Context.Sequence,
			Index:      commandResponse.Context.Index,
			Type:       headers.ResponseType(commandResponse.Context.Type),
		}
		var result []byte
		if commandResponse.Response != nil {
			result = commandResponse.Response.GetOperation().Result
		}
		return SessionOutput{
			Header: responseHeader,
			Result: streams.Result{
				Value: result,
			},
		}, nil
	})

	go func() {
		_ = partition.Write(ctx, bytes, stream)
	}()
	return nil
}

// DoQuery submits a query to the service
func (s *Server) DoQuery(ctx context.Context, name string, input []byte, header *headers.RequestHeader) ([]byte, *headers.ResponseHeader, error) {
	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(int(header.Partition))
	if partition.MustLeader() && !partition.IsLeader() {
		return nil, &headers.ResponseHeader{
			Status: headers.ResponseStatus_NOT_LEADER,
			Leader: partition.Leader(),
		}, nil
	}

	sessionRequest := &service.SessionRequest{
		Request: &service.SessionRequest_Query{
			Query: &service.SessionQueryRequest{
				Context: &service.SessionQueryContext{
					SessionID:          header.SessionID,
					LastIndex:          header.Index,
					LastSequenceNumber: header.RequestID,
				},
				Query: &service.ServiceQueryRequest{
					Service: &service.ServiceId{
						Type:      s.Type,
						Name:      header.Name.Name,
						Namespace: header.Name.Namespace,
					},
					Request: &service.ServiceQueryRequest_Operation{
						Operation: &service.ServiceOperationRequest{
							Method: name,
							Value:  input,
						},
					},
				},
			},
		},
	}

	bytes, err := proto.Marshal(sessionRequest)
	if err != nil {
		return nil, nil, err
	}

	// Create a unary stream
	stream := streams.NewUnaryStream()

	// Read the request
	if err := partition.Read(ctx, bytes, stream); err != nil {
		return nil, nil, err
	}

	// Wait for the result
	result, ok := stream.Receive()
	if !ok {
		return nil, nil, errors.New("write channel closed")
	}

	// If the result failed, return the error
	if result.Failed() {
		return nil, nil, result.Error
	}

	sessionResponse := &service.SessionResponse{}
	err = proto.Unmarshal(result.Value.([]byte), sessionResponse)
	if err != nil {
		return nil, nil, err
	}

	queryResponse := sessionResponse.GetQuery()
	responseHeader := &headers.ResponseHeader{
		SessionID: header.SessionID,
		Index:     queryResponse.Context.Index,
	}
	return queryResponse.Response.GetOperation().Result, responseHeader, nil
}

// DoQueryStream submits a streaming query to the service
func (s *Server) DoQueryStream(ctx context.Context, name string, input []byte, header *headers.RequestHeader, stream streams.WriteStream) error {
	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(int(header.Partition))
	if partition.MustLeader() && !partition.IsLeader() {
		stream.Value(SessionOutput{
			Header: &headers.ResponseHeader{
				Status: headers.ResponseStatus_NOT_LEADER,
				Leader: partition.Leader(),
			},
		})
		stream.Close()
		return nil
	}

	sessionRequest := &service.SessionRequest{
		Request: &service.SessionRequest_Query{
			Query: &service.SessionQueryRequest{
				Context: &service.SessionQueryContext{
					SessionID:          header.SessionID,
					LastIndex:          header.Index,
					LastSequenceNumber: header.RequestID,
				},
				Query: &service.ServiceQueryRequest{
					Service: &service.ServiceId{
						Type:      s.Type,
						Name:      header.Name.Name,
						Namespace: header.Name.Namespace,
					},
					Request: &service.ServiceQueryRequest_Operation{
						Operation: &service.ServiceOperationRequest{
							Method: name,
							Value:  input,
						},
					},
				},
			},
		},
	}

	bytes, err := proto.Marshal(sessionRequest)
	if err != nil {
		return err
	}

	stream = streams.NewDecodingStream(stream, func(value interface{}) (interface{}, error) {
		sessionResponse := &service.SessionResponse{}
		if err := proto.Unmarshal(value.([]byte), sessionResponse); err != nil {
			return nil, err
		}
		queryResponse := sessionResponse.GetQuery()
		responseHeader := &headers.ResponseHeader{
			SessionID: header.SessionID,
			Index:     queryResponse.Context.Index,
			Type:      headers.ResponseType(queryResponse.Context.Type),
		}
		var result []byte
		if queryResponse.Response != nil {
			result = queryResponse.Response.GetOperation().Result
		}
		return SessionOutput{
			Header: responseHeader,
			Result: streams.Result{
				Value: result,
			},
		}, nil
	})
	go func() {
		_ = partition.Read(ctx, bytes, stream)
	}()
	return nil
}

// DoMetadata submits a metadata query to the service
func (s *Server) DoMetadata(ctx context.Context, serviceType primitive.PrimitiveType, namespace string, header *headers.RequestHeader) ([]*service.ServiceId, *headers.ResponseHeader, error) {
	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(int(header.Partition))
	if partition.MustLeader() && !partition.IsLeader() {
		return nil, &headers.ResponseHeader{
			Status: headers.ResponseStatus_NOT_LEADER,
			Leader: partition.Leader(),
		}, nil
	}

	sessionRequest := &service.SessionRequest{
		Request: &service.SessionRequest_Query{
			Query: &service.SessionQueryRequest{
				Context: &service.SessionQueryContext{
					SessionID:          header.SessionID,
					LastIndex:          header.Index,
					LastSequenceNumber: header.RequestID,
				},
				Query: &service.ServiceQueryRequest{
					Service: &service.ServiceId{
						Type:      s.Type,
						Name:      header.Name.Name,
						Namespace: header.Name.Namespace,
					},
					Request: &service.ServiceQueryRequest_Metadata{
						Metadata: &service.ServiceMetadataRequest{
							Type:      service.ServiceType(serviceType),
							Namespace: namespace,
						},
					},
				},
			},
		},
	}

	bytes, err := proto.Marshal(sessionRequest)
	if err != nil {
		return nil, nil, err
	}

	// Create a unary stream
	stream := streams.NewUnaryStream()

	// Read the request
	if err := partition.Read(ctx, bytes, stream); err != nil {
		return nil, nil, err
	}

	// Wait for the result
	result, ok := stream.Receive()
	if !ok {
		return nil, nil, errors.New("read channel closed")
	}

	// If the result failed, return the error
	if result.Failed() {
		return nil, nil, result.Error
	}

	sessionResponse := &service.SessionResponse{}
	err = proto.Unmarshal(result.Value.([]byte), sessionResponse)
	if err != nil {
		return nil, nil, err
	}

	queryResponse := sessionResponse.GetQuery()
	responseHeader := &headers.ResponseHeader{
		SessionID: header.SessionID,
		Index:     queryResponse.Context.Index,
	}
	return queryResponse.Response.GetMetadata().Services, responseHeader, nil
}

// DoOpenSession opens a new session
func (s *Server) DoOpenSession(ctx context.Context, header *headers.RequestHeader, timeout *time.Duration) (*headers.ResponseHeader, error) {
	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(int(header.Partition))
	if partition.MustLeader() && !partition.IsLeader() {
		return &headers.ResponseHeader{
			Status: headers.ResponseStatus_NOT_LEADER,
			Leader: partition.Leader(),
		}, nil
	}

	sessionRequest := &service.SessionRequest{
		Request: &service.SessionRequest_OpenSession{
			OpenSession: &service.OpenSessionRequest{
				Timeout: timeout,
			},
		},
	}

	bytes, err := proto.Marshal(sessionRequest)
	if err != nil {
		return nil, err
	}

	// Create a unary stream
	stream := streams.NewUnaryStream()

	// Write the request
	if err := partition.Write(ctx, bytes, stream); err != nil {
		return nil, err
	}

	// Wait for the result
	result, ok := stream.Receive()
	if !ok {
		return nil, errors.New("write channel closed")
	}

	// If the result failed, return the error
	if result.Failed() {
		return nil, result.Error
	}

	sessionResponse := &service.SessionResponse{}
	err = proto.Unmarshal(result.Value.([]byte), sessionResponse)
	if err != nil {
		return nil, err
	}

	sessionID := sessionResponse.GetOpenSession().SessionID
	return &headers.ResponseHeader{
		SessionID: sessionID,
		Index:     sessionID,
	}, nil
}

// DoKeepAliveSession keeps a session alive
func (s *Server) DoKeepAliveSession(ctx context.Context, header *headers.RequestHeader) (*headers.ResponseHeader, error) {
	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(int(header.Partition))
	if partition.MustLeader() && !partition.IsLeader() {
		return &headers.ResponseHeader{
			Status: headers.ResponseStatus_NOT_LEADER,
			Leader: partition.Leader(),
		}, nil
	}

	// Create a unary stream
	stream := streams.NewUnaryStream()

	streams := make(map[uint64]uint64)
	for _, stream := range header.Streams {
		streams[stream.StreamID] = stream.ResponseID
	}

	sessionRequest := &service.SessionRequest{
		Request: &service.SessionRequest_KeepAlive{
			KeepAlive: &service.KeepAliveRequest{
				SessionID:       header.SessionID,
				CommandSequence: header.RequestID,
				Streams:         streams,
			},
		},
	}

	bytes, err := proto.Marshal(sessionRequest)
	if err != nil {
		return nil, err
	}

	// Write the request
	if err := partition.Write(ctx, bytes, stream); err != nil {
		return nil, err
	}

	// Wait for the result
	result, ok := stream.Receive()
	if !ok {
		return nil, errors.New("write channel closed")
	}

	// If the result failed, return the error
	if result.Failed() {
		return nil, result.Error
	}

	sessionResponse := &service.SessionResponse{}
	err = proto.Unmarshal(result.Value.([]byte), sessionResponse)
	if err != nil {
		return nil, err
	}
	return &headers.ResponseHeader{
		SessionID: header.SessionID,
	}, nil
}

// DoCloseSession closes a session
func (s *Server) DoCloseSession(ctx context.Context, header *headers.RequestHeader) (*headers.ResponseHeader, error) {
	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(int(header.Partition))
	if partition.MustLeader() && !partition.IsLeader() {
		return &headers.ResponseHeader{
			Status: headers.ResponseStatus_NOT_LEADER,
			Leader: partition.Leader(),
		}, nil
	}

	sessionRequest := &service.SessionRequest{
		Request: &service.SessionRequest_CloseSession{
			CloseSession: &service.CloseSessionRequest{
				SessionID: header.SessionID,
			},
		},
	}

	bytes, err := proto.Marshal(sessionRequest)
	if err != nil {
		return nil, err
	}

	// Create a unary stream
	stream := streams.NewUnaryStream()

	// Write the request
	if err := partition.Write(ctx, bytes, stream); err != nil {
		return nil, err
	}

	// Wait for the result
	result, ok := stream.Receive()
	if !ok {
		return nil, errors.New("write channel closed")
	}

	// If the result failed, return the error
	if result.Failed() {
		return nil, result.Error
	}

	sessionResponse := &service.SessionResponse{}
	err = proto.Unmarshal(result.Value.([]byte), sessionResponse)
	if err != nil {
		return nil, err
	}
	return &headers.ResponseHeader{
		SessionID: header.SessionID,
	}, nil
}

// DoCreateService creates the service
func (s *Server) DoCreateService(ctx context.Context, header *headers.RequestHeader) (*headers.ResponseHeader, error) {
	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(int(header.Partition))
	if partition.MustLeader() && !partition.IsLeader() {
		return &headers.ResponseHeader{
			Status: headers.ResponseStatus_NOT_LEADER,
			Leader: partition.Leader(),
		}, nil
	}

	sessionRequest := &service.SessionRequest{
		Request: &service.SessionRequest_Command{
			Command: &service.SessionCommandRequest{
				Context: &service.SessionCommandContext{
					SessionID:      header.SessionID,
					SequenceNumber: header.RequestID,
				},
				Command: &service.ServiceCommandRequest{
					Service: &service.ServiceId{
						Type:      s.Type,
						Name:      header.Name.Name,
						Namespace: header.Name.Namespace,
					},
					Request: &service.ServiceCommandRequest_Create{
						Create: &service.ServiceCreateRequest{},
					},
				},
			},
		},
	}

	bytes, err := proto.Marshal(sessionRequest)
	if err != nil {
		return nil, err
	}

	// Create a unary stream
	stream := streams.NewUnaryStream()

	// Write the request
	if err := partition.Write(ctx, bytes, stream); err != nil {
		return nil, err
	}

	// Wait for the result
	result, ok := stream.Receive()
	if !ok {
		return nil, errors.New("write channel closed")
	}

	// If the result failed, return the error
	if result.Failed() {
		return nil, result.Error
	}

	sessionResponse := &service.SessionResponse{}
	err = proto.Unmarshal(result.Value.([]byte), sessionResponse)
	if err != nil {
		return nil, err
	}

	commandResponse := sessionResponse.GetCommand()
	responseHeader := &headers.ResponseHeader{
		SessionID:  header.SessionID,
		StreamID:   commandResponse.Context.StreamID,
		ResponseID: commandResponse.Context.Sequence,
		Index:      commandResponse.Context.Index,
	}
	return responseHeader, nil
}

// DoCloseService closes the service
func (s *Server) DoCloseService(ctx context.Context, header *headers.RequestHeader) (*headers.ResponseHeader, error) {
	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(int(header.Partition))
	if partition.MustLeader() && !partition.IsLeader() {
		return &headers.ResponseHeader{
			Status: headers.ResponseStatus_NOT_LEADER,
			Leader: partition.Leader(),
		}, nil
	}

	sessionRequest := &service.SessionRequest{
		Request: &service.SessionRequest_Command{
			Command: &service.SessionCommandRequest{
				Context: &service.SessionCommandContext{
					SessionID:      header.SessionID,
					SequenceNumber: header.RequestID,
				},
				Command: &service.ServiceCommandRequest{
					Service: &service.ServiceId{
						Type:      s.Type,
						Name:      header.Name.Name,
						Namespace: header.Name.Namespace,
					},
					Request: &service.ServiceCommandRequest_Close{
						Close: &service.ServiceCloseRequest{},
					},
				},
			},
		},
	}

	bytes, err := proto.Marshal(sessionRequest)
	if err != nil {
		return nil, err
	}

	// Create a unary stream
	stream := streams.NewUnaryStream()

	// Write the request
	if err := partition.Write(ctx, bytes, stream); err != nil {
		return nil, err
	}

	// Wait for the result
	result, ok := stream.Receive()
	if !ok {
		return nil, errors.New("write channel closed")
	}

	// If the result failed, return the error
	if result.Failed() {
		return nil, result.Error
	}

	sessionResponse := &service.SessionResponse{}
	err = proto.Unmarshal(result.Value.([]byte), sessionResponse)
	if err != nil {
		return nil, err
	}

	commandResponse := sessionResponse.GetCommand()
	responseHeader := &headers.ResponseHeader{
		SessionID:  header.SessionID,
		StreamID:   commandResponse.Context.StreamID,
		ResponseID: commandResponse.Context.Sequence,
		Index:      commandResponse.Context.Index,
	}
	return responseHeader, nil
}

// DoDeleteService deletes the service
func (s *Server) DoDeleteService(ctx context.Context, header *headers.RequestHeader) (*headers.ResponseHeader, error) {
	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(int(header.Partition))
	if partition.MustLeader() && !partition.IsLeader() {
		return &headers.ResponseHeader{
			Status: headers.ResponseStatus_NOT_LEADER,
			Leader: partition.Leader(),
		}, nil
	}

	sessionRequest := &service.SessionRequest{
		Request: &service.SessionRequest_Command{
			Command: &service.SessionCommandRequest{
				Context: &service.SessionCommandContext{
					SessionID:      header.SessionID,
					SequenceNumber: header.RequestID,
				},
				Command: &service.ServiceCommandRequest{
					Service: &service.ServiceId{
						Type:      s.Type,
						Name:      header.Name.Name,
						Namespace: header.Name.Namespace,
					},
					Request: &service.ServiceCommandRequest_Delete{
						Delete: &service.ServiceDeleteRequest{},
					},
				},
			},
		},
	}

	bytes, err := proto.Marshal(sessionRequest)
	if err != nil {
		return nil, err
	}

	// Create a unary stream
	stream := streams.NewUnaryStream()

	// Write the request
	if err := partition.Write(ctx, bytes, stream); err != nil {
		return nil, err
	}

	// Wait for the result
	result, ok := stream.Receive()
	if !ok {
		return nil, errors.New("write channel closed")
	}

	// If the result failed, return the error
	if result.Failed() {
		return nil, result.Error
	}

	sessionResponse := &service.SessionResponse{}
	err = proto.Unmarshal(result.Value.([]byte), sessionResponse)
	if err != nil {
		return nil, err
	}

	commandResponse := sessionResponse.GetCommand()
	responseHeader := &headers.ResponseHeader{
		SessionID:  header.SessionID,
		StreamID:   commandResponse.Context.StreamID,
		ResponseID: commandResponse.Context.Sequence,
		Index:      commandResponse.Context.Index,
	}
	return responseHeader, nil
}

// SessionOutput is a result for session-supporting servers containing session header information
type SessionOutput struct {
	streams.Result
	Header *headers.ResponseHeader
}
