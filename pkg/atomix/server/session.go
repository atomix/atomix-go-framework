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
	"github.com/atomix/go-framework/pkg/atomix/node"
	"github.com/atomix/go-framework/pkg/atomix/service"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/golang/protobuf/proto"
	"time"
)

// SessionizedServer is a base server for servers that support sessions
type SessionizedServer struct {
	Protocol node.Protocol
	Type     string
}

// write sends a write to the service
func (s *SessionizedServer) write(ctx context.Context, partition node.Partition, request []byte, header *headers.RequestHeader) ([]byte, error) {
	serviceRequest := &service.ServiceRequest{
		Id: &service.ServiceId{
			Type:      s.Type,
			Name:      header.Name.Name,
			Namespace: header.Name.Namespace,
		},
		Request: &service.ServiceRequest_Command{
			Command: request,
		},
	}

	bytes, err := proto.Marshal(serviceRequest)
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

	// Decode and return the response
	serviceResponse := &service.ServiceResponse{}
	err = proto.Unmarshal(result.Value.([]byte), serviceResponse)
	if err != nil {
		return nil, err
	}
	return serviceResponse.GetCommand(), nil
}

// writeStream sends a streaming write to the service
func (s *SessionizedServer) writeStream(ctx context.Context, partition node.Partition, request []byte, header *headers.RequestHeader, stream streams.WriteStream) error {
	serviceRequest := &service.ServiceRequest{
		Id: &service.ServiceId{
			Type:      s.Type,
			Name:      header.Name.Name,
			Namespace: header.Name.Namespace,
		},
		Request: &service.ServiceRequest_Command{
			Command: request,
		},
	}

	bytes, err := proto.Marshal(serviceRequest)
	if err != nil {
		return err
	}

	// Create a goroutine to convert the results into raw form
	stream = streams.NewEncodingStream(stream, func(value interface{}) (interface{}, error) {
		serviceResponse := &service.ServiceResponse{}
		err := proto.Unmarshal(value.([]byte), serviceResponse)
		if err != nil {
			return nil, err
		}
		return serviceResponse.GetCommand(), nil
	})

	go partition.Write(ctx, bytes, stream)
	return nil
}

// read sends a read to the service
func (s *SessionizedServer) read(ctx context.Context, partition node.Partition, request []byte, header *headers.RequestHeader) ([]byte, error) {
	serviceRequest := &service.ServiceRequest{
		Id: &service.ServiceId{
			Type:      s.Type,
			Name:      header.Name.Name,
			Namespace: header.Name.Namespace,
		},
		Request: &service.ServiceRequest_Query{
			Query: request,
		},
	}

	bytes, err := proto.Marshal(serviceRequest)
	if err != nil {
		return nil, err
	}

	// Create a unary stream
	stream := streams.NewUnaryStream()

	// Read the request
	if err := partition.Read(ctx, bytes, stream); err != nil {
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

	serviceResponse := &service.ServiceResponse{}
	err = proto.Unmarshal(result.Value.([]byte), serviceResponse)
	if err != nil {
		return nil, err
	}
	return serviceResponse.GetQuery(), nil
}

// readStream sends a streaming read to the service
func (s *SessionizedServer) readStream(ctx context.Context, partition node.Partition, request []byte, header *headers.RequestHeader, stream streams.WriteStream) error {
	serviceRequest := &service.ServiceRequest{
		Id: &service.ServiceId{
			Type:      s.Type,
			Name:      header.Name.Name,
			Namespace: header.Name.Namespace,
		},
		Request: &service.ServiceRequest_Query{
			Query: request,
		},
	}

	bytes, err := proto.Marshal(serviceRequest)
	if err != nil {
		return err
	}

	// Convert the stream results into raw form
	stream = streams.NewDecodingStream(stream, func(value interface{}) (interface{}, error) {
		serviceResponse := &service.ServiceResponse{}
		err := proto.Unmarshal(value.([]byte), serviceResponse)
		if err != nil {
			return nil, err
		}
		return serviceResponse.GetQuery(), nil
	})

	go func() {
		_ = partition.Read(ctx, bytes, stream)
	}()
	return nil
}

// Command submits a command to the service
func (s *SessionizedServer) Command(ctx context.Context, name string, input []byte, header *headers.RequestHeader) ([]byte, *headers.ResponseHeader, error) {
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
				Name:  name,
				Input: input,
			},
		},
	}

	bytes, err := proto.Marshal(sessionRequest)
	if err != nil {
		return nil, nil, err
	}

	bytes, err = s.write(ctx, partition, bytes, header)
	if err != nil {
		return nil, nil, err
	}

	sessionResponse := &service.SessionResponse{}
	err = proto.Unmarshal(bytes, sessionResponse)
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
	return commandResponse.Output, responseHeader, nil
}

// CommandStream submits a streaming command to the service
func (s *SessionizedServer) CommandStream(ctx context.Context, name string, input []byte, header *headers.RequestHeader, stream streams.WriteStream) error {
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
				Name:  name,
				Input: input,
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
		return SessionOutput{
			Header: responseHeader,
			Result: streams.Result{
				Value: commandResponse.Output,
			},
		}, nil
	})
	return s.writeStream(ctx, partition, bytes, header, stream)
}

// Query submits a query to the service
func (s *SessionizedServer) Query(ctx context.Context, name string, input []byte, header *headers.RequestHeader) ([]byte, *headers.ResponseHeader, error) {
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
				Name:  name,
				Input: input,
			},
		},
	}

	bytes, err := proto.Marshal(sessionRequest)
	if err != nil {
		return nil, nil, err
	}

	bytes, err = s.read(ctx, partition, bytes, header)
	if err != nil {
		return nil, nil, err
	}

	sessionResponse := &service.SessionResponse{}
	err = proto.Unmarshal(bytes, sessionResponse)
	if err != nil {
		return nil, nil, err
	}

	queryResponse := sessionResponse.GetQuery()
	responseHeader := &headers.ResponseHeader{
		SessionID: header.SessionID,
		Index:     queryResponse.Context.Index,
	}
	return queryResponse.Output, responseHeader, nil
}

// QueryStream submits a streaming query to the service
func (s *SessionizedServer) QueryStream(ctx context.Context, name string, input []byte, header *headers.RequestHeader, stream streams.WriteStream) error {
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
				Name:  name,
				Input: input,
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
		return SessionOutput{
			Header: responseHeader,
			Result: streams.Result{
				Value: queryResponse.Output,
			},
		}, nil
	})
	return s.readStream(ctx, partition, bytes, header, stream)
}

// OpenSession opens a new session
func (s *SessionizedServer) OpenSession(ctx context.Context, header *headers.RequestHeader, timeout *time.Duration) (*headers.ResponseHeader, error) {
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

	bytes, err = s.write(ctx, partition, bytes, header)
	if err != nil {
		return nil, err
	}

	sessionResponse := &service.SessionResponse{}
	err = proto.Unmarshal(bytes, sessionResponse)
	if err != nil {
		return nil, err
	}

	sessionID := sessionResponse.GetOpenSession().SessionID
	return &headers.ResponseHeader{
		SessionID: sessionID,
		Index:     sessionID,
	}, nil
}

// KeepAliveSession keeps a session alive
func (s *SessionizedServer) KeepAliveSession(ctx context.Context, header *headers.RequestHeader) (*headers.ResponseHeader, error) {
	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(int(header.Partition))
	if partition.MustLeader() && !partition.IsLeader() {
		return &headers.ResponseHeader{
			Status: headers.ResponseStatus_NOT_LEADER,
			Leader: partition.Leader(),
		}, nil
	}

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

	bytes, err = s.write(ctx, partition, bytes, header)
	if err != nil {
		return nil, err
	}

	sessionResponse := &service.SessionResponse{}
	err = proto.Unmarshal(bytes, sessionResponse)
	if err != nil {
		return nil, err
	}
	return &headers.ResponseHeader{
		SessionID: header.SessionID,
	}, nil
}

// CloseSession closes a session
func (s *SessionizedServer) CloseSession(ctx context.Context, header *headers.RequestHeader) (*headers.ResponseHeader, error) {
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

	bytes, err = s.write(ctx, partition, bytes, header)
	if err != nil {
		return nil, err
	}

	sessionResponse := &service.SessionResponse{}
	err = proto.Unmarshal(bytes, sessionResponse)
	if err != nil {
		return nil, err
	}
	return &headers.ResponseHeader{
		SessionID: header.SessionID,
	}, nil
}

// Delete deletes the service
func (s *SessionizedServer) Delete(ctx context.Context, header *headers.RequestHeader) (*headers.ResponseHeader, error) {
	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(int(header.Partition))
	if partition.MustLeader() && !partition.IsLeader() {
		return &headers.ResponseHeader{
			Status: headers.ResponseStatus_NOT_LEADER,
			Leader: partition.Leader(),
		}, nil
	}

	serviceRequest := &service.ServiceRequest{
		Id: &service.ServiceId{
			Type:      s.Type,
			Name:      header.Name.Name,
			Namespace: header.Name.Namespace,
		},
		Request: &service.ServiceRequest_Delete{
			Delete: &service.DeleteRequest{},
		},
	}

	bytes, err := proto.Marshal(serviceRequest)
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

	// Decode and return the response
	serviceResponse := &service.ServiceResponse{}
	err = proto.Unmarshal(result.Value.([]byte), serviceResponse)
	if err != nil {
		return nil, err
	}
	return &headers.ResponseHeader{}, nil
}

// SessionOutput is a result for session-supporting servers containing session header information
type SessionOutput struct {
	streams.Result
	Header *headers.ResponseHeader
}
