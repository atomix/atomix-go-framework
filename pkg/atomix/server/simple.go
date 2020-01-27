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
	"github.com/gogo/protobuf/proto"
)

// SimpleServer is a base server for servers that do not support sessions
type SimpleServer struct {
	Protocol node.Protocol
	Type     string
}

// Command submits a command to the service
func (s *SimpleServer) Command(ctx context.Context, name string, input []byte, header *headers.RequestHeader) ([]byte, *headers.ResponseHeader, error) {
	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(int(header.Partition))
	if partition.MustLeader() && !partition.IsLeader() {
		return nil, &headers.ResponseHeader{
			Status: headers.ResponseStatus_NOT_LEADER,
			Leader: partition.Leader(),
		}, nil
	}

	commandRequest := &service.CommandRequest{
		Context: &service.RequestContext{
			Index: header.Index,
		},
		Name:    name,
		Command: input,
	}

	bytes, err := proto.Marshal(commandRequest)
	if err != nil {
		return nil, nil, err
	}

	bytes, err = s.write(ctx, partition, bytes, header)
	if err != nil {
		return nil, nil, err
	}

	commandResponse := &service.CommandResponse{}
	err = proto.Unmarshal(bytes, commandResponse)
	if err != nil {
		return nil, nil, err
	}

	responseHeader := &headers.ResponseHeader{
		SessionID: header.SessionID,
		Index:     commandResponse.Context.Index,
	}
	return commandResponse.Output, responseHeader, nil
}

// write sends a write to the service
func (s *SimpleServer) write(ctx context.Context, partition node.Partition, request []byte, header *headers.RequestHeader) ([]byte, error) {
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

// Query submits a query to the service
func (s *SimpleServer) Query(ctx context.Context, name string, input []byte, header *headers.RequestHeader) ([]byte, *headers.ResponseHeader, error) {
	// If the client requires a leader and is not the leader, return an error
	partition := s.Protocol.Partition(int(header.Partition))
	if partition.MustLeader() && !partition.IsLeader() {
		return nil, &headers.ResponseHeader{
			Status: headers.ResponseStatus_NOT_LEADER,
			Leader: partition.Leader(),
		}, nil
	}

	queryRequest := &service.QueryRequest{
		Context: &service.RequestContext{
			Index: header.Index,
		},
		Name:  name,
		Query: input,
	}

	bytes, err := proto.Marshal(queryRequest)
	if err != nil {
		return nil, nil, err
	}

	bytes, err = s.read(ctx, partition, bytes, header)
	if err != nil {
		return nil, nil, err
	}

	queryResponse := &service.QueryResponse{}
	err = proto.Unmarshal(bytes, queryResponse)
	if err != nil {
		return nil, nil, err
	}

	responseHeader := &headers.ResponseHeader{
		SessionID: header.SessionID,
		Index:     queryResponse.Context.Index,
	}
	return queryResponse.Output, responseHeader, nil
}

// read sends a read to the service
func (s *SimpleServer) read(ctx context.Context, partition node.Partition, request []byte, header *headers.RequestHeader) ([]byte, error) {
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

// Open opens a simple session
func (s *SimpleServer) Open(ctx context.Context, header *headers.RequestHeader) (*headers.ResponseHeader, error) {
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
		Request: &service.ServiceRequest_Create{
			Create: &service.CreateRequest{},
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

// Delete deletes the service
func (s *SimpleServer) Delete(ctx context.Context, header *headers.RequestHeader) (*headers.ResponseHeader, error) {
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
