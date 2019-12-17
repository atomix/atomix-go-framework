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
	"github.com/atomix/atomix-api/proto/atomix/headers"
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	streams "github.com/atomix/atomix-go-node/pkg/atomix/stream"
	"github.com/gogo/protobuf/proto"
)

// SimpleServer is a base server for servers that do not support sessions
type SimpleServer struct {
	Client node.Client
	Type   string
}

// Command submits a command to the service
func (s *SimpleServer) Command(ctx context.Context, name string, input []byte, header *headers.RequestHeader) ([]byte, *headers.ResponseHeader, error) {
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

	bytes, err = s.Write(ctx, bytes, header)
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

// Write sends a write to the service
func (s *SimpleServer) Write(ctx context.Context, request []byte, header *headers.RequestHeader) ([]byte, error) {
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

	// Create a write channel
	ch := make(chan streams.Result)

	// Write the request
	if err := s.Client.Write(ctx, bytes, streams.NewChannelStream(ch)); err != nil {
		return nil, err
	}

	// Wait for the result
	result, ok := <-ch
	if !ok {
		return nil, errors.New("write channel closed")
	}

	// If the result failed, return the error
	if result.Failed() {
		return nil, result.Error
	}

	// Decode and return the response
	serviceResponse := &service.ServiceResponse{}
	err = proto.Unmarshal(result.Value, serviceResponse)
	if err != nil {
		return nil, err
	}
	return serviceResponse.GetCommand(), nil
}

// Query submits a query to the service
func (s *SimpleServer) Query(ctx context.Context, name string, input []byte, header *headers.RequestHeader) ([]byte, *headers.ResponseHeader, error) {
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

	bytes, err = s.Read(ctx, bytes, header)
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

// Read sends a read to the service
func (s *SimpleServer) Read(ctx context.Context, request []byte, header *headers.RequestHeader) ([]byte, error) {
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

	// Create a read channel
	ch := make(chan streams.Result)

	// Read the request
	if err := s.Client.Read(ctx, bytes, streams.NewChannelStream(ch)); err != nil {
		return nil, err
	}

	// Wait for the result
	result, ok := <-ch
	if !ok {
		return nil, errors.New("write channel closed")
	}

	// If the result failed, return the error
	if result.Failed() {
		return nil, result.Error
	}

	serviceResponse := &service.ServiceResponse{}
	err = proto.Unmarshal(result.Value, serviceResponse)
	if err != nil {
		return nil, err
	}
	return serviceResponse.GetQuery(), nil
}

// Open opens a simple session
func (s *SimpleServer) Open(ctx context.Context, header *headers.RequestHeader) error {
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
		return err
	}

	// Create a write channel
	ch := make(chan streams.Result)

	// Write the request
	if err := s.Client.Write(ctx, bytes, streams.NewChannelStream(ch)); err != nil {
		return err
	}

	// Wait for the result
	result, ok := <-ch
	if !ok {
		return errors.New("write channel closed")
	}

	// If the result failed, return the error
	if result.Failed() {
		return result.Error
	}

	// Decode and return the response
	serviceResponse := &service.ServiceResponse{}
	err = proto.Unmarshal(result.Value, serviceResponse)
	if err != nil {
		return err
	}
	return nil
}

// Delete deletes the service
func (s *SimpleServer) Delete(ctx context.Context, header *headers.RequestHeader) error {
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
		return err
	}

	// Create a write channel
	ch := make(chan streams.Result)

	// Write the request
	if err := s.Client.Write(ctx, bytes, streams.NewChannelStream(ch)); err != nil {
		return err
	}

	// Wait for the result
	result, ok := <-ch
	if !ok {
		return errors.New("write channel closed")
	}

	// If the result failed, return the error
	if result.Failed() {
		return result.Error
	}

	// Decode and return the response
	serviceResponse := &service.ServiceResponse{}
	err = proto.Unmarshal(result.Value, serviceResponse)
	if err != nil {
		return err
	}
	return nil
}
