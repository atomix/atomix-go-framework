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

package rsm

import (
	"context"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	"github.com/atomix/go-framework/pkg/atomix/driver/env"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	storage "github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm"
	"github.com/atomix/go-framework/pkg/atomix/util/async"
	"google.golang.org/grpc"
)

func RegisterPrimitiveServer(server *grpc.Server, client *Client, env env.DriverEnv) {
	primitiveapi.RegisterPrimitiveServer(server, newPrimitiveServer(client, env))
}

func newPrimitiveServer(client *Client, env env.DriverEnv) primitiveapi.PrimitiveServer {
	return &PrimitiveServer{
		Client: client,
		env:    env,
		log:    logging.GetLogger("atomix", "primitive"),
	}
}

type PrimitiveServer struct {
	*Client
	env env.DriverEnv
	log logging.Logger
}

func (s *PrimitiveServer) Create(ctx context.Context, request *primitiveapi.CreateRequest) (*primitiveapi.CreateResponse, error) {
	s.log.Debugf("Received CreateRequest %+v", request)
	if request.Headers.PrimitiveID.Namespace == "" {
		request.Headers.PrimitiveID.Namespace = s.env.Namespace
	}
	partitions := s.Partitions()
	service := storage.ServiceId{
		Type:      request.Headers.PrimitiveID.Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	err := async.IterAsync(len(partitions), func(i int) error {
		return partitions[i].DoCreateService(ctx, service)
	})
	if err != nil {
		s.log.Errorf("Request CreateRequest failed: %v", err)
		return nil, err
	}
	response := &primitiveapi.CreateResponse{}
	s.log.Debugf("Sending CreateResponse %+v", response)
	return response, nil
}

func (s *PrimitiveServer) Close(ctx context.Context, request *primitiveapi.CloseRequest) (*primitiveapi.CloseResponse, error) {
	s.log.Debugf("Received CloseRequest %+v", request)
	if request.Headers.PrimitiveID.Namespace == "" {
		request.Headers.PrimitiveID.Namespace = s.env.Namespace
	}
	partitions := s.Partitions()
	service := storage.ServiceId{
		Type:      request.Headers.PrimitiveID.Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	err := async.IterAsync(len(partitions), func(i int) error {
		return partitions[i].DoCloseService(ctx, service)
	})
	if err != nil {
		s.log.Errorf("Request CloseRequest failed: %v", err)
		return nil, err
	}
	response := &primitiveapi.CloseResponse{}
	s.log.Debugf("Sending CloseResponse %+v", response)
	return response, nil
}

func (s *PrimitiveServer) Delete(ctx context.Context, request *primitiveapi.DeleteRequest) (*primitiveapi.DeleteResponse, error) {
	s.log.Debugf("Received DeleteRequest %+v", request)
	if request.Headers.PrimitiveID.Namespace == "" {
		request.Headers.PrimitiveID.Namespace = s.env.Namespace
	}
	partitions := s.Partitions()
	service := storage.ServiceId{
		Type:      request.Headers.PrimitiveID.Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	err := async.IterAsync(len(partitions), func(i int) error {
		return partitions[i].DoDeleteService(ctx, service)
	})
	if err != nil {
		s.log.Errorf("Request DeleteRequest failed: %v", err)
		return nil, err
	}
	response := &primitiveapi.DeleteResponse{}
	s.log.Debugf("Sending DeleteResponse %+v", response)
	return response, nil
}
