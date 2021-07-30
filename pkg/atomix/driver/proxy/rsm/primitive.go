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
	primitiveapi "github.com/atomix/atomix-api/go/atomix/primitive"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/env"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util/async"
	"google.golang.org/grpc"
)

func RegisterPrimitiveServer(server *grpc.Server, client *Client, env env.DriverEnv) {
	primitiveapi.RegisterPrimitiveServer(server, newPrimitiveServer(client, env))
}

func newPrimitiveServer(client *Client, env env.DriverEnv) primitiveapi.PrimitiveServer {
	return &PrimitiveServer{
		Client: client,
		env:    env,
	}
}

type PrimitiveServer struct {
	*Client
	env env.DriverEnv
}

func (s *PrimitiveServer) Create(ctx context.Context, request *primitiveapi.CreateRequest) (*primitiveapi.CreateResponse, error) {
	log.Debugf("Received CreateRequest %+v", request)
	if request.Headers.PrimitiveID.Namespace == "" {
		request.Headers.PrimitiveID.Namespace = s.env.Namespace
	}
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		_, err := partitions[i].GetService(ctx, request.Headers.PrimitiveID)
		return err
	})
	if err != nil {
		log.Errorf("Request CreateRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	response := &primitiveapi.CreateResponse{}
	log.Debugf("Sending CreateResponse %+v", response)
	return response, nil
}

func (s *PrimitiveServer) Close(ctx context.Context, request *primitiveapi.CloseRequest) (*primitiveapi.CloseResponse, error) {
	log.Debugf("Received CloseRequest %+v", request)
	if request.Headers.PrimitiveID.Namespace == "" {
		request.Headers.PrimitiveID.Namespace = s.env.Namespace
	}
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		session, ok := partitions[i].getService(request.Headers.PrimitiveID)
		if !ok {
			return nil
		}
		return session.close(ctx)
	})
	if err != nil {
		log.Errorf("Request CloseRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	response := &primitiveapi.CloseResponse{}
	log.Debugf("Sending CloseResponse %+v", response)
	return response, nil
}

func (s *PrimitiveServer) Delete(ctx context.Context, request *primitiveapi.DeleteRequest) (*primitiveapi.DeleteResponse, error) {
	log.Debugf("Received DeleteRequest %+v", request)
	err := errors.NewNotSupported("Delete not supported")
	log.Errorf("Request DeleteRequest failed: %v", err)
	return nil, errors.Proto(err)
}
