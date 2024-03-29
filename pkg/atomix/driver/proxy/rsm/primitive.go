// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package rsm

import (
	"context"
	primitiveapi "github.com/atomix/atomix-api/go/atomix/primitive"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/env"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	storage "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
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
	serviceInfo := storage.ServiceInfo{
		Type:      storage.ServiceType(request.Headers.PrimitiveID.Type),
		Namespace: s.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	err := async.IterAsync(len(partitions), func(i int) error {
		_, err := partitions[i].GetService(ctx, serviceInfo)
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
	serviceInfo := storage.ServiceInfo{
		Type:      storage.ServiceType(request.Headers.PrimitiveID.Type),
		Namespace: s.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	err := async.IterAsync(len(partitions), func(i int) error {
		session, ok := partitions[i].getService(serviceInfo)
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
