// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package gossip

import (
	"context"
	primitiveapi "github.com/atomix/atomix-api/go/atomix/primitive"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/env"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util/async"
	"google.golang.org/grpc"
)

func RegisterPrimitiveServer(server *grpc.Server, client *Client, env env.DriverEnv) {
	primitiveapi.RegisterPrimitiveServer(server, newPrimitiveServer(client, env))
}

func newPrimitiveServer(client *Client, env env.DriverEnv) primitiveapi.PrimitiveServer {
	return &PrimitiveServer{
		Server: NewServer(client, GossipConfig{}),
		env:    env,
		log:    logging.GetLogger("atomix", "primitive"),
	}
}

type PrimitiveServer struct {
	*Server
	env env.DriverEnv
	log logging.Logger
}

func (s *PrimitiveServer) Create(ctx context.Context, request *primitiveapi.CreateRequest) (*primitiveapi.CreateResponse, error) {
	s.log.Debugf("Received CreateRequest %+v", request)
	if request.Headers.PrimitiveID.Namespace == "" {
		request.Headers.PrimitiveID.Namespace = s.env.Namespace
	}
	partitions := s.Partitions()
	responses, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
		conn, err := partitions[i].Connect()
		if err != nil {
			s.log.Errorf("Request CreateRequest failed: %v", err)
			return nil, err
		}
		client := primitiveapi.NewPrimitiveClient(conn)
		return client.Create(ctx, request)
	})
	if err != nil {
		s.log.Errorf("Request CreateRequest failed: %v", err)
		return nil, err
	}
	response := responses[0].(*primitiveapi.CreateResponse)
	s.log.Debugf("Sending CreateResponse %+v", response)
	return response, nil
}

func (s *PrimitiveServer) Close(ctx context.Context, request *primitiveapi.CloseRequest) (*primitiveapi.CloseResponse, error) {
	s.log.Debugf("Received CloseRequest %+v", request)
	if request.Headers.PrimitiveID.Namespace == "" {
		request.Headers.PrimitiveID.Namespace = s.env.Namespace
	}
	partitions := s.Partitions()
	responses, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
		conn, err := partitions[i].Connect()
		if err != nil {
			s.log.Errorf("Request CloseRequest failed: %v", err)
			return nil, err
		}
		client := primitiveapi.NewPrimitiveClient(conn)
		return client.Close(ctx, request)
	})
	if err != nil {
		s.log.Errorf("Request CloseRequest failed: %v", err)
		return nil, err
	}
	response := responses[0].(*primitiveapi.CloseResponse)
	s.log.Debugf("Sending CloseResponse %+v", response)
	return response, nil
}

func (s *PrimitiveServer) Delete(ctx context.Context, request *primitiveapi.DeleteRequest) (*primitiveapi.DeleteResponse, error) {
	s.log.Debugf("Received DeleteRequest %+v", request)
	if request.Headers.PrimitiveID.Namespace == "" {
		request.Headers.PrimitiveID.Namespace = s.env.Namespace
	}
	partitions := s.Partitions()
	responses, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
		conn, err := partitions[i].Connect()
		if err != nil {
			s.log.Errorf("Request DeleteRequest failed: %v", err)
			return nil, err
		}
		client := primitiveapi.NewPrimitiveClient(conn)
		return client.Delete(ctx, request)
	})
	if err != nil {
		s.log.Errorf("Request DeleteRequest failed: %v", err)
		return nil, err
	}
	response := responses[0].(*primitiveapi.DeleteResponse)
	s.log.Debugf("Sending DeleteResponse %+v", response)
	return response, nil
}
