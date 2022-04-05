// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package gossip

import (
	"context"
	primitiveapi "github.com/atomix/atomix-api/go/atomix/primitive"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"google.golang.org/grpc"
)

func RegisterPrimitiveServer(server *grpc.Server) {
	primitiveapi.RegisterPrimitiveServer(server, newPrimitiveServer())
}

func newPrimitiveServer() primitiveapi.PrimitiveServer {
	return &PrimitiveServer{
		log: logging.GetLogger("atomix", "primitive"),
	}
}

type PrimitiveServer struct {
	log logging.Logger
}

func (s *PrimitiveServer) Create(ctx context.Context, request *primitiveapi.CreateRequest) (*primitiveapi.CreateResponse, error) {
	s.log.Debugf("Received CreateRequest %+v", request)
	response := &primitiveapi.CreateResponse{}
	s.log.Debugf("Sending CreateResponse %+v", response)
	return response, nil
}

func (s *PrimitiveServer) Close(ctx context.Context, request *primitiveapi.CloseRequest) (*primitiveapi.CloseResponse, error) {
	s.log.Debugf("Received CloseRequest %+v", request)
	response := &primitiveapi.CloseResponse{}
	s.log.Debugf("Sending CloseResponse %+v", response)
	return response, nil
}

func (s *PrimitiveServer) Delete(ctx context.Context, request *primitiveapi.DeleteRequest) (*primitiveapi.DeleteResponse, error) {
	s.log.Debugf("Received DeleteRequest %+v", request)
	response := &primitiveapi.DeleteResponse{}
	s.log.Debugf("Sending DeleteResponse %+v", response)
	return response, nil
}
