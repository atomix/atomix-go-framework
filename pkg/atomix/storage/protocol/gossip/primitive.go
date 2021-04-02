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

package gossip

import (
	"context"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"google.golang.org/grpc"
)

func RegisterPrimitiveServer(server *grpc.Server) {
	primitiveapi.RegisterPrimitiveServiceServer(server, newPrimitiveServer())
}

func newPrimitiveServer() primitiveapi.PrimitiveServiceServer {
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
