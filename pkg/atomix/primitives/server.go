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

package primitives

import (
	"context"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	"github.com/atomix/go-framework/pkg/atomix/drivers"
	"github.com/atomix/go-framework/pkg/atomix/logging"
)

var log = logging.GetLogger("atomix", "primitives")

func NewServer(drivers *drivers.Registry, primitives *Registry) *Server {
	return &Server{
		drivers:    drivers,
		primitives: primitives,
	}
}

type Server struct {
	drivers    *drivers.Registry
	primitives *Registry
}

func (s *Server) GetPrimitive(ctx context.Context, request *primitiveapi.GetPrimitiveRequest) (*primitiveapi.GetPrimitiveResponse, error) {
	log.Debugf("Received GetPrimitiveRequest %+v", request)
	primitive, err := s.primitives.GetPrimitive(request.Name)
	if err != nil {
		log.Warnf("GetPrimitiveRequest %+v failed: %v", request, err)
		return nil, err
	}
	response := &primitiveapi.GetPrimitiveResponse{
		Primitive: primitive,
	}
	log.Debugf("Sending GetPrimitiveResponse %+v", response)
	return response, nil
}

func (s *Server) ListPrimitives(ctx context.Context, request *primitiveapi.ListPrimitivesRequest) (*primitiveapi.ListPrimitivesResponse, error) {
	log.Debugf("Received ListPrimitivesRequest %+v", request)
	primitives, err := s.primitives.ListPrimitives()
	if err != nil {
		log.Warnf("ListPrimitivesRequest %+v failed: %v", request, err)
		return nil, err
	}
	response := &primitiveapi.ListPrimitivesResponse{
		Primitives: primitives,
	}
	log.Debugf("Sending ListPrimitivesResponse %+v", response)
	return response, nil
}

func (s *Server) LookupPrimitive(ctx context.Context, request *primitiveapi.LookupPrimitiveRequest) (*primitiveapi.LookupPrimitiveResponse, error) {
	log.Debugf("Received LookupPrimitiveRequest %+v", request)
	primitive, err := s.primitives.GetPrimitive(request.Name)
	if err != nil {
		log.Warnf("LookupPrimitiveRequest %+v failed: %v", request, err)
		return nil, err
	}
	driver, err := s.drivers.GetDriver(primitive.Driver)
	if err != nil {
		log.Warnf("LookupPrimitiveRequest %+v failed: %v", request, err)
		return nil, err
	}
	response := &primitiveapi.LookupPrimitiveResponse{
		Proxy: driver.Proxy,
	}
	log.Debugf("Sending LookupPrimitiveResponse %+v", response)
	return response, nil
}

func (s *Server) AddPrimitive(ctx context.Context, request *primitiveapi.AddPrimitiveRequest) (*primitiveapi.AddPrimitiveResponse, error) {
	log.Debugf("Received AddPrimitiveRequest %+v", request)
	if _, err := s.drivers.GetDriver(request.Primitive.Driver); err != nil {
		log.Warnf("AddPrimitiveRequest %+v failed: %v", request, err)
		return nil, err
	}
	if err := s.primitives.AddPrimitive(request.Primitive); err != nil {
		log.Warnf("AddPrimitiveRequest %+v failed: %v", request, err)
		return nil, err
	}
	response := &primitiveapi.AddPrimitiveResponse{}
	log.Debugf("Sending AddPrimitiveResponse %+v", response)
	return response, nil
}

func (s *Server) RemovePrimitive(ctx context.Context, request *primitiveapi.RemovePrimitiveRequest) (*primitiveapi.RemovePrimitiveResponse, error) {
	log.Debugf("Received RemovePrimitiveRequest %+v", request)
	err := s.primitives.RemovePrimitive(request.Name)
	if err != nil {
		log.Warnf("RemovePrimitiveRequest %+v failed: %v", request, err)
		return nil, err
	}
	response := &primitiveapi.RemovePrimitiveResponse{}
	log.Debugf("Sending RemovePrimitiveResponse %+v", response)
	return response, nil
}

var _ primitiveapi.PrimitiveRegistryServiceServer = &Server{}
var _ primitiveapi.PrimitiveManagementServiceServer = &Server{}
