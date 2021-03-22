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

package coordinator

import (
	"context"
	coordinatorapi "github.com/atomix/api/go/atomix/management/coordinator"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
)

func NewServer(drivers *DriverRegistry, primitives *PrimitiveRegistry) *Server {
	return &Server{
		drivers:    drivers,
		primitives: primitives,
	}
}

type Server struct {
	drivers    *DriverRegistry
	primitives *PrimitiveRegistry
}

func (s *Server) AddDriver(ctx context.Context, request *coordinatorapi.AddDriverRequest) (*coordinatorapi.AddDriverResponse, error) {
	log.Debugf("Received AddDriverRequest %+v", request)
	if _, err := s.drivers.GetDriver(request.Driver.ID); err != nil {
		log.Warnf("AddDriverRequest %+v failed: %v", request, err)
		return nil, err
	}
	if err := s.drivers.AddDriver(request.Driver); err != nil {
		log.Warnf("AddDriverRequest %+v failed: %v", request, err)
		return nil, err
	}
	response := &coordinatorapi.AddDriverResponse{}
	log.Debugf("Sending AddDriverResponse %+v", response)
	return response, nil
}

func (s *Server) RemoveDriver(ctx context.Context, request *coordinatorapi.RemoveDriverRequest) (*coordinatorapi.RemoveDriverResponse, error) {
	log.Debugf("Received RemoveDriverRequest %+v", request)
	err := s.drivers.RemoveDriver(request.DriverID)
	if err != nil {
		log.Warnf("RemoveDriverRequest %+v failed: %v", request, err)
		return nil, err
	}
	response := &coordinatorapi.RemoveDriverResponse{}
	log.Debugf("Sending RemoveDriverResponse %+v", response)
	return response, nil
}

func (s *Server) AddPrimitive(ctx context.Context, request *coordinatorapi.AddPrimitiveRequest) (*coordinatorapi.AddPrimitiveResponse, error) {
	log.Debugf("Received AddPrimitiveRequest %+v", request)
	if _, err := s.drivers.GetDriver(request.Primitive.Driver); err != nil {
		log.Warnf("AddPrimitiveRequest %+v failed: %v", request, err)
		return nil, err
	}
	if err := s.primitives.AddPrimitive(request.Primitive); err != nil {
		log.Warnf("AddPrimitiveRequest %+v failed: %v", request, err)
		return nil, err
	}
	response := &coordinatorapi.AddPrimitiveResponse{}
	log.Debugf("Sending AddPrimitiveResponse %+v", response)
	return response, nil
}

func (s *Server) RemovePrimitive(ctx context.Context, request *coordinatorapi.RemovePrimitiveRequest) (*coordinatorapi.RemovePrimitiveResponse, error) {
	log.Debugf("Received RemovePrimitiveRequest %+v", request)
	err := s.primitives.RemovePrimitive(request.PrimitiveID)
	if err != nil {
		log.Warnf("RemovePrimitiveRequest %+v failed: %v", request, err)
		return nil, err
	}
	response := &coordinatorapi.RemovePrimitiveResponse{}
	log.Debugf("Sending RemovePrimitiveResponse %+v", response)
	return response, nil
}

func (s *Server) LookupPrimitive(ctx context.Context, request *primitiveapi.LookupPrimitiveRequest) (*primitiveapi.LookupPrimitiveResponse, error) {
	log.Debugf("Received LookupPrimitiveRequest %+v", request)
	primitive, err := s.primitives.GetPrimitive(coordinatorapi.PrimitiveId{
		Type:      request.PrimitiveID.Type,
		Namespace: request.PrimitiveID.Namespace,
		Name:      request.PrimitiveID.Name,
	})
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
		Primitive: primitiveapi.PrimitiveMeta{
			ID:   request.PrimitiveID,
			Host: driver.Host,
			Port: driver.Port,
		},
	}
	log.Debugf("Sending LookupPrimitiveResponse %+v", response)
	return response, nil
}

var _ coordinatorapi.PrimitiveManagementServiceServer = &Server{}
var _ coordinatorapi.DriverManagementServiceServer = &Server{}
var _ primitiveapi.PrimitiveDiscoveryServiceServer = &Server{}
