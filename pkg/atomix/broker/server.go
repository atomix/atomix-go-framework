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

package broker

import (
	"context"
	"fmt"
	brokerapi "github.com/atomix/api/go/atomix/management/broker"
	driverapi "github.com/atomix/api/go/atomix/management/driver"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"google.golang.org/grpc"
	"sync"
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
	mu         sync.RWMutex
}

func (s *Server) AddDriver(ctx context.Context, request *brokerapi.AddDriverRequest) (*brokerapi.AddDriverResponse, error) {
	log.Debugf("Received AddDriverRequest %+v", request)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Add the driver to the driver registry
	if err := s.drivers.AddDriver(request.Driver); err != nil && !errors.IsAlreadyExists(err) {
		log.Warnf("AddDriverRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	// Connect to the driver over gRPC
	driverConn, err := grpc.Dial(fmt.Sprintf("%s:%d", request.Driver.Host, request.Driver.Port), grpc.WithInsecure())
	if err != nil {
		log.Warnf("AddDriverRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	defer driverConn.Close()

	// Configure the driver via the DriverManagementService
	driverClient := driverapi.NewDriverManagementServiceClient(driverConn)
	driverRequest := &driverapi.ConfigureDriverRequest{
		Driver: driverapi.DriverConfig{
			Protocol: request.Driver.Protocol,
		},
	}
	if _, err := driverClient.ConfigureDriver(ctx, driverRequest); err != nil {
		log.Warnf("AddDriverRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response := &brokerapi.AddDriverResponse{}
	log.Debugf("Sending AddDriverResponse %+v", response)
	return response, nil
}

func (s *Server) UpdateDriver(ctx context.Context, request *brokerapi.UpdateDriverRequest) (*brokerapi.UpdateDriverResponse, error) {
	log.Debugf("Received UpdateDriverRequest %+v", request)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Update the driver in the driver registry
	if err := s.drivers.UpdateDriver(request.Driver); err != nil {
		log.Warnf("UpdateDriverRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	// Connect to the driver over gRPC
	driverConn, err := grpc.Dial(fmt.Sprintf("%s:%d", request.Driver.Host, request.Driver.Port), grpc.WithInsecure())
	if err != nil {
		log.Warnf("UpdateDriverRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	defer driverConn.Close()

	// Configure the driver via the DriverManagementService
	driverClient := driverapi.NewDriverManagementServiceClient(driverConn)
	driverRequest := &driverapi.ConfigureDriverRequest{
		Driver: driverapi.DriverConfig{
			Protocol: request.Driver.Protocol,
		},
	}
	if _, err := driverClient.ConfigureDriver(ctx, driverRequest); err != nil {
		log.Warnf("UpdateDriverRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response := &brokerapi.UpdateDriverResponse{}
	log.Debugf("Sending UpdateDriverResponse %+v", response)
	return response, nil
}

func (s *Server) RemoveDriver(ctx context.Context, request *brokerapi.RemoveDriverRequest) (*brokerapi.RemoveDriverResponse, error) {
	log.Debugf("Received RemoveDriverRequest %+v", request)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Remove the driver from the driver registry
	err := s.drivers.RemoveDriver(request.DriverID)
	if err != nil && !errors.IsNotFound(err) {
		log.Warnf("RemoveDriverRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	response := &brokerapi.RemoveDriverResponse{}
	log.Debugf("Sending RemoveDriverResponse %+v", response)
	return response, nil
}

func (s *Server) AddPrimitive(ctx context.Context, request *brokerapi.AddPrimitiveRequest) (*brokerapi.AddPrimitiveResponse, error) {
	log.Debugf("Received AddPrimitiveRequest %+v", request)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Get the driver configuration from the driver registry
	driver, err := s.drivers.GetDriver(request.Primitive.Driver)
	if err != nil {
		log.Warnf("AddPrimitiveRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	// If the driver is found, add the primitive configuration to the primitive registry
	if err := s.primitives.AddPrimitive(request.Primitive); err != nil && !errors.IsAlreadyExists(err) {
		log.Warnf("AddPrimitiveRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	// Connect to the driver over gRPC
	driverConn, err := grpc.Dial(fmt.Sprintf("%s:%d", driver.Host, driver.Port), grpc.WithInsecure())
	if err != nil {
		log.Warnf("AddPrimitiveRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	defer driverConn.Close()

	// Configure the driver to add a new proxy for the primitive
	proxyClient := driverapi.NewProxyManagementServiceClient(driverConn)
	addProxyRequest := &driverapi.AddProxyRequest{
		Proxy: driverapi.ProxyConfig{
			ID: driverapi.ProxyId{
				Type:      request.Primitive.ID.Type,
				Namespace: request.Primitive.ID.Namespace,
				Name:      request.Primitive.ID.Name,
			},
			Read:  request.Primitive.Proxy.Read,
			Write: request.Primitive.Proxy.Write,
		},
	}
	if _, err := proxyClient.AddProxy(ctx, addProxyRequest); err != nil {
		log.Warnf("AddDriverRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response := &brokerapi.AddPrimitiveResponse{}
	log.Debugf("Sending AddPrimitiveResponse %+v", response)
	return response, nil
}

func (s *Server) RemovePrimitive(ctx context.Context, request *brokerapi.RemovePrimitiveRequest) (*brokerapi.RemovePrimitiveResponse, error) {
	log.Debugf("Received RemovePrimitiveRequest %+v", request)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Get the primitive configuration from the primitive registry
	primitive, err := s.primitives.GetPrimitive(request.PrimitiveID)
	if errors.IsNotFound(err) {
		response := &brokerapi.RemovePrimitiveResponse{}
		log.Debugf("Sending RemovePrimitiveResponse %+v", response)
		return response, nil
	} else if err != nil {
		log.Warnf("RemovePrimitiveRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	// If the primitive is found, get the driver configuration for the primitive from the driver registry
	driver, err := s.drivers.GetDriver(primitive.Driver)
	if errors.IsNotFound(err) {
		// If the driver is not found, remove the primitive and return
		if err := s.primitives.RemovePrimitive(request.PrimitiveID); errors.IsNotFound(err) {
			response := &brokerapi.RemovePrimitiveResponse{}
			log.Debugf("Sending RemovePrimitiveResponse %+v", response)
			return response, nil
		} else if err != nil {
			log.Warnf("RemovePrimitiveRequest %+v failed: %v", request, err)
			return nil, errors.Proto(err)
		}
		response := &brokerapi.RemovePrimitiveResponse{}
		log.Debugf("Sending RemovePrimitiveResponse %+v", response)
		return response, nil
	} else if err != nil {
		log.Warnf("RemovePrimitiveRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	// Connect to the driver over gRPC
	driverConn, err := grpc.Dial(fmt.Sprintf("%s:%d", driver.Host, driver.Port), grpc.WithInsecure())
	if err != nil {
		log.Warnf("RemovePrimitiveRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}
	defer driverConn.Close()

	// Remove the primitive proxy from the driver
	proxyClient := driverapi.NewProxyManagementServiceClient(driverConn)
	removeProxyRequest := &driverapi.RemoveProxyRequest{
		ProxyID: driverapi.ProxyId{
			Type:      primitive.ID.Type,
			Namespace: primitive.ID.Namespace,
			Name:      primitive.ID.Name,
		},
	}
	if _, err := proxyClient.RemoveProxy(ctx, removeProxyRequest); err != nil {
		log.Warnf("RemovePrimitiveRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	// Once the proxy has been removed, remove the primitive from the registry
	if err := s.primitives.RemovePrimitive(request.PrimitiveID); errors.IsNotFound(err) {
		response := &brokerapi.RemovePrimitiveResponse{}
		log.Debugf("Sending RemovePrimitiveResponse %+v", response)
		return response, nil
	} else if err != nil {
		log.Warnf("RemovePrimitiveRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response := &brokerapi.RemovePrimitiveResponse{}
	log.Debugf("Sending RemovePrimitiveResponse %+v", response)
	return response, nil
}

func (s *Server) LookupPrimitive(ctx context.Context, request *primitiveapi.LookupPrimitiveRequest) (*primitiveapi.LookupPrimitiveResponse, error) {
	log.Debugf("Received LookupPrimitiveRequest %+v", request)

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Get the primitive configuration from the primitive registry
	primitive, err := s.primitives.GetPrimitive(brokerapi.PrimitiveId{
		Type:      request.PrimitiveID.Type,
		Namespace: request.PrimitiveID.Namespace,
		Name:      request.PrimitiveID.Name,
	})
	if err != nil {
		log.Warnf("LookupPrimitiveRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	// Get the configuration for the primitive driver from the driver registry
	driver, err := s.drivers.GetDriver(primitive.Driver)
	if err != nil {
		log.Warnf("LookupPrimitiveRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	// Respond with the driver connection info
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

var _ brokerapi.PrimitiveManagementServiceServer = &Server{}
var _ brokerapi.DriverManagementServiceServer = &Server{}
var _ primitiveapi.PrimitiveDiscoveryServiceServer = &Server{}
