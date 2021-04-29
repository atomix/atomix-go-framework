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
	brokerapi "github.com/atomix/atomix-api/go/atomix/management/broker"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"sync"
)

func NewServer(primitives *PrimitiveRegistry) *Server {
	return &Server{
		primitives: primitives,
	}
}

type Server struct {
	primitives *PrimitiveRegistry
	mu         sync.RWMutex
}

func (s *Server) RegisterPrimitive(ctx context.Context, request *brokerapi.RegisterPrimitiveRequest) (*brokerapi.RegisterPrimitiveResponse, error) {
	log.Debugf("Received RegisterPrimitiveRequest %+v", request)

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.primitives.AddPrimitive(request.PrimitiveID, request.Address); err != nil && !errors.IsAlreadyExists(err) {
		log.Warnf("RegisterPrimitiveRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response := &brokerapi.RegisterPrimitiveResponse{}
	log.Debugf("Sending RegisterPrimitiveResponse %+v", response)
	return response, nil
}

func (s *Server) UnregisterPrimitive(ctx context.Context, request *brokerapi.UnregisterPrimitiveRequest) (*brokerapi.UnregisterPrimitiveResponse, error) {
	log.Debugf("Received UnregisterPrimitiveRequest %+v", request)

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.primitives.RemovePrimitive(request.PrimitiveID); err != nil && !errors.IsNotFound(err) {
		log.Warnf("UnregisterPrimitiveRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	response := &brokerapi.UnregisterPrimitiveResponse{}
	log.Debugf("Sending UnregisterPrimitiveResponse %+v", response)
	return response, nil
}

func (s *Server) LookupPrimitive(ctx context.Context, request *brokerapi.LookupPrimitiveRequest) (*brokerapi.LookupPrimitiveResponse, error) {
	log.Debugf("Received LookupPrimitiveRequest %+v", request)

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Get the primitive configuration from the primitive registry
	address, err := s.primitives.LookupPrimitive(request.PrimitiveID)
	if err != nil {
		log.Warnf("LookupPrimitiveRequest %+v failed: %v", request, err)
		return nil, errors.Proto(err)
	}

	// Respond with the driver connection info
	response := &brokerapi.LookupPrimitiveResponse{
		Address: address,
	}
	log.Debugf("Sending LookupPrimitiveResponse %+v", response)
	return response, nil
}

var _ brokerapi.BrokerServer = &Server{}
