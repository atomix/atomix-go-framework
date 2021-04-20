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

package driver

import (
	"context"
	driverapi "github.com/atomix/api/go/atomix/management/driver"
	"github.com/atomix/go-framework/pkg/atomix/errors"
)

func newServer(driver *Driver) *Server {
	return &Server{
		driver: driver,
	}
}

type Server struct {
	driver *Driver
}

func (s *Server) StartAgent(ctx context.Context, request *driverapi.StartAgentRequest) (*driverapi.StartAgentResponse, error) {
	s.driver.log.Debugf("Received StartAgentRequest %+v", request)
	if err := s.driver.startAgent(request.AgentID, request.Address, request.Config); err != nil {
		s.driver.log.Warnf("StartAgentRequest %+v failed: %s", request, err)
		return nil, errors.Proto(err)
	}
	response := &driverapi.StartAgentResponse{}
	s.driver.log.Debugf("Sending StartAgentResponse %+v", response)
	return response, nil
}

func (s *Server) ConfigureAgent(ctx context.Context, request *driverapi.ConfigureAgentRequest) (*driverapi.ConfigureAgentResponse, error) {
	s.driver.log.Debugf("Received ConfigureAgentRequest %+v", request)
	if err := s.driver.configureAgent(request.AgentID, request.Config); err != nil {
		s.driver.log.Warnf("ConfigureAgentRequest %+v failed: %s", request, err)
		return nil, errors.Proto(err)
	}
	response := &driverapi.ConfigureAgentResponse{}
	s.driver.log.Debugf("Sending ConfigureAgentResponse %+v", response)
	return response, nil
}

func (s *Server) StopAgent(ctx context.Context, request *driverapi.StopAgentRequest) (*driverapi.StopAgentResponse, error) {
	s.driver.log.Debugf("Received StopAgentRequest %+v", request)
	if err := s.driver.stopAgent(request.AgentID); err != nil {
		s.driver.log.Warnf("StopAgentRequest %+v failed: %s", request, err)
		return nil, errors.Proto(err)
	}
	response := &driverapi.StopAgentResponse{}
	s.driver.log.Debugf("Sending StopAgentResponse %+v", response)
	return response, nil
}

var _ driverapi.DriverServer = &Server{}
