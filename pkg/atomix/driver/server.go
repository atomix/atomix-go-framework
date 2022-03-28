// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
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
