// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
)

func newServer(agent *Agent) *Server {
	return &Server{
		agent: agent,
	}
}

type Server struct {
	agent *Agent
}

func (s *Server) CreateProxy(ctx context.Context, request *driverapi.CreateProxyRequest) (*driverapi.CreateProxyResponse, error) {
	s.agent.log.Debugf("Received CreateProxyRequest %+v", request)
	if err := s.agent.createProxy(request.ProxyID, request.Options); err != nil {
		s.agent.log.Warnf("CreateProxyRequest %+v failed: %s", request, err)
		return nil, errors.Proto(err)
	}
	response := &driverapi.CreateProxyResponse{}
	s.agent.log.Debugf("Sending CreateProxyResponse %+v", response)
	return response, nil
}

func (s *Server) DestroyProxy(ctx context.Context, request *driverapi.DestroyProxyRequest) (*driverapi.DestroyProxyResponse, error) {
	s.agent.log.Debugf("Received DestroyProxyRequest %+v", request)
	if err := s.agent.destroyProxy(request.ProxyID); err != nil {
		s.agent.log.Warnf("DestroyProxyRequest %+v failed: %s", request, err)
		return nil, errors.Proto(err)
	}
	response := &driverapi.DestroyProxyResponse{}
	s.agent.log.Debugf("Sending DestroyProxyResponse %+v", response)
	return response, nil
}

var _ driverapi.AgentServer = &Server{}
