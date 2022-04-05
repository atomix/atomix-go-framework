// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"google.golang.org/grpc"
)

// NewAgent creates a new agent node
func NewAgent(protocol proxy.Protocol) *Agent {
	return &Agent{
		Protocol: protocol,
		log:      logging.GetLogger("atomix", "agent", protocol.Name()),
	}
}

// Agent is an agent node
type Agent struct {
	proxy.Protocol
	log logging.Logger
}

// Start starts the node
func (a *Agent) Start() error {
	a.Services().RegisterService(func(s *grpc.Server) {
		server := newServer(a)
		driverapi.RegisterAgentServer(s, server)
	})
	if err := a.Protocol.Start(); err != nil {
		return err
	}
	return nil
}

func (a *Agent) createProxy(id driverapi.ProxyId, options driverapi.ProxyOptions) error {
	primitive, err := a.Protocol.Primitives().GetPrimitiveType(id.Type)
	if err != nil {
		return err
	}
	return primitive.AddProxy(id, options)
}

func (a *Agent) destroyProxy(id driverapi.ProxyId) error {
	primitive, err := a.Protocol.Primitives().GetPrimitiveType(id.Type)
	if err != nil {
		return err
	}
	return primitive.RemoveProxy(id)
}
