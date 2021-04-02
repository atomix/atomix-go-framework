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

package agent

import (
	driverapi "github.com/atomix/api/go/atomix/management/driver"
	"github.com/atomix/go-framework/pkg/atomix/driver/proxy"
	"github.com/atomix/go-framework/pkg/atomix/logging"
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
