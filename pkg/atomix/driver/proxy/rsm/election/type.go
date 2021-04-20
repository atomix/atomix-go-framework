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

package election

import (
	driverapi "github.com/atomix/api/go/atomix/management/driver"
	electionapi "github.com/atomix/api/go/atomix/primitive/election"
	"github.com/atomix/go-framework/pkg/atomix/driver/primitive"
	electiondriver "github.com/atomix/go-framework/pkg/atomix/driver/primitive/election"
	electionro "github.com/atomix/go-framework/pkg/atomix/driver/proxy/ro/election"
	"github.com/atomix/go-framework/pkg/atomix/driver/proxy/rsm"
	"google.golang.org/grpc"
)

func Register(protocol *rsm.Protocol) {
	protocol.Primitives().RegisterPrimitiveType(newLeaderElectionType(protocol))
}

func newLeaderElectionType(protocol *rsm.Protocol) primitive.PrimitiveType {
	return &electionType{
		protocol: protocol,
		registry: electiondriver.NewProxyRegistry(),
	}
}

type electionType struct {
	protocol *rsm.Protocol
	registry *electiondriver.ProxyRegistry
}

func (p *electionType) Name() string {
	return Type
}

func (p *electionType) RegisterServer(s *grpc.Server) {
	electionapi.RegisterLeaderElectionServiceServer(s, electiondriver.NewProxyServer(p.registry))
}

func (p *electionType) AddProxy(id driverapi.ProxyId, options driverapi.ProxyOptions) error {
	server := NewProxyServer(p.protocol.Client)
	if !options.Write {
		server = electionro.NewProxyServer(server)
	}
	return p.registry.AddProxy(id.PrimitiveId, server)
}

func (p *electionType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(id.PrimitiveId)
}

var _ primitive.PrimitiveType = &electionType{}
