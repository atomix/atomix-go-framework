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

package leader

import (
	driverapi "github.com/atomix/api/go/atomix/management/driver"
	leaderapi "github.com/atomix/api/go/atomix/primitive/leader"
	"github.com/atomix/go-framework/pkg/atomix/driver/primitive"
	leaderdriver "github.com/atomix/go-framework/pkg/atomix/driver/primitive/leader"
	leaderro "github.com/atomix/go-framework/pkg/atomix/driver/proxy/ro/leader"
	"github.com/atomix/go-framework/pkg/atomix/driver/proxy/rsm"
	"google.golang.org/grpc"
)

func RegisterLeaderLatchProxy(protocol *rsm.Protocol) {
	protocol.Primitives().RegisterPrimitiveType(newLeaderLatchType(protocol))
}

func newLeaderLatchType(protocol *rsm.Protocol) primitive.PrimitiveType {
	return &leaderLatchType{
		protocol: protocol,
		registry: leaderdriver.NewProxyRegistry(),
	}
}

type leaderLatchType struct {
	protocol *rsm.Protocol
	registry *leaderdriver.ProxyRegistry
}

func (p *leaderLatchType) Name() string {
	return Type
}

func (p *leaderLatchType) RegisterServer(s *grpc.Server) {
	leaderapi.RegisterLeaderLatchServiceServer(s, leaderdriver.NewProxyServer(p.registry))
}

func (p *leaderLatchType) AddProxy(id driverapi.ProxyId, options driverapi.ProxyOptions) error {
	server := NewProxyServer(p.protocol.Client)
	if !options.Write {
		server = leaderro.NewProxyServer(server)
	}
	return p.registry.AddProxy(id.PrimitiveId, server)
}

func (p *leaderLatchType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(id.PrimitiveId)
}

var _ primitive.PrimitiveType = &leaderLatchType{}
