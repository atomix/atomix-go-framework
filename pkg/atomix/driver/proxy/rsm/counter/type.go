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

package counter

import (
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	counterapi "github.com/atomix/atomix-api/go/atomix/primitive/counter"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive"
	counterdriver "github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive/counter"
	counterro "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/ro/counter"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm"
	"google.golang.org/grpc"
)

func Register(protocol *rsm.Protocol) {
	protocol.Primitives().RegisterPrimitiveType(newCounterType(protocol))
}

func newCounterType(protocol *rsm.Protocol) primitive.PrimitiveType {
	return &counterType{
		protocol: protocol,
		registry: counterdriver.NewProxyRegistry(),
	}
}

type counterType struct {
	protocol *rsm.Protocol
	registry *counterdriver.ProxyRegistry
}

func (p *counterType) Name() string {
	return Type
}

func (p *counterType) RegisterServer(s *grpc.Server) {
	counterapi.RegisterCounterServiceServer(s, counterdriver.NewProxyServer(p.registry, p.protocol.Env))
}

func (p *counterType) AddProxy(id driverapi.ProxyId, options driverapi.ProxyOptions) error {
	server := NewProxyServer(p.protocol.Client)
	if !options.Write {
		server = counterro.NewProxyServer(server)
	}
	return p.registry.AddProxy(id.PrimitiveId, server)
}

func (p *counterType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(id.PrimitiveId)
}

var _ primitive.PrimitiveType = &counterType{}
