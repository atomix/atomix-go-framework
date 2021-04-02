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

package set

import (
	driverapi "github.com/atomix/api/go/atomix/management/driver"
	setapi "github.com/atomix/api/go/atomix/primitive/set"
	"github.com/atomix/go-framework/pkg/atomix/driver/primitive"
	setdriver "github.com/atomix/go-framework/pkg/atomix/driver/primitive/set"
	setro "github.com/atomix/go-framework/pkg/atomix/driver/proxy/ro/set"
	"github.com/atomix/go-framework/pkg/atomix/driver/proxy/rsm"
	"google.golang.org/grpc"
)

func RegisterSetProxy(protocol *rsm.Protocol) {
	protocol.Primitives().RegisterPrimitiveType(newSetType(protocol))
}

func newSetType(protocol *rsm.Protocol) primitive.PrimitiveType {
	return &setType{
		protocol: protocol,
		registry: setdriver.NewProxyRegistry(),
	}
}

type setType struct {
	protocol *rsm.Protocol
	registry *setdriver.ProxyRegistry
}

func (p *setType) Name() string {
	return Type
}

func (p *setType) RegisterServer(s *grpc.Server) {
	setapi.RegisterSetServiceServer(s, setdriver.NewProxyServer(p.registry))
}

func (p *setType) AddProxy(id driverapi.ProxyId, options driverapi.ProxyOptions) error {
	server := NewProxyServer(p.protocol.Client)
	if !options.Write {
		server = setro.NewProxyServer(server)
	}
	return p.registry.AddProxy(id.PrimitiveId, server)
}

func (p *setType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(id.PrimitiveId)
}

var _ primitive.PrimitiveType = &setType{}
