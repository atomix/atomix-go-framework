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

package value

import (
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	valueapi "github.com/atomix/atomix-api/go/atomix/primitive/value"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive"
	valueproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive/value"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/gossip"
	valuero "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/ro/value"
	"github.com/gogo/protobuf/jsonpb"
	"google.golang.org/grpc"
)

func Register(protocol *gossip.Protocol) {
	protocol.Primitives().RegisterPrimitiveType(newValueType(protocol))
}

const Type = "Value"

func newValueType(protocol *gossip.Protocol) primitive.PrimitiveType {
	return &valueType{
		protocol: protocol,
		registry: valueproxy.NewProxyRegistry(),
	}
}

type valueType struct {
	protocol *gossip.Protocol
	registry *valueproxy.ProxyRegistry
}

func (p *valueType) Name() string {
	return Type
}

func (p *valueType) RegisterServer(s *grpc.Server) {
	valueapi.RegisterValueServiceServer(s, valueproxy.NewProxyServer(p.registry, p.protocol.Env))
}

func (p *valueType) AddProxy(id driverapi.ProxyId, options driverapi.ProxyOptions) error {
	config := gossip.GossipConfig{}
	if err := jsonpb.UnmarshalString(string(options.Config), &config); err != nil {
		return err
	}
	server := NewProxyServer(gossip.NewServer(p.protocol.Client, config))
	if !options.Write {
		server = valuero.NewProxyServer(server)
	}
	return p.registry.AddProxy(id.PrimitiveId, server)
}

func (p *valueType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(id.PrimitiveId)
}

var _ primitive.PrimitiveType = &valueType{}
