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
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	setapi "github.com/atomix/atomix-api/go/atomix/primitive/set"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive"
	setproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive/set"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/gossip"
	setro "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/ro/set"
	"github.com/gogo/protobuf/jsonpb"
	"google.golang.org/grpc"
)

func Register(protocol *gossip.Protocol) {
	protocol.Primitives().RegisterPrimitiveType(newSetType(protocol))
}

const Type = "Set"

func newSetType(protocol *gossip.Protocol) primitive.PrimitiveType {
	return &setType{
		protocol: protocol,
		registry: setproxy.NewProxyRegistry(),
	}
}

type setType struct {
	protocol *gossip.Protocol
	registry *setproxy.ProxyRegistry
}

func (p *setType) Name() string {
	return Type
}

func (p *setType) RegisterServer(s *grpc.Server) {
	setapi.RegisterSetServiceServer(s, setproxy.NewProxyServer(p.registry, p.protocol.Env))
}

func (p *setType) AddProxy(id driverapi.ProxyId, options driverapi.ProxyOptions) error {
	config := gossip.GossipConfig{}
	if options.Config != nil {
		if err := jsonpb.UnmarshalString(string(options.Config), &config); err != nil {
			return err
		}
	}
	server := NewProxyServer(gossip.NewServer(p.protocol.Client, config))
	if !options.Write {
		server = setro.NewProxyServer(server)
	}
	return p.registry.AddProxy(id.PrimitiveId, server)
}

func (p *setType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(id.PrimitiveId)
}

var _ primitive.PrimitiveType = &setType{}
