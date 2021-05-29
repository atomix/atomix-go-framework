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

package _map

import (
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	mapapi "github.com/atomix/atomix-api/go/atomix/primitive/map"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive"
	mapproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive/map"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/gossip"
	mapro "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/ro/map"
	"github.com/gogo/protobuf/jsonpb"
	"google.golang.org/grpc"
)

func Register(protocol *gossip.Protocol) {
	protocol.Primitives().RegisterPrimitiveType(newMapType(protocol))
}

const Type = "Map"

func newMapType(protocol *gossip.Protocol) primitive.PrimitiveType {
	return &mapType{
		protocol: protocol,
		registry: mapproxy.NewProxyRegistry(),
	}
}

type mapType struct {
	protocol *gossip.Protocol
	registry *mapproxy.ProxyRegistry
}

func (p *mapType) Name() string {
	return Type
}

func (p *mapType) RegisterServer(s *grpc.Server) {
	mapapi.RegisterMapServiceServer(s, mapproxy.NewProxyServer(p.registry, p.protocol.Env))
}

func (p *mapType) AddProxy(id driverapi.ProxyId, options driverapi.ProxyOptions) error {
	config := gossip.GossipConfig{}
	if err := jsonpb.UnmarshalString(string(options.Config), &config); err != nil {
		return err
	}
	server := NewProxyServer(gossip.NewServer(p.protocol.Client, config))
	if !options.Write {
		server = mapro.NewProxyServer(server)
	}
	return p.registry.AddProxy(id.PrimitiveId, server)
}

func (p *mapType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(id.PrimitiveId)
}

var _ primitive.PrimitiveType = &mapType{}
