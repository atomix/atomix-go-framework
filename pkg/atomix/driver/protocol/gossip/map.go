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

package gossip

import (
	driverapi "github.com/atomix/api/go/atomix/management/driver"
	"github.com/atomix/api/go/atomix/primitive/map"
	"github.com/atomix/go-framework/pkg/atomix/driver/primitive"
	mapdriver "github.com/atomix/go-framework/pkg/atomix/driver/primitive/map"
	mapro "github.com/atomix/go-framework/pkg/atomix/proxy/ro/map"
	mapproxy "github.com/atomix/go-framework/pkg/atomix/proxy/gossip/map"
	"google.golang.org/grpc"
)

func RegisterMapProxy(node *Node) {
	node.RegisterPrimitiveType(newMapType(node))
}

const MapType = "Map"

func newMapType(node *Node) primitive.PrimitiveType {
	return &mapType{
		node:     node,
		registry: mapdriver.NewMapProxyRegistry(),
	}
}

type mapType struct {
	node     *Node
	registry *mapdriver.MapProxyRegistry
}

func (p *mapType) Name() string {
	return MapType
}

func (p *mapType) RegisterServer(s *grpc.Server) {
	_map.RegisterMapServiceServer(s, mapdriver.NewMapProxyServer(p.registry))
}

func (p *mapType) AddProxy(config driverapi.ProxyConfig) error {
	server := mapproxy.NewMapProxyServer(p.node.Client)
	if !config.Write {
		server = mapro.NewReadOnlyMapServer(server)
	}
	return p.registry.AddProxy(getPrimitiveId(config.ID), server)
}

func (p *mapType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(getPrimitiveId(id))
}

var _ primitive.PrimitiveType = &mapType{}
