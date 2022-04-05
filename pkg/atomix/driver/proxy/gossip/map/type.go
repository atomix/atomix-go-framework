// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
	if options.Config != nil {
		if err := jsonpb.UnmarshalString(string(options.Config), &config); err != nil {
			return err
		}
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
