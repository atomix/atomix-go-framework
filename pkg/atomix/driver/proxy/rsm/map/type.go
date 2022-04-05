// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package _map //nolint:golint

import (
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	mapapi "github.com/atomix/atomix-api/go/atomix/primitive/map"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive"
	mapdriver "github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive/map"
	mapro "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/ro/map"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm"
	"github.com/gogo/protobuf/jsonpb"
	"google.golang.org/grpc"
)

// Register registers the map proxy
func Register(protocol *rsm.Protocol) {
	protocol.Primitives().RegisterPrimitiveType(newMapType(protocol))
}

func newMapType(protocol *rsm.Protocol) primitive.PrimitiveType {
	return &mapType{
		protocol: protocol,
		registry: mapdriver.NewProxyRegistry(),
	}
}

type mapType struct {
	protocol *rsm.Protocol
	registry *mapdriver.ProxyRegistry
}

func (p *mapType) Name() string {
	return Type
}

func (p *mapType) RegisterServer(s *grpc.Server) {
	mapapi.RegisterMapServiceServer(s, mapdriver.NewProxyServer(p.registry, p.protocol.Env))
}

func (p *mapType) AddProxy(id driverapi.ProxyId, options driverapi.ProxyOptions) error {
	config := rsm.RSMConfig{}
	if options.Config != nil {
		if err := jsonpb.UnmarshalString(string(options.Config), &config); err != nil {
			return err
		}
	}
	server := NewProxyServer(p.protocol.Client, config.ReadSync)
	if !options.Write {
		server = mapro.NewProxyServer(server)
	}
	return p.registry.AddProxy(id.PrimitiveId, server)
}

func (p *mapType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(id.PrimitiveId)
}

var _ primitive.PrimitiveType = &mapType{}
