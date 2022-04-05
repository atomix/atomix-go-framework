// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
	if options.Config != nil {
		if err := jsonpb.UnmarshalString(string(options.Config), &config); err != nil {
			return err
		}
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
