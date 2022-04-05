// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package counter

import (
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	counterapi "github.com/atomix/atomix-api/go/atomix/primitive/counter"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive"
	counterdriver "github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive/counter"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/gossip"
	counterro "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/ro/counter"
	"github.com/gogo/protobuf/jsonpb"
	"google.golang.org/grpc"
)

func Register(protocol *gossip.Protocol) {
	protocol.Primitives().RegisterPrimitiveType(newCounterType(protocol))
}

const Type = "Counter"

func newCounterType(protocol *gossip.Protocol) primitive.PrimitiveType {
	return &counterType{
		protocol: protocol,
		registry: counterdriver.NewProxyRegistry(),
	}
}

type counterType struct {
	protocol *gossip.Protocol
	registry *counterdriver.ProxyRegistry
}

func (p *counterType) Name() string {
	return Type
}

func (p *counterType) RegisterServer(s *grpc.Server) {
	counterapi.RegisterCounterServiceServer(s, counterdriver.NewProxyServer(p.registry, p.protocol.Env))
}

func (p *counterType) AddProxy(id driverapi.ProxyId, options driverapi.ProxyOptions) error {
	config := gossip.GossipConfig{}
	if options.Config != nil {
		if err := jsonpb.UnmarshalString(string(options.Config), &config); err != nil {
			return err
		}
	}
	server := NewProxyServer(gossip.NewServer(p.protocol.Client, config))
	if !options.Write {
		server = counterro.NewProxyServer(server)
	}
	return p.registry.AddProxy(id.PrimitiveId, server)
}

func (p *counterType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(id.PrimitiveId)
}

var _ primitive.PrimitiveType = &counterType{}
