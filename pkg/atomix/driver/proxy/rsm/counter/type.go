// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package counter

import (
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	counterapi "github.com/atomix/atomix-api/go/atomix/primitive/counter"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive"
	counterdriver "github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive/counter"
	counterro "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/ro/counter"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm"
	"github.com/gogo/protobuf/jsonpb"
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
	config := rsm.RSMConfig{}
	if options.Config != nil {
		if err := jsonpb.UnmarshalString(string(options.Config), &config); err != nil {
			return err
		}
	}
	server := NewProxyServer(p.protocol.Client, config.ReadSync)
	if !options.Write {
		server = counterro.NewProxyServer(server)
	}
	return p.registry.AddProxy(id.PrimitiveId, server)
}

func (p *counterType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(id.PrimitiveId)
}

var _ primitive.PrimitiveType = &counterType{}
