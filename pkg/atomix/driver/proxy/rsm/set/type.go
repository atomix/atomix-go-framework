// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package set

import (
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	setapi "github.com/atomix/atomix-api/go/atomix/primitive/set"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive"
	setdriver "github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive/set"
	setro "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/ro/set"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm"
	"github.com/gogo/protobuf/jsonpb"
	"google.golang.org/grpc"
)

func Register(protocol *rsm.Protocol) {
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
	setapi.RegisterSetServiceServer(s, setdriver.NewProxyServer(p.registry, p.protocol.Env))
}

func (p *setType) AddProxy(id driverapi.ProxyId, options driverapi.ProxyOptions) error {
	config := rsm.RSMConfig{}
	if options.Config != nil {
		if err := jsonpb.UnmarshalString(string(options.Config), &config); err != nil {
			return err
		}
	}
	server := NewProxyServer(p.protocol.Client, config.ReadSync)
	if !options.Write {
		server = setro.NewProxyServer(server)
	}
	return p.registry.AddProxy(id.PrimitiveId, server)
}

func (p *setType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(id.PrimitiveId)
}

var _ primitive.PrimitiveType = &setType{}
