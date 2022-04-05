// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	valueapi "github.com/atomix/atomix-api/go/atomix/primitive/value"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive"
	valuedriver "github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive/value"
	valuero "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/ro/value"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm"
	"github.com/gogo/protobuf/jsonpb"
	"google.golang.org/grpc"
)

func Register(protocol *rsm.Protocol) {
	protocol.Primitives().RegisterPrimitiveType(newValueType(protocol))
}

func newValueType(protocol *rsm.Protocol) primitive.PrimitiveType {
	return &valueType{
		protocol: protocol,
		registry: valuedriver.NewProxyRegistry(),
	}
}

type valueType struct {
	protocol *rsm.Protocol
	registry *valuedriver.ProxyRegistry
}

func (p *valueType) Name() string {
	return Type
}

func (p *valueType) RegisterServer(s *grpc.Server) {
	valueapi.RegisterValueServiceServer(s, valuedriver.NewProxyServer(p.registry, p.protocol.Env))
}

func (p *valueType) AddProxy(id driverapi.ProxyId, options driverapi.ProxyOptions) error {
	config := rsm.RSMConfig{}
	if options.Config != nil {
		if err := jsonpb.UnmarshalString(string(options.Config), &config); err != nil {
			return err
		}
	}
	server := NewProxyServer(p.protocol.Client, config.ReadSync)
	if !options.Write {
		server = valuero.NewProxyServer(server)
	}
	return p.registry.AddProxy(id.PrimitiveId, server)
}

func (p *valueType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(id.PrimitiveId)
}

var _ primitive.PrimitiveType = &valueType{}
