// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package lock

import (
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	lockapi "github.com/atomix/atomix-api/go/atomix/primitive/lock"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive"
	lockdriver "github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive/lock"
	lockro "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/ro/lock"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm"
	"github.com/gogo/protobuf/jsonpb"
	"google.golang.org/grpc"
)

func Register(protocol *rsm.Protocol) {
	protocol.Primitives().RegisterPrimitiveType(newLockType(protocol))
}

func newLockType(protocol *rsm.Protocol) primitive.PrimitiveType {
	return &lockType{
		protocol: protocol,
		registry: lockdriver.NewProxyRegistry(),
	}
}

type lockType struct {
	protocol *rsm.Protocol
	registry *lockdriver.ProxyRegistry
}

func (p *lockType) Name() string {
	return Type
}

func (p *lockType) RegisterServer(s *grpc.Server) {
	lockapi.RegisterLockServiceServer(s, lockdriver.NewProxyServer(p.registry, p.protocol.Env))
}

func (p *lockType) AddProxy(id driverapi.ProxyId, options driverapi.ProxyOptions) error {
	config := rsm.RSMConfig{}
	if options.Config != nil {
		if err := jsonpb.UnmarshalString(string(options.Config), &config); err != nil {
			return err
		}
	}
	server := NewProxyServer(p.protocol.Client, config.ReadSync)
	if !options.Write {
		server = lockro.NewProxyServer(server)
	}
	return p.registry.AddProxy(id.PrimitiveId, server)
}

func (p *lockType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(id.PrimitiveId)
}

var _ primitive.PrimitiveType = &lockType{}
