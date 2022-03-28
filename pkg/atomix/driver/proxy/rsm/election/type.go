// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package election

import (
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	electionapi "github.com/atomix/atomix-api/go/atomix/primitive/election"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive"
	electiondriver "github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive/election"
	electionro "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/ro/election"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm"
	"github.com/gogo/protobuf/jsonpb"
	"google.golang.org/grpc"
)

func Register(protocol *rsm.Protocol) {
	protocol.Primitives().RegisterPrimitiveType(newLeaderElectionType(protocol))
}

func newLeaderElectionType(protocol *rsm.Protocol) primitive.PrimitiveType {
	return &electionType{
		protocol: protocol,
		registry: electiondriver.NewProxyRegistry(),
	}
}

type electionType struct {
	protocol *rsm.Protocol
	registry *electiondriver.ProxyRegistry
}

func (p *electionType) Name() string {
	return Type
}

func (p *electionType) RegisterServer(s *grpc.Server) {
	electionapi.RegisterLeaderElectionServiceServer(s, electiondriver.NewProxyServer(p.registry, p.protocol.Env))
}

func (p *electionType) AddProxy(id driverapi.ProxyId, options driverapi.ProxyOptions) error {
	config := rsm.RSMConfig{}
	if options.Config != nil {
		if err := jsonpb.UnmarshalString(string(options.Config), &config); err != nil {
			return err
		}
	}
	server := NewProxyServer(p.protocol.Client, config.ReadSync)
	if !options.Write {
		server = electionro.NewProxyServer(server)
	}
	return p.registry.AddProxy(id.PrimitiveId, server)
}

func (p *electionType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(id.PrimitiveId)
}

var _ primitive.PrimitiveType = &electionType{}
