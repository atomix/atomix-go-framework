// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package indexedmap

import (
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	indexedmapapi "github.com/atomix/atomix-api/go/atomix/primitive/indexedmap"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive"
	indexedmapdriver "github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive/indexedmap"
	indexedmapro "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/ro/indexedmap"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm"
	"github.com/gogo/protobuf/jsonpb"
	"google.golang.org/grpc"
)

func Register(protocol *rsm.Protocol) {
	protocol.Primitives().RegisterPrimitiveType(newIndexedMapType(protocol))
}

func newIndexedMapType(protocol *rsm.Protocol) primitive.PrimitiveType {
	return &indexedmapType{
		protocol: protocol,
		registry: indexedmapdriver.NewProxyRegistry(),
	}
}

type indexedmapType struct {
	protocol *rsm.Protocol
	registry *indexedmapdriver.ProxyRegistry
}

func (p *indexedmapType) Name() string {
	return Type
}

func (p *indexedmapType) RegisterServer(s *grpc.Server) {
	indexedmapapi.RegisterIndexedMapServiceServer(s, indexedmapdriver.NewProxyServer(p.registry, p.protocol.Env))
}

func (p *indexedmapType) AddProxy(id driverapi.ProxyId, options driverapi.ProxyOptions) error {
	config := rsm.RSMConfig{}
	if options.Config != nil {
		if err := jsonpb.UnmarshalString(string(options.Config), &config); err != nil {
			return err
		}
	}
	server := NewProxyServer(p.protocol.Client, config.ReadSync)
	if !options.Write {
		server = indexedmapro.NewProxyServer(server)
	}
	return p.registry.AddProxy(id.PrimitiveId, server)
}

func (p *indexedmapType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(id.PrimitiveId)
}

var _ primitive.PrimitiveType = &indexedmapType{}
