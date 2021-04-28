// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package indexedmap

import (
	driverapi "github.com/atomix/api/go/atomix/management/driver"
	indexedmapapi "github.com/atomix/api/go/atomix/primitive/indexedmap"
	"github.com/atomix/go-framework/pkg/atomix/driver/primitive"
	indexedmapdriver "github.com/atomix/go-framework/pkg/atomix/driver/primitive/indexedmap"
	indexedmapro "github.com/atomix/go-framework/pkg/atomix/driver/proxy/ro/indexedmap"
	"github.com/atomix/go-framework/pkg/atomix/driver/proxy/rsm"
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
	server := NewProxyServer(p.protocol.Client)
	if !options.Write {
		server = indexedmapro.NewProxyServer(server)
	}
	return p.registry.AddProxy(id.PrimitiveId, server)
}

func (p *indexedmapType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(id.PrimitiveId)
}

var _ primitive.PrimitiveType = &indexedmapType{}
