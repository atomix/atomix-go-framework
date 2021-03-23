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

package rsm

import (
	driverapi "github.com/atomix/api/go/atomix/management/driver"
	indexedmapapi "github.com/atomix/api/go/atomix/primitive/indexedmap"
	"github.com/atomix/go-framework/pkg/atomix/driver/primitive"
	indexedmapdriver "github.com/atomix/go-framework/pkg/atomix/driver/primitive/indexedmap"
	indexedmapro "github.com/atomix/go-framework/pkg/atomix/proxy/ro/indexedmap"
	indexedmapproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/indexedmap"
	"google.golang.org/grpc"
)

func RegisterIndexedMapProxy(node *Node) {
	node.RegisterPrimitiveType(newIndexedMapType(node))
}

const IndexedMapType = "IndexedMap"

func newIndexedMapType(node *Node) primitive.PrimitiveType {
	return &indexedmapType{
		node:     node,
		registry: indexedmapdriver.NewIndexedMapProxyRegistry(),
	}
}

type indexedmapType struct {
	node     *Node
	registry *indexedmapdriver.IndexedMapProxyRegistry
}

func (p *indexedmapType) Name() string {
	return IndexedMapType
}

func (p *indexedmapType) RegisterServer(s *grpc.Server) {
	indexedmapapi.RegisterIndexedMapServiceServer(s, indexedmapdriver.NewIndexedMapProxyServer(p.registry))
}

func (p *indexedmapType) AddProxy(config driverapi.ProxyConfig) error {
	server := indexedmapproxy.NewIndexedMapProxyServer(p.node)
	if !config.Write {
		server = indexedmapro.NewReadOnlyIndexedMapServer(server)
	}
	return p.registry.AddProxy(getPrimitiveId(config.ID), server)
}

func (p *indexedmapType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(getPrimitiveId(id))
}

var _ primitive.PrimitiveType = &indexedmapType{}
