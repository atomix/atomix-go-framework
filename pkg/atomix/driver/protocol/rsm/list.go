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
	listapi "github.com/atomix/api/go/atomix/primitive/list"
	"github.com/atomix/go-framework/pkg/atomix/driver/primitive"
	listdriver "github.com/atomix/go-framework/pkg/atomix/driver/primitive/list"
	listro "github.com/atomix/go-framework/pkg/atomix/proxy/ro/list"
	listproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/list"
	"google.golang.org/grpc"
)

func RegisterListProxy(node *Node) {
	node.RegisterPrimitiveType(newListType(node))
}

const ListType = "List"

func newListType(node *Node) primitive.PrimitiveType {
	return &listType{
		node:     node,
		registry: listdriver.NewListProxyRegistry(),
	}
}

type listType struct {
	node     *Node
	registry *listdriver.ListProxyRegistry
}

func (p *listType) Name() string {
	return ListType
}

func (p *listType) RegisterServer(s *grpc.Server) {
	listapi.RegisterListServiceServer(s, listdriver.NewListProxyServer(p.registry))
}

func (p *listType) AddProxy(config driverapi.ProxyConfig) error {
	server := listproxy.NewListProxyServer(p.node)
	if !config.Write {
		server = listro.NewReadOnlyListServer(server)
	}
	return p.registry.AddProxy(getPrimitiveId(config.ID), server)
}

func (p *listType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(getPrimitiveId(id))
}

var _ primitive.PrimitiveType = &listType{}
