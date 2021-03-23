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
	setapi "github.com/atomix/api/go/atomix/primitive/set"
	"github.com/atomix/go-framework/pkg/atomix/driver/primitive"
	setdriver "github.com/atomix/go-framework/pkg/atomix/driver/primitive/set"
	setro "github.com/atomix/go-framework/pkg/atomix/proxy/ro/set"
	setproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/set"
	"google.golang.org/grpc"
)

func RegisterSetProxy(node *Node) {
	node.RegisterPrimitiveType(newSetType(node))
}

const SetType = "Set"

func newSetType(node *Node) primitive.PrimitiveType {
	return &setType{
		node:     node,
		registry: setdriver.NewSetProxyRegistry(),
	}
}

type setType struct {
	node     *Node
	registry *setdriver.SetProxyRegistry
}

func (p *setType) Name() string {
	return SetType
}

func (p *setType) RegisterServer(s *grpc.Server) {
	setapi.RegisterSetServiceServer(s, setdriver.NewSetProxyServer(p.registry))
}

func (p *setType) AddProxy(config driverapi.ProxyConfig) error {
	server := setproxy.NewSetProxyServer(p.node)
	if !config.Write {
		server = setro.NewReadOnlySetServer(server)
	}
	return p.registry.AddProxy(getPrimitiveId(config.ID), server)
}

func (p *setType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(getPrimitiveId(id))
}

var _ primitive.PrimitiveType = &setType{}
