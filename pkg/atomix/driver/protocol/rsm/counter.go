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
	counterapi "github.com/atomix/api/go/atomix/primitive/counter"
	counterdriver "github.com/atomix/go-framework/pkg/atomix/driver/primitive/counter"
	counterro "github.com/atomix/go-framework/pkg/atomix/proxy/ro/counter"
	counterproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/counter"
	"google.golang.org/grpc"
)

func RegisterCounterProxy(node *Node) {
	node.RegisterPrimitiveType(newCounterType(node))
}

const CounterType = "Counter"

func newCounterType(node *Node) PrimitiveType {
	return &counterType{
		node:     node,
		registry: counterdriver.NewCounterProxyRegistry(),
	}
}

type counterType struct {
	node     *Node
	registry *counterdriver.CounterProxyRegistry
}

func (p *counterType) Name() string {
	return CounterType
}

func (p *counterType) RegisterServer(s *grpc.Server) {
	counterapi.RegisterCounterServiceServer(s, counterdriver.NewCounterProxyServer(p.registry))
}

func (p *counterType) AddProxy(config driverapi.ProxyConfig) error {
	server := counterproxy.NewCounterProxyServer(p.node)
	if !config.Write {
		server = counterro.NewReadOnlyCounterServer(server)
	}
	return p.registry.AddProxy(getPrimitiveId(config.ID), server)
}

func (p *counterType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(getPrimitiveId(id))
}

var _ PrimitiveType = &counterType{}
