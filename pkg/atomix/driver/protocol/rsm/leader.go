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
	leaderapi "github.com/atomix/api/go/atomix/primitive/leader"
	"github.com/atomix/go-framework/pkg/atomix/driver/primitive"
	leaderdriver "github.com/atomix/go-framework/pkg/atomix/driver/primitive/leader"
	leaderro "github.com/atomix/go-framework/pkg/atomix/proxy/ro/leader"
	leaderproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/leader"
	"google.golang.org/grpc"
)

func RegisterLeaderLatchProxy(node *Node) {
	node.RegisterPrimitiveType(newLeaderType(node))
}

const LeaderType = "LeaderLatch"

func newLeaderType(node *Node) primitive.PrimitiveType {
	return &leaderType{
		node:     node,
		registry: leaderdriver.NewLeaderLatchProxyRegistry(),
	}
}

type leaderType struct {
	node     *Node
	registry *leaderdriver.LeaderLatchProxyRegistry
}

func (p *leaderType) Name() string {
	return LeaderType
}

func (p *leaderType) RegisterServer(s *grpc.Server) {
	leaderapi.RegisterLeaderLatchServiceServer(s, leaderdriver.NewLeaderLatchProxyServer(p.registry))
}

func (p *leaderType) AddProxy(config driverapi.ProxyConfig) error {
	server := leaderproxy.NewLeaderLatchProxyServer(p.node.Client)
	if !config.Write {
		server = leaderro.NewReadOnlyLeaderLatchServer(server)
	}
	return p.registry.AddProxy(getPrimitiveId(config.ID), server)
}

func (p *leaderType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(getPrimitiveId(id))
}

var _ primitive.PrimitiveType = &leaderType{}
