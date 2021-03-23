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
	lockapi "github.com/atomix/api/go/atomix/primitive/lock"
	"github.com/atomix/go-framework/pkg/atomix/driver/primitive"
	lockdriver "github.com/atomix/go-framework/pkg/atomix/driver/primitive/lock"
	lockro "github.com/atomix/go-framework/pkg/atomix/proxy/ro/lock"
	lockproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/lock"
	"google.golang.org/grpc"
)

func RegisterLockProxy(node *Node) {
	node.RegisterPrimitiveType(newLockType(node))
}

const LockType = "Lock"

func newLockType(node *Node) primitive.PrimitiveType {
	return &lockType{
		node:     node,
		registry: lockdriver.NewLockProxyRegistry(),
	}
}

type lockType struct {
	node     *Node
	registry *lockdriver.LockProxyRegistry
}

func (p *lockType) Name() string {
	return LockType
}

func (p *lockType) RegisterServer(s *grpc.Server) {
	lockapi.RegisterLockServiceServer(s, lockdriver.NewLockProxyServer(p.registry))
}

func (p *lockType) AddProxy(config driverapi.ProxyConfig) error {
	server := lockproxy.NewLockProxyServer(p.node)
	if !config.Write {
		server = lockro.NewReadOnlyLockServer(server)
	}
	return p.registry.AddProxy(getPrimitiveId(config.ID), server)
}

func (p *lockType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(getPrimitiveId(id))
}

var _ primitive.PrimitiveType = &lockType{}
