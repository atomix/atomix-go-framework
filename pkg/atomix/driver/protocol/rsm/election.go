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
	electionapi "github.com/atomix/api/go/atomix/primitive/election"
	"github.com/atomix/go-framework/pkg/atomix/driver/primitive"
	electiondriver "github.com/atomix/go-framework/pkg/atomix/driver/primitive/election"
	electionro "github.com/atomix/go-framework/pkg/atomix/proxy/ro/election"
	electionproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/election"
	"google.golang.org/grpc"
)

func RegisterElectionProxy(node *Node) {
	node.RegisterPrimitiveType(newElectionType(node))
}

const ElectionType = "Election"

func newElectionType(node *Node) primitive.PrimitiveType {
	return &electionType{
		node:     node,
		registry: electiondriver.NewElectionProxyRegistry(),
	}
}

type electionType struct {
	node     *Node
	registry *electiondriver.ElectionProxyRegistry
}

func (p *electionType) Name() string {
	return ElectionType
}

func (p *electionType) RegisterServer(s *grpc.Server) {
	electionapi.RegisterLeaderElectionServiceServer(s, electiondriver.NewElectionProxyServer(p.registry))
}

func (p *electionType) AddProxy(config driverapi.ProxyConfig) error {
	server := electionproxy.NewElectionProxyServer(p.node.Client)
	if !config.Write {
		server = electionro.NewReadOnlyLeaderElectionServer(server)
	}
	return p.registry.AddProxy(getPrimitiveId(config.ID), server)
}

func (p *electionType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(getPrimitiveId(id))
}

var _ primitive.PrimitiveType = &electionType{}
