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

package lock

import (
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	lockapi "github.com/atomix/atomix-api/go/atomix/primitive/lock"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive"
	lockdriver "github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive/lock"
	lockro "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/ro/lock"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm"
	"github.com/gogo/protobuf/jsonpb"
	"google.golang.org/grpc"
)

func Register(protocol *rsm.Protocol) {
	protocol.Primitives().RegisterPrimitiveType(newLockType(protocol))
}

func newLockType(protocol *rsm.Protocol) primitive.PrimitiveType {
	return &lockType{
		protocol: protocol,
		registry: lockdriver.NewProxyRegistry(),
	}
}

type lockType struct {
	protocol *rsm.Protocol
	registry *lockdriver.ProxyRegistry
}

func (p *lockType) Name() string {
	return Type
}

func (p *lockType) RegisterServer(s *grpc.Server) {
	lockapi.RegisterLockServiceServer(s, lockdriver.NewProxyServer(p.registry, p.protocol.Env))
}

func (p *lockType) AddProxy(id driverapi.ProxyId, options driverapi.ProxyOptions) error {
	config := rsm.RSMConfig{}
	if options.Config != nil {
		if err := jsonpb.UnmarshalString(string(options.Config), &config); err != nil {
			return err
		}
	}
	server := NewProxyServer(p.protocol.Client, config.ReadSync)
	if !options.Write {
		server = lockro.NewProxyServer(server)
	}
	return p.registry.AddProxy(id.PrimitiveId, server)
}

func (p *lockType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(id.PrimitiveId)
}

var _ primitive.PrimitiveType = &lockType{}
