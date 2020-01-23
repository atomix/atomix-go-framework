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

package node

import (
	"github.com/atomix/go-framework/pkg/atomix/service"
	"google.golang.org/grpc"
)

var registry = newRegistry()

// GetRegistry returns the service registry
func GetRegistry() *Registry {
	return registry
}

// RegisterServer registers a service server
func RegisterServer(server func(*grpc.Server, Protocol)) {
	registry.RegisterServer(server)
}

// RegisterService registers a new service
func RegisterService(name string, service func(ctx service.Context) service.Service) {
	registry.RegisterService(name, service)
}

// RegisterServers registers service servers on the given gRPC server
func RegisterServers(server *grpc.Server, protocol Protocol) {
	for _, s := range registry.servers {
		s(server, protocol)
	}
}

// Registry is a registry of service types
type Registry struct {
	servers  []func(*grpc.Server, Protocol)
	services map[string]func(ctx service.Context) service.Service
}

// RegisterServer registers a new primitive server
func (r *Registry) RegisterServer(server func(*grpc.Server, Protocol)) {
	r.servers = append(r.servers, server)
}

// RegisterService registers a new primitive service
func (r *Registry) RegisterService(name string, service func(ctx service.Context) service.Service) {
	r.services[name] = service
}

// getType returns a service type by name
func (r *Registry) getType(name string) func(sctx service.Context) service.Service {
	return r.services[name]
}

// newRegistry returns a new primitive type registry
func newRegistry() *Registry {
	return &Registry{
		servers:  make([]func(*grpc.Server, Protocol), 0),
		services: make(map[string]func(ctx service.Context) service.Service),
	}
}
