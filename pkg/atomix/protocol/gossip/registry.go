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

package gossip

import (
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"google.golang.org/grpc"
)

// RegisterServerFunc is a function for registering a server
type RegisterServerFunc func(server *grpc.Server, manager *Manager)

// NewServiceFunc is a function for creating a replica
type NewServiceFunc func(serviceID ServiceID, partition *Partition) (Service, error)

// Registry is a primitive registry
type Registry interface {
	// RegisterServer registers a server
	RegisterServer(f RegisterServerFunc)

	// GetServers gets a list of servers
	GetServers() []RegisterServerFunc

	// RegisterService registers a service
	RegisterService(t ServiceType, f NewServiceFunc)

	// GetServiceFuncs gets a list of services
	GetServiceFuncs() map[ServiceType]NewServiceFunc

	// GetServiceFunc gets a service by type
	GetServiceFunc(t ServiceType) (NewServiceFunc, error)
}

// primitiveRegistry is the default primitive registry
type primitiveRegistry struct {
	servers  []RegisterServerFunc
	services map[ServiceType]NewServiceFunc
}

func (r *primitiveRegistry) RegisterServer(f RegisterServerFunc) {
	r.servers = append(r.servers, f)
}

func (r *primitiveRegistry) GetServers() []RegisterServerFunc {
	return r.servers
}

func (r *primitiveRegistry) RegisterService(t ServiceType, f NewServiceFunc) {
	r.services[t] = f
}

func (r *primitiveRegistry) GetServiceFuncs() map[ServiceType]NewServiceFunc {
	return r.services
}

func (r *primitiveRegistry) GetServiceFunc(t ServiceType) (NewServiceFunc, error) {
	service, ok := r.services[t]
	if !ok {
		return nil, errors.NewUnknown("unknown service type '%s'", t)
	}
	return service, nil
}

// NewRegistry creates a new primitive registry
func NewRegistry() Registry {
	return &primitiveRegistry{
		services: make(map[ServiceType]NewServiceFunc),
	}
}
