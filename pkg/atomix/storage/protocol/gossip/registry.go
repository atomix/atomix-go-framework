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
	"context"
	"github.com/atomix/atomix-go-sdk/pkg/atomix/errors"
	"github.com/atomix/atomix-go-sdk/pkg/atomix/time"
	"google.golang.org/grpc"
)

// RegisterServerFunc is a function for registering a server
type RegisterServerFunc func(server *grpc.Server, manager *Manager)

// NewServiceFunc is a function for creating a replica
type NewServiceFunc func(ctx context.Context, serviceID ServiceId, partition *Partition, clock time.Clock, replicas int) (Service, error)

// Registry is the default primitive registry
type Registry struct {
	servers  []RegisterServerFunc
	services map[ServiceType]NewServiceFunc
}

func (r *Registry) RegisterServer(f RegisterServerFunc) {
	r.servers = append(r.servers, f)
}

func (r *Registry) GetServers() []RegisterServerFunc {
	return r.servers
}

func (r *Registry) RegisterService(t ServiceType, f NewServiceFunc) {
	r.services[t] = f
}

func (r *Registry) GetServiceFuncs() map[ServiceType]NewServiceFunc {
	return r.services
}

func (r *Registry) GetServiceFunc(t ServiceType) (NewServiceFunc, error) {
	service, ok := r.services[t]
	if !ok {
		return nil, errors.NewUnknown("unknown service type '%s'", t)
	}
	return service, nil
}

// NewRegistry creates a new primitive registry
func NewRegistry() *Registry {
	return &Registry{
		services: make(map[ServiceType]NewServiceFunc),
	}
}
