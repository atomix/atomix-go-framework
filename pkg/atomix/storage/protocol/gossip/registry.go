// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package gossip

import (
	"context"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/time"
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
