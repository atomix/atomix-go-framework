// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"google.golang.org/grpc"
)

// RegisterServiceFunc is a function for registering a service
type RegisterServiceFunc func(server *grpc.Server)

// NewServiceRegistry creates a new service registry
func NewServiceRegistry() *ServiceRegistry {
	return &ServiceRegistry{
		services: make([]RegisterServiceFunc, 0),
	}
}

// ServiceRegistry is a registry of services
type ServiceRegistry struct {
	services []RegisterServiceFunc
}

// RegisterService registers a service factory function
func (r *ServiceRegistry) RegisterService(service RegisterServiceFunc) {
	r.services = append(r.services, service)
}

// GetServices returns the registered service factory functions
func (r *ServiceRegistry) GetServices() []RegisterServiceFunc {
	return r.services
}
