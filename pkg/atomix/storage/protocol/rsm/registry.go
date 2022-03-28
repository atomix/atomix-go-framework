// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package rsm

// NewServiceFunc is a function for creating a primitive service
type NewServiceFunc func(context ServiceContext) Service

// Registry is the default primitive registry
type Registry struct {
	services map[ServiceType]NewServiceFunc
}

func (r *Registry) Register(primitiveType ServiceType, primitive NewServiceFunc) {
	r.services[primitiveType] = primitive
}

func (r *Registry) GetServices() []NewServiceFunc {
	services := make([]NewServiceFunc, 0, len(r.services))
	for _, service := range r.services {
		services = append(services, service)
	}
	return services
}

func (r *Registry) GetService(primitiveType ServiceType) NewServiceFunc {
	return r.services[primitiveType]
}

// NewRegistry creates a new primitive registry
func NewRegistry() *Registry {
	return &Registry{
		services: make(map[ServiceType]NewServiceFunc),
	}
}
