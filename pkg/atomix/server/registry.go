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

package server

import (
	"google.golang.org/grpc"
)

// PrimitiveServer is an interface for registering a primitive proxy server
type PrimitiveServer interface {
	RegisterServer(server *grpc.Server)
}

// Registry is a primitive registry
type Registry interface {
	// Register registers a primitive
	Register(primitiveType string, primitive PrimitiveServer)

	// GetPrimitives gets a list of primitives
	GetPrimitives() []PrimitiveServer

	// GetPrimitive gets a primitive by type
	GetPrimitive(primitiveType string) PrimitiveServer
}

// primitiveRegistry is the default primitive registry
type primitiveRegistry struct {
	primitives map[string]PrimitiveServer
}

func (r *primitiveRegistry) Register(primitiveType string, primitive PrimitiveServer) {
	r.primitives[primitiveType] = primitive
}

func (r *primitiveRegistry) GetPrimitives() []PrimitiveServer {
	primitives := make([]PrimitiveServer, 0, len(r.primitives))
	for _, primitive := range r.primitives {
		primitives = append(primitives, primitive)
	}
	return primitives
}

func (r *primitiveRegistry) GetPrimitive(primitiveType string) PrimitiveServer {
	return r.primitives[primitiveType]
}

// NewRegistry creates a new primitive registry
func NewRegistry() Registry {
	return &primitiveRegistry{
		primitives: make(map[string]PrimitiveServer),
	}
}
