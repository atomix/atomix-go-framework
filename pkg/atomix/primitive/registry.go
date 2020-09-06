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

package primitive

import (
	"github.com/atomix/api/proto/atomix/primitive"
	"google.golang.org/grpc"
)

// Primitive is a primitive type
type Primitive interface {
	// RegisterServer registers the primitive server with the gRPC server
	RegisterServer(server *grpc.Server, client ProtocolClient)

	// NewService creates a new primitive service
	NewService(scheduler Scheduler, context ServiceContext) Service
}

// Registry is a primitive registry
type Registry interface {
	// Register registers a primitive
	Register(primitiveType primitive.PrimitiveType, primitive Primitive)

	// GetPrimitives gets a list of primitives
	GetPrimitives() []Primitive

	// GetPrimitive gets a primitive by type
	GetPrimitive(primitiveType primitive.PrimitiveType) Primitive
}

var registry = newRegistry()

// GetRegistry returns the service registry
func GetRegistry() Registry {
	return registry
}

// Register registers a primitive type
func Register(primitiveType primitive.PrimitiveType, primitive Primitive) {
	GetRegistry().Register(primitiveType, primitive)
}

// primitiveRegistry is the default primitive registry
type primitiveRegistry struct {
	primitives map[primitive.PrimitiveType]Primitive
}

func (r *primitiveRegistry) Register(primitiveType primitive.PrimitiveType, primitive Primitive) {
	r.primitives[primitiveType] = primitive
}

func (r *primitiveRegistry) GetPrimitives() []Primitive {
	primitives := make([]Primitive, 0, len(r.primitives))
	for _, primitive := range r.primitives {
		primitives = append(primitives, primitive)
	}
	return primitives
}

func (r *primitiveRegistry) GetPrimitive(primitiveType primitive.PrimitiveType) Primitive {
	return r.primitives[primitiveType]
}

// newRegistry creates a default primitive registry
func newRegistry() Registry {
	return &primitiveRegistry{
		primitives: make(map[primitive.PrimitiveType]Primitive),
	}
}
