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
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"sync"
)

// NewPrimitiveTypeRegistry creates a new primitive type registry
func NewPrimitiveTypeRegistry() *PrimitiveTypeRegistry {
	return &PrimitiveTypeRegistry{
		primitiveTypes: make(map[string]PrimitiveType),
	}
}

// PrimitiveTypeRegistry is a primitive registry
type PrimitiveTypeRegistry struct {
	primitiveTypes map[string]PrimitiveType
	mu             sync.RWMutex
}

// RegisterPrimitiveType registers a primitive type
func (r *PrimitiveTypeRegistry) RegisterPrimitiveType(primitiveType PrimitiveType) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.primitiveTypes[primitiveType.Name()] = primitiveType
}

// GetPrimitiveType gets a primitive type by name
func (r *PrimitiveTypeRegistry) GetPrimitiveType(name string) (PrimitiveType, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	primitiveType, ok := r.primitiveTypes[name]
	if !ok {
		return nil, errors.NewNotFound("primitive type '%s' not found", name)
	}
	return primitiveType, nil
}

// ListPrimitiveTypes gets a list of primitive types
func (r *PrimitiveTypeRegistry) ListPrimitiveTypes() []PrimitiveType {
	r.mu.RLock()
	defer r.mu.RUnlock()
	primitiveTypes := make([]PrimitiveType, 0, len(r.primitiveTypes))
	for _, primitiveType := range r.primitiveTypes {
		primitiveTypes = append(primitiveTypes, primitiveType)
	}
	return primitiveTypes
}
