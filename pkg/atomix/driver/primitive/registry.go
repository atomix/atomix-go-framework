// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
