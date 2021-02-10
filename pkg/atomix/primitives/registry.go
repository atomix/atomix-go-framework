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

package primitives

import (
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"sync"
)

// NewRegistry creates a new primitives registry
func NewRegistry() *Registry {
	return &Registry{
		primitives: make(map[string]primitiveapi.PrimitiveMeta),
	}
}

// Registry is a primitives registry
type Registry struct {
	primitives map[string]primitiveapi.PrimitiveMeta
	mu         sync.RWMutex
}

func (m *Registry) AddPrimitive(primitive primitiveapi.PrimitiveMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.primitives[primitive.Name]; ok {
		return errors.NewAlreadyExists("primitive '%s' already exists", primitive.Name)
	}
	m.primitives[primitive.Name] = primitive
	return nil
}

func (m *Registry) RemovePrimitive(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.primitives[name]; !ok {
		return errors.NewNotFound("primitive '%s' not found", name)
	}
	delete(m.primitives, name)
	return nil
}

func (m *Registry) GetPrimitive(name string) (primitiveapi.PrimitiveMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	proxy, ok := m.primitives[name]
	if !ok {
		return primitiveapi.PrimitiveMeta{}, errors.NewNotFound("primitive '%s' not found", name)
	}
	return proxy, nil
}

func (m *Registry) ListPrimitives() ([]primitiveapi.PrimitiveMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	primitives := make([]primitiveapi.PrimitiveMeta, 0, len(m.primitives))
	for _, primitive := range m.primitives {
		primitives = append(primitives, primitive)
	}
	return primitives, nil
}
