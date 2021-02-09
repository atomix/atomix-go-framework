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

func (r *Registry) AddPrimitive(primitive primitiveapi.PrimitiveMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.primitives[primitive.Name]; ok {
		return errors.NewAlreadyExists("primitive '%s' already exists", primitive.Name)
	}
	r.primitives[primitive.Name] = primitive
	return nil
}

func (r *Registry) RemovePrimitive(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.primitives[name]; !ok {
		return errors.NewNotFound("primitive '%s' not found", name)
	}
	delete(r.primitives, name)
	return nil
}

func (r *Registry) GetPrimitive(name string) (primitiveapi.PrimitiveMeta, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	proxy, ok := r.primitives[name]
	if !ok {
		return primitiveapi.PrimitiveMeta{}, errors.NewNotFound("primitive '%s' not found", name)
	}
	return proxy, nil
}

func (r *Registry) ListPrimitives() ([]primitiveapi.PrimitiveMeta, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	primitives := make([]primitiveapi.PrimitiveMeta, 0, len(r.primitives))
	for _, primitive := range r.primitives {
		primitives = append(primitives, primitive)
	}
	return primitives, nil
}
