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

package coordinator

import (
	coordinatorapi "github.com/atomix/api/go/atomix/management/coordinator"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"sync"
)

// newPrimitiveRegistry creates a new primitive registry
func newPrimitiveRegistry() *PrimitiveRegistry {
	return &PrimitiveRegistry{
		primitives: make(map[coordinatorapi.PrimitiveId]coordinatorapi.PrimitiveConfig),
	}
}

// PrimitiveRegistry is a primitive registry
type PrimitiveRegistry struct {
	primitives map[coordinatorapi.PrimitiveId]coordinatorapi.PrimitiveConfig
	mu         sync.RWMutex
}

func (r *PrimitiveRegistry) AddPrimitive(primitive coordinatorapi.PrimitiveConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.primitives[primitive.ID]; ok {
		return errors.NewAlreadyExists("primitive '%s' already exists", primitive.ID)
	}
	r.primitives[primitive.ID] = primitive
	return nil
}

func (r *PrimitiveRegistry) RemovePrimitive(id coordinatorapi.PrimitiveId) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.primitives[id]; ok {
		return errors.NewNotFound("primitive '%s' not found", id)
	}
	delete(r.primitives, id)
	return nil
}

func (r *PrimitiveRegistry) GetPrimitive(id coordinatorapi.PrimitiveId) (coordinatorapi.PrimitiveConfig, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	primitive, ok := r.primitives[id]
	if !ok {
		return coordinatorapi.PrimitiveConfig{}, errors.NewNotFound("primitive '%s' not found", id)
	}
	return primitive, nil
}

func (r *PrimitiveRegistry) ListPrimitives() ([]coordinatorapi.PrimitiveConfig, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	primitives := make([]coordinatorapi.PrimitiveConfig, 0, len(r.primitives))
	for _, primitive := range r.primitives {
		primitives = append(primitives, primitive)
	}
	return primitives, nil
}
