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

package broker

import (
	brokerapi "github.com/atomix/api/go/atomix/management/broker"
	"github.com/atomix/go-framework/pkg/atomix/errors"
)

// newPrimitiveRegistry creates a new primitive registry
func newPrimitiveRegistry() *PrimitiveRegistry {
	return &PrimitiveRegistry{
		primitives: make(map[brokerapi.PrimitiveId]brokerapi.PrimitiveConfig),
	}
}

// PrimitiveRegistry is a primitive registry
// The registry is not thread safe!
type PrimitiveRegistry struct {
	primitives map[brokerapi.PrimitiveId]brokerapi.PrimitiveConfig
}

func (r *PrimitiveRegistry) AddPrimitive(primitive brokerapi.PrimitiveConfig) error {
	if _, ok := r.primitives[primitive.ID]; ok {
		return errors.NewAlreadyExists("primitive '%s' already exists", primitive.ID)
	}
	r.primitives[primitive.ID] = primitive
	return nil
}

func (r *PrimitiveRegistry) RemovePrimitive(id brokerapi.PrimitiveId) error {
	if _, ok := r.primitives[id]; ok {
		return errors.NewNotFound("primitive '%s' not found", id)
	}
	delete(r.primitives, id)
	return nil
}

func (r *PrimitiveRegistry) GetPrimitive(id brokerapi.PrimitiveId) (brokerapi.PrimitiveConfig, error) {
	primitive, ok := r.primitives[id]
	if !ok {
		return brokerapi.PrimitiveConfig{}, errors.NewNotFound("primitive '%s' not found", id)
	}
	return primitive, nil
}

func (r *PrimitiveRegistry) ListPrimitives() ([]brokerapi.PrimitiveConfig, error) {
	primitives := make([]brokerapi.PrimitiveConfig, 0, len(r.primitives))
	for _, primitive := range r.primitives {
		primitives = append(primitives, primitive)
	}
	return primitives, nil
}
