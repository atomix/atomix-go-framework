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
	"context"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"sync"
)

// NewResolver creates a new primitives resolver
func NewResolver(coordinator *cluster.Replica) *Resolver {
	return &Resolver{
		registry: NewRegistry(),
		client:   NewRegistryClient(coordinator),
	}
}

// Resolver is a primitives resolver
type Resolver struct {
	registry *Registry
	client   *RegistryClient
	mu       sync.RWMutex
}

func (r *Resolver) GetPrimitive(ctx context.Context, name string) (primitiveapi.PrimitiveMeta, error) {
	r.mu.RLock()
	primitive, err := r.registry.GetPrimitive(name)
	r.mu.RUnlock()
	if err == nil {
		return primitive, nil
	} else if !errors.IsNotFound(err) {
		return primitiveapi.PrimitiveMeta{}, err
	} else {
		r.mu.Lock()
		defer r.mu.Unlock()
		if primitive, err := r.registry.GetPrimitive(name); err == nil {
			return primitive, nil
		} else if !errors.IsNotFound(err) {
			return primitiveapi.PrimitiveMeta{}, err
		}

		primitive, err := r.client.GetPrimitive(ctx, name)
		if err != nil {
			return primitiveapi.PrimitiveMeta{}, err
		}
		err = r.registry.AddPrimitive(*primitive)
		if err != nil {
			return primitiveapi.PrimitiveMeta{}, err
		}
		return *primitive, nil
	}
}
