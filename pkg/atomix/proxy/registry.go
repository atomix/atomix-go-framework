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

package proxy

import (
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"sync"
)

// PrimitiveType is a primitive type
type PrimitiveType struct {
	Name         string
	proxyFunc    NewProxyFunc
	cacheFunc    NewDecoratorFunc
	readOnlyFunc NewDecoratorFunc
}

func (t *PrimitiveType) NewProxy() (interface{}, error) {
	return t.proxyFunc()
}

func (t *PrimitiveType) NewCacheDecorator(s interface{}) (interface{}, error) {
	if t.cacheFunc == nil {
		return nil, errors.NewNotSupported("cache not supported for primitive type '%s'", t.Name)
	}
	return t.cacheFunc(s), nil
}

func (t *PrimitiveType) NewReadOnlyDecorator(s interface{}) (interface{}, error) {
	if t.readOnlyFunc == nil {
		return nil, errors.NewNotSupported("read-only not supported for primitive type '%s'", t.Name)
	}
	return t.readOnlyFunc(s), nil
}

type NewProxyFunc func() (interface{}, error)

type NewDecoratorFunc func(interface{}) interface{}

// NewPrimitiveTypeRegistry creates a new primitive type registry
func NewPrimitiveTypeRegistry() *PrimitiveTypeRegistry {
	return &PrimitiveTypeRegistry{
		primitiveTypes: make(map[string]*PrimitiveType),
	}
}

// PrimitiveTypeRegistry is a primitive registry
type PrimitiveTypeRegistry struct {
	primitiveTypes map[string]*PrimitiveType
	mu             sync.RWMutex
}

func (r *PrimitiveTypeRegistry) registerFunc(name string, f func(*PrimitiveType)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	primitiveType, ok := r.primitiveTypes[name]
	if !ok {
		primitiveType = &PrimitiveType{
			Name: name,
		}
		r.primitiveTypes[name] = primitiveType
	}
	f(primitiveType)
}

// RegisterProxyFunc registers a primitive proxy
func (r *PrimitiveTypeRegistry) RegisterProxyFunc(name string, f NewProxyFunc) {
	r.registerFunc(name, func(primitiveType *PrimitiveType) {
		primitiveType.proxyFunc = f
	})
}

// RegisterCacheDecoratorFunc registers a primitive cache decorator
func (r *PrimitiveTypeRegistry) RegisterCacheDecoratorFunc(name string, f NewDecoratorFunc) {
	r.registerFunc(name, func(primitiveType *PrimitiveType) {
		primitiveType.cacheFunc = f
	})
}

// RegisterReadOnlyDecoratorFunc registers a primitive read-only decorator
func (r *PrimitiveTypeRegistry) RegisterReadOnlyDecoratorFunc(name string, f NewDecoratorFunc) {
	r.registerFunc(name, func(primitiveType *PrimitiveType) {
		primitiveType.readOnlyFunc = f
	})
}

// GetPrimitiveType gets a primitive type by name
func (r *PrimitiveTypeRegistry) GetPrimitiveType(name string) (*PrimitiveType, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	primitiveType, ok := r.primitiveTypes[name]
	if !ok {
		return nil, errors.NewNotFound("primitive type '%s' not found", name)
	}
	return primitiveType, nil
}

// ListPrimitiveTypes gets a list of primitive types
func (r *PrimitiveTypeRegistry) ListPrimitiveTypes() ([]*PrimitiveType, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	primitiveTypes := make([]*PrimitiveType, 0, len(r.primitiveTypes))
	for _, primitiveType := range r.primitiveTypes {
		primitiveTypes = append(primitiveTypes, primitiveType)
	}
	return primitiveTypes, nil
}
