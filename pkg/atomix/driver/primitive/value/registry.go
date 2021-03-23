
package value

import (
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"sync"
	value "github.com/atomix/api/go/atomix/primitive/value"
)

// NewValueProxyRegistry creates a new ValueProxyRegistry
func NewValueProxyRegistry() *ValueProxyRegistry {
	return &ValueProxyRegistry{
		proxies: make(map[primitiveapi.PrimitiveId]value.ValueServiceServer),
	}
}

type ValueProxyRegistry struct {
	proxies map[primitiveapi.PrimitiveId]value.ValueServiceServer
	mu      sync.RWMutex
}

func (r *ValueProxyRegistry) AddProxy(id primitiveapi.PrimitiveId, server value.ValueServiceServer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.proxies[id]; ok {
		return errors.NewAlreadyExists("proxy '%s' already exists", id)
	}
	r.proxies[id] = server
	return nil
}

func (r *ValueProxyRegistry) RemoveProxy(id primitiveapi.PrimitiveId) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.proxies[id]; !ok {
		return errors.NewNotFound("proxy '%s' not found", id)
	}
	delete(r.proxies, id)
	return nil
}

func (r *ValueProxyRegistry) GetProxy(id primitiveapi.PrimitiveId) (value.ValueServiceServer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	proxy, ok := r.proxies[id]
	if !ok {
		return nil, errors.NewNotFound("proxy '%s' not found", id)
	}
	return proxy, nil
}
